use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;

use chrono::{DateTime, SecondsFormat, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::cmd::{self, Channel, Fund, ListChannels, ListFunds};
use crate::snapshot_metadata::{DatasetMetadata, FieldMetadata};

const HISTORY_SCHEMA_VERSION: u32 = 2;
const POLICY_FILE: &str = "channel-policy-history.jsonl.gz";
const POLICY_SCHEMA_FILE: &str = "channel-policy-history.schema.json";
const LIQUIDITY_FILE: &str = "channel-liquidity-history.jsonl.gz";
const LIQUIDITY_SCHEMA_FILE: &str = "channel-liquidity-history.schema.json";
const CHANNEL_FUNDS_FILE: &str = "channel-funds-history.jsonl.gz";
const CHANNEL_FUNDS_SCHEMA_FILE: &str = "channel-funds-history.schema.json";
pub(crate) const DEFAULT_PROCESSED_DIRECTORY: &str = "/var/lib/lightdash/history/processed";
pub(crate) const SNAPSHOT_HISTORY_MANIFEST: &str = "history-manifest.json";

pub(crate) struct ImportedHistory {
    pub manifest_file: String,
    pub datasets: BTreeMap<String, DatasetMetadata>,
    pub channel_funds: Vec<ChannelFundsHistoryPoint>,
}

pub(crate) struct ChannelFundsHistoryPoint {
    pub observed_at: DateTime<Utc>,
    pub channel_funds_msat: u64,
}

#[derive(Deserialize, Serialize)]
struct HistoryManifest {
    schema_version: u32,
    generated_at: String,
    node_id: String,
    source: HistorySource,
    datasets: BTreeMap<String, DatasetMetadata>,
}

#[derive(Deserialize, Serialize)]
struct HistorySource {
    raw_directory: String,
    channel_archive_count: usize,
    funds_archive_count: usize,
}

#[derive(Clone, PartialEq, Serialize)]
struct PolicyValues {
    active: Option<bool>,
    capacity_msat: u64,
    base_fee_msat: u64,
    fee_ppm: u64,
    delay_blocks: u64,
    htlc_min_msat: u64,
    htlc_max_msat: u64,
}

#[derive(Serialize)]
struct PolicyHistoryRecord<'a> {
    observed_at: String,
    policy_last_updated_at: Option<String>,
    short_channel_id: &'a str,
    source_node_id: &'a str,
    destination_node_id: &'a str,
    direction: &'static str,
    #[serde(flatten)]
    values: PolicyValues,
}

#[derive(Clone, PartialEq, Serialize)]
struct LiquidityValues {
    peer_id: String,
    connected: bool,
    state: String,
    short_channel_id: Option<String>,
    local_balance_msat: u64,
    capacity_msat: u64,
    local_balance_percent: Option<f64>,
}

#[derive(Serialize)]
struct LiquidityHistoryRecord<'a> {
    observed_at: String,
    channel_id: &'a str,
    #[serde(flatten)]
    values: &'a LiquidityValues,
}

#[derive(Deserialize, Serialize)]
struct ChannelFundsHistoryRecord {
    observed_at: String,
    channel_funds_msat: u64,
    normal_channel_count: usize,
}

pub fn run_rebuild(raw_directory: &str, output_directory: &str) -> Result<(), String> {
    let node_id = cmd::get_info().id;
    rebuild_history(
        Path::new(raw_directory),
        Path::new(output_directory),
        &node_id,
    )
}

pub fn run_export(directory: &str) -> Result<(), String> {
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    write_export(Path::new(directory), &mut writer)?;
    writer
        .flush()
        .map_err(|e| format!("flushing history export failed: {e}"))
}

pub(crate) fn import_for_snapshot(
    snapshot_directory: &Path,
    configured_directory: Option<&str>,
    expected_node_id: &str,
) -> Result<ImportedHistory, String> {
    let processed_directory = configured_directory.unwrap_or(DEFAULT_PROCESSED_DIRECTORY);
    if cmd::using_ssh() {
        log::info!("Fetching processed history from remote node");
        let archive = cmd::remote_command_output(
            "lightdash",
            &["history", "export", "--directory", processed_directory],
        )?;
        import_tar_for_snapshot(&archive, snapshot_directory, expected_node_id)
    } else {
        log::info!("Loading processed history from {processed_directory}");
        import_directory_for_snapshot(
            Path::new(processed_directory),
            snapshot_directory,
            expected_node_id,
        )
    }
}

fn import_directory_for_snapshot(
    processed_directory: &Path,
    snapshot_directory: &Path,
    expected_node_id: &str,
) -> Result<ImportedHistory, String> {
    let manifest_path = processed_directory.join("manifest.json");
    let manifest_bytes = fs::read(&manifest_path).map_err(|e| {
        format!(
            "reading history manifest `{}` failed: {e}",
            manifest_path.display()
        )
    })?;
    let manifest = validate_history_manifest(&manifest_bytes, expected_node_id)?;

    for dataset in manifest.datasets.values() {
        for relative_path in [&dataset.path, &dataset.schema_path] {
            validate_export_path(relative_path)?;
            let source = processed_directory.join(relative_path);
            let destination = snapshot_directory.join(relative_path);
            fs::copy(&source, &destination).map_err(|e| {
                format!(
                    "copying processed history `{}` to `{}` failed: {e}",
                    source.display(),
                    destination.display()
                )
            })?;
        }
    }
    fs::write(
        snapshot_directory.join(SNAPSHOT_HISTORY_MANIFEST),
        &manifest_bytes,
    )
    .map_err(|e| format!("writing snapshot history manifest failed: {e}"))?;

    let channel_funds = read_channel_funds_history(processed_directory, &manifest)?;
    Ok(ImportedHistory {
        manifest_file: SNAPSHOT_HISTORY_MANIFEST.to_string(),
        datasets: manifest.datasets,
        channel_funds,
    })
}

fn import_tar_for_snapshot(
    archive: &[u8],
    snapshot_directory: &Path,
    expected_node_id: &str,
) -> Result<ImportedHistory, String> {
    let entries = read_tar_entries(archive)?;
    let manifest_bytes = entries
        .get("manifest.json")
        .ok_or_else(|| "history export does not contain manifest.json".to_string())?;
    let manifest = validate_history_manifest(manifest_bytes, expected_node_id)?;

    for dataset in manifest.datasets.values() {
        for relative_path in [&dataset.path, &dataset.schema_path] {
            validate_export_path(relative_path)?;
            let content = entries.get(relative_path).ok_or_else(|| {
                format!("history export is missing manifest file `{relative_path}`")
            })?;
            fs::write(snapshot_directory.join(relative_path), content).map_err(|e| {
                format!("writing snapshot history file `{relative_path}` failed: {e}")
            })?;
        }
    }
    fs::write(
        snapshot_directory.join(SNAPSHOT_HISTORY_MANIFEST),
        manifest_bytes,
    )
    .map_err(|e| format!("writing snapshot history manifest failed: {e}"))?;

    let channel_funds = read_channel_funds_history(snapshot_directory, &manifest)?;
    Ok(ImportedHistory {
        manifest_file: SNAPSHOT_HISTORY_MANIFEST.to_string(),
        datasets: manifest.datasets,
        channel_funds,
    })
}

fn read_channel_funds_history(
    directory: &Path,
    manifest: &HistoryManifest,
) -> Result<Vec<ChannelFundsHistoryPoint>, String> {
    let dataset = manifest
        .datasets
        .get("channel_funds_history")
        .ok_or_else(|| "processed history is missing channel_funds_history".to_string())?;
    let path = directory.join(&dataset.path);
    let reader = BufReader::new(GzDecoder::new(
        File::open(&path).map_err(|e| format!("opening `{}` failed: {e}", path.display()))?,
    ));
    reader
        .lines()
        .map(|line| {
            let line = line.map_err(|e| format!("reading `{}` failed: {e}", path.display()))?;
            let record: ChannelFundsHistoryRecord = serde_json::from_str(&line)
                .map_err(|e| format!("parsing `{}` failed: {e}", path.display()))?;
            let observed_at = DateTime::parse_from_rfc3339(&record.observed_at)
                .map_err(|e| format!("invalid observed_at in `{}`: {e}", path.display()))?
                .with_timezone(&Utc);
            Ok(ChannelFundsHistoryPoint {
                observed_at,
                channel_funds_msat: record.channel_funds_msat,
            })
        })
        .collect()
}

fn validate_history_manifest(
    bytes: &[u8],
    expected_node_id: &str,
) -> Result<HistoryManifest, String> {
    let manifest: HistoryManifest = serde_json::from_slice(bytes)
        .map_err(|e| format!("parsing processed history manifest failed: {e}"))?;
    if manifest.schema_version != HISTORY_SCHEMA_VERSION {
        return Err(format!(
            "unsupported history schema version {}; expected {HISTORY_SCHEMA_VERSION}",
            manifest.schema_version
        ));
    }
    if manifest.node_id != expected_node_id {
        return Err(format!(
            "processed history belongs to node {}, but snapshot node is {expected_node_id}",
            manifest.node_id
        ));
    }
    Ok(manifest)
}

fn read_tar_entries(archive: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut entries = BTreeMap::new();
    let mut offset = 0_usize;
    while offset + 512 <= archive.len() {
        let header = &archive[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            return Ok(entries);
        }
        if header[156] != b'0' && header[156] != 0 {
            return Err(format!("unsupported tar entry type {}", header[156]));
        }
        let name_length = header[..100]
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(100);
        let name = std::str::from_utf8(&header[..name_length])
            .map_err(|e| format!("history export contains a non-UTF-8 path: {e}"))?;
        validate_export_path(name)?;
        let size_text = std::str::from_utf8(&header[124..136])
            .map_err(|e| format!("history export contains an invalid size: {e}"))?
            .trim_matches(char::from(0));
        let size = u64::from_str_radix(size_text, 8)
            .map_err(|e| format!("history export contains invalid size `{size_text}`: {e}"))?
            as usize;
        let content_start = offset + 512;
        let content_end = content_start
            .checked_add(size)
            .ok_or_else(|| "history export entry size overflowed".to_string())?;
        if content_end > archive.len() {
            return Err(format!("history export entry `{name}` is truncated"));
        }
        if entries
            .insert(
                name.to_string(),
                archive[content_start..content_end].to_vec(),
            )
            .is_some()
        {
            return Err(format!("history export contains duplicate entry `{name}`"));
        }
        offset = content_start + size.div_ceil(512) * 512;
    }
    Err("history export is missing the tar end marker".to_string())
}

fn rebuild_history(
    raw_directory: &Path,
    output_directory: &Path,
    node_id: &str,
) -> Result<(), String> {
    let channel_archives = archive_files(&raw_directory.join("channels"))?;
    let funds_archives = archive_files(&raw_directory.join("funds"))?;

    fs::create_dir_all(output_directory).map_err(|e| {
        format!(
            "creating processed history directory `{}` failed: {e}",
            output_directory.display()
        )
    })?;

    log::info!(
        "Rebuilding history from {} channel archives and {} funds archives",
        channel_archives.len(),
        funds_archives.len()
    );

    let policy_temp = temporary_path(output_directory, POLICY_FILE);
    let liquidity_temp = temporary_path(output_directory, LIQUIDITY_FILE);
    let channel_funds_temp = temporary_path(output_directory, CHANNEL_FUNDS_FILE);
    let policy_count = write_policy_history(&policy_temp, &channel_archives, node_id)?;
    let (liquidity_count, channel_funds_count) =
        write_liquidity_history(&liquidity_temp, &channel_funds_temp, &funds_archives)?;

    replace_file(&policy_temp, &output_directory.join(POLICY_FILE))?;
    replace_file(&liquidity_temp, &output_directory.join(LIQUIDITY_FILE))?;
    replace_file(
        &channel_funds_temp,
        &output_directory.join(CHANNEL_FUNDS_FILE),
    )?;

    let datasets = history_dataset_metadata(policy_count, liquidity_count, channel_funds_count);
    write_json_atomic(
        &output_directory.join(POLICY_SCHEMA_FILE),
        datasets
            .get("channel_policy_history")
            .expect("policy metadata exists"),
    )?;
    write_json_atomic(
        &output_directory.join(LIQUIDITY_SCHEMA_FILE),
        datasets
            .get("channel_liquidity_history")
            .expect("liquidity metadata exists"),
    )?;
    write_json_atomic(
        &output_directory.join(CHANNEL_FUNDS_SCHEMA_FILE),
        datasets
            .get("channel_funds_history")
            .expect("channel funds metadata exists"),
    )?;

    let manifest = HistoryManifest {
        schema_version: HISTORY_SCHEMA_VERSION,
        generated_at: format_datetime(Utc::now()),
        node_id: node_id.to_string(),
        source: HistorySource {
            raw_directory: raw_directory.display().to_string(),
            channel_archive_count: channel_archives.len(),
            funds_archive_count: funds_archives.len(),
        },
        datasets,
    };
    write_json_atomic(&output_directory.join("manifest.json"), &manifest)?;

    log::info!(
        "History rebuild completed with {policy_count} policy, {liquidity_count} liquidity, and {channel_funds_count} channel-funds change points in {}",
        output_directory.display()
    );
    Ok(())
}

fn archive_files(directory: &Path) -> Result<Vec<(u64, PathBuf)>, String> {
    let entries = fs::read_dir(directory).map_err(|e| {
        format!(
            "reading raw history directory `{}` failed: {e}",
            directory.display()
        )
    })?;
    let mut archives = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| format!("reading entry in `{}` failed: {e}", directory.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            log::warn!(
                "Skipping history archive with a non-UTF-8 name: {}",
                path.display()
            );
            continue;
        };
        let Some(timestamp) = file_name
            .strip_suffix(".json.xz")
            .and_then(|value| value.parse::<u64>().ok())
        else {
            log::debug!("Skipping non-history file {}", path.display());
            continue;
        };
        archives.push((timestamp, path));
    }
    archives.sort_by_key(|(timestamp, _)| *timestamp);
    Ok(archives)
}

fn write_policy_history(
    path: &Path,
    archives: &[(u64, PathBuf)],
    node_id: &str,
) -> Result<usize, String> {
    let file =
        File::create(path).map_err(|e| format!("creating `{}` failed: {e}", path.display()))?;
    let mut writer = GzEncoder::new(BufWriter::new(file), Compression::default());
    let mut previous: HashMap<(String, String), PolicyValues> = HashMap::new();
    let mut record_count = 0;

    for (index, (timestamp, archive)) in archives.iter().enumerate() {
        log_progress("channel", index, archives.len(), archive);
        let channels: ListChannels = read_xz_json(archive)?;
        let observed_at = format_timestamp(*timestamp)?;
        for channel in channels
            .channels
            .iter()
            .filter(|channel| channel.source == node_id || channel.destination == node_id)
        {
            let values = policy_values(channel);
            let key = (channel.short_channel_id.clone(), channel.source.clone());
            if previous.get(&key) == Some(&values) {
                continue;
            }
            let record = PolicyHistoryRecord {
                observed_at: observed_at.clone(),
                policy_last_updated_at: timestamp_string(channel.last_update),
                short_channel_id: &channel.short_channel_id,
                source_node_id: &channel.source,
                destination_node_id: &channel.destination,
                direction: if channel.source == node_id {
                    "local"
                } else {
                    "remote"
                },
                values: values.clone(),
            };
            write_json_line(&mut writer, &record)?;
            previous.insert(key, values);
            record_count += 1;
        }
    }

    let mut writer = writer
        .finish()
        .map_err(|e| format!("finishing `{}` failed: {e}", path.display()))?;
    writer
        .flush()
        .map_err(|e| format!("flushing `{}` failed: {e}", path.display()))?;
    Ok(record_count)
}

fn write_liquidity_history(
    path: &Path,
    channel_funds_path: &Path,
    archives: &[(u64, PathBuf)],
) -> Result<(usize, usize), String> {
    let file =
        File::create(path).map_err(|e| format!("creating `{}` failed: {e}", path.display()))?;
    let mut writer = GzEncoder::new(BufWriter::new(file), Compression::default());
    let channel_funds_file = File::create(channel_funds_path)
        .map_err(|e| format!("creating `{}` failed: {e}", channel_funds_path.display()))?;
    let mut channel_funds_writer =
        GzEncoder::new(BufWriter::new(channel_funds_file), Compression::default());
    let mut previous: HashMap<String, LiquidityValues> = HashMap::new();
    let mut previous_channel_funds = None;
    let mut record_count = 0;
    let mut channel_funds_count = 0;

    for (index, (timestamp, archive)) in archives.iter().enumerate() {
        log_progress("funds", index, archives.len(), archive);
        let funds: ListFunds = read_xz_json(archive)?;
        let observed_at = format_timestamp(*timestamp)?;
        let normal_channels = funds
            .channels
            .iter()
            .filter(|fund| fund.state == "CHANNELD_NORMAL");
        let channel_funds_msat = normal_channels
            .clone()
            .map(|fund| fund.our_amount_msat)
            .sum();
        let normal_channel_count = normal_channels.count();
        let current_channel_funds = (channel_funds_msat, normal_channel_count);
        if previous_channel_funds != Some(current_channel_funds) {
            write_json_line(
                &mut channel_funds_writer,
                &ChannelFundsHistoryRecord {
                    observed_at: observed_at.clone(),
                    channel_funds_msat,
                    normal_channel_count,
                },
            )?;
            previous_channel_funds = Some(current_channel_funds);
            channel_funds_count += 1;
        }
        for fund in &funds.channels {
            let values = liquidity_values(fund);
            if previous.get(&fund.channel_id) == Some(&values) {
                continue;
            }
            let record = LiquidityHistoryRecord {
                observed_at: observed_at.clone(),
                channel_id: &fund.channel_id,
                values: &values,
            };
            write_json_line(&mut writer, &record)?;
            previous.insert(fund.channel_id.clone(), values);
            record_count += 1;
        }
    }

    let mut writer = writer
        .finish()
        .map_err(|e| format!("finishing `{}` failed: {e}", path.display()))?;
    writer
        .flush()
        .map_err(|e| format!("flushing `{}` failed: {e}", path.display()))?;
    let mut channel_funds_writer = channel_funds_writer
        .finish()
        .map_err(|e| format!("finishing `{}` failed: {e}", channel_funds_path.display()))?;
    channel_funds_writer
        .flush()
        .map_err(|e| format!("flushing `{}` failed: {e}", channel_funds_path.display()))?;
    Ok((record_count, channel_funds_count))
}

fn policy_values(channel: &Channel) -> PolicyValues {
    PolicyValues {
        active: channel.active,
        capacity_msat: channel.amount_msat,
        base_fee_msat: channel.base_fee_millisatoshi,
        fee_ppm: channel.fee_per_millionth,
        delay_blocks: channel.delay,
        htlc_min_msat: channel.htlc_minimum_msat,
        htlc_max_msat: channel.htlc_maximum_msat,
    }
}

fn liquidity_values(fund: &Fund) -> LiquidityValues {
    LiquidityValues {
        peer_id: fund.peer_id.clone(),
        connected: fund.connected,
        state: fund.state.clone(),
        short_channel_id: fund.short_channel_id.clone(),
        local_balance_msat: fund.our_amount_msat,
        capacity_msat: fund.amount_msat,
        local_balance_percent: if fund.amount_msat == 0 {
            None
        } else {
            Some(fund.our_amount_msat as f64 / fund.amount_msat as f64 * 100.0)
        },
    }
}

fn read_xz_json<T: DeserializeOwned>(path: &Path) -> Result<T, String> {
    let output = Command::new("xzcat")
        .arg(path)
        .output()
        .map_err(|e| format!("executing xzcat for `{}` failed: {e}", path.display()))?;
    if !output.status.success() {
        return Err(format!(
            "xzcat failed for `{}` with status {}: {}",
            path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|e| {
        format!(
            "parsing historical archive `{}` failed: {e}",
            path.display()
        )
    })
}

fn write_json_line(writer: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|e| format!("serializing historical record failed: {e}"))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("writing historical record failed: {e}"))
}

fn replace_file(source: &Path, destination: &Path) -> Result<(), String> {
    fs::rename(source, destination).map_err(|e| {
        format!(
            "replacing `{}` with `{}` failed: {e}",
            destination.display(),
            source.display()
        )
    })
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let temporary = temporary_path(
        path.parent().expect("processed files have a parent"),
        path.file_name()
            .and_then(|name| name.to_str())
            .expect("processed file names are UTF-8"),
    );
    let file = File::create(&temporary)
        .map_err(|e| format!("creating `{}` failed: {e}", temporary.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .map_err(|e| format!("serializing `{}` failed: {e}", path.display()))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("writing `{}` failed: {e}", temporary.display()))?;
    writer
        .flush()
        .map_err(|e| format!("flushing `{}` failed: {e}", temporary.display()))?;
    replace_file(&temporary, path)
}

fn temporary_path(directory: &Path, file_name: &str) -> PathBuf {
    directory.join(format!(".{file_name}.tmp-{}", std::process::id()))
}

fn log_progress(kind: &str, index: usize, total: usize, path: &Path) {
    if index % 25 == 0 || index + 1 == total {
        log::info!(
            "Processing {kind} history archive {}/{}: {}",
            index + 1,
            total,
            path.display()
        );
    }
}

fn format_timestamp(timestamp: u64) -> Result<String, String> {
    DateTime::from_timestamp(timestamp as i64, 0)
        .map(format_datetime)
        .ok_or_else(|| format!("history timestamp {timestamp} is outside the supported range"))
}

fn timestamp_string(timestamp: u64) -> Option<String> {
    DateTime::from_timestamp(timestamp as i64, 0).map(format_datetime)
}

fn format_datetime(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn history_dataset_metadata(
    policy_count: usize,
    liquidity_count: usize,
    channel_funds_count: usize,
) -> BTreeMap<String, DatasetMetadata> {
    BTreeMap::from([
        (
            "channel_policy_history".to_string(),
            DatasetMetadata {
                path: POLICY_FILE.to_string(),
                schema_path: POLICY_SCHEMA_FILE.to_string(),
                format: "gzip-jsonl".to_string(),
                description: "Change points for both directions of local-node channel policies, derived from archived listchannels responses.".to_string(),
                record_count: policy_count,
                primary_key: None,
                fields: policy_fields(),
            },
        ),
        (
            "channel_liquidity_history".to_string(),
            DatasetMetadata {
                path: LIQUIDITY_FILE.to_string(),
                schema_path: LIQUIDITY_SCHEMA_FILE.to_string(),
                format: "gzip-jsonl".to_string(),
                description: "Change points for local channel balances and states, derived from archived listfunds responses.".to_string(),
                record_count: liquidity_count,
                primary_key: None,
                fields: liquidity_fields(),
            },
        ),
        (
            "channel_funds_history".to_string(),
            DatasetMetadata {
                path: CHANNEL_FUNDS_FILE.to_string(),
                schema_path: CHANNEL_FUNDS_SCHEMA_FILE.to_string(),
                format: "gzip-jsonl".to_string(),
                description: "Change points for total local balance in normal channels, derived from complete archived listfunds responses for time-weighted deployed-capital calculations.".to_string(),
                record_count: channel_funds_count,
                primary_key: None,
                fields: channel_funds_fields(),
            },
        ),
    ])
}

fn write_export(directory: &Path, writer: &mut impl Write) -> Result<(), String> {
    let manifest_path = directory.join("manifest.json");
    let manifest: HistoryManifest =
        serde_json::from_slice(&fs::read(&manifest_path).map_err(|e| {
            format!(
                "reading history manifest `{}` failed: {e}",
                manifest_path.display()
            )
        })?)
        .map_err(|e| {
            format!(
                "parsing history manifest `{}` failed: {e}",
                manifest_path.display()
            )
        })?;

    let mut paths = BTreeSet::from(["manifest.json".to_string()]);
    for dataset in manifest.datasets.values() {
        validate_export_path(&dataset.path)?;
        validate_export_path(&dataset.schema_path)?;
        paths.insert(dataset.path.clone());
        paths.insert(dataset.schema_path.clone());
    }

    for relative_path in paths {
        write_tar_file(directory, &relative_path, writer)?;
    }
    writer
        .write_all(&[0; 1024])
        .map_err(|e| format!("finishing history export tar stream failed: {e}"))
}

fn validate_export_path(path: &str) -> Result<(), String> {
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "unsafe processed history path `{}`",
            path.display()
        ));
    }
    if path.as_os_str().len() > 100 {
        return Err(format!(
            "processed history path `{}` is too long for the export format",
            path.display()
        ));
    }
    Ok(())
}

fn write_tar_file(
    directory: &Path,
    relative_path: &str,
    writer: &mut impl Write,
) -> Result<(), String> {
    validate_export_path(relative_path)?;
    let path = directory.join(relative_path);
    let metadata = fs::metadata(&path)
        .map_err(|e| format!("reading metadata for `{}` failed: {e}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "processed history path `{}` is not a file",
            path.display()
        ));
    }

    let mut header = [0_u8; 512];
    write_tar_text(&mut header[0..100], relative_path)?;
    write_tar_octal(&mut header[100..108], 0o644)?;
    write_tar_octal(&mut header[108..116], 0)?;
    write_tar_octal(&mut header[116..124], 0)?;
    write_tar_octal(&mut header[124..136], metadata.len())?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    write_tar_octal(&mut header[136..148], modified)?;
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
    write_tar_checksum(&mut header[148..156], checksum)?;

    writer
        .write_all(&header)
        .map_err(|e| format!("writing tar header for `{relative_path}` failed: {e}"))?;
    let mut file =
        File::open(&path).map_err(|e| format!("opening `{}` failed: {e}", path.display()))?;
    io::copy(&mut file, writer)
        .map_err(|e| format!("streaming `{}` failed: {e}", path.display()))?;
    let padding = (512 - metadata.len() % 512) % 512;
    if padding > 0 {
        let zeroes = [0_u8; 512];
        writer
            .write_all(&zeroes[..padding as usize])
            .map_err(|e| format!("writing tar padding for `{relative_path}` failed: {e}"))?;
    }
    Ok(())
}

fn write_tar_text(field: &mut [u8], value: &str) -> Result<(), String> {
    if value.len() > field.len() {
        return Err(format!("tar text field is too small for `{value}`"));
    }
    field[..value.len()].copy_from_slice(value.as_bytes());
    Ok(())
}

fn write_tar_octal(field: &mut [u8], value: u64) -> Result<(), String> {
    let value = format!("{:0width$o}", value, width = field.len() - 1);
    if value.len() >= field.len() {
        return Err(format!("value `{value}` does not fit in tar numeric field"));
    }
    field[..value.len()].copy_from_slice(value.as_bytes());
    field[value.len()] = 0;
    Ok(())
}

fn write_tar_checksum(field: &mut [u8], value: u64) -> Result<(), String> {
    let value = format!("{value:06o}");
    if value.len() != 6 || field.len() != 8 {
        return Err(format!("checksum `{value}` does not fit in tar header"));
    }
    field[..6].copy_from_slice(value.as_bytes());
    field[6] = 0;
    field[7] = b' ';
    Ok(())
}

fn policy_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("observed_at".into(), field("string", false, Some("rfc3339_utc"), "Time encoded in the source archive filename; this is when the policy was observed.")),
        ("policy_last_updated_at".into(), field("string", true, Some("rfc3339_utc"), "Gossip last_update time reported for this directed policy.")),
        ("short_channel_id".into(), field("string", false, None, "Short channel ID identifying the channel.")),
        ("source_node_id".into(), field("string", false, None, "Node that advertised this directed policy.")),
        ("destination_node_id".into(), field("string", false, None, "Node receiving forwards under this directed policy.")),
        ("direction".into(), field("string", false, None, "Whether source_node_id is the local node (`local`) or its peer (`remote`).")),
        ("active".into(), field("boolean", true, None, "Whether the directed channel was active when observed.")),
        ("capacity_msat".into(), field("integer", false, Some("msat"), "Full channel capacity reported by listchannels.")),
        ("base_fee_msat".into(), field("integer", false, Some("msat"), "Fixed forwarding fee for the directed policy.")),
        ("fee_ppm".into(), field("integer", false, Some("ppm"), "Proportional forwarding fee for the directed policy.")),
        ("delay_blocks".into(), field("integer", false, Some("block"), "CLTV delta required by the directed policy.")),
        ("htlc_min_msat".into(), field("integer", false, Some("msat"), "Minimum HTLC amount accepted by the directed policy.")),
        ("htlc_max_msat".into(), field("integer", false, Some("msat"), "Maximum HTLC amount accepted by the directed policy.")),
    ])
}

fn liquidity_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("observed_at".into(), field("string", false, Some("rfc3339_utc"), "Time encoded in the source archive filename; this is when the balance was observed.")),
        ("channel_id".into(), field("string", false, None, "Full local channel identifier, used as the stable series key.")),
        ("short_channel_id".into(), field("string", true, None, "Short channel ID when assigned.")),
        ("peer_id".into(), field("string", false, None, "Public key of the remote peer.")),
        ("connected".into(), field("boolean", false, None, "Whether the peer connection was active when observed.")),
        ("state".into(), field("string", false, None, "Core Lightning channel state when observed.")),
        ("local_balance_msat".into(), field("integer", false, Some("msat"), "Channel balance controlled by the local node.")),
        ("capacity_msat".into(), field("integer", false, Some("msat"), "Full channel capacity.")),
        ("local_balance_percent".into(), formula(field("number", true, Some("percent"), "Local balance as a percentage of full channel capacity."), "local_balance_msat / capacity_msat * 100")),
    ])
}

fn channel_funds_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("observed_at".into(), field("string", false, Some("rfc3339_utc"), "Time encoded in the source archive filename; the total remains in effect until the next change point.")),
        ("channel_funds_msat".into(), formula(field("integer", false, Some("msat"), "Total local balance held in CHANNELD_NORMAL channels at this observation."), "sum(listfunds.channels[state = CHANNELD_NORMAL].our_amount_msat)")),
        ("normal_channel_count".into(), formula(field("integer", false, Some("channel"), "Number of CHANNELD_NORMAL channels included in channel_funds_msat."), "count(listfunds.channels[state = CHANNELD_NORMAL])")),
    ])
}

fn field(json_type: &str, nullable: bool, unit: Option<&str>, description: &str) -> FieldMetadata {
    FieldMetadata {
        json_type: json_type.to_string(),
        nullable,
        unit: unit.map(str::to_string),
        description: description.to_string(),
        formula: None,
        source: None,
        aggregation: None,
        warning: None,
    }
}

fn formula(mut metadata: FieldMetadata, value: &str) -> FieldMetadata {
    metadata.formula = Some(value.to_string());
    metadata
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::time::{SystemTime, UNIX_EPOCH};

    use flate2::read::GzDecoder;
    use serde_json::Value;

    use super::{
        import_directory_for_snapshot, import_tar_for_snapshot, rebuild_history, write_export,
    };

    #[test]
    fn rebuilds_change_point_history_and_metadata() {
        let root = temporary_test_directory();
        let raw = root.join("raw");
        let output = root.join("processed");
        fs::create_dir_all(raw.join("channels")).unwrap();
        fs::create_dir_all(raw.join("funds")).unwrap();

        write_xz(
            &raw.join("channels/1700000000.json.xz"),
            r#"{"channels":[{"source":"local","destination":"peer","short_channel_id":"1x1x1","amount_msat":2000000,"last_update":1699999990,"base_fee_millisatoshi":1000,"fee_per_millionth":100,"delay":34,"htlc_minimum_msat":0,"htlc_maximum_msat":1900000,"active":true},{"source":"peer","destination":"local","short_channel_id":"1x1x1","amount_msat":2000000,"last_update":1699999991,"base_fee_millisatoshi":0,"fee_per_millionth":200,"delay":34,"htlc_minimum_msat":0,"htlc_maximum_msat":1800000,"active":true}]}"#,
        );
        write_xz(
            &raw.join("channels/1700003600.json.xz"),
            r#"{"channels":[{"source":"local","destination":"peer","short_channel_id":"1x1x1","amount_msat":2000000,"last_update":1700003500,"base_fee_millisatoshi":1000,"fee_per_millionth":150,"delay":34,"htlc_minimum_msat":0,"htlc_maximum_msat":1900000,"active":true},{"source":"peer","destination":"local","short_channel_id":"1x1x1","amount_msat":2000000,"last_update":1699999991,"base_fee_millisatoshi":0,"fee_per_millionth":200,"delay":34,"htlc_minimum_msat":0,"htlc_maximum_msat":1800000,"active":true}]}"#,
        );
        write_xz(
            &raw.join("funds/1700000000.json.xz"),
            r#"{"channels":[{"peer_id":"peer","connected":true,"state":"CHANNELD_NORMAL","channel_id":"full-id","short_channel_id":"1x1x1","our_amount_msat":500000,"amount_msat":2000000,"funding_txid":"txid","funding_output":0}],"outputs":[]}"#,
        );
        write_xz(
            &raw.join("funds/1700003600.json.xz"),
            r#"{"channels":[{"peer_id":"peer","connected":true,"state":"CHANNELD_NORMAL","channel_id":"full-id","short_channel_id":"1x1x1","our_amount_msat":750000,"amount_msat":2000000,"funding_txid":"txid","funding_output":0}],"outputs":[]}"#,
        );
        write_xz(
            &raw.join("funds/1700007200.json.xz"),
            r#"{"channels":[],"outputs":[]}"#,
        );

        rebuild_history(&raw, &output, "local").unwrap();

        let manifest: Value =
            serde_json::from_slice(&fs::read(output.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["schema_version"], 2);
        assert_eq!(
            manifest["datasets"]["channel_policy_history"]["record_count"],
            3
        );
        assert_eq!(
            manifest["datasets"]["channel_liquidity_history"]["record_count"],
            2
        );
        assert_eq!(
            manifest["datasets"]["channel_funds_history"]["record_count"],
            3
        );

        let policy_lines = read_gzip_lines(&output.join("channel-policy-history.jsonl.gz"));
        assert_eq!(policy_lines.len(), 3);
        assert_eq!(policy_lines[2]["fee_ppm"], 150);
        assert_eq!(policy_lines[2]["direction"], "local");

        let liquidity_lines = read_gzip_lines(&output.join("channel-liquidity-history.jsonl.gz"));
        assert_eq!(liquidity_lines.len(), 2);
        assert_eq!(liquidity_lines[1]["local_balance_percent"], 37.5);
        let channel_funds_lines = read_gzip_lines(&output.join("channel-funds-history.jsonl.gz"));
        assert_eq!(channel_funds_lines.len(), 3);
        assert_eq!(channel_funds_lines[0]["channel_funds_msat"], 500_000);
        assert_eq!(channel_funds_lines[1]["channel_funds_msat"], 750_000);
        assert_eq!(channel_funds_lines[2]["channel_funds_msat"], 0);
        assert_eq!(channel_funds_lines[2]["normal_channel_count"], 0);
        assert!(output.join("channel-policy-history.schema.json").is_file());
        assert!(output
            .join("channel-liquidity-history.schema.json")
            .is_file());

        let mut export = Vec::new();
        write_export(&output, &mut export).unwrap();
        assert_eq!(
            tar_entry_names(&export),
            vec![
                "channel-funds-history.jsonl.gz",
                "channel-funds-history.schema.json",
                "channel-liquidity-history.jsonl.gz",
                "channel-liquidity-history.schema.json",
                "channel-policy-history.jsonl.gz",
                "channel-policy-history.schema.json",
                "manifest.json",
            ]
        );

        let local_snapshot = root.join("local-snapshot");
        fs::create_dir_all(&local_snapshot).unwrap();
        let local_import =
            import_directory_for_snapshot(&output, &local_snapshot, "local").unwrap();
        assert_eq!(local_import.datasets.len(), 3);
        assert_eq!(local_import.channel_funds.len(), 3);
        assert!(local_snapshot.join(&local_import.manifest_file).is_file());
        assert!(local_snapshot
            .join("channel-policy-history.jsonl.gz")
            .is_file());

        let remote_snapshot = root.join("remote-snapshot");
        fs::create_dir_all(&remote_snapshot).unwrap();
        let remote_import = import_tar_for_snapshot(&export, &remote_snapshot, "local").unwrap();
        assert_eq!(remote_import.datasets.len(), 3);
        assert_eq!(remote_import.channel_funds.len(), 3);
        assert!(remote_snapshot.join(&remote_import.manifest_file).is_file());
        assert!(remote_snapshot
            .join("channel-liquidity-history.schema.json")
            .is_file());
        assert!(import_tar_for_snapshot(&export, &remote_snapshot, "another-node").is_err());

        fs::remove_dir_all(root).unwrap();
    }

    fn write_xz(path: &Path, content: &str) {
        let file = File::create(path).unwrap();
        let mut child = Command::new("xz")
            .arg("-c")
            .stdin(Stdio::piped())
            .stdout(Stdio::from(file))
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(content.as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success());
    }

    fn read_gzip_lines(path: &Path) -> Vec<Value> {
        let mut content = String::new();
        GzDecoder::new(File::open(path).unwrap())
            .read_to_string(&mut content)
            .unwrap();
        content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn tar_entry_names(archive: &[u8]) -> Vec<&str> {
        let mut names = Vec::new();
        let mut offset = 0;
        while offset + 512 <= archive.len() && archive[offset..offset + 512].iter().any(|b| *b != 0)
        {
            let header = &archive[offset..offset + 512];
            let name_length = header[..100]
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(100);
            names.push(std::str::from_utf8(&header[..name_length]).unwrap());
            let size = u64::from_str_radix(
                std::str::from_utf8(&header[124..136])
                    .unwrap()
                    .trim_matches(char::from(0)),
                8,
            )
            .unwrap();
            offset += 512 + size.div_ceil(512) as usize * 512;
        }
        names
    }

    fn temporary_test_directory() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lightdash-history-test-{}-{nonce}",
            std::process::id()
        ))
    }
}
