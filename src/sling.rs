use crate::cmd::{Fund, SettledForward};
use crate::store::Store;
use chrono::Utc;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const SOURCE_PPM_MAX: u64 = 300;
const MAX_BALANCE: f64 = 0.5;
const LOOKBACK_HOURS: i64 = 24;
const MIN_AMOUNT_SAT: u64 = 10_000;
const CMD: &str = "lightning-cli";
/// Minimum balance percentage (our funds / total capacity) for a channel to be used as candidate.
const MIN_CANDIDATE_BALANCE: f64 = 0.7;
const TARGET_IN_STATE: &str = "SENT_REMOVE_ACK_REVOCATION";
const OPPOSITE_OUT_STATE: &str = "RCVD_REMOVE_ACK_REVOCATION";
const OPPOSITE_FEE_MARGIN_SAT: u64 = 100;

/// Computes candidates with liquidity for sling rebalancing.
///
/// Sling's `outppm` parameter filters candidates by ppm but not by balance.
/// We want to pull from channels where:
///   1. ppm < SOURCE_PPM_MAX (drain low-fee channels)
///   2. balance > MIN_CANDIDATE_BALANCE (only drain channels that have enough liquidity on our side)
///
/// Since sling doesn't support filtering by balance natively, we explicitly compute
/// the candidates list ourselves and pass it via the `candidates` parameter.
fn compute_candidates<'a>(store: &'a Store, channels: &'a [Fund]) -> Vec<&'a String> {
    channels
        .iter()
        .filter_map(|ch| {
            let scid = ch.short_channel_id.as_ref()?;
            let our = store.get_channel(scid, &store.info.id)?;
            if our.fee_per_millionth < SOURCE_PPM_MAX && ch.perc_float() > MIN_CANDIDATE_BALANCE {
                Some(scid)
            } else {
                None
            }
        })
        .collect()
}

/// Formats candidates as a JSON array string for sling's `candidates` parameter.
fn candidates_to_json(candidates: &[&String]) -> String {
    format!(
        "[{}]",
        candidates
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[derive(Default)]
struct RecentOutboundStats {
    count: u64,
    fee_ppm_weighted_sum: u64,
    routed_sat: u64,
}

fn recent_outbound_stats(
    settled_forwards: &[SettledForward],
    short_channel_id: &str,
) -> RecentOutboundStats {
    settled_forwards
        .into_iter()
        .filter(|f| f.out_channel == short_channel_id)
        .fold(RecentOutboundStats::default(), |mut acc, f| {
            acc.count += 1;
            acc.fee_ppm_weighted_sum += f.fee_ppm.saturating_mul(f.out_sat);
            acc.routed_sat += f.out_sat;
            acc
        })
}

impl RecentOutboundStats {
    fn average_fee_ppm(&self) -> u64 {
        if self.routed_sat == 0 {
            0
        } else {
            self.fee_ppm_weighted_sum / self.routed_sat
        }
    }
}

fn compute_max_ppm(avg_fee_ppm: u64) -> u64 {
    avg_fee_ppm / 2
}

fn stop_existing_sling_jobs() {
    log::info!("EXECUTE_SLING is set, stopping existing sling jobs before creating new ones");
    let result = crate::cmd::cmd_result(CMD, &["sling-stop"]);
    log::debug!("sling-stop return: {result}");
}

#[derive(Clone, Deserialize)]
struct SlingSnapshotEntry {
    alias: String,
    last_success_reb: String,
    rebamount: String,
    scid: String,
    status: Vec<String>,
    w_feeppm: u64,
}

#[derive(Clone)]
struct BalancedRebalanceTarget {
    alias: String,
    scid: String,
    rebamount_sat: u64,
    last_success_reb: String,
    max_out_amount_sat: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct HtlcRecord {
    short_channel_id: String,
    amount_msat: u64,
    direction: String,
    payment_hash: String,
    state: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RebalanceOppositeEntry {
    pub alias: String,
    pub target_scid: String,
    pub rebamount_sat: u64,
    pub last_success_reb: String,
    pub payment_hash: Option<String>,
    pub payment_hash_candidates: Vec<String>,
    pub incoming_htlc_state: Option<String>,
    pub match_status: String,
    pub opposite_scid: Option<String>,
    pub candidate_opposite_scids: Vec<String>,
}

struct SlingSnapshotArtifact {
    file_path: PathBuf,
    entries: Vec<SlingSnapshotEntry>,
}

fn snapshot_sling_stats(directory: &str) -> Option<SlingSnapshotArtifact> {
    let path = Path::new(directory);
    if let Err(e) = fs::create_dir_all(path) {
        log::error!(
            "failed to create sling stats directory {}: {}",
            directory,
            e
        );
        return None;
    }

    let stats = crate::cmd::cmd_result(CMD, &["sling-stats", "true"]);
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let file_path = path.join(format!("sling-stats-{timestamp}.json"));
    let entries = match serde_json::from_value::<Vec<SlingSnapshotEntry>>(stats.clone()) {
        Ok(entries) => entries,
        Err(e) => {
            log::error!("failed to parse sling stats snapshot: {}", e);
            return None;
        }
    };
    match serde_json::to_string_pretty(&stats) {
        Ok(json) => match fs::write(&file_path, json) {
            Ok(_) => {
                log::info!("saved sling stats snapshot to {}", file_path.display());
                Some(SlingSnapshotArtifact { file_path, entries })
            }
            Err(e) => {
                log::error!(
                    "failed to write sling stats snapshot {}: {}",
                    file_path.display(),
                    e
                );
                None
            }
        },
        Err(e) => {
            log::error!("failed to serialize sling stats snapshot: {}", e);
            None
        }
    }
}

fn has_balanced_status(statuses: &[String]) -> bool {
    statuses.iter().any(|status| status.contains("Balanced"))
}

fn parse_rebamount_sat(rebamount: &str) -> Option<u64> {
    rebamount.replace(',', "").parse().ok()
}

fn balanced_targets(entries: &[SlingSnapshotEntry]) -> Vec<BalancedRebalanceTarget> {
    entries
        .iter()
        .filter(|entry| has_balanced_status(&entry.status))
        .filter_map(|entry| {
            if entry.last_success_reb == "Never" {
                return None;
            }
            let rebamount_sat = parse_rebamount_sat(&entry.rebamount)?;
            let fee_budget_sat = ((rebamount_sat.saturating_mul(entry.w_feeppm))
                .saturating_add(999_999))
                / 1_000_000;
            Some(BalancedRebalanceTarget {
                alias: entry.alias.clone(),
                scid: entry.scid.clone(),
                rebamount_sat,
                last_success_reb: entry.last_success_reb.clone(),
                max_out_amount_sat: rebamount_sat
                    .saturating_add(fee_budget_sat)
                    .saturating_add(OPPOSITE_FEE_MARGIN_SAT),
            })
        })
        .collect()
}

fn write_rebalance_opposites(snapshot_path: &Path, opposites: &[RebalanceOppositeEntry]) {
    let Some(file_name) = snapshot_path.file_name().and_then(|name| name.to_str()) else {
        log::error!(
            "failed to derive opposites sidecar name from {}",
            snapshot_path.display()
        );
        return;
    };
    let sidecar_name = file_name.replacen("sling-stats-", "sling-opposites-", 1);
    let sidecar_path = snapshot_path.with_file_name(sidecar_name);
    match serde_json::to_string_pretty(opposites) {
        Ok(json) => match fs::write(&sidecar_path, json) {
            Ok(_) => log::info!(
                "saved rebalance opposites snapshot to {}",
                sidecar_path.display()
            ),
            Err(e) => log::error!(
                "failed to write rebalance opposites snapshot {}: {}",
                sidecar_path.display(),
                e
            ),
        },
        Err(e) => log::error!("failed to serialize rebalance opposites snapshot: {}", e),
    }
}

fn derive_rebalance_opposites(entries: &[SlingSnapshotEntry]) -> Vec<RebalanceOppositeEntry> {
    let targets = balanced_targets(entries);
    if targets.is_empty() {
        return Vec::new();
    }

    match derive_rebalance_opposites_from_listhtlcs(&targets) {
        Ok(opposites) => opposites,
        Err(e) => {
            log::error!("failed to derive rebalance opposite channels: {}", e);
            targets
                .into_iter()
                .map(|target| RebalanceOppositeEntry {
                    alias: target.alias,
                    target_scid: target.scid,
                    rebamount_sat: target.rebamount_sat,
                    last_success_reb: target.last_success_reb,
                    payment_hash: None,
                    payment_hash_candidates: Vec::new(),
                    incoming_htlc_state: None,
                    match_status: "not_found".to_string(),
                    opposite_scid: None,
                    candidate_opposite_scids: Vec::new(),
                })
                .collect()
        }
    }
}

fn derive_rebalance_opposites_from_listhtlcs(
    targets: &[BalancedRebalanceTarget],
) -> Result<Vec<RebalanceOppositeEntry>, String> {
    let mut command = if cfg!(debug_assertions) && Path::new("test-json/listhtlcs.json").exists() {
        let mut command = Command::new("cat");
        command.arg("test-json/listhtlcs.json");
        command
    } else {
        let mut command = Command::new(CMD);
        command.arg("listhtlcs");
        command
    };

    let mut child = command
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning listhtlcs returned {e:?}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture listhtlcs stdout".to_string())?;
    let opposites = derive_rebalance_opposites_from_reader(targets, stdout)?;
    let status = child
        .wait()
        .map_err(|e| format!("waiting for listhtlcs returned {e:?}"))?;
    if !status.success() {
        return Err(format!("listhtlcs exited with status {status}"));
    }
    Ok(opposites)
}

fn derive_rebalance_opposites_from_reader<R: Read>(
    targets: &[BalancedRebalanceTarget],
    reader: R,
) -> Result<Vec<RebalanceOppositeEntry>, String> {
    let target_by_scid: HashMap<&str, &BalancedRebalanceTarget> = targets
        .iter()
        .map(|target| (target.scid.as_str(), target))
        .collect();

    let mut incoming_by_scid: HashMap<String, Vec<HtlcRecord>> = HashMap::new();
    let mut outgoing_by_hash: HashMap<String, Vec<HtlcRecord>> = HashMap::new();

    stream_listhtlcs_from_reader(reader, |htlc| {
        let amount_sat = htlc.amount_msat / 1000;
        if let Some(target) = target_by_scid.get(htlc.short_channel_id.as_str()) {
            if htlc.direction == "in"
                && htlc.state == TARGET_IN_STATE
                && amount_sat == target.rebamount_sat
            {
                incoming_by_scid
                    .entry(htlc.short_channel_id.clone())
                    .or_default()
                    .push(htlc);
            }
            return;
        }

        if htlc.direction != "out" || htlc.state != OPPOSITE_OUT_STATE {
            return;
        }

        if targets.iter().any(|target| {
            amount_sat >= target.rebamount_sat && amount_sat <= target.max_out_amount_sat
        }) {
            outgoing_by_hash
                .entry(htlc.payment_hash.clone())
                .or_default()
                .push(htlc);
        }
    })?;

    Ok(targets
        .iter()
        .map(|target| {
            let incoming_candidates = incoming_by_scid.get(&target.scid);
            let mut payment_hash_candidates: Vec<String> = incoming_candidates
                .into_iter()
                .flat_map(|items| items.iter().map(|item| item.payment_hash.clone()))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            payment_hash_candidates.sort();

            let mut candidate_opposite_scids: Vec<String> = payment_hash_candidates
                .iter()
                .flat_map(|payment_hash| outgoing_by_hash.get(payment_hash).into_iter().flatten())
                .filter(|htlc| htlc.short_channel_id != target.scid)
                .map(|htlc| htlc.short_channel_id.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            candidate_opposite_scids.sort();

            let incoming_state = incoming_candidates
                .and_then(|items| items.first())
                .map(|item| item.state.clone());

            let (match_status, opposite_scid, payment_hash) = match (
                payment_hash_candidates.len(),
                candidate_opposite_scids.len(),
            ) {
                (0, _) => ("not_found".to_string(), None, None),
                (1, 1) => (
                    "matched".to_string(),
                    candidate_opposite_scids.first().cloned(),
                    payment_hash_candidates.first().cloned(),
                ),
                (_, 0) => (
                    "not_found".to_string(),
                    None,
                    payment_hash_candidates.first().cloned(),
                ),
                _ => ("ambiguous".to_string(), None, None),
            };

            RebalanceOppositeEntry {
                alias: target.alias.clone(),
                target_scid: target.scid.clone(),
                rebamount_sat: target.rebamount_sat,
                last_success_reb: target.last_success_reb.clone(),
                payment_hash,
                payment_hash_candidates,
                incoming_htlc_state: incoming_state,
                match_status,
                opposite_scid,
                candidate_opposite_scids,
            }
        })
        .collect())
}

fn stream_listhtlcs_from_reader<R: Read, F: FnMut(HtlcRecord)>(
    reader: R,
    mut handler: F,
) -> Result<(), String> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    ListhtlcsRootSeed {
        handler: &mut handler,
    }
    .deserialize(&mut deserializer)
    .map_err(|e| format!("streaming listhtlcs failed with {e:?}"))
}

struct ListhtlcsRootSeed<'a, F> {
    handler: &'a mut F,
}

impl<'de, 'a, F: FnMut(HtlcRecord)> DeserializeSeed<'de> for ListhtlcsRootSeed<'a, F> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ListhtlcsRootVisitor {
            handler: self.handler,
        })
    }
}

struct ListhtlcsRootVisitor<'a, F> {
    handler: &'a mut F,
}

impl<'de, 'a, F: FnMut(HtlcRecord)> Visitor<'de> for ListhtlcsRootVisitor<'a, F> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a listhtlcs response object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            if key == "htlcs" {
                map.next_value_seed(ListhtlcsArraySeed {
                    handler: self.handler,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(())
    }
}

struct ListhtlcsArraySeed<'a, F> {
    handler: &'a mut F,
}

impl<'de, 'a, F: FnMut(HtlcRecord)> DeserializeSeed<'de> for ListhtlcsArraySeed<'a, F> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ListhtlcsArrayVisitor {
            handler: self.handler,
        })
    }
}

struct ListhtlcsArrayVisitor<'a, F> {
    handler: &'a mut F,
}

impl<'de, 'a, F: FnMut(HtlcRecord)> Visitor<'de> for ListhtlcsArrayVisitor<'a, F> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of htlcs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(htlc) = seq.next_element::<HtlcRecord>()? {
            (self.handler)(htlc);
        }
        Ok(())
    }
}

/// We search empty channels and try to pull sats on them from a list of candidates that are ~full and cheap.
pub fn run_sling(store: &Store, directory: &str) {
    let channels = store.normal_channels();
    let recent_settled = store.filter_forwards_by_hours(LOOKBACK_HOURS);
    let recent_settled: Vec<_> = recent_settled
        .into_iter()
        .filter(|f| f.status == "settled")
        .filter_map(|f| SettledForward::try_from(f).ok())
        .collect();
    if let Some(snapshot) = snapshot_sling_stats(directory) {
        let opposites = derive_rebalance_opposites(&snapshot.entries);
        write_rebalance_opposites(&snapshot.file_path, &opposites);
    }
    log::info!(
        "Sling inputs: channels:{} recent_settled_{}h:{} target_balance<=:{:.0}% candidate_balance>=:{:.0}% min_amount:{}sat",
        channels.len(),
        LOOKBACK_HOURS,
        recent_settled.len(),
        MAX_BALANCE * 100.0,
        MIN_CANDIDATE_BALANCE * 100.0,
        MIN_AMOUNT_SAT
    );

    let candidates = compute_candidates(store, &channels);
    if candidates.is_empty() {
        log::info!("No suitable candidates found (ppm < {SOURCE_PPM_MAX} and balance > {MIN_CANDIDATE_BALANCE})");
        return;
    }

    let candidates_json = candidates_to_json(&candidates);
    log::info!("Using {} candidates: {candidates_json}", candidates.len());

    let execute_sling = std::env::var("EXECUTE_SLING").is_ok();
    if execute_sling {
        stop_existing_sling_jobs();
    }

    let mut skipped_balance = 0u64;
    let mut skipped_missing_scid = 0u64;
    let mut targets_without_local_channel_info = 0u64;
    let mut skipped_no_recent_outbound = 0u64;
    let mut skipped_zero_budget = 0u64;
    let mut skipped_small_amount = 0u64;
    let mut suggested = 0u64;

    for channel in channels {
        let balance = channel.perc_float();
        if balance > MAX_BALANCE {
            skipped_balance += 1;
            continue;
        }

        let Some(scid) = &channel.short_channel_id else {
            skipped_missing_scid += 1;
            continue;
        };

        let alias = store.get_node_alias(&channel.peer_id);
        let my_ppm = store
            .get_channel(scid, &store.info.id)
            .map(|our| our.fee_per_millionth);
        if my_ppm.is_none() {
            targets_without_local_channel_info += 1;
            log::info!(
                "missing local channel info for scid:{scid}, continuing with forward-driven rebalance logic"
            );
        }
        let my_ppm_log = my_ppm
            .map(|ppm| ppm.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let recent = recent_outbound_stats(&recent_settled, scid);
        let average_fee_ppm = recent.average_fee_ppm();
        let amount = recent.routed_sat;
        let max_ppm = compute_max_ppm(average_fee_ppm);

        if recent.count == 0 {
            skipped_no_recent_outbound += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}h:0 amount:0s avg_fee_ppm:0 channel_ppm:{} no_recent_outbound, skipping",
                balance * 100.0,
                LOOKBACK_HOURS,
                my_ppm_log,
            );
            continue;
        }

        if amount < MIN_AMOUNT_SAT {
            skipped_small_amount += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}h:{} amount:{}s avg_fee_ppm:{} channel_ppm:{} below_min_amount:{}s, skipping",
                balance * 100.0,
                LOOKBACK_HOURS,
                recent.count,
                amount,
                average_fee_ppm,
                my_ppm_log,
                MIN_AMOUNT_SAT,
            );
            continue;
        }

        if max_ppm == 0 {
            skipped_zero_budget += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}h:{} amount:{}s avg_fee_ppm:{} channel_ppm:{} maxppm:0, skipping",
                balance * 100.0,
                LOOKBACK_HOURS,
                recent.count,
                amount,
                average_fee_ppm,
                my_ppm_log,
            );
            continue;
        }

        suggested += 1;

        // Build arguments as a Vec to avoid shell quoting issues.
        // When calling a program directly (not via shell), we pass raw values
        // without shell-style quoting like single quotes around the JSON array.
        let candidates_arg = format!("candidates={candidates_json}");
        let args = [
            "sling-once",
            "-k",
            &format!("scid={scid}"),
            "direction=pull",
            &candidates_arg,
            &format!("maxppm={max_ppm}"),
            &format!("amount={amount}"),
            &format!("onceamount={amount}"),
        ];
        log::info!(
            "{alias} balance:{:.1}% recent_out_{}h:{} amount:{}s avg_fee_ppm:{} channel_ppm:{} maxppm:{} -> {CMD} {}",
            balance * 100.0,
            LOOKBACK_HOURS,
            recent.count,
            amount,
            average_fee_ppm,
            my_ppm_log,
            max_ppm,
            args.join(" ")
        );
        if execute_sling {
            log::info!("executing `{CMD} {}` {alias}", args.join(" "));

            let result = crate::cmd::cmd_result(CMD, &args);
            log::debug!("cmd return: {result}");
        }
    }

    log::info!(
        "Sling summary: suggested:{} skipped_balance:{} skipped_no_recent_outbound:{} skipped_small_amount:{} skipped_zero_budget:{} skipped_missing_scid:{} targets_without_local_channel_info:{}",
        suggested,
        skipped_balance,
        skipped_no_recent_outbound,
        skipped_small_amount,
        skipped_zero_budget,
        skipped_missing_scid,
        targets_without_local_channel_info
    );
}

#[cfg(test)]
mod tests {
    use super::{
        compute_max_ppm, derive_rebalance_opposites_from_reader, parse_rebamount_sat,
        BalancedRebalanceTarget,
    };

    #[test]
    fn compute_max_ppm_is_zero_without_recent_avg_ppm() {
        assert_eq!(compute_max_ppm(0), 0);
    }

    #[test]
    fn compute_max_ppm_uses_half_of_average_fee_ppm() {
        assert_eq!(compute_max_ppm(446), 223);
        assert_eq!(compute_max_ppm(2085), 1042);
        assert_eq!(compute_max_ppm(90), 45);
    }

    #[test]
    fn parse_rebamount_sat_handles_commas() {
        assert_eq!(parse_rebamount_sat("25,799"), Some(25_799));
        assert_eq!(parse_rebamount_sat("1000"), Some(1000));
        assert_eq!(parse_rebamount_sat("bad"), None);
    }

    #[test]
    fn derive_rebalance_opposite_channel_from_streamed_htlcs() {
        let targets = vec![BalancedRebalanceTarget {
            alias: "SLEEPYWHISPER".to_string(),
            scid: "866191x460x2".to_string(),
            rebamount_sat: 25_799,
            last_success_reb: "2026-04-16 07:23:22".to_string(),
            max_out_amount_sat: 25_900,
        }];
        let listhtlcs = r#"{
          "htlcs": [
            {
              "short_channel_id": "867798x3251x1",
              "amount_msat": 25825000,
              "direction": "out",
              "payment_hash": "61a3761dd11c3bb202a7cdf005d3a61988d06de23bbb5dc31be033e9c9f10b4d",
              "state": "RCVD_REMOVE_ACK_REVOCATION",
              "created_index": 11200965,
              "updated_index": 14953283
            },
            {
              "short_channel_id": "866191x460x2",
              "amount_msat": 25799000,
              "direction": "in",
              "payment_hash": "61a3761dd11c3bb202a7cdf005d3a61988d06de23bbb5dc31be033e9c9f10b4d",
              "state": "SENT_REMOVE_ACK_REVOCATION",
              "created_index": 11200966,
              "updated_index": 14953274
            }
          ]
        }"#;

        let opposites =
            derive_rebalance_opposites_from_reader(&targets, listhtlcs.as_bytes()).unwrap();

        assert_eq!(opposites.len(), 1);
        assert_eq!(opposites[0].match_status, "matched");
        assert_eq!(opposites[0].opposite_scid.as_deref(), Some("867798x3251x1"));
        assert_eq!(
            opposites[0].payment_hash.as_deref(),
            Some("61a3761dd11c3bb202a7cdf005d3a61988d06de23bbb5dc31be033e9c9f10b4d")
        );
    }

    #[test]
    fn derive_rebalance_opposite_channel_marks_ambiguous() {
        let targets = vec![BalancedRebalanceTarget {
            alias: "SLEEPYWHISPER".to_string(),
            scid: "866191x460x2".to_string(),
            rebamount_sat: 25_799,
            last_success_reb: "2026-04-16 07:23:22".to_string(),
            max_out_amount_sat: 25_900,
        }];
        let listhtlcs = r#"{
          "htlcs": [
            {
              "short_channel_id": "866191x460x2",
              "amount_msat": 25799000,
              "direction": "in",
              "payment_hash": "hash-a",
              "state": "SENT_REMOVE_ACK_REVOCATION",
              "created_index": 1
            },
            {
              "short_channel_id": "867798x3251x1",
              "amount_msat": 25825000,
              "direction": "out",
              "payment_hash": "hash-a",
              "state": "RCVD_REMOVE_ACK_REVOCATION",
              "created_index": 2
            },
            {
              "short_channel_id": "866191x460x2",
              "amount_msat": 25799000,
              "direction": "in",
              "payment_hash": "hash-b",
              "state": "SENT_REMOVE_ACK_REVOCATION",
              "created_index": 3
            },
            {
              "short_channel_id": "903672x2135x0",
              "amount_msat": 25800000,
              "direction": "out",
              "payment_hash": "hash-b",
              "state": "RCVD_REMOVE_ACK_REVOCATION",
              "created_index": 4
            }
          ]
        }"#;

        let opposites =
            derive_rebalance_opposites_from_reader(&targets, listhtlcs.as_bytes()).unwrap();

        assert_eq!(opposites.len(), 1);
        assert_eq!(opposites[0].match_status, "ambiguous");
        assert!(opposites[0].opposite_scid.is_none());
        assert_eq!(opposites[0].candidate_opposite_scids.len(), 2);
    }
}
