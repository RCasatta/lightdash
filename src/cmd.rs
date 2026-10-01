use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::fs::{self, File};
use std::io;
use std::path::PathBuf;
use std::process::{Command, Output as ProcessOutput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use crate::error_panic;

static SSH_DESTINATION: OnceLock<String> = OnceLock::new();
static TEST_DATA: AtomicBool = AtomicBool::new(false);
const DEFAULT_LOCAL_AVAILDB_PATH: &str = ".lightning/bitcoin/summars/availdb.json";
const TEST_AVAILDB_PATH: &str = "test-json/availdb.json";
pub const GETROUTES_LAYERS: [&str; 3] = ["auto.localchans", "auto.sourcefree", "xpay"];

pub fn configure_ssh(destination: Option<String>) -> Result<(), String> {
    let Some(destination) = destination else {
        return Ok(());
    };
    if destination.is_empty() {
        return Err("SSH destination cannot be empty".to_string());
    }
    if destination.starts_with('-') || destination.chars().any(char::is_whitespace) {
        return Err(format!("invalid SSH destination `{destination}`"));
    }

    SSH_DESTINATION
        .set(destination)
        .map_err(|_| "SSH destination was already configured".to_string())
}

/// Read bundled `test-json/` fixtures instead of querying a node, and skip datastore writes.
pub fn enable_test_data() {
    TEST_DATA.store(true, Ordering::Relaxed);
}

pub fn using_test_data() -> bool {
    TEST_DATA.load(Ordering::Relaxed)
}

pub(crate) fn using_ssh() -> bool {
    SSH_DESTINATION.get().is_some()
}

pub(crate) fn remote_command_output(cmd: &str, args: &[&str]) -> Result<Vec<u8>, String> {
    let destination = SSH_DESTINATION
        .get()
        .ok_or_else(|| "remote command requested without --ssh".to_string())?;
    let (description, result) = execute_ssh_command(destination, cmd, args);
    let output = result.map_err(|e| format!("executing `{description}` failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "`{description}` exited with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

pub fn read_availdb_json(path: Option<&str>) -> Result<Value, String> {
    let configured_path = path
        .map(str::to_string)
        .or_else(|| std::env::var("AVAILDB_PATH").ok());

    if let Some(destination) = SSH_DESTINATION.get() {
        let remote_path = configured_path
            .as_deref()
            .map(normalize_remote_home_path)
            .unwrap_or(DEFAULT_LOCAL_AVAILDB_PATH);
        return read_remote_json_file(destination, remote_path);
    }

    let local_path = if let Some(path) = configured_path {
        expand_local_home_path(&path)?
    } else if using_test_data() {
        PathBuf::from(TEST_AVAILDB_PATH)
    } else {
        let home = std::env::var_os("HOME")
            .ok_or_else(|| "HOME is not set; pass --availdb explicitly".to_string())?;
        PathBuf::from(home).join(DEFAULT_LOCAL_AVAILDB_PATH)
    };

    let content = fs::read_to_string(&local_path)
        .map_err(|e| format!("reading availdb `{}` failed: {e}", local_path.display()))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("parsing availdb `{}` failed: {e}", local_path.display()))
}

/// Where a node query reads its data in `--test-data` mode.
enum Fixture {
    Gz(&'static str),
    Xz(&'static str),
    Plain(&'static str),
}

/// Runs a read-only `lightning-cli` query, or reads its fixture in `--test-data` mode, and
/// parses the response. Errors name the RPC so a failed collection is easy to diagnose.
fn query_node<T: DeserializeOwned>(fixture: Fixture, rpc: &str) -> Result<T, String> {
    let value = if using_test_data() {
        match fixture {
            Fixture::Gz(path) => read_gz_json(path)?,
            Fixture::Xz(path) => cmd_result_fallible("xzcat", &[path])?,
            Fixture::Plain(path) => {
                let content = fs::read_to_string(path)
                    .map_err(|e| format!("reading fixture `{path}` failed: {e}"))?;
                serde_json::from_str(&content)
                    .map_err(|e| format!("parsing fixture `{path}` failed: {e}"))?
            }
        }
    } else {
        cmd_result_fallible("lightning-cli", &[rpc])?
    };
    if let (Some(code), Some(message)) = (value.get("code"), value.get("message")) {
        return Err(format!(
            "`lightning-cli {rpc}` returned error {code}: {message}"
        ));
    }
    serde_json::from_value(value).map_err(|e| format!("parsing `{rpc}` response failed: {e}"))
}

pub fn list_funds() -> Result<ListFunds, String> {
    query_node(Fixture::Gz("test-json/listfunds.gz"), "listfunds")
}

pub fn list_nodes() -> Result<ListNodes, String> {
    query_node(Fixture::Gz("test-json/listnodes.gz"), "listnodes")
}

pub fn list_channels() -> Result<ListChannels, String> {
    query_node(Fixture::Gz("test-json/listchannels.gz"), "listchannels")
}

pub fn list_peers() -> Result<ListPeers, String> {
    query_node(Fixture::Gz("test-json/listpeers.gz"), "listpeers")
}

pub fn list_peer_channels() -> Result<ListPeerChannels, String> {
    query_node(
        Fixture::Xz("test-json/listpeerchannels.xz"),
        "listpeerchannels",
    )
}

pub fn list_forwards() -> Result<ListForwards, String> {
    query_node(Fixture::Xz("test-json/listforwards.xz"), "listforwards")
}

pub fn list_closed_channels() -> Result<ListClosedChannels, String> {
    query_node(
        Fixture::Gz("test-json/listclosedchannels.gz"),
        "listclosedchannels",
    )
}

pub fn bkpr_list_account_events() -> Result<BkprListAccountEvents, String> {
    query_node(
        Fixture::Gz("test-json/bkpr-listaccountevents.gz"),
        "bkpr-listaccountevents",
    )
}

pub fn bkpr_list_income() -> Result<BkprListIncome, String> {
    query_node(
        Fixture::Plain("test-json/bkpr-listincome"),
        "bkpr-listincome",
    )
}

pub fn get_info() -> Result<GetInfo, String> {
    query_node(Fixture::Plain("test-json/getinfo"), "getinfo")
}

pub fn get_routes(
    source: &str,
    destination: &str,
    amount_msat: u64,
    max_fee_msat: u64,
) -> Result<GetRoutesOutcome, String> {
    let v = if using_test_data() {
        Ok(cmd_result("cat", &["test-json/getroutes"]))
    } else {
        let amount_msat = format!("{amount_msat}msat");
        let max_fee_msat = format!("{max_fee_msat}msat");
        let layers = serde_json::to_string(&GETROUTES_LAYERS).expect("route layers serialize");
        cmd_result_fallible(
            "lightning-cli",
            &[
                "getroutes",
                source,
                destination,
                &amount_msat,
                &layers,
                &max_fee_msat,
                "9",
                "2016",
                "1",
            ],
        )
    }?;
    Ok(parse_get_routes_outcome(v))
}

fn parse_get_routes_outcome(v: Value) -> GetRoutesOutcome {
    if let Ok(routes) = serde_json::from_value(v.clone()) {
        return GetRoutesOutcome::Found(routes);
    }
    let message = v.get("message").and_then(Value::as_str).unwrap_or_default();
    if message.contains("timed out") || message.contains("deadline") {
        GetRoutesOutcome::TimedOut
    } else {
        GetRoutesOutcome::NotFound
    }
}

pub fn cmd_result(cmd: &str, args: &[impl AsRef<str>]) -> Value {
    let args: Vec<&str> = args.iter().map(|s| s.as_ref()).collect();
    let (description, result) = execute_command(cmd, &args);
    let data = match result {
        Ok(data) => data,
        Err(e) => {
            error_panic!("executing `{description}` returned {e:?}");
        }
    };
    let s = std::str::from_utf8(&data.stdout).unwrap();
    match serde_json::from_str(s) {
        Ok(v) => v,
        Err(e) => {
            let stderr = std::str::from_utf8(&data.stderr).unwrap_or("<stderr is not utf8>");
            error_panic!(
                "executing `{description}` exited with status {} and stdout `{s}` stderr `{stderr}`; parsing json returned {e:?}",
                data.status
            );
        }
    }
}

fn execute_command(cmd: &str, args: &[&str]) -> (String, io::Result<ProcessOutput>) {
    if cmd == "lightning-cli" {
        let args = lightning_cli_json_args(args);
        if let Some(destination) = SSH_DESTINATION.get() {
            return execute_ssh_command(destination, cmd, &args);
        }

        let description = format!("{cmd} {}", args.join(" "));
        let result = Command::new(cmd).args(args).output();
        return (description, result);
    }

    let description = format!("{cmd} {}", args.join(" "));
    let result = Command::new(cmd).args(args).output();
    (description, result)
}

fn cmd_result_fallible(cmd: &str, args: &[impl AsRef<str>]) -> Result<Value, String> {
    let args: Vec<&str> = args.iter().map(|arg| arg.as_ref()).collect();
    let (description, result) = execute_command(cmd, &args);
    let data = result.map_err(|error| format!("executing `{description}` failed: {error}"))?;
    let stdout = std::str::from_utf8(&data.stdout)
        .map_err(|error| format!("`{description}` returned non-UTF-8 output: {error}"))?;
    serde_json::from_str(stdout).map_err(|error| {
        let stderr = std::str::from_utf8(&data.stderr).unwrap_or("<stderr is not utf8>");
        format!(
            "executing `{description}` exited with status {} and stderr `{stderr}`; parsing JSON returned {error}",
            data.status
        )
    })
}

fn lightning_cli_json_args<'a>(args: &[&'a str]) -> Vec<&'a str> {
    let mut json_args = Vec::with_capacity(args.len() + 2);
    json_args.extend(["--json", "--notifications=none"]);
    json_args.extend_from_slice(args);
    json_args
}

fn execute_ssh_command(
    destination: &str,
    cmd: &str,
    args: &[&str],
) -> (String, io::Result<ProcessOutput>) {
    let remote_command = build_remote_command(cmd, args);
    let description = format!("ssh -C {destination} {remote_command}");
    let result = Command::new("ssh")
        .arg("-C")
        .arg(destination)
        .arg(&remote_command)
        .output();
    (description, result)
}

fn read_remote_json_file(destination: &str, path: &str) -> Result<Value, String> {
    let (description, result) = execute_ssh_command(destination, "cat", &["--", path]);
    let data = result.map_err(|e| format!("executing `{description}` failed: {e}"))?;
    let stdout = std::str::from_utf8(&data.stdout)
        .map_err(|e| format!("`{description}` returned non-UTF-8 output: {e}"))?;
    if !data.status.success() {
        let stderr = std::str::from_utf8(&data.stderr).unwrap_or("<stderr is not utf8>");
        return Err(format!(
            "`{description}` exited with status {}: {stderr}",
            data.status
        ));
    }

    serde_json::from_str(stdout).map_err(|e| format!("parsing `{description}` output failed: {e}"))
}

fn expand_local_home_path(path: &str) -> Result<PathBuf, String> {
    let Some(relative_path) = path.strip_prefix("~/") else {
        return Ok(PathBuf::from(path));
    };
    let home = std::env::var_os("HOME")
        .ok_or_else(|| "HOME is not set; use an absolute path".to_string())?;
    Ok(PathBuf::from(home).join(relative_path))
}

fn normalize_remote_home_path(path: &str) -> &str {
    path.strip_prefix("~/").unwrap_or(path)
}

fn build_remote_command(cmd: &str, args: &[&str]) -> String {
    std::iter::once(cmd)
        .chain(args.iter().copied())
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(&byte))
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

fn read_gz_json(path: &str) -> Result<Value, String> {
    let file = File::open(path).map_err(|e| format!("opening fixture `{path}` failed: {e}"))?;
    serde_json::from_reader(GzDecoder::new(file))
        .map_err(|e| format!("parsing gzip fixture `{path}` failed: {e}"))
}

// fn lcli_named(subcmd: &str, args: &[&str]) -> String {
//     let data = std::process::Command::new("lightning-cli")
//         .arg(subcmd)
//         .arg("-k")
//         .args(args)
//         .output()
//         .unwrap();
//     std::str::from_utf8(&data.stdout).unwrap().to_string()
// }

#[derive(Deserialize, Debug)]
pub struct GetInfo {
    pub id: String,
    pub blockheight: u64,
}

#[derive(Deserialize, Debug)]
pub struct ListChannels {
    pub channels: Vec<Channel>,
}

#[derive(Deserialize, Debug)]
pub struct ListPeers {
    pub peers: Vec<Peer>,
}

#[derive(Deserialize, Debug)]
pub struct ListPeerChannels {
    pub channels: Vec<ListPeerChannelsChannel>,
}

#[derive(Deserialize, Debug)]
pub struct ListPeerChannelsChannel {
    pub state: String,
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub short_channel_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub private: Option<bool>,
    #[serde(default)]
    pub updates: Option<PeerChannelUpdates>,
    #[serde(default)]
    pub fee_base_msat: Option<u64>,
    #[serde(default)]
    pub fee_proportional_millionths: Option<u64>,
    #[serde(default)]
    pub to_us_msat: u64,
    #[serde(default)]
    pub spendable_msat: u64,
    #[serde(default)]
    pub our_reserve_msat: Option<u64>,
    #[serde(default)]
    pub minimum_htlc_out_msat: u64,
    #[serde(default)]
    pub maximum_htlc_out_msat: u64,
    #[serde(default)]
    pub htlcs: Vec<PeerChannelHtlc>,
    #[serde(default)]
    pub status: Vec<String>,
}

/// An HTLC currently in flight on a channel.
#[derive(Deserialize, Debug)]
pub struct PeerChannelHtlc {
    /// `out` for HTLCs we offered, `in` for HTLCs the peer offered.
    pub direction: String,
    pub amount_msat: u64,
}

#[derive(Deserialize, Debug)]
pub struct PeerChannelUpdates {
    #[serde(default)]
    pub local: Option<PeerChannelUpdate>,
    #[serde(default)]
    pub remote: Option<PeerChannelUpdate>,
}

#[derive(Deserialize, Debug)]
pub struct PeerChannelUpdate {
    pub htlc_minimum_msat: u64,
    pub htlc_maximum_msat: u64,
    pub cltv_expiry_delta: u64,
    pub fee_base_msat: u64,
    pub fee_proportional_millionths: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub num_channels: u64,
    #[serde(default)]
    pub features: Option<String>,
}

impl Peer {
    /// Whether the peer's INIT features advertise `option_support_large_channel` (bits 18/19).
    /// Unknown features count as unsupported.
    pub fn supports_large_channels(&self) -> bool {
        self.features.as_deref().is_some_and(|features| {
            feature_bit_is_set(features, 18) || feature_bit_is_set(features, 19)
        })
    }
}

/// Reads BOLT 9 feature `bit` from a hex feature bitmap, counting from the rightmost byte.
pub(crate) fn feature_bit_is_set(features: &str, bit: usize) -> bool {
    if features.len() % 2 != 0 {
        return false;
    }

    let byte_from_end = bit / 8;
    let Some(start) = features.len().checked_sub((byte_from_end + 1) * 2) else {
        return false;
    };
    let Ok(byte) = u8::from_str_radix(&features[start..start + 2], 16) else {
        return false;
    };

    byte & (1 << (bit % 8)) != 0
}

#[derive(Deserialize, Debug, Clone)]
pub struct Channel {
    pub source: String,
    pub destination: String,
    pub short_channel_id: String,
    pub amount_msat: u64,
    pub last_update: u64,
    pub base_fee_millisatoshi: u64,
    pub fee_per_millionth: u64,
    pub delay: u64,
    pub htlc_minimum_msat: u64,
    pub htlc_maximum_msat: u64,
    #[serde(default)]
    pub active: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct ListNodes {
    pub nodes: Vec<Node>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Node {
    pub nodeid: String,
    pub alias: Option<String>,
    #[serde(default)]
    pub addresses: Vec<Value>,
}

#[derive(Deserialize, Debug)]
pub struct ListFunds {
    pub channels: Vec<Fund>,
    pub outputs: Vec<Output>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Fund {
    pub peer_id: String,
    pub connected: bool,
    pub state: String,
    pub channel_id: String,
    pub short_channel_id: Option<String>,
    pub our_amount_msat: u64,
    pub amount_msat: u64,
    pub funding_txid: String,
    pub funding_output: u32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Output {
    pub amount_msat: u64,
}

impl Fund {
    pub fn perc_float(&self) -> f64 {
        (self.our_amount_msat as f64 / self.amount_msat as f64).clamp(0.0, 1.0)
    }

    pub fn short_channel_id(&self) -> String {
        self.short_channel_id.clone().unwrap_or("".to_string())
    }
}

#[derive(Deserialize)]
pub struct ListForwards {
    pub forwards: Vec<Forward>,
}

#[derive(Deserialize, Debug)]
pub struct ListClosedChannels {
    pub closedchannels: Vec<ClosedChannel>,
}

#[derive(Deserialize, Debug)]
pub struct BkprListAccountEvents {
    pub events: Vec<BkprAccountEvent>,
}

#[derive(Deserialize, Debug)]
pub struct BkprListIncome {
    pub income_events: Vec<BkprIncomeEvent>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BkprIncomeEvent {
    pub account: String,
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub credit_msat: u64,
    #[serde(default)]
    pub debit_msat: u64,
    pub timestamp: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BkprAccountEvent {
    pub account: String,
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub credit_msat: u64,
    #[serde(default)]
    pub debit_msat: u64,
    #[serde(default)]
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub payment_id: Option<String>,
    #[serde(default)]
    pub fees_msat: Option<u64>,
    #[serde(default)]
    pub is_rebalance: bool,
    #[serde(default)]
    pub part_id: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClosedChannel {
    #[serde(default)]
    pub channel_id: String,
    #[serde(default)]
    pub peer_id: Option<String>,
    #[serde(default)]
    pub short_channel_id: Option<String>,
    pub opener: String,
    #[serde(default)]
    pub closer: Option<String>,
    #[serde(default)]
    pub total_htlcs_sent: Option<u64>,
    #[serde(default)]
    pub total_msat: u64,
    pub funding_txid: String,
    pub final_to_us_msat: u64,
    #[serde(default)]
    pub last_commitment_txid: Option<String>,
    #[serde(default)]
    pub last_stable_connection: Option<u64>,
    pub close_cause: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Forward {
    pub in_channel: String,
    pub out_channel: Option<String>,
    pub fee_msat: Option<u64>,
    pub in_msat: u64,
    pub out_msat: Option<u64>,
    pub status: String,
    pub received_time: f64,
    pub resolved_time: Option<f64>,
    #[serde(default)]
    pub failreason: Option<String>,
    #[serde(default)]
    pub failcode: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct SettledForward {
    pub in_channel: String,
    pub out_channel: String,
    pub fee_msat: u64,
    pub out_msat: u64,
    pub fee_sat: u64,
    pub out_sat: u64,
    pub resolved_time: DateTime<Utc>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RouteNode {
    #[serde(default)]
    pub node_id_out: Option<String>,
    #[serde(default)]
    pub next_node_id: Option<String>,
}

impl RouteNode {
    pub fn outgoing_node_id(&self) -> Option<&str> {
        self.node_id_out.as_deref().or(self.next_node_id.as_deref())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct GetRoutesRoute {
    pub path: Vec<RouteNode>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GetRoutes {
    pub routes: Vec<GetRoutesRoute>,
}

pub enum GetRoutesOutcome {
    Found(GetRoutes),
    TimedOut,
    NotFound,
}

impl TryFrom<Forward> for SettledForward {
    type Error = ();

    fn try_from(value: Forward) -> Result<Self, Self::Error> {
        let fee_msat = value.fee_msat.ok_or(())?;
        let out_msat = value.out_msat.ok_or(())?;

        Ok(Self {
            in_channel: value.in_channel,
            out_channel: value.out_channel.ok_or(())?,
            fee_msat,
            out_msat,
            fee_sat: fee_msat / 1000,
            out_sat: out_msat / 1000,
            resolved_time: DateTime::from_timestamp(value.resolved_time.ok_or(())? as i64, 0)
                .ok_or(())?,
        })
    }
}

// Datastore API methods
/// Store data in the datastore with a given key and string value.
/// This version correctly formats BOTH the key and the string value as JSON.
pub fn datastore_string(
    key: &[&str],
    value: &str,
    mode: DatastoreMode,
) -> Result<DatastoreResponse, String> {
    // In debug mode, skip datastore operations
    if using_test_data() {
        log::debug!("Debug mode: Skipping datastore_string for key {:?}", key);
        return Ok(DatastoreResponse {
            key: key.iter().map(|s| s.to_string()).collect(),
            string: Some(value.to_string()),
        });
    }

    // 1. JSON-encode the key array.
    //    Example: &["lightdash", "last_run"] -> "[\"lightdash\",\"last_run\"]"
    let key_json = serde_json::to_string(key)
        .map_err(|e| format!("Failed to serialize key to JSON: {}", e))?;

    // 2. JSON-encode the string value. THIS IS THE CRITICAL FIX.
    //    Example: "1760287752" -> "\"1760287752\"" (Note the added quotes)
    let value_json = serde_json::to_string(value)
        .map_err(|e| format!("Failed to serialize value to JSON: {}", e))?;

    let args: Vec<String> = vec![
        "datastore".to_string(),
        "-k".to_string(),
        // Correct format: key=["lightdash","last_run"]
        format!("key={}", key_json),
        // Correct format: string="1760287752"
        format!("string={}", value_json), // <-- This line is now correct
        format!("mode={}", mode.as_str()),
    ];

    log::debug!("Executing lightning-cli with args: {:?}", args);
    let response_value = cmd_result("lightning-cli", &args);
    log::debug!("Received response: {:?}", response_value);

    // It's also good practice to check for an error response before parsing
    if response_value.get("code").is_some() {
        return Err(format!(
            "lightning-cli returned an error: {:?}",
            response_value
        ));
    }

    serde_json::from_value(response_value)
        .map_err(|e| format!("Failed to parse successful response JSON: {} ", e,))
}

/// List/retrieve data from the datastore, optionally filtered by key
pub fn listdatastore(key: Option<&[&str]>) -> Result<ListDatastore, String> {
    // In debug mode, return empty datastore
    if using_test_data() {
        log::debug!("Debug mode: Skipping listdatastore for key {:?}", key);
        return Ok(ListDatastore { datastore: vec![] });
    }

    let v = if let Some(k) = key {
        let key_json = serde_json::to_string(k).map_err(|e| e.to_string())?;
        cmd_result("lightning-cli", &["listdatastore", &key_json])
    } else {
        cmd_result("lightning-cli", &["listdatastore"])
    };

    serde_json::from_value(v).map_err(|e| format!("Failed to parse response: {}", e))
}

/// Delete data from the datastore
pub fn _deldatastore(key: &[&str]) -> Result<DatastoreResponse, String> {
    // In debug mode, skip datastore operations
    if using_test_data() {
        log::debug!("Debug mode: Skipping _deldatastore for key {:?}", key);
        return Ok(DatastoreResponse {
            key: key.iter().map(|s| s.to_string()).collect(),
            string: None,
        });
    }

    let key_json = serde_json::to_string(key).map_err(|e| e.to_string())?;
    let args = vec!["deldatastore", "-k", "key", &key_json];

    let v = cmd_result("lightning-cli", &args);
    serde_json::from_value(v).map_err(|e| format!("Failed to parse response: {}", e))
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum DatastoreMode {
    MustCreate,
    MustReplace,
    CreateOrReplace,
    MustAppend,
    CreateOrAppend,
}

impl DatastoreMode {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            DatastoreMode::MustCreate => "must-create",
            DatastoreMode::MustReplace => "must-replace",
            DatastoreMode::CreateOrReplace => "create-or-replace",
            DatastoreMode::MustAppend => "must-append",
            DatastoreMode::CreateOrAppend => "create-or-append",
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatastoreResponse {
    pub key: Vec<String>,
    #[serde(default)]
    pub string: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ListDatastore {
    pub datastore: Vec<DatastoreResponse>,
}

#[cfg(test)]
mod command_tests {
    use super::{
        build_remote_command, feature_bit_is_set, lightning_cli_json_args,
        normalize_remote_home_path, parse_get_routes_outcome, shell_quote, GetRoutes,
        GetRoutesOutcome, ListPeerChannels, Peer,
    };

    #[test]
    fn large_channel_support_reads_feature_bits_18_and_19() {
        let peer = |features: Option<&str>| Peer {
            id: "peer".to_string(),
            num_channels: 1,
            features: features.map(str::to_string),
        };
        assert!(peer(Some("040000")).supports_large_channels());
        assert!(peer(Some("080000")).supports_large_channels());
        assert!(!peer(Some("020000")).supports_large_channels());
        assert!(!peer(None).supports_large_channels());
    }

    #[test]
    fn bolt_feature_bits_are_read_from_the_rightmost_byte() {
        assert!(feature_bit_is_set("01", 0));
        assert!(feature_bit_is_set("4000000000000000", 62));
        assert!(feature_bit_is_set("8000000000000000", 63));
        assert!(!feature_bit_is_set("0000000000000000", 62));
        assert!(!feature_bit_is_set("01", 62));
        assert!(!feature_bit_is_set("not-hex", 0));
    }

    #[test]
    fn lightning_cli_output_is_json_without_notifications() {
        assert_eq!(
            lightning_cli_json_args(&["getroutes", "source", "destination"]),
            [
                "--json",
                "--notifications=none",
                "getroutes",
                "source",
                "destination"
            ]
        );
    }

    #[test]
    fn getroutes_deadline_error_is_reported_as_timeout() {
        let outcome = parse_get_routes_outcome(serde_json::json!({
            "code": 210,
            "message": "single_path_routes: timed out after deadline"
        }));
        assert!(matches!(outcome, GetRoutesOutcome::TimedOut));
    }

    #[test]
    fn remote_lightning_cli_command_is_shell_quoted() {
        assert_eq!(
            build_remote_command(
                "lightning-cli",
                &["signmessage", "hello world", "apostrophe's"]
            ),
            "lightning-cli signmessage 'hello world' 'apostrophe'\\''s'"
        );
    }

    #[test]
    fn safe_remote_arguments_remain_readable() {
        assert_eq!(shell_quote("getinfo"), "getinfo");
        assert_eq!(shell_quote("id=123x4x5"), "id=123x4x5");
    }

    #[test]
    fn remote_home_paths_are_relative_to_the_ssh_login_directory() {
        assert_eq!(
            normalize_remote_home_path("~/.lightning/bitcoin/summars/availdb.json"),
            ".lightning/bitcoin/summars/availdb.json"
        );
        assert_eq!(
            normalize_remote_home_path("/srv/availdb.json"),
            "/srv/availdb.json"
        );
    }

    #[test]
    fn listpeerchannels_deserializes_private_channel_policies() {
        let response: ListPeerChannels = serde_json::from_str(
            r#"{
                "channels": [{
                    "state": "CHANNELD_NORMAL",
                    "channel_id": "channel-id",
                    "short_channel_id": "1x2x3",
                    "private": true,
                    "updates": {
                        "local": {
                            "htlc_minimum_msat": 1000,
                            "htlc_maximum_msat": 2000000,
                            "cltv_expiry_delta": 34,
                            "fee_base_msat": 1000,
                            "fee_proportional_millionths": 823
                        },
                        "remote": {
                            "htlc_minimum_msat": 0,
                            "htlc_maximum_msat": 1900000,
                            "cltv_expiry_delta": 72,
                            "fee_base_msat": 100000,
                            "fee_proportional_millionths": 0
                        }
                    }
                }]
            }"#,
        )
        .unwrap();

        let channel = &response.channels[0];
        assert_eq!(channel.private, Some(true));
        let updates = channel.updates.as_ref().unwrap();
        assert_eq!(
            updates.local.as_ref().unwrap().fee_proportional_millionths,
            823
        );
        assert_eq!(
            updates.remote.as_ref().unwrap().fee_proportional_millionths,
            0
        );
    }

    #[test]
    fn getroutes_accepts_current_and_deprecated_outgoing_node_fields() {
        let response: GetRoutes = serde_json::from_str(
            r#"{
                "routes": [{
                    "path": [{
                        "node_id_out": "current",
                        "next_node_id": "deprecated"
                    }]
                }]
            }"#,
        )
        .unwrap();

        assert_eq!(
            response.routes[0].path[0].outgoing_node_id(),
            Some("current")
        );
    }
}

#[cfg(all(test, feature = "large-fixture-tests"))]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn gz_bkpr_fixture_matches_confirmed_rebalance_totals() {
        enable_test_data();
        let events = bkpr_list_account_events().unwrap();
        let rebalance_events: Vec<_> = events
            .events
            .iter()
            .filter(|event| event.is_rebalance)
            .collect();
        let payments: HashSet<_> = rebalance_events
            .iter()
            .filter_map(|event| event.payment_id.as_deref())
            .collect();
        let fees_msat: u64 = rebalance_events
            .iter()
            .map(|event| event.fees_msat.unwrap_or(0))
            .sum();
        let net_debit_msat: i64 = rebalance_events
            .iter()
            .map(|event| event.debit_msat as i64 - event.credit_msat as i64)
            .sum();

        assert_eq!(rebalance_events.len(), 3186);
        assert_eq!(payments.len(), 1593);
        assert_eq!(fees_msat, 7_598_600);
        assert_eq!(net_debit_msat, 7_598_600);
    }
}
