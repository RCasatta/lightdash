use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::PathBuf;
use std::process::{Command, Output as ProcessOutput, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use crate::error_panic;

static SSH_DESTINATION: OnceLock<String> = OnceLock::new();
const DEFAULT_LOCAL_AVAILDB_PATH: &str = ".lightning/bitcoin/summars/availdb.json";
const TEST_AVAILDB_PATH: &str = "test-json/availdb.json";
pub const GETROUTES_TIMEOUT_SECONDS: u64 = 12;

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

pub fn using_test_data() -> bool {
    cfg!(debug_assertions) && SSH_DESTINATION.get().is_none()
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

pub fn list_funds() -> ListFunds {
    let v = if using_test_data() {
        gz_json_file("test-json/listfunds.gz")
    } else {
        cmd_result("lightning-cli", &["listfunds"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn list_nodes() -> ListNodes {
    let v = if using_test_data() {
        gz_json_file("test-json/listnodes.gz")
    } else {
        cmd_result("lightning-cli", &["listnodes"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn list_channels() -> ListChannels {
    let v = if using_test_data() {
        gz_json_file("test-json/listchannels.gz")
    } else {
        cmd_result("lightning-cli", &["listchannels"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn read_xz_channels(path: &str) -> ListChannels {
    let v = cmd_result("xzcat", &[path]);
    serde_json::from_value(v).unwrap()
}

pub fn read_xz_funds(path: &str) -> ListFunds {
    let v = cmd_result("xzcat", &[path]);
    serde_json::from_value(v).unwrap()
}

pub fn list_peers() -> ListPeers {
    let v = if using_test_data() {
        gz_json_file("test-json/listpeers.gz")
    } else {
        cmd_result("lightning-cli", &["listpeers"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn list_peer_channels() -> ListPeerChannels {
    let v = if using_test_data() {
        cmd_result("xzcat", &["test-json/listpeerchannels.xz"])
    } else {
        cmd_result("lightning-cli", &["listpeerchannels"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn list_forwards() -> ListForwards {
    let v = if using_test_data() {
        cmd_result("xzcat", &["test-json/listforwards.xz"])
    } else {
        cmd_result("lightning-cli", &["listforwards"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn list_closed_channels() -> ListClosedChannels {
    let v = if using_test_data() {
        gz_json_file("test-json/listclosedchannels.gz")
    } else {
        cmd_result("lightning-cli", &["listclosedchannels"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn bkpr_list_account_events() -> BkprListAccountEvents {
    let v = if using_test_data() {
        gz_json_file("test-json/bkpr-listaccountevents.gz")
    } else {
        cmd_result("lightning-cli", &["bkpr-listaccountevents"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn bkpr_list_income() -> BkprListIncome {
    let v = if using_test_data() {
        cmd_result("cat", &["test-json/bkpr-listincome"])
    } else {
        cmd_result("lightning-cli", &["bkpr-listincome"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn get_info() -> GetInfo {
    let v = if using_test_data() {
        cmd_result("cat", &["test-json/getinfo"])
    } else {
        cmd_result("lightning-cli", &["getinfo"])
    };
    serde_json::from_value(v).unwrap()
}

pub fn get_routes(
    source: &str,
    destination: &str,
    amount_msat: u64,
    max_fee_msat: u64,
) -> Result<Option<GetRoutes>, String> {
    let v = if using_test_data() {
        Ok(cmd_result("cat", &["test-json/getroutes"]))
    } else {
        let amount_msat = format!("{amount_msat}msat");
        let max_fee_msat = format!("{max_fee_msat}msat");
        cmd_result_with_timeout(
            "lightning-cli",
            &[
                "getroutes",
                source,
                destination,
                &amount_msat,
                r#"["auto.localchans","auto.sourcefree"]"#,
                &max_fee_msat,
                "9",
                "2016",
                "1",
            ],
            Duration::from_secs(GETROUTES_TIMEOUT_SECONDS),
        )
    }?;
    Ok(serde_json::from_value(v).ok())
}

/// Sign a message with the node's key for authentication purposes
pub fn signmessage(message: &str) -> String {
    let v = cmd_result("lightning-cli", &["signmessage", message]);
    serde_json::from_value::<SignMessageResponse>(v)
        .unwrap()
        .zbase
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

fn cmd_result_with_timeout(
    cmd: &str,
    args: &[impl AsRef<str>],
    timeout: Duration,
) -> Result<Value, String> {
    let args: Vec<&str> = args.iter().map(|arg| arg.as_ref()).collect();
    let (description, result) = execute_command_with_timeout(cmd, &args, timeout);
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

fn execute_command_with_timeout(
    cmd: &str,
    args: &[&str],
    timeout: Duration,
) -> (String, io::Result<ProcessOutput>) {
    let (description, mut command) = configured_command(cmd, args);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let result = run_command_with_timeout(command, timeout);
    (description, result)
}

fn configured_command(cmd: &str, args: &[&str]) -> (String, Command) {
    if cmd == "lightning-cli" {
        let args = lightning_cli_json_args(args);
        if let Some(destination) = SSH_DESTINATION.get() {
            let remote_command = build_remote_command(cmd, &args);
            let description = format!("ssh -C {destination} {remote_command}");
            let mut command = Command::new("ssh");
            command.arg("-C").arg(destination).arg(&remote_command);
            return (description, command);
        }

        let description = format!("{cmd} {}", args.join(" "));
        let mut command = Command::new(cmd);
        command.args(args);
        return (description, command);
    }

    let description = format!("{cmd} {}", args.join(" "));
    let mut command = Command::new(cmd);
    command.args(args);
    (description, command)
}

fn run_command_with_timeout(mut command: Command, timeout: Duration) -> io::Result<ProcessOutput> {
    let mut child = command.spawn()?;
    let mut child_stdout = child.stdout.take().expect("piped stdout exists");
    let mut child_stderr = child.stderr.take().expect("piped stderr exists");
    let started = Instant::now();
    let (status, timed_out, stdout, stderr) = thread::scope(|scope| {
        let stdout_reader = scope.spawn(move || {
            let mut stdout = Vec::new();
            child_stdout.read_to_end(&mut stdout).map(|_| stdout)
        });
        let stderr_reader = scope.spawn(move || {
            let mut stderr = Vec::new();
            child_stderr.read_to_end(&mut stderr).map(|_| stderr)
        });
        let (status, timed_out) = loop {
            if let Some(status) = child.try_wait()? {
                break Ok((status, false));
            }
            if started.elapsed() >= timeout {
                child.kill()?;
                break child.wait().map(|status| (status, true));
            }
            thread::sleep(Duration::from_millis(25));
        }?;
        let stdout = stdout_reader.join().expect("stdout reader did not panic")?;
        let stderr = stderr_reader.join().expect("stderr reader did not panic")?;
        Ok::<_, io::Error>((status, timed_out, stdout, stderr))
    })?;
    if timed_out {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("command exceeded {:.1} seconds", timeout.as_secs_f64()),
        ));
    }
    Ok(ProcessOutput {
        status,
        stdout,
        stderr,
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

fn gz_json_file(path: &str) -> Value {
    let file = File::open(path).unwrap_or_else(|e| {
        error_panic!("opening `{path}` returned {e:?}");
    });
    let decoder = GzDecoder::new(file);
    serde_json::from_reader(decoder).unwrap_or_else(|e| {
        error_panic!("parsing gzip json `{path}` returned {e:?}");
    })
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
pub struct SignMessageResponse {
    pub zbase: String,
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
    pub maximum_htlc_out_msat: u64,
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
    pub last_timestamp: Option<u64>,
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
    pub fn perc(&self) -> u64 {
        (self.perc_float() * 100.0).floor() as u64
    }
    pub fn perc_float(&self) -> f64 {
        (self.our_amount_msat as f64 / self.amount_msat as f64).clamp(0.0, 1.0)
    }

    pub fn short_channel_id(&self) -> String {
        self.short_channel_id.clone().unwrap_or("".to_string())
    }

    pub fn block_born(&self) -> Option<u64> {
        self.short_channel_id
            .as_ref()?
            .split("x")
            .next()?
            .parse()
            .ok()
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

impl ClosedChannel {
    /// Get the block height when this channel was opened from the short_channel_id
    pub fn block_born(&self) -> Option<u64> {
        self.short_channel_id
            .as_ref()?
            .split("x")
            .next()?
            .parse()
            .ok()
    }

    /// Get short_channel_id or a placeholder if not available
    pub fn short_channel_id_display(&self) -> String {
        self.short_channel_id
            .clone()
            .unwrap_or_else(|| "N/A".to_string())
    }
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
    pub fee_ppm: u64,
    pub resolved_time: DateTime<Utc>,
    pub received_time: DateTime<Utc>,
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

impl TryFrom<Forward> for SettledForward {
    type Error = ();

    fn try_from(value: Forward) -> Result<Self, Self::Error> {
        let fee_msat = value.fee_msat.ok_or(())?;
        let out_msat = value.out_msat.ok_or(())?;
        let fee_ppm = if out_msat == 0 {
            0
        } else {
            ((fee_msat as f64 / out_msat as f64) * 1_000_000.0) as u64
        };

        Ok(Self {
            in_channel: value.in_channel,
            out_channel: value.out_channel.ok_or(())?,
            fee_msat,
            out_msat,
            fee_sat: fee_msat / 1000,
            out_sat: out_msat / 1000,
            fee_ppm,
            resolved_time: DateTime::from_timestamp(value.resolved_time.ok_or(())? as i64, 0)
                .ok_or(())?,
            received_time: DateTime::from_timestamp(value.received_time as i64, 0).ok_or(())?,
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
    use std::process::Command;
    use std::time::Duration;

    use super::{
        build_remote_command, lightning_cli_json_args, normalize_remote_home_path,
        run_command_with_timeout, shell_quote, GetRoutes, ListPeerChannels,
    };

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
    fn command_timeout_terminates_a_slow_process() {
        let mut command = Command::new("sleep");
        command
            .arg("1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let error = run_command_with_timeout(command, Duration::from_millis(25)).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
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
        let events = bkpr_list_account_events();
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
