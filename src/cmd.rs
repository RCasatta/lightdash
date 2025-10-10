use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

pub fn _sling_jobsettings() -> HashMap<String, JobSetting> {
    let str = if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/sling-jobsettings"])
    } else {
        cmd_result("lightning-cli", &["sling-jobsettings"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn list_funds() -> ListFunds {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listfunds.gz"])
    } else {
        cmd_result("lightning-cli", &["listfunds"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn list_nodes() -> ListNodes {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listnodes.gz"])
    } else {
        cmd_result("lightning-cli", &["listnodes"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn list_channels() -> ListChannels {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listchannels.gz"])
    } else {
        cmd_result("lightning-cli", &["listchannels"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn list_peers() -> ListPeers {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listpeers.gz"])
    } else {
        cmd_result("lightning-cli", &["listpeers"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn list_forwards() -> ListForwards {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listforwards.gz"])
    } else {
        cmd_result("lightning-cli", &["listforwards"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn list_closed_channels() -> ListClosedChannels {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listclosedchannels.gz"])
    } else {
        cmd_result("lightning-cli", &["listclosedchannels"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn get_info() -> GetInfo {
    let str = if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/getinfo"])
    } else {
        cmd_result("lightning-cli", &["getinfo"])
    };
    serde_json::from_str(&str).unwrap()
}

pub fn get_route(id: &str) -> Option<GetRoute> {
    let str = if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/getroute"])
    } else {
        cmd_result("lightning-cli", &["getroute", id, "10000000", "10"]) // TODO parametrize amount and riskfactor
    };
    serde_json::from_str(&str).ok()
}

pub fn cmd_result(cmd: &str, args: &[&str]) -> String {
    // println!("cmd:{cmd} args:{args:?}");
    let data = std::process::Command::new(cmd).args(args).output().unwrap();
    std::str::from_utf8(&data.stdout).unwrap().to_string()
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
pub struct JobSetting {
    pub amount_msat: u64,
    pub maxppm: u64,
    pub outppm: u64,
    pub sat_direction: String,
}

#[derive(Deserialize, Debug)]
pub struct ListPeers {
    pub peers: Vec<Peer>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub connected: bool,
    pub num_channels: u64,
    pub features: String,
    #[serde(default)]
    pub channels: Vec<PeerChannel>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PeerChannel {
    pub state: String,
    #[serde(default)]
    pub short_channel_id: Option<String>,
    #[serde(default)]
    pub direction: Option<u64>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub funding_txid: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Channel {
    pub source: String,
    pub destination: String,
    pub short_channel_id: String,
    pub amount_msat: u64,
    pub active: bool,
    pub last_update: u64,
    pub base_fee_millisatoshi: u64,
    pub fee_per_millionth: u64,
    pub delay: u64,
    pub htlc_minimum_msat: u64,
    pub htlc_maximum_msat: u64,
    pub features: String,
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

impl Fund {
    pub fn perc(&self) -> u64 {
        (self.perc_float() * 100.0).floor() as u64
    }
    pub fn perc_float(&self) -> f64 {
        (self.our_amount_msat as f64 / self.amount_msat as f64)
            .min(1.0)
            .max(0.0)
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

#[derive(Deserialize, Debug, Clone)]
pub struct ClosedChannel {
    #[serde(default)]
    pub peer_id: Option<String>,
    pub channel_id: String,
    #[serde(default)]
    pub short_channel_id: Option<String>,
    #[serde(default)]
    pub alias: Option<ChannelAlias>,
    pub opener: String,
    #[serde(default)]
    pub closer: Option<String>,
    #[serde(default)]
    pub private: Option<bool>,
    #[serde(default)]
    pub channel_type: Option<ChannelType>,
    #[serde(default)]
    pub total_local_commitments: Option<u64>,
    #[serde(default)]
    pub total_remote_commitments: Option<u64>,
    #[serde(default)]
    pub total_htlcs_sent: Option<u64>,
    pub funding_txid: String,
    pub funding_outnum: u32,
    #[serde(default)]
    pub leased: Option<bool>,
    pub total_msat: u64,
    pub final_to_us_msat: u64,
    pub min_to_us_msat: u64,
    pub max_to_us_msat: u64,
    #[serde(default)]
    pub last_commitment_txid: Option<String>,
    #[serde(default)]
    pub last_commitment_fee_msat: Option<u64>,
    pub close_cause: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChannelAlias {
    #[serde(rename = "local")]
    pub local_alias: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChannelType {
    pub bits: Vec<u32>,
    pub names: Vec<String>,
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
}

#[derive(Clone, Debug)]
pub struct SettledForward {
    pub in_channel: String,
    pub out_channel: String,
    pub fee_sat: u64,
    pub out_sat: u64,
    pub fee_ppm: u64,
    pub resolved_time: DateTime<Utc>,
    pub received_time: DateTime<Utc>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RouteNode {
    pub id: String,
    pub channel: String,
    pub direction: u8,
    pub amount_msat: u64,
    pub delay: u64,
    pub style: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GetRoute {
    pub route: Vec<RouteNode>,
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

/// Store data in the datastore with a given key and string value
pub fn datastore_string(
    key: &[&str],
    value: &str,
    mode: DatastoreMode,
) -> Result<DatastoreResponse, String> {
    let key_json = serde_json::to_string(key).map_err(|e| e.to_string())?;
    let args = vec![
        "datastore",
        "-k",
        "key",
        &key_json,
        "string",
        value,
        "mode",
        mode.as_str(),
    ];

    let str = cmd_result("lightning-cli", &args);
    serde_json::from_str(&str).map_err(|e| format!("Failed to parse response: {}", e))
}

/// Store data in the datastore with a given key and hex value
pub fn datastore_hex(
    key: &[&str],
    hex: &str,
    mode: DatastoreMode,
) -> Result<DatastoreResponse, String> {
    let key_json = serde_json::to_string(key).map_err(|e| e.to_string())?;
    let args = vec![
        "datastore",
        "-k",
        "key",
        &key_json,
        "hex",
        hex,
        "mode",
        mode.as_str(),
    ];

    let str = cmd_result("lightning-cli", &args);
    serde_json::from_str(&str).map_err(|e| format!("Failed to parse response: {}", e))
}

/// List/retrieve data from the datastore, optionally filtered by key
pub fn listdatastore(key: Option<&[&str]>) -> Result<ListDatastore, String> {
    let str = if let Some(k) = key {
        let key_json = serde_json::to_string(k).map_err(|e| e.to_string())?;
        cmd_result("lightning-cli", &["listdatastore", "-k", "key", &key_json])
    } else {
        cmd_result("lightning-cli", &["listdatastore"])
    };

    serde_json::from_str(&str).map_err(|e| format!("Failed to parse response: {}", e))
}

/// Delete data from the datastore
pub fn deldatastore(key: &[&str]) -> Result<DatastoreResponse, String> {
    let key_json = serde_json::to_string(key).map_err(|e| e.to_string())?;
    let args = vec!["deldatastore", "-k", "key", &key_json];

    let str = cmd_result("lightning-cli", &args);
    serde_json::from_str(&str).map_err(|e| format!("Failed to parse response: {}", e))
}

#[derive(Debug, Clone, Copy)]
pub enum DatastoreMode {
    MustCreate,
    MustReplace,
    CreateOrReplace,
    MustAppend,
    CreateOrAppend,
}

impl DatastoreMode {
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
    pub generation: Option<u64>,
    #[serde(default)]
    pub hex: Option<String>,
    #[serde(default)]
    pub string: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ListDatastore {
    pub datastore: Vec<DatastoreResponse>,
}
