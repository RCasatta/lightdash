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
        cmd_result("cat", &["test-json/listfunds"])
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
        cmd_result("cat", &["test-json/listpeers"])
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

impl Node {
    fn alias(&self) -> String {
        self.alias.clone().unwrap_or("".to_string())
    }
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

    pub fn alias_or_id(&self, m: &HashMap<&String, &Node>) -> String {
        pad_or_trunc(
            &m.get(&self.peer_id).map(|e| e.alias()).unwrap_or(format!(
                "{}...{}",
                &self.peer_id[0..8],
                &self.peer_id[58..]
            )),
            24,
        )
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
        Ok(Self {
            in_channel: value.in_channel,
            out_channel: value.out_channel.ok_or(())?,
            fee_sat: value.fee_msat.map(|e| e / 1000).ok_or(())?,
            resolved_time: DateTime::from_timestamp(value.resolved_time.ok_or(())? as i64, 0)
                .ok_or(())?,
            received_time: DateTime::from_timestamp(value.received_time as i64, 0).ok_or(())?,
        })
    }
}

pub fn pad_or_trunc(s: &str, l: usize) -> String {
    // println!("DEBUG {s} has {} chars", s.chars().count());
    if s.chars().count() > l {
        s.chars().take(l).collect()
    } else {
        format!("{:width$}", s, width = l)
    }
}
