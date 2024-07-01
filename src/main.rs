use serde::Deserialize;
use std::collections::HashMap;

fn main() {
    let zcat = list_channels();
    println!("len:{}", zcat.len());
    let channels: ListChannels = serde_json::from_str(&zcat).unwrap();
    println!("channels:{}", channels.channels.len());

    let zcat = list_nodes();
    println!("len:{}", zcat.len());
    let nodes: ListNodes = serde_json::from_str(&zcat).unwrap();
    println!("nodes:{}", nodes.nodes.len());

    let nodes_by_id: HashMap<_, _> = nodes
        .nodes
        .iter()
        .filter(|e| e.alias.is_some())
        .map(|e| (&e.nodeid, e.alias.as_ref().unwrap()))
        .collect();

    let cat = list_funds();
    println!("len:{}", cat.len());
    let funds: ListFunds = serde_json::from_str(&cat).unwrap();
    println!("my channels:{}", funds.channels.len());

    let channels_by_source: HashMap<_, _> =
        channels.channels.iter().map(|e| (&e.source, e)).collect();

    for c in funds.channels {
        println!(
            "{} {} perc:{}",
            c.peer_id,
            nodes_by_id.get(&c.peer_id).unwrap_or(&&"".to_string()),
            c.perc()
        );
    }
}

fn list_funds() -> String {
    if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/listfunds"])
    } else {
        cmd_result("lightning-cli", &["listfunds"])
    }
}

fn list_nodes() -> String {
    if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listnodes.gz"])
    } else {
        cmd_result("lightning-cli", &["listnodes"])
    }
}

fn list_channels() -> String {
    if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listchannels.gz"])
    } else {
        cmd_result("lightning-cli", &["listchannels"])
    }
}

fn cmd_result(cmd: &str, args: &[&str]) -> String {
    println!("executing `{cmd}`");
    let data = std::process::Command::new(cmd).args(args).output().unwrap();
    std::str::from_utf8(&data.stdout).unwrap().to_string()
}

#[derive(Deserialize)]
struct ListChannels {
    channels: Vec<Channel>,
}

#[derive(Deserialize)]
struct Channel {
    source: String,
    destination: String,
    short_channel_id: String,
    amount_msat: u64,
    active: bool,
    last_update: u64,
    base_fee_millisatoshi: u64,
    fee_per_millionth: u64,
    delay: u64,
    htlc_minimum_msat: u64,
    htlc_maximum_msat: u64,
    features: String,
}

#[derive(Deserialize)]
struct ListNodes {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
struct Node {
    nodeid: String,
    alias: Option<String>,
    last_timestamp: Option<u64>,
}

#[derive(Deserialize)]
struct ListFunds {
    channels: Vec<Fund>,
}

#[derive(Deserialize, Debug)]
struct Fund {
    peer_id: String,
    connected: bool,
    state: String,
    channel_id: String,
    short_channel_id: String,
    our_amount_msat: u64,
    amount_msat: u64,
    funding_txid: String,
    funding_output: u32,
}

impl Fund {
    fn perc(&self) -> u64 {
        ((self.our_amount_msat as f64 / self.amount_msat as f64) * 100.0).floor() as u64
    }
}
