use serde::Deserialize;
use std::collections::HashMap;

fn main() {
    let info: GetInfo = serde_json::from_str(&get_info()).unwrap();
    println!("my id:{}", info.id);

    let channels: ListChannels = serde_json::from_str(&list_channels()).unwrap();
    let nodes: ListNodes = serde_json::from_str(&list_nodes()).unwrap();
    println!(
        "network channels:{} nodes:{}",
        channels.channels.len(),
        nodes.nodes.len()
    );

    let nodes_by_id: HashMap<_, _> = nodes
        .nodes
        .iter()
        .filter(|e| e.alias.is_some())
        .map(|e| (&e.nodeid, e.alias.as_ref().unwrap()))
        .collect();

    let cat = list_funds();
    let funds: ListFunds = serde_json::from_str(&cat).unwrap();
    let normal_channels: Vec<_> = funds
        .channels
        .into_iter()
        .filter(|c| c.state == "CHANNELD_NORMAL")
        .collect();

    let forwards: ListForwards = serde_json::from_str(&list_forwards()).unwrap();
    let settled = forwards
        .forwards
        .iter()
        .filter(|e| e.status == "settled")
        .count();
    println!("forwards: {}/{} ", settled, forwards.forwards.len());

    let channels_by_id: HashMap<_, _> = channels
        .channels
        .iter()
        .map(|e| ((&e.short_channel_id, &e.source), e))
        .collect();

    let zero_fees = normal_channels.iter().all(|c| {
        channels_by_id
            .get(&(&c.short_channel_id, &info.id))
            .map(|e| e.base_fee_millisatoshi)
            .unwrap_or(1)
            == 0
    });
    println!(
        "my channels: {} - zero base fees? {}",
        normal_channels.len(),
        zero_fees
    );

    let mut lines = std::collections::BTreeMap::new();
    for c in normal_channels {
        let perc = c.perc();
        let our = channels_by_id.get(&(&c.short_channel_id, &info.id));
        let our_fee = our
            .map(|e| e.fee_per_millionth.to_string())
            .unwrap_or("".to_string());
        let our_base_fee = our
            .map(|e| (e.base_fee_millisatoshi / 1000).to_string())
            .unwrap_or("".to_string());
        let our_min = our
            .map(|e| (e.htlc_minimum_msat / 1000).to_string())
            .unwrap_or("".to_string());
        let our_max = our
            .map(|e| (e.htlc_maximum_msat / 1000).to_string())
            .unwrap_or("".to_string());

        let amount = c.amount_msat / 1000;

        let their = channels_by_id.get(&(&c.short_channel_id, &c.peer_id));
        let their_fee = their
            .map(|e| e.fee_per_millionth.to_string())
            .unwrap_or("".to_string());
        let their_base_fee = their
            .map(|e| (e.base_fee_millisatoshi / 1000).to_string())
            .unwrap_or("".to_string());
        let min_max = format!("{our_min}/{our_max}");

        let s = format!(
            "{min_max:>12} {our_base_fee:1} {our_fee:>5} {:>15} {amount:8} {:>3}% ({}) {their_fee:>5} {their_base_fee:>5}",
            c.short_channel_id,
            c.perc(),
            c.alias_or_id(&nodes_by_id),
        );
        lines.insert(perc, s);
    }
    for line in lines.values() {
        println!("{line}");
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

fn list_forwards() -> String {
    if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listforwards.gz"])
    } else {
        cmd_result("lightning-cli", &["listforwards"])
    }
}

fn get_info() -> String {
    if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/getinfo"])
    } else {
        cmd_result("lightning-cli", &["getinfo"])
    }
}

fn cmd_result(cmd: &str, args: &[&str]) -> String {
    let data = std::process::Command::new(cmd).args(args).output().unwrap();
    std::str::from_utf8(&data.stdout).unwrap().to_string()
}

#[derive(Deserialize, Debug)]
struct GetInfo {
    id: String,
}

#[derive(Deserialize, Debug)]
struct ListChannels {
    channels: Vec<Channel>,
}

#[derive(Deserialize, Debug)]
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

#[derive(Deserialize, Debug)]
struct ListNodes {
    nodes: Vec<Node>,
}

#[derive(Deserialize, Debug)]
struct Node {
    nodeid: String,
    alias: Option<String>,
    last_timestamp: Option<u64>,
}

#[derive(Deserialize, Debug)]
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

    fn alias_or_id(&self, m: &HashMap<&String, &String>) -> String {
        pad_or_trunc(
            m.get(&self.peer_id).unwrap_or(&&format!(
                "{}...{}",
                &self.peer_id[0..8],
                &self.peer_id[58..]
            )),
            22,
        )
    }
}

fn pad_or_trunc(s: &str, l: usize) -> String {
    if s.chars().count() > l {
        s.chars().take(l).collect()
    } else {
        format!("{:width$}", s, width = l)
    }
}

#[derive(Deserialize)]
struct ListForwards {
    forwards: Vec<Forward>,
}

#[derive(Deserialize, Debug)]
struct Forward {
    in_channel: String,
    out_channel: Option<String>,
    in_msat: u64,
    out_msat: Option<u64>,
    status: String,
    received_time: f64,
    resolved_time: Option<f64>,
}
