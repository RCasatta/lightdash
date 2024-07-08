use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;

fn main() {
    let now = Utc::now();
    let info = get_info();
    println!("my id:{}", info.id);

    let channels = list_channels();
    let nodes = list_nodes();
    println!(
        "network channels:{} nodes:{}",
        channels.channels.len(),
        nodes.nodes.len()
    );

    let nodes_by_id: HashMap<_, _> = nodes
        .nodes
        .iter()
        .filter(|e| e.alias.is_some())
        .map(|e| (&e.nodeid, e))
        .collect();

    let funds = list_funds();
    let normal_channels: Vec<_> = funds
        .channels
        .into_iter()
        .filter(|c| c.state == "CHANNELD_NORMAL")
        .collect();

    let forwards = list_forwards();
    let settled: Vec<_> = forwards
        .forwards
        .iter()
        .filter(|e| e.status == "settled")
        .collect();

    let jobs = sling_jobsettings();

    println!("forwards: {}/{} ", settled.len(), forwards.forwards.len());
    let mut last_year = 0f64;
    let mut last_month = 0f64;
    let mut last_week = 0f64;
    let mut first = now;
    let mut per_channel_montly_forwards: HashMap<String, u64> = HashMap::new();
    for s in settled.iter() {
        let d = DateTime::from_timestamp(s.resolved_time.unwrap() as i64, 0).unwrap();
        first = first.min(d);
        let days_elapsed = now.signed_duration_since(d).num_days();
        if days_elapsed < 365 {
            last_year += 1.0;
            if days_elapsed < 30 {
                if let Some(channel) = s.out_channel.as_ref() {
                    *per_channel_montly_forwards
                        .entry(channel.to_string())
                        .or_default() += 1;
                }

                last_month += 1.0;
                if days_elapsed < 7 {
                    last_week += 1.0;
                }
            }
        }
    }
    let el = now.signed_duration_since(first).num_days();
    println!(
        "settled frequency ever:{:.2} year:{:.2} month:{:.2} week:{:.2}",
        settled.len() as f64 / el as f64,
        last_year / 365.0,
        last_month / 30.0,
        last_week / 7.0
    );

    let mut sum_fee_rate = 0u128;
    let mut count = 0u128;
    for c in channels.channels.iter() {
        if c.base_fee_millisatoshi != 0 {
            continue;
        }
        if c.fee_per_millionth > 10000 {
            continue;
        }
        sum_fee_rate += c.fee_per_millionth as u128;
        count += 1;
    }
    let network_average = (sum_fee_rate / count) as u64;
    println!(
        "network average fee: {network_average} per millionth {:.3}% ",
        network_average as f64 / 10000.0
    );

    let channels_by_id: HashMap<_, _> = channels
        .channels
        .iter()
        .map(|e| ((&e.short_channel_id, &e.source), e))
        .collect();

    let zero_fees = normal_channels.iter().all(|c| {
        channels_by_id
            .get(&(&c.short_channel_id(), &info.id))
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
        let short_channel_id = c.short_channel_id();
        let our = channels_by_id.get(&(&short_channel_id, &info.id));
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

        let their = channels_by_id.get(&(&short_channel_id, &c.peer_id));
        let their_fee = their
            .map(|e| e.fee_per_millionth.to_string())
            .unwrap_or("".to_string());
        let their_base_fee = their
            .map(|e| (e.base_fee_millisatoshi / 1000).to_string())
            .unwrap_or("".to_string());
        let min_max = format!("{our_min}/{our_max}");

        let last_timestamp = nodes_by_id
            .get(&c.peer_id)
            .map(|e| DateTime::from_timestamp(e.last_timestamp.unwrap_or(0) as i64, 0).unwrap())
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        let last_timestamp_delta = cut_days(now.signed_duration_since(last_timestamp).num_days());

        let last_update = their
            .map(|e| DateTime::from_timestamp(e.last_update as i64, 0).unwrap())
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        let last_update_delta = cut_days(now.signed_duration_since(last_update).num_days());
        let short_channel_id = c.short_channel_id();
        let alias_or_id = c.alias_or_id(&nodes_by_id);

        let perc_float = c.perc_float();
        let out_fee = calc_setchannel(
            &short_channel_id,
            perc_float,
            amount,
            our,
            their,
            network_average,
        );

        calc_slingjobs(&short_channel_id, &jobs, out_fee, perc_float, amount);

        let monthly_forw = per_channel_montly_forwards
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let s = format!(
            "{min_max:>12} {our_base_fee:1} {our_fee:>5} {short_channel_id:>15} {amount:8} {perc:>3}% {their_fee:>5} {their_base_fee:>3} {last_timestamp_delta:>3} {last_update_delta:>3} {monthly_forw:>3} {alias_or_id}"
        );
        lines.insert((perc_float * 100000.0) as u64, s);
    }
    for line in lines.values() {
        println!("{line}");
    }
}

// lightning-cli sling-job -k scid=848864x399x0 direction=push amount=1000 maxppm=500 outppm=200 depleteuptoamount=100000
fn calc_slingjobs(
    scid: &str,
    jobs: &HashMap<String, JobSetting>,
    calc_fee: u64,
    perc: f64,
    _amount: u64,
) {
    let current = jobs.get(scid);
    let maxppm = calc_fee - calc_fee / 4; // maxppm fee for rebalance 25% less the fee we want on the channel
    let dir = if perc < 0.4 {
        "pull"
    } else if perc > 0.6 {
        "push"
    } else {
        return;
    };
    if let Some(c) = current {
        if c.maxppm == calc_fee {
            return;
        }
    }

    println!("`lightning-cli sling-job -k scid={scid} amount=1000 depleteuptoamount=100000 maxppm={maxppm} outppm={maxppm} direction={dir}`",);
}

fn calc_setchannel(
    short_channel_id: &str,
    perc: f64,
    amount: u64,
    our: Option<&&Channel>,
    _their: Option<&&Channel>,
    network_average: u64,
) -> u64 {
    let calc_fee = (((1.0 - perc) + 0.5) * (network_average as f64)) as u64;
    let max_htlc = amount / 2 + 100;
    // let their_fee = their.map(|e| e.fee_per_millionth).unwrap_or(calc_fee);
    // let adj_calc_fee = (calc_fee + their_fee) / 2;
    let final_fee = calc_fee.max(100);
    let our_fee = our.map(|e| e.fee_per_millionth).unwrap_or(final_fee);

    let diff = (our_fee as f64 - final_fee as f64) / final_fee as f64;
    if diff.abs() > 0.05 {
        println!("`lightning-cli setchannel {short_channel_id} 0 {final_fee} 10sat {max_htlc}sat` diff:{diff:.2}");
    }
    final_fee
}

fn sling_jobsettings() -> HashMap<String, JobSetting> {
    let str = if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/sling-jobsettings"])
    } else {
        cmd_result("lightning-cli", &["sling-jobsettings"])
    };
    serde_json::from_str(&str).unwrap()
}

fn list_funds() -> ListFunds {
    let str = if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/listfunds"])
    } else {
        cmd_result("lightning-cli", &["listfunds"])
    };
    serde_json::from_str(&str).unwrap()
}

fn list_nodes() -> ListNodes {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listnodes.gz"])
    } else {
        cmd_result("lightning-cli", &["listnodes"])
    };
    serde_json::from_str(&str).unwrap()
}

fn list_channels() -> ListChannels {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listchannels.gz"])
    } else {
        cmd_result("lightning-cli", &["listchannels"])
    };
    serde_json::from_str(&str).unwrap()
}

fn list_forwards() -> ListForwards {
    let str = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listforwards.gz"])
    } else {
        cmd_result("lightning-cli", &["listforwards"])
    };
    serde_json::from_str(&str).unwrap()
}

fn get_info() -> GetInfo {
    let str = if cfg!(debug_assertions) {
        cmd_result("cat", &["test-json/getinfo"])
    } else {
        cmd_result("lightning-cli", &["getinfo"])
    };
    serde_json::from_str(&str).unwrap()
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
struct JobSetting {
    amount_msat: u64,
    maxppm: u64,
    outppm: u64,
    sat_direction: String,
}

#[derive(Deserialize, Debug)]
struct Channel {
    source: String,
    // destination: String,
    short_channel_id: String,
    // amount_msat: u64,
    // active: bool,
    last_update: u64,
    base_fee_millisatoshi: u64,
    fee_per_millionth: u64,
    // delay: u64,
    htlc_minimum_msat: u64,
    htlc_maximum_msat: u64,
    // features: String,
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

impl Node {
    fn alias(&self) -> String {
        self.alias.clone().unwrap_or("".to_string())
    }
}

#[derive(Deserialize, Debug)]
struct ListFunds {
    channels: Vec<Fund>,
}

#[derive(Deserialize, Debug)]
struct Fund {
    peer_id: String,
    // connected: bool,
    state: String,
    // channel_id: String,
    short_channel_id: Option<String>,
    our_amount_msat: u64,
    amount_msat: u64,
    // funding_txid: String,
    // funding_output: u32,
}

impl Fund {
    fn perc(&self) -> u64 {
        ((self.our_amount_msat as f64 / self.amount_msat as f64) * 100.0).floor() as u64
    }
    fn perc_float(&self) -> f64 {
        self.our_amount_msat as f64 / self.amount_msat as f64
    }

    fn short_channel_id(&self) -> String {
        self.short_channel_id.clone().unwrap_or("".to_string())
    }

    fn alias_or_id(&self, m: &HashMap<&String, &Node>) -> String {
        pad_or_trunc(
            &m.get(&self.peer_id).map(|e| e.alias()).unwrap_or(format!(
                "{}...{}",
                &self.peer_id[0..8],
                &self.peer_id[58..]
            )),
            24,
        )
    }
}

fn cut_days(d: i64) -> String {
    if d > 99 {
        "99+".to_string()
    } else {
        format!("{d:>2}d")
    }
}

fn pad_or_trunc(s: &str, l: usize) -> String {
    // println!("DEBUG {s} has {} chars", s.chars().count());
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
    // in_channel: String,
    out_channel: Option<String>,
    // in_msat: u64,
    // out_msat: Option<u64>,
    status: String,
    // received_time: f64,
    resolved_time: Option<f64>,
}
