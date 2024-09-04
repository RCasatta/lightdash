use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

/// Compute the minimum ppm of the channel according to the percentual owned by us
/// The intention is to signal via an high fee the channel depletion
fn min_ppm(perc: f64) -> u64 {
    const PPM_MIN: u64 = 100; // minimum betwee 100% and 50%
    const PPM_MAX: u64 = 2000; // when channel 0%, between 0% and 50% increase linearly
    let delta = (PPM_MAX - PPM_MIN) as f64;
    if perc > 0.5 {
        PPM_MIN
    } else {
        ((PPM_MAX as f64) + (2.0 * perc * -delta)) as u64 // since perc>0 this is positive
    }
}

const STEP_PERC: f64 = 0.06;

mod cmd;

use cmd::*;

#[derive(Default)]
struct ChannelMeta {
    count: u64,
    fee_sum: u64,
}

impl ChannelMeta {
    pub fn avg_fee(&self) -> f64 {
        self.fee_sum as f64 / self.count as f64
    }
}

fn main() {
    let now = Utc::now();
    println!("{}", now);
    let info = get_info();
    println!("my id:{}", info.id);

    let channels = list_channels();
    let nodes = list_nodes();
    let peers = list_peers();

    println!(
        "network channels:{} nodes:{} peers:{}",
        channels.channels.len(),
        nodes.nodes.len(),
        peers.peers.len(),
    );

    let peers_ids: HashSet<_> = peers
        .peers
        .iter()
        .filter(|e| e.num_channels > 0)
        .map(|e| &e.id)
        .collect();

    let nodes_by_id: HashMap<_, _> = nodes
        .nodes
        .iter()
        .filter(|e| e.alias.is_some())
        .map(|e| (&e.nodeid, e))
        .collect();

    let mut chan_meta_per_node = HashMap::new();

    for c in channels.channels.iter() {
        let meta: &mut ChannelMeta = chan_meta_per_node.entry(&c.source).or_default();
        meta.count += 1;
        meta.fee_sum += c.fee_per_millionth;
    }

    let funds = list_funds();
    let normal_channels: Vec<_> = funds
        .channels
        .into_iter()
        .filter(|c| c.state == "CHANNELD_NORMAL")
        .collect();

    let forwards = list_forwards();
    let total_forwards = forwards.forwards.len();
    let settled: Vec<_> = forwards
        .forwards
        .into_iter()
        .filter(|e| e.status == "settled")
        .map(|e| SettledForward::try_from(e).unwrap())
        .collect();
    let settled_24h = filter_forwards(&settled, 24, &now);

    // let jobs = sling_jobsettings();
    let forwards_perc = (settled.len() as f64 / total_forwards as f64) * 100.0;

    println!(
        "forwards: {}/{} {:.1}%",
        settled.len(),
        total_forwards,
        forwards_perc
    );
    let mut last_year = 0f64;
    let mut last_month = 0f64;
    let mut last_week = 0f64;
    let mut first = now;

    let mut per_channel_ever_forwards: HashMap<String, u64> = HashMap::new();
    let mut per_channel_ever_fee_sat: HashMap<String, u64> = HashMap::new();

    let mut per_channel_forwards_in: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out: HashMap<String, u64> = HashMap::new();

    for s in settled.iter() {
        let d = s.resolved_time;
        first = first.min(d);
        let days_elapsed = now.signed_duration_since(d).num_days();
        *per_channel_forwards_in
            .entry(s.in_channel.to_string())
            .or_default() += 1;
        *per_channel_forwards_out
            .entry(s.out_channel.to_string())
            .or_default() += 1;

        *per_channel_ever_forwards
            .entry(s.out_channel.to_string())
            .or_default() += 1;
        *per_channel_ever_fee_sat
            .entry(s.out_channel.to_string())
            .or_default() += s.fee_sat;

        if days_elapsed < 365 {
            last_year += 1.0;
            if days_elapsed < 30 {
                last_month += 1.0;
                if days_elapsed < 7 {
                    last_week += 1.0;
                }
            }
        }
    }

    if std::env::var("ONLY_ROUTES").is_ok() {
        calc_routes(nodes_by_id, peers_ids, &chan_meta_per_node);
        return;
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

    let mut lines = vec![];
    let mut sling_lines = vec![];

    for fund in normal_channels {
        let perc = fund.perc();
        let short_channel_id = fund.short_channel_id();
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

        let amount = fund.amount_msat / 1000;

        let their = channels_by_id.get(&(&short_channel_id, &fund.peer_id));
        let their_fee = their
            .map(|e| e.fee_per_millionth.to_string())
            .unwrap_or("".to_string());
        let their_base_fee = their
            .map(|e| (e.base_fee_millisatoshi / 1000).to_string())
            .unwrap_or("".to_string());
        let min_max = format!("{our_min}/{our_max}");

        let last_timestamp = nodes_by_id
            .get(&fund.peer_id)
            .map(|e| DateTime::from_timestamp(e.last_timestamp.unwrap_or(0) as i64, 0).unwrap())
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        let last_timestamp_delta = cut_days(now.signed_duration_since(last_timestamp).num_days());

        let last_update = their
            .map(|e| DateTime::from_timestamp(e.last_update as i64, 0).unwrap())
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        let last_update_delta = cut_days(now.signed_duration_since(last_update).num_days());
        let short_channel_id = fund.short_channel_id();
        let alias_or_id = fund.alias_or_id(&nodes_by_id);

        let (_new_fee, cmd) =
            calc_setchannel(&short_channel_id, &alias_or_id, &fund, our, &settled_24h);

        let ever_forw = *per_channel_ever_forwards
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_fee = *per_channel_ever_fee_sat
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_in = *per_channel_forwards_in
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_out = *per_channel_forwards_out
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forward_in_out = ever_forw_out + ever_forw_in;
        let is_sink = (ever_forw_out as f64 / ever_forward_in_out as f64) * 100.0;

        if let Some(l) = calc_slingjobs(
            &short_channel_id,
            is_sink / 100.0,
            fund.perc_float(),
            ever_forward_in_out,
            &alias_or_id,
        ) {
            sling_lines.push(l);
        }

        let s = format!(
            "{min_max:>12} {our_base_fee:1} {our_fee:>5} {short_channel_id:>15} {amount:8} {perc:>3}% {their_fee:>5} {their_base_fee:>3} {last_timestamp_delta:>3} {last_update_delta:>3} {ever_forw:>3} {ever_forw_fee:>5}sat {is_sink:>3.0}% {alias_or_id}"
        );
        lines.push((perc, s, cmd));
    }

    lines.sort_by(|a, b| a.0.cmp(&b.0));
    println!("min_max our_base_fee our_fee scid amount perc their_fee their_base_fee last_tstamp_delta last_upd_delta monthly_forw monthly_forw_fee is_sink alias_or_id");

    for (_, l1, _) in lines.iter() {
        println!("{l1}");
    }

    for (_, _, l2) in lines {
        if let Some(l) = l2 {
            println!("{l}");
        }
    }

    for (cmd, details) in sling_lines.iter() {
        println!("`{cmd}` {details}");
        let execute = std::env::var("EXECUTE_SLING_JOBS").is_ok();
        if execute {
            let split: Vec<&str> = cmd.split(' ').collect();
            let _result = cmd_result(split[0], &split[1..]);
        }
    }
}

fn calc_routes(
    nodes_by_id: HashMap<&String, &Node>,
    peers_ids: HashSet<&String>,
    chan_meta: &HashMap<&String, ChannelMeta>,
) {
    let mut counters = HashMap::new();
    let mut hop_sum = 0usize;
    let mut total = 0;
    for id in nodes_by_id.keys() {
        if let Some(route) = get_route(id) {
            let mut nodes = route.route;
            hop_sum += nodes.len();
            total += 1;
            nodes.pop(); // remove the random destination
            for n in nodes.iter() {
                if !peers_ids.contains(&n.id) {
                    *counters.entry(n.id.to_string()).or_insert(0u64) += 1;
                }
            }
        }
    }
    let mut counters_vec: Vec<_> = counters.into_iter().filter(|e| e.1 > 2).collect();
    counters_vec.sort_by(|a, b| b.1.cmp(&a.1));

    let average_hops = hop_sum as f64 / total as f64;
    println!("\nNode most present in random routes (average hops:{average_hops:.2}):");
    for c in counters_vec {
        let id = &c.0;
        let count = c.1;
        let alias = nodes_by_id
            .get(id)
            .map(|n| n.alias.clone())
            .flatten()
            .unwrap_or("".to_string());
        let avg_fee = chan_meta.get(&c.0).unwrap().avg_fee();
        println!("{id} {count:>5} avg_fee:{avg_fee:>6.1} {alias}");
    }
}

// lightning-cli sling-job -k scid=848864x399x0 direction=push amount=1000 maxppm=500 outppm=200 depleteuptoamount=100000
fn calc_slingjobs(
    scid: &str,
    is_sink: f64,
    perc_us: f64,
    ever_forward_in_out: u64,
    alias: &str,
) -> Option<(String, String)> {
    let amount = 100000;
    let maxppm = 100;

    let (dir, out_ppm, target) = if perc_us < 0.25 && is_sink > 0.8 {
        ("pull", 1100, 0.4)
    } else if perc_us > 0.75 && is_sink < 0.2 {
        ("push", 900, 0.6)
    } else {
        return None;
    };

    let cmd = format!("lightning-cli sling-job -k scid={scid} amount={amount} maxppm={maxppm} outppm={out_ppm} direction={dir} target={target}");
    let details =
        format!("perc_us:{perc_us:.2} is_sink:{is_sink:.2} {ever_forward_in_out} {alias}");
    Some((cmd, details))
}

fn calc_setchannel(
    short_channel_id: &str,
    alias: &str,
    fund: &Fund,
    our: Option<&&Channel>,
    settled_24h: &[SettledForward],
) -> (u64, Option<String>) {
    let perc = fund.perc_float();
    // let amount = fund.amount_msat;
    // let our_amount = fund.our_amount_msat;
    let max_htlc_sat = fund.amount_msat / 1000;
    let max_htlc_sat = format!("{max_htlc_sat}sat");

    let min_ppm = min_ppm(perc);

    let current_ppm = our.map(|e| e.fee_per_millionth).unwrap_or(min_ppm);

    let forwards_last_24h = did_forward(short_channel_id, &settled_24h);
    let did_forwards_last_24h = !forwards_last_24h.is_empty();
    let step = (current_ppm as f64 * STEP_PERC) as u64;
    let new_ppm = if did_forwards_last_24h {
        current_ppm.saturating_add(step)
    } else {
        current_ppm.saturating_sub(step)
    };

    let new_ppm = new_ppm.max(min_ppm);

    let truncated_min = min_ppm == new_ppm;

    let result = if current_ppm != new_ppm {
        let cmd = "lightning-cli";
        let args = format!("setchannel {short_channel_id} 0 {new_ppm} 10sat {max_htlc_sat}");

        let execute = std::env::var("EXECUTE").is_ok();
        if execute | truncated_min {
            // execute is true once a day
            // but we need to trim for min to have faster reaction on channel depletion
            let splitted_args: Vec<&str> = args.split(' ').collect();
            let result = cmd_result(cmd, &splitted_args);
            println!("{result}");
        }

        Some(format!(
            "`{cmd} {args}` was:{current_ppm} perc:{perc:.2} min:{min_ppm} forward_last_24h:{} truncated_min:{truncated_min} {alias}",
            forwards_last_24h.len()
        ))
    } else {
        None
    };

    (new_ppm, result)
}

fn filter_forwards(
    forwards: &[SettledForward],
    hour: i64,
    now: &DateTime<Utc>,
) -> Vec<SettledForward> {
    forwards
        .iter()
        .filter(|f| now.signed_duration_since(f.resolved_time).num_hours() <= hour)
        .cloned()
        .collect()
}

fn did_forward<'a>(
    short_channel_id: &str,
    forwards: &'a [SettledForward],
) -> Vec<&'a SettledForward> {
    forwards
        .iter()
        .filter(|f| f.out_channel == short_channel_id)
        .collect()
}

pub fn cut_days(d: i64) -> String {
    if d > 99 {
        "99+".to_string()
    } else {
        format!("{d:>2}d")
    }
}
