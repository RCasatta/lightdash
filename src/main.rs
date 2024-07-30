use chrono::{DateTime, Utc};
use rand::prelude::SliceRandom;
use std::collections::{HashMap, HashSet};

const PPM_100: u64 = 100; // when channel 100%
const PPM_0: u64 = 1000; // when channel 0%
const STEP: u64 = 20;

mod cmd;

use cmd::*;

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

    println!("forwards: {}/{} ", settled.len(), total_forwards);
    let mut last_year = 0f64;
    let mut last_month = 0f64;
    let mut last_week = 0f64;
    let mut first = now;
    let mut per_channel_montly_forwards: HashMap<String, u64> = HashMap::new();
    let mut per_channel_montly_fee_sat: HashMap<String, u64> = HashMap::new();

    for s in settled.iter() {
        let d = s.resolved_time;
        first = first.min(d);
        let days_elapsed = now.signed_duration_since(d).num_days();
        if days_elapsed < 365 {
            last_year += 1.0;
            if days_elapsed < 30 {
                *per_channel_montly_forwards
                    .entry(s.out_channel.to_string())
                    .or_default() += 1;
                *per_channel_montly_fee_sat
                    .entry(s.out_channel.to_string())
                    .or_default() += s.fee_sat;

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

    let mut lines = vec![];

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

        let (_new_fee, cmd) = calc_setchannel(&short_channel_id, &fund, our, &settled_24h);

        // calc_slingjobs(&short_channel_id, &jobs, out_fee, perc_float, amount);

        let monthly_forw = per_channel_montly_forwards
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let monthly_forw_fee = per_channel_montly_fee_sat
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let s = format!(
            "{min_max:>12} {our_base_fee:1} {our_fee:>5} {short_channel_id:>15} {amount:8} {perc:>3}% {their_fee:>5} {their_base_fee:>3} {last_timestamp_delta:>3} {last_update_delta:>3} {monthly_forw:>3} {monthly_forw_fee:>5}sat {alias_or_id}"
        );
        lines.push((perc, s, cmd));
    }

    lines.sort_by(|a, b| a.0.cmp(&b.0));

    for (_, l1, _) in lines.iter() {
        println!("{l1}");
    }

    for (_, _, l2) in lines {
        if let Some(l) = l2 {
            println!("{l}");
        }
    }

    // getroute
    let nodes_ids: Vec<_> = nodes_by_id.keys().collect();
    let mut rng = rand::thread_rng();
    let mut counters = HashMap::new();
    for _ in 0..1000 {
        let id = nodes_ids.choose(&mut rng).unwrap();
        if let Some(route) = get_route(id) {
            let mut nodes = route.route;
            nodes.pop(); // remove the random destination
            for n in nodes.iter() {
                if !peers_ids.contains(&n.id) {
                    *counters.entry(n.id.to_string()).or_insert(0u64) += 1;
                }
            }
        }
    }
    let mut counters_vec: Vec<_> = counters.into_iter().filter(|e| e.1 > 5).collect();
    counters_vec.sort_by(|a, b| a.1.cmp(&b.1));

    println!("\nNode most present in random routes:");
    for c in counters_vec {
        let id = &c.0;
        let count = c.1;
        let alias = nodes_by_id
            .get(id)
            .map(|n| n.alias.clone())
            .flatten()
            .unwrap_or("".to_string());
        println!("{id} {count:>5} {alias:?}");
    }
}

// lightning-cli sling-job -k scid=848864x399x0 direction=push amount=1000 maxppm=500 outppm=200 depleteuptoamount=100000
// fn _calc_slingjobs(
//     scid: &str,
//     jobs: &HashMap<String, JobSetting>,
//     calc_fee: u64,
//     perc: f64,
//     _amount: u64,
// ) {
//     let current = jobs.get(scid);
//     let maxppm = calc_fee - calc_fee / 4; // maxppm fee for rebalance 25% less the fee we want on the channel
//     let dir = if perc < 0.4 {
//         "pull"
//     } else if perc > 0.6 {
//         "push"
//     } else {
//         return;
//     };
//     if let Some(c) = current {
//         if c.maxppm == calc_fee {
//             return;
//         }
//     }

//     println!("`lightning-cli sling-job -k scid={scid} amount=1000 depleteuptoamount=100000 maxppm={maxppm} outppm={maxppm} direction={dir}`",);
// }

fn calc_setchannel(
    short_channel_id: &str,
    fund: &Fund,
    our: Option<&&Channel>,
    settled_24h: &[SettledForward],
) -> (u64, Option<String>) {
    let perc = fund.perc_float();
    // let amount = fund.amount_msat;
    // let our_amount = fund.our_amount_msat;
    let max_htlc_sat = fund.amount_msat / 1000;
    let max_htlc_sat = format!("{max_htlc_sat}sat");

    // if perc 1.0 => ppm = MAX_PPM
    // if perc 0.0 => ppm = MIN_PPM

    let min_ppm = PPM_100 + ((PPM_0 - PPM_100) as f64 * (1.0 - perc)) as u64;

    let current_ppm = our.map(|e| e.fee_per_millionth).unwrap_or(min_ppm);

    let did_forward_last_24h = did_forward(short_channel_id, &settled_24h);
    let new_ppm = if did_forward_last_24h {
        current_ppm.saturating_add(STEP)
    } else {
        current_ppm.saturating_sub(STEP)
    };

    let new_ppm = new_ppm.max(min_ppm);

    // let calc_fee = (((1.0 - perc) + 0.5) * (network_average as f64)) as u64;
    // let max_htlc = amount / 2 + 100;
    // let their_fee = their.map(|e| e.fee_per_millionth).unwrap_or(calc_fee);
    // let adj_calc_fee = (calc_fee + their_fee) / 2;
    // let final_fee = calc_fee.max(100);
    // let our_fee = our.map(|e| e.fee_per_millionth).unwrap_or(final_fee);
    // let our_amount = our.map(|e| e).unwrap_or(final_fee);

    let result = if current_ppm != new_ppm {
        let cmd = "lightning-cli";
        let args = format!("setchannel {short_channel_id} 0 {new_ppm} 10sat {max_htlc_sat}");

        let execute = std::env::var("EXECUTE").is_ok();
        if execute {
            let splitted_args: Vec<&str> = args.split(' ').collect();
            let result = cmd_result(cmd, &splitted_args);
            println!("{result}");
        }

        Some(format!(
            "`{cmd} {args}` current was:{current_ppm} did_forward_last_24h:{did_forward_last_24h}"
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

fn did_forward(short_channel_id: &str, forwards: &[SettledForward]) -> bool {
    for f in forwards {
        if &f.out_channel == short_channel_id {
            return true;
        }
    }
    false
}

pub fn cut_days(d: i64) -> String {
    if d > 99 {
        "99+".to_string()
    } else {
        format!("{d:>2}d")
    }
}
