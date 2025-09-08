use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::cmd::*;
use crate::common::*;

pub fn run_dashboard() {
    let now = Utc::now();
    println!("{}", now);
    let info = get_info();
    println!("my id:{}", info.id);
    let current_block = info.blockheight;

    let channels = list_channels();
    let nodes = list_nodes();
    let peers = list_peers();

    println!(
        "network channels:{} nodes:{} peers:{}",
        channels.channels.len(),
        nodes.nodes.len(),
        peers.peers.len(),
    );

    let _peers_ids: std::collections::HashSet<_> = peers
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
        let meta: &mut ChannelFee = chan_meta_per_node.entry(&c.source).or_default();
        meta.count += 1;
        meta.fee_sum += c.fee_per_millionth;
        meta.fee_rates.insert(c.fee_per_millionth);
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
    let mut per_channel_ever_incoming_fee_sat: HashMap<String, i64> = HashMap::new();

    let mut per_channel_forwards_in: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out: HashMap<String, u64> = HashMap::new();

    let mut per_channel_forwards_in_last_month: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out_last_month: HashMap<String, u64> = HashMap::new();

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
        *per_channel_ever_incoming_fee_sat
            .entry(s.in_channel.to_string())
            .or_default() -= s.fee_sat as i64;

        if days_elapsed < 365 {
            last_year += 1.0;
            if days_elapsed < 30 {
                last_month += 1.0;

                *per_channel_forwards_in_last_month
                    .entry(s.in_channel.to_string())
                    .or_default() += 1;
                *per_channel_forwards_out_last_month
                    .entry(s.out_channel.to_string())
                    .or_default() += 1;

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
    let mut sling_lines = vec![];

    let mut perces = vec![];

    let mut channels = vec![];

    // Compute ChannelMeta
    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();

        let ever_forw_in = *per_channel_forwards_in
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_out = *per_channel_forwards_out
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forward_in_out = ever_forw_out + ever_forw_in;

        // 100% is sink, 0% is source
        let is_sink = if ever_forward_in_out == 0 {
            // Avoid resulting in NaN
            0.5
        } else {
            (ever_forw_out as f64) / (ever_forward_in_out as f64)
        };

        let last_month_forw_in = *per_channel_forwards_in_last_month
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let last_month_forw_out = *per_channel_forwards_out_last_month
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let last_month_forward_in_out = last_month_forw_out + last_month_forw_in;

        // 100% is sink, 0% is source
        let is_sink_last_month = if last_month_forward_in_out == 0 {
            // Avoid resulting in NaN
            0.5
        } else {
            (last_month_forw_out as f64) / (last_month_forward_in_out as f64)
        };

        let perc = fund.perc_float();
        let rebalance = if perc < 0.3 && is_sink_last_month >= 0.5 {
            Rebalance::PullIn
        } else if perc > 0.7 && is_sink_last_month <= 0.5 {
            Rebalance::PushOut
        } else {
            Rebalance::Nothing
        };

        let alias_or_id = fund.alias_or_id(&nodes_by_id);

        let c = ChannelMeta {
            fund: fund.clone(),
            is_sink,
            rebalance,
            alias_or_id,
            is_sink_last_month,
            block_born: fund.block_born().unwrap_or(0),
        };
        channels.push(c);
    }
    let pull_in: Vec<_> = channels
        .iter()
        .filter(|e| e.rebalance == Rebalance::PullIn)
        .map(|e| e.fund.short_channel_id())
        .collect();
    let push_out: Vec<_> = channels
        .iter()
        .filter(|e| e.rebalance == Rebalance::PushOut)
        .map(|e| e.fund.short_channel_id())
        .collect();
    println!("pull_in:{} push_out:{}", pull_in.len(), push_out.len());

    for channel in channels {
        let fund = &channel.fund;
        let perc = fund.perc();
        perces.push(fund.perc_float());
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

        let (_new_fee, cmd) = calc_setchannel(
            &short_channel_id,
            &channel.alias_or_id(),
            &fund,
            our,
            &settled_24h,
        );

        let ever_forw = *per_channel_ever_forwards
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_fee = *per_channel_ever_fee_sat
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_fee_incom = *per_channel_ever_incoming_fee_sat
            .get(&short_channel_id)
            .unwrap_or(&0i64);

        let ever_forw_in_out = ever_forw_fee + ever_forw_fee_incom.abs() as u64;

        // gain is millisat "gained" per block, a millisat is gained is it an effective fee from outgoing forward, but also if it's an ineffective fee as incoming forward.
        let gain = ((ever_forw_in_out as f64 / (current_block - channel.block_born) as f64)
            * 1000.0) as u64;

        let ever_forw_in = *per_channel_forwards_in
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_out = *per_channel_forwards_out
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forward_in_out = ever_forw_out + ever_forw_in;

        let is_sink_perc = channel.is_sink_perc();
        let is_sink_last_month_perc = channel.is_sink_last_month_perc();
        let alias_or_id = channel.alias_or_id();

        if let Some(l) = calc_slingjobs(
            short_channel_id.clone(),
            fund.perc_float(),
            ever_forward_in_out,
            &alias_or_id,
            &channel,
            &pull_in,
            &push_out,
        ) {
            sling_lines.push(l);
        }

        let push_pull = if push_out.contains(&short_channel_id) {
            "push"
        } else if pull_in.contains(&short_channel_id) {
            "pull"
        } else {
            ""
        };

        let s = format!(
            "{min_max:>12} {our_base_fee:1} {our_fee:>5} {short_channel_id:>15} {amount:8} {perc:>3}% {their_fee:>5} {their_base_fee:>3} {last_timestamp_delta:>3} {last_update_delta:>3} {ever_forw:>3} {ever_forw_fee:>5}s {ever_forw_fee_incom:>5}s {gain:>5}g {is_sink_perc:>4} {is_sink_last_month_perc:>4}  {push_pull:4}  {alias_or_id}"
        );
        lines.push((perc, s, cmd));
    }

    let sum_perces: f64 = perces.iter().sum();
    let mean_perces = sum_perces / perces.len() as f64;
    let quad_diff_perces: f64 = perces
        .iter()
        .map(|e| (mean_perces - e) * (mean_perces - e))
        .sum();
    let variance = quad_diff_perces / (perces.len() as f64 - 1.0);
    println!(
        "mean_perces:{:.1} variance:{:.1}",
        mean_perces * 100.0,
        variance * 100.0
    );

    lines.sort_by(|a, b| a.0.cmp(&b.0));
    println!("min_max our_base_fee our_fee scid amount perc their_fee their_base_fee last_tstamp_delta last_upd_delta monthly_forw monthly_forw_fee is_sink push/pull alias_or_id");

    for (_, l1, _) in lines.iter() {
        println!("{l1}");
    }

    for (_, _, l2) in lines {
        if let Some(l) = l2 {
            println!("{l}");
        }
    }

    // Display sling jobs without executing
    for (cmd, details) in sling_lines.iter() {
        println!("`{cmd}` {details}");
    }
}
