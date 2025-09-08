use chrono::Utc;
use std::collections::HashMap;

use crate::cmd::*;
use crate::common::*;

pub fn run_sling() {
    let now = Utc::now();
    println!("{}", now);
    let info = get_info();
    let _current_block = info.blockheight;

    let channels = list_channels();
    let nodes = list_nodes();
    let peers = list_peers();

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
    let settled: Vec<_> = forwards
        .forwards
        .into_iter()
        .filter(|e| e.status == "settled")
        .map(|e| SettledForward::try_from(e).unwrap())
        .collect();

    let mut per_channel_forwards_in: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_in_last_month: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out_last_month: HashMap<String, u64> = HashMap::new();

    for s in settled.iter() {
        let days_elapsed = now.signed_duration_since(s.resolved_time).num_days();
        *per_channel_forwards_in
            .entry(s.in_channel.to_string())
            .or_default() += 1;
        *per_channel_forwards_out
            .entry(s.out_channel.to_string())
            .or_default() += 1;

        if days_elapsed < 30 {
            *per_channel_forwards_in_last_month
                .entry(s.in_channel.to_string())
                .or_default() += 1;
            *per_channel_forwards_out_last_month
                .entry(s.out_channel.to_string())
                .or_default() += 1;
        }
    }

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

    let mut sling_lines = vec![];

    for channel in channels {
        let fund = &channel.fund;
        let short_channel_id = fund.short_channel_id();

        let ever_forw_in = *per_channel_forwards_in
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_out = *per_channel_forwards_out
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forward_in_out = ever_forw_out + ever_forw_in;
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
    }

    // Execute sling jobs
    let result = cmd_result("lightning-cli", &["sling-deletejob", "all"]);
    println!("{result}");
    for (cmd, details) in sling_lines.iter() {
        println!("`{cmd}` {details}");
        let split: Vec<&str> = cmd.split(' ').collect();
        let result = cmd_result(split[0], &split[1..]);
        println!("{result}");
    }
    let result = cmd_result("lightning-cli", &["sling-go"]);
    println!("{result}");
}
