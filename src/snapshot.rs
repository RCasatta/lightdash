use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;

use crate::cmd::{ClosedChannel, Forward, Fund};
use crate::store::{RebalancePart, Store};

const SCHEMA_VERSION: u32 = 1;

#[derive(Serialize)]
struct SnapshotManifest<'a> {
    schema_version: u32,
    generated_at: String,
    node_id: &'a str,
    block_height: u64,
    files: SnapshotFiles,
}

#[derive(Serialize)]
struct SnapshotFiles {
    summary: &'static str,
    channels: &'static str,
    closed_channels: &'static str,
    forwards: &'static str,
    rebalances: &'static str,
}

#[derive(Serialize)]
struct SummarySnapshot<'a> {
    node_id: &'a str,
    block_height: u64,
    peer_count: usize,
    network_channel_count: usize,
    current_channel_count: usize,
    normal_channel_count: usize,
    closed_channel_count: usize,
    forward_attempt_count: usize,
    settled_forward_count: usize,
    onchain_balance_msat: u64,
    channel_funds_sat: u64,
    total_forwarding_fees_sat: u64,
    total_rebalance_cost_msat: u64,
    net_routing_revenue_msat: i64,
    roic: RoicSnapshot,
}

#[derive(Serialize)]
struct RoicSnapshot {
    periods: Vec<RoicPeriodSnapshot>,
    routed_12_months_sat: u64,
    capital_velocity_12_months: f64,
    effective_fee_rate_12_months_bps: f64,
    rebalance_cost_12_months_msat: u64,
    net_roic_12_months_percent: f64,
}

#[derive(Serialize)]
struct RoicPeriodSnapshot {
    months: i64,
    forwarding_fees_sat: u64,
    annualized_gross_roic_percent: f64,
}

#[derive(Serialize)]
struct ChannelSnapshot {
    channel_id: String,
    short_channel_id: Option<String>,
    peer_id: String,
    peer_alias: String,
    connected: bool,
    state: String,
    is_normal: bool,
    capacity_msat: u64,
    local_balance_msat: u64,
    local_balance_percent: Option<f64>,
    age_days: Option<i64>,
    uptime_ratio: Option<f64>,
    outbound_fee_ppm: Option<u64>,
    inbound_fee_ppm: Option<u64>,
    settled_forward_count: usize,
    routed_out_sat: u64,
    forwarding_fees_sat: u64,
    indirect_fees_sat: u64,
    historical_effective_fee_ppm: Option<f64>,
    time_decayed_variable_fee_ppm: Option<f64>,
    rebalance_target_cost_msat: u64,
    rebalance_target_credit_msat: u64,
    rebalance_effective_fee_ppm: Option<f64>,
    rebalance_source_cost_msat: u64,
    net_routing_revenue_msat: i64,
    gross_roic_percent: Option<f64>,
    net_roic_percent: Option<f64>,
    indirect_roic_percent: Option<f64>,
}

#[derive(Serialize)]
struct ClosedChannelSnapshot {
    channel_id: String,
    short_channel_id: Option<String>,
    peer_id: Option<String>,
    peer_alias: Option<String>,
    opener: String,
    closer: Option<String>,
    capacity_msat: u64,
    final_local_balance_msat: u64,
    total_htlcs_sent: Option<u64>,
    funding_txid: String,
    last_commitment_txid: Option<String>,
    last_stable_connection_at: Option<String>,
    close_cause: String,
    age_days: Option<i64>,
    net_roic_percent: Option<f64>,
    indirect_roic_percent: Option<f64>,
}

#[derive(Serialize)]
struct ForwardSnapshot<'a> {
    in_channel: &'a str,
    out_channel: Option<&'a str>,
    status: &'a str,
    in_msat: u64,
    out_msat: Option<u64>,
    fee_msat: Option<u64>,
    received_at: Option<String>,
    resolved_at: Option<String>,
    fail_reason: Option<&'a str>,
    fail_code: Option<u32>,
}

#[derive(Serialize)]
struct RebalanceSnapshot<'a> {
    payment_id: &'a str,
    part_id: u64,
    source_account: &'a str,
    target_account: &'a str,
    source_channel_id: Option<&'a str>,
    target_channel_id: Option<&'a str>,
    debit_msat: u64,
    credit_msat: u64,
    fees_msat: u64,
    timestamp: Option<u64>,
    resolved_at: Option<String>,
}

#[derive(Default)]
struct ChannelForwardMetrics {
    settled_forward_count: usize,
    routed_out_sat: u64,
    forwarding_fees_sat: u64,
    indirect_fees_sat: u64,
    weighted_variable_fees_sat: f64,
    weighted_routed_out_sat: f64,
}

#[derive(Default)]
struct ChannelRebalanceMetrics {
    target_cost_msat: u64,
    target_credit_msat: u64,
    source_cost_msat: u64,
}

pub fn run_snapshot(store: &Store, directory: &str) -> io::Result<()> {
    let directory = Path::new(directory);
    fs::create_dir_all(directory)?;

    let generated_at = format_datetime(store.snapshot_time());
    let manifest = SnapshotManifest {
        schema_version: SCHEMA_VERSION,
        generated_at,
        node_id: &store.info.id,
        block_height: store.info.blockheight,
        files: SnapshotFiles {
            summary: "summary.json",
            channels: "channels.json",
            closed_channels: "closed-channels.json",
            forwards: "forwards.jsonl",
            rebalances: "rebalances.jsonl",
        },
    };
    write_json(directory.join("manifest.json"), &manifest)?;

    let summary = build_summary(store);
    write_json(directory.join("summary.json"), &summary)?;

    let forward_metrics = aggregate_channel_forwards(store);
    let rebalance_metrics = aggregate_channel_rebalances(store);
    let channels: Vec<_> = store
        .funds
        .channels
        .iter()
        .map(|channel| build_channel_snapshot(store, channel, &forward_metrics, &rebalance_metrics))
        .collect();
    write_json(directory.join("channels.json"), &channels)?;

    let closed_channels: Vec<_> = store
        .closed_channels
        .closedchannels
        .iter()
        .map(|channel| {
            build_closed_channel_snapshot(store, channel, &forward_metrics, &rebalance_metrics)
        })
        .collect();
    write_json(directory.join("closed-channels.json"), &closed_channels)?;

    write_json_lines(
        directory.join("forwards.jsonl"),
        store.forwards.forwards.iter().map(build_forward_snapshot),
    )?;
    write_json_lines(
        directory.join("rebalances.jsonl"),
        store.rebalance_parts().map(build_rebalance_snapshot),
    )?;

    log::info!("Snapshot generated successfully in {}", directory.display());
    Ok(())
}

fn build_summary(store: &Store) -> SummarySnapshot<'_> {
    let roic = store.get_roic_data();
    SummarySnapshot {
        node_id: &store.info.id,
        block_height: store.info.blockheight,
        peer_count: store.peers.peers.len(),
        network_channel_count: store.channels_len(),
        current_channel_count: store.funds.channels.len(),
        normal_channel_count: store.normal_channels().len(),
        closed_channel_count: store.closed_channels.closedchannels.len(),
        forward_attempt_count: store.forwards_len(),
        settled_forward_count: store.settled_forwards().len(),
        onchain_balance_msat: store
            .funds
            .outputs
            .iter()
            .map(|output| output.amount_msat)
            .sum(),
        channel_funds_sat: roic.total_funds,
        total_forwarding_fees_sat: store.total_forwarding_fees_sat(),
        total_rebalance_cost_msat: store.total_rebalance_cost_msat(),
        net_routing_revenue_msat: store.net_routing_revenue_msat(),
        roic: RoicSnapshot {
            periods: [
                (1, roic.fees_1_month, roic.gross_roic_1_month),
                (3, roic.fees_3_months, roic.gross_roic_3_months),
                (6, roic.fees_6_months, roic.gross_roic_6_months),
                (12, roic.fees_12_months, roic.gross_roic_12_months),
            ]
            .into_iter()
            .map(
                |(months, forwarding_fees_sat, annualized_gross_roic_percent)| RoicPeriodSnapshot {
                    months,
                    forwarding_fees_sat,
                    annualized_gross_roic_percent,
                },
            )
            .collect(),
            routed_12_months_sat: roic.routed_12_months,
            capital_velocity_12_months: roic.capital_velocity_12_months,
            effective_fee_rate_12_months_bps: roic.effective_fee_rate_12_months_bps,
            rebalance_cost_12_months_msat: roic.rebalance_cost_12_months_msat,
            net_roic_12_months_percent: roic.net_roic_12_months,
        },
    }
}

fn build_channel_snapshot(
    store: &Store,
    channel: &Fund,
    forward_metrics: &HashMap<String, ChannelForwardMetrics>,
    rebalance_metrics: &HashMap<String, ChannelRebalanceMetrics>,
) -> ChannelSnapshot {
    let short_channel_id = channel.short_channel_id.as_deref();
    let empty_forwards = ChannelForwardMetrics::default();
    let empty_rebalances = ChannelRebalanceMetrics::default();
    let forwards = short_channel_id
        .and_then(|scid| forward_metrics.get(scid))
        .unwrap_or(&empty_forwards);
    let rebalances = short_channel_id
        .and_then(|scid| rebalance_metrics.get(scid))
        .unwrap_or(&empty_rebalances);
    let age_days = short_channel_id.and_then(|scid| store.get_channel_age_days(scid));
    let gross_revenue_msat = forwards.forwarding_fees_sat as i64 * 1000;
    let net_routing_revenue_msat = gross_revenue_msat - rebalances.target_cost_msat as i64;
    let indirect_revenue_msat = forwards.indirect_fees_sat as i64 * 1000;

    ChannelSnapshot {
        channel_id: channel.channel_id.clone(),
        short_channel_id: channel.short_channel_id.clone(),
        peer_id: channel.peer_id.clone(),
        peer_alias: store.get_node_alias(&channel.peer_id),
        connected: channel.connected,
        state: channel.state.clone(),
        is_normal: channel.state == "CHANNELD_NORMAL",
        capacity_msat: channel.amount_msat,
        local_balance_msat: channel.our_amount_msat,
        local_balance_percent: if channel.amount_msat == 0 {
            None
        } else {
            Some(channel.perc_float() * 100.0)
        },
        age_days,
        uptime_ratio: store.avail_map.get(&channel.peer_id).copied(),
        outbound_fee_ppm: short_channel_id
            .and_then(|scid| store.get_channel(scid, &store.info.id))
            .map(|network_channel| network_channel.fee_per_millionth),
        inbound_fee_ppm: short_channel_id
            .and_then(|scid| store.get_channel(scid, &channel.peer_id))
            .map(|network_channel| network_channel.fee_per_millionth),
        settled_forward_count: forwards.settled_forward_count,
        routed_out_sat: forwards.routed_out_sat,
        forwarding_fees_sat: forwards.forwarding_fees_sat,
        indirect_fees_sat: forwards.indirect_fees_sat,
        historical_effective_fee_ppm: ratio_ppm(
            forwards.forwarding_fees_sat as f64,
            forwards.routed_out_sat as f64,
        ),
        time_decayed_variable_fee_ppm: ratio_ppm(
            forwards.weighted_variable_fees_sat,
            forwards.weighted_routed_out_sat,
        ),
        rebalance_target_cost_msat: rebalances.target_cost_msat,
        rebalance_target_credit_msat: rebalances.target_credit_msat,
        rebalance_effective_fee_ppm: ratio_ppm(
            rebalances.target_cost_msat as f64,
            rebalances.target_credit_msat as f64,
        ),
        rebalance_source_cost_msat: rebalances.source_cost_msat,
        net_routing_revenue_msat,
        gross_roic_percent: annualized_roic_percent(
            gross_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        net_roic_percent: annualized_roic_percent(
            net_routing_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        indirect_roic_percent: annualized_roic_percent(
            indirect_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
    }
}

fn aggregate_channel_forwards(store: &Store) -> HashMap<String, ChannelForwardMetrics> {
    const HALF_LIFE_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
    const OUR_BASE_FEE_SAT: u64 = 1;

    let mut metrics: HashMap<String, ChannelForwardMetrics> = HashMap::new();
    for forward in store.settled_forwards() {
        let incoming = metrics.entry(forward.in_channel.clone()).or_default();
        incoming.settled_forward_count += 1;
        incoming.indirect_fees_sat += forward.fee_sat;

        let outgoing = metrics.entry(forward.out_channel.clone()).or_default();
        if forward.out_channel != forward.in_channel {
            outgoing.settled_forward_count += 1;
        }
        outgoing.routed_out_sat += forward.out_sat;
        outgoing.forwarding_fees_sat += forward.fee_sat;

        let age_seconds = store
            .snapshot_time()
            .signed_duration_since(forward.resolved_time)
            .num_seconds()
            .max(0) as f64;
        let decay = 0.5_f64.powf(age_seconds / HALF_LIFE_SECONDS);
        outgoing.weighted_variable_fees_sat +=
            forward.fee_sat.saturating_sub(OUR_BASE_FEE_SAT) as f64 * decay;
        outgoing.weighted_routed_out_sat += forward.out_sat as f64 * decay;
    }
    metrics
}

fn aggregate_channel_rebalances(store: &Store) -> HashMap<String, ChannelRebalanceMetrics> {
    let mut metrics: HashMap<String, ChannelRebalanceMetrics> = HashMap::new();
    for part in store.rebalance_parts() {
        if let Some(target_channel_id) = &part.target_channel_id {
            let target = metrics.entry(target_channel_id.clone()).or_default();
            target.target_cost_msat += part.fees_msat;
            target.target_credit_msat += part.credit_msat;
        }
        if let Some(source_channel_id) = &part.source_channel_id {
            metrics
                .entry(source_channel_id.clone())
                .or_default()
                .source_cost_msat += part.fees_msat;
        }
    }
    metrics
}

fn ratio_ppm(numerator: f64, denominator: f64) -> Option<f64> {
    if denominator == 0.0 {
        None
    } else {
        Some(numerator * 1_000_000.0 / denominator)
    }
}

fn annualized_roic_percent(
    revenue_msat: i64,
    capacity_msat: u64,
    age_days: Option<i64>,
) -> Option<f64> {
    let age_days = age_days?;
    if age_days <= 0 || capacity_msat == 0 {
        return Some(0.0);
    }

    Some((revenue_msat as f64 / capacity_msat as f64) * (365.0 / age_days as f64) * 100.0)
}

fn build_closed_channel_snapshot(
    store: &Store,
    channel: &ClosedChannel,
    forward_metrics: &HashMap<String, ChannelForwardMetrics>,
    rebalance_metrics: &HashMap<String, ChannelRebalanceMetrics>,
) -> ClosedChannelSnapshot {
    let short_channel_id = channel.short_channel_id.as_deref();
    let forwarding_fees_sat = short_channel_id
        .and_then(|scid| forward_metrics.get(scid))
        .map(|metrics| metrics.forwarding_fees_sat)
        .unwrap_or_default();
    let indirect_fees_sat = short_channel_id
        .and_then(|scid| forward_metrics.get(scid))
        .map(|metrics| metrics.indirect_fees_sat)
        .unwrap_or_default();
    let rebalance_target_cost_msat = short_channel_id
        .and_then(|scid| rebalance_metrics.get(scid))
        .map(|metrics| metrics.target_cost_msat)
        .unwrap_or_default();
    let age_days = store.get_closed_channel_age_days(channel);

    ClosedChannelSnapshot {
        channel_id: channel.channel_id.clone(),
        short_channel_id: channel.short_channel_id.clone(),
        peer_id: channel.peer_id.clone(),
        peer_alias: channel
            .peer_id
            .as_deref()
            .map(|peer_id| store.get_node_alias(peer_id)),
        opener: channel.opener.clone(),
        closer: channel.closer.clone(),
        capacity_msat: channel.total_msat,
        final_local_balance_msat: channel.final_to_us_msat,
        total_htlcs_sent: channel.total_htlcs_sent,
        funding_txid: channel.funding_txid.clone(),
        last_commitment_txid: channel.last_commitment_txid.clone(),
        last_stable_connection_at: channel.last_stable_connection.and_then(format_timestamp),
        close_cause: channel.close_cause.clone(),
        age_days,
        net_roic_percent: annualized_roic_percent(
            forwarding_fees_sat as i64 * 1000 - rebalance_target_cost_msat as i64,
            channel.total_msat,
            age_days,
        ),
        indirect_roic_percent: annualized_roic_percent(
            indirect_fees_sat as i64 * 1000,
            channel.total_msat,
            age_days,
        ),
    }
}

fn build_forward_snapshot(forward: &Forward) -> ForwardSnapshot<'_> {
    ForwardSnapshot {
        in_channel: &forward.in_channel,
        out_channel: forward.out_channel.as_deref(),
        status: &forward.status,
        in_msat: forward.in_msat,
        out_msat: forward.out_msat,
        fee_msat: forward.fee_msat,
        received_at: format_unix_seconds(forward.received_time),
        resolved_at: forward.resolved_time.and_then(format_unix_seconds),
        fail_reason: forward.failreason.as_deref(),
        fail_code: forward.failcode,
    }
}

fn build_rebalance_snapshot(part: &RebalancePart) -> RebalanceSnapshot<'_> {
    RebalanceSnapshot {
        payment_id: &part.payment_id,
        part_id: part.part_id,
        source_account: &part.source_account,
        target_account: &part.target_account,
        source_channel_id: part.source_channel_id.as_deref(),
        target_channel_id: part.target_channel_id.as_deref(),
        debit_msat: part.debit_msat,
        credit_msat: part.credit_msat,
        fees_msat: part.fees_msat,
        timestamp: part.timestamp,
        resolved_at: part.timestamp.and_then(format_timestamp),
    }
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value).map_err(io::Error::other)?;
    writer.write_all(b"\n")
}

fn write_json_lines<T: Serialize>(
    path: impl AsRef<Path>,
    values: impl IntoIterator<Item = T>,
) -> io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for value in values {
        serde_json::to_writer(&mut writer, &value).map_err(io::Error::other)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn format_unix_seconds(timestamp: f64) -> Option<String> {
    DateTime::from_timestamp(timestamp as i64, 0).map(format_datetime)
}

fn format_timestamp(timestamp: u64) -> Option<String> {
    DateTime::from_timestamp(timestamp as i64, 0).map(format_datetime)
}

fn format_datetime(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::{annualized_roic_percent, ratio_ppm};

    #[test]
    fn ratio_ppm_returns_none_for_no_volume() {
        assert_eq!(ratio_ppm(10.0, 0.0), None);
        assert_eq!(ratio_ppm(10.0, 1_000.0), Some(10_000.0));
    }

    #[test]
    fn annualized_roic_preserves_negative_net_revenue() {
        assert_eq!(
            annualized_roic_percent(-1_000, 100_000, Some(365)),
            Some(-1.0)
        );
    }

    #[test]
    fn annualized_roic_is_null_without_channel_age() {
        assert_eq!(annualized_roic_percent(1_000, 100_000, None), None);
    }
}
