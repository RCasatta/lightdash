use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::cmd::{self, ClosedChannel, Forward, Fund};
use crate::history;
use crate::snapshot_metadata::{build_dataset_metadata, DatasetCounts, DatasetMetadata};
use crate::store::{RebalancePart, Store};

pub(crate) const SCHEMA_VERSION: u32 = 9;

#[derive(Deserialize, Serialize)]
pub(crate) struct SnapshotManifest {
    pub schema_version: u32,
    pub generated_at: String,
    pub node_id: String,
    pub block_height: u64,
    pub files: SnapshotFiles,
    pub datasets: BTreeMap<String, DatasetMetadata>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct SnapshotFiles {
    pub summary: String,
    pub channels: String,
    pub closed_channels: String,
    pub settled_forwards: String,
    pub other_forwards: String,
    pub rebalances: String,
    pub rebalance_status: String,
    pub history_manifest: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct SummarySnapshot {
    pub node_id: String,
    pub block_height: u64,
    pub peer_count: usize,
    pub network_channel_count: usize,
    pub current_channel_count: usize,
    pub normal_channel_count: usize,
    pub closed_channel_count: usize,
    pub forward_attempt_count: usize,
    pub settled_forward_count: usize,
    pub onchain_balance_msat: u64,
    pub channel_funds_sat: u64,
    pub total_forwarding_fees_sat: u64,
    pub total_rebalance_cost_msat: u64,
    pub net_routing_revenue_msat: i64,
    pub roic: RoicSnapshot,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct RoicSnapshot {
    pub periods: Vec<RoicPeriodSnapshot>,
    pub routed_12_months_sat: u64,
    pub capital_velocity_12_months: f64,
    pub effective_fee_rate_12_months_bps: f64,
    pub rebalance_cost_12_months_msat: u64,
    pub net_roic_12_months_percent: f64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct RoicPeriodSnapshot {
    pub months: i64,
    pub forwarding_fees_sat: u64,
    pub annualized_gross_roic_percent: f64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ChannelSnapshot {
    pub channel_id: String,
    pub short_channel_id: Option<String>,
    pub funding_txid: String,
    pub funding_output: u32,
    pub peer_id: String,
    pub peer_alias: String,
    pub connected: bool,
    pub state: String,
    pub is_normal: bool,
    pub capacity_msat: u64,
    pub local_balance_msat: u64,
    pub local_balance_percent: Option<f64>,
    pub age_days: Option<i64>,
    pub uptime_ratio: Option<f64>,
    pub outbound_fee_ppm: Option<u64>,
    pub inbound_fee_ppm: Option<u64>,
    pub outbound_base_fee_msat: Option<u64>,
    pub outbound_htlc_min_msat: Option<u64>,
    pub outbound_htlc_max_msat: Option<u64>,
    pub outbound_delay_blocks: Option<u64>,
    pub last_fee_adjustment_at: Option<String>,
    pub settled_forward_count: usize,
    pub routed_out_sat: u64,
    pub forwarding_fees_sat: u64,
    pub indirect_fees_sat: u64,
    pub historical_effective_fee_ppm: Option<f64>,
    pub time_decayed_variable_fee_ppm: Option<f64>,
    pub rebalance_target_cost_msat: u64,
    pub rebalance_target_credit_msat: u64,
    pub rebalance_effective_fee_ppm: Option<f64>,
    pub rebalance_source_cost_msat: u64,
    pub net_routing_revenue_msat: i64,
    pub gross_capacity_return_percent: Option<f64>,
    pub net_capacity_return_percent: Option<f64>,
    pub indirect_capacity_contribution_percent: Option<f64>,
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
    net_capacity_return_percent: Option<f64>,
    indirect_capacity_contribution_percent: Option<f64>,
}

#[derive(Serialize)]
struct ForwardSnapshot<'a> {
    in_channel: &'a str,
    out_channel: Option<&'a str>,
    in_peer_id: Option<String>,
    in_peer_alias: Option<String>,
    out_peer_id: Option<String>,
    out_peer_alias: Option<String>,
    status: &'a str,
    in_msat: u64,
    out_msat: Option<u64>,
    fee_msat: Option<u64>,
    fee_ppm: Option<f64>,
    received_at: Option<String>,
    resolved_at: Option<String>,
    elapsed_seconds: Option<f64>,
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
    fee_ppm: Option<f64>,
    target_historical_fee_ppm: Option<f64>,
    timestamp: Option<u64>,
    resolved_at: Option<String>,
}

#[derive(Deserialize)]
struct RawRebalanceStatus {
    alias: String,
    #[serde(default)]
    last_channel_partner: Option<String>,
    last_route_taken: String,
    last_success_reb: String,
    pubkey: String,
    rebamount: String,
    scid: String,
    status: Vec<String>,
    w_feeppm: u64,
}

#[derive(Serialize)]
struct RebalanceStatusSnapshot {
    short_channel_id: String,
    peer_id: String,
    peer_alias: String,
    last_channel_partner_id: Option<String>,
    statuses: Vec<String>,
    is_balanced: bool,
    has_no_cheap_route: bool,
    rebalance_amount_sat: u64,
    weighted_fee_ppm: u64,
    last_route_at: Option<String>,
    last_success_at: Option<String>,
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

pub fn run_snapshot(
    store: &Store,
    directory: &str,
    history_directory: Option<&str>,
    without_history: bool,
) -> io::Result<()> {
    let directory = Path::new(directory);
    fs::create_dir_all(directory)?;

    let mut files = SnapshotFiles {
        summary: "summary.json".to_string(),
        channels: "channels.json".to_string(),
        closed_channels: "closed-channels.json".to_string(),
        settled_forwards: "settled-forwards.jsonl".to_string(),
        other_forwards: "other-forwards.jsonl".to_string(),
        rebalances: "rebalances.jsonl".to_string(),
        rebalance_status: "rebalance-status.json".to_string(),
        history_manifest: None,
    };
    let rebalance_status = build_rebalance_status_snapshot()?;
    let settled_forward_count = store.settled_forwards().len();
    let mut datasets = build_dataset_metadata(
        &files,
        DatasetCounts {
            channels: store.funds.channels.len(),
            closed_channels: store.closed_channels.closedchannels.len(),
            settled_forwards: settled_forward_count,
            other_forwards: store.forwards_len() - settled_forward_count,
            rebalances: store.rebalance_parts().count(),
            rebalance_status: rebalance_status.len(),
        },
    );
    let include_history =
        !(without_history || cmd::using_test_data() && history_directory.is_none());
    if include_history {
        let imported = history::import_for_snapshot(directory, history_directory, &store.info.id)
            .map_err(io::Error::other)?;
        files.history_manifest = Some(imported.manifest_file);
        for (name, metadata) in imported.datasets {
            if datasets.insert(name.clone(), metadata).is_some() {
                return Err(io::Error::other(format!(
                    "history dataset `{name}` conflicts with a snapshot dataset"
                )));
            }
        }
    } else if without_history {
        log::info!("Processed history omitted by --without-history");
    } else {
        log::info!("Processed history omitted in test-data mode");
    }
    let generated_at = format_datetime(store.snapshot_time());
    let manifest = SnapshotManifest {
        schema_version: SCHEMA_VERSION,
        generated_at,
        node_id: store.info.id.clone(),
        block_height: store.info.blockheight,
        files,
        datasets,
    };
    write_json(directory.join("manifest.json"), &manifest)?;
    for dataset in manifest.datasets.values() {
        write_json(directory.join(&dataset.schema_path), dataset)?;
    }

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
        directory.join("settled-forwards.jsonl"),
        store
            .forwards
            .forwards
            .iter()
            .filter(|forward| forward.status == "settled")
            .map(|forward| build_forward_snapshot(store, forward)),
    )?;
    write_json_lines(
        directory.join("other-forwards.jsonl"),
        store
            .forwards
            .forwards
            .iter()
            .filter(|forward| forward.status != "settled")
            .map(|forward| build_forward_snapshot(store, forward)),
    )?;
    write_json_lines(
        directory.join("rebalances.jsonl"),
        store
            .rebalance_parts()
            .map(|part| build_rebalance_snapshot(store, part)),
    )?;
    write_json(directory.join("rebalance-status.json"), &rebalance_status)?;

    log::info!("Snapshot generated successfully in {}", directory.display());
    Ok(())
}

fn build_summary(store: &Store) -> SummarySnapshot {
    let roic = store.get_roic_data();
    SummarySnapshot {
        node_id: store.info.id.clone(),
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
        funding_txid: channel.funding_txid.clone(),
        funding_output: channel.funding_output,
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
        outbound_base_fee_msat: short_channel_id
            .and_then(|scid| store.get_channel(scid, &store.info.id))
            .map(|network_channel| network_channel.base_fee_millisatoshi),
        outbound_htlc_min_msat: short_channel_id
            .and_then(|scid| store.get_channel(scid, &store.info.id))
            .map(|network_channel| network_channel.htlc_minimum_msat),
        outbound_htlc_max_msat: short_channel_id
            .and_then(|scid| store.get_channel(scid, &store.info.id))
            .map(|network_channel| network_channel.htlc_maximum_msat),
        outbound_delay_blocks: short_channel_id
            .and_then(|scid| store.get_channel(scid, &store.info.id))
            .map(|network_channel| network_channel.delay),
        last_fee_adjustment_at: short_channel_id
            .and_then(|scid| store.get_setchannel_timestamp(scid))
            .and_then(|timestamp| u64::try_from(timestamp).ok())
            .and_then(format_timestamp),
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
        gross_capacity_return_percent: annualized_capacity_return_percent(
            gross_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        net_capacity_return_percent: annualized_capacity_return_percent(
            net_routing_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        indirect_capacity_contribution_percent: annualized_capacity_return_percent(
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

fn annualized_capacity_return_percent(
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
        net_capacity_return_percent: annualized_capacity_return_percent(
            forwarding_fees_sat as i64 * 1000 - rebalance_target_cost_msat as i64,
            channel.total_msat,
            age_days,
        ),
        indirect_capacity_contribution_percent: annualized_capacity_return_percent(
            indirect_fees_sat as i64 * 1000,
            channel.total_msat,
            age_days,
        ),
    }
}

fn build_forward_snapshot<'a>(store: &Store, forward: &'a Forward) -> ForwardSnapshot<'a> {
    let fee_ppm = match (forward.fee_msat, forward.out_msat) {
        (Some(fee_msat), Some(out_msat)) if out_msat > 0 => {
            Some(fee_msat as f64 * 1_000_000.0 / out_msat as f64)
        }
        _ => None,
    };
    let elapsed_seconds = forward
        .resolved_time
        .map(|resolved_time| resolved_time - forward.received_time);
    let (in_peer_id, in_peer_alias) = forward_peer(store, &forward.in_channel);
    let (out_peer_id, out_peer_alias) = forward
        .out_channel
        .as_deref()
        .map(|short_channel_id| forward_peer(store, short_channel_id))
        .unwrap_or((None, None));

    ForwardSnapshot {
        in_channel: &forward.in_channel,
        out_channel: forward.out_channel.as_deref(),
        in_peer_id,
        in_peer_alias,
        out_peer_id,
        out_peer_alias,
        status: &forward.status,
        in_msat: forward.in_msat,
        out_msat: forward.out_msat,
        fee_msat: forward.fee_msat,
        fee_ppm,
        received_at: format_unix_seconds(forward.received_time),
        resolved_at: forward.resolved_time.and_then(format_unix_seconds),
        elapsed_seconds,
        fail_reason: forward.failreason.as_deref(),
        fail_code: forward.failcode,
    }
}

fn forward_peer(store: &Store, short_channel_id: &str) -> (Option<String>, Option<String>) {
    let peer_id = store
        .get_fund(short_channel_id)
        .map(|channel| channel.peer_id.clone())
        .or_else(|| {
            store
                .closed_channels
                .closedchannels
                .iter()
                .find(|channel| channel.short_channel_id.as_deref() == Some(short_channel_id))
                .and_then(|channel| channel.peer_id.clone())
        })
        .or_else(|| {
            store
                .get_channel(short_channel_id, &store.info.id)
                .map(|channel| channel.destination.clone())
        });
    let peer_alias = peer_id
        .as_deref()
        .map(|peer_id| store.get_node_alias(peer_id));
    (peer_id, peer_alias)
}

fn build_rebalance_snapshot<'a>(store: &Store, part: &'a RebalancePart) -> RebalanceSnapshot<'a> {
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
        fee_ppm: ratio_ppm(part.fees_msat as f64, part.credit_msat as f64),
        target_historical_fee_ppm: part
            .target_channel_id
            .as_deref()
            .and_then(|scid| store.get_channel_effective_fee_ppm(scid)),
        timestamp: part.timestamp,
        resolved_at: part.timestamp.and_then(format_timestamp),
    }
}

fn build_rebalance_status_snapshot() -> io::Result<Vec<RebalanceStatusSnapshot>> {
    let raw: Vec<RawRebalanceStatus> = serde_json::from_value(crate::sling::current_sling_stats())
        .map_err(|e| io::Error::other(format!("parsing current Sling status failed: {e}")))?;

    raw.into_iter()
        .map(|entry| {
            let rebalance_amount_sat =
                entry
                    .rebamount
                    .replace(',', "")
                    .parse::<u64>()
                    .map_err(|e| {
                        io::Error::other(format!(
                            "parsing Sling rebalance amount `{}` failed: {e}",
                            entry.rebamount
                        ))
                    })?;
            let is_balanced = entry
                .status
                .iter()
                .any(|status| status.contains("Balanced"));
            let has_no_cheap_route = entry
                .status
                .iter()
                .any(|status| status.contains("NoCheapRoute"));
            Ok(RebalanceStatusSnapshot {
                short_channel_id: entry.scid,
                peer_id: entry.pubkey,
                peer_alias: entry.alias,
                last_channel_partner_id: entry.last_channel_partner,
                statuses: entry.status,
                is_balanced,
                has_no_cheap_route,
                rebalance_amount_sat,
                weighted_fee_ppm: entry.w_feeppm,
                last_route_at: parse_sling_timestamp(&entry.last_route_taken),
                last_success_at: parse_sling_timestamp(&entry.last_success_reb),
            })
        })
        .collect()
}

fn parse_sling_timestamp(value: &str) -> Option<String> {
    if value == "Never" {
        return None;
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|timestamp| format_datetime(timestamp.and_utc()))
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
    use super::{annualized_capacity_return_percent, ratio_ppm};

    #[test]
    fn ratio_ppm_returns_none_for_no_volume() {
        assert_eq!(ratio_ppm(10.0, 0.0), None);
        assert_eq!(ratio_ppm(10.0, 1_000.0), Some(10_000.0));
    }

    #[test]
    fn annualized_capacity_return_preserves_negative_net_revenue() {
        assert_eq!(
            annualized_capacity_return_percent(-1_000, 100_000, Some(365)),
            Some(-1.0)
        );
    }

    #[test]
    fn annualized_capacity_return_is_null_without_channel_age() {
        assert_eq!(
            annualized_capacity_return_percent(1_000, 100_000, None),
            None
        );
    }
}
