use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::cmd::{self, ClosedChannel, Forward, Fund};
use crate::history;
use crate::lnplus::{self, PoolNode, PoolNodesSource};
use crate::routes::{self, RouteCandidate, RouteRun};
use crate::snapshot_metadata::{
    build_dataset_metadata, lnplus_pools_dataset, route_candidate_snapshot_fields,
    route_partners_dataset, DatasetCounts, DatasetMetadata,
};
use crate::store::{RebalancePart, Store};

pub(crate) const SCHEMA_VERSION: u32 = 30;
const REBALANCE_SOURCE_90D_SECONDS: u64 = 90 * 24 * 60 * 60;

#[derive(Deserialize, Serialize)]
pub(crate) struct SnapshotManifest {
    pub schema_version: u32,
    pub generated_at: String,
    pub node_id: String,
    pub block_height: u64,
    pub files: SnapshotFiles,
    pub lnplus_pools_source: Option<PoolNodesSource>,
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
    pub routes_manifest: Option<String>,
    pub lnplus_pools: Option<String>,
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
    pub pending_channel_balance_msat: u64,
    pub pending_channel_count: usize,
    pub estimated_total_balance_msat: u64,
    pub channel_funds_sat: u64,
    pub normal_channel_capacity_sat: u64,
    pub channel_funds_percent_of_capacity: Option<f64>,
    pub channel_balance_target_stddev_percentage_points: f64,
    pub network_average_fee_ppm: f64,
    pub network_median_fee_ppm: f64,
    pub node_average_fee_ppm: f64,
    pub node_median_fee_ppm: f64,
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
    pub lease_fee_earnings_12_months_msat: u64,
    pub lease_fee_cost_12_months_msat: u64,
    pub rebalance_cost_12_months_msat: u64,
    pub net_roic_12_months_percent: f64,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct RoicPeriodSnapshot {
    pub months: i64,
    pub forwarding_fees_sat: u64,
    pub lease_fee_earnings_msat: u64,
    pub average_channel_funds_sat: f64,
    pub capital_history_coverage_ratio: f64,
    pub annualized_gross_roic_percent: f64,
}

struct AverageChannelFunds {
    sats: f64,
    coverage_ratio: f64,
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
    pub peer_supports_splicing: Option<bool>,
    pub private: Option<bool>,
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
    pub time_decayed_fee_ppm: Option<f64>,
    pub rebalance_target_cost_msat: u64,
    pub rebalance_target_credit_msat: u64,
    pub rebalance_effective_fee_ppm: Option<f64>,
    pub rebalance_source_cost_msat: u64,
    pub rebalance_source_debit_msat: u64,
    pub rebalance_source_debit_90d_msat: u64,
    pub rebalance_source_effective_fee_ppm: Option<f64>,
    pub lease_fee_earnings_msat: u64,
    pub lease_fee_cost_msat: u64,
    pub net_routing_revenue_msat: i64,
    pub net_revenue_msat: i128,
    pub gross_capacity_return_percent: Option<f64>,
    pub net_capacity_return_percent: Option<f64>,
    pub indirect_capacity_contribution_percent: Option<f64>,
    pub combined_capacity_return_percent: Option<f64>,
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
    lease_fee_earnings_msat: u64,
    lease_fee_cost_msat: u64,
    net_revenue_msat: i128,
    net_capacity_return_percent: Option<f64>,
    indirect_capacity_contribution_percent: Option<f64>,
    combined_capacity_return_percent: Option<f64>,
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
    source_peer_alias: Option<String>,
    target_peer_alias: Option<String>,
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
    last_channel_partner_alias: Option<String>,
    statuses: Vec<String>,
    is_balanced: bool,
    has_no_cheap_route: bool,
    rebalance_amount_sat: u64,
    weighted_fee_ppm: u64,
    last_route_at: Option<String>,
    last_success_at: Option<String>,
}

#[derive(Serialize)]
struct LnPlusPoolSnapshot {
    node_id: String,
    alias: Option<String>,
    credits_balance_sat: u64,
    min_channel_size_sat: u64,
    capacity_sat: u64,
    open_channels: u64,
    connection: String,
    clearnet_address: Option<String>,
    tor_address: Option<String>,
    lnp_rank: u64,
    lnp_rank_name: String,
    positive_ratings: u64,
    negative_ratings: u64,
    url: String,
    offer_open: bool,
    is_current_peer: bool,
    had_channel_in_past: bool,
}

#[derive(Serialize)]
struct RouteCandidateSnapshot {
    #[serde(flatten)]
    candidate: RouteCandidate,
    weighted_route_score: f64,
    weighted_route_rank: usize,
    #[serde(flatten)]
    lnplus: LnPlusOfferFields,
}

#[derive(Serialize)]
struct RoutePartnerSnapshot {
    node_id: String,
    alias: String,
    connectable: bool,
    channel_count: u64,
    average_fee_ppm: f64,
    fee_diversity: f64,
    weighted_route_score: f64,
    weighted_route_rank: usize,
    total_appearances: u64,
    appearances_1k_sat: u64,
    appearances_10k_sat: u64,
    appearances_100k_sat: u64,
    appearances_1m_sat: u64,
    largest_amount_sat: u64,
    past_channel_count: usize,
    past_capacity_msat: u64,
    past_lifetime_days: Option<i64>,
    past_net_revenue_msat: Option<i128>,
    past_net_capacity_return_percent: Option<f64>,
    past_local_closes: usize,
    past_remote_closes: usize,
    #[serde(flatten)]
    lnplus: LnPlusOfferFields,
}

#[derive(Serialize)]
struct LnPlusOfferFields {
    lnplus_pool_member: Option<bool>,
    lnplus_pool_offer_open: Option<bool>,
    lnplus_pool_credits_sat: Option<u64>,
    lnplus_pool_min_channel_size_sat: Option<u64>,
    lnplus_connection: Option<String>,
    lnplus_negative_ratings: Option<u64>,
    lnplus_url: Option<String>,
}

#[derive(Default)]
struct ChannelForwardMetrics {
    settled_forward_count: usize,
    routed_out_sat: u64,
    forwarding_fees_sat: u64,
    indirect_fees_sat: u64,
}

#[derive(Default)]
struct ChannelRebalanceMetrics {
    target_cost_msat: u64,
    target_credit_msat: u64,
    source_cost_msat: u64,
    source_credit_msat: u64,
    source_debit_msat: u64,
    source_debit_90d_msat: u64,
}

pub fn run_snapshot(
    store: &Store,
    directory: &str,
    history_directory: Option<&str>,
    without_history: bool,
    routes_directory: Option<&str>,
    without_routes: bool,
    without_lnplus: bool,
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
        routes_manifest: None,
        lnplus_pools: None,
    };
    let rebalance_status = build_rebalance_status_snapshot(store)?;
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
    let channel_funds_history = if include_history {
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
        imported.channel_funds
    } else if without_history {
        log::info!("Processed history omitted by --without-history");
        Vec::new()
    } else {
        log::info!("Processed history omitted in test-data mode");
        Vec::new()
    };
    let include_routes = !(without_routes || cmd::using_test_data() && routes_directory.is_none());
    if include_routes {
        let imported = routes::import_for_snapshot(store, directory, routes_directory)
            .map_err(io::Error::other)?;
        files.routes_manifest = Some(imported.manifest_file);
        for (name, metadata) in imported.datasets {
            if datasets.insert(name.clone(), metadata).is_some() {
                return Err(io::Error::other(format!(
                    "routes dataset `{name}` conflicts with a snapshot dataset"
                )));
            }
        }
    } else if without_routes {
        log::info!("Route analysis omitted by --without-routes");
    } else {
        log::info!("Route analysis omitted in test-data mode");
    }
    let (lnplus_pools, lnplus_pools_source) = if without_lnplus {
        log::info!("LN+ pool offers omitted by --without-lnplus");
        (None, None)
    } else {
        match lnplus::fetch_pool_nodes() {
            Ok((nodes, source)) => {
                let pools = build_lnplus_pools_snapshot(store, nodes);
                let path = "lnplus-pools.json".to_string();
                datasets.insert(
                    "lnplus_pools".to_string(),
                    lnplus_pools_dataset(&path, pools.len()),
                );
                write_json(directory.join(&path), &pools)?;
                files.lnplus_pools = Some(path);
                (Some(pools), Some(source))
            }
            Err(error) => {
                log::warn!("LN+ pool offers omitted because fetching them failed: {error}");
                (None, None)
            }
        }
    };
    let forward_metrics = aggregate_channel_forwards(store);
    let rebalance_metrics = aggregate_channel_rebalances(store);
    let closed_channels: Vec<_> = store
        .closed_channels
        .closedchannels
        .iter()
        .map(|channel| {
            build_closed_channel_snapshot(store, channel, &forward_metrics, &rebalance_metrics)
        })
        .collect();
    if include_routes {
        let candidates_path = directory.join(&datasets["route_candidates"].path);
        let mut candidates: Vec<RouteCandidate> = read_json(&candidates_path)?;
        let runs_path = directory.join(&datasets["route_runs"].path);
        let mut runs: Vec<RouteRun> = read_json(&runs_path)?;
        // Caches built by an older node binary may still contain retired probe amounts.
        runs.retain(|run| routes::ROUTE_AMOUNTS_SAT.contains(&run.amount_sat));
        candidates.retain(|candidate| routes::ROUTE_AMOUNTS_SAT.contains(&candidate.amount_sat));
        write_json(&runs_path, &runs)?;
        let runs_dataset = datasets
            .get_mut("route_runs")
            .expect("imported routes include route_runs");
        runs_dataset.record_count = runs.len();
        let (candidates, partners) =
            build_route_snapshots(candidates, &runs, lnplus_pools.as_deref(), &closed_channels);
        let partners_path = "route-partners.json".to_string();
        write_json(directory.join(&partners_path), &partners)?;
        datasets.insert(
            "route_partners".to_string(),
            route_partners_dataset(&partners_path, partners.len()),
        );
        let candidates_dataset = datasets
            .get_mut("route_candidates")
            .expect("imported routes include route_candidates");
        candidates_dataset.record_count = candidates.len();
        candidates_dataset
            .fields
            .extend(route_candidate_snapshot_fields());
        candidates_dataset.description.push_str(
            " Snapshot generation adds a node-level weighted route score and joins LN+ Liquidity Pool offers by node_id.",
        );
        write_json(candidates_path, &candidates)?;
    }
    let generated_at = format_datetime(store.snapshot_time());
    let manifest = SnapshotManifest {
        schema_version: SCHEMA_VERSION,
        generated_at,
        node_id: store.info.id.clone(),
        block_height: store.info.blockheight,
        lnplus_pools_source,
        files,
        datasets,
    };
    write_json(directory.join("manifest.json"), &manifest)?;
    for dataset in manifest.datasets.values() {
        write_json(directory.join(&dataset.schema_path), dataset)?;
    }

    let summary = build_summary(store, &channel_funds_history);
    write_json(directory.join("summary.json"), &summary)?;

    let channels: Vec<_> = store
        .funds
        .channels
        .iter()
        .map(|channel| build_channel_snapshot(store, channel, &forward_metrics, &rebalance_metrics))
        .collect();
    write_json(directory.join("channels.json"), &channels)?;

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

fn build_summary(
    store: &Store,
    channel_funds_history: &[history::ChannelFundsHistoryPoint],
) -> SummarySnapshot {
    let roic = store.get_roic_data();
    let periods = [
        (1, roic.fees_1_month, roic.lease_fee_earnings_1_month_msat),
        (3, roic.fees_3_months, roic.lease_fee_earnings_3_months_msat),
        (6, roic.fees_6_months, roic.lease_fee_earnings_6_months_msat),
        (
            12,
            roic.fees_12_months,
            roic.lease_fee_earnings_12_months_msat,
        ),
    ]
    .into_iter()
    .map(|(months, forwarding_fees_sat, lease_fee_earnings_msat)| {
        let average = average_channel_funds(
            channel_funds_history,
            &store.snapshot_time(),
            months,
            roic.total_funds,
        );
        let gross_revenue_sat =
            forwarding_fees_sat as f64 + lease_fee_earnings_msat as f64 / 1000.0;
        let annualized_gross_roic_percent = if average.sats == 0.0 {
            0.0
        } else {
            gross_revenue_sat * (12.0 / months as f64) / average.sats * 100.0
        };
        RoicPeriodSnapshot {
            months,
            forwarding_fees_sat,
            lease_fee_earnings_msat,
            average_channel_funds_sat: average.sats,
            capital_history_coverage_ratio: average.coverage_ratio,
            annualized_gross_roic_percent,
        }
    })
    .collect::<Vec<_>>();
    let average_12_months = periods
        .iter()
        .find(|period| period.months == 12)
        .expect("twelve-month ROIC period exists")
        .average_channel_funds_sat;
    let net_revenue_12_months_msat = roic.fees_12_months as f64 * 1000.0
        + roic.lease_fee_earnings_12_months_msat as f64
        - roic.lease_fee_cost_12_months_msat as f64
        - roic.rebalance_cost_12_months_msat as f64;
    let normal_channels = store.normal_channels();
    let normal_channel_capacity_sat = normal_channels
        .iter()
        .map(|channel| channel.amount_msat / 1000)
        .sum();
    let (network_average_fee_ppm, network_median_fee_ppm) = store.network_channel_fees();
    let (node_average_fee_ppm, node_median_fee_ppm) = store.node_channel_fees();
    let onchain_balance_msat = store
        .funds
        .outputs
        .iter()
        .map(|output| output.amount_msat)
        .sum();
    let pending_channels = store
        .funds
        .channels
        .iter()
        .filter(|channel| {
            let peer_status = store
                .get_peer_channel(&channel.channel_id)
                .map(|peer_channel| peer_channel.status.as_slice());
            is_pending_channel_balance(&channel.state, peer_status)
        })
        .collect::<Vec<_>>();
    let pending_channel_balance_msat = pending_channels
        .iter()
        .map(|channel| channel.our_amount_msat)
        .sum();
    let normal_channel_balance_msat = normal_channels
        .iter()
        .map(|channel| channel.our_amount_msat)
        .sum::<u64>();
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
        onchain_balance_msat,
        pending_channel_balance_msat,
        pending_channel_count: pending_channels.len(),
        estimated_total_balance_msat: normal_channel_balance_msat
            + onchain_balance_msat
            + pending_channel_balance_msat,
        channel_funds_sat: roic.total_funds,
        normal_channel_capacity_sat,
        channel_funds_percent_of_capacity: percentage(
            roic.total_funds,
            normal_channel_capacity_sat,
        ),
        channel_balance_target_stddev_percentage_points:
            channel_balance_target_stddev_percentage_points(&normal_channels),
        network_average_fee_ppm,
        network_median_fee_ppm,
        node_average_fee_ppm,
        node_median_fee_ppm,
        total_forwarding_fees_sat: store.total_forwarding_fees_sat(),
        total_rebalance_cost_msat: store.total_rebalance_cost_msat(),
        net_routing_revenue_msat: store.net_routing_revenue_msat(),
        roic: RoicSnapshot {
            periods,
            routed_12_months_sat: roic.routed_12_months,
            capital_velocity_12_months: if average_12_months == 0.0 {
                0.0
            } else {
                roic.routed_12_months as f64 / average_12_months
            },
            effective_fee_rate_12_months_bps: roic.effective_fee_rate_12_months_bps,
            lease_fee_earnings_12_months_msat: roic.lease_fee_earnings_12_months_msat,
            lease_fee_cost_12_months_msat: roic.lease_fee_cost_12_months_msat,
            rebalance_cost_12_months_msat: roic.rebalance_cost_12_months_msat,
            net_roic_12_months_percent: if average_12_months == 0.0 {
                0.0
            } else {
                net_revenue_12_months_msat / 1000.0 / average_12_months * 100.0
            },
        },
    }
}

fn is_pending_channel_balance(state: &str, peer_status: Option<&[String]>) -> bool {
    state != "CHANNELD_NORMAL"
        && peer_status.is_none_or(|statuses| {
            !statuses
                .iter()
                .any(|status| status.contains("All outputs resolved"))
        })
}

fn average_channel_funds(
    history: &[history::ChannelFundsHistoryPoint],
    end: &DateTime<Utc>,
    months: i64,
    current_channel_funds_sat: u64,
) -> AverageChannelFunds {
    let window_seconds = months.saturating_mul(30).saturating_mul(24 * 60 * 60);
    if window_seconds <= 0 {
        return AverageChannelFunds {
            sats: current_channel_funds_sat as f64,
            coverage_ratio: 0.0,
        };
    }
    let start = *end - chrono::Duration::seconds(window_seconds);
    let mut current = history
        .iter()
        .filter(|point| point.observed_at <= start)
        .max_by_key(|point| point.observed_at)
        .map(|point| (start, point.channel_funds_msat));
    let mut weighted_msat_seconds = 0_u128;
    let mut covered_seconds = 0_u64;

    for point in history
        .iter()
        .filter(|point| point.observed_at > start && point.observed_at <= *end)
    {
        if let Some((previous_at, previous_msat)) = current {
            let seconds = point
                .observed_at
                .signed_duration_since(previous_at)
                .num_seconds()
                .max(0) as u64;
            weighted_msat_seconds =
                weighted_msat_seconds.saturating_add(previous_msat as u128 * seconds as u128);
            covered_seconds = covered_seconds.saturating_add(seconds);
        }
        current = Some((point.observed_at, point.channel_funds_msat));
    }
    if let Some((previous_at, previous_msat)) = current {
        let seconds = end.signed_duration_since(previous_at).num_seconds().max(0) as u64;
        weighted_msat_seconds =
            weighted_msat_seconds.saturating_add(previous_msat as u128 * seconds as u128);
        covered_seconds = covered_seconds.saturating_add(seconds);
    }

    if covered_seconds == 0 {
        AverageChannelFunds {
            sats: current_channel_funds_sat as f64,
            coverage_ratio: 0.0,
        }
    } else {
        AverageChannelFunds {
            sats: weighted_msat_seconds as f64 / covered_seconds as f64 / 1000.0,
            coverage_ratio: covered_seconds as f64 / window_seconds as f64,
        }
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
    let lease_fees = store.lease_fee_totals_for_account(&channel.channel_id);
    let gross_routing_revenue_msat = forwards.forwarding_fees_sat as i64 * 1000;
    let net_routing_revenue_msat = gross_routing_revenue_msat - rebalances.target_cost_msat as i64;
    let gross_revenue_msat = gross_routing_revenue_msat as i128 + lease_fees.earned_msat as i128;
    let net_revenue_msat = net_routing_revenue_msat as i128 + lease_fees.earned_msat as i128
        - lease_fees.paid_msat as i128;
    let indirect_revenue_msat = forwards.indirect_fees_sat as i128 * 1000;
    let combined_revenue_msat = net_revenue_msat + indirect_revenue_msat;
    let peer_channel = store.get_peer_channel(&channel.channel_id);
    let local_update = peer_channel
        .and_then(|peer_channel| peer_channel.updates.as_ref())
        .and_then(|updates| updates.local.as_ref());
    let remote_update = peer_channel
        .and_then(|peer_channel| peer_channel.updates.as_ref())
        .and_then(|updates| updates.remote.as_ref());
    let outbound_network_channel =
        short_channel_id.and_then(|scid| store.get_channel(scid, &store.info.id));
    let inbound_network_channel =
        short_channel_id.and_then(|scid| store.get_channel(scid, &channel.peer_id));

    ChannelSnapshot {
        channel_id: channel.channel_id.clone(),
        short_channel_id: channel.short_channel_id.clone(),
        funding_txid: channel.funding_txid.clone(),
        funding_output: channel.funding_output,
        peer_id: channel.peer_id.clone(),
        peer_alias: store.get_node_alias(&channel.peer_id),
        connected: channel.connected,
        peer_supports_splicing: store.peer_supports_splicing(&channel.peer_id),
        private: peer_channel.and_then(|peer_channel| peer_channel.private),
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
        outbound_fee_ppm: local_update
            .map(|update| update.fee_proportional_millionths)
            .or_else(|| {
                peer_channel.and_then(|peer_channel| peer_channel.fee_proportional_millionths)
            })
            .or_else(|| {
                outbound_network_channel.map(|network_channel| network_channel.fee_per_millionth)
            }),
        inbound_fee_ppm: remote_update
            .map(|update| update.fee_proportional_millionths)
            .or_else(|| {
                inbound_network_channel.map(|network_channel| network_channel.fee_per_millionth)
            }),
        outbound_base_fee_msat: local_update
            .map(|update| update.fee_base_msat)
            .or_else(|| peer_channel.and_then(|peer_channel| peer_channel.fee_base_msat))
            .or_else(|| {
                outbound_network_channel
                    .map(|network_channel| network_channel.base_fee_millisatoshi)
            }),
        outbound_htlc_min_msat: local_update
            .map(|update| update.htlc_minimum_msat)
            .or_else(|| {
                outbound_network_channel.map(|network_channel| network_channel.htlc_minimum_msat)
            }),
        outbound_htlc_max_msat: local_update
            .map(|update| update.htlc_maximum_msat)
            .or_else(|| {
                outbound_network_channel.map(|network_channel| network_channel.htlc_maximum_msat)
            }),
        outbound_delay_blocks: local_update
            .map(|update| update.cltv_expiry_delta)
            .or_else(|| outbound_network_channel.map(|network_channel| network_channel.delay)),
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
        time_decayed_fee_ppm: short_channel_id
            .and_then(|scid| store.get_channel_time_decayed_fee_ppm(scid)),
        rebalance_target_cost_msat: rebalances.target_cost_msat,
        rebalance_target_credit_msat: rebalances.target_credit_msat,
        rebalance_effective_fee_ppm: ratio_ppm(
            rebalances.target_cost_msat as f64,
            rebalances.target_credit_msat as f64,
        ),
        rebalance_source_cost_msat: rebalances.source_cost_msat,
        rebalance_source_debit_msat: rebalances.source_debit_msat,
        rebalance_source_debit_90d_msat: rebalances.source_debit_90d_msat,
        rebalance_source_effective_fee_ppm: ratio_ppm(
            rebalances.source_cost_msat as f64,
            rebalances.source_credit_msat as f64,
        ),
        lease_fee_earnings_msat: lease_fees.earned_msat,
        lease_fee_cost_msat: lease_fees.paid_msat,
        net_routing_revenue_msat,
        net_revenue_msat,
        gross_capacity_return_percent: annualized_capacity_return_percent(
            gross_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        net_capacity_return_percent: annualized_capacity_return_percent(
            net_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        indirect_capacity_contribution_percent: annualized_capacity_return_percent(
            indirect_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
        combined_capacity_return_percent: annualized_capacity_return_percent(
            combined_revenue_msat,
            channel.amount_msat,
            age_days,
        ),
    }
}

fn aggregate_channel_forwards(store: &Store) -> HashMap<String, ChannelForwardMetrics> {
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
    }
    metrics
}

fn aggregate_channel_rebalances(store: &Store) -> HashMap<String, ChannelRebalanceMetrics> {
    let mut metrics: HashMap<String, ChannelRebalanceMetrics> = HashMap::new();
    let snapshot_timestamp = u64::try_from(store.snapshot_time().timestamp()).unwrap_or_default();
    for part in store.rebalance_parts() {
        if let Some(target_channel_id) = &part.target_channel_id {
            let target = metrics.entry(target_channel_id.clone()).or_default();
            target.target_cost_msat += part.fees_msat;
            target.target_credit_msat += part.credit_msat;
        }
        if let Some(source_channel_id) = &part.source_channel_id {
            let source = metrics.entry(source_channel_id.clone()).or_default();
            source.source_cost_msat += part.fees_msat;
            source.source_credit_msat += part.credit_msat;
            source.source_debit_msat += part.debit_msat;
            if timestamp_in_lookback(
                part.timestamp,
                snapshot_timestamp,
                REBALANCE_SOURCE_90D_SECONDS,
            ) {
                source.source_debit_90d_msat += part.debit_msat;
            }
        }
    }
    metrics
}

fn timestamp_in_lookback(timestamp: Option<u64>, end: u64, window_seconds: u64) -> bool {
    timestamp
        .is_some_and(|timestamp| (end.saturating_sub(window_seconds)..=end).contains(&timestamp))
}

fn ratio_ppm(numerator: f64, denominator: f64) -> Option<f64> {
    if denominator == 0.0 {
        None
    } else {
        Some(numerator * 1_000_000.0 / denominator)
    }
}

fn annualized_capacity_return_percent(
    revenue_msat: i128,
    capacity_msat: u64,
    age_days: Option<i64>,
) -> Option<f64> {
    let age_days = age_days?;
    if age_days <= 0 || capacity_msat == 0 {
        return Some(0.0);
    }

    Some((revenue_msat as f64 / capacity_msat as f64) * (365.0 / age_days as f64) * 100.0)
}

fn channel_balance_target_stddev_percentage_points(channels: &[Fund]) -> f64 {
    if channels.is_empty() {
        return 0.0;
    }

    let mean_squared_distance = channels
        .iter()
        .map(|channel| {
            let distance_from_target = channel.perc_float() - 0.5;
            distance_from_target * distance_from_target
        })
        .sum::<f64>()
        / channels.len() as f64;

    mean_squared_distance.sqrt() * 100.0
}

fn percentage(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64 * 100.0)
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
    let lease_fees = store.lease_fee_totals_for_account(&channel.channel_id);
    let net_revenue_msat = forwarding_fees_sat as i128 * 1000 + lease_fees.earned_msat as i128
        - lease_fees.paid_msat as i128
        - rebalance_target_cost_msat as i128;
    let indirect_revenue_msat = indirect_fees_sat as i128 * 1000;
    let combined_revenue_msat = net_revenue_msat + indirect_revenue_msat;

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
        lease_fee_earnings_msat: lease_fees.earned_msat,
        lease_fee_cost_msat: lease_fees.paid_msat,
        net_revenue_msat,
        net_capacity_return_percent: annualized_capacity_return_percent(
            net_revenue_msat,
            channel.total_msat,
            age_days,
        ),
        indirect_capacity_contribution_percent: annualized_capacity_return_percent(
            indirect_revenue_msat,
            channel.total_msat,
            age_days,
        ),
        combined_capacity_return_percent: annualized_capacity_return_percent(
            combined_revenue_msat,
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
        source_peer_alias: part
            .source_channel_id
            .as_deref()
            .and_then(|short_channel_id| forward_peer(store, short_channel_id).1),
        target_peer_alias: part
            .target_channel_id
            .as_deref()
            .and_then(|short_channel_id| forward_peer(store, short_channel_id).1),
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

fn build_rebalance_status_snapshot(store: &Store) -> io::Result<Vec<RebalanceStatusSnapshot>> {
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
            let last_channel_partner_alias = entry
                .last_channel_partner
                .as_deref()
                .and_then(|short_channel_id| forward_peer(store, short_channel_id).1);
            Ok(RebalanceStatusSnapshot {
                short_channel_id: entry.scid,
                peer_id: entry.pubkey,
                peer_alias: entry.alias,
                last_channel_partner_id: entry.last_channel_partner,
                last_channel_partner_alias,
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

fn build_lnplus_pools_snapshot(store: &Store, nodes: Vec<PoolNode>) -> Vec<LnPlusPoolSnapshot> {
    let peer_ids = store.peers_ids();
    let former_peer_ids: HashSet<&str> = store
        .closed_channels
        .closedchannels
        .iter()
        .filter_map(|channel| channel.peer_id.as_deref())
        .collect();
    nodes
        .into_iter()
        .filter(|node| is_node_id(&node.pubkey))
        .map(|node| LnPlusPoolSnapshot {
            url: format!("https://lightningnetwork.plus/nodes/{}", node.pubkey),
            offer_open: node.credits_balance_sats > 0
                && node.credits_balance_sats >= node.min_channel_size_sats,
            is_current_peer: peer_ids.contains(&node.pubkey),
            had_channel_in_past: former_peer_ids.contains(node.pubkey.as_str()),
            node_id: node.pubkey,
            alias: node.alias,
            credits_balance_sat: node.credits_balance_sats,
            min_channel_size_sat: node.min_channel_size_sats,
            capacity_sat: node.capacity_sats,
            open_channels: node.open_channels,
            connection: node.connection,
            clearnet_address: node.clearnet_address,
            tor_address: node.tor_address,
            lnp_rank: node.lnp_rank,
            lnp_rank_name: node.lnp_rank_name,
            positive_ratings: node.lnp_positive_ratings_received,
            negative_ratings: node.lnp_negative_ratings_received,
        })
        .collect()
}

fn is_node_id(value: &str) -> bool {
    value.len() == 66 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Pseudo-count of routes added to each amount's denominator so amounts probed with few
/// evaluated routes cannot dominate the weighted score through one or two appearances.
const ROUTE_SCORE_PRIOR_ROUTES: f64 = 100.0;

/// Weight of a probe amount in the weighted route score: 1 for 1k sats, 2 for 10k, 3 for 100k...
fn route_amount_weight(amount_sat: u64) -> f64 {
    (amount_sat as f64 / 100.0).log10().max(0.0)
}

/// Node-level weighted route score keyed by node ID
fn weighted_route_scores(candidates: &[RouteCandidate], runs: &[RouteRun]) -> HashMap<String, f64> {
    let evaluated: HashMap<u64, usize> = runs
        .iter()
        .map(|run| (run.amount_sat, run.evaluated_routes))
        .collect();
    let mut scores: HashMap<String, f64> = HashMap::new();
    for candidate in candidates {
        let evaluated_routes = evaluated.get(&candidate.amount_sat).copied().unwrap_or(0);
        let share =
            candidate.appearances as f64 / (evaluated_routes as f64 + ROUTE_SCORE_PRIOR_ROUTES);
        *scores.entry(candidate.node_id.clone()).or_default() +=
            1000.0 * route_amount_weight(candidate.amount_sat) * share;
    }
    scores
}

fn lnplus_offer_fields(
    pools: Option<&HashMap<&str, &LnPlusPoolSnapshot>>,
    node_id: &str,
) -> LnPlusOfferFields {
    let pool = pools.and_then(|pools| pools.get(node_id).copied());
    LnPlusOfferFields {
        lnplus_pool_member: pools.map(|_| pool.is_some()),
        lnplus_pool_offer_open: pools.map(|_| pool.is_some_and(|pool| pool.offer_open)),
        lnplus_pool_credits_sat: pool.map(|pool| pool.credits_balance_sat),
        lnplus_pool_min_channel_size_sat: pool.map(|pool| pool.min_channel_size_sat),
        lnplus_connection: pool.map(|pool| pool.connection.clone()),
        lnplus_negative_ratings: pool.map(|pool| pool.negative_ratings),
        lnplus_url: pool.map(|pool| pool.url.clone()),
    }
}

/// Per-amount candidate rows enriched with node-level fields, plus one row per candidate node
fn build_route_snapshots(
    candidates: Vec<RouteCandidate>,
    runs: &[RouteRun],
    lnplus_pools: Option<&[LnPlusPoolSnapshot]>,
    closed_channels: &[ClosedChannelSnapshot],
) -> (Vec<RouteCandidateSnapshot>, Vec<RoutePartnerSnapshot>) {
    let scores = weighted_route_scores(&candidates, runs);
    let mut ranked: Vec<(&String, &f64)> = scores.iter().collect();
    ranked.sort_by(|a, b| b.1.total_cmp(a.1).then_with(|| a.0.cmp(b.0)));
    let ranks: HashMap<&str, usize> = ranked
        .iter()
        .enumerate()
        .map(|(index, (node_id, _))| (node_id.as_str(), index + 1))
        .collect();
    let pools: Option<HashMap<&str, &LnPlusPoolSnapshot>> = lnplus_pools.map(|pools| {
        pools
            .iter()
            .map(|pool| (pool.node_id.as_str(), pool))
            .collect()
    });
    let mut closed_by_peer: HashMap<&str, Vec<&ClosedChannelSnapshot>> = HashMap::new();
    for channel in closed_channels {
        if let Some(peer_id) = channel.peer_id.as_deref() {
            closed_by_peer.entry(peer_id).or_default().push(channel);
        }
    }

    let mut partners: BTreeMap<usize, RoutePartnerSnapshot> = BTreeMap::new();
    for candidate in &candidates {
        let rank = ranks[candidate.node_id.as_str()];
        let partner = partners.entry(rank).or_insert_with(|| {
            let past = closed_by_peer
                .get(candidate.node_id.as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            new_route_partner(candidate, scores[&candidate.node_id], rank, past, {
                lnplus_offer_fields(pools.as_ref(), &candidate.node_id)
            })
        });
        partner.total_appearances += candidate.appearances;
        if candidate.appearances > 0 {
            partner.largest_amount_sat = partner.largest_amount_sat.max(candidate.amount_sat);
        }
        match candidate.amount_sat {
            1_000 => partner.appearances_1k_sat += candidate.appearances,
            10_000 => partner.appearances_10k_sat += candidate.appearances,
            100_000 => partner.appearances_100k_sat += candidate.appearances,
            1_000_000 => partner.appearances_1m_sat += candidate.appearances,
            _ => {}
        }
    }

    let candidates = candidates
        .into_iter()
        .map(|candidate| RouteCandidateSnapshot {
            weighted_route_score: scores[&candidate.node_id],
            weighted_route_rank: ranks[candidate.node_id.as_str()],
            lnplus: lnplus_offer_fields(pools.as_ref(), &candidate.node_id),
            candidate,
        })
        .collect();
    (candidates, partners.into_values().collect())
}

fn new_route_partner(
    candidate: &RouteCandidate,
    weighted_route_score: f64,
    weighted_route_rank: usize,
    past: &[&ClosedChannelSnapshot],
    lnplus: LnPlusOfferFields,
) -> RoutePartnerSnapshot {
    let aged: Vec<(&ClosedChannelSnapshot, i64)> = past
        .iter()
        .filter_map(|channel| Some((*channel, channel.age_days?)))
        .collect();
    let capacity_years: f64 = aged
        .iter()
        .map(|(channel, days)| channel.capacity_msat as f64 * *days as f64 / 365.0)
        .sum();
    let aged_revenue: i128 = aged
        .iter()
        .map(|(channel, _)| channel.net_revenue_msat)
        .sum();
    RoutePartnerSnapshot {
        node_id: candidate.node_id.clone(),
        alias: candidate.alias.clone(),
        connectable: candidate.connectable,
        channel_count: candidate.channel_count,
        average_fee_ppm: candidate.average_fee_ppm,
        fee_diversity: candidate.fee_diversity,
        weighted_route_score,
        weighted_route_rank,
        total_appearances: 0,
        appearances_1k_sat: 0,
        appearances_10k_sat: 0,
        appearances_100k_sat: 0,
        appearances_1m_sat: 0,
        largest_amount_sat: 0,
        past_channel_count: past.len(),
        past_capacity_msat: past.iter().map(|channel| channel.capacity_msat).sum(),
        past_lifetime_days: (!aged.is_empty()).then(|| aged.iter().map(|(_, days)| days).sum()),
        past_net_revenue_msat: (!past.is_empty())
            .then(|| past.iter().map(|channel| channel.net_revenue_msat).sum()),
        past_net_capacity_return_percent: (capacity_years > 0.0)
            .then(|| aged_revenue as f64 / capacity_years * 100.0),
        past_local_closes: past
            .iter()
            .filter(|channel| channel.closer.as_deref() == Some("local"))
            .count(),
        past_remote_closes: past
            .iter()
            .filter(|channel| channel.closer.as_deref() == Some("remote"))
            .count(),
        lnplus,
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::other(format!("parsing `{}` failed: {e}", path.display())))
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
    use chrono::{Duration, TimeZone, Utc};

    use crate::history::ChannelFundsHistoryPoint;

    use super::{
        annualized_capacity_return_percent, average_channel_funds, build_route_snapshots,
        channel_balance_target_stddev_percentage_points, is_node_id, is_pending_channel_balance,
        percentage, ratio_ppm, route_amount_weight, timestamp_in_lookback, weighted_route_scores,
        ClosedChannelSnapshot, LnPlusPoolSnapshot,
    };
    use crate::cmd::Fund;
    use crate::routes::{RouteCandidate, RouteRun};

    #[test]
    fn pending_channel_balance_excludes_normal_and_fully_resolved_channels() {
        let unresolved = vec![
            "ONCHAIN:Tracking our own unilateral close".to_string(),
            "ONCHAIN:1 outputs unresolved: in 93 blocks will spend DELAYED_OUTPUT_TO_US"
                .to_string(),
        ];
        let resolved = vec![
            "ONCHAIN:Tracking mutual close transaction".to_string(),
            "ONCHAIN:All outputs resolved: waiting 51 more blocks before forgetting channel"
                .to_string(),
        ];

        assert!(!is_pending_channel_balance("CHANNELD_NORMAL", None));
        assert!(is_pending_channel_balance("CLOSINGD_SIGEXCHANGE", None));
        assert!(is_pending_channel_balance("ONCHAIN", Some(&unresolved)));
        assert!(!is_pending_channel_balance("ONCHAIN", Some(&resolved)));
    }

    #[test]
    fn ratio_ppm_returns_none_for_no_volume() {
        assert_eq!(ratio_ppm(10.0, 0.0), None);
        assert_eq!(ratio_ppm(10.0, 1_000.0), Some(10_000.0));
    }

    #[test]
    fn timestamp_lookback_includes_boundaries_and_rejects_missing_or_future_values() {
        let end = 10_000;
        let window = 1_000;

        assert!(timestamp_in_lookback(Some(9_000), end, window));
        assert!(timestamp_in_lookback(Some(10_000), end, window));
        assert!(!timestamp_in_lookback(Some(8_999), end, window));
        assert!(!timestamp_in_lookback(Some(10_001), end, window));
        assert!(!timestamp_in_lookback(None, end, window));
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

    #[test]
    fn percentage_reports_share_and_handles_zero_capacity() {
        assert_eq!(percentage(1, 2), Some(50.0));
        assert_eq!(percentage(0, 0), None);
    }

    #[test]
    fn channel_funds_average_is_time_weighted_across_changes() {
        let end = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let history = vec![
            ChannelFundsHistoryPoint {
                observed_at: end - Duration::days(31),
                channel_funds_msat: 1_000_000,
            },
            ChannelFundsHistoryPoint {
                observed_at: end - Duration::days(15),
                channel_funds_msat: 3_000_000,
            },
        ];

        let average = average_channel_funds(&history, &end, 1, 9_000);

        assert_eq!(average.sats, 2_000.0);
        assert_eq!(average.coverage_ratio, 1.0);
    }

    #[test]
    fn channel_funds_average_reports_partial_history_coverage() {
        let end = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let history = vec![ChannelFundsHistoryPoint {
            observed_at: end - Duration::days(15),
            channel_funds_msat: 3_000_000,
        }];

        let average = average_channel_funds(&history, &end, 1, 9_000);

        assert_eq!(average.sats, 3_000.0);
        assert_eq!(average.coverage_ratio, 0.5);
    }

    fn route_candidate(amount_sat: u64, node_id: &str, appearances: u64) -> RouteCandidate {
        RouteCandidate {
            amount_sat,
            rank: 1,
            node_id: node_id.to_string(),
            alias: node_id.to_string(),
            connectable: true,
            had_channel_in_past: false,
            appearances,
            appearance_ratio: None,
            average_fee_ppm: 0.0,
            fee_diversity: 0.0,
            channel_count: 1,
        }
    }

    fn route_run(amount_sat: u64, evaluated_routes: usize) -> RouteRun {
        RouteRun {
            amount_sat,
            max_fee_msat: 0,
            scanned_nodes: 0,
            eligible_destinations: 0,
            processed_destinations: 0,
            queried_destinations: 0,
            capacity_filtered_destinations: 0,
            evaluated_routes,
            failed_routes: 0,
            timed_out_routes: 0,
            budget_exhausted: false,
            elapsed_seconds: 0.0,
            candidate_nodes: 0,
            recurring_candidate_nodes: 0,
            average_hops: 0.0,
        }
    }

    fn lnplus_pool(node_id: &str, credits_balance_sat: u64) -> LnPlusPoolSnapshot {
        LnPlusPoolSnapshot {
            node_id: node_id.to_string(),
            alias: None,
            credits_balance_sat,
            min_channel_size_sat: 100_000,
            capacity_sat: 0,
            open_channels: 0,
            connection: "Clearnet".to_string(),
            clearnet_address: None,
            tor_address: None,
            lnp_rank: 1,
            lnp_rank_name: "Mercury".to_string(),
            positive_ratings: 0,
            negative_ratings: 2,
            url: format!("https://lightningnetwork.plus/nodes/{node_id}"),
            offer_open: true,
            is_current_peer: false,
            had_channel_in_past: false,
        }
    }

    #[test]
    fn route_amount_weight_grows_one_per_order_of_magnitude() {
        assert_eq!(route_amount_weight(1_000), 1.0);
        assert!((route_amount_weight(100_000) - 3.0).abs() < 1e-12);
        assert_eq!(route_amount_weight(10), 0.0);
    }

    #[test]
    fn weighted_route_score_favors_larger_amounts_and_smooths_sparse_runs() {
        let runs = [
            route_run(1_000, 900),
            route_run(100_000, 400),
            route_run(1_000_000, 0),
        ];
        let candidates = [
            route_candidate(1_000, "small", 50),
            route_candidate(100_000, "large", 20),
            route_candidate(1_000_000, "sparse", 1),
        ];

        let scores = weighted_route_scores(&candidates, &runs);

        assert!((scores["small"] - 1000.0 * 50.0 / 1000.0).abs() < 1e-9);
        assert!((scores["large"] - 1000.0 * 3.0 * 20.0 / 500.0).abs() < 1e-9);
        assert!((scores["sparse"] - 1000.0 * 4.0 / 100.0).abs() < 1e-9);
        assert!(scores["large"] > scores["small"]);
        assert!(scores["sparse"] < scores["small"]);
    }

    #[test]
    fn enriched_candidates_share_node_rank_and_join_pool_offers() {
        let runs = [route_run(1_000, 100), route_run(10_000, 100)];
        let candidates = vec![
            route_candidate(1_000, "a", 1),
            route_candidate(10_000, "a", 1),
            route_candidate(1_000, "b", 10),
        ];
        let pools = [lnplus_pool("a", 5_000_000)];

        let (enriched, _) = build_route_snapshots(candidates, &runs, Some(&pools), &[]);

        assert_eq!(enriched[0].weighted_route_rank, 2);
        assert_eq!(enriched[1].weighted_route_rank, 2);
        assert_eq!(
            enriched[0].weighted_route_score,
            enriched[1].weighted_route_score
        );
        assert_eq!(enriched[2].weighted_route_rank, 1);
        assert_eq!(enriched[0].lnplus.lnplus_pool_member, Some(true));
        assert_eq!(enriched[0].lnplus.lnplus_pool_offer_open, Some(true));
        assert_eq!(enriched[2].lnplus.lnplus_pool_offer_open, Some(false));
        assert_eq!(enriched[0].lnplus.lnplus_pool_credits_sat, Some(5_000_000));
        assert_eq!(enriched[0].lnplus.lnplus_negative_ratings, Some(2));
        assert_eq!(enriched[2].lnplus.lnplus_pool_member, Some(false));
        assert_eq!(enriched[2].lnplus.lnplus_pool_credits_sat, None);

        let json = serde_json::to_value(&enriched[0]).unwrap();
        assert_eq!(json["node_id"], "a");
        assert_eq!(json["amount_sat"], 1_000);
    }

    #[test]
    fn enriched_candidates_report_unknown_pool_membership_without_lnplus() {
        let (enriched, _) = build_route_snapshots(
            vec![route_candidate(1_000, "a", 1)],
            &[route_run(1_000, 10)],
            None,
            &[],
        );

        assert_eq!(enriched[0].lnplus.lnplus_pool_member, None);
        assert_eq!(enriched[0].lnplus.lnplus_url, None);
    }

    fn closed_channel(peer_id: &str, closer: &str, age_days: i64) -> ClosedChannelSnapshot {
        ClosedChannelSnapshot {
            channel_id: format!("{peer_id}-{closer}-{age_days}"),
            short_channel_id: None,
            peer_id: Some(peer_id.to_string()),
            peer_alias: None,
            opener: "local".to_string(),
            closer: Some(closer.to_string()),
            capacity_msat: 1_000_000_000,
            final_local_balance_msat: 0,
            total_htlcs_sent: None,
            funding_txid: String::new(),
            last_commitment_txid: None,
            last_stable_connection_at: None,
            close_cause: "user".to_string(),
            age_days: Some(age_days),
            lease_fee_earnings_msat: 0,
            lease_fee_cost_msat: 0,
            net_revenue_msat: 10_000_000,
            net_capacity_return_percent: None,
            indirect_capacity_contribution_percent: None,
            combined_capacity_return_percent: None,
        }
    }

    #[test]
    fn route_partners_group_amounts_and_past_channels_by_node() {
        let runs = [route_run(1_000, 100), route_run(100_000, 100)];
        let candidates = vec![
            route_candidate(1_000, "a", 3),
            route_candidate(100_000, "a", 2),
            route_candidate(1_000, "b", 40),
        ];
        let closed = [
            closed_channel("a", "local", 365),
            closed_channel("a", "remote", 365),
        ];

        let (_, partners) = build_route_snapshots(candidates, &runs, None, &closed);

        assert_eq!(partners.len(), 2);
        assert_eq!(partners[0].node_id, "b");
        assert_eq!(partners[0].weighted_route_rank, 1);
        assert_eq!(partners[0].past_channel_count, 0);
        assert_eq!(partners[0].past_net_revenue_msat, None);
        let a = &partners[1];
        assert_eq!(a.total_appearances, 5);
        assert_eq!(a.appearances_1k_sat, 3);
        assert_eq!(a.appearances_100k_sat, 2);
        assert_eq!(a.largest_amount_sat, 100_000);
        assert_eq!(a.past_channel_count, 2);
        assert_eq!(a.past_lifetime_days, Some(730));
        assert_eq!(a.past_net_revenue_msat, Some(20_000_000));
        assert!((a.past_net_capacity_return_percent.unwrap() - 1.0).abs() < 1e-9);
        assert_eq!((a.past_local_closes, a.past_remote_closes), (1, 1));
        assert_eq!(a.lnplus.lnplus_pool_member, None);
    }

    #[test]
    fn node_id_validation_rejects_non_pubkeys() {
        assert!(is_node_id(&"02".repeat(33)));
        assert!(!is_node_id("javascript:alert(1)"));
        assert!(!is_node_id(&"0g".repeat(33)));
    }

    #[test]
    fn channel_balance_target_stddev_measures_distance_from_fifty_percent() {
        let channels = [fund(250, 1_000), fund(750, 1_000)];

        assert_eq!(
            channel_balance_target_stddev_percentage_points(&channels),
            25.0
        );
        assert_eq!(channel_balance_target_stddev_percentage_points(&[]), 0.0);
    }

    fn fund(our_amount_msat: u64, amount_msat: u64) -> Fund {
        Fund {
            peer_id: "peer".to_string(),
            connected: true,
            state: "CHANNELD_NORMAL".to_string(),
            channel_id: "channel".to_string(),
            short_channel_id: Some("1x1x1".to_string()),
            our_amount_msat,
            amount_msat,
            funding_txid: "txid".to_string(),
            funding_output: 0,
        }
    }
}
