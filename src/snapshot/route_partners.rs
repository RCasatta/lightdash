//! Route-candidate enrichment for snapshots: the node-level weighted route score, per-node
//! route partner rows, and the join with LN+ Liquidity Pool offers.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Serialize;

use super::ClosedChannelSnapshot;
use crate::lnplus::PoolNode;
use crate::routes::{RouteCandidate, RouteRun};
use crate::store::Store;

#[derive(Serialize)]
pub(super) struct LnPlusPoolSnapshot {
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
pub(super) struct RouteCandidateSnapshot {
    #[serde(flatten)]
    candidate: RouteCandidate,
    weighted_route_score: f64,
    weighted_route_rank: usize,
    #[serde(flatten)]
    lnplus: LnPlusOfferFields,
}

#[derive(Serialize)]
pub(super) struct RoutePartnerSnapshot {
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

pub(super) fn build_lnplus_pools_snapshot(
    store: &Store,
    nodes: Vec<PoolNode>,
) -> Vec<LnPlusPoolSnapshot> {
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
pub(super) fn build_route_snapshots(
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

#[cfg(test)]
mod tests {
    use super::{
        build_route_snapshots, is_node_id, route_amount_weight, weighted_route_scores,
        LnPlusPoolSnapshot,
    };
    use crate::routes::{RouteCandidate, RouteRun};
    use crate::snapshot::ClosedChannelSnapshot;

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
}
