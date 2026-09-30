use crate::cmd::{self, Forward, SettledForward};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebalancePart {
    pub payment_id: String,
    pub part_id: u64,
    pub source_account: String,
    pub target_account: String,
    pub source_channel_id: Option<String>,
    pub target_channel_id: Option<String>,
    pub debit_msat: u64,
    pub credit_msat: u64,
    pub fees_msat: u64,
    pub timestamp: Option<u64>,
}

/// Store containing all data fetched from the Lightning node
pub struct Store {
    pub info: cmd::GetInfo,
    pub channels: cmd::ListChannels,
    pub peer_channels: cmd::ListPeerChannels,
    pub peers: cmd::ListPeers,
    pub funds: cmd::ListFunds,
    pub forwards: cmd::ListForwards,
    pub closed_channels: cmd::ListClosedChannels,
    rebalance_parts: Vec<RebalancePart>,
    income_events: Vec<cmd::BkprIncomeEvent>,
    // Cached computed data
    nodes_by_id: HashMap<String, cmd::Node>,
    channels_by_id: HashMap<(String, String), cmd::Channel>,
    forward_cache: ForwardCache,
    setchannel_timestamps: HashMap<String, i64>,
    now: DateTime<Utc>,
    pub avail_map: HashMap<String, f64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaseFeeTotals {
    pub earned_msat: u64,
    pub paid_msat: u64,
}

fn account_to_channel_map(
    funds: &cmd::ListFunds,
    closed_channels: &cmd::ListClosedChannels,
) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = funds
        .channels
        .iter()
        .map(|channel| {
            (
                channel.channel_id.clone(),
                channel
                    .short_channel_id
                    .clone()
                    .unwrap_or_else(|| channel.channel_id.clone()),
            )
        })
        .collect();

    for channel in &closed_channels.closedchannels {
        if channel.channel_id.is_empty() {
            continue;
        }

        map.entry(channel.channel_id.clone()).or_insert_with(|| {
            channel
                .short_channel_id
                .clone()
                .unwrap_or_else(|| channel.channel_id.clone())
        });
    }

    map
}

#[derive(Default)]
struct RebalancePartBuilder {
    debit: Option<cmd::BkprAccountEvent>,
    credit: Option<cmd::BkprAccountEvent>,
    fees_msat: u64,
    timestamp: Option<u64>,
}

fn is_rebalance_candidate_event(event: &cmd::BkprAccountEvent) -> bool {
    event.is_rebalance || event.tag == "invoice"
}

#[derive(Default)]
struct ChannelForwardMetrics {
    settled_count: usize,
    outbound_fees_sat: u64,
    indirect_fees_sat: u64,
    routed_out_sat: u64,
    weighted_tppm_fees_msat: f64,
    weighted_tppm_routed_msat: f64,
}

#[derive(Default)]
struct ForwardCache {
    settled: Vec<SettledForward>,
    metrics_by_channel: HashMap<String, ChannelForwardMetrics>,
}

fn build_forward_cache(forwards: &cmd::ListForwards, now: DateTime<Utc>) -> ForwardCache {
    const HALF_LIFE_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
    const MIN_TPPM_OUT_MSAT: u64 = 1_000 * 1_000;

    let mut settled: Vec<_> = forwards
        .forwards
        .iter()
        .filter(|forward| forward.status == "settled")
        .map(|forward| SettledForward::try_from(forward.clone()).unwrap())
        .collect();
    settled.sort_by(|a, b| b.resolved_time.cmp(&a.resolved_time));

    let mut metrics_by_channel: HashMap<String, ChannelForwardMetrics> = HashMap::new();
    for forward in &settled {
        let incoming = metrics_by_channel
            .entry(forward.in_channel.clone())
            .or_default();
        incoming.settled_count += 1;
        incoming.indirect_fees_sat += forward.fee_sat;

        let outgoing = metrics_by_channel
            .entry(forward.out_channel.clone())
            .or_default();
        if forward.out_channel != forward.in_channel {
            outgoing.settled_count += 1;
        }
        outgoing.outbound_fees_sat += forward.fee_sat;
        outgoing.routed_out_sat += forward.out_sat;

        if forward.out_msat >= MIN_TPPM_OUT_MSAT {
            let age_seconds = now
                .signed_duration_since(forward.resolved_time)
                .num_seconds()
                .max(0) as f64;
            let decay = 0.5_f64.powf(age_seconds / HALF_LIFE_SECONDS);
            outgoing.weighted_tppm_fees_msat += forward.fee_msat as f64 * decay;
            outgoing.weighted_tppm_routed_msat += forward.out_msat as f64 * decay;
        }
    }

    ForwardCache {
        settled,
        metrics_by_channel,
    }
}

pub(crate) fn match_rebalance_parts(
    events: &[cmd::BkprAccountEvent],
    account_to_channel: &HashMap<String, String>,
) -> Vec<RebalancePart> {
    let mut grouped: HashMap<(String, u64), RebalancePartBuilder> = HashMap::new();

    for event in events
        .iter()
        .filter(|event| is_rebalance_candidate_event(event))
    {
        let (Some(payment_id), Some(part_id)) = (&event.payment_id, event.part_id) else {
            log::debug!(
                "Ignoring rebalance event without payment_id or part_id on account {}",
                event.account
            );
            continue;
        };

        let builder = grouped.entry((payment_id.clone(), part_id)).or_default();
        builder.fees_msat += event.fees_msat.unwrap_or(0);
        builder.timestamp = builder.timestamp.or(event.timestamp);

        if event.debit_msat > 0 {
            builder.debit = Some(event.clone());
        }
        if event.credit_msat > 0 {
            builder.credit = Some(event.clone());
        }
    }

    grouped
        .into_iter()
        .filter_map(|((payment_id, part_id), builder)| {
            let Some(debit) = builder.debit else {
                log::debug!(
                    "Ignoring rebalance payment {payment_id} part {part_id} without debit row"
                );
                return None;
            };
            let Some(credit) = builder.credit else {
                log::debug!(
                    "Ignoring rebalance payment {payment_id} part {part_id} without credit row"
                );
                return None;
            };

            Some(RebalancePart {
                payment_id,
                part_id,
                source_channel_id: account_to_channel.get(&debit.account).cloned(),
                target_channel_id: account_to_channel.get(&credit.account).cloned(),
                source_account: debit.account,
                target_account: credit.account,
                debit_msat: debit.debit_msat,
                credit_msat: credit.credit_msat,
                fees_msat: builder.fees_msat,
                timestamp: builder.timestamp,
            })
        })
        .collect()
}

impl Store {
    /// Create a new Store by fetching all data from the Lightning node
    pub fn new(availdb: Option<String>) -> Self {
        let start_time = std::time::Instant::now();
        log::debug!("Fetching data from Lightning node...");
        let now = Utc::now();
        let info = cmd::get_info();
        let channels = cmd::list_channels();
        let peer_channels = cmd::list_peer_channels();
        let peers = cmd::list_peers();
        let funds = cmd::list_funds();
        let forwards = cmd::list_forwards();
        let account_events = cmd::bkpr_list_account_events();
        let income_events = cmd::bkpr_list_income().income_events;
        let nodes = cmd::list_nodes();
        let closed_channels = cmd::list_closed_channels();
        log::debug!("Data fetched successfully");
        let forward_cache = build_forward_cache(&forwards, now);
        log::info!(
            "Cached {} settled forwards across {} channels",
            forward_cache.settled.len(),
            forward_cache.metrics_by_channel.len()
        );

        log::info!("Loading availdb");
        let avail_map: HashMap<String, f64> = match cmd::read_availdb_json(availdb.as_deref()) {
            Ok(value) => match serde_json::from_value::<HashMap<String, Value>>(value) {
                Ok(outer) => outer
                    .into_iter()
                    .filter_map(|(node_id, data)| {
                        data.get("avail")
                            .and_then(Value::as_f64)
                            .map(|avail| (node_id, avail))
                    })
                    .collect(),
                Err(e) => {
                    log::warn!("Failed to parse availdb entries: {e}");
                    HashMap::new()
                }
            },
            Err(e) => {
                log::warn!("Availability data is unavailable: {e}");
                HashMap::new()
            }
        };
        log::info!("Loaded availdb with {} entries", avail_map.len());

        // Compute cached data
        let nodes_by_id = nodes
            .nodes
            .iter()
            .filter(|e| e.alias.is_some())
            .map(|e| (e.nodeid.clone(), e.clone()))
            .collect();

        let channels_by_id = channels
            .channels
            .iter()
            .map(|e| ((e.short_channel_id.clone(), e.source.clone()), e.clone()))
            .collect();

        let account_to_channel = account_to_channel_map(&funds, &closed_channels);
        let rebalance_parts = match_rebalance_parts(&account_events.events, &account_to_channel);
        log::info!(
            "Loaded {} matched rebalance parts from bookkeeper events",
            rebalance_parts.len()
        );

        // Query setchannel timestamps from datastore
        let mut setchannel_timestamps = HashMap::new();
        if let Ok(datastore) = cmd::listdatastore(Some(&["lightdash", "last_setchannel"])) {
            log::info!(
                "Loaded {} setchannel timestamps from datastore",
                datastore.datastore.len()
            );
            for entry in datastore.datastore {
                // The key format is ["lightdash", "last_setchannel", "short_channel_id"]
                if entry.key.len() == 3
                    && entry.key[0] == "lightdash"
                    && entry.key[1] == "last_setchannel"
                {
                    let short_channel_id = &entry.key[2];
                    if let Some(timestamp_str) = &entry.string {
                        if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                            setchannel_timestamps.insert(short_channel_id.clone(), timestamp);
                        }
                    }
                }
            }
        }

        let store = Self {
            info,
            channels,
            peer_channels,
            peers,
            funds,
            forwards,
            closed_channels,
            rebalance_parts,
            income_events,
            nodes_by_id,
            channels_by_id,
            forward_cache,
            setchannel_timestamps,
            now,
            avail_map,
        };

        let duration = start_time.elapsed();
        log::info!(
            "Store initialization completed in {:.2}s",
            duration.as_secs_f64()
        );

        store
    }

    /// Get normal channels (channels in CHANNELD_NORMAL state)
    pub fn normal_channels(&self) -> Vec<cmd::Fund> {
        self.funds
            .channels
            .iter()
            .filter(|c| c.state == "CHANNELD_NORMAL")
            .cloned()
            .collect()
    }

    /// Get settled forwards by most recent first
    pub fn settled_forwards(&self) -> Vec<SettledForward> {
        self.forward_cache.settled.clone()
    }

    pub fn settled_out_channel_ids(&self) -> impl Iterator<Item = &str> {
        self.forward_cache
            .settled
            .iter()
            .map(|forward| forward.out_channel.as_str())
    }

    pub fn total_rebalance_cost_msat(&self) -> u64 {
        self.rebalance_parts.iter().map(|part| part.fees_msat).sum()
    }

    pub fn rebalance_cost_last_months_msat(&self, months: i64) -> u64 {
        let days = months * 30;
        self.rebalance_parts
            .iter()
            .filter(|part| {
                let Some(timestamp) = part.timestamp else {
                    return false;
                };
                let Some(datetime) = DateTime::from_timestamp(timestamp as i64, 0) else {
                    return false;
                };
                self.now.signed_duration_since(datetime).num_days() <= days
            })
            .map(|part| part.fees_msat)
            .sum()
    }

    pub fn rebalance_parts_last_days(&self, days: i64) -> Vec<&RebalancePart> {
        let mut parts: Vec<_> = self
            .rebalance_parts
            .iter()
            .filter(|part| {
                let Some(timestamp) = part.timestamp else {
                    return false;
                };
                let Some(datetime) = DateTime::from_timestamp(timestamp as i64, 0) else {
                    return false;
                };
                self.now.signed_duration_since(datetime).num_days() <= days
            })
            .collect();

        parts.sort_by(|a, b| {
            b.timestamp
                .cmp(&a.timestamp)
                .then_with(|| a.payment_id.cmp(&b.payment_id))
                .then_with(|| a.part_id.cmp(&b.part_id))
        });
        parts
    }

    pub fn rebalance_parts(&self) -> impl Iterator<Item = &RebalancePart> {
        self.rebalance_parts.iter()
    }

    pub fn snapshot_time(&self) -> DateTime<Utc> {
        self.now
    }

    pub fn total_forwarding_fees_sat(&self) -> u64 {
        self.forward_cache
            .settled
            .iter()
            .map(|forward| forward.fee_sat)
            .sum()
    }

    pub fn net_routing_revenue_msat(&self) -> i64 {
        self.total_forwarding_fees_sat() as i64 * 1000 - self.total_rebalance_cost_msat() as i64
    }

    pub fn channels_len(&self) -> usize {
        self.channels.channels.len()
    }

    pub fn channels(&self) -> impl Iterator<Item = &cmd::Channel> {
        self.channels.channels.iter()
    }

    /// Get a local channel's peer-specific state and policies by stable channel ID.
    pub fn get_peer_channel(&self, channel_id: &str) -> Option<&cmd::ListPeerChannelsChannel> {
        self.peer_channels
            .channels
            .iter()
            .find(|channel| channel.channel_id.as_deref() == Some(channel_id))
    }

    /// Whether the peer's negotiated INIT features include BOLT 9 option_splice.
    ///
    /// Returns `None` when Core Lightning did not expose the peer's INIT features.
    pub fn peer_supports_splicing(&self, peer_id: &str) -> Option<bool> {
        let features = self
            .peers
            .peers
            .iter()
            .find(|peer| peer.id == peer_id)?
            .features
            .as_deref()?;

        Some(feature_bit_is_set(features, 62) || feature_bit_is_set(features, 63))
    }

    pub fn forwards_len(&self) -> usize {
        self.forwards.forwards.len()
    }

    /// Get a channel by short_channel_id and source
    pub fn get_channel(&self, short_channel_id: &str, source: &str) -> Option<&cmd::Channel> {
        self.channels_by_id
            .get(&(short_channel_id.to_string(), source.to_string()))
    }

    /// Get the alias for a node ID, or format the ID if no alias exists
    pub fn get_node_alias(&self, node_id: &str) -> String {
        self.nodes_by_id
            .get(node_id)
            .and_then(|e| e.alias.clone())
            .unwrap_or_else(|| {
                if node_id.len() >= 66 {
                    format!("{}...{}", &node_id[0..8], &node_id[58..])
                } else {
                    node_id.to_string()
                }
            })
    }

    /// Whether the node advertises at least one network address in gossip.
    pub fn is_node_connectable(&self, node_id: &str) -> bool {
        self.nodes_by_id
            .get(node_id)
            .is_some_and(|node| !node.addresses.is_empty())
    }

    /// Get node IDs that have aliases
    pub fn node_ids_with_aliases(&self) -> Vec<String> {
        self.nodes_by_id.keys().cloned().collect()
    }

    /// Get a set of peer IDs that have channels
    pub fn peers_ids(&self) -> HashSet<String> {
        self.peers
            .peers
            .iter()
            .filter(|e| e.num_channels > 0)
            .map(|e| e.id.clone())
            .collect()
    }

    /// Get channel metadata per node (fee info aggregated by source node)
    pub fn chan_meta_per_node(&self) -> HashMap<&str, ChannelFee> {
        let mut chan_meta: HashMap<&str, ChannelFee> = HashMap::new();

        for c in &self.channels.channels {
            let meta = chan_meta.entry(&c.source).or_default();
            meta.count += 1;
            meta.fee_sum += c.fee_per_millionth;
            meta.fee_rates.insert(c.fee_per_millionth);
        }

        chan_meta
    }

    /// Get fees earned in sats for the last N months from settled forwards
    pub fn fees_earned_last_months(&self, months: i64) -> u64 {
        let days = months * 30; // Approximating 30 days per month like the bash script
        self.forward_cache
            .settled
            .iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_days() <= days)
            .map(|f| f.fee_sat)
            .sum()
    }

    /// Get total routed amount in sats for the last N months from settled forwards
    pub fn routed_last_months_sats(&self, months: i64) -> u64 {
        let days = months * 30; // Approximating 30 days per month like the bash script
        self.forward_cache
            .settled
            .iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_days() <= days)
            .map(|f| f.out_sat)
            .sum()
    }

    /// Get total channel funds in sats
    pub fn total_channel_funds_sats(&self) -> u64 {
        self.normal_channels()
            .iter()
            .map(|c| c.our_amount_msat / 1000)
            .sum()
    }

    pub fn lease_fee_totals_last_months(&self, months: i64) -> LeaseFeeTotals {
        let period_seconds = months.saturating_mul(30).saturating_mul(24 * 60 * 60);
        let start_timestamp = self.now.timestamp().saturating_sub(period_seconds);
        self.sum_lease_fees(|event| {
            let timestamp = i64::try_from(event.timestamp).unwrap_or(i64::MAX);
            timestamp >= start_timestamp && timestamp <= self.now.timestamp()
        })
    }

    pub fn lease_fee_totals_for_account(&self, account: &str) -> LeaseFeeTotals {
        self.sum_lease_fees(|event| event.account == account)
    }

    fn sum_lease_fees(&self, predicate: impl Fn(&cmd::BkprIncomeEvent) -> bool) -> LeaseFeeTotals {
        self.income_events
            .iter()
            .filter(|event| event.tag == "lease_fee" && predicate(event))
            .fold(LeaseFeeTotals::default(), |mut totals, event| {
                totals.earned_msat = totals.earned_msat.saturating_add(event.credit_msat);
                totals.paid_msat = totals.paid_msat.saturating_add(event.debit_msat);
                totals
            })
    }

    /// Calculate the effective fee rate in basis points for a period.
    pub fn calculate_effective_fee_rate_bps(&self, months: i64) -> f64 {
        let total_routed = self.routed_last_months_sats(months);
        if total_routed == 0 {
            return 0.0;
        }

        self.fees_earned_last_months(months) as f64 * 10_000.0 / total_routed as f64
    }

    /// Get ROIC data with gross and net annualized returns.
    pub fn get_roic_data(&self) -> RoicData {
        let lease_fees_1_month = self.lease_fee_totals_last_months(1);
        let lease_fees_3_months = self.lease_fee_totals_last_months(3);
        let lease_fees_6_months = self.lease_fee_totals_last_months(6);
        let lease_fees_12_months = self.lease_fee_totals_last_months(12);
        RoicData {
            fees_1_month: self.fees_earned_last_months(1),
            fees_3_months: self.fees_earned_last_months(3),
            fees_6_months: self.fees_earned_last_months(6),
            fees_12_months: self.fees_earned_last_months(12),
            total_funds: self.total_channel_funds_sats(),
            lease_fee_earnings_1_month_msat: lease_fees_1_month.earned_msat,
            lease_fee_earnings_3_months_msat: lease_fees_3_months.earned_msat,
            lease_fee_earnings_6_months_msat: lease_fees_6_months.earned_msat,
            lease_fee_earnings_12_months_msat: lease_fees_12_months.earned_msat,
            lease_fee_cost_12_months_msat: lease_fees_12_months.paid_msat,
            routed_12_months: self.routed_last_months_sats(12),
            effective_fee_rate_12_months_bps: self.calculate_effective_fee_rate_bps(12),
            rebalance_cost_12_months_msat: self.rebalance_cost_last_months_msat(12),
        }
    }

    /// Get setchannel timestamp from datastore if it exists
    pub fn get_setchannel_timestamp(&self, short_channel_id: &str) -> Option<i64> {
        self.setchannel_timestamps.get(short_channel_id).copied()
    }

    /// Get exact forwarding fees earned by a channel during the trailing day window.
    pub fn get_channel_forwarding_fees_last_days_msat(
        &self,
        short_channel_id: &str,
        days: i64,
    ) -> u64 {
        self.forward_cache
            .settled
            .iter()
            .filter(|forward| forward.out_channel == short_channel_id)
            .filter(|forward| {
                self.now
                    .signed_duration_since(forward.resolved_time)
                    .num_hours()
                    <= days * 24
            })
            .map(|forward| forward.fee_msat)
            .sum()
    }

    pub fn get_channel_forwarding_fee_totals(&self, short_channel_id: &str) -> (u64, u64) {
        self.forward_cache
            .metrics_by_channel
            .get(short_channel_id)
            .map(|metrics| (metrics.outbound_fees_sat, metrics.routed_out_sat))
            .unwrap_or_default()
    }

    /// Get historical effective fee rate in ppm for outbound forwards on a channel.
    ///
    /// This is the per-channel version of the ROIC page effective fee rate:
    /// fees earned divided by total routed amount.
    pub fn get_channel_effective_fee_ppm(&self, short_channel_id: &str) -> Option<f64> {
        let (total_fees, total_routed) = self.get_channel_forwarding_fee_totals(short_channel_id);
        if total_routed == 0 {
            return None;
        }

        Some(total_fees as f64 * 1_000_000.0 / total_routed as f64)
    }

    /// Get time-decayed full fee rate in ppm for outbound forwards of at least 1,000 sats.
    ///
    /// Uses exact millisatoshi amounts, includes the base fee, and applies a one-week
    /// half-life. The average is weighted by routed amount:
    /// sum(fee_msat * decay) divided by sum(out_msat * decay).
    pub fn get_channel_time_decayed_fee_ppm(&self, short_channel_id: &str) -> Option<f64> {
        let metrics = self
            .forward_cache
            .metrics_by_channel
            .get(short_channel_id)?;
        let weighted_fees = metrics.weighted_tppm_fees_msat;
        let weighted_routed = metrics.weighted_tppm_routed_msat;

        if weighted_routed == 0.0 {
            return None;
        }

        Some(weighted_fees * 1_000_000.0 / weighted_routed)
    }

    /// Get target-attributed rebalance cost during the trailing day window.
    pub fn get_channel_rebalance_target_cost_last_days_msat(
        &self,
        short_channel_id: &str,
        days: i64,
    ) -> u64 {
        self.rebalance_parts_last_days(days)
            .into_iter()
            .filter(|part| part.target_channel_id.as_deref() == Some(short_channel_id))
            .map(|part| part.fees_msat)
            .sum()
    }

    pub fn get_closed_channel_age_days(&self, channel: &cmd::ClosedChannel) -> Option<i64> {
        let short_channel_id = channel.short_channel_id.as_deref()?;
        let age_days_to_now = self.get_channel_age_days(short_channel_id)?;

        let last_stable_connection = channel.last_stable_connection?;
        let close_time = DateTime::from_timestamp(last_stable_connection as i64, 0)?;
        let days_since_close = self.now.signed_duration_since(close_time).num_days().max(0);

        Some((age_days_to_now - days_since_close).max(1))
    }

    /// Get channel age in days from block height (approximate)
    pub fn get_channel_age_days(&self, short_channel_id: &str) -> Option<i64> {
        // Parse block height directly from short_channel_id (format: "block_height x tx_index x output_index")
        let block_height: u64 = short_channel_id.split('x').next()?.parse().ok()?;

        // Approximate blocks per day (144 blocks per day on average)
        let blocks_per_day = 144;

        // Calculate approximate age in days
        // Note: This is approximate since we don't have the exact genesis block time
        // and block times can vary. For a more accurate calculation, we'd need
        // access to block timestamps.
        let age_blocks = self.info.blockheight.saturating_sub(block_height);
        Some((age_blocks / blocks_per_day) as i64)
    }

    /// Get fund (channel capacity info) by short_channel_id
    pub fn get_fund(&self, short_channel_id: &str) -> Option<&cmd::Fund> {
        self.funds
            .channels
            .iter()
            .find(|f| f.short_channel_id.as_deref() == Some(short_channel_id))
    }

    pub fn network_channel_fees(&self) -> (f64, f64) {
        let mut fees: Vec<u64> = self
            .channels()
            .filter(|c| c.base_fee_millisatoshi == 0 && c.fee_per_millionth <= 10000)
            .map(|c| c.fee_per_millionth)
            .collect();

        if fees.is_empty() {
            return (0.0, 0.0);
        }

        let sum: u64 = fees.iter().sum();
        let average = sum as f64 / fees.len() as f64;

        fees.sort_unstable();
        let median = if fees.len() % 2 == 0 {
            let mid = fees.len() / 2;
            (fees[mid - 1] as f64 + fees[mid] as f64) / 2.0
        } else {
            let mid = fees.len() / 2;
            fees[mid] as f64
        };

        (average, median)
    }

    pub fn node_channel_fees(&self) -> (f64, f64) {
        // Compute fees from the channel list (funds) by looking up each channel's fee info
        let mut fees: Vec<u64> = self
            .normal_channels()
            .iter()
            .filter_map(|fund| {
                let scid = fund.short_channel_id.as_ref()?;
                let channel = self.get_channel(scid, &self.info.id)?;

                Some(channel.fee_per_millionth)
            })
            .collect();

        if fees.is_empty() {
            return (0.0, 0.0);
        }

        let sum: u64 = fees.iter().sum();
        let average = sum as f64 / fees.len() as f64;

        fees.sort_unstable();
        let median = if fees.len() % 2 == 0 {
            let mid = fees.len() / 2;
            (fees[mid - 1] as f64 + fees[mid] as f64) / 2.0
        } else {
            let mid = fees.len() / 2;
            fees[mid] as f64
        };

        (average, median)
    }

    pub(crate) fn filter_forwards_by_hours(&self, hours: i64) -> Vec<Forward> {
        self.forwards
            .forwards
            .iter()
            .filter(move |f| {
                let received_time =
                    DateTime::from_timestamp(f.received_time as i64, 0).unwrap_or(self.now);
                self.now.signed_duration_since(received_time).num_hours() <= hours
            })
            .cloned()
            .collect()
    }
}

fn feature_bit_is_set(features: &str, bit: usize) -> bool {
    if features.len() % 2 != 0 {
        return false;
    }

    let byte_from_end = bit / 8;
    let Some(start) = features.len().checked_sub((byte_from_end + 1) * 2) else {
        return false;
    };
    let Ok(byte) = u8::from_str_radix(&features[start..start + 2], 16) else {
        return false;
    };

    byte & (1 << (bit % 8)) != 0
}

/// Helper struct to compute the average fee of the channels of a node
#[derive(Default)]
pub struct ChannelFee {
    pub count: u64,
    pub fee_sum: u64,
    pub fee_rates: HashSet<u64>,
}

impl ChannelFee {
    pub fn avg_fee(&self) -> f64 {
        self.fee_sum as f64 / self.count as f64
    }

    pub fn fee_diversity(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.fee_rates.len() as f64 / self.count as f64
    }
}

/// ROIC calculation data.
pub struct RoicData {
    pub fees_1_month: u64,
    pub fees_3_months: u64,
    pub fees_6_months: u64,
    pub fees_12_months: u64,
    pub total_funds: u64,
    pub lease_fee_earnings_1_month_msat: u64,
    pub lease_fee_earnings_3_months_msat: u64,
    pub lease_fee_earnings_6_months_msat: u64,
    pub lease_fee_earnings_12_months_msat: u64,
    pub lease_fee_cost_12_months_msat: u64,
    pub routed_12_months: u64,
    pub effective_fee_rate_12_months_bps: f64,
    pub rebalance_cost_12_months_msat: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const INCOMING_SCID: &str = "147440x1x0";
    const OUTGOING_SCID: &str = "147440x2x0";
    const NOW_TIMESTAMP: i64 = 2_000_000_000;

    #[test]
    fn bolt_feature_bits_are_read_from_the_rightmost_byte() {
        assert!(feature_bit_is_set("01", 0));
        assert!(feature_bit_is_set("4000000000000000", 62));
        assert!(feature_bit_is_set("8000000000000000", 63));
        assert!(!feature_bit_is_set("0000000000000000", 62));
        assert!(!feature_bit_is_set("01", 62));
        assert!(!feature_bit_is_set("not-hex", 0));
    }

    fn parse_events(json: &str) -> Vec<cmd::BkprAccountEvent> {
        serde_json::from_str::<cmd::BkprListAccountEvents>(json)
            .unwrap()
            .events
    }

    fn account_map() -> HashMap<String, String> {
        HashMap::from([
            ("source-account".to_string(), "source-scid".to_string()),
            ("target-account".to_string(), "target-scid".to_string()),
            ("other-source".to_string(), "other-source-scid".to_string()),
            ("other-target".to_string(), "other-target-scid".to_string()),
        ])
    }

    fn fund(short_channel_id: &str, amount_msat: u64) -> cmd::Fund {
        cmd::Fund {
            peer_id: "shared-peer".to_string(),
            connected: true,
            state: "CHANNELD_NORMAL".to_string(),
            channel_id: format!("channel-{short_channel_id}"),
            short_channel_id: Some(short_channel_id.to_string()),
            our_amount_msat: amount_msat / 2,
            amount_msat,
            funding_txid: "funding-txid".to_string(),
            funding_output: 0,
        }
    }

    fn forward(
        in_channel: &str,
        out_channel: &str,
        fee_msat: u64,
        status: &str,
        resolved_time: i64,
    ) -> cmd::Forward {
        forward_with_amount(
            in_channel,
            out_channel,
            fee_msat,
            1_000_000,
            status,
            resolved_time,
        )
    }

    fn forward_with_amount(
        in_channel: &str,
        out_channel: &str,
        fee_msat: u64,
        out_msat: u64,
        status: &str,
        resolved_time: i64,
    ) -> cmd::Forward {
        cmd::Forward {
            in_channel: in_channel.to_string(),
            out_channel: Some(out_channel.to_string()),
            fee_msat: Some(fee_msat),
            in_msat: out_msat + fee_msat,
            out_msat: Some(out_msat),
            status: status.to_string(),
            received_time: (resolved_time - 1) as f64,
            resolved_time: Some(resolved_time as f64),
            failreason: None,
            failcode: None,
        }
    }

    #[test]
    fn tppm_includes_full_fee_and_excludes_forwards_below_one_thousand_sats() {
        let store = test_store(
            vec![fund(OUTGOING_SCID, 1_000_000_000)],
            vec![
                forward_with_amount(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    100_000,
                    999_999,
                    "settled",
                    NOW_TIMESTAMP - 60,
                ),
                forward_with_amount(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    2_000,
                    1_000_000,
                    "settled",
                    NOW_TIMESTAMP - 60,
                ),
                forward_with_amount(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    3_000,
                    2_000_000,
                    "settled",
                    NOW_TIMESTAMP - 60,
                ),
            ],
            vec![],
        );

        let tppm = store
            .get_channel_time_decayed_fee_ppm(OUTGOING_SCID)
            .unwrap();
        assert!((tppm - 1_666.666_666_666_666_7).abs() < f64::EPSILON);
    }

    #[test]
    fn channel_profitability_metrics_use_trailing_window() {
        let recent_timestamp = NOW_TIMESTAMP - 60;
        let old_timestamp = NOW_TIMESTAMP - 91 * 24 * 60 * 60;
        let rebalance_part = |part_id, fees_msat, timestamp| RebalancePart {
            payment_id: format!("payment-{part_id}"),
            part_id,
            source_account: "source-account".to_string(),
            target_account: "target-account".to_string(),
            source_channel_id: Some(INCOMING_SCID.to_string()),
            target_channel_id: Some(OUTGOING_SCID.to_string()),
            debit_msat: 1_000_000 + fees_msat,
            credit_msat: 1_000_000,
            fees_msat,
            timestamp: Some(timestamp as u64),
        };
        let store = test_store(
            vec![fund(OUTGOING_SCID, 1_000_000_000)],
            vec![
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    3_000,
                    "settled",
                    recent_timestamp,
                ),
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    7_000,
                    "settled",
                    old_timestamp,
                ),
            ],
            vec![
                rebalance_part(0, 1_000, recent_timestamp),
                rebalance_part(1, 2_000, old_timestamp),
            ],
        );

        assert_eq!(
            store.get_channel_forwarding_fees_last_days_msat(OUTGOING_SCID, 90),
            3_000
        );
        assert_eq!(
            store.get_channel_rebalance_target_cost_last_days_msat(OUTGOING_SCID, 90),
            1_000
        );
    }

    fn test_store(
        funds: Vec<cmd::Fund>,
        forwards: Vec<cmd::Forward>,
        rebalance_parts: Vec<RebalancePart>,
    ) -> Store {
        let now = DateTime::from_timestamp(NOW_TIMESTAMP, 0).unwrap();
        let forwards = cmd::ListForwards { forwards };
        let forward_cache = build_forward_cache(&forwards, now);
        Store {
            info: cmd::GetInfo {
                id: "our-node".to_string(),
                blockheight: 200_000,
            },
            channels: cmd::ListChannels { channels: vec![] },
            peer_channels: cmd::ListPeerChannels { channels: vec![] },
            peers: cmd::ListPeers { peers: vec![] },
            funds: cmd::ListFunds {
                channels: funds,
                outputs: vec![],
            },
            forwards,
            closed_channels: cmd::ListClosedChannels {
                closedchannels: vec![],
            },
            rebalance_parts,
            income_events: vec![],
            nodes_by_id: HashMap::new(),
            channels_by_id: HashMap::new(),
            forward_cache,
            setchannel_timestamps: HashMap::new(),
            now,
            avail_map: HashMap::new(),
        }
    }

    #[test]
    fn lease_fees_are_included_in_node_and_channel_roic() {
        let rebalance_parts = vec![RebalancePart {
            payment_id: "rebalance-payment".to_string(),
            part_id: 0,
            source_account: "source-account".to_string(),
            target_account: format!("channel-{OUTGOING_SCID}"),
            source_channel_id: Some(INCOMING_SCID.to_string()),
            target_channel_id: Some(OUTGOING_SCID.to_string()),
            debit_msat: 1_002_000,
            credit_msat: 1_000_000,
            fees_msat: 2_000,
            timestamp: Some(NOW_TIMESTAMP as u64 - 60),
        }];
        let mut store = test_store(
            vec![fund(OUTGOING_SCID, 1_000_000_000)],
            vec![forward(
                INCOMING_SCID,
                OUTGOING_SCID,
                10_000,
                "settled",
                NOW_TIMESTAMP - 60,
            )],
            rebalance_parts,
        );
        store.income_events = vec![
            cmd::BkprIncomeEvent {
                account: format!("channel-{OUTGOING_SCID}"),
                tag: "lease_fee".to_string(),
                credit_msat: 50_000,
                debit_msat: 0,
                timestamp: NOW_TIMESTAMP as u64 - 60,
            },
            cmd::BkprIncomeEvent {
                account: format!("channel-{OUTGOING_SCID}"),
                tag: "lease_fee".to_string(),
                credit_msat: 0,
                debit_msat: 5_000,
                timestamp: NOW_TIMESTAMP as u64 - 60,
            },
        ];

        assert_eq!(
            store.lease_fee_totals_last_months(1),
            LeaseFeeTotals {
                earned_msat: 50_000,
                paid_msat: 5_000,
            }
        );
    }

    #[test]
    fn closed_channel_lifetime_requires_a_closure_timestamp() {
        let store = test_store(vec![], vec![], vec![]);
        let mut channel = cmd::ClosedChannel {
            channel_id: "closed-channel".to_string(),
            peer_id: None,
            short_channel_id: Some(INCOMING_SCID.to_string()),
            opener: "local".to_string(),
            closer: None,
            total_htlcs_sent: None,
            total_msat: 1_000_000,
            funding_txid: "funding-txid".to_string(),
            final_to_us_msat: 500_000,
            last_commitment_txid: None,
            last_stable_connection: None,
            close_cause: "unknown".to_string(),
        };

        assert_eq!(store.get_closed_channel_age_days(&channel), None);

        channel.last_stable_connection = Some((NOW_TIMESTAMP - 100 * 24 * 60 * 60) as u64);
        assert_eq!(store.get_closed_channel_age_days(&channel), Some(265));
    }

    #[test]
    fn rebalance_matching_builds_debit_credit_pair_with_fees() {
        let events = parse_events(
            r#"{
                "events": [
                    {
                        "account": "source-account",
                        "tag": "routed",
                        "credit_msat": 0,
                        "debit_msat": 100500,
                        "timestamp": 1000,
                        "payment_id": "payment-1",
                        "fees_msat": 500,
                        "is_rebalance": true,
                        "part_id": 0
                    },
                    {
                        "account": "target-account",
                        "tag": "routed",
                        "credit_msat": 100000,
                        "debit_msat": 0,
                        "timestamp": 1000,
                        "payment_id": "payment-1",
                        "is_rebalance": true,
                        "part_id": 0
                    }
                ]
            }"#,
        );

        let parts = match_rebalance_parts(&events, &account_map());

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].payment_id, "payment-1");
        assert_eq!(parts[0].part_id, 0);
        assert_eq!(parts[0].source_channel_id.as_deref(), Some("source-scid"));
        assert_eq!(parts[0].target_channel_id.as_deref(), Some("target-scid"));
        assert_eq!(parts[0].fees_msat, 500);
        assert_eq!(parts[0].debit_msat, 100500);
        assert_eq!(parts[0].credit_msat, 100000);

        let target_rebalance_ppm =
            parts[0].fees_msat as f64 * 1_000_000.0 / parts[0].credit_msat as f64;
        assert_eq!(target_rebalance_ppm, 5000.0);
    }

    #[test]
    fn rebalance_matching_keeps_multiple_parts_for_same_payment() {
        let events = parse_events(
            r#"{
                "events": [
                    {
                        "account": "source-account",
                        "credit_msat": 0,
                        "debit_msat": 100100,
                        "payment_id": "payment-2",
                        "fees_msat": 100,
                        "is_rebalance": true,
                        "part_id": 0
                    },
                    {
                        "account": "target-account",
                        "credit_msat": 100000,
                        "debit_msat": 0,
                        "payment_id": "payment-2",
                        "is_rebalance": true,
                        "part_id": 0
                    },
                    {
                        "account": "other-source",
                        "credit_msat": 0,
                        "debit_msat": 200200,
                        "payment_id": "payment-2",
                        "fees_msat": 200,
                        "is_rebalance": true,
                        "part_id": 1
                    },
                    {
                        "account": "other-target",
                        "credit_msat": 200000,
                        "debit_msat": 0,
                        "payment_id": "payment-2",
                        "is_rebalance": true,
                        "part_id": 1
                    }
                ]
            }"#,
        );

        let mut parts = match_rebalance_parts(&events, &account_map());
        parts.sort_by_key(|part| part.part_id);

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].payment_id, "payment-2");
        assert_eq!(parts[0].part_id, 0);
        assert_eq!(parts[0].fees_msat, 100);
        assert_eq!(parts[1].payment_id, "payment-2");
        assert_eq!(parts[1].part_id, 1);
        assert_eq!(parts[1].fees_msat, 200);
        assert_eq!(parts.iter().map(|part| part.fees_msat).sum::<u64>(), 300);
    }

    #[test]
    fn rebalance_matching_treats_missing_or_null_fees_as_zero() {
        let events = parse_events(
            r#"{
                "events": [
                    {
                        "account": "source-account",
                        "credit_msat": 0,
                        "debit_msat": 100000,
                        "payment_id": "payment-3",
                        "fees_msat": null,
                        "is_rebalance": true,
                        "part_id": 0
                    },
                    {
                        "account": "target-account",
                        "credit_msat": 100000,
                        "debit_msat": 0,
                        "payment_id": "payment-3",
                        "is_rebalance": true,
                        "part_id": 0
                    }
                ]
            }"#,
        );

        let parts = match_rebalance_parts(&events, &account_map());

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].fees_msat, 0);
    }

    #[test]
    fn rebalance_matching_ignores_unmatched_rows_without_panic() {
        let events = parse_events(
            r#"{
                "events": [
                    {
                        "account": "source-account",
                        "credit_msat": 0,
                        "debit_msat": 100500,
                        "payment_id": "payment-4",
                        "fees_msat": 500,
                        "is_rebalance": true,
                        "part_id": 0
                    },
                    {
                        "account": "target-account",
                        "credit_msat": 100000,
                        "debit_msat": 0,
                        "payment_id": "payment-5",
                        "is_rebalance": true,
                        "part_id": 0
                    }
                ]
            }"#,
        );

        let parts = match_rebalance_parts(&events, &account_map());

        assert!(parts.is_empty());
    }

    #[test]
    fn rebalance_matching_includes_self_invoice_pairs_without_rebalance_flag() {
        let events = parse_events(
            r#"{
                "events": [
                    {
                        "account": "source-account",
                        "tag": "invoice",
                        "credit_msat": 0,
                        "debit_msat": 10005011,
                        "payment_id": "payment-6",
                        "fees_msat": 5011,
                        "is_rebalance": false,
                        "part_id": 0,
                        "timestamp": 1783711902
                    },
                    {
                        "account": "target-account",
                        "tag": "invoice",
                        "credit_msat": 10000000,
                        "debit_msat": 0,
                        "payment_id": "payment-6",
                        "is_rebalance": false,
                        "part_id": 0,
                        "timestamp": 1783711903
                    },
                    {
                        "account": "other-source",
                        "tag": "routed",
                        "credit_msat": 0,
                        "debit_msat": 5000000,
                        "payment_id": "forward-1",
                        "fees_msat": 100,
                        "is_rebalance": false,
                        "part_id": 0
                    },
                    {
                        "account": "other-target",
                        "tag": "routed",
                        "credit_msat": 5000100,
                        "debit_msat": 0,
                        "payment_id": "forward-1",
                        "fees_msat": 100,
                        "is_rebalance": false,
                        "part_id": 0
                    }
                ]
            }"#,
        );

        let parts = match_rebalance_parts(&events, &account_map());

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].payment_id, "payment-6");
        assert_eq!(parts[0].source_channel_id.as_deref(), Some("source-scid"));
        assert_eq!(parts[0].target_channel_id.as_deref(), Some("target-scid"));
        assert_eq!(parts[0].debit_msat, 10005011);
        assert_eq!(parts[0].credit_msat, 10000000);
        assert_eq!(parts[0].fees_msat, 5011);
        assert_eq!(parts[0].timestamp, Some(1783711902));
    }

    #[cfg(feature = "large-fixture-tests")]
    #[test]
    fn gz_bkpr_fixture_matches_expected_rebalance_parts() {
        let events = cmd::bkpr_list_account_events();
        let parts = match_rebalance_parts(&events.events, &HashMap::new());

        assert_eq!(parts.len(), 1827);
        assert_eq!(
            parts.iter().map(|part| part.fees_msat).sum::<u64>(),
            8_276_748
        );
    }
}
