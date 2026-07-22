use crate::cmd::{self, datastore_string, DatastoreMode, Forward, SettledForward};
use crate::common::ChannelFee;
use chrono::{DateTime, Datelike, Utc};
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
    pub peers: cmd::ListPeers,
    pub funds: cmd::ListFunds,
    pub forwards: cmd::ListForwards,
    pub nodes: cmd::ListNodes,
    pub closed_channels: cmd::ListClosedChannels,
    rebalance_parts: Vec<RebalancePart>,
    income_events: Vec<cmd::BkprIncomeEvent>,
    // Cached computed data
    nodes_by_id: HashMap<String, cmd::Node>,
    channels_by_id: HashMap<(String, String), cmd::Channel>,
    node_channel_counts: HashMap<String, usize>,
    forward_cache: ForwardCache,
    peer_notes: HashMap<String, String>,
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

fn annualized_channel_capacity_return_percent(
    revenue_msat: i128,
    channel_capacity_msat: u64,
    age_days: i64,
) -> f64 {
    if age_days <= 0 || channel_capacity_msat == 0 {
        return 0.0;
    }

    (revenue_msat as f64 / channel_capacity_msat as f64) * (365.0 / age_days as f64) * 100.0
}

#[derive(Default)]
struct ChannelForwardMetrics {
    settled_count: usize,
    outbound_fees_sat: u64,
    indirect_fees_sat: u64,
    routed_out_sat: u64,
    weighted_fees_sat: f64,
    weighted_variable_fees_sat: f64,
    weighted_routed_sat: f64,
}

#[derive(Default)]
struct ForwardCache {
    settled: Vec<SettledForward>,
    metrics_by_channel: HashMap<String, ChannelForwardMetrics>,
    settled_indices_by_channel: HashMap<String, Vec<usize>>,
    local_failed_indices_by_channel: HashMap<String, Vec<usize>>,
    failed_indices_by_channel: HashMap<String, Vec<usize>>,
}

fn build_forward_cache(forwards: &cmd::ListForwards, now: DateTime<Utc>) -> ForwardCache {
    const HALF_LIFE_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
    const OUR_BASE_FEE_SAT: u64 = 1;

    let mut settled: Vec<_> = forwards
        .forwards
        .iter()
        .filter(|forward| forward.status == "settled")
        .map(|forward| SettledForward::try_from(forward.clone()).unwrap())
        .collect();
    settled.sort_by(|a, b| b.resolved_time.cmp(&a.resolved_time));

    let mut metrics_by_channel: HashMap<String, ChannelForwardMetrics> = HashMap::new();
    let mut settled_indices_by_channel: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, forward) in settled.iter().enumerate() {
        let incoming = metrics_by_channel
            .entry(forward.in_channel.clone())
            .or_default();
        incoming.settled_count += 1;
        incoming.indirect_fees_sat += forward.fee_sat;
        settled_indices_by_channel
            .entry(forward.in_channel.clone())
            .or_default()
            .push(index);

        let outgoing = metrics_by_channel
            .entry(forward.out_channel.clone())
            .or_default();
        if forward.out_channel != forward.in_channel {
            outgoing.settled_count += 1;
            settled_indices_by_channel
                .entry(forward.out_channel.clone())
                .or_default()
                .push(index);
        }
        outgoing.outbound_fees_sat += forward.fee_sat;
        outgoing.routed_out_sat += forward.out_sat;

        let age_seconds = now
            .signed_duration_since(forward.resolved_time)
            .num_seconds()
            .max(0) as f64;
        let decay = 0.5_f64.powf(age_seconds / HALF_LIFE_SECONDS);
        outgoing.weighted_fees_sat += forward.fee_sat as f64 * decay;
        outgoing.weighted_variable_fees_sat +=
            forward.fee_sat.saturating_sub(OUR_BASE_FEE_SAT) as f64 * decay;
        outgoing.weighted_routed_sat += forward.out_sat as f64 * decay;
    }

    let mut local_failed_indices_by_channel: HashMap<String, Vec<usize>> = HashMap::new();
    let mut failed_indices_by_channel: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, forward) in forwards.forwards.iter().enumerate() {
        let indices_by_channel = match forward.status.as_str() {
            "local_failed" => &mut local_failed_indices_by_channel,
            "failed" => &mut failed_indices_by_channel,
            _ => continue,
        };
        indices_by_channel
            .entry(forward.in_channel.clone())
            .or_default()
            .push(index);
        if let Some(out_channel) = &forward.out_channel {
            if out_channel != &forward.in_channel {
                indices_by_channel
                    .entry(out_channel.clone())
                    .or_default()
                    .push(index);
            }
        }
    }

    for indices in local_failed_indices_by_channel
        .values_mut()
        .chain(failed_indices_by_channel.values_mut())
    {
        indices.sort_by(|a, b| {
            forwards.forwards[*b]
                .received_time
                .partial_cmp(&forwards.forwards[*a].received_time)
                .unwrap()
        });
    }

    ForwardCache {
        settled,
        metrics_by_channel,
        settled_indices_by_channel,
        local_failed_indices_by_channel,
        failed_indices_by_channel,
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

        // Precompute node channel counts
        let mut node_channel_counts: HashMap<String, usize> = HashMap::new();
        for channel in channels.channels.iter() {
            *node_channel_counts
                .entry(channel.source.clone())
                .or_insert(0) += 1;
            *node_channel_counts
                .entry(channel.destination.clone())
                .or_insert(0) += 1;
        }

        // Query peer notes from datastore
        let mut peer_notes = HashMap::new();
        if let Ok(datastore) = cmd::listdatastore(Some(&["lightdash", "peer_note"])) {
            log::info!(
                "Loaded {} peer notes from datastore",
                datastore.datastore.len()
            );
            for entry in datastore.datastore {
                // The key format is ["lightdash", "peer_note", "peer_id"]
                if entry.key.len() == 3
                    && entry.key[0] == "lightdash"
                    && entry.key[1] == "peer_note"
                {
                    let peer_id = &entry.key[2];
                    if let Some(note) = &entry.string {
                        peer_notes.insert(peer_id.clone(), note.clone());
                    }
                }
            }
        }

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

        let timestamp = Utc::now().timestamp().to_string();
        let result = datastore_string(
            &["lightdash", "last_run", &timestamp],
            &timestamp,
            DatastoreMode::CreateOrReplace,
        )
        .unwrap();

        log::info!("Last run timestamp saved: {:?}", result);

        let store = Self {
            info,
            channels,
            peers,
            funds,
            forwards,
            nodes,
            closed_channels,
            rebalance_parts,
            income_events,
            nodes_by_id,
            channels_by_id,
            node_channel_counts,
            forward_cache,
            peer_notes,
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

    /// Filter settled forwards to only include those resolved within the last N days
    pub fn filter_settled_forwards_by_days(&self, days: i64) -> Vec<SettledForward> {
        self.forward_cache
            .settled
            .iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_hours() <= days * 24)
            .cloned()
            .collect()
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

    pub fn peers(&self) -> impl Iterator<Item = &cmd::Peer> {
        self.peers.peers.iter()
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

    pub fn nodes_len(&self) -> usize {
        self.nodes.nodes.len()
    }

    pub fn nodes(&self) -> impl Iterator<Item = &cmd::Node> {
        self.nodes.nodes.iter()
    }

    pub fn node_total_channels(&self, nodeid: &str) -> usize {
        *self.node_channel_counts.get(nodeid).unwrap_or(&0)
    }

    /// Get all channels connected to a specific node
    pub fn get_node_channels(&self, nodeid: &str) -> Vec<&cmd::Channel> {
        self.channels()
            .filter(|channel| channel.source == nodeid)
            .collect()
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

    /// Get a vector of 7 elements counting settled forwards by weekday
    /// Index 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    pub fn forwards_by_weekday(&self) -> Vec<usize> {
        let mut weekday_counts = vec![0; 7];

        for forward in &self.forward_cache.settled {
            let weekday = forward.resolved_time.weekday();
            let index = match weekday {
                chrono::Weekday::Sun => 0,
                chrono::Weekday::Mon => 1,
                chrono::Weekday::Tue => 2,
                chrono::Weekday::Wed => 3,
                chrono::Weekday::Thu => 4,
                chrono::Weekday::Fri => 5,
                chrono::Weekday::Sat => 6,
            };
            weekday_counts[index] += 1;
        }

        weekday_counts
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

    /// Get total channel funds in BTC
    pub fn channels_balance_btc(&self) -> f64 {
        (self.total_channel_funds_sats() as f64) / 100_000_000.0
    }

    /// Get total onchain balance in BTC
    pub fn onchain_balance_btc(&self) -> f64 {
        let total_msat: u64 = self.funds.outputs.iter().map(|o| o.amount_msat).sum();
        // Convert from msat to sats, then to BTC
        (total_msat as f64) / 1_000.0 / 100_000_000.0
    }

    /// Get total wallet balance in BTC
    pub fn total_balance_btc(&self) -> f64 {
        self.onchain_balance_btc() + self.channels_balance_btc()
    }

    /// Calculate annualized gross ROIC for the given time period.
    pub fn calculate_gross_roic_percent(&self, months: i64) -> f64 {
        let total_funds = self.total_channel_funds_sats();

        if total_funds == 0 {
            return 0.0;
        }

        let lease_fees = self.lease_fee_totals_last_months(months);
        let gross_revenue_msat =
            self.fees_earned_last_months(months) as u128 * 1000 + lease_fees.earned_msat as u128;
        let annualization_factor = 12.0 / months as f64;
        (gross_revenue_msat as f64 / 1000.0 * 100.0 * annualization_factor) / total_funds as f64
    }

    pub fn calculate_net_roic_percent(&self, months: i64) -> f64 {
        let total_funds = self.total_channel_funds_sats();
        if total_funds == 0 {
            return 0.0;
        }

        let lease_fees = self.lease_fee_totals_last_months(months);
        let net_fees_msat = self.fees_earned_last_months(months) as i128 * 1000
            + lease_fees.earned_msat as i128
            - lease_fees.paid_msat as i128
            - self.rebalance_cost_last_months_msat(months) as i128;
        let annualization_factor = 12.0 / months as f64;
        (net_fees_msat as f64 / 1000.0 * 100.0 * annualization_factor) / total_funds as f64
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

    /// Get total amount transacted in sats for the last month
    pub fn transacted_last_month_sats(&self) -> u64 {
        self.routed_last_months_sats(1)
    }

    /// Calculate the annualized capital velocity for a period.
    ///
    /// This follows the ROIC decomposition convention: routed volume divided by
    /// deployed channel funds, annualized for periods shorter than one year.
    pub fn calculate_capital_velocity(&self, months: i64) -> f64 {
        let total_funds = self.total_channel_funds_sats();
        if total_funds == 0 {
            return 0.0;
        }

        let annualization_factor = 12.0 / months as f64;
        self.routed_last_months_sats(months) as f64 * annualization_factor / total_funds as f64
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
            gross_roic_1_month: self.calculate_gross_roic_percent(1),
            gross_roic_3_months: self.calculate_gross_roic_percent(3),
            gross_roic_6_months: self.calculate_gross_roic_percent(6),
            gross_roic_12_months: self.calculate_gross_roic_percent(12),
            lease_fee_earnings_1_month_msat: lease_fees_1_month.earned_msat,
            lease_fee_earnings_3_months_msat: lease_fees_3_months.earned_msat,
            lease_fee_earnings_6_months_msat: lease_fees_6_months.earned_msat,
            lease_fee_earnings_12_months_msat: lease_fees_12_months.earned_msat,
            lease_fee_cost_12_months_msat: lease_fees_12_months.paid_msat,
            transacted_last_month: self.transacted_last_month_sats(),
            routed_12_months: self.routed_last_months_sats(12),
            capital_velocity_12_months: self.calculate_capital_velocity(12),
            effective_fee_rate_12_months_bps: self.calculate_effective_fee_rate_bps(12),
            rebalance_cost_12_months_msat: self.rebalance_cost_last_months_msat(12),
            net_roic_12_months: self.calculate_net_roic_percent(12),
        }
    }

    /// Get closed channels with enriched information (alias)
    pub fn get_closed_channels_info(&self) -> Vec<ClosedChannelInfo> {
        let mut closed_channels: Vec<ClosedChannelInfo> = self
            .closed_channels
            .closedchannels
            .iter()
            .map(|channel| {
                let alias = if let Some(peer_id) = &channel.peer_id {
                    self.get_node_alias(peer_id)
                } else {
                    "Unknown Peer".to_string()
                };
                let opening_block = channel.block_born();

                ClosedChannelInfo {
                    channel: channel.clone(),
                    alias,
                    opening_block,
                }
            })
            .collect();

        // Sort by short_channel_id like in the bash script
        closed_channels.sort_by(|a, b| {
            a.channel
                .short_channel_id_display()
                .cmp(&b.channel.short_channel_id_display())
        });

        closed_channels
    }

    /// Get the number of closed channels
    pub fn closed_channels_len(&self) -> usize {
        self.closed_channels.closedchannels.len()
    }

    /// Get peer note from datastore if it exists
    pub fn get_peer_note(&self, peer_id: &str) -> Option<&String> {
        self.peer_notes.get(peer_id)
    }

    /// Get setchannel timestamp from datastore if it exists
    pub fn get_setchannel_timestamp(&self, short_channel_id: &str) -> Option<i64> {
        self.setchannel_timestamps.get(short_channel_id).copied()
    }

    /// Get fee distribution data for a specific peer based on its channels
    pub fn get_peer_fee_distribution(&self, peer_id: &str) -> FeeDistribution {
        let ppm_ranges = Self::generate_ppm_ranges();

        let mut outgoing_amounts: Vec<u64> = vec![0; ppm_ranges.len()];
        let mut incoming_amounts: Vec<u64> = vec![0; ppm_ranges.len()];

        for c in self.channels.channels.iter() {
            if c.destination == peer_id {
                for (i, (min_ppm, max_ppm, _)) in ppm_ranges.iter().enumerate() {
                    if c.fee_per_millionth >= *min_ppm && c.fee_per_millionth <= *max_ppm {
                        outgoing_amounts[i] += c.amount_msat / 1000;
                    }
                }
            }
            if c.source == peer_id {
                for (i, (min_ppm, max_ppm, _)) in ppm_ranges.iter().enumerate() {
                    if c.fee_per_millionth >= *min_ppm && c.fee_per_millionth <= *max_ppm {
                        incoming_amounts[i] += c.amount_msat / 1000;
                    }
                }
            }
        }

        FeeDistribution {
            labels: ppm_ranges
                .iter()
                .map(|(_, _, label)| label.to_string())
                .collect(),
            outgoing_amounts,
            incoming_amounts,
        }
    }

    /// Get fee statistics (mean and median) for a specific peer
    pub fn get_peer_fee_stats(&self, peer_id: &str) -> FeeStats {
        // Collect all outgoing channel fees (peer is destination) with their amounts
        let mut outgoing_fees: Vec<(u64, u64)> = self
            .channels
            .channels
            .iter()
            .filter(|c| c.destination == peer_id)
            .map(|c| (c.fee_per_millionth, c.amount_msat / 1000))
            .collect();

        // Collect all incoming channel fees (peer is source) with their amounts
        let mut incoming_fees: Vec<(u64, u64)> = self
            .channels
            .channels
            .iter()
            .filter(|c| c.source == peer_id)
            .map(|c| (c.fee_per_millionth, c.amount_msat / 1000))
            .collect();

        FeeStats {
            outgoing_mean: Self::calculate_weighted_mean(&outgoing_fees),
            outgoing_median: Self::calculate_weighted_median(&mut outgoing_fees),
            incoming_mean: Self::calculate_weighted_mean(&incoming_fees),
            incoming_median: Self::calculate_weighted_median(&mut incoming_fees),
        }
    }

    /// Calculate weighted mean from (fee, amount) pairs
    fn calculate_weighted_mean(fees: &[(u64, u64)]) -> u64 {
        if fees.is_empty() {
            return 0;
        }

        let total_weighted: u64 = fees.iter().map(|(fee, amount)| fee * amount).sum();
        let total_amount: u64 = fees.iter().map(|(_, amount)| amount).sum();

        if total_amount == 0 {
            0
        } else {
            total_weighted / total_amount
        }
    }

    /// Calculate weighted median from (fee, amount) pairs
    fn calculate_weighted_median(fees: &mut [(u64, u64)]) -> u64 {
        if fees.is_empty() {
            return 0;
        }

        // Sort by fee
        fees.sort_by_key(|(fee, _)| *fee);

        let total_amount: u64 = fees.iter().map(|(_, amount)| amount).sum();
        if total_amount == 0 {
            return 0;
        }

        let half_total = total_amount / 2;
        let mut cumulative = 0u64;

        for (fee, amount) in fees.iter() {
            cumulative += amount;
            if cumulative >= half_total {
                return *fee;
            }
        }

        // Fallback to last fee (shouldn't reach here)
        fees.last().map(|(fee, _)| *fee).unwrap_or(0)
    }

    fn generate_ppm_ranges() -> Vec<(u64, u64, String)> {
        vec![
            // 0 to 10 by 2
            (0, 1, "0-1 ppm".to_string()),
            (2, 3, "2-3 ppm".to_string()),
            (4, 5, "4-5 ppm".to_string()),
            (6, 7, "6-7 ppm".to_string()),
            (8, 10, "8-10 ppm".to_string()),
            // 11 to 100 by 20
            (11, 30, "11-30 ppm".to_string()),
            (31, 50, "31-50 ppm".to_string()),
            (51, 70, "51-70 ppm".to_string()),
            (71, 100, "71-100 ppm".to_string()),
            // 100 to 1000 by 200
            (101, 300, "101-300 ppm".to_string()),
            (301, 500, "301-500 ppm".to_string()),
            (501, 700, "501-700 ppm".to_string()),
            (701, 1000, "701-1000 ppm".to_string()),
            // 1000 to 5000 by 2000
            (1001, 3000, "1001-3000 ppm".to_string()),
            (3001, 5000, "3001-5000 ppm".to_string()),
        ]
    }

    /// Get total number of forwards for a specific channel (both inbound and outbound)
    pub fn get_channel_total_forwards(&self, short_channel_id: &str) -> usize {
        self.forward_cache
            .metrics_by_channel
            .get(short_channel_id)
            .map(|metrics| metrics.settled_count)
            .unwrap_or_default()
    }

    /// Get total fees earned for a specific channel (from outbound forwards)
    pub fn get_channel_total_fees(&self, short_channel_id: &str) -> u64 {
        self.forward_cache
            .metrics_by_channel
            .get(short_channel_id)
            .map(|metrics| metrics.outbound_fees_sat)
            .unwrap_or_default()
    }

    /// Get fees attributed indirectly to a channel acting as the incoming channel.
    pub fn get_channel_indirect_fees(&self, short_channel_id: &str) -> u64 {
        self.forward_cache
            .metrics_by_channel
            .get(short_channel_id)
            .map(|metrics| metrics.indirect_fees_sat)
            .unwrap_or_default()
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

    /// Get time-decayed effective fee rate in ppm for outbound forwards on a channel.
    ///
    /// Uses a 1-week half-life. The average is still weighted by routed amount:
    /// sum(fee * decay) divided by sum(amount * decay).
    pub fn get_channel_time_decayed_effective_fee_ppm(
        &self,
        short_channel_id: &str,
    ) -> Option<f64> {
        let metrics = self
            .forward_cache
            .metrics_by_channel
            .get(short_channel_id)?;
        let weighted_fees = metrics.weighted_fees_sat;
        let weighted_routed = metrics.weighted_routed_sat;

        if weighted_routed == 0.0 {
            return None;
        }

        Some(weighted_fees * 1_000_000.0 / weighted_routed)
    }

    /// Get time-decayed variable fee rate in ppm after removing our fixed 1 sat base fee.
    ///
    /// This keeps the same 1-week half-life and amount weighting as TPPM, but reduces
    /// each outbound forward fee by 1000 msat before averaging.
    pub fn get_channel_time_decayed_variable_fee_ppm(&self, short_channel_id: &str) -> Option<f64> {
        let metrics = self
            .forward_cache
            .metrics_by_channel
            .get(short_channel_id)?;
        let weighted_fees = metrics.weighted_variable_fees_sat;
        let weighted_routed = metrics.weighted_routed_sat;

        if weighted_routed == 0.0 {
            return None;
        }

        Some(weighted_fees * 1_000_000.0 / weighted_routed)
    }

    pub fn get_channel_rebalance_target_cost_msat(&self, short_channel_id: &str) -> u64 {
        self.rebalance_parts
            .iter()
            .filter(|part| part.target_channel_id.as_deref() == Some(short_channel_id))
            .map(|part| part.fees_msat)
            .sum()
    }

    pub fn get_channel_rebalance_effective_fee_ppm(&self, short_channel_id: &str) -> Option<f64> {
        let (fees_msat, credited_msat) = self.get_channel_rebalance_target_totals(short_channel_id);
        if credited_msat == 0 {
            return None;
        }

        Some(fees_msat as f64 * 1_000_000.0 / credited_msat as f64)
    }

    pub fn get_channel_rebalance_target_totals(&self, short_channel_id: &str) -> (u64, u64) {
        self.rebalance_parts
            .iter()
            .filter(|part| part.target_channel_id.as_deref() == Some(short_channel_id))
            .fold((0u64, 0u64), |(fees, credited), part| {
                (fees + part.fees_msat, credited + part.credit_msat)
            })
    }

    pub fn get_channel_rebalance_source_cost_msat(&self, short_channel_id: &str) -> u64 {
        self.rebalance_parts
            .iter()
            .filter(|part| part.source_channel_id.as_deref() == Some(short_channel_id))
            .map(|part| part.fees_msat)
            .sum()
    }

    pub fn get_channel_rebalance_target_part_count(&self, short_channel_id: &str) -> usize {
        self.rebalance_parts
            .iter()
            .filter(|part| part.target_channel_id.as_deref() == Some(short_channel_id))
            .count()
    }

    pub fn get_channel_rebalance_target_payment_count(&self, short_channel_id: &str) -> usize {
        self.rebalance_parts
            .iter()
            .filter(|part| part.target_channel_id.as_deref() == Some(short_channel_id))
            .map(|part| part.payment_id.as_str())
            .collect::<HashSet<_>>()
            .len()
    }

    fn channel_rebalance_target_timestamps<'a>(
        &'a self,
        short_channel_id: &'a str,
    ) -> impl Iterator<Item = DateTime<Utc>> + 'a {
        self.rebalance_parts
            .iter()
            .filter(move |part| part.target_channel_id.as_deref() == Some(short_channel_id))
            .filter_map(|part| part.timestamp)
            .filter_map(|timestamp| DateTime::from_timestamp(timestamp as i64, 0))
    }

    pub fn get_channel_first_rebalance_target_timestamp(
        &self,
        short_channel_id: &str,
    ) -> Option<DateTime<Utc>> {
        self.channel_rebalance_target_timestamps(short_channel_id)
            .min()
    }

    pub fn get_channel_last_rebalance_target_timestamp(
        &self,
        short_channel_id: &str,
    ) -> Option<DateTime<Utc>> {
        self.channel_rebalance_target_timestamps(short_channel_id)
            .max()
    }

    pub fn get_channel_net_routing_revenue_msat(&self, short_channel_id: &str) -> i64 {
        self.get_channel_total_fees(short_channel_id) as i64 * 1000
            - self.get_channel_rebalance_target_cost_msat(short_channel_id) as i64
    }

    pub fn get_channel_net_revenue_msat(&self, short_channel_id: &str) -> i128 {
        let Some(fund) = self.get_fund(short_channel_id) else {
            return self.get_channel_net_routing_revenue_msat(short_channel_id) as i128;
        };
        let lease_fees = self.lease_fee_totals_for_account(&fund.channel_id);
        self.get_channel_net_routing_revenue_msat(short_channel_id) as i128
            + lease_fees.earned_msat as i128
            - lease_fees.paid_msat as i128
    }

    pub fn get_channel_net_capacity_return(&self, short_channel_id: &str) -> Option<f64> {
        let age_days = self.get_channel_age_days(short_channel_id)?;
        let fund = self.get_fund(short_channel_id)?;
        let net_revenue_msat = self.get_channel_net_revenue_msat(short_channel_id);
        Some(annualized_channel_capacity_return_percent(
            net_revenue_msat,
            fund.amount_msat,
            age_days,
        ))
    }

    pub fn get_channel_indirect_capacity_contribution(
        &self,
        short_channel_id: &str,
    ) -> Option<f64> {
        let age_days = self.get_channel_age_days(short_channel_id)?;
        let fund = self.get_fund(short_channel_id)?;
        let indirect_fees_msat = self.get_channel_indirect_fees(short_channel_id) as i128 * 1000;
        Some(annualized_channel_capacity_return_percent(
            indirect_fees_msat,
            fund.amount_msat,
            age_days,
        ))
    }

    pub fn get_closed_channel_net_capacity_return(
        &self,
        channel: &cmd::ClosedChannel,
    ) -> Option<f64> {
        let short_channel_id = channel.short_channel_id.as_deref()?;
        let age_days = self.get_closed_channel_age_days(channel)?;
        let lease_fees = self.lease_fee_totals_for_account(&channel.channel_id);
        let net_revenue_msat = self.get_channel_net_routing_revenue_msat(short_channel_id) as i128
            + lease_fees.earned_msat as i128
            - lease_fees.paid_msat as i128;
        Some(annualized_channel_capacity_return_percent(
            net_revenue_msat,
            channel.total_msat,
            age_days,
        ))
    }

    pub fn get_closed_channel_indirect_capacity_contribution(
        &self,
        channel: &cmd::ClosedChannel,
    ) -> Option<f64> {
        let short_channel_id = channel.short_channel_id.as_deref()?;
        let age_days = self.get_closed_channel_age_days(channel)?;
        let indirect_fees_msat = self.get_channel_indirect_fees(short_channel_id) as i128 * 1000;
        Some(annualized_channel_capacity_return_percent(
            indirect_fees_msat,
            channel.total_msat,
            age_days,
        ))
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

    /// Get the annualized gross return based on forwarding and earned lease fees.
    pub fn get_channel_gross_capacity_return(&self, short_channel_id: &str) -> Option<f64> {
        let age_days = self.get_channel_age_days(short_channel_id)?;
        let fund = self.get_fund(short_channel_id)?;
        let lease_fees = self.lease_fee_totals_for_account(&fund.channel_id);
        let gross_revenue_msat = self.get_channel_total_fees(short_channel_id) as i128 * 1000
            + lease_fees.earned_msat as i128;
        Some(annualized_channel_capacity_return_percent(
            gross_revenue_msat,
            fund.amount_msat,
            age_days,
        ))
    }

    /// Get all settled forwards for a specific channel (both inbound and outbound)
    pub fn get_channel_forwards(&self, short_channel_id: &str) -> Vec<SettledForward> {
        self.forward_cache
            .settled_indices_by_channel
            .get(short_channel_id)
            .into_iter()
            .flatten()
            .map(|index| self.forward_cache.settled[*index].clone())
            .collect()
    }

    /// Get local_failed forwards for a specific channel (both inbound and outbound)
    pub fn get_channel_local_failed_forwards(&self, short_channel_id: &str) -> Vec<Forward> {
        self.forwards_for_channel(
            short_channel_id,
            &self.forward_cache.local_failed_indices_by_channel,
        )
    }

    /// Get failed forwards for a specific channel (both inbound and outbound)
    pub fn get_channel_failed_forwards(&self, short_channel_id: &str) -> Vec<Forward> {
        self.forwards_for_channel(
            short_channel_id,
            &self.forward_cache.failed_indices_by_channel,
        )
    }

    fn forwards_for_channel(
        &self,
        short_channel_id: &str,
        indices_by_channel: &HashMap<String, Vec<usize>>,
    ) -> Vec<Forward> {
        indices_by_channel
            .get(short_channel_id)
            .into_iter()
            .flatten()
            .map(|index| self.forwards.forwards[*index].clone())
            .collect()
    }

    /// Get local_failed forwards with WIRE_TEMPORARY_CHANNEL_FAILURE grouped by out_channel
    /// Returns a vector of ChannelFailureData sorted by month count descending
    pub fn local_failed_temp_channel_failure_by_out_channel(&self) -> Vec<ChannelFailureData> {
        let mut channel_day_counts: HashMap<String, usize> = HashMap::new();
        let mut channel_week_counts: HashMap<String, usize> = HashMap::new();
        let mut channel_month_counts: HashMap<String, usize> = HashMap::new();

        for forward in &self.forwards.forwards {
            if let Some(received) = DateTime::from_timestamp(forward.received_time as i64, 0) {
                if self.now.signed_duration_since(received).num_days() > 365 {
                    continue;
                }

                if forward.status == "local_failed" && forward.out_channel.is_some() {
                    let channel = forward.out_channel.as_ref().unwrap();
                    let hours_ago = self.now.signed_duration_since(received).num_hours();

                    // Count failures by period
                    if hours_ago <= 24 {
                        *channel_day_counts.entry(channel.clone()).or_insert(0) += 1;
                    }
                    if hours_ago <= 7 * 24 {
                        *channel_week_counts.entry(channel.clone()).or_insert(0) += 1;
                    }
                    if hours_ago <= 30 * 24 {
                        *channel_month_counts.entry(channel.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        // Collect all channels that have at least one failure
        let mut all_channels: HashSet<String> = HashSet::new();
        all_channels.extend(channel_day_counts.keys().cloned());
        all_channels.extend(channel_week_counts.keys().cloned());
        all_channels.extend(channel_month_counts.keys().cloned());

        let mut result: Vec<ChannelFailureData> = all_channels
            .into_iter()
            .map(|channel| ChannelFailureData {
                channel_id: channel.clone(),
                counts: PeriodCounts {
                    day: *channel_day_counts.get(&channel).unwrap_or(&0),
                    week: *channel_week_counts.get(&channel).unwrap_or(&0),
                    month: *channel_month_counts.get(&channel).unwrap_or(&0),
                },
            })
            .collect();

        result.sort_by(|a, b| b.counts.month.cmp(&a.counts.month)); // Sort by month count descending
        result
    }

    /// Get all failed forwards grouped by out_channel
    /// Returns a vector of ChannelFailureData sorted by month count descending
    pub fn failed_forwards_by_out_channel(&self) -> Vec<ChannelFailureData> {
        let mut channel_day_counts: HashMap<String, usize> = HashMap::new();
        let mut channel_week_counts: HashMap<String, usize> = HashMap::new();
        let mut channel_month_counts: HashMap<String, usize> = HashMap::new();

        for forward in &self.forwards.forwards {
            if let Some(received) = DateTime::from_timestamp(forward.received_time as i64, 0) {
                if self.now.signed_duration_since(received).num_days() > 365 {
                    continue;
                }

                if forward.status == "failed" && forward.out_channel.is_some() {
                    let channel = forward.out_channel.as_ref().unwrap();
                    let hours_ago = self.now.signed_duration_since(received).num_hours();

                    // Count failures by period
                    if hours_ago <= 24 {
                        *channel_day_counts.entry(channel.clone()).or_insert(0) += 1;
                    }
                    if hours_ago <= 7 * 24 {
                        *channel_week_counts.entry(channel.clone()).or_insert(0) += 1;
                    }
                    if hours_ago <= 30 * 24 {
                        *channel_month_counts.entry(channel.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        // Collect all channels that have at least one failure
        let mut all_channels: HashSet<String> = HashSet::new();
        all_channels.extend(channel_day_counts.keys().cloned());
        all_channels.extend(channel_week_counts.keys().cloned());
        all_channels.extend(channel_month_counts.keys().cloned());

        let mut result: Vec<ChannelFailureData> = all_channels
            .into_iter()
            .map(|channel| ChannelFailureData {
                channel_id: channel.clone(),
                counts: PeriodCounts {
                    day: *channel_day_counts.get(&channel).unwrap_or(&0),
                    week: *channel_week_counts.get(&channel).unwrap_or(&0),
                    month: *channel_month_counts.get(&channel).unwrap_or(&0),
                },
            })
            .collect();

        result.sort_by(|a, b| b.counts.month.cmp(&a.counts.month)); // Sort by month count descending
        result
    }

    /// Get count of forwards by status within the last N days
    pub fn count_forwards_by_status_days(&self, status: &str, days: i64) -> usize {
        self.forwards
            .forwards
            .iter()
            .filter(|f| {
                f.status == status && f.received_time > 0.0 && {
                    if let Some(dt) = DateTime::from_timestamp(f.received_time as i64, 0) {
                        self.now.signed_duration_since(dt).num_hours() <= days * 24
                    } else {
                        false
                    }
                }
            })
            .count()
    }

    /// Get forward statistics for different time periods
    pub fn get_forward_statistics(&self) -> ForwardStatistics {
        // Last day
        let day_settled = self.count_forwards_by_status_days("settled", 1);
        let day_failed = self.count_forwards_by_status_days("failed", 1);
        let day_local_failed = self.count_forwards_by_status_days("local_failed", 1);
        let day_all = day_settled + day_failed + day_local_failed;

        // Last week
        let week_settled = self.count_forwards_by_status_days("settled", 7);
        let week_failed = self.count_forwards_by_status_days("failed", 7);
        let week_local_failed = self.count_forwards_by_status_days("local_failed", 7);
        let week_all = week_settled + week_failed + week_local_failed;

        // Last month (30 days)
        let month_settled = self.count_forwards_by_status_days("settled", 30);
        let month_failed = self.count_forwards_by_status_days("failed", 30);
        let month_local_failed = self.count_forwards_by_status_days("local_failed", 30);
        let month_all = month_settled + month_failed + month_local_failed;

        ForwardStatistics {
            day_settled,
            day_failed,
            day_local_failed,
            day_all,
            week_settled,
            week_failed,
            week_local_failed,
            week_all,
            month_settled,
            month_failed,
            month_local_failed,
            month_all,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    const INCOMING_SCID: &str = "147440x1x0";
    const OUTGOING_SCID: &str = "147440x2x0";
    const UNRELATED_SCID: &str = "147440x3x0";
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
        cmd::Forward {
            in_channel: in_channel.to_string(),
            out_channel: Some(out_channel.to_string()),
            fee_msat: Some(fee_msat),
            in_msat: 1_000_000 + fee_msat,
            out_msat: Some(1_000_000),
            status: status.to_string(),
            received_time: (resolved_time - 1) as f64,
            resolved_time: Some(resolved_time as f64),
            failreason: None,
            failcode: None,
        }
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
            peers: cmd::ListPeers { peers: vec![] },
            funds: cmd::ListFunds {
                channels: funds,
                outputs: vec![],
            },
            forwards,
            nodes: cmd::ListNodes { nodes: vec![] },
            closed_channels: cmd::ListClosedChannels {
                closedchannels: vec![],
            },
            rebalance_parts,
            income_events: vec![],
            nodes_by_id: HashMap::new(),
            channels_by_id: HashMap::new(),
            node_channel_counts: HashMap::new(),
            forward_cache,
            peer_notes: HashMap::new(),
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
        assert!((store.calculate_gross_roic_percent(1) - 0.144).abs() < f64::EPSILON);
        assert!((store.calculate_net_roic_percent(1) - 0.1272).abs() < f64::EPSILON);
        assert!(
            (store
                .get_channel_gross_capacity_return(OUTGOING_SCID)
                .unwrap()
                - 0.006)
                .abs()
                < f64::EPSILON
        );
        assert!(
            (store
                .get_channel_net_capacity_return(OUTGOING_SCID)
                .unwrap()
                - 0.0053)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn indirect_fees_credit_only_settled_incoming_channel() {
        let store = test_store(
            vec![
                fund(INCOMING_SCID, 1_000_000_000),
                fund(OUTGOING_SCID, 1_000_000_000),
                fund(UNRELATED_SCID, 1_000_000_000),
            ],
            vec![
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    10_999,
                    "settled",
                    NOW_TIMESTAMP - 60,
                ),
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    50_000,
                    "failed",
                    NOW_TIMESTAMP - 50,
                ),
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    60_000,
                    "pending",
                    NOW_TIMESTAMP - 40,
                ),
                forward(
                    UNRELATED_SCID,
                    OUTGOING_SCID,
                    7_000,
                    "settled",
                    NOW_TIMESTAMP - 30,
                ),
            ],
            vec![],
        );

        assert_eq!(store.get_channel_indirect_fees(INCOMING_SCID), 10);
        assert_eq!(store.get_channel_indirect_fees(OUTGOING_SCID), 0);
        assert_eq!(store.get_channel_indirect_fees(UNRELATED_SCID), 7);
        assert_eq!(store.get_channel_indirect_fees("unknown-scid"), 0);
        assert_eq!(store.get_channel_total_fees(OUTGOING_SCID), 17);
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
    fn indirect_fee_period_filter_uses_settled_forward_resolution_time() {
        let store = test_store(
            vec![fund(INCOMING_SCID, 1_000_000_000)],
            vec![
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    11_000,
                    "settled",
                    NOW_TIMESTAMP - 29 * 24 * 60 * 60,
                ),
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    13_000,
                    "settled",
                    NOW_TIMESTAMP - 31 * 24 * 60 * 60,
                ),
            ],
            vec![],
        );

        let period_forwards = store.filter_settled_forwards_by_days(30);
        assert_eq!(
            period_forwards
                .iter()
                .filter(|forward| forward.in_channel == INCOMING_SCID)
                .map(|forward| forward.fee_sat)
                .sum::<u64>(),
            11
        );
    }

    #[test]
    fn cached_channel_failure_lists_are_filtered_and_sorted() {
        let store = test_store(
            vec![fund(INCOMING_SCID, 1_000_000_000)],
            vec![
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    0,
                    "failed",
                    NOW_TIMESTAMP - 30,
                ),
                forward(
                    UNRELATED_SCID,
                    INCOMING_SCID,
                    0,
                    "failed",
                    NOW_TIMESTAMP - 10,
                ),
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    0,
                    "local_failed",
                    NOW_TIMESTAMP - 20,
                ),
                forward(
                    INCOMING_SCID,
                    OUTGOING_SCID,
                    0,
                    "pending",
                    NOW_TIMESTAMP - 5,
                ),
            ],
            vec![],
        );

        let failed = store.get_channel_failed_forwards(INCOMING_SCID);
        assert_eq!(failed.len(), 2);
        assert!(failed[0].received_time > failed[1].received_time);

        let local_failed = store.get_channel_local_failed_forwards(INCOMING_SCID);
        assert_eq!(local_failed.len(), 1);
        assert_eq!(local_failed[0].status, "local_failed");
    }

    #[test]
    fn indirect_capacity_contribution_is_safe_for_zero_capacity_and_missing_channels() {
        let store = test_store(
            vec![fund(INCOMING_SCID, 0)],
            vec![forward(
                INCOMING_SCID,
                OUTGOING_SCID,
                10_000,
                "settled",
                NOW_TIMESTAMP - 60,
            )],
            vec![],
        );

        assert_eq!(
            store.get_channel_indirect_capacity_contribution(INCOMING_SCID),
            Some(0.0)
        );
        assert_eq!(
            store.get_channel_indirect_capacity_contribution("unknown-scid"),
            None
        );
    }

    #[test]
    fn indirect_capacity_contribution_does_not_change_net_return_or_rebalance_cost() {
        let rebalance_parts = vec![RebalancePart {
            payment_id: "rebalance-payment".to_string(),
            part_id: 0,
            source_account: "source-account".to_string(),
            target_account: "target-account".to_string(),
            source_channel_id: Some(UNRELATED_SCID.to_string()),
            target_channel_id: Some(OUTGOING_SCID.to_string()),
            debit_msat: 1_002_000,
            credit_msat: 1_000_000,
            fees_msat: 2_000,
            timestamp: Some(NOW_TIMESTAMP as u64 - 60),
        }];
        let store = test_store(
            vec![
                fund(INCOMING_SCID, 1_000_000_000),
                fund(OUTGOING_SCID, 1_000_000_000),
            ],
            vec![forward(
                INCOMING_SCID,
                OUTGOING_SCID,
                10_999,
                "settled",
                NOW_TIMESTAMP - 60,
            )],
            rebalance_parts,
        );

        assert_eq!(
            store.get_channel_rebalance_target_cost_msat(OUTGOING_SCID),
            2_000
        );
        assert_eq!(
            store.get_channel_net_routing_revenue_msat(OUTGOING_SCID),
            8_000
        );
        let net_capacity_return = store
            .get_channel_net_capacity_return(OUTGOING_SCID)
            .unwrap();
        let indirect_capacity_contribution = store
            .get_channel_indirect_capacity_contribution(INCOMING_SCID)
            .unwrap();
        assert!((net_capacity_return - 0.0008).abs() < f64::EPSILON);
        assert!((indirect_capacity_contribution - 0.001).abs() < f64::EPSILON);
        assert_eq!(
            store.get_channel_indirect_capacity_contribution(OUTGOING_SCID),
            Some(0.0)
        );
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

/// ROIC calculation data.
pub struct RoicData {
    pub fees_1_month: u64,
    pub fees_3_months: u64,
    pub fees_6_months: u64,
    pub fees_12_months: u64,
    pub total_funds: u64,
    pub gross_roic_1_month: f64,
    pub gross_roic_3_months: f64,
    pub gross_roic_6_months: f64,
    pub gross_roic_12_months: f64,
    pub lease_fee_earnings_1_month_msat: u64,
    pub lease_fee_earnings_3_months_msat: u64,
    pub lease_fee_earnings_6_months_msat: u64,
    pub lease_fee_earnings_12_months_msat: u64,
    pub lease_fee_cost_12_months_msat: u64,
    pub transacted_last_month: u64,
    pub routed_12_months: u64,
    pub capital_velocity_12_months: f64,
    pub effective_fee_rate_12_months_bps: f64,
    pub rebalance_cost_12_months_msat: u64,
    pub net_roic_12_months: f64,
}

/// Forward statistics for different time periods
pub struct ForwardStatistics {
    pub day_settled: usize,
    pub day_failed: usize,
    pub day_local_failed: usize,
    pub day_all: usize,
    pub week_settled: usize,
    pub week_failed: usize,
    pub week_local_failed: usize,
    pub week_all: usize,
    pub month_settled: usize,
    pub month_failed: usize,
    pub month_local_failed: usize,
    pub month_all: usize,
}

impl ForwardStatistics {
    /// Calculate success ratio as percentage
    pub fn success_ratio(&self, settled: usize, total: usize) -> f64 {
        if total == 0 {
            0.0
        } else {
            (settled as f64 / total as f64) * 100.0
        }
    }

    pub fn day_success_ratio(&self) -> f64 {
        self.success_ratio(self.day_settled, self.day_all)
    }

    pub fn week_success_ratio(&self) -> f64 {
        self.success_ratio(self.week_settled, self.week_all)
    }

    pub fn month_success_ratio(&self) -> f64 {
        self.success_ratio(self.month_settled, self.month_all)
    }

    /// Calculate per-day averages
    pub fn day_per_day(&self, count: usize) -> f64 {
        count as f64 / 1.0
    }

    pub fn week_per_day(&self, count: usize) -> f64 {
        count as f64 / 7.0
    }

    pub fn month_per_day(&self, count: usize) -> f64 {
        count as f64 / 30.0
    }
}

/// Data structure for closed channel with enriched information
#[derive(Debug, Clone)]
pub struct ClosedChannelInfo {
    pub channel: cmd::ClosedChannel,
    pub alias: String,
    pub opening_block: Option<u64>,
}

/// Fee distribution data structure for histogram visualization
pub struct FeeDistribution {
    pub labels: Vec<String>,
    pub outgoing_amounts: Vec<u64>,
    pub incoming_amounts: Vec<u64>,
}

/// Fee statistics for a peer
pub struct FeeStats {
    pub outgoing_mean: u64,
    pub outgoing_median: u64,
    pub incoming_mean: u64,
    pub incoming_median: u64,
}

/// Period counts for channel failures
#[derive(Debug, Clone)]
pub struct PeriodCounts {
    pub day: usize,
    pub week: usize,
    pub month: usize,
}

/// Channel failure data with period-based counts
#[derive(Debug, Clone)]
pub struct ChannelFailureData {
    pub channel_id: String,
    pub counts: PeriodCounts,
}
