use crate::cmd::{self, datastore_string, DatastoreMode, SettledForward};
use crate::common::ChannelFee;
use chrono::{DateTime, Datelike, Utc};
use std::collections::{HashMap, HashSet};

/// Store containing all data fetched from the Lightning node
pub struct Store {
    pub info: cmd::GetInfo,
    pub channels: cmd::ListChannels,
    pub peers: cmd::ListPeers,
    pub funds: cmd::ListFunds,
    pub forwards: cmd::ListForwards,
    pub nodes: cmd::ListNodes,
    pub closed_channels: cmd::ListClosedChannels,
    // Cached computed data
    nodes_by_id: HashMap<String, cmd::Node>,
    channels_by_id: HashMap<(String, String), cmd::Channel>,
    peer_notes: HashMap<String, String>,
    setchannel_timestamps: HashMap<String, i64>,
    now: DateTime<Utc>,
}

impl Store {
    /// Create a new Store by fetching all data from the Lightning node
    pub fn new() -> Self {
        let start_time = std::time::Instant::now();
        log::debug!("Fetching data from Lightning node...");
        let now = Utc::now();
        let info = cmd::get_info();
        let channels = cmd::list_channels();
        let peers = cmd::list_peers();
        let funds = cmd::list_funds();
        let forwards = cmd::list_forwards();
        let nodes = cmd::list_nodes();
        let closed_channels = cmd::list_closed_channels();
        log::debug!("Data fetched successfully");

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
            nodes_by_id,
            channels_by_id,
            peer_notes,
            setchannel_timestamps,
            now,
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
        let mut f: Vec<_> = self
            .forwards
            .forwards
            .iter()
            .filter(|e| e.status == "settled")
            .map(|e| SettledForward::try_from(e.clone()).unwrap())
            .collect();
        f.sort_by(|a, b| b.resolved_time.cmp(&a.resolved_time));
        f
    }

    /// Filter settled forwards to only include those resolved within the last N hours
    pub fn filter_settled_forwards_by_hours(&self, hours: i64) -> Vec<SettledForward> {
        self.settled_forwards()
            .into_iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_hours() <= hours)
            .collect()
    }

    /// Filter settled forwards to only include those resolved within the last N days
    pub fn filter_settled_forwards_by_days(&self, days: i64) -> Vec<SettledForward> {
        self.settled_forwards()
            .into_iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_days() <= days)
            .collect()
    }

    pub fn channels_len(&self) -> usize {
        self.channels.channels.len()
    }

    pub fn channels(&self) -> impl Iterator<Item = &cmd::Channel> {
        self.channels.channels.iter()
    }

    pub fn peers_len(&self) -> usize {
        self.peers.peers.len()
    }

    pub fn peers(&self) -> impl Iterator<Item = &cmd::Peer> {
        self.peers.peers.iter()
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

    pub fn nodes_with_channels_len(&self) -> usize {
        self.nodes()
            .filter(|n| {
                self.channels()
                    .any(|c| c.source == n.nodeid || c.destination == n.nodeid)
            })
            .count()
    }

    pub fn node_total_channels(&self, nodeid: &str) -> usize {
        self.channels()
            .filter(|c| c.source == *nodeid || c.destination == *nodeid)
            .count()
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
    pub fn chan_meta_per_node(&self) -> HashMap<String, ChannelFee> {
        let mut chan_meta: HashMap<String, ChannelFee> = HashMap::new();

        for c in &self.channels.channels {
            let meta = chan_meta
                .entry(c.source.clone())
                .or_insert_with(ChannelFee::default);
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

        for forward in self.settled_forwards() {
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
        self.settled_forwards()
            .into_iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_days() <= days)
            .map(|f| f.fee_sat)
            .sum()
    }

    /// Get total channel funds in sats
    pub fn total_channel_funds_sats(&self) -> u64 {
        self.normal_channels()
            .iter()
            .map(|c| c.our_amount_msat / 1000)
            .sum()
    }

    /// Get total onchain balance in BTC
    pub fn onchain_balance_btc(&self) -> f64 {
        let total_msat: u64 = self.funds.outputs.iter().map(|o| o.amount_msat).sum();
        // Convert from msat to sats, then to BTC
        (total_msat as f64) / 1_000.0 / 100_000_000.0
    }

    /// Calculate projected yearly APY percentage for given time period
    pub fn calculate_apy_percent(&self, months: i64) -> f64 {
        let fees_earned = self.fees_earned_last_months(months);
        let total_funds = self.total_channel_funds_sats();

        if total_funds == 0 {
            return 0.0;
        }

        let annualization_factor = 12.0 / months as f64;
        (fees_earned as f64 * 100.0 * annualization_factor) / total_funds as f64
    }

    /// Get total amount transacted in sats for the last month
    pub fn transacted_last_month_sats(&self) -> u64 {
        self.settled_forwards()
            .into_iter()
            .filter(|f| self.now.signed_duration_since(f.resolved_time).num_days() <= 30)
            .map(|f| f.out_sat)
            .sum()
    }

    /// Get APY data structure with all calculations
    pub fn get_apy_data(&self) -> ApyData {
        ApyData {
            fees_1_month: self.fees_earned_last_months(1),
            fees_3_months: self.fees_earned_last_months(3),
            fees_6_months: self.fees_earned_last_months(6),
            fees_12_months: self.fees_earned_last_months(12),
            total_funds: self.total_channel_funds_sats(),
            apy_1_month: self.calculate_apy_percent(1),
            apy_3_months: self.calculate_apy_percent(3),
            apy_6_months: self.calculate_apy_percent(6),
            apy_12_months: self.calculate_apy_percent(12),
            transacted_last_month: self.transacted_last_month_sats(),
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
        self.settled_forwards()
            .iter()
            .filter(|f| f.in_channel == short_channel_id || f.out_channel == short_channel_id)
            .count()
    }

    /// Get total fees earned for a specific channel (from outbound forwards)
    pub fn get_channel_total_fees(&self, short_channel_id: &str) -> u64 {
        self.settled_forwards()
            .iter()
            .filter(|f| f.out_channel == short_channel_id)
            .map(|f| f.fee_sat)
            .sum()
    }

    /// Get channel age in days from block height (approximate)
    pub fn get_channel_age_days(&self, short_channel_id: &str) -> Option<i64> {
        // Find the channel in the funds
        let normal_channels = self.normal_channels();
        let channel = normal_channels
            .iter()
            .find(|c| c.short_channel_id.as_ref().map(|s| s.as_str()) == Some(short_channel_id))?;

        // Get block height from short_channel_id
        let block_height = channel.block_born()?;

        // Approximate blocks per day (144 blocks per day on average)
        let blocks_per_day = 144;

        // Calculate approximate age in days
        // Note: This is approximate since we don't have the exact genesis block time
        // and block times can vary. For a more accurate calculation, we'd need
        // access to block timestamps.
        let age_blocks = self.info.blockheight.saturating_sub(block_height);
        Some((age_blocks / blocks_per_day) as i64)
    }

    /// Get average satoshis earned per day for a specific channel
    pub fn get_channel_sats_per_day(&self, short_channel_id: &str) -> Option<f64> {
        let age_days = self.get_channel_age_days(short_channel_id)?;
        if age_days <= 0 {
            return Some(0.0);
        }

        let total_fees = self.get_channel_total_fees(short_channel_id);
        Some(total_fees as f64 / age_days as f64)
    }

    /// Get all settled forwards for a specific channel (both inbound and outbound)
    pub fn get_channel_forwards(&self, short_channel_id: &str) -> Vec<SettledForward> {
        self.settled_forwards()
            .into_iter()
            .filter(|f| f.in_channel == short_channel_id || f.out_channel == short_channel_id)
            .collect()
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
}

/// APY calculation data
pub struct ApyData {
    pub fees_1_month: u64,
    pub fees_3_months: u64,
    pub fees_6_months: u64,
    pub fees_12_months: u64,
    pub total_funds: u64,
    pub apy_1_month: f64,
    pub apy_3_months: f64,
    pub apy_6_months: f64,
    pub apy_12_months: f64,
    pub transacted_last_month: u64,
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
