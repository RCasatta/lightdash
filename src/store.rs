use crate::cmd::{self, SettledForward};
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
    // Cached computed data
    nodes_by_id: HashMap<String, cmd::Node>,
    channels_by_id: HashMap<(String, String), cmd::Channel>,
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

        let store = Self {
            info,
            channels,
            peers,
            funds,
            forwards,
            nodes,
            nodes_by_id,
            channels_by_id,
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
}
