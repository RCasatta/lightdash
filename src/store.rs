use crate::cmd::{self, SettledForward};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

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
        println!("Fetching data from Lightning node...");
        let now = Utc::now();
        let info = cmd::get_info();
        let channels = cmd::list_channels();
        let peers = cmd::list_peers();
        let funds = cmd::list_funds();
        let forwards = cmd::list_forwards();
        let nodes = cmd::list_nodes();
        println!("Data fetched successfully");

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

        Self {
            info,
            channels,
            peers,
            funds,
            forwards,
            nodes,
            nodes_by_id,
            channels_by_id,
            now,
        }
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

    /// Get settled forwards
    pub fn settled_forwards(&self) -> Vec<SettledForward> {
        self.forwards
            .forwards
            .iter()
            .filter(|e| e.status == "settled")
            .map(|e| SettledForward::try_from(e.clone()).unwrap())
            .collect()
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
}
