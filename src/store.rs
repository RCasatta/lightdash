use crate::cmd::{self, SettledForward};
use std::collections::HashMap;

/// Store containing all data fetched from the Lightning node
pub struct Store {
    pub info: cmd::GetInfo,
    pub channels: cmd::ListChannels,
    pub peers: cmd::ListPeers,
    pub funds: cmd::ListFunds,
    pub forwards: cmd::ListForwards,
    pub nodes: cmd::ListNodes,
}

impl Store {
    /// Create a new Store by fetching all data from the Lightning node
    pub fn new() -> Self {
        println!("Fetching data from Lightning node...");
        let info = cmd::get_info();
        let channels = cmd::list_channels();
        let peers = cmd::list_peers();
        let funds = cmd::list_funds();
        let forwards = cmd::list_forwards();
        let nodes = cmd::list_nodes();
        println!("Data fetched successfully");
        Self {
            info,
            channels,
            peers,
            funds,
            forwards,
            nodes,
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

    /// Get a HashMap of node ID to Node for nodes that have aliases
    pub fn nodes_by_id(&self) -> HashMap<&String, &cmd::Node> {
        self.nodes
            .nodes
            .iter()
            .filter(|e| e.alias.is_some())
            .map(|e| (&e.nodeid, e))
            .collect()
    }

    /// Get a HashMap of (short_channel_id, source) to Channel
    pub fn channels_by_id(&self) -> HashMap<(&String, &String), &cmd::Channel> {
        self.channels
            .channels
            .iter()
            .map(|e| ((&e.short_channel_id, &e.source), e))
            .collect()
    }

    /// Get the alias for a node ID, or format the ID if no alias exists
    pub fn get_node_alias(&self, node_id: &str) -> String {
        let nodes_by_id = self.nodes_by_id();
        let node_id_string = node_id.to_string();

        nodes_by_id
            .get(&node_id_string)
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
