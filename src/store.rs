use crate::cmd::{self, SettledForward};

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
}
