use crate::cmd;

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
}
