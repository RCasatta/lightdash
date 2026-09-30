//! LightningNetwork.Plus API client
//!
//! Fetches the public Liquidity Pool node list for snapshots.

use serde::{Deserialize, Serialize};

const LNPLUS_API_BASE: &str = "https://lightningnetwork.plus/api/2";
const POOL_NODES_PAGE_SIZE: usize = 50;
const POOL_NODES_MAX_PAGES: u32 = 100;

/// A node currently visible in the public LN+ Liquidity Pool
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PoolNode {
    pub pubkey: String,
    #[serde(default)]
    pub alias: Option<String>,
    pub web_url: String,
    pub credits_balance_sats: u64,
    pub min_channel_size_sats: u64,
    pub capacity_sats: u64,
    pub open_channels: u64,
    pub connection: String,
    #[serde(default)]
    pub clearnet_address: Option<String>,
    #[serde(default)]
    pub tor_address: Option<String>,
    pub lnp_rank: u64,
    pub lnp_rank_name: String,
    pub lnp_positive_ratings_received: u64,
    pub lnp_negative_ratings_received: u64,
    #[serde(default)]
    pub highlighted: bool,
}

/// Where and how the pool node list was fetched
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PoolNodesSource {
    pub endpoint: String,
    pub fetched_at: String,
    pub pages: u32,
    pub filters: String,
}

/// Fetch every page of the public, unauthenticated `get_pool_nodes` endpoint
///
/// No server-side filters are applied, so the result is the complete pool and consumers can
/// filter on credits, minimum channel size, or connection type themselves.
pub fn fetch_pool_nodes() -> Result<(Vec<PoolNode>, PoolNodesSource), String> {
    let endpoint = format!("{LNPLUS_API_BASE}/get_pool_nodes");
    let fetched_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut nodes: Vec<PoolNode> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut pages = 0;
    for page in 1..=POOL_NODES_MAX_PAGES {
        let batch: Vec<PoolNode> = ureq::get(&endpoint)
            .query("page", &page.to_string())
            .call()
            .map_err(|e| format!("fetching LN+ pool nodes page {page} failed: {e}"))?
            .into_json()
            .map_err(|e| format!("parsing LN+ pool nodes page {page} failed: {e}"))?;
        pages = page;
        let batch_len = batch.len();
        nodes.extend(
            batch
                .into_iter()
                .filter(|node| seen.insert(node.pubkey.clone())),
        );
        if batch_len < POOL_NODES_PAGE_SIZE {
            break;
        }
    }
    log::info!("Fetched {} LN+ pool nodes in {pages} pages", nodes.len());
    Ok((
        nodes,
        PoolNodesSource {
            endpoint,
            fetched_at,
            pages,
            filters: "none".to_string(),
        },
    ))
}
