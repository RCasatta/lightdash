//! LightningNetwork.Plus API client
//!
//! Fetches liquidity swap data from LN+ and saves it locally as JSON.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;

use crate::cmd;

const LNPLUS_API_BASE: &str = "https://lightningnetwork.plus/api/2";

/// Swap status filter for API queries
#[derive(Debug, Clone, Copy)]
pub enum SwapStatus {
    Pending,
    Opening,
    Completed,
}

impl fmt::Display for SwapStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwapStatus::Pending => write!(f, "pending"),
            SwapStatus::Opening => write!(f, "opening"),
            SwapStatus::Completed => write!(f, "completed"),
        }
    }
}

/// LN+ API client with authentication
pub struct LnPlusClient {
    message: String,
    signature: String,
}

impl LnPlusClient {
    /// Create a new authenticated client by getting a message from LN+ and signing it
    pub fn new() -> Result<Self, String> {
        // Step 1: Get message to sign from LN+
        log::info!("Fetching authentication message from LN+...");
        let get_message_url = format!("{LNPLUS_API_BASE}/get_message");
        let response: GetMessageResponse = ureq::get(&get_message_url)
            .call()
            .map_err(|e| format!("Failed to get message from LN+: {e}"))?
            .into_json()
            .map_err(|e| format!("Failed to parse get_message response: {e}"))?;

        log::info!(
            "Got message: {}, expires at: {}",
            response.message,
            response.expires_at
        );

        // Step 2: Sign the message with our node
        log::info!("Signing message with node...");
        let sign_response = cmd::signmessage(&response.message);
        log::info!("Message signed successfully");

        Ok(Self {
            message: response.message,
            signature: sign_response.zbase,
        })
    }

    /// Fetch swaps from LN+ with optional status filter
    ///
    /// # Arguments
    /// * `status` - Optional status filter (pending, opening, completed)
    pub fn get_swaps(&self, status: Option<SwapStatus>) -> Result<Vec<Swap>, String> {
        let url = match status {
            Some(s) => {
                log::info!("Fetching {s} swaps from LN+...");
                format!("{LNPLUS_API_BASE}/get_swaps/status={s}")
            }
            None => {
                log::info!("Fetching all swaps from LN+...");
                format!("{LNPLUS_API_BASE}/get_swaps")
            }
        };

        let response: Vec<Swap> = ureq::get(&url)
            .query("message", &self.message)
            .query("signature", &self.signature)
            .call()
            .map_err(|e| format!("Failed to fetch swaps: {e}"))?
            .into_json()
            .map_err(|e| format!("Failed to parse swaps response: {e}"))?;

        log::info!("Fetched {} swaps", response.len());
        Ok(response)
    }
}

/// Run the lnplus command: authenticate and fetch swaps
pub fn run_lnplus(output_dir: &str) {
    // Create output directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(output_dir) {
        log::error!("Failed to create output directory {output_dir}: {e}");
        return;
    }

    // Authenticate with LN+
    let client = match LnPlusClient::new() {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to authenticate with LN+: {e}");
            return;
        }
    };

    // Fetch and save pending swaps
    match client.get_swaps(Some(SwapStatus::Pending)) {
        Ok(swaps) => {
            let swaps_file = format!("{output_dir}/lnplus_swaps.json");
            match serde_json::to_string_pretty(&swaps) {
                Ok(json) => match fs::write(&swaps_file, json) {
                    Ok(_) => log::info!("Swaps saved to {swaps_file} ({} swaps)", swaps.len()),
                    Err(e) => log::error!("Failed to write swaps file: {e}"),
                },
                Err(e) => log::error!("Failed to serialize swaps: {e}"),
            }
        }
        Err(e) => log::error!("Failed to fetch swaps: {e}"),
    }
}

// ============================================================================
// API Response Types
// ============================================================================

#[derive(Deserialize, Debug)]
struct GetMessageResponse {
    message: String,
    expires_at: String,
}

/// A liquidity swap from LN+
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Swap {
    pub id: u64,
    pub web_url: String,
    #[serde(default)]
    pub image_url: Option<String>,
    #[serde(default)]
    pub created_by_pubkey: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub starts: Option<String>,
    #[serde(default)]
    pub ends: Option<String>,
    pub status: String,
    #[serde(default)]
    pub humanized_status: Option<String>,
    pub capacity_sats: u64,
    pub duration_months: u64,
    pub participant_max_count: u64,
    #[serde(default)]
    pub rating_received: Option<String>,
    #[serde(default)]
    pub rating_given: Option<String>,
    pub participant_applied_count: u64,
    pub participant_waiting_for_count: u64,
    #[serde(default)]
    pub participant_min_capacity_sats: Option<u64>,
    #[serde(default)]
    pub participant_min_channels_count: Option<u64>,
    #[serde(default)]
    pub clearnet_connection_allowed: Option<bool>,
    #[serde(default)]
    pub tor_connection_allowed: Option<bool>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub prime: Option<bool>,
    #[serde(default)]
    pub pro: Option<bool>,
    #[serde(default)]
    pub private: Option<bool>,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub comments_count: Option<u64>,
    #[serde(default)]
    pub participants: Vec<Participant>,
}

/// A participant in a swap
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Participant {
    pub pubkey: String,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub lnplus_rank_number: Option<u64>,
    #[serde(default)]
    pub lnplus_rank_name: Option<String>,
    #[serde(default)]
    pub contribution_status: Option<String>,
    #[serde(default)]
    pub channel_open_status: Option<String>,
    #[serde(default)]
    pub channel_point_to: Option<String>,
    #[serde(default)]
    pub channel_point_from: Option<String>,
}
