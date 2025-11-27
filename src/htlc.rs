// Reduce htlc max if our funds are lower than htlc max.
// This avoids local failures when forwarding HTLCs.
//
// Based on:
// lightning-cli listpeerchannels | jq '.channels | map(select(.to_us_msat < .maximum_htlc_out_msat)) | map(select(.to_us_msat != 0)) | map({short_channel_id, to_us_msat, maximum_htlc_out_msat})'
//
// To change only the htlc max use named command like:
// lightning-cli -k setchannel 866501x2973x1 htlcmax=1000000

use std::cmp::max;

use crate::cmd::{cmd_result, list_peer_channels};
use crate::fees::largest_power_of_two_leq;

pub fn run_htlc() {
    log::info!("Running HTLC max adjustment");

    let channels = list_peer_channels();
    log::info!("Found {} channels", channels.channels.len());

    let channels_to_adjust: Vec<_> = channels
        .channels
        .iter()
        .filter(|c| c.state == "CHANNELD_NORMAL")
        .filter(|c| c.short_channel_id.is_some())
        .filter(|c| c.to_us_msat != 0)
        .filter(|c| c.to_us_msat < c.maximum_htlc_out_msat)
        .collect();

    log::info!(
        "Found {} channels needing HTLC max adjustment",
        channels_to_adjust.len()
    );

    for channel in channels_to_adjust {
        let scid = channel.short_channel_id.as_ref().unwrap();
        let new_htlc_max = max(largest_power_of_two_leq(channel.to_us_msat), 1);

        log::info!(
            "Adjusting {scid}: to_us_msat={} max_htlc:{}->{}",
            channel.to_us_msat,
            channel.maximum_htlc_out_msat,
            new_htlc_max
        );

        set_channel_htlc_max(scid, new_htlc_max);
    }

    log::info!("HTLC max adjustment completed");
}

fn set_channel_htlc_max(short_channel_id: &str, htlc_max: u64) {
    if cfg!(debug_assertions) {
        log::debug!("Debug mode: would set htlcmax={htlc_max} for {short_channel_id}");
        return;
    }

    let htlc_max_arg = format!("htlcmax={htlc_max}");
    let result = cmd_result(
        "lightning-cli",
        &["setchannel", "-k", short_channel_id, &htlc_max_arg],
    );
    log::debug!("setchannel result: {result:?}");
}
