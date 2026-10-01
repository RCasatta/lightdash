// Lower the advertised max HTLC as soon as a channel can no longer forward that much.
// This avoids local failures when forwarding HTLCs.
//
// The basis is what a single HTLC can actually carry (see `fees::htlc_max_basis_msat`), and the
// advertised value is its power-of-two floor so gossip reveals only the order of magnitude.
// This minutely job only lowers the maximum; the daily fee run raises it again, which keeps
// increases to at most one channel_update per channel per day.
//
// To change only the HTLC limits use named command like:
// lightning-cli -k setchannel 866501x2973x1 htlcmax=1000000

use std::collections::HashSet;

use crate::cmd::{cmd_result, list_peer_channels, list_peers, using_test_data};
use crate::fees::{htlc_max_basis_msat, htlc_max_msat};

pub fn run_htlc() -> Result<(), String> {
    log::info!("Running HTLC max adjustment");

    let channels = list_peer_channels()?;
    let large_channel_peers: HashSet<String> = list_peers()?
        .peers
        .into_iter()
        .filter(|peer| peer.supports_large_channels())
        .map(|peer| peer.id)
        .collect();
    log::info!("Found {} channels", channels.channels.len());

    let mut adjusted = 0;
    for channel in channels
        .channels
        .iter()
        .filter(|channel| channel.state == "CHANNELD_NORMAL")
    {
        let Some(scid) = channel.short_channel_id.as_deref() else {
            continue;
        };
        let basis_msat =
            htlc_max_basis_msat(channel, large_channel_peers.contains(&channel.peer_id));
        let new_htlc_max = htlc_max_msat(basis_msat);
        if channel.maximum_htlc_out_msat != 0 && new_htlc_max >= channel.maximum_htlc_out_msat {
            continue;
        }
        // The minimum cannot exceed the maximum, so lower it together when needed.
        let new_htlc_min = (new_htlc_max < channel.minimum_htlc_out_msat).then_some(new_htlc_max);

        log::info!(
            "Adjusting {scid}: to_us_msat={} spendable_msat={} basis_msat={basis_msat} max_htlc:{}->{new_htlc_max}{}",
            channel.to_us_msat,
            channel.spendable_msat,
            channel.maximum_htlc_out_msat,
            new_htlc_min
                .map(|min| format!(" min_htlc:{}->{min}", channel.minimum_htlc_out_msat))
                .unwrap_or_default()
        );
        set_channel_htlc_limits(scid, new_htlc_min, new_htlc_max);
        adjusted += 1;
    }

    log::info!("HTLC max adjustment completed, adjusted {adjusted} channels");
    Ok(())
}

fn set_channel_htlc_limits(short_channel_id: &str, htlc_min: Option<u64>, htlc_max: u64) {
    if using_test_data() {
        log::debug!("Test data: would set htlcmax={htlc_max} for {short_channel_id}");
        return;
    }

    let mut args = vec![
        "setchannel".to_string(),
        "-k".to_string(),
        format!("id={short_channel_id}"),
        format!("htlcmax={htlc_max}"),
    ];
    if let Some(htlc_min) = htlc_min {
        args.push(format!("htlcmin={htlc_min}"));
    }
    log::info!("Executing: `lightning-cli {}`", args.join(" "));
    let result = cmd_result("lightning-cli", &args);
    log::debug!("setchannel result: {result}");
}
