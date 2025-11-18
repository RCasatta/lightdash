use crate::cmd::Forward;
use crate::store::Store;
use chrono::Utc;

pub const PPM_MIN: u64 = 10;
pub const PPM_MAX: u64 = 5000;
pub const SLING_AMOUNT: u64 = 50000; // amount used for rebalancing
pub const MIN_HTLC: u64 = 100000; // msat
pub const STEP_PERC: f64 = 0.1; // percentage change when channel is doing routing (+) in the last 24 hours or not doing it (-)
pub const FEE_BASE: u64 = 1000; // msat

pub fn run_fees(store: &Store) {
    let normal_channels = store.normal_channels();
    let forwards_24h = store.filter_forwards_by_hours(24);

    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();
        let our = match store.get_channel(&short_channel_id, &store.info.id) {
            Some(c) => c,
            None => continue,
        };
        let alias_or_id = store.get_node_alias(&fund.peer_id);

        calc_setchannel(&short_channel_id, &alias_or_id, &fund, our, &forwards_24h);
    }
}

/// Returns the largest power of 2 that is less than or equal to n.
/// For n = 0, returns 0.
/// For n > 0, returns the highest power of 2 <= n.
pub fn largest_power_of_two_leq(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        1u64 << (63 - n.leading_zeros())
    }
}

pub fn calc_setchannel<'a>(
    short_channel_id: &str,
    alias: &str,
    fund: &crate::cmd::Fund,
    our: &crate::cmd::Channel,
    forwards_24h: &[Forward],
) {
    let perc = fund.perc_float(); // how full of our funds is the channel
    let disp_perc = format!("{:.1}%", perc * 100.0);
    let current_channel_forwards = did_forward(short_channel_id, &forwards_24h);
    // TODO count forwards by status ok
    let current_ppm = our.fee_per_millionth;
    let current_max_htlc_sat = our.htlc_maximum_msat;
    let current_min_htlc_sat = our.htlc_minimum_msat;
    let our_amount_msat = fund.our_amount_msat;

    // Compute the largest power of 2 <= our_amount_msat for max HTLC
    let new_max_htlc_msat = largest_power_of_two_leq(our_amount_msat);
    let new_min_htlc_msat = std::cmp::min(MIN_HTLC, new_max_htlc_msat); // min_htlc cannot be greater than max_htlc
    let new_ppm = if current_channel_forwards.len() == 0 {
        // no good or bad forwards, reduce fee
        // we reduce proportionally to how full is the channel, depleted channel (<10% never reduce)
        let mut saturating_sub_perc = perc - 0.1;
        if saturating_sub_perc < 0.0 {
            saturating_sub_perc = 0.0;
        }
        let reduce_perc = STEP_PERC * saturating_sub_perc;
        current_ppm - (current_ppm as f64 * reduce_perc) as u64
    } else {
        // there are forwards or errors, increase fee
        // TODO: tollerate some amount of errors and not increase fee in that case

        let increase_perc = STEP_PERC;
        current_ppm + (current_ppm as f64 * increase_perc) as u64
    };

    let new_ppm = new_ppm.clamp(PPM_MIN, PPM_MAX);

    let data = if new_ppm == current_ppm {
        "EQU"
    } else if new_ppm > current_ppm {
        "INC"
    } else {
        "DEC"
    };

    log::info!("{data} {short_channel_id} with {alias}. my_fund:{our_amount_msat} ({disp_perc})  ppm:{current_ppm}->{new_ppm} max_htlc:{current_max_htlc_sat}->{new_max_htlc_msat} min_htlc:{current_min_htlc_sat}->{new_min_htlc_msat}");
    if current_ppm != new_ppm
        || current_max_htlc_sat != new_max_htlc_msat
        || current_min_htlc_sat != new_min_htlc_msat
    {
        let cmd = "lightning-cli";
        let args = format!(
            "setchannel {short_channel_id} {FEE_BASE} {new_ppm} {new_min_htlc_msat} {new_max_htlc_msat}"
        );

        if std::env::var("EXECUTE_SETCHANNEL").is_ok() {
            log::info!("executing `{cmd} {args}` {alias}");

            // Always execute fee adjustments
            let splitted_args: Vec<&str> = args.split(' ').collect();
            let result = crate::cmd::cmd_result(cmd, &splitted_args);
            log::info!("cmd return: {result}");

            // Save timestamp to datastore
            let timestamp = Utc::now().timestamp().to_string();
            if let Err(e) = crate::cmd::datastore_string(
                &["lightdash", "last_setchannel", short_channel_id],
                &timestamp,
                crate::cmd::DatastoreMode::CreateOrReplace,
            ) {
                log::error!(
                    "Failed to save setchannel timestamp for {}: {}",
                    short_channel_id,
                    e
                );
            }
        } else {
            log::info!("would execute `{cmd} {args}` {alias}");
        }
    } else {
        log::info!("skipping {short_channel_id}")
    };
}

pub fn did_forward<'a>(
    short_channel_id: &str,
    forwards: &'a [crate::cmd::Forward],
) -> Vec<&'a crate::cmd::Forward> {
    forwards
        .iter()
        .filter(|f| f.out_channel.as_ref().unwrap_or(&"".to_string()) == short_channel_id)
        .collect()
}
