use chrono::Utc;
use std::collections::HashSet;

pub const PPM_MIN: u64 = 10; // minimum between 100% and 50%
pub const PPM_MAX: u64 = 2000; // when channel 0%, between 0% and 50% increase linearly
pub const SLING_AMOUNT: u64 = 50000; // amount used for rebalancing
pub const MIN_HTLC: u64 = 100; // minimum htlc amount in sats
pub const STEP_PERC: f64 = 0.05; // percentage change when channel is doing routing (+) in the last 24 hours or not doing it (-)

/// Compute the minimum ppm of the channel according to the percentual owned by us
/// The intention is to signal via an high fee the channel depletion
pub fn min_ppm(perc: f64) -> u64 {
    if perc > 0.5 {
        PPM_MIN
    } else {
        let delta = (PPM_MAX - PPM_MIN) as f64;
        ((PPM_MAX as f64) + (2.0 * perc * -delta)) as u64 // since perc>0 this is positive
    }
}

/// Helper struct to compute the average fee of the channels of a node
#[derive(Default)]
pub struct ChannelFee {
    pub count: u64,
    pub fee_sum: u64,
    pub fee_rates: HashSet<u64>,
}

impl ChannelFee {
    pub fn avg_fee(&self) -> f64 {
        self.fee_sum as f64 / self.count as f64
    }

    pub fn fee_diversity(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.fee_rates.len() as f64 / self.count as f64
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Rebalance {
    PushOut,
    PullIn,
    Nothing,
}

pub struct ChannelMeta {
    pub fund: crate::cmd::Fund,
    pub is_sink: f64,
    pub is_sink_last_month: f64,
    pub rebalance: Rebalance,
    pub alias_or_id: String,
    pub block_born: u64,
}

impl ChannelMeta {
    pub fn is_sink_perc(&self) -> String {
        format!("{:.0}%", self.is_sink * 100.0)
    }
    pub fn is_sink_last_month_perc(&self) -> String {
        format!("{:.0}%", self.is_sink_last_month * 100.0)
    }

    pub fn alias_or_id(&self) -> String {
        self.alias_or_id.clone()
    }
}

// lightning-cli sling-job -k scid=848864x399x0 direction=push amount=1000 maxppm=500 outppm=200 depleteuptoamount=100000
pub fn calc_slingjobs(
    scid: String,
    perc_us: f64,
    ever_forward_in_out: u64,
    alias: &str,
    channel: &ChannelMeta,
    pull_in: &[String],
    push_out: &[String],
) -> Option<(String, String)> {
    let amount = SLING_AMOUNT;
    let maxppm = PPM_MIN;
    let is_sink_perc = channel.is_sink_perc();

    let (dir, candidates, target) = match channel.rebalance {
        Rebalance::PullIn => ("pull", push_out, 0.3),
        Rebalance::PushOut => ("push", pull_in, 0.7),
        Rebalance::Nothing => return None,
    };

    let candidates = format!("{candidates:?}").replace(" ", "");
    // let candidates = format!("[\"{}\"]", candidates.join("\",\""));

    let cmd = format!("lightning-cli sling-job -k scid={scid} amount={amount} maxppm={maxppm} direction={dir} candidates={candidates} target={target:.2}");
    let details =
        format!("perc_us:{perc_us:.2} is_sink:{is_sink_perc} {ever_forward_in_out} {alias}");
    Some((cmd, details))
}

pub fn calc_setchannel(
    short_channel_id: &str,
    alias: &str,
    fund: &crate::cmd::Fund,
    our: Option<&&crate::cmd::Channel>,
    settled_24h: &[crate::cmd::SettledForward],
) -> (u64, Option<String>) {
    let perc = fund.perc_float();
    // let amount = fund.amount_msat;
    // let our_amount = fund.our_amount_msat;
    let max_htlc_sat = ((fund.amount_msat as f64 / 1000.0) * 0.7) as u64; // we aim for 70% in rebalance
    let max_htlc_sat = format!("{max_htlc_sat}sat");

    let min_ppm = min_ppm(perc);

    let current_ppm = our.map(|e| e.fee_per_millionth).unwrap_or(min_ppm);

    let forwards_last_24h = did_forward(short_channel_id, &settled_24h);
    let did_forwards_last_24h = !forwards_last_24h.is_empty();
    let step = (current_ppm as f64 * STEP_PERC) as u64;
    let new_ppm = if did_forwards_last_24h {
        current_ppm.saturating_add(step)
    } else {
        current_ppm.saturating_sub(step)
    };

    let new_ppm = new_ppm.clamp(min_ppm, PPM_MAX);

    let truncated_min = min_ppm == new_ppm;

    let result = if current_ppm != new_ppm {
        let cmd = "lightning-cli";
        let args =
            format!("setchannel {short_channel_id} 1000 {new_ppm} {MIN_HTLC}sat {max_htlc_sat}");

        // Always execute fee adjustments
        let splitted_args: Vec<&str> = args.split(' ').collect();
        let _result = crate::cmd::cmd_result(cmd, &splitted_args);
        // println!("{result}");

        // Save timestamp to datastore
        let timestamp = Utc::now().timestamp().to_string();
        if let Err(e) = crate::cmd::datastore_string(
            &["lightdash", "last_setchannel", short_channel_id],
            &timestamp,
            crate::cmd::DatastoreMode::CreateOrReplace,
        ) {
            eprintln!(
                "Failed to save setchannel timestamp for {}: {}",
                short_channel_id, e
            );
        }

        let truncated_min_str = if truncated_min { "truncated_min" } else { "" };

        Some(format!(
            "`{cmd} {args}` was:{current_ppm} perc:{perc:.2} min:{min_ppm} forward_last_24h:{} {truncated_min_str} {alias}",
            forwards_last_24h.len()
        ))
    } else {
        None
    };

    (new_ppm, result)
}

pub fn did_forward<'a>(
    short_channel_id: &str,
    forwards: &'a [crate::cmd::SettledForward],
) -> Vec<&'a crate::cmd::SettledForward> {
    forwards
        .iter()
        .filter(|f| f.out_channel == short_channel_id)
        .collect()
}

pub fn cut_days(d: i64) -> String {
    if d > 99 {
        "99+".to_string()
    } else {
        format!("{d:>2}d")
    }
}

use chrono::Duration;

/// Format a duration in a human-readable way
pub fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.num_seconds().abs();
    let days = total_seconds / 86400;
    let hours = (total_seconds % 86400) / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}
