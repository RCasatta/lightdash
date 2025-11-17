use chrono::Utc;
use std::collections::HashSet;

pub const PPM_MIN: u64 = 10;
pub const PPM_MAX: u64 = 5000;
pub const SLING_AMOUNT: u64 = 50000; // amount used for rebalancing
pub const MIN_HTLC: u64 = 100; // minimum htlc amount in sats
pub const STEP_PERC: f64 = 0.1; // percentage change when channel is doing routing (+) in the last 24 hours or not doing it (-)
pub const FEE_BASE: u64 = 1000; // msat

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

pub fn calc_setchannel<'a>(
    short_channel_id: &str,
    alias: &str,
    fund: &crate::cmd::Fund,
    our: &crate::cmd::Channel,
    forwards_24h: &[Forward],
) {
    let perc = fund.perc_float(); // how full of our funds is the channel
    let current_channel_forwards = did_forward(short_channel_id, &forwards_24h);
    let current_ppm = our.fee_per_millionth;
    let current_max_htlc_sat = our.htlc_maximum_msat;

    // Compute the largest power of 2 <= our_amount_msat for max HTLC
    let new_max_htlc_sat = if fund.our_amount_msat == 0 {
        0
    } else {
        fund.our_amount_msat & !(fund.our_amount_msat - 1)
    };
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

    log::info!("{data} {short_channel_id} with {alias}. my_fund:{perc:.1}%  ppm:{current_ppm}->{new_ppm} max_htlc:{current_max_htlc_sat}->{new_max_htlc_sat}");
    if current_ppm != new_ppm || current_max_htlc_sat != new_max_htlc_sat {
        let cmd = "lightning-cli";
        let args = format!(
            "setchannel {short_channel_id} {FEE_BASE} {new_ppm} {MIN_HTLC}sat {new_max_htlc_sat}sat"
        );
        log::info!("executing `{cmd} {args}` {alias}");

        return; // TODO: remove me

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

pub fn cut_days(d: i64) -> String {
    if d > 99 {
        "99+".to_string()
    } else {
        format!("{d:>2}d")
    }
}

use chrono::Duration;

use crate::cmd::Forward;

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
