use crate::fees::PPM_MIN;
use chrono::Duration;
use std::collections::HashSet;

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
    let amount = crate::fees::SLING_AMOUNT;
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

pub fn cut_days(d: i64) -> String {
    if d > 99 {
        "99+".to_string()
    } else {
        format!("{d:>2}d")
    }
}

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
