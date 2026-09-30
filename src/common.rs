use std::collections::HashSet;

const SATS_GROUP_SEPARATOR: char = '\u{2009}';

pub fn format_sats(amount: u64) -> String {
    let digits = amount.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);

    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            formatted.push(SATS_GROUP_SEPARATOR);
        }
        formatted.push(ch);
    }

    formatted
}

pub fn channel_balance_target_stddev_percentage_points(channels: &[crate::cmd::Fund]) -> f64 {
    if channels.is_empty() {
        return 0.0;
    }

    let mean_squared_distance = channels
        .iter()
        .map(|channel| {
            let distance_from_target = channel.perc_float() - 0.5;
            distance_from_target * distance_from_target
        })
        .sum::<f64>()
        / channels.len() as f64;

    mean_squared_distance.sqrt() * 100.0
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

#[cfg(test)]
mod tests {
    use crate::cmd::Fund;

    use super::{channel_balance_target_stddev_percentage_points, format_sats};

    #[test]
    fn formats_sats_with_thin_space_groups() {
        assert_eq!(format_sats(0), "0");
        assert_eq!(format_sats(999), "999");
        assert_eq!(format_sats(1_000), "1\u{2009}000");
        assert_eq!(
            format_sats(1_234_567_890),
            "1\u{2009}234\u{2009}567\u{2009}890"
        );
    }

    #[test]
    fn channel_balance_target_stddev_measures_distance_from_fifty_percent() {
        let channels = [fund(250, 1_000), fund(750, 1_000)];

        assert_eq!(
            channel_balance_target_stddev_percentage_points(&channels),
            25.0
        );
        assert_eq!(channel_balance_target_stddev_percentage_points(&[]), 0.0);
    }

    fn fund(our_amount_msat: u64, amount_msat: u64) -> Fund {
        Fund {
            peer_id: "peer".to_string(),
            connected: true,
            state: "CHANNELD_NORMAL".to_string(),
            channel_id: "channel".to_string(),
            short_channel_id: Some("1x1x1".to_string()),
            our_amount_msat,
            amount_msat,
            funding_txid: "txid".to_string(),
            funding_output: 0,
        }
    }
}
