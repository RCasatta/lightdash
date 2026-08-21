use std::cmp::{max, min, Ordering};
use std::collections::HashSet;

use chrono::Utc;

use crate::cmd::Forward;
use crate::store::Store;

pub const PPM_MIN: u64 = 1;
pub const PPM_MAX: u64 = 5000;
pub const DEPLETED_LOCAL_BALANCE_SAT: u64 = 50000;
pub const MIN_HTLC: u64 = 100000; // msat
pub const FORWARD_INCREASE_PERCENT: u64 = 5;
pub const DEPLETED_INCREASE_PERCENT: u64 = 1;
pub const BOOTSTRAP_DECREASE_PERCENT: u64 = 15;
pub const NORMAL_DECREASE_PERCENT: u64 = 2;
pub const FEE_BASE: u64 = 1000; // msat
pub const MIN_ROUTED_24H_SAT: u64 = 5000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeeState {
    Bootstrap,
    Normal,
    Depleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ForwardActivity {
    None,
    BelowMinimum,
    MeetsMinimum,
}

impl FeeState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Normal => "normal",
            Self::Depleted => "depleted",
        }
    }
}

pub fn run_fees(store: &Store) {
    let normal_channels = store.normal_channels();
    let forwards_24h = store.filter_forwards_by_hours(24);
    let ever_settled_out_channels: HashSet<&str> = store.settled_out_channel_ids().collect();

    let mut equ_count = 0;
    let mut inc_count = 0;
    let mut dec_count = 0;
    let mut dis_count = 0;

    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();
        let our = match store.get_channel(&short_channel_id, &store.info.id) {
            Some(c) => c,
            None => continue,
        };
        let alias_or_id = store.get_node_alias(&fund.peer_id);
        let avail = store.avail_map.get(&fund.peer_id).cloned();

        let trend = calc_setchannel(
            &short_channel_id,
            &alias_or_id,
            fund,
            our,
            &forwards_24h,
            ever_settled_out_channels.contains(short_channel_id.as_str()),
            avail,
        );
        match trend {
            "EQU" => equ_count += 1,
            "INC" => inc_count += 1,
            "DEC" => dec_count += 1,
            "DIS" => dis_count += 1,
            _ => {}
        }
    }
    log::info!("setchannel trend: EQU:{equ_count} INC:{inc_count} DEC:{dec_count} DIS:{dis_count}");
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

fn fee_state(local_balance_sat: u64, ever_forwarded: bool) -> FeeState {
    if local_balance_sat < DEPLETED_LOCAL_BALANCE_SAT {
        FeeState::Depleted
    } else if ever_forwarded {
        FeeState::Normal
    } else {
        FeeState::Bootstrap
    }
}

fn increase_ppm_ceil(current_ppm: u64, percent: u64) -> u64 {
    let numerator = current_ppm as u128 * (100 + percent) as u128;
    ((numerator + 99) / 100).min(u64::MAX as u128) as u64
}

fn decrease_ppm_floor(current_ppm: u64, percent: u64) -> u64 {
    let numerator = current_ppm as u128 * (100 - percent) as u128;
    (numerator / 100).min(u64::MAX as u128) as u64
}

fn forward_activity(settled_forward_count: usize, routed_msat: u64) -> ForwardActivity {
    if settled_forward_count == 0 {
        ForwardActivity::None
    } else if routed_msat >= MIN_ROUTED_24H_SAT * 1000 {
        ForwardActivity::MeetsMinimum
    } else {
        ForwardActivity::BelowMinimum
    }
}

fn adjusted_ppm(current_ppm: u64, state: FeeState, activity: ForwardActivity) -> u64 {
    let adjusted = match activity {
        ForwardActivity::MeetsMinimum => increase_ppm_ceil(current_ppm, FORWARD_INCREASE_PERCENT),
        ForwardActivity::BelowMinimum => current_ppm,
        ForwardActivity::None => match state {
            FeeState::Bootstrap => decrease_ppm_floor(current_ppm, BOOTSTRAP_DECREASE_PERCENT),
            FeeState::Normal => decrease_ppm_floor(current_ppm, NORMAL_DECREASE_PERCENT),
            FeeState::Depleted => increase_ppm_ceil(current_ppm, DEPLETED_INCREASE_PERCENT),
        },
    };

    adjusted.clamp(PPM_MIN, PPM_MAX)
}

pub fn calc_setchannel(
    short_channel_id: &str,
    alias: &str,
    fund: &crate::cmd::Fund,
    our: &crate::cmd::Channel,
    forwards_24h: &[Forward],
    ever_forwarded: bool,
    avail: Option<f64>,
) -> &'static str {
    let channel_fund_perc_ours = fund.perc_float(); // how full of our funds is the channel
    let disp_perc = format!("{:.1}%", channel_fund_perc_ours * 100.0);
    let current_channel_forwards = did_forward(short_channel_id, forwards_24h);
    let forwards_all = current_channel_forwards.len();
    let forwards_ok = current_channel_forwards
        .iter()
        .filter(|e| e.status == "settled")
        .count();
    let forwards_ko = forwards_all - forwards_ok;
    let routed_24h_msat = current_channel_forwards
        .iter()
        .filter(|forward| forward.status == "settled")
        .filter_map(|forward| forward.out_msat)
        .fold(0u64, u64::saturating_add);
    let activity = forward_activity(forwards_ok, routed_24h_msat);

    let current_ppm = our.fee_per_millionth;
    let current_max_htlc_sat = our.htlc_maximum_msat;
    let current_min_htlc_sat = our.htlc_minimum_msat;
    let our_amount_msat = fund.our_amount_msat;
    let local_balance_sat = our_amount_msat / 1000;
    let state = fee_state(local_balance_sat, ever_forwarded);

    if let Some(avail) = avail {
        if avail < 0.8 {
            // the channel is not available enough, "disable" it by setting htlc to 1msat

            let cmd = "lightning-cli";
            let args = format!("setchannel {short_channel_id} {FEE_BASE} {current_ppm} 1 1");
            let splitted_args: Vec<&str> = args.split(' ').collect();
            log::info!(
                "DIS {short_channel_id} with {alias}. avail:{:.1}%",
                avail * 100.0
            );
            if std::env::var("EXECUTE_SETCHANNEL").is_ok() {
                let result = crate::cmd::cmd_result(cmd, &splitted_args);
                log::debug!("cmd return: {result}");
            }
            return "DIS";
        }
    }

    // Compute the largest power of 2 <= our_amount_msat for max HTLC
    let new_max_htlc_msat = max(largest_power_of_two_leq(our_amount_msat), 1); // max_htlc canno be 0 when min_htlc is 1

    let new_min_htlc_msat = min(
        max(MIN_HTLC, current_min_htlc_sat), // some peer may enforce an higher than MIN_HTLC minimum value, thus we use the higher value
        max(new_max_htlc_msat, 1), // min_htlc cannot be greater than max_htlc and lower than 1
    );

    let new_ppm = adjusted_ppm(current_ppm, state, activity);

    let changes = current_ppm != new_ppm
        || current_max_htlc_sat != new_max_htlc_msat
        || current_min_htlc_sat != new_min_htlc_msat;

    let data = match new_ppm.cmp(&current_ppm) {
        Ordering::Equal => "EQU",
        Ordering::Greater => "INC",
        Ordering::Less => "DEC",
    };

    if changes {
        let mut change_parts = Vec::new();
        if current_ppm != new_ppm {
            change_parts.push(format!("ppm:{current_ppm}->{new_ppm}"));
        }
        if current_max_htlc_sat != new_max_htlc_msat {
            change_parts.push(format!(
                "max_htlc:{current_max_htlc_sat}->{new_max_htlc_msat}"
            ));
        }
        if current_min_htlc_sat != new_min_htlc_msat {
            change_parts.push(format!(
                "min_htlc:{current_min_htlc_sat}->{new_min_htlc_msat}"
            ));
        }
        let change_str = change_parts.join(" ");
        log::info!(
            "{data} state:{} ok:{forwards_ok} ko:{forwards_ko} routed_24h_msat:{routed_24h_msat} {short_channel_id} with {alias}. my_fund:{our_amount_msat} ({disp_perc})  {change_str}",
            state.as_str()
        );

        let cmd = "lightning-cli";
        let args = format!(
            "setchannel {short_channel_id} {FEE_BASE} {new_ppm} {new_min_htlc_msat} {new_max_htlc_msat}"
        );

        if std::env::var("EXECUTE_SETCHANNEL").is_ok() {
            log::info!("executing `{cmd} {args}` {alias}");

            let splitted_args: Vec<&str> = args.split(' ').collect();
            let result = crate::cmd::cmd_result(cmd, &splitted_args);
            log::debug!("cmd return: {result}");

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
        log::info!(
            "EQU state:{} routed_24h_msat:{routed_24h_msat} no changes in {short_channel_id} with {alias}, skipping",
            state.as_str()
        )
    };
    data
}

pub fn did_forward<'a>(
    short_channel_id: &str,
    forwards: &'a [crate::cmd::Forward],
) -> Vec<&'a crate::cmd::Forward> {
    forwards
        .iter()
        .filter(|f| f.out_channel.as_deref() == Some(short_channel_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_state_gives_depleted_balance_precedence() {
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT - 1, false),
            FeeState::Depleted
        );
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT - 1, true),
            FeeState::Depleted
        );
    }

    #[test]
    fn fee_state_distinguishes_bootstrap_and_normal_channels() {
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT, false),
            FeeState::Bootstrap
        );
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT, true),
            FeeState::Normal
        );
    }

    #[test]
    fn percentage_increases_round_up() {
        assert_eq!(increase_ppm_ceil(10, FORWARD_INCREASE_PERCENT), 11);
        assert_eq!(increase_ppm_ceil(100, FORWARD_INCREASE_PERCENT), 105);
        assert_eq!(increase_ppm_ceil(101, DEPLETED_INCREASE_PERCENT), 103);
    }

    #[test]
    fn percentage_decreases_round_down() {
        assert_eq!(decrease_ppm_floor(2_500, BOOTSTRAP_DECREASE_PERCENT), 2_125);
        assert_eq!(decrease_ppm_floor(100, NORMAL_DECREASE_PERCENT), 98);
    }

    #[test]
    fn recent_forward_increases_every_channel_state() {
        for state in [FeeState::Bootstrap, FeeState::Normal, FeeState::Depleted] {
            assert_eq!(adjusted_ppm(100, state, ForwardActivity::MeetsMinimum), 105);
        }
        assert_eq!(
            adjusted_ppm(PPM_MIN, FeeState::Depleted, ForwardActivity::MeetsMinimum),
            2
        );
    }

    #[test]
    fn low_volume_forwarding_keeps_ppm_unchanged() {
        for state in [FeeState::Bootstrap, FeeState::Normal, FeeState::Depleted] {
            assert_eq!(adjusted_ppm(100, state, ForwardActivity::BelowMinimum), 100);
        }
    }

    #[test]
    fn forwarding_activity_requires_5000_routed_sats() {
        assert_eq!(forward_activity(0, 0), ForwardActivity::None);
        assert_eq!(
            forward_activity(1, MIN_ROUTED_24H_SAT * 1000 - 1),
            ForwardActivity::BelowMinimum
        );
        assert_eq!(
            forward_activity(1, MIN_ROUTED_24H_SAT * 1000),
            ForwardActivity::MeetsMinimum
        );
    }

    #[test]
    fn idle_policy_depends_on_channel_state() {
        assert_eq!(
            adjusted_ppm(100, FeeState::Bootstrap, ForwardActivity::None),
            85
        );
        assert_eq!(
            adjusted_ppm(100, FeeState::Normal, ForwardActivity::None),
            98
        );
        assert_eq!(
            adjusted_ppm(100, FeeState::Depleted, ForwardActivity::None),
            101
        );
    }

    #[test]
    fn adjusted_ppm_respects_bounds() {
        assert_eq!(
            adjusted_ppm(PPM_MIN, FeeState::Bootstrap, ForwardActivity::None),
            PPM_MIN
        );
        assert_eq!(
            adjusted_ppm(PPM_MAX, FeeState::Depleted, ForwardActivity::None),
            PPM_MAX
        );
    }

    #[test]
    fn test_largest_power_of_two_leq() {
        // Test edge cases
        assert_eq!(largest_power_of_two_leq(0), 0);
        assert_eq!(largest_power_of_two_leq(1), 1);
        assert_eq!(largest_power_of_two_leq(2), 2);
        assert_eq!(largest_power_of_two_leq(3), 2);
        assert_eq!(largest_power_of_two_leq(4), 4);
        assert_eq!(largest_power_of_two_leq(5), 4);
        assert_eq!(largest_power_of_two_leq(6), 4);
        assert_eq!(largest_power_of_two_leq(7), 4);
        assert_eq!(largest_power_of_two_leq(8), 8);
        assert_eq!(largest_power_of_two_leq(9), 8);
        assert_eq!(largest_power_of_two_leq(10), 8);
        assert_eq!(largest_power_of_two_leq(15), 8);
        assert_eq!(largest_power_of_two_leq(16), 16);
        assert_eq!(largest_power_of_two_leq(17), 16);
        assert_eq!(largest_power_of_two_leq(31), 16);
        assert_eq!(largest_power_of_two_leq(32), 32);
        assert_eq!(largest_power_of_two_leq(33), 32);

        // Test larger values
        assert_eq!(largest_power_of_two_leq(1000), 512);
        assert_eq!(largest_power_of_two_leq(1024), 1024);
        assert_eq!(largest_power_of_two_leq(1025), 1024);
        assert_eq!(largest_power_of_two_leq(u64::MAX), 1 << 63); // 2^63, the highest bit
        assert_eq!(largest_power_of_two_leq(2450000000), 2147483648);
    }
}
