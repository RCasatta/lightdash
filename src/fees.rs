use std::cmp::{max, min, Ordering};
use std::collections::{HashMap, HashSet};

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
/// Datastore key under `lightdash` holding the unrounded ppm of each channel, so that the
/// percentage steps are applied without integer rounding bias at low ppm values.
const EXACT_PPM_KEY: &str = "fee_ppm_exact";

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
    let exact_ppms = load_exact_ppms();

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
            &alias_or_id,
            fund,
            our,
            &forwards_24h,
            ever_settled_out_channels.contains(short_channel_id.as_str()),
            avail,
            exact_ppms.get(&short_channel_id).copied(),
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

fn fee_state(local_balance_sat: u64, channel_capacity_sat: u64, ever_forwarded: bool) -> FeeState {
    let depleted_threshold_sat = DEPLETED_LOCAL_BALANCE_SAT.max(channel_capacity_sat / 20);
    if local_balance_sat < depleted_threshold_sat {
        FeeState::Depleted
    } else if ever_forwarded {
        FeeState::Normal
    } else {
        FeeState::Bootstrap
    }
}

/// Loads the unrounded ppm persisted by previous runs, keyed by short channel id.
fn load_exact_ppms() -> HashMap<String, f64> {
    let mut exact_ppms = HashMap::new();
    match crate::cmd::listdatastore(Some(&["lightdash", EXACT_PPM_KEY])) {
        Ok(datastore) => {
            for entry in datastore.datastore {
                // The key format is ["lightdash", EXACT_PPM_KEY, "short_channel_id"]
                if entry.key.len() != 3 {
                    continue;
                }
                if let Some(ppm) = entry.string.and_then(|s| s.parse::<f64>().ok()) {
                    exact_ppms.insert(entry.key[2].clone(), ppm);
                }
            }
            log::info!(
                "Loaded {} exact ppm values from datastore",
                exact_ppms.len()
            );
        }
        Err(e) => log::error!("Failed to load exact ppm values from datastore: {e}"),
    }
    exact_ppms
}

fn round_ppm(exact_ppm: f64) -> u64 {
    exact_ppm.round() as u64
}

/// The unrounded ppm to adjust from. The stored value is used only when it still rounds to
/// the ppm currently set on the channel; otherwise the channel fee was changed outside this
/// algorithm (manually, or never stored) and the current ppm is the new starting point.
fn starting_ppm(current_ppm: u64, stored_exact_ppm: Option<f64>) -> f64 {
    match stored_exact_ppm {
        Some(exact) if exact.is_finite() && round_ppm(exact) == current_ppm => exact,
        _ => current_ppm as f64,
    }
}

fn increase_ppm(exact_ppm: f64, percent: u64) -> f64 {
    exact_ppm * (100 + percent) as f64 / 100.0
}

fn decrease_ppm(exact_ppm: f64, percent: u64) -> f64 {
    exact_ppm * (100 - percent) as f64 / 100.0
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

/// Returns the new unrounded ppm; callers round it only when setting the channel fee.
fn adjusted_ppm(exact_ppm: f64, state: FeeState, activity: ForwardActivity) -> f64 {
    let adjusted = match activity {
        ForwardActivity::MeetsMinimum => increase_ppm(exact_ppm, FORWARD_INCREASE_PERCENT),
        ForwardActivity::BelowMinimum => exact_ppm,
        ForwardActivity::None => match state {
            FeeState::Bootstrap => decrease_ppm(exact_ppm, BOOTSTRAP_DECREASE_PERCENT),
            FeeState::Normal => decrease_ppm(exact_ppm, NORMAL_DECREASE_PERCENT),
            FeeState::Depleted => increase_ppm(exact_ppm, DEPLETED_INCREASE_PERCENT),
        },
    };

    adjusted.clamp(PPM_MIN as f64, PPM_MAX as f64)
}

fn save_exact_ppm(short_channel_id: &str, exact_ppm: f64) {
    if let Err(e) = crate::cmd::datastore_string(
        &["lightdash", EXACT_PPM_KEY, short_channel_id],
        &format!("{exact_ppm:.6}"),
        crate::cmd::DatastoreMode::CreateOrReplace,
    ) {
        log::error!("Failed to save exact ppm for {short_channel_id}: {e}");
    }
}

pub fn calc_setchannel(
    alias: &str,
    fund: &crate::cmd::Fund,
    our: &crate::cmd::Channel,
    forwards_24h: &[Forward],
    ever_forwarded: bool,
    avail: Option<f64>,
    stored_exact_ppm: Option<f64>,
) -> &'static str {
    let short_channel_id = fund.short_channel_id();
    let short_channel_id = short_channel_id.as_str();
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
    let channel_capacity_sat = fund.amount_msat / 1000;
    let state = fee_state(local_balance_sat, channel_capacity_sat, ever_forwarded);

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

    let exact_ppm = starting_ppm(current_ppm, stored_exact_ppm);
    let new_exact_ppm = adjusted_ppm(exact_ppm, state, activity);
    let new_ppm = round_ppm(new_exact_ppm);

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
            change_parts.push(format!(
                "ppm:{current_ppm}->{new_ppm} exact_ppm:{exact_ppm:.3}->{new_exact_ppm:.3}"
            ));
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
            "EQU state:{} routed_24h_msat:{routed_24h_msat} exact_ppm:{exact_ppm:.3}->{new_exact_ppm:.3} no changes in {short_channel_id} with {alias}, skipping",
            state.as_str()
        )
    };

    // Persist the unrounded ppm even when the rounded channel fee did not change, so that
    // small steps accumulate across runs.
    if std::env::var("EXECUTE_SETCHANNEL").is_ok()
        && stored_exact_ppm.map(|p| format!("{p:.6}")) != Some(format!("{new_exact_ppm:.6}"))
    {
        save_exact_ppm(short_channel_id, new_exact_ppm);
    }
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
            fee_state(DEPLETED_LOCAL_BALANCE_SAT - 1, 1_000_000, false),
            FeeState::Depleted
        );
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT - 1, 1_000_000, true),
            FeeState::Depleted
        );
    }

    #[test]
    fn fee_state_distinguishes_bootstrap_and_normal_channels() {
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT, 1_000_000, false),
            FeeState::Bootstrap
        );
        assert_eq!(
            fee_state(DEPLETED_LOCAL_BALANCE_SAT, 1_000_000, true),
            FeeState::Normal
        );
    }

    #[test]
    fn fee_state_scales_depleted_threshold_with_channel_capacity() {
        assert_eq!(fee_state(249_999, 5_000_000, true), FeeState::Depleted);
        assert_eq!(fee_state(250_000, 5_000_000, true), FeeState::Normal);
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn percentage_steps_are_exact() {
        assert_close(increase_ppm(10.0, FORWARD_INCREASE_PERCENT), 10.5);
        assert_close(increase_ppm(101.0, DEPLETED_INCREASE_PERCENT), 102.01);
        assert_close(decrease_ppm(2_500.0, BOOTSTRAP_DECREASE_PERCENT), 2_125.0);
        assert_close(decrease_ppm(14.0, NORMAL_DECREASE_PERCENT), 13.72);
    }

    #[test]
    fn round_ppm_rounds_to_nearest() {
        assert_eq!(round_ppm(13.72), 14);
        assert_eq!(round_ppm(1.05), 1);
        assert_eq!(round_ppm(1.5), 2);
    }

    #[test]
    fn starting_ppm_uses_stored_value_only_when_it_matches_current_ppm() {
        assert_close(starting_ppm(14, Some(13.72)), 13.72);
        assert_close(starting_ppm(14, None), 14.0);
        // Fee changed outside the algorithm, e.g. a manual bump.
        assert_close(starting_ppm(50, Some(1.3)), 50.0);
        assert_close(starting_ppm(14, Some(f64::NAN)), 14.0);
    }

    #[test]
    fn idle_day_at_low_ppm_decreases_by_two_percent_not_one_ppm() {
        // Integer floor rounding used to turn 14 into 13 (-7%); now it stays at 14.
        let exact = adjusted_ppm(14.0, FeeState::Normal, ForwardActivity::None);
        assert_close(exact, 13.72);
        assert_eq!(round_ppm(exact), 14);
    }

    #[test]
    fn low_ppm_keeps_intended_forward_to_idle_balance() {
        // One forwarding day recovers about two and a half idle days, at any ppm level.
        let mut exact = 10.0;
        for _ in 0..20 {
            exact = adjusted_ppm(exact, FeeState::Normal, ForwardActivity::MeetsMinimum);
            for _ in 0..2 {
                exact = adjusted_ppm(exact, FeeState::Normal, ForwardActivity::None);
            }
        }
        // 1.05 * 0.98^2 > 1, so the fee rises instead of sinking to PPM_MIN.
        assert!(exact > 10.0, "exact ppm drifted down to {exact}");
    }

    #[test]
    fn small_steps_accumulate_from_ppm_min() {
        let mut exact = PPM_MIN as f64;
        for _ in 0..9 {
            exact = adjusted_ppm(exact, FeeState::Normal, ForwardActivity::MeetsMinimum);
        }
        assert_eq!(round_ppm(exact), 2);
    }

    #[test]
    fn recent_forward_increases_every_channel_state() {
        for state in [FeeState::Bootstrap, FeeState::Normal, FeeState::Depleted] {
            assert_close(
                adjusted_ppm(100.0, state, ForwardActivity::MeetsMinimum),
                105.0,
            );
        }
    }

    #[test]
    fn low_volume_forwarding_keeps_ppm_unchanged() {
        for state in [FeeState::Bootstrap, FeeState::Normal, FeeState::Depleted] {
            assert_close(
                adjusted_ppm(100.0, state, ForwardActivity::BelowMinimum),
                100.0,
            );
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
        assert_close(
            adjusted_ppm(100.0, FeeState::Bootstrap, ForwardActivity::None),
            85.0,
        );
        assert_close(
            adjusted_ppm(100.0, FeeState::Normal, ForwardActivity::None),
            98.0,
        );
        assert_close(
            adjusted_ppm(100.0, FeeState::Depleted, ForwardActivity::None),
            101.0,
        );
    }

    #[test]
    fn adjusted_ppm_respects_bounds() {
        assert_close(
            adjusted_ppm(PPM_MIN as f64, FeeState::Bootstrap, ForwardActivity::None),
            PPM_MIN as f64,
        );
        assert_close(
            adjusted_ppm(PPM_MAX as f64, FeeState::Depleted, ForwardActivity::None),
            PPM_MAX as f64,
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
