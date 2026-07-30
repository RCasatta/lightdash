use crate::cmd::Fund;
use crate::store::Store;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

const SOURCE_PPM_FALLBACK: u64 = 300;
const SOURCE_PPM_TARGET_VALUE_MULTIPLIER: f64 = 0.3;
const TARGET_ELIGIBLE_MAX_BALANCE: f64 = 0.3;
const TARGET_REBALANCE_BALANCE: f64 = 0.5;
const MIN_AMOUNT_SAT: u64 = 10_000;
const BOOTSTRAP_MAX_PPM: u64 = crate::fees::PPM_MIN;
const BUDGET_PPM_MIN: u64 = crate::fees::PPM_MIN;
const BUDGET_PPM_REALIZED_FEE_MULTIPLIER: f64 = 0.6;
// Rebalance budget cap. Keep this below the general channel fee cap because
// this is what we are willing to pay, not what we are willing to charge.
const BUDGET_PPM_MAX: u64 = 1100;

// Bootstrap rebalances make the channel greater than the depleted threshold,
// triggering dynamic fee search.
const LOW_LOCAL_BOOTSTRAP_THRESHOLD_SAT: u64 = 10;
const LOW_LOCAL_BOOTSTRAP_AMOUNT_SAT: u64 = crate::fees::DEPLETED_LOCAL_BALANCE_SAT * 2;

// We are okay paying a lot for first bootstrap, because it is one-shot and amount-limited.
const LOW_LOCAL_BOOTSTRAP_MAX_PPM: u64 = BUDGET_PPM_MAX;

const BOOTSTRAP_CAPACITY_DIVISOR: u64 = 20;
const CANDIDATE_DEPLETE_UP_TO_PERCENT: &str = "0.5";
const CANDIDATE_DEPLETE_UP_TO_AMOUNT_SAT: u64 = 1_000_000;
const CMD: &str = "lightning-cli";
/// Minimum balance percentage (our funds / total capacity) for a channel to be used as candidate.
const MIN_CANDIDATE_BALANCE: f64 = 0.7;

/// Computes candidates with liquidity for a target's Sling rebalance.
///
/// Sling's `outppm` parameter filters candidates by ppm but not by balance.
/// We want to pull from channels where:
///   1. ppm < source_ppm_max (drain channels cheap enough for this target)
///   2. balance > MIN_CANDIDATE_BALANCE (only drain channels that have enough liquidity on our side)
///
/// Since sling doesn't support filtering by balance natively, we explicitly compute
/// the candidates list ourselves and pass it via the `candidates` parameter.
fn compute_candidates<'a>(
    store: &'a Store,
    channels: &'a [Fund],
    source_ppm_max: u64,
) -> Vec<&'a String> {
    channels
        .iter()
        .filter_map(|ch| {
            let scid = ch.short_channel_id.as_ref()?;
            let our = store.get_channel(scid, &store.info.id)?;
            if our.fee_per_millionth < source_ppm_max && ch.perc_float() > MIN_CANDIDATE_BALANCE {
                Some(scid)
            } else {
                None
            }
        })
        .collect()
}

/// Formats candidates as a JSON array string for sling's `candidates` parameter.
fn candidates_to_json(candidates: &[&String]) -> String {
    format!(
        "[{}]",
        candidates
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn is_target_eligible(balance: f64) -> bool {
    balance <= TARGET_ELIGIBLE_MAX_BALANCE
}

fn usable_ppm(ppm: Option<f64>) -> Option<f64> {
    ppm.filter(|ppm| ppm.is_finite() && *ppm > 0.0)
}

fn compute_source_ppm_max(historical_fee_ppm: Option<f64>, channel_ppm: Option<u64>) -> u64 {
    let Some(historical_fee_ppm) = usable_ppm(historical_fee_ppm) else {
        return SOURCE_PPM_FALLBACK;
    };

    let target_value_ppm = channel_ppm
        .map(|channel_ppm| historical_fee_ppm.min(channel_ppm as f64))
        .unwrap_or(historical_fee_ppm);

    ((target_value_ppm * SOURCE_PPM_TARGET_VALUE_MULTIPLIER) as u64)
        .clamp(BUDGET_PPM_MIN, BUDGET_PPM_MAX)
}

fn compute_budget_ppm(
    tppm: Option<f64>,
    historical_fee_ppm: Option<f64>,
    channel_ppm: Option<u64>,
) -> u64 {
    let realized_metrics = [usable_ppm(tppm), usable_ppm(historical_fee_ppm)];
    let has_realized_fee_history = realized_metrics.iter().any(Option::is_some);
    if !has_realized_fee_history {
        return BOOTSTRAP_MAX_PPM;
    }

    let (sum, count) = realized_metrics
        .into_iter()
        .flatten()
        .fold((0.0, 0u64), |(sum, count), ppm| (sum + ppm, count + 1));

    if count == 0 {
        return BOOTSTRAP_MAX_PPM;
    }

    let budget = (sum / count as f64) * BUDGET_PPM_REALIZED_FEE_MULTIPLIER;
    let budget = (budget as u64).clamp(BUDGET_PPM_MIN, BUDGET_PPM_MAX);

    match channel_ppm {
        Some(channel_ppm) => budget.min(channel_ppm),
        None => budget,
    }
}

fn compute_base_rebalance_amount(channel_capacity_sat: u64) -> Option<u64> {
    let target_amount = channel_capacity_sat / BOOTSTRAP_CAPACITY_DIVISOR;
    let amount = target_amount - (target_amount % 4);

    if amount < MIN_AMOUNT_SAT {
        None
    } else {
        Some(amount)
    }
}

fn compute_capacity_rebalance_amounts(
    channel_capacity_sat: u64,
    local_balance_sat: u64,
) -> Option<u64> {
    let target_local_sat = (channel_capacity_sat as f64 * TARGET_REBALANCE_BALANCE) as u64;
    let missing_to_target_sat = target_local_sat.saturating_sub(local_balance_sat);
    let bootstrap_amount = compute_base_rebalance_amount(channel_capacity_sat)?;
    let amount = bootstrap_amount.min(missing_to_target_sat);
    let amount = amount - (amount % 4);

    if amount < MIN_AMOUNT_SAT {
        None
    } else {
        Some(amount)
    }
}

fn should_bootstrap_low_local(local_balance_sat: u64) -> bool {
    local_balance_sat < LOW_LOCAL_BOOTSTRAP_THRESHOLD_SAT
}

fn rebalance_jitter_seed(scid: &str) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    scid.hash(&mut hasher);
    now.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    hasher.finish()
}

fn compute_job_amount(amount_sat: u64, jitter_seed: u64) -> u64 {
    let values = [10_000, 20_000, 40_000, 80_000, 160_000, 320_000];
    let index = (jitter_seed % values.len() as u64) as usize;
    values[index].min(amount_sat).max(MIN_AMOUNT_SAT)
}

fn low_local_bootstrap_args<'a>(
    scid: &'a str,
    candidates_arg: &'a str,
    amount_arg: &'a str,
    onceamount_arg: &'a str,
    maxppm_arg: &'a str,
) -> [&'a str; 8] {
    [
        "sling-once",
        "-k",
        scid,
        "direction=pull",
        candidates_arg,
        maxppm_arg,
        amount_arg,
        onceamount_arg,
    ]
}

fn reset_existing_sling_jobs() {
    log::info!(
        "EXECUTE_SLING is set, stopping and deleting existing sling jobs before creating new ones"
    );

    let result = crate::cmd::cmd_result(CMD, &["sling-stop"]);
    log::debug!("sling-stop return: {result}");

    let result = crate::cmd::cmd_result(CMD, &["sling-deletejob", "all"]);
    log::debug!("sling-deletejob all return: {result}");
}

fn start_sling_jobs() {
    log::info!("EXECUTE_SLING is set, starting created sling jobs with sling-go");
    let result = crate::cmd::cmd_result(CMD, &["sling-go"]);
    log::debug!("sling-go return: {result}");
}

fn get_sling_stats(scid: Option<&str>) -> Value {
    if crate::cmd::using_test_data() {
        match scid {
            Some(_) => {
                crate::cmd::cmd_result("cat", &["test-json/sling-stats/sling-stats-details.json"])
            }
            None => crate::cmd::cmd_result(
                "cat",
                &["test-json/sling-stats/sling-stats-20260416T060307Z.json"],
            ),
        }
    } else {
        match scid {
            Some(scid) => crate::cmd::cmd_result(CMD, &["sling-stats", scid, "true"]),
            None => crate::cmd::cmd_result(CMD, &["sling-stats", "true"]),
        }
    }
}

pub fn current_sling_stats() -> Value {
    let mut stats = get_sling_stats(None);
    enrich_sling_stats_with_last_channel_partner(&mut stats, |scid| get_sling_stats(Some(scid)));
    stats
}

fn enrich_sling_stats_with_last_channel_partner(
    stats: &mut Value,
    mut get_details: impl FnMut(&str) -> Value,
) {
    let Some(entries) = stats.as_array_mut() else {
        log::error!("sling-stats snapshot is not an array, skipping channel detail enrichment");
        return;
    };

    for entry in entries {
        let Some(scid) = entry.get("scid").and_then(Value::as_str) else {
            continue;
        };

        let details = get_details(scid);
        let Some(last_channel_partner) = details
            .get("successes_in_time_window")
            .and_then(|v| v.get("last_channel_partner"))
            .and_then(Value::as_str)
        else {
            continue;
        };

        if let Some(entry_object) = entry.as_object_mut() {
            entry_object.insert(
                "last_channel_partner".to_string(),
                Value::String(last_channel_partner.to_string()),
            );
        }
    }
}

/// We search empty channels and try to pull sats on them from a list of candidates that are ~full and cheap.
pub fn run_sling(store: &Store) {
    let channels = store.normal_channels();
    log::info!(
        "Sling inputs: channels:{} target_eligible_balance<={:.0}% rebalance_target:{:.0}% candidate_balance>=:{:.0}% candidate_fallback_ppm:<{} candidate_target_value_multiplier:{:.0}% min_amount:{}sat depleteuptopercent:{} depleteuptoamount:{}",
        channels.len(),
        TARGET_ELIGIBLE_MAX_BALANCE * 100.0,
        TARGET_REBALANCE_BALANCE * 100.0,
        MIN_CANDIDATE_BALANCE * 100.0,
        SOURCE_PPM_FALLBACK,
        SOURCE_PPM_TARGET_VALUE_MULTIPLIER * 100.0,
        MIN_AMOUNT_SAT,
        CANDIDATE_DEPLETE_UP_TO_PERCENT,
        CANDIDATE_DEPLETE_UP_TO_AMOUNT_SAT
    );

    let execute_sling = std::env::var("EXECUTE_SLING").is_ok();
    if execute_sling {
        reset_existing_sling_jobs();
    }

    let mut skipped_balance = 0u64;
    let mut skipped_missing_scid = 0u64;
    let mut targets_without_local_channel_info = 0u64;
    let mut skipped_small_amount = 0u64;
    let mut skipped_no_candidates = 0u64;
    let mut suggested = 0u64;
    let mut bootstrap = 0u64;

    for channel in &channels {
        let balance = channel.perc_float();
        if !is_target_eligible(balance) {
            skipped_balance += 1;
            continue;
        }

        let Some(scid) = &channel.short_channel_id else {
            skipped_missing_scid += 1;
            continue;
        };

        let alias = store.get_node_alias(&channel.peer_id);
        let my_ppm = store
            .get_channel(scid, &store.info.id)
            .map(|our| our.fee_per_millionth);
        if my_ppm.is_none() {
            targets_without_local_channel_info += 1;
            log::info!(
                "missing local channel info for scid:{scid}, continuing with forward-driven rebalance logic"
            );
        }
        let my_ppm_log = my_ppm
            .map(|ppm| ppm.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let tppm = store.get_channel_time_decayed_fee_ppm(scid);
        let historical_fee_ppm = store.get_channel_effective_fee_ppm(scid);
        let budget_ppm = compute_budget_ppm(tppm, historical_fee_ppm, my_ppm);
        let tppm_log = tppm
            .map(|ppm| ppm.trunc().to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let historical_fee_ppm_log = historical_fee_ppm
            .map(|ppm| ppm.trunc().to_string())
            .unwrap_or_else(|| "n/a".to_string());

        let channel_capacity_sat = channel.amount_msat / 1000;
        let local_balance_sat = channel.our_amount_msat / 1000;

        if should_bootstrap_low_local(local_balance_sat) {
            let candidates = compute_candidates(store, &channels, SOURCE_PPM_FALLBACK);
            if candidates.is_empty() {
                skipped_no_candidates += 1;
                let result = "skip-no-cand";
                log::info!(
                    "balance:{:>5.1}% amount:{LOW_LOCAL_BOOTSTRAP_AMOUNT_SAT:>6}s tppm:{tppm_log:>6} hist_fee_ppm:{historical_fee_ppm_log:>6} channel_ppm:{my_ppm_log:>5} maxppm:{LOW_LOCAL_BOOTSTRAP_MAX_PPM:>4} src_ppm_max:<{SOURCE_PPM_FALLBACK:>4} cand:{:>3} result:{result:<12} alias:{alias}",
                    balance * 100.0,
                    0,
                );
                continue;
            }

            bootstrap += 1;
            let candidates_arg = format!("candidates={}", candidates_to_json(&candidates));
            let scid_arg = format!("scid={scid}");
            let amount_arg = format!("amount={LOW_LOCAL_BOOTSTRAP_AMOUNT_SAT}");
            let onceamount_arg = format!("onceamount={LOW_LOCAL_BOOTSTRAP_AMOUNT_SAT}");
            let maxppm_arg = format!("maxppm={LOW_LOCAL_BOOTSTRAP_MAX_PPM}");
            let args = low_local_bootstrap_args(
                &scid_arg,
                &candidates_arg,
                &amount_arg,
                &onceamount_arg,
                &maxppm_arg,
            );
            let result = "bootstrap";
            log::info!(
                "balance:{:>5.1}% amount:{LOW_LOCAL_BOOTSTRAP_AMOUNT_SAT:>6}s tppm:{tppm_log:>6} hist_fee_ppm:{historical_fee_ppm_log:>6} channel_ppm:{my_ppm_log:>5} maxppm:{LOW_LOCAL_BOOTSTRAP_MAX_PPM:>4} src_ppm_max:<{SOURCE_PPM_FALLBACK:>4} cand:{:>3} result:{result:<12} alias:{alias}",
                balance * 100.0,
                candidates.len(),
            );
            log::debug!("{alias} candidates: {candidates:?}");
            log::debug!("{CMD} {}", args.join(" "));
            if execute_sling {
                log::info!("executing `{CMD} sling-once` {alias} scid:{scid}");

                let result = crate::cmd::cmd_result(CMD, &args);
                log::debug!("cmd return: {result}");
            }
            continue;
        }

        let source_ppm_max = compute_source_ppm_max(historical_fee_ppm, my_ppm);
        let Some(amount_hint) =
            compute_capacity_rebalance_amounts(channel_capacity_sat, local_balance_sat)
        else {
            skipped_small_amount += 1;
            let result = "skip-small";
            log::info!(
                "balance:{:>5.1}% amount:{:>6}s tppm:{tppm_log:>6} hist_fee_ppm:{historical_fee_ppm_log:>6} channel_ppm:{my_ppm_log:>5} maxppm:{budget_ppm:>4} src_ppm_max:<{source_ppm_max:>4} cand:{:>3} result:{result:<12} alias:{alias}",
                balance * 100.0,
                0,
                "n/a",
            );
            continue;
        };

        let job_amount = compute_job_amount(amount_hint, rebalance_jitter_seed(scid));
        let candidates = compute_candidates(store, &channels, source_ppm_max);
        if candidates.is_empty() {
            skipped_no_candidates += 1;
            let result = "skip-no-cand";
            log::info!(
                "balance:{:>5.1}% amount:{job_amount:>6}s tppm:{tppm_log:>6} hist_fee_ppm:{historical_fee_ppm_log:>6} channel_ppm:{my_ppm_log:>5} maxppm:{budget_ppm:>4} src_ppm_max:<{source_ppm_max:>4} cand:{:>3} result:{result:<12} alias:{alias}",
                balance * 100.0,
                0,
            );
            continue;
        }

        let candidates_arg = format!("candidates={}", candidates_to_json(&candidates));

        // Build arguments as a Vec to avoid shell quoting issues.
        // When calling a program directly (not via shell), we pass raw values
        // without shell-style quoting like single quotes around the JSON array.
        let amount_arg = format!("amount={job_amount}");
        let maxppm_arg = format!("maxppm={budget_ppm}");
        let target_arg = format!("target={TARGET_REBALANCE_BALANCE}");
        let deplete_percent_arg = format!("depleteuptopercent={CANDIDATE_DEPLETE_UP_TO_PERCENT}");
        let deplete_amount_arg = format!("depleteuptoamount={CANDIDATE_DEPLETE_UP_TO_AMOUNT_SAT}");
        let args = [
            "sling-job",
            "-k",
            &format!("scid={scid}"),
            "direction=pull",
            &amount_arg,
            &maxppm_arg,
            &target_arg,
            &candidates_arg,
            &deplete_percent_arg,
            &deplete_amount_arg,
        ];
        let result = "job";
        log::info!(
            "balance:{:>5.1}% amount:{job_amount:>6}s tppm:{tppm_log:>6} hist_fee_ppm:{historical_fee_ppm_log:>6} channel_ppm:{my_ppm_log:>5} maxppm:{budget_ppm:>4} src_ppm_max:<{source_ppm_max:>4} cand:{:>3} result:{result:<12} alias:{alias}",
            balance * 100.0,
            candidates.len(),
        );
        log::debug!("{alias} candidates: {candidates:?}");
        log::debug!("{CMD} {}", args.join(" "));
        if execute_sling {
            log::info!("executing `{CMD} sling-job` {alias} scid:{scid}");

            let result = crate::cmd::cmd_result(CMD, &args);
            log::debug!("cmd return: {result}");
        }
        suggested += 1;
    }

    log::info!(
        "Sling summary: suggested:{} bootstrap:{} skipped_balance:{} skipped_small_amount:{} skipped_no_candidates:{} skipped_missing_scid:{} targets_without_local_channel_info:{}",
        suggested,
        bootstrap,
        skipped_balance,
        skipped_small_amount,
        skipped_no_candidates,
        skipped_missing_scid,
        targets_without_local_channel_info
    );

    if execute_sling && suggested > 0 {
        start_sling_jobs();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_base_rebalance_amount, compute_budget_ppm, compute_capacity_rebalance_amounts,
        compute_job_amount, compute_source_ppm_max, enrich_sling_stats_with_last_channel_partner,
        is_target_eligible, low_local_bootstrap_args, should_bootstrap_low_local,
        BOOTSTRAP_MAX_PPM, BUDGET_PPM_MAX, BUDGET_PPM_MIN, BUDGET_PPM_REALIZED_FEE_MULTIPLIER,
        SOURCE_PPM_FALLBACK,
    };
    use serde_json::Value;

    fn realized_fee_budget(ppm: f64) -> u64 {
        (ppm * BUDGET_PPM_REALIZED_FEE_MULTIPLIER) as u64
    }

    fn read_json(content: &str) -> Value {
        serde_json::from_str(content).unwrap()
    }

    #[test]
    fn compute_budget_ppm_uses_realized_fee_multiplier_on_metric_average() {
        assert_eq!(
            compute_budget_ppm(Some(2_947.0), Some(316.0), Some(1_223)),
            realized_fee_budget((2_947.0 + 316.0) / 2.0)
        );
        assert_eq!(
            compute_budget_ppm(Some(400.0), Some(200.0), Some(300)),
            realized_fee_budget((400.0 + 200.0) / 2.0)
        );
    }

    #[test]
    fn compute_budget_ppm_falls_back_to_realized_fee_multiplier_on_available_metric() {
        assert_eq!(
            compute_budget_ppm(Some(400.0), None, None),
            realized_fee_budget(400.0)
        );
        assert_eq!(
            compute_budget_ppm(None, Some(200.0), None),
            realized_fee_budget(200.0)
        );
    }

    #[test]
    fn compute_budget_ppm_falls_back_without_realized_fee_history() {
        assert_eq!(compute_budget_ppm(None, None, None), BOOTSTRAP_MAX_PPM);
        assert_eq!(
            compute_budget_ppm(None, None, Some(2_500)),
            BOOTSTRAP_MAX_PPM
        );
    }

    #[test]
    fn compute_budget_ppm_ignores_unusable_metrics() {
        assert_eq!(
            compute_budget_ppm(Some(f64::NAN), Some(300.0), None),
            realized_fee_budget(300.0)
        );
        assert_eq!(
            compute_budget_ppm(Some(400.0), Some(0.0), None),
            realized_fee_budget(400.0)
        );
        assert_eq!(
            compute_budget_ppm(Some(0.0), Some(f64::INFINITY), None),
            BOOTSTRAP_MAX_PPM
        );
    }

    #[test]
    fn compute_budget_ppm_is_clamped() {
        assert_eq!(
            compute_budget_ppm(Some(1.0), Some(1.0), None),
            BUDGET_PPM_MIN
        );
        assert_eq!(
            compute_budget_ppm(Some(100_000.0), Some(100_000.0), None),
            BUDGET_PPM_MAX
        );
    }

    #[test]
    fn compute_budget_ppm_never_exceeds_channel_ppm() {
        assert_eq!(compute_budget_ppm(Some(7.0), Some(174.0), Some(10)), 10);
        assert_eq!(compute_budget_ppm(Some(40.0), Some(407.0), Some(78)), 78);
    }

    #[test]
    fn compute_source_ppm_max_uses_thirty_percent_of_target_value() {
        assert_eq!(compute_source_ppm_max(Some(500.0), Some(500)), 150);
        assert_eq!(compute_source_ppm_max(Some(500.0), Some(2_000)), 150);
        assert_eq!(compute_source_ppm_max(Some(500.0), None), 150);
        assert_eq!(compute_source_ppm_max(Some(1_000.0), Some(1_000)), 300);
        assert_eq!(compute_source_ppm_max(Some(2_000.0), Some(2_000)), 600);
    }

    #[test]
    fn compute_source_ppm_max_uses_lower_current_channel_ppm() {
        assert_eq!(compute_source_ppm_max(Some(2_000.0), Some(1_000)), 300);
    }

    #[test]
    fn compute_source_ppm_max_falls_back_without_history() {
        assert_eq!(
            compute_source_ppm_max(None, Some(2_000)),
            SOURCE_PPM_FALLBACK
        );
        assert_eq!(
            compute_source_ppm_max(Some(f64::NAN), Some(2_000)),
            SOURCE_PPM_FALLBACK
        );
    }

    #[test]
    fn compute_source_ppm_max_is_clamped() {
        assert_eq!(compute_source_ppm_max(Some(1.0), Some(1)), BUDGET_PPM_MIN);
        assert_eq!(
            compute_source_ppm_max(Some(100_000.0), Some(100_000)),
            BUDGET_PPM_MAX
        );
    }

    #[test]
    fn compute_base_rebalance_amount_uses_five_percent_for_large_channels() {
        assert_eq!(compute_base_rebalance_amount(2_000_000), Some(100_000));
    }

    #[test]
    fn compute_base_rebalance_amount_uses_five_percent_for_smaller_channels() {
        assert_eq!(compute_base_rebalance_amount(300_000), Some(15_000));
    }

    #[test]
    fn compute_base_rebalance_amount_skips_below_minimum() {
        assert_eq!(compute_base_rebalance_amount(199_999), None);
    }

    #[test]
    fn compute_capacity_rebalance_amounts_caps_at_missing_target_balance() {
        assert_eq!(
            compute_capacity_rebalance_amounts(1_000_000, 490_000),
            Some(10_000)
        );
    }

    #[test]
    fn compute_capacity_rebalance_amounts_skips_when_missing_target_is_too_small() {
        assert_eq!(compute_capacity_rebalance_amounts(1_000_000, 495_000), None);
    }

    #[test]
    fn target_eligibility_uses_thirty_percent_balance() {
        assert!(is_target_eligible(0.30));
        assert!(!is_target_eligible(0.31));
    }

    #[test]
    fn low_local_bootstrap_uses_sling_once_arguments() {
        assert_eq!(
            low_local_bootstrap_args(
                "scid=1x2x3",
                "candidates=[]",
                "amount=60000",
                "onceamount=60000",
                "maxppm=500",
            ),
            [
                "sling-once",
                "-k",
                "scid=1x2x3",
                "direction=pull",
                "candidates=[]",
                "maxppm=500",
                "amount=60000",
                "onceamount=60000",
            ]
        );
    }

    #[test]
    fn low_local_bootstrap_includes_dust_balances_below_ten_sats() {
        assert!(should_bootstrap_low_local(0));
        assert!(should_bootstrap_low_local(2));
        assert!(should_bootstrap_low_local(9));
        assert!(!should_bootstrap_low_local(10));
    }

    #[test]
    fn compute_job_amount_returns_random_predefined_value() {
        assert_eq!(compute_job_amount(200_000, 0), 10_000);
        assert_eq!(compute_job_amount(200_000, 1), 20_000);
        assert_eq!(compute_job_amount(200_000, 2), 40_000);
        assert_eq!(compute_job_amount(200_000, 3), 80_000);
        assert_eq!(compute_job_amount(200_000, 4), 160_000);
        assert_eq!(compute_job_amount(200_000, 5), 200_000);
        assert_eq!(compute_job_amount(2_000_000, 5), 320_000);
        assert_eq!(compute_job_amount(10_000, 2), 10_000);
    }

    #[test]
    fn enriches_snapshot_entries_with_last_channel_partner_when_available() {
        let mut stats = read_json(
            r#"
            [
              {
                "alias": "HOPPINGSQUIRREL",
                "last_route_taken": "Never",
                "last_success_reb": "Never",
                "pubkey": "029fe435040c8b665f731f2b0c81d039238ef1e3a1b1de0afac2b476361a26d675",
                "rebamount": "0",
                "scid": "882249x867x0",
                "status": ["1:NoCheapRoute"],
                "w_feeppm": 0
              },
              {
                "alias": "SLEEPYWHISPER",
                "last_route_taken": "2026-04-16 07:23:22",
                "last_success_reb": "2026-04-16 07:23:22",
                "pubkey": "0362dfd94dab64f1d00775aeae4365c1755360353b2f0a54d6f37cc8d438aed008",
                "rebamount": "25,799",
                "scid": "866191x460x2",
                "status": ["1:Balanced"],
                "w_feeppm": 1019
              }
            ]
            "#,
        );
        let details = read_json(
            r#"
            {
              "successes_in_time_window": {
                "last_channel_partner": "867798x3251x1"
              }
            }
            "#,
        );

        enrich_sling_stats_with_last_channel_partner(&mut stats, |_scid| details.clone());

        let entries = stats.as_array().unwrap();
        assert_eq!(
            entries[0]
                .get("last_channel_partner")
                .and_then(Value::as_str),
            Some("867798x3251x1")
        );
        assert_eq!(
            entries[1]
                .get("last_channel_partner")
                .and_then(Value::as_str),
            Some("867798x3251x1")
        );
    }
}
