use crate::cmd::Fund;
use crate::store::Store;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

const SOURCE_PPM_MAX: u64 = 300;
const TARGET_ELIGIBLE_MAX_BALANCE: f64 = 0.3;
const TARGET_REBALANCE_BALANCE: f64 = 0.5;
const MIN_AMOUNT_SAT: u64 = 10_000;
const BOOTSTRAP_MAX_PPM: u64 = crate::fees::PPM_MIN;
const BUDGET_PPM_MIN: u64 = crate::fees::PPM_MIN;
// Rebalance budget cap. Keep this below the general channel fee cap because
// this is what we are willing to pay, not what we are willing to charge.
const BUDGET_PPM_MAX: u64 = 1000;
const BOOTSTRAP_AMOUNT_CAP_SAT: u64 = 50_000;
const BOOTSTRAP_CAPACITY_DIVISOR: u64 = 20;
const REBALANCE_JOB_AMOUNT_CAP_SAT: u64 = 100_000;
const REBALANCE_JOB_AMOUNT_JITTER_PERCENT: u64 = 10;
const CANDIDATE_DEPLETE_UP_TO_PERCENT: &str = "0.5";
const CANDIDATE_DEPLETE_UP_TO_AMOUNT_SAT: u64 = 1_000_000;
const CMD: &str = "lightning-cli";
/// Minimum balance percentage (our funds / total capacity) for a channel to be used as candidate.
const MIN_CANDIDATE_BALANCE: f64 = 0.7;

/// Computes candidates with liquidity for sling rebalancing.
///
/// Sling's `outppm` parameter filters candidates by ppm but not by balance.
/// We want to pull from channels where:
///   1. ppm < SOURCE_PPM_MAX (drain low-fee channels)
///   2. balance > MIN_CANDIDATE_BALANCE (only drain channels that have enough liquidity on our side)
///
/// Since sling doesn't support filtering by balance natively, we explicitly compute
/// the candidates list ourselves and pass it via the `candidates` parameter.
fn compute_full_candidates<'a>(store: &'a Store, channels: &'a [Fund]) -> Vec<&'a String> {
    channels
        .iter()
        .filter_map(|ch| {
            let scid = ch.short_channel_id.as_ref()?;
            let our = store.get_channel(scid, &store.info.id)?;
            if our.fee_per_millionth < SOURCE_PPM_MAX && ch.perc_float() > MIN_CANDIDATE_BALANCE {
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

    let budget = (sum / count as f64) / 2.0;
    let budget = (budget as u64).clamp(BUDGET_PPM_MIN, BUDGET_PPM_MAX);

    match channel_ppm {
        Some(channel_ppm) => budget.min(channel_ppm),
        None => budget,
    }
}

fn compute_base_rebalance_amount(channel_capacity_sat: u64) -> Option<u64> {
    let target_amount =
        (channel_capacity_sat / BOOTSTRAP_CAPACITY_DIVISOR).min(BOOTSTRAP_AMOUNT_CAP_SAT);
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
    let capped_amount = amount_sat.min(REBALANCE_JOB_AMOUNT_CAP_SAT);
    let jitter_span = capped_amount * REBALANCE_JOB_AMOUNT_JITTER_PERCENT / 100;
    if jitter_span == 0 {
        return capped_amount;
    }

    let jitter_range = 2 * jitter_span + 1;
    let jitter = jitter_seed % jitter_range;
    let jittered_amount = capped_amount as i64 + jitter as i64 - jitter_span as i64;
    jittered_amount.clamp(MIN_AMOUNT_SAT as i64, REBALANCE_JOB_AMOUNT_CAP_SAT as i64) as u64
}

fn delete_existing_sling_jobs() {
    log::info!("EXECUTE_SLING is set, deleting existing sling jobs before creating new ones");
    let result = crate::cmd::cmd_result(CMD, &["sling-deletejob", "all"]);
    log::debug!("sling-deletejob all return: {result}");
}

fn get_sling_stats(scid: Option<&str>) -> Value {
    if cfg!(debug_assertions) {
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
        "Sling inputs: channels:{} target_eligible_balance<={:.0}% rebalance_target:{:.0}% candidate_balance>=:{:.0}% min_amount:{}sat depleteuptopercent:{} depleteuptoamount:{}",
        channels.len(),
        TARGET_ELIGIBLE_MAX_BALANCE * 100.0,
        TARGET_REBALANCE_BALANCE * 100.0,
        MIN_CANDIDATE_BALANCE * 100.0,
        MIN_AMOUNT_SAT,
        CANDIDATE_DEPLETE_UP_TO_PERCENT,
        CANDIDATE_DEPLETE_UP_TO_AMOUNT_SAT
    );

    let candidates = compute_full_candidates(store, &channels);
    log::info!(
        "{} candidates found (ppm < {SOURCE_PPM_MAX} and balance > {MIN_CANDIDATE_BALANCE})",
        candidates.len()
    );
    log::info!("candidates: {:?}", candidates);
    if candidates.is_empty() {
        return;
    }

    let candidates_json = candidates_to_json(&candidates);

    let execute_sling = std::env::var("EXECUTE_SLING").is_ok();
    if execute_sling {
        delete_existing_sling_jobs();
    }

    let mut skipped_balance = 0u64;
    let mut skipped_missing_scid = 0u64;
    let mut targets_without_local_channel_info = 0u64;
    let mut skipped_small_amount = 0u64;
    let mut suggested = 0u64;

    for channel in channels {
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
        let tppm = store.get_channel_time_decayed_variable_fee_ppm(scid);
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
        let Some(amount_hint) =
            compute_capacity_rebalance_amounts(channel_capacity_sat, local_balance_sat)
        else {
            skipped_small_amount += 1;
            log::info!(
                "{alias} balance:{:.1}% amount:0s tppm:{} historical_fee_ppm:{} channel_ppm:{} below_min_amount:{}s, skipping",
                balance * 100.0,
                tppm_log,
                historical_fee_ppm_log,
                my_ppm_log,
                MIN_AMOUNT_SAT,
            );
            continue;
        };

        suggested += 1;
        let job_amount = compute_job_amount(amount_hint, rebalance_jitter_seed(scid));

        // Build arguments as a Vec to avoid shell quoting issues.
        // When calling a program directly (not via shell), we pass raw values
        // without shell-style quoting like single quotes around the JSON array.
        let candidates_arg = format!("candidates={candidates_json}");
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
        log::info!(
            "{alias} balance:{:.1}% amount:{}s tppm:{} historical_fee_ppm:{} channel_ppm:{} maxppm:{}",
            balance * 100.0,
            job_amount,
            tppm_log,
            historical_fee_ppm_log,
            my_ppm_log,
            budget_ppm,
        );
        log::debug!("{CMD} {}", args.join(" "));
        if execute_sling {
            log::info!("executing `{CMD} sling-job` {alias} scid:{scid}");

            let result = crate::cmd::cmd_result(CMD, &args);
            log::debug!("cmd return: {result}");
        }
    }

    log::info!(
        "Sling summary: suggested:{} skipped_balance:{} skipped_small_amount:{} skipped_missing_scid:{} targets_without_local_channel_info:{}",
        suggested,
        skipped_balance,
        skipped_small_amount,
        skipped_missing_scid,
        targets_without_local_channel_info
    );
}

#[cfg(test)]
mod tests {
    use super::{
        compute_base_rebalance_amount, compute_budget_ppm, compute_capacity_rebalance_amounts,
        compute_job_amount, enrich_sling_stats_with_last_channel_partner, is_target_eligible,
        BOOTSTRAP_MAX_PPM, BUDGET_PPM_MAX, BUDGET_PPM_MIN, REBALANCE_JOB_AMOUNT_CAP_SAT,
        REBALANCE_JOB_AMOUNT_JITTER_PERCENT,
    };
    use serde_json::Value;

    fn read_json(content: &str) -> Value {
        serde_json::from_str(content).unwrap()
    }

    #[test]
    fn compute_budget_ppm_uses_half_of_available_metric_average() {
        assert_eq!(
            compute_budget_ppm(Some(2_947.0), Some(316.0), Some(1_223)),
            815
        );
        assert_eq!(compute_budget_ppm(Some(400.0), Some(200.0), Some(300)), 150);
    }

    #[test]
    fn compute_budget_ppm_falls_back_to_half_of_available_metric() {
        assert_eq!(compute_budget_ppm(Some(400.0), None, None), 200);
        assert_eq!(compute_budget_ppm(None, Some(200.0), None), 100);
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
        assert_eq!(compute_budget_ppm(Some(f64::NAN), Some(300.0), None), 150);
        assert_eq!(compute_budget_ppm(Some(400.0), Some(0.0), None), 200);
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
    fn compute_base_rebalance_amount_caps_large_channels() {
        assert_eq!(compute_base_rebalance_amount(2_000_000), Some(50_000));
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
    fn compute_job_amount_can_keep_the_base_amount_with_neutral_jitter() {
        let jitter_span = 50_000 * REBALANCE_JOB_AMOUNT_JITTER_PERCENT / 100;
        assert_eq!(compute_job_amount(50_000, jitter_span), 50_000);
    }

    #[test]
    fn compute_job_amount_applies_bounded_jitter() {
        let jitter_span = 50_000 * REBALANCE_JOB_AMOUNT_JITTER_PERCENT / 100;
        assert_eq!(compute_job_amount(50_000, 0), 45_000);
        assert_eq!(compute_job_amount(50_000, 2 * jitter_span), 55_000);
    }

    #[test]
    fn compute_job_amount_caps_after_jitter() {
        let jitter_span = REBALANCE_JOB_AMOUNT_CAP_SAT * REBALANCE_JOB_AMOUNT_JITTER_PERCENT / 100;
        assert_eq!(
            compute_job_amount(500_000, 2 * jitter_span),
            REBALANCE_JOB_AMOUNT_CAP_SAT
        );
    }

    #[test]
    fn compute_job_amount_keeps_minimum_without_rounding_to_multiple_of_four() {
        assert_eq!(compute_job_amount(10_001, 0), 10_000);
        assert_eq!(compute_job_amount(50_003, 5_000), 50_003);
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
