use crate::cmd::{Fund, SettledForward};
use crate::store::Store;

const SOURCE_PPM_MAX: u64 = 300;
const MAX_BALANCE: f64 = 0.3;
const LOOKBACK_DAYS: i64 = 30;
const AMOUNT: u64 = 100000;
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
fn compute_candidates<'a>(store: &'a Store, channels: &'a [Fund]) -> Vec<&'a String> {
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

#[derive(Default)]
struct RecentOutboundStats {
    count: u64,
    fees_sat: u64,
    routed_sat: u64,
}

fn recent_outbound_stats(
    settled_forwards: &[SettledForward],
    short_channel_id: &str,
) -> RecentOutboundStats {
    settled_forwards
        .into_iter()
        .filter(|f| f.out_channel == short_channel_id)
        .fold(RecentOutboundStats::default(), |mut acc, f| {
            acc.count += 1;
            acc.fees_sat += f.fee_sat;
            acc.routed_sat += f.out_sat;
            acc
        })
}

fn compute_max_ppm(recent_fees_sat: u64) -> u64 {
    if AMOUNT == 0 {
        return 0;
    }

    // Cap rebalance spend to a third the realized fee rate over the last 30 days.
    // This stays below the recent routing income for the channel even if the
    // full onceamount is consumed.
    recent_fees_sat.saturating_mul(1_000_000) / AMOUNT / 3
}

/// We search empty channels and try to pull sats on them from a list of candidates that are ~full and cheap.
pub fn run_sling(store: &Store) {
    let channels = store.normal_channels();
    let recent_settled = store.filter_settled_forwards_by_days(LOOKBACK_DAYS);
    log::info!(
        "Sling inputs: channels:{} recent_settled_{}d:{} target_balance<=:{:.0}% candidate_balance>=:{:.0}% amount:{}sat",
        channels.len(),
        LOOKBACK_DAYS,
        recent_settled.len(),
        MAX_BALANCE * 100.0,
        MIN_CANDIDATE_BALANCE * 100.0,
        AMOUNT
    );

    let candidates = compute_candidates(store, &channels);
    if candidates.is_empty() {
        log::info!("No suitable candidates found (ppm < {SOURCE_PPM_MAX} and balance > {MIN_CANDIDATE_BALANCE})");
        return;
    }

    let candidates_json = candidates_to_json(&candidates);
    log::info!("Using {} candidates: {candidates_json}", candidates.len());

    let mut skipped_balance = 0u64;
    let mut skipped_missing_scid = 0u64;
    let mut skipped_missing_our = 0u64;
    let mut skipped_zero_budget = 0u64;
    let mut suggested = 0u64;

    for channel in channels {
        let balance = channel.perc_float();
        if balance > MAX_BALANCE {
            skipped_balance += 1;
            continue;
        }

        let Some(scid) = &channel.short_channel_id else {
            skipped_missing_scid += 1;
            continue;
        };

        let Some(our) = store.get_channel(scid, &store.info.id) else {
            skipped_missing_our += 1;
            continue;
        };

        let alias = store.get_node_alias(&channel.peer_id);
        let my_ppm = our.fee_per_millionth;
        let recent = recent_outbound_stats(&recent_settled, scid);
        let max_ppm = compute_max_ppm(recent.fees_sat);
        let realized_ppm = if recent.routed_sat > 0 {
            recent.fees_sat.saturating_mul(1_000_000) / recent.routed_sat
        } else {
            0
        };

        if max_ppm == 0 {
            skipped_zero_budget += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}d:{} recent_fees_{}d:{}s recent_routed_{}d:{}s realized_ppm:{} channel_ppm:{} maxppm:0, skipping",
                balance * 100.0,
                LOOKBACK_DAYS,
                recent.count,
                LOOKBACK_DAYS,
                recent.fees_sat,
                LOOKBACK_DAYS,
                recent.routed_sat,
                realized_ppm,
                my_ppm,
            );
            continue;
        }

        suggested += 1;

        // Build arguments as a Vec to avoid shell quoting issues.
        // When calling a program directly (not via shell), we pass raw values
        // without shell-style quoting like single quotes around the JSON array.
        let candidates_arg = format!("candidates={candidates_json}");
        let args = [
            "sling-once",
            "-k",
            &format!("scid={scid}"),
            "direction=pull",
            &candidates_arg,
            &format!("maxppm={max_ppm}"),
            &format!("amount={AMOUNT}"),
            &format!("onceamount={AMOUNT}"),
        ];
        log::info!(
            "{alias} balance:{:.1}% recent_out_{}d:{} recent_fees_{}d:{}s recent_routed_{}d:{}s realized_ppm:{} channel_ppm:{} maxppm:{} -> {CMD} {}",
            balance * 100.0,
            LOOKBACK_DAYS,
            recent.count,
            LOOKBACK_DAYS,
            recent.fees_sat,
            LOOKBACK_DAYS,
            recent.routed_sat,
            realized_ppm,
            my_ppm,
            max_ppm,
            args.join(" ")
        );
        if std::env::var("EXECUTE_SLING").is_ok() {
            log::info!("executing `{CMD} {}` {alias}", args.join(" "));

            let result = crate::cmd::cmd_result(CMD, &args);
            log::debug!("cmd return: {result}");
        }
    }

    log::info!(
        "Sling summary: suggested:{} skipped_balance:{} skipped_zero_budget:{} skipped_missing_scid:{} skipped_missing_our:{}",
        suggested,
        skipped_balance,
        skipped_zero_budget,
        skipped_missing_scid,
        skipped_missing_our
    );
}

#[cfg(test)]
mod tests {
    use super::compute_max_ppm;

    #[test]
    fn compute_max_ppm_is_zero_without_recent_fees() {
        assert_eq!(compute_max_ppm(0), 0);
    }

    #[test]
    fn compute_max_ppm_uses_third_of_recent_realized_fee_rate() {
        assert_eq!(compute_max_ppm(50), 166);
        assert_eq!(compute_max_ppm(1), 3);
    }
}
