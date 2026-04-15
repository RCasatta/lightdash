use crate::cmd::{Fund, SettledForward};
use crate::store::Store;

const SOURCE_PPM_MAX: u64 = 300;
const MAX_BALANCE: f64 = 0.5;
const LOOKBACK_HOURS: i64 = 24;
const MIN_AMOUNT_SAT: u64 = 10_000;
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
    fee_ppm_weighted_sum: u64,
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
            acc.fee_ppm_weighted_sum += f.fee_ppm.saturating_mul(f.out_sat);
            acc.routed_sat += f.out_sat;
            acc
        })
}

impl RecentOutboundStats {
    fn average_fee_ppm(&self) -> u64 {
        if self.routed_sat == 0 {
            0
        } else {
            self.fee_ppm_weighted_sum / self.routed_sat
        }
    }
}

fn compute_max_ppm(avg_fee_ppm: u64) -> u64 {
    avg_fee_ppm / 2
}

fn stop_existing_sling_jobs() {
    log::info!("EXECUTE_SLING is set, stopping existing sling jobs before creating new ones");
    let result = crate::cmd::cmd_result(CMD, &["sling-stop"]);
    log::debug!("sling-stop return: {result}");
}

/// We search empty channels and try to pull sats on them from a list of candidates that are ~full and cheap.
pub fn run_sling(store: &Store) {
    let channels = store.normal_channels();
    let recent_settled = store.filter_forwards_by_hours(LOOKBACK_HOURS);
    let recent_settled: Vec<_> = recent_settled
        .into_iter()
        .filter(|f| f.status == "settled")
        .filter_map(|f| SettledForward::try_from(f).ok())
        .collect();
    log::info!(
        "Sling inputs: channels:{} recent_settled_{}h:{} target_balance<=:{:.0}% candidate_balance>=:{:.0}% min_amount:{}sat",
        channels.len(),
        LOOKBACK_HOURS,
        recent_settled.len(),
        MAX_BALANCE * 100.0,
        MIN_CANDIDATE_BALANCE * 100.0,
        MIN_AMOUNT_SAT
    );

    let candidates = compute_candidates(store, &channels);
    if candidates.is_empty() {
        log::info!("No suitable candidates found (ppm < {SOURCE_PPM_MAX} and balance > {MIN_CANDIDATE_BALANCE})");
        return;
    }

    let candidates_json = candidates_to_json(&candidates);
    log::info!("Using {} candidates: {candidates_json}", candidates.len());

    let execute_sling = std::env::var("EXECUTE_SLING").is_ok();
    if execute_sling {
        stop_existing_sling_jobs();
    }

    let mut skipped_balance = 0u64;
    let mut skipped_missing_scid = 0u64;
    let mut skipped_missing_our = 0u64;
    let mut skipped_no_recent_outbound = 0u64;
    let mut skipped_zero_budget = 0u64;
    let mut skipped_small_amount = 0u64;
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
            log::info!("missing local channel info for scid:{scid}, skipping");
            continue;
        };

        let alias = store.get_node_alias(&channel.peer_id);
        let my_ppm = our.fee_per_millionth;
        let recent = recent_outbound_stats(&recent_settled, scid);
        let average_fee_ppm = recent.average_fee_ppm();
        let amount = recent.routed_sat;
        let max_ppm = compute_max_ppm(average_fee_ppm);

        if recent.count == 0 {
            skipped_no_recent_outbound += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}h:0 amount:0s avg_fee_ppm:0 channel_ppm:{} no_recent_outbound, skipping",
                balance * 100.0,
                LOOKBACK_HOURS,
                my_ppm,
            );
            continue;
        }

        if amount < MIN_AMOUNT_SAT {
            skipped_small_amount += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}h:{} amount:{}s avg_fee_ppm:{} channel_ppm:{} below_min_amount:{}s, skipping",
                balance * 100.0,
                LOOKBACK_HOURS,
                recent.count,
                amount,
                average_fee_ppm,
                my_ppm,
                MIN_AMOUNT_SAT,
            );
            continue;
        }

        if max_ppm == 0 {
            skipped_zero_budget += 1;
            log::info!(
                "{alias} balance:{:.1}% recent_out_{}h:{} amount:{}s avg_fee_ppm:{} channel_ppm:{} maxppm:0, skipping",
                balance * 100.0,
                LOOKBACK_HOURS,
                recent.count,
                amount,
                average_fee_ppm,
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
            &format!("amount={amount}"),
            &format!("onceamount={amount}"),
        ];
        log::info!(
            "{alias} balance:{:.1}% recent_out_{}h:{} amount:{}s avg_fee_ppm:{} channel_ppm:{} maxppm:{} -> {CMD} {}",
            balance * 100.0,
            LOOKBACK_HOURS,
            recent.count,
            amount,
            average_fee_ppm,
            my_ppm,
            max_ppm,
            args.join(" ")
        );
        if execute_sling {
            log::info!("executing `{CMD} {}` {alias}", args.join(" "));

            let result = crate::cmd::cmd_result(CMD, &args);
            log::debug!("cmd return: {result}");
        }
    }

    log::info!(
        "Sling summary: suggested:{} skipped_balance:{} skipped_no_recent_outbound:{} skipped_small_amount:{} skipped_zero_budget:{} skipped_missing_scid:{} skipped_missing_our:{}",
        suggested,
        skipped_balance,
        skipped_no_recent_outbound,
        skipped_small_amount,
        skipped_zero_budget,
        skipped_missing_scid,
        skipped_missing_our
    );
}

#[cfg(test)]
mod tests {
    use super::compute_max_ppm;

    #[test]
    fn compute_max_ppm_is_zero_without_recent_avg_ppm() {
        assert_eq!(compute_max_ppm(0), 0);
    }

    #[test]
    fn compute_max_ppm_uses_half_of_average_fee_ppm() {
        assert_eq!(compute_max_ppm(446), 223);
        assert_eq!(compute_max_ppm(2085), 1042);
        assert_eq!(compute_max_ppm(90), 45);
    }
}
