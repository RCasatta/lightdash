use crate::cmd::Fund;
use crate::store::Store;

const SOURCE_PPM_MAX: u64 = 300;
const MAX_BALANCE: f64 = 0.1;
const AMOUNT: u64 = 100000;
const CMD: &str = "lightning-cli";
/// Minimum balance percentage (our funds / total capacity) for a channel to be used as candidate.
const MIN_CANDIDATE_BALANCE: f64 = 0.7;

/// Computes candidates for sling rebalancing.
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

/// We search empty channels and try to pull sats on them from a list of candidates that are ~full and cheap.
pub fn run_sling(store: &Store) {
    let channels = store.normal_channels();

    let candidates = compute_candidates(store, &channels);
    if candidates.is_empty() {
        log::info!("No suitable candidates found (ppm < {SOURCE_PPM_MAX} and balance > {MIN_CANDIDATE_BALANCE})");
        return;
    }

    let candidates_json = candidates_to_json(&candidates);
    log::info!("Using {} candidates: {candidates_json}", candidates.len());

    for channel in channels {
        if let Some(scid) = &channel.short_channel_id {
            if channel.perc_float() > MAX_BALANCE {
                continue;
            }

            let our = store.get_channel(scid, &store.info.id);
            if let Some(our) = our {
                let forwards = store.get_channel_forwards(scid).len() as u64;
                let alias = store.get_node_alias(&channel.peer_id);

                // established channels have a good ppm estimation and we can risk more.
                // New one on the contrary will have a bigger factor thus a lower maxppm to use.
                // The 3 means I want to pay 33% of the ppm I am rebalancing, just to be conservative.
                let factor = 20u64.saturating_sub(forwards) + 3u64;

                let my_ppm = our.fee_per_millionth;
                let max_ppm = (my_ppm - SOURCE_PPM_MAX) / factor;

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
                    "{alias} factor:{factor} channel_ppm:{my_ppm} maxppm:{max_ppm} -> {CMD} {}",
                    args.join(" ")
                );
                if std::env::var("EXECUTE_SLING").is_ok() {
                    log::info!("executing `{CMD} {}` {alias}", args.join(" "));

                    let result = crate::cmd::cmd_result(CMD, &args);
                    log::debug!("cmd return: {result}");
                }
            }
        }
    }
}
