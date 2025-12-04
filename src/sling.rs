use crate::store::Store;

const SOURCE_PPM_MAX: u64 = 300;
const MAX_BALANCE: f64 = 0.1;
const AMOUNT: u64 = 100000;
const CMD: &str = "lightning-cli";

pub fn run_sling(store: &Store) {
    let channels = store.normal_channels();
    for channel in channels {
        if let Some(scid) = &channel.short_channel_id {
            if channel.perc_float() < MAX_BALANCE {
                let our = store.get_channel(&scid, &store.info.id);
                if let Some(our) = our {
                    let forwards = store.get_channel_forwards(scid).len() as u64;
                    let alias = store.get_node_alias(&channel.peer_id);

                    // established channels have a good ppm estimation and we can risk more.
                    // New one on the contrary will have a bigger factor thus a lower maxppm to use.
                    // The 3 means I want to pay 33% of the ppm I am rebalancing, just to be conservative.
                    let factor = 20u64.saturating_sub(forwards) + 3u64;

                    let my_ppm = our.fee_per_millionth;
                    let max_ppm = (my_ppm - SOURCE_PPM_MAX) / factor;
                    let args = format!("sling-once -k scid={scid} direction=pull outppm={SOURCE_PPM_MAX} maxppm={max_ppm} amount={AMOUNT} onceamount={AMOUNT}");
                    log::info!(
                        "{alias} factor:{factor} channel_ppm:{my_ppm} maxppm:{max_ppm} -> {args} "
                    );
                    if std::env::var("EXECUTE_SLING").is_ok() {
                        log::info!("executing `{CMD} {args}` {alias}");

                        let splitted_args: Vec<&str> = args.split(' ').collect();
                        let result = crate::cmd::cmd_result(&args, &splitted_args);
                        log::debug!("cmd return: {result}");
                    }
                }
            }
        }
    }
}
