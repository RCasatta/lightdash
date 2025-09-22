use std::collections::HashMap;

use crate::cmd::*;
use crate::store::Store;

pub fn run_routes(store: &Store) {
    let chan_meta = store.chan_meta_per_node();
    let peers_ids = store.peers_ids();
    let nodes_by_id_keys = store.node_ids_with_aliases();

    let mut counters = HashMap::new();
    let mut hop_sum = 0usize;
    let mut total = 0;

    for id in &nodes_by_id_keys {
        // Skip nodes that have less than 2 channels
        if chan_meta
            .get(id)
            .map_or(true, |chan_info| chan_info.count < 2)
        {
            continue;
        }
        if let Some(route) = get_route(id) {
            let mut nodes = route.route;
            hop_sum += nodes.len();
            total += 1;
            nodes.pop(); // remove the random destination
            for n in nodes.iter() {
                if !peers_ids.contains(&n.id) {
                    *counters.entry(n.id.to_string()).or_insert(0u64) += 1;
                }
            }
        }
    }
    let mut counters_vec: Vec<_> = counters.into_iter().filter(|e| e.1 > 2).collect();
    counters_vec.sort_by(|a, b| b.1.cmp(&a.1));

    let average_hops = hop_sum as f64 / total as f64;
    log::debug!("\nNode most present in random routes (average hops:{average_hops:.2}):");
    for c in counters_vec {
        let id = &c.0;
        let count = c.1;
        let alias = store.get_node_alias(id);
        let chan_info = chan_meta.get(&c.0).unwrap();
        let avg_fee = chan_info.avg_fee();
        let fee_diversity = format!("{:.3}", chan_info.fee_diversity());
        let num_chans = chan_info.count;
        log::debug!("{id} {count:>5} avg:{avg_fee:>6.1} dvr:{fee_diversity:>6} chans:{num_chans:>4} {alias}");
    }
}
