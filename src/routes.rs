use std::collections::{HashMap, HashSet};

use crate::cmd::*;
use crate::common::*;

pub fn run_routes() {
    let nodes = list_nodes();
    let peers = list_peers();

    let _peers_ids: HashSet<_> = peers
        .peers
        .iter()
        .filter(|e| e.num_channels > 0)
        .map(|e| &e.id)
        .collect();

    let nodes_by_id: HashMap<_, _> = nodes
        .nodes
        .iter()
        .filter(|e| e.alias.is_some())
        .map(|e| (&e.nodeid, e))
        .collect();

    let channels = list_channels();
    let mut chan_meta_per_node = HashMap::new();

    for c in channels.channels.iter() {
        let meta: &mut ChannelFee = chan_meta_per_node.entry(&c.source).or_default();
        meta.count += 1;
        meta.fee_sum += c.fee_per_millionth;
        meta.fee_rates.insert(c.fee_per_millionth);
    }

    calc_routes(nodes_by_id, _peers_ids, &chan_meta_per_node);
}

pub fn calc_routes(
    nodes_by_id: HashMap<&String, &Node>,
    peers_ids: HashSet<&String>,
    chan_meta: &HashMap<&String, ChannelFee>,
) {
    let mut counters = HashMap::new();
    let mut hop_sum = 0usize;
    let mut total = 0;
    for id in nodes_by_id.keys() {
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
    println!("\nNode most present in random routes (average hops:{average_hops:.2}):");
    for c in counters_vec {
        let id = &c.0;
        let count = c.1;
        let alias = nodes_by_id
            .get(id)
            .map(|n| n.alias.clone())
            .flatten()
            .unwrap_or("".to_string());
        let chan_info = chan_meta.get(&c.0).unwrap();
        let avg_fee = chan_info.avg_fee();
        let fee_diversity = format!("{:.3}", chan_info.fee_diversity());
        let num_chans = chan_info.count;
        println!("{id} {count:>5} avg:{avg_fee:>6.1} dvr:{fee_diversity:>6} chans:{num_chans:>4} {alias}");
    }
}
