use chrono::Utc;
use std::collections::HashMap;

use crate::cmd::*;
use crate::common::*;

pub fn run_fees() {
    let now = Utc::now();
    println!("{}", now);
    let info = get_info();

    let channels = list_channels();
    let nodes = list_nodes();

    let nodes_by_id: HashMap<_, _> = nodes
        .nodes
        .iter()
        .filter(|e| e.alias.is_some())
        .map(|e| (&e.nodeid, e))
        .collect();

    let funds = list_funds();
    let normal_channels: Vec<_> = funds
        .channels
        .into_iter()
        .filter(|c| c.state == "CHANNELD_NORMAL")
        .collect();

    let forwards = list_forwards();
    let settled: Vec<_> = forwards
        .forwards
        .into_iter()
        .filter(|e| e.status == "settled")
        .map(|e| SettledForward::try_from(e).unwrap())
        .collect();
    let settled_24h = filter_forwards(&settled, 24, &now);

    let channels_by_id: HashMap<_, _> = channels
        .channels
        .iter()
        .map(|e| ((&e.short_channel_id, &e.source), e))
        .collect();

    let mut lines = vec![];

    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();
        let our = channels_by_id.get(&(&short_channel_id, &info.id));
        let alias_or_id = fund.alias_or_id(&nodes_by_id);

        let (_new_fee, cmd) =
            calc_setchannel(&short_channel_id, &alias_or_id, &fund, our, &settled_24h);

        lines.push(cmd);
    }

    for cmd in lines {
        if let Some(c) = cmd {
            println!("{c}");
        }
    }
}
