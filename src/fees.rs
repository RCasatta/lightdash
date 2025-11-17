use crate::common::*;
use crate::store::Store;

pub fn run_fees(store: &Store) {
    let normal_channels = store.normal_channels();
    let forwards_24h = store.filter_forwards_by_hours(24);

    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();
        let our = match store.get_channel(&short_channel_id, &store.info.id) {
            Some(c) => c,
            None => continue,
        };
        let alias_or_id = store.get_node_alias(&fund.peer_id);

        calc_setchannel(&short_channel_id, &alias_or_id, &fund, our, &forwards_24h);
    }
}
