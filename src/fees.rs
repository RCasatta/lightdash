use crate::common::*;
use crate::store::Store;

pub fn run_fees(store: &Store) {
    let normal_channels = store.normal_channels();
    let settled_24h = store.filter_settled_forwards_by_hours(24);

    let mut lines = vec![];

    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();
        let our = store.get_channel(&short_channel_id, &store.info.id);
        let alias_or_id = store.get_node_alias(&fund.peer_id);

        let (_new_fee, cmd) = calc_setchannel(
            &short_channel_id,
            &alias_or_id,
            &fund,
            our.as_ref(),
            &settled_24h,
        );

        lines.push(cmd);
    }

    for cmd in lines {
        if let Some(c) = cmd {
            log::debug!("{c}");
        }
    }
}
