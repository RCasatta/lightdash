use crate::cmd;
use crate::store::Store;
use std::collections::HashMap;
use std::fs;
use std::io::Write;

type NodeId = String;
type ChannelId = String;
type Channels = HashMap<ChannelId, HashMap<NodeId, Vec<ElementData>>>;

struct ElementData {
    timestamp: u32,
    base_fee_millisatoshi: u32,
    fee_per_millionth: u32,
    htlc_minimum: u32,
    htlc_maximum: u32,
    active: bool,
}

pub fn run_channels(_store: &Store, dir: &str, output_dir: &str) {
    log::info!("Running channels command for directory: {}", dir);
    log::info!("Output directory: {}", output_dir);

    // Create output directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(output_dir) {
        log::error!("Failed to create output directory {}: {}", output_dir, e);
        return;
    }

    // Read all files in the given directory
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            log::error!("Failed to read directory {}: {}", dir, e);
            return;
        }
    };

    let mut channels: Channels = HashMap::new();

    // Filter for files ending with .json.xz and process them
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                log::warn!("Error reading directory entry: {}", e);
                continue;
            }
        };

        let path = entry.path();
        let filename = path.to_string_lossy();
        log::info!("Processing path: {}", filename);
        if filename.ends_with(".json.xz") {
            log::info!("Processing channel file: {}", filename);
            let only_name = path.file_name().and_then(|n| n.to_str()).unwrap();
            log::info!("Only name: {}", only_name);
            let timestamp = only_name.split(".").next().unwrap().parse::<u64>().unwrap();
            log::info!("Timestamp: {}", timestamp);

            // Read the channel data from the compressed file
            let list_channels = cmd::read_xz_channels(&filename);
            let _channel_count = list_channels.channels.len();

            for channel in list_channels.channels {
                let el = channels
                    .entry(channel.short_channel_id)
                    .or_insert(HashMap::new());
                let vec = el.entry(channel.source).or_insert(Vec::new());
                vec.push(ElementData {
                    timestamp: timestamp as u32,
                    base_fee_millisatoshi: channel.base_fee_millisatoshi as u32,
                    fee_per_millionth: channel.fee_per_millionth as u32,
                    htlc_minimum: (channel.htlc_minimum_msat / 1000) as u32,
                    htlc_maximum: (channel.htlc_maximum_msat / 1000) as u32,
                    active: channel.active,
                });
            }
        }
    }

    // Write CSV files for each channel
    for (channel_id, nodes) in channels.iter() {
        if nodes.len() != 2 {
            log::warn!(
                "Channel {} has {} nodes, expected 2. Skipping CSV generation.",
                channel_id,
                nodes.len()
            );
            continue;
        }

        // Get the two node IDs and sort them
        let mut node_ids: Vec<&NodeId> = nodes.keys().collect();
        node_ids.sort();
        let node_0 = node_ids[0];
        let node_1 = node_ids[1];

        // Collect all timestamps and create a map of timestamp -> (node_0_data, node_1_data)
        let mut timestamp_data: HashMap<u32, (Option<&ElementData>, Option<&ElementData>)> =
            HashMap::new();

        // Add data from node_0
        if let Some(elements) = nodes.get(node_0) {
            for element in elements {
                timestamp_data
                    .entry(element.timestamp)
                    .or_insert((None, None))
                    .0 = Some(element);
            }
        }

        // Add data from node_1
        if let Some(elements) = nodes.get(node_1) {
            for element in elements {
                timestamp_data
                    .entry(element.timestamp)
                    .or_insert((None, None))
                    .1 = Some(element);
            }
        }

        // Create filename with output directory
        let filename = format!("{}/{}_{}_{}.csv", output_dir, channel_id, node_0, node_1);
        log::info!("Writing CSV file: {}", filename);

        // Write CSV file
        match fs::File::create(&filename) {
            Ok(mut file) => {
                // Write header
                if let Err(e) = writeln!(
                    file,
                    "timestamp,base_fee_0,base_fee_1,fee_per_millionth_0,fee_per_millionth_1,htlc_max_0,htlc_max_1"
                ) {
                    log::error!("Failed to write header to {}: {}", filename, e);
                    continue;
                }

                // Sort timestamps and write data rows
                let mut timestamps: Vec<u32> = timestamp_data.keys().cloned().collect();
                timestamps.sort();
                let row_count = timestamps.len();

                for timestamp in timestamps {
                    if let Some((data_0, data_1)) = timestamp_data.get(&timestamp) {
                        let base_fee_msat_0 = data_0
                            .map(|d| d.base_fee_millisatoshi.to_string())
                            .unwrap_or_else(|| "".to_string());
                        let base_fee_msat_1 = data_1
                            .map(|d| d.base_fee_millisatoshi.to_string())
                            .unwrap_or_else(|| "".to_string());
                        let fee_per_millionth_0 = data_0
                            .map(|d| d.fee_per_millionth.to_string())
                            .unwrap_or_else(|| "".to_string());
                        let fee_per_millionth_1 = data_1
                            .map(|d| d.fee_per_millionth.to_string())
                            .unwrap_or_else(|| "".to_string());
                        let htlc_max_0 = data_0
                            .map(|d| d.htlc_maximum.to_string())
                            .unwrap_or_else(|| "".to_string());
                        let htlc_max_1 = data_1
                            .map(|d| d.htlc_maximum.to_string())
                            .unwrap_or_else(|| "".to_string());

                        if let Err(e) = writeln!(
                            file,
                            "{},{},{},{},{},{},{}",
                            timestamp,
                            base_fee_msat_0,
                            base_fee_msat_1,
                            fee_per_millionth_0,
                            fee_per_millionth_1,
                            htlc_max_0,
                            htlc_max_1
                        ) {
                            log::error!("Failed to write data row to {}: {}", filename, e);
                            break;
                        }
                    }
                }

                log::info!("Successfully wrote {} rows to {}", row_count, filename);
            }
            Err(e) => {
                log::error!("Failed to create file {}: {}", filename, e);
            }
        }
    }

    log::info!("Channels command completed");
}
