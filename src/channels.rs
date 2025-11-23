use crate::cmd;
use crate::store::Store;
use std::fs;

pub fn run_channels(_store: &Store, dir: &str) {
    log::info!("Running channels command for directory: {}", dir);

    // Read all files in the given directory
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            log::error!("Failed to read directory {}: {}", dir, e);
            return;
        }
    };

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
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            if filename.ends_with(".json.xz") {
                let filepath = path.to_string_lossy();
                log::info!("Processing channel file: {}", filename);

                // Read the channel data from the compressed file
                let list_channels = cmd::read_xz_channels(&filepath);
                let channel_count = list_channels.channels.len();

                log::info!("Found {} channels in {}", channel_count, filename);
            }
        }
    }
    log::info!("Channels command completed");
}
