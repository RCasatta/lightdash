use crate::cmd;
use flate2::write::GzEncoder;
use flate2::Compression;
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

pub fn run_channels(dir: &str, output_dir: &str) {
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

        // Generate SVG chart
        let svg_filename = format!("{}/{}.svgz", output_dir, channel_id);
        log::info!("Writing SVGZ file: {}", svg_filename);

        match generate_svg_chart(&timestamp_data, node_0, node_1) {
            Ok(svg_content) => {
                // Compress the SVG content with gzip
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                match encoder.write_all(svg_content.as_bytes()) {
                    Ok(_) => match encoder.finish() {
                        Ok(compressed) => match fs::write(&svg_filename, compressed) {
                            Ok(_) => log::info!("Successfully wrote SVGZ to {}", svg_filename),
                            Err(e) => {
                                log::error!("Failed to write SVGZ file {}: {}", svg_filename, e)
                            }
                        },
                        Err(e) => log::error!("Failed to finish gzip compression: {}", e),
                    },
                    Err(e) => log::error!("Failed to compress SVG data: {}", e),
                }
            }
            Err(e) => log::error!("Failed to generate SVG chart: {}", e),
        }
    }

    log::info!("Channels command completed");
}

fn generate_svg_chart(
    timestamp_data: &HashMap<u32, (Option<&ElementData>, Option<&ElementData>)>,
    node_0: &str,
    node_1: &str,
) -> Result<String, String> {
    // Collect and sort timestamps
    let mut timestamps: Vec<u32> = timestamp_data.keys().cloned().collect();
    if timestamps.is_empty() {
        return Err("No data to plot".to_string());
    }
    timestamps.sort();

    // Find min and max values for scaling
    let mut min_fee = u32::MAX;
    let mut max_fee = 0u32;

    for (data_0, data_1) in timestamp_data.values() {
        if let Some(d) = data_0 {
            min_fee = min_fee.min(d.fee_per_millionth);
            max_fee = max_fee.max(d.fee_per_millionth);
        }
        if let Some(d) = data_1 {
            min_fee = min_fee.min(d.fee_per_millionth);
            max_fee = max_fee.max(d.fee_per_millionth);
        }
    }

    // Add some padding to the y-axis
    let y_padding = (max_fee - min_fee).max(1) / 10;
    min_fee = min_fee.saturating_sub(y_padding);
    max_fee = max_fee + y_padding;

    // SVG dimensions
    let width = 1200;
    let height = 600;
    let margin_left = 80;
    let margin_right = 250;
    let margin_top = 40;
    let margin_bottom = 80;
    let plot_width = width - margin_left - margin_right;
    let plot_height = height - margin_top - margin_bottom;

    let min_timestamp = *timestamps.first().unwrap();
    let max_timestamp = *timestamps.last().unwrap();
    let time_range = (max_timestamp - min_timestamp).max(1);

    // Helper function to scale x coordinate
    let scale_x = |timestamp: u32| -> i32 {
        margin_left
            + ((timestamp - min_timestamp) as f64 / time_range as f64 * plot_width as f64) as i32
    };

    // Helper function to scale y coordinate
    let scale_y = |value: u32| -> i32 {
        if max_fee == min_fee {
            return margin_top + plot_height / 2;
        }
        margin_top + plot_height
            - ((value - min_fee) as f64 / (max_fee - min_fee) as f64 * plot_height as f64) as i32
    };

    // Build SVG
    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {} {}" width="{}" height="{}">"##,
        width, height, width, height
    ));
    svg.push_str("\n");

    // Add title
    svg.push_str(&format!(
        r##"  <text x="{}" y="25" font-family="Arial, sans-serif" font-size="18" font-weight="bold" text-anchor="middle">Fee Per Millionth Over Time</text>"##,
        width / 2
    ));
    svg.push_str("\n");

    // Draw axes
    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="black" stroke-width="2"/>"##,
        margin_left,
        margin_top + plot_height,
        margin_left + plot_width,
        margin_top + plot_height
    ));
    svg.push_str("\n");
    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="black" stroke-width="2"/>"##,
        margin_left,
        margin_top,
        margin_left,
        margin_top + plot_height
    ));
    svg.push_str("\n");

    // Add axis labels
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" text-anchor="middle">Date</text>"##,
        margin_left + plot_width / 2, height - 20
    ));
    svg.push_str("\n");
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" transform="rotate(-90, {}, {})">Fee Per Millionth</text>"##,
        20, margin_top + plot_height / 2, 20, margin_top + plot_height / 2
    ));
    svg.push_str("\n");

    // Add y-axis ticks and grid
    let num_y_ticks = 5;
    for i in 0..=num_y_ticks {
        let value = min_fee + (max_fee - min_fee) * i / num_y_ticks;
        let y = scale_y(value);

        // Grid line
        svg.push_str(&format!(
            r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#e0e0e0" stroke-width="1"/>"##,
            margin_left,
            y,
            margin_left + plot_width,
            y
        ));
        svg.push_str("\n");

        // Tick label
        svg.push_str(&format!(
            r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="12" text-anchor="end" alignment-baseline="middle">{}</text>"##,
            margin_left - 10, y, value
        ));
        svg.push_str("\n");
    }

    // Add x-axis ticks with dates
    let num_x_ticks = 6;
    for i in 0..=num_x_ticks {
        let timestamp = min_timestamp + (max_timestamp - min_timestamp) * i / num_x_ticks;
        let x = scale_x(timestamp);

        // Format date (Unix timestamp to date)
        let date = format_timestamp(timestamp);

        svg.push_str(&format!(
            r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="11" text-anchor="end" transform="rotate(-45, {}, {})">{}  </text>"##,
            x, margin_top + plot_height + 15, x, margin_top + plot_height + 15, date
        ));
        svg.push_str("\n");
    }

    // Plot data for node_0
    let mut points_0: Vec<(i32, i32)> = Vec::new();
    for timestamp in &timestamps {
        if let Some((Some(data), _)) = timestamp_data.get(timestamp) {
            let x = scale_x(*timestamp);
            let y = scale_y(data.fee_per_millionth);
            points_0.push((x, y));
        }
    }

    if !points_0.is_empty() {
        let path_data: String = points_0
            .iter()
            .enumerate()
            .map(|(i, (x, y))| {
                if i == 0 {
                    format!("M {} {}", x, y)
                } else {
                    format!("L {} {}", x, y)
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        svg.push_str(&format!(
            r##"  <path d="{}" fill="none" stroke="#2563eb" stroke-width="2"/>"##,
            path_data
        ));
        svg.push_str("\n");

        // Add circles for data points
        for (x, y) in points_0 {
            svg.push_str(&format!(
                r##"  <circle cx="{}" cy="{}" r="3" fill="#2563eb"/>"##,
                x, y
            ));
            svg.push_str("\n");
        }
    }

    // Plot data for node_1
    let mut points_1: Vec<(i32, i32)> = Vec::new();
    for timestamp in &timestamps {
        if let Some((_, Some(data))) = timestamp_data.get(timestamp) {
            let x = scale_x(*timestamp);
            let y = scale_y(data.fee_per_millionth);
            points_1.push((x, y));
        }
    }

    if !points_1.is_empty() {
        let path_data: String = points_1
            .iter()
            .enumerate()
            .map(|(i, (x, y))| {
                if i == 0 {
                    format!("M {} {}", x, y)
                } else {
                    format!("L {} {}", x, y)
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        svg.push_str(&format!(
            r##"  <path d="{}" fill="none" stroke="#dc2626" stroke-width="2"/>"##,
            path_data
        ));
        svg.push_str("\n");

        // Add circles for data points
        for (x, y) in points_1 {
            svg.push_str(&format!(
                r##"  <circle cx="{}" cy="{}" r="3" fill="#dc2626"/>"##,
                x, y
            ));
            svg.push_str("\n");
        }
    }

    // Add legend
    let legend_x = width - margin_right + 20;
    let legend_y = margin_top + 20;

    svg.push_str(&format!(
        r##"  <rect x="{}" y="{}" width="200" height="80" fill="white" stroke="black" stroke-width="1"/>"##,
        legend_x - 10, legend_y - 10
    ));
    svg.push_str("\n");

    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" font-weight="bold">Legend</text>"##,
        legend_x, legend_y + 5
    ));
    svg.push_str("\n");

    // Node 0 legend
    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#2563eb" stroke-width="3"/>"##,
        legend_x,
        legend_y + 25,
        legend_x + 30,
        legend_y + 25
    ));
    svg.push_str("\n");
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, monospace" font-size="10" alignment-baseline="middle">{}</text>"##,
        legend_x + 40, legend_y + 25, truncate_node_id(node_0)
    ));
    svg.push_str("\n");

    // Node 1 legend
    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#dc2626" stroke-width="3"/>"##,
        legend_x,
        legend_y + 50,
        legend_x + 30,
        legend_y + 50
    ));
    svg.push_str("\n");
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, monospace" font-size="10" alignment-baseline="middle">{}</text>"##,
        legend_x + 40, legend_y + 50, truncate_node_id(node_1)
    ));
    svg.push_str("\n");

    svg.push_str("</svg>\n");

    Ok(svg)
}

fn format_timestamp(timestamp: u32) -> String {
    // Convert Unix timestamp to a readable date format
    // Simple approximation: days since epoch
    const SECONDS_PER_DAY: u32 = 86400;
    let days = timestamp / SECONDS_PER_DAY;

    // Unix epoch: 1970-01-01
    let epoch_year = 1970;
    let mut year = epoch_year;
    let mut remaining_days = days;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    // Find month and day
    let days_in_months = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    let mut day = remaining_days + 1;

    for days_in_month in &days_in_months {
        if day <= *days_in_month {
            break;
        }
        day -= *days_in_month;
        month += 1;
    }

    format!("{:04}-{:02}-{:02}", year, month, day)
}

fn is_leap_year(year: u32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn truncate_node_id(node_id: &str) -> String {
    if node_id.len() > 16 {
        format!("{}...{}", &node_id[..8], &node_id[node_id.len() - 8..])
    } else {
        node_id.to_string()
    }
}
