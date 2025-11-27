use crate::cmd;
use flate2::write::GzEncoder;
use flate2::Compression;
use maud::{html, Markup, DOCTYPE};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;

type NodeId = String;
type ChannelId = String;
type Channels = HashMap<ChannelId, HashMap<NodeId, Vec<ElementData>>>;

struct ElementData {
    timestamp: u32,
    // base_fee_millisatoshi: u32,
    fee_per_millionth: u32,
    // htlc_minimum: u32,
    htlc_maximum: u32,
    // active: bool,
}

enum ChartType {
    Fee,
    HtlcMax,
}

pub fn run_channels(dir: &str, output_dir: &str) {
    log::info!("Running channels command for directory: {}", dir);
    log::info!("Output directory: {}", output_dir);

    // Create output directories for fees, htlc-max charts, and channel pages
    let fees_dir = format!("{}/fees", output_dir);
    let htlc_max_dir = format!("{}/htlc-max", output_dir);
    let channels_dir = format!("{}/channels", output_dir);

    if let Err(e) = fs::create_dir_all(&fees_dir) {
        log::error!("Failed to create fees directory {}: {}", fees_dir, e);
        return;
    }

    if let Err(e) = fs::create_dir_all(&htlc_max_dir) {
        log::error!(
            "Failed to create htlc-max directory {}: {}",
            htlc_max_dir,
            e
        );
        return;
    }

    if let Err(e) = fs::create_dir_all(&channels_dir) {
        log::error!(
            "Failed to create channels directory {}: {}",
            channels_dir,
            e
        );
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
        if filename.ends_with(".json.xz") {
            log::info!("Processing channel file: {}", filename);
            let only_name = path.file_name().and_then(|n| n.to_str()).unwrap();
            let timestamp = only_name.split(".").next().unwrap().parse::<u64>().unwrap();

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
                    // base_fee_millisatoshi: channel.base_fee_millisatoshi as u32,
                    fee_per_millionth: channel.fee_per_millionth as u32,
                    // htlc_minimum: (channel.htlc_minimum_msat / 1000) as u32,
                    htlc_maximum: (channel.htlc_maximum_msat / 1000) as u32,
                    // active: channel.active,
                });
            }
        }
    }

    let mut monodirectional = 0;

    // Write CSV files for each channel
    for (i, (channel_id, nodes)) in channels.iter().enumerate() {
        if i % 1000 == 0 {
            log::info!("Processing channel {}/{}", i, channels.len());
        }
        if nodes.len() != 2 {
            monodirectional += 1;
            continue;
        }

        // Get the two node IDs and sort them
        let mut node_ids: Vec<&NodeId> = nodes.keys().collect();
        node_ids.sort();
        let node_0 = node_ids[0];
        let node_1 = node_ids[1];

        // Collect all timestamps and create a map of timestamp -> (node_0_data, node_1_data)
        let mut timestamp_data: BTreeMap<u32, (Option<&ElementData>, Option<&ElementData>)> =
            BTreeMap::new();

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

        // // Create filename with output directory
        // let filename = format!("{}/{}_{}_{}.csv", output_dir, channel_id, node_0, node_1);
        // log::info!("Writing CSV file: {}", filename);

        // // Write CSV file
        // match fs::File::create(&filename) {
        //     Ok(mut file) => {
        //         // Write header
        //         if let Err(e) = writeln!(
        //             file,
        //             "timestamp,base_fee_0,base_fee_1,fee_per_millionth_0,fee_per_millionth_1,htlc_max_0,htlc_max_1"
        //         ) {
        //             log::error!("Failed to write header to {}: {}", filename, e);
        //             continue;
        //         }

        //         // Sort timestamps and write data rows
        //         let mut timestamps: Vec<u32> = timestamp_data.keys().cloned().collect();
        //         timestamps.sort();
        //         let row_count = timestamps.len();

        //         for timestamp in timestamps {
        //             if let Some((data_0, data_1)) = timestamp_data.get(&timestamp) {
        //                 let base_fee_msat_0 = data_0
        //                     .map(|d| d.base_fee_millisatoshi.to_string())
        //                     .unwrap_or_else(|| "".to_string());
        //                 let base_fee_msat_1 = data_1
        //                     .map(|d| d.base_fee_millisatoshi.to_string())
        //                     .unwrap_or_else(|| "".to_string());
        //                 let fee_per_millionth_0 = data_0
        //                     .map(|d| d.fee_per_millionth.to_string())
        //                     .unwrap_or_else(|| "".to_string());
        //                 let fee_per_millionth_1 = data_1
        //                     .map(|d| d.fee_per_millionth.to_string())
        //                     .unwrap_or_else(|| "".to_string());
        //                 let htlc_max_0 = data_0
        //                     .map(|d| d.htlc_maximum.to_string())
        //                     .unwrap_or_else(|| "".to_string());
        //                 let htlc_max_1 = data_1
        //                     .map(|d| d.htlc_maximum.to_string())
        //                     .unwrap_or_else(|| "".to_string());

        //                 if let Err(e) = writeln!(
        //                     file,
        //                     "{},{},{},{},{},{},{}",
        //                     timestamp,
        //                     base_fee_msat_0,
        //                     base_fee_msat_1,
        //                     fee_per_millionth_0,
        //                     fee_per_millionth_1,
        //                     htlc_max_0,
        //                     htlc_max_1
        //                 ) {
        //                     log::error!("Failed to write data row to {}: {}", filename, e);
        //                     break;
        //                 }
        //             }
        //         }

        //         log::info!("Successfully wrote {} rows to {}", row_count, filename);
        //     }
        //     Err(e) => {
        //         log::error!("Failed to create file {}: {}", filename, e);
        //     }
        // }

        // Generate Fee SVG chart
        let fee_svg_filename = format!("{}/fees/{}.svgz", output_dir, channel_id);
        log::debug!("Writing Fee SVGZ file: {}", fee_svg_filename);

        match generate_svg_chart(&timestamp_data, node_0, node_1, ChartType::Fee) {
            Ok(svg_content) => {
                write_compressed_svg(&fee_svg_filename, &svg_content);
            }
            Err(e) => log::error!("Failed to generate Fee SVG chart: {}", e),
        }

        // Generate HTLC Max SVG chart
        let htlc_max_svg_filename = format!("{}/htlc-max/{}.svgz", output_dir, channel_id);
        log::debug!("Writing HTLC Max SVGZ file: {}", htlc_max_svg_filename);

        match generate_svg_chart(&timestamp_data, node_0, node_1, ChartType::HtlcMax) {
            Ok(svg_content) => {
                write_compressed_svg(&htlc_max_svg_filename, &svg_content);
            }
            Err(e) => log::error!("Failed to generate HTLC Max SVG chart: {}", e),
        }

        // Generate channel HTML page
        let channel_html = create_channel_page(channel_id, node_0, node_1);
        let channel_html_filename = format!("{}/channels/{}.html", output_dir, channel_id);
        match fs::write(&channel_html_filename, channel_html.into_string()) {
            Ok(_) => log::debug!("Channel page generated: {}", channel_html_filename),
            Err(e) => log::error!(
                "Failed to write channel page {}: {}",
                channel_html_filename,
                e
            ),
        }
    }
    log::info!("Monodirectional channels: {}", monodirectional);

    log::info!("Channels command completed");
}

fn write_compressed_svg(filename: &str, svg_content: &str) {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    match encoder.write_all(svg_content.as_bytes()) {
        Ok(_) => match encoder.finish() {
            Ok(compressed) => match fs::write(filename, compressed) {
                Ok(_) => log::debug!("Successfully wrote SVGZ to {}", filename),
                Err(e) => log::error!("Failed to write SVGZ file {}: {}", filename, e),
            },
            Err(e) => log::error!("Failed to finish gzip compression: {}", e),
        },
        Err(e) => log::error!("Failed to compress SVG data: {}", e),
    }
}

fn create_channel_page(channel_id: &str, node_0: &str, node_1: &str) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                title { "Channel " (channel_id) }
                meta charset="utf-8";
                link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>";
                style {
                    r#"
                    body {
                        font-family: 'Courier New', monospace;
                        background-color: #1e1e1e;
                        color: #f8f8f2;
                        margin: 0;
                        padding: 20px;
                        line-height: 1.4;
                    }
                    .container {
                        max-width: 1400px;
                        margin: 0 auto;
                    }
                    .header {
                        background-color: #2c3e50;
                        color: white;
                        padding: 20px;
                        border-radius: 8px;
                        margin-bottom: 20px;
                        text-align: center;
                    }
                    .info-card {
                        background-color: #2d3748;
                        padding: 20px;
                        border-radius: 8px;
                        margin-bottom: 20px;
                    }
                    .info-item {
                        margin: 10px 0;
                    }
                    .label {
                        color: #a0aec0;
                    }
                    .value {
                        color: #63b3ed;
                        font-weight: bold;
                    }
                    a {
                        color: #63b3ed;
                        text-decoration: none;
                    }
                    a:hover {
                        text-decoration: underline;
                    }
                    h1 { color: #f8f8f2; margin: 0; }
                    h2 { color: #63b3ed; margin-top: 0; }
                    .chart-container {
                        margin: 20px 0;
                    }
                    .timestamp {
                        color: #a0aec0;
                        font-size: 0.9em;
                        text-align: center;
                        margin-top: 20px;
                    }
                    "#
                }
            }
            body {
                div class="container" {
                    div class="header" {
                        h1 { "⚡ Channel " (channel_id) }
                    }

                    div class="info-card" {
                        h2 { "Channel Information" }
                        div class="info-item" {
                            span class="label" { "Short Channel ID: " }
                            span class="value" { (channel_id) }
                        }
                        div class="info-item" {
                            span class="label" { "Node 1: " }
                            span class="value" {
                                a href={(format!("https://mempool.space/lightning/node/{node_0}"))} target="_blank" {
                                    (truncate_node_id(node_0))
                                }
                            }
                        }
                        div class="info-item" {
                            span class="label" { "Node 2: " }
                            span class="value" {
                                a href={(format!("https://mempool.space/lightning/node/{node_1}"))} target="_blank" {
                                    (truncate_node_id(node_1))
                                }
                            }
                        }
                    }

                    div class="info-card" {
                        h2 { "Channel Fee History" }
                        div class="chart-container" {
                            object data={(format!("../fees/{channel_id}.svgz"))} type="image/svg+xml" style="width: 100%; background-color:rgb(235, 230, 230); margin:10px" {
                                p { "Fee chart not available for this channel." }
                            }
                        }
                    }

                    div class="info-card" {
                        h2 { "Channel HTLC Max History" }
                        div class="chart-container" {
                            object data={(format!("../htlc-max/{channel_id}.svgz"))} type="image/svg+xml" style="width: 100%; background-color:rgb(235, 230, 230); margin:10px" {
                                p { "HTLC max chart not available for this channel." }
                            }
                        }
                    }

                    div class="timestamp" {
                        "Generated by lightdash channels command"
                    }
                }
            }
        }
    }
}

fn generate_svg_chart(
    timestamp_data: &BTreeMap<u32, (Option<&ElementData>, Option<&ElementData>)>,
    node_0: &str,
    node_1: &str,
    chart_type: ChartType,
) -> Result<String, String> {
    // Collect timestamps (already sorted in BTreeMap)
    let timestamps: Vec<u32> = timestamp_data.keys().cloned().collect();
    if timestamps.is_empty() {
        return Err("No data to plot".to_string());
    }

    // Find min and max values for scaling based on chart type
    let mut min_value = u32::MAX;
    let mut max_value = 0u32;

    for (data_0, data_1) in timestamp_data.values() {
        let (val_0, val_1) = match chart_type {
            ChartType::Fee => (
                data_0.map(|d| d.fee_per_millionth),
                data_1.map(|d| d.fee_per_millionth),
            ),
            ChartType::HtlcMax => (
                data_0.map(|d| d.htlc_maximum),
                data_1.map(|d| d.htlc_maximum),
            ),
        };

        if let Some(v) = val_0 {
            min_value = min_value.min(v);
            max_value = max_value.max(v);
        }
        if let Some(v) = val_1 {
            min_value = min_value.min(v);
            max_value = max_value.max(v);
        }
    }

    // Add some padding to the y-axis
    let y_padding = (max_value - min_value).max(1) / 10;
    min_value = min_value.saturating_sub(y_padding);
    max_value = max_value + y_padding;

    // Cap the y-axis for Fee charts to avoid outliers squashing the chart
    const FEE_Y_AXIS_MAX: u32 = 4000;
    if matches!(chart_type, ChartType::Fee) && max_value > FEE_Y_AXIS_MAX {
        max_value = FEE_Y_AXIS_MAX;
    }

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
        if max_value == min_value {
            return margin_top + plot_height / 2;
        }
        margin_top + plot_height
            - ((value - min_value) as f64 / (max_value - min_value) as f64 * plot_height as f64)
                as i32
    };

    // Build SVG
    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {} {}" width="{}" height="{}">"##,
        width, height, width, height
    ));
    svg.push_str("\n");

    // Add background
    svg.push_str(&format!(
        r##"  <rect width="{}" height="{}" fill="#f5f5f5"/>"##,
        width, height
    ));
    svg.push_str("\n");

    // Add title based on chart type
    let title = match chart_type {
        ChartType::Fee => "Fee Per Millionth Over Time",
        ChartType::HtlcMax => "HTLC Maximum (sats) Over Time",
    };
    svg.push_str(&format!(
        r##"  <text x="{}" y="25" font-family="Arial, sans-serif" font-size="18" font-weight="bold" text-anchor="middle">{}</text>"##,
        width / 2, title
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

    let y_axis_label = match chart_type {
        ChartType::Fee => "Fee Per Millionth",
        ChartType::HtlcMax => "HTLC Maximum (sats)",
    };
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" transform="rotate(-90, {}, {})">{}</text>"##,
        20, margin_top + plot_height / 2, 20, margin_top + plot_height / 2, y_axis_label
    ));
    svg.push_str("\n");

    // Add y-axis ticks and grid
    let num_y_ticks = 5;
    for i in 0..=num_y_ticks {
        let value = min_value + (max_value - min_value) * i / num_y_ticks;
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
    let mut points_0: Vec<(i32, i32, u32, u32)> = Vec::new();
    for timestamp in &timestamps {
        if let Some((Some(data), _)) = timestamp_data.get(timestamp) {
            let x = scale_x(*timestamp);
            let value = match chart_type {
                ChartType::Fee => data.fee_per_millionth,
                ChartType::HtlcMax => data.htlc_maximum,
            };
            let y = scale_y(value);
            points_0.push((x, y, *timestamp, value));
        }
    }
    let points_0 = deduplicate_consecutive_points(points_0);

    if !points_0.is_empty() {
        let path_data: String = points_0
            .iter()
            .enumerate()
            .map(|(i, (x, y, _, _))| {
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
        for (x, y, timestamp, value) in points_0 {
            let date_str = format_timestamp(timestamp);
            let value_label = match chart_type {
                ChartType::Fee => format!("Fee: {}", value),
                ChartType::HtlcMax => format!("HTLC Max: {} sats", value),
            };
            svg.push_str(&format!(
                r##"  <circle cx="{}" cy="{}" r="3" fill="#2563eb"><title>{} ({})</title></circle>"##,
                x, y, value_label, date_str
            ));
            svg.push_str("\n");
        }
    }

    // Plot data for node_1
    let mut points_1: Vec<(i32, i32, u32, u32)> = Vec::new();
    for timestamp in &timestamps {
        if let Some((_, Some(data))) = timestamp_data.get(timestamp) {
            let x = scale_x(*timestamp);
            let value = match chart_type {
                ChartType::Fee => data.fee_per_millionth,
                ChartType::HtlcMax => data.htlc_maximum,
            };
            let y = scale_y(value);
            points_1.push((x, y, *timestamp, value));
        }
    }
    let points_1 = deduplicate_consecutive_points(points_1);

    if !points_1.is_empty() {
        let path_data: String = points_1
            .iter()
            .enumerate()
            .map(|(i, (x, y, _, _))| {
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
        for (x, y, timestamp, value) in points_1 {
            let date_str = format_timestamp(timestamp);
            let value_label = match chart_type {
                ChartType::Fee => format!("Fee: {}", value),
                ChartType::HtlcMax => format!("HTLC Max: {} sats", value),
            };
            svg.push_str(&format!(
                r##"  <circle cx="{}" cy="{}" r="3" fill="#dc2626"><title>{} ({})</title></circle>"##,
                x, y, value_label, date_str
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

    // Add watermark
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" font-style="italic" fill="#888888" text-anchor="end">LOUDTOLL since 2019</text>"##,
        width - 20, height - 15
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

/// Removes consecutive duplicate points, keeping the first and last of each run.
/// Points are (x, y, timestamp, value) tuples - duplicates are determined by value.
/// Example: [1,2,5,5,5,5,10,10,11] -> [1,2,5,5,10,10,11]
fn deduplicate_consecutive_points(points: Vec<(i32, i32, u32, u32)>) -> Vec<(i32, i32, u32, u32)> {
    if points.len() <= 2 {
        return points;
    }

    let mut result = Vec::with_capacity(points.len());
    let mut i = 0;

    while i < points.len() {
        let current_value = points[i].3;
        let run_start = i;

        // Find the end of the current run of equal values
        while i < points.len() && points[i].3 == current_value {
            i += 1;
        }
        let run_end = i - 1;

        // Keep first point of the run
        result.push(points[run_start]);

        // Keep last point of the run if it's different from the first
        if run_end > run_start {
            result.push(points[run_end]);
        }
    }

    result
}
