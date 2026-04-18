use crate::cmd;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::HashMap;
use std::fs;
use std::io::Write;

type ChannelKey = String;
type PeerId = String;
type ChannelHistory = HashMap<ChannelKey, ChannelSeries>;

struct ChannelSeries {
    peer_id: PeerId,
    samples: Vec<FundSnapshot>,
}

struct FundSnapshot {
    timestamp: u32,
    our_amount_sat: u64,
    remote_amount_sat: u64,
    total_amount_sat: u64,
}

#[derive(Clone, Copy)]
enum ChartType {
    Liquidity,
    Ratio,
}

pub fn run_funds(dir: &str, output_dir: &str) {
    log::info!("Running funds command for directory: {}", dir);
    log::info!("Output directory: {}", output_dir);

    let liquidity_dir = format!("{}/liquidity", output_dir);
    let ratio_dir = format!("{}/ratio", output_dir);
    if let Err(e) = fs::create_dir_all(&liquidity_dir) {
        log::error!(
            "Failed to create liquidity directory {}: {}",
            liquidity_dir,
            e
        );
        return;
    }

    if let Err(e) = fs::create_dir_all(&ratio_dir) {
        log::error!("Failed to create ratio directory {}: {}", ratio_dir, e);
        return;
    }

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            log::error!("Failed to read directory {}: {}", dir, e);
            return;
        }
    };

    let mut channels: ChannelHistory = HashMap::new();

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
        if !filename.ends_with(".json.xz") {
            continue;
        }

        let only_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => {
                log::warn!("Skipping file with invalid UTF-8 name: {:?}", path);
                continue;
            }
        };

        let timestamp = match only_name
            .split('.')
            .next()
            .and_then(|s| s.parse::<u32>().ok())
        {
            Some(timestamp) => timestamp,
            None => {
                log::warn!("Skipping file with invalid timestamp prefix: {}", filename);
                continue;
            }
        };

        log::info!("Processing funds file: {}", filename);
        let list_funds = cmd::read_xz_funds(&filename);

        for fund in list_funds.channels {
            let channel_key = channel_key(&fund);
            let our_amount_sat = fund.our_amount_msat / 1000;
            let total_amount_sat = fund.amount_msat / 1000;
            let remote_amount_sat = total_amount_sat.saturating_sub(our_amount_sat);

            let series = channels
                .entry(channel_key)
                .or_insert_with(|| ChannelSeries {
                    peer_id: fund.peer_id.clone(),
                    samples: Vec::new(),
                });

            series.samples.push(FundSnapshot {
                timestamp,
                our_amount_sat,
                remote_amount_sat,
                total_amount_sat,
            });
        }
    }

    let channel_count = channels.len();
    for (i, (channel_key, series)) in channels.iter_mut().enumerate() {
        if i % 1000 == 0 {
            log::info!("Processing funds channel {}/{}", i, channel_count);
        }

        series.samples.sort_by_key(|sample| sample.timestamp);
        series.samples.dedup_by_key(|sample| sample.timestamp);

        let liquidity_svg_filename = format!("{}/liquidity/{}.svgz", output_dir, channel_key);
        match generate_svg_chart(series, ChartType::Liquidity) {
            Ok(svg_content) => write_compressed_svg(&liquidity_svg_filename, &svg_content),
            Err(e) => log::error!(
                "Failed to generate liquidity chart for {}: {}",
                channel_key,
                e
            ),
        }

        let ratio_svg_filename = format!("{}/ratio/{}.svgz", output_dir, channel_key);
        match generate_svg_chart(series, ChartType::Ratio) {
            Ok(svg_content) => write_compressed_svg(&ratio_svg_filename, &svg_content),
            Err(e) => log::error!(
                "Failed to generate liquidity ratio chart for {}: {}",
                channel_key,
                e
            ),
        }
    }

    log::info!("Funds command completed");
}

fn channel_key(fund: &cmd::Fund) -> String {
    fund.short_channel_id
        .clone()
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| fund.channel_id.clone())
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

fn generate_svg_chart(series: &ChannelSeries, chart_type: ChartType) -> Result<String, String> {
    if series.samples.is_empty() {
        return Err("No data to plot".to_string());
    }

    let mut min_value = u64::MAX;
    let mut max_value = 0u64;

    for sample in &series.samples {
        let (value_0, value_1) = match chart_type {
            ChartType::Liquidity => (sample.our_amount_sat, sample.remote_amount_sat),
            ChartType::Ratio => {
                let ratio = liquidity_percent(sample);
                (ratio, 100_u64.saturating_sub(ratio))
            }
        };
        min_value = min_value.min(value_0).min(value_1);
        max_value = max_value.max(value_0).max(value_1);
    }

    let y_padding = (max_value - min_value).max(1) / 10;

    if matches!(chart_type, ChartType::Liquidity) {
        min_value = 0;
        max_value += y_padding;
    } else {
        min_value = 0;
        max_value = 100;
    }

    let width = 1200;
    let height = 600;
    let margin_left = 80;
    let margin_right = 250;
    let margin_top = 40;
    let margin_bottom = 80;
    let plot_width = width - margin_left - margin_right;
    let plot_height = height - margin_top - margin_bottom;

    let min_timestamp = series.samples.first().unwrap().timestamp;
    let max_timestamp = series.samples.last().unwrap().timestamp;
    let time_range = (max_timestamp - min_timestamp).max(1);

    let scale_x = |timestamp: u32| -> i32 {
        margin_left
            + ((timestamp - min_timestamp) as f64 / time_range as f64 * plot_width as f64) as i32
    };

    let scale_y = |value: u64| -> i32 {
        if max_value == min_value {
            return margin_top + plot_height / 2;
        }
        margin_top + plot_height
            - ((value - min_value) as f64 / (max_value - min_value) as f64 * plot_height as f64)
                as i32
    };

    let mut local_points = Vec::new();
    let mut remote_points = Vec::new();

    for sample in &series.samples {
        let x = scale_x(sample.timestamp);
        let (local_value, remote_value) = match chart_type {
            ChartType::Liquidity => (sample.our_amount_sat, sample.remote_amount_sat),
            ChartType::Ratio => {
                let ratio = liquidity_percent(sample);
                (ratio, 100_u64.saturating_sub(ratio))
            }
        };
        local_points.push((x, scale_y(local_value), sample.timestamp, local_value));
        remote_points.push((x, scale_y(remote_value), sample.timestamp, remote_value));
    }

    let local_points = deduplicate_consecutive_points(local_points);
    let remote_points = deduplicate_consecutive_points(remote_points);

    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {} {}" width="{}" height="{}">"##,
        width, height, width, height
    ));
    svg.push('\n');
    svg.push_str(&format!(
        r##"  <rect width="{}" height="{}" fill="#f5f5f5"/>"##,
        width, height
    ));
    svg.push('\n');

    let title = match chart_type {
        ChartType::Liquidity => "Channel Liquidity Over Time",
        ChartType::Ratio => "Channel Liquidity Ratio Over Time",
    };
    svg.push_str(&format!(
        r##"  <text x="{}" y="25" font-family="Arial, sans-serif" font-size="18" font-weight="bold" text-anchor="middle">{}</text>"##,
        width / 2, title
    ));
    svg.push('\n');

    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="black" stroke-width="2"/>"##,
        margin_left,
        margin_top + plot_height,
        margin_left + plot_width,
        margin_top + plot_height
    ));
    svg.push('\n');
    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="black" stroke-width="2"/>"##,
        margin_left,
        margin_top,
        margin_left,
        margin_top + plot_height
    ));
    svg.push('\n');

    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" text-anchor="middle">Date</text>"##,
        margin_left + plot_width / 2, height - 20
    ));
    svg.push('\n');

    let y_axis_label = match chart_type {
        ChartType::Liquidity => "Satoshis",
        ChartType::Ratio => "Percent",
    };
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" transform="rotate(-90, {}, {})">{}</text>"##,
        20, margin_top + plot_height / 2, 20, margin_top + plot_height / 2, y_axis_label
    ));
    svg.push('\n');

    let num_y_ticks = 5;
    for i in 0..=num_y_ticks {
        let value = min_value + (max_value - min_value) * i / num_y_ticks;
        let y = scale_y(value);

        svg.push_str(&format!(
            r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#e0e0e0" stroke-width="1"/>"##,
            margin_left,
            y,
            margin_left + plot_width,
            y
        ));
        svg.push('\n');

        let tick_label = match chart_type {
            ChartType::Liquidity => value.to_string(),
            ChartType::Ratio => format!("{}%", value),
        };
        svg.push_str(&format!(
            r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="12" text-anchor="end" alignment-baseline="middle">{}</text>"##,
            margin_left - 10, y, tick_label
        ));
        svg.push('\n');
    }

    let num_x_ticks = 6;
    for i in 0..=num_x_ticks {
        let timestamp = min_timestamp + (max_timestamp - min_timestamp) * i / num_x_ticks;
        let x = scale_x(timestamp);
        let date = format_timestamp(timestamp);

        svg.push_str(&format!(
            r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="11" text-anchor="end" transform="rotate(-45, {}, {})">{}  </text>"##,
            x, margin_top + plot_height + 15, x, margin_top + plot_height + 15, date
        ));
        svg.push('\n');
    }

    append_series_to_svg(&mut svg, &local_points, "#2563eb", "Local", chart_type);
    append_series_to_svg(&mut svg, &remote_points, "#dc2626", "Remote", chart_type);

    let legend_x = width - margin_right + 20;
    let legend_y = margin_top + 20;

    svg.push_str(&format!(
        r##"  <rect x="{}" y="{}" width="200" height="105" fill="white" stroke="black" stroke-width="1"/>"##,
        legend_x - 10, legend_y - 10
    ));
    svg.push('\n');
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" font-weight="bold">Legend</text>"##,
        legend_x, legend_y + 5
    ));
    svg.push('\n');

    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#2563eb" stroke-width="3"/>"##,
        legend_x,
        legend_y + 25,
        legend_x + 30,
        legend_y + 25
    ));
    svg.push('\n');
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, monospace" font-size="10" alignment-baseline="middle">Local</text>"##,
        legend_x + 40,
        legend_y + 25
    ));
    svg.push('\n');

    svg.push_str(&format!(
        r##"  <line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#dc2626" stroke-width="3"/>"##,
        legend_x,
        legend_y + 50,
        legend_x + 30,
        legend_y + 50
    ));
    svg.push('\n');
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, monospace" font-size="10" alignment-baseline="middle">Remote</text>"##,
        legend_x + 40,
        legend_y + 50
    ));
    svg.push('\n');

    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, monospace" font-size="10">Peer: {}</text>"##,
        legend_x,
        legend_y + 78,
        truncate_node_id(&series.peer_id)
    ));
    svg.push('\n');

    let total_capacity = series
        .samples
        .last()
        .map(|sample| sample.total_amount_sat)
        .unwrap_or(0);
    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, monospace" font-size="10">Cap: {} sats</text>"##,
        legend_x,
        legend_y + 95,
        total_capacity
    ));
    svg.push('\n');

    svg.push_str(&format!(
        r##"  <text x="{}" y="{}" font-family="Arial, sans-serif" font-size="14" font-style="italic" fill="#888888" text-anchor="end">LOUDTOLL since 2019</text>"##,
        width - 20, height - 15
    ));
    svg.push('\n');
    svg.push_str("</svg>\n");

    Ok(svg)
}

fn append_series_to_svg(
    svg: &mut String,
    points: &[(i32, i32, u32, u64)],
    color: &str,
    label: &str,
    chart_type: ChartType,
) {
    if points.is_empty() {
        return;
    }

    let path_data = points
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
        r##"  <path d="{}" fill="none" stroke="{}" stroke-width="2" opacity="0.7"/>"##,
        path_data, color
    ));
    svg.push('\n');

    for (x, y, timestamp, value) in points {
        let date_str = format_timestamp(*timestamp);
        let value_label = match chart_type {
            ChartType::Liquidity => format!("{}: {} sats", label, value),
            ChartType::Ratio => format!("{}: {}%", label, value),
        };
        svg.push_str(&format!(
            r##"  <circle cx="{}" cy="{}" r="4" fill="{}" opacity="0.7"><title>{} ({})</title></circle>"##,
            x, y, color, value_label, date_str
        ));
        svg.push('\n');
    }
}

fn liquidity_percent(sample: &FundSnapshot) -> u64 {
    if sample.total_amount_sat == 0 {
        0
    } else {
        ((sample.our_amount_sat as f64 / sample.total_amount_sat as f64) * 100.0).round() as u64
    }
}

fn format_timestamp(timestamp: u32) -> String {
    const SECONDS_PER_DAY: u32 = 86400;
    let days = timestamp / SECONDS_PER_DAY;

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

fn deduplicate_consecutive_points(points: Vec<(i32, i32, u32, u64)>) -> Vec<(i32, i32, u32, u64)> {
    if points.len() <= 2 {
        return points;
    }

    let mut result = Vec::with_capacity(points.len());
    let mut i = 0;

    while i < points.len() {
        let current_value = points[i].3;
        let run_start = i;

        while i < points.len() && points[i].3 == current_value {
            i += 1;
        }
        let run_end = i - 1;

        result.push(points[run_start]);
        if run_end > run_start {
            result.push(points[run_end]);
        }
    }

    result
}

fn truncate_node_id(node_id: &str) -> String {
    if node_id.len() > 16 {
        format!("{}...{}", &node_id[..8], &node_id[node_id.len() - 8..])
    } else {
        node_id.to_string()
    }
}
