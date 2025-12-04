use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs;

use crate::cmd;
use crate::{common::*, store::Store};
use maud::{html, Markup, PreEscaped, DOCTYPE};

#[derive(Clone)]
struct NodeChannelDisplay {
    other_node_alias: String,
    other_node_id: String,
    short_channel_id: String,
    fee_rate_ppm: u64,
}

/// Create common HTML header with title
fn create_html_header(title: &str) -> Markup {
    html! {
        head {
            title { (title) }
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
                .content {
                    background-color: #2d3748;
                    padding: 20px;
                    border-radius: 8px;
                    margin-bottom: 20px;
                    overflow-x: auto;
                    white-space: pre;
                }
                .section {
                    margin-bottom: 30px;
                }
                .section-title {
                    color: #63b3ed;
                    border-bottom: 2px solid #63b3ed;
                    padding-bottom: 5px;
                    margin-bottom: 15px;
                    font-size: 1.2em;
                    font-weight: bold;
                }
                .timestamp {
                    color: #a0aec0;
                    font-size: 0.9em;
                    text-align: center;
                }
                a {
                    color: #63b3ed;
                    text-decoration: none;
                }
                a:hover {
                    text-decoration: underline;
                }
                .back-link {
                    display: inline-block;
                    margin-bottom: 20px;
                    color: #63b3ed;
                    font-size: 1.1em;
                }
                .progress-bar {
                    width: 100%;
                    height: 20px;
                    background-color: #2d3748;
                    border-radius: 10px;
                    margin-top: 10px;
                    overflow: hidden;
                }
                .progress-fill {
                    height: 100%;
                    background-color: #63b3ed;
                    border-radius: 10px;
                    transition: width 0.3s ease;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }
                th, td {
                    border: 1px solid #4a5568;
                    padding: 8px 12px;
                    text-align: left;
                }
                th {
                    background-color: #2d3748;
                    color: #63b3ed;
                    font-weight: bold;
                }
                tbody tr:nth-child(even) {
                    background-color: #2d3748;
                }
                tbody tr:nth-child(odd) {
                    background-color: #1a202c;
                }
                tbody tr:hover {
                    background-color: #4a5568;
                }
                th[data-sort] {
                    cursor: pointer;
                    user-select: none;
                    position: relative;
                }
                th[data-sort]:hover {
                    background-color: #4a5568;
                }
                th[data-sort]:after {
                    content: ' ⇅';
                    opacity: 0.5;
                }
                th[data-sort].asc:after {
                    content: ' ↑';
                    opacity: 1;
                }
                th[data-sort].desc:after {
                    content: ' ↓';
                    opacity: 1;
                }
                .number-cell {
                    text-align: right;
                }
                "#
            }
            script {
                (PreEscaped(include_str!("script.js")))
            }
        }
    }
}

/// Create common page header with navigation links
fn create_page_header(title: &str, is_subdir: bool) -> Markup {
    let (
        home_link,
        nodes_link,
        channels_link,
        forwards_link,
        routes_link,
        failures_link,
        apy_link,
        closed_link,
    ) = if is_subdir {
        (
            "../index.html",
            "../nodes/",
            "../channels/",
            "../forwards-week.html",
            "../routes-10000.html",
            "../failures.html",
            "../apy.html",
            "../closed-channels.html",
        )
    } else {
        (
            "index.html",
            "nodes/",
            "channels/",
            "forwards-week.html",
            "routes-10000.html",
            "failures.html",
            "apy.html",
            "closed-channels.html",
        )
    };

    html! {
        div class="header" {
            h1 { (title) }
            div class="back-link" {
                a href=(home_link) { "Home" } " | "
                a href=(nodes_link) { "Nodes" } " | "
                a href=(channels_link) { "Channels" } " | "
                a href=(forwards_link) { "Forwards" } " | "
                a href=(routes_link) { "Routes" } " | "
                a href=(failures_link) { "Failures" } " | "
                a href=(apy_link) { "APY" } " | "
                a href=(closed_link) { "Closed" }
            }
        }
    }
}

/// Create common HTML footer
fn create_html_footer(timestamp: &str) -> Markup {
    html! {
        footer {
            div class="timestamp" {
                "Generated at: " (timestamp)
            }
        }
    }
}

/// Wrap content in a complete HTML page
fn wrap_in_html_page(title: &str, content: Markup, timestamp: &str) -> Markup {
    html! {
        (DOCTYPE)
        html {
            (create_html_header(title))
            body {
                div class="container" {
                    (content)
                    (create_html_footer(timestamp))
                }
            }
        }
    }
}

fn create_node_pages(
    directory: &str,
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    min_channels: usize,
) {
    let nodes_dir = format!("{}/nodes", directory);

    // Create nodes directory
    if let Err(e) = fs::create_dir_all(&nodes_dir) {
        log::debug!("Error creating nodes directory {}: {}", nodes_dir, e);
        return;
    }

    log::debug!("Creating node pages in: {}", nodes_dir);

    // Filter and sort nodes: only those with at least min_channels, sorted by alias
    let mut filtered_nodes: Vec<&cmd::Node> = store
        .nodes()
        .filter(|n| store.node_total_channels(&n.nodeid) >= min_channels)
        .collect();
    log::info!("Filtered nodes: {}", filtered_nodes.len());
    filtered_nodes.sort_by(|a, b| {
        store
            .get_node_alias(&a.nodeid)
            .cmp(&store.get_node_alias(&b.nodeid))
    });

    // Create nodes index page with comma-separated links (sorted)
    let nodes_index_content = html! {
        (create_page_header("Nodes", true))

        div class="info-card" {
            h2 { "Node List" }
            p { "Total nodes with channels: " (filtered_nodes.len()) }

            div class="node-links" {
                @for (i, node) in filtered_nodes.iter().enumerate() {
                    @if i > 0 { ", " }
                    a href={(format!("{}.html", node.nodeid))} {
                        (store.get_node_alias(&node.nodeid))
                    }
                }
            }
        }
    };

    let nodes_index_html = wrap_in_html_page(
        "Nodes Directory",
        nodes_index_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );
    let nodes_index_path = format!("{}/index.html", nodes_dir);

    match fs::write(&nodes_index_path, nodes_index_html.into_string()) {
        Ok(_) => log::debug!("Nodes index page generated: {}", nodes_index_path),
        Err(e) => log::debug!("Error writing nodes index page: {}", e),
    }

    // Precompute peer map for quick lookup of connected peers
    let mut peer_map: HashMap<String, &cmd::Peer> = HashMap::new();
    for peer in store.peers() {
        peer_map.insert(peer.id.clone(), peer);
    }

    // Create individual pages for filtered nodes
    for node in filtered_nodes {
        let nodeid = node.nodeid.clone();
        let alias = store.get_node_alias(&nodeid);

        // Default values for non-peers
        let connected = peer_map.contains_key(&nodeid);
        let num_channels = store.node_total_channels(&nodeid);

        let note = store.get_peer_note(&nodeid);
        let _peer_channels = if let Some(peer) = peer_map.get(&nodeid) {
            peer.channels.clone()
        } else {
            vec![]
        };

        // Fee distribution based on channels to this node
        let fee_dist = store.get_peer_fee_distribution(&nodeid);
        let fee_stats = store.get_peer_fee_stats(&nodeid);
        let max_amount = fee_dist
            .outgoing_amounts
            .iter()
            .chain(fee_dist.incoming_amounts.iter())
            .max()
            .copied()
            .unwrap_or(1);

        // Collect channel display data
        let mut node_channels: Vec<NodeChannelDisplay> = store
            .get_node_channels(&nodeid)
            .into_iter()
            .map(|channel| {
                let other_node_id = channel.destination.clone();
                let other_node_alias = store.get_node_alias(&other_node_id);

                NodeChannelDisplay {
                    other_node_alias,
                    other_node_id,
                    short_channel_id: channel.short_channel_id.clone(),
                    fee_rate_ppm: channel.fee_per_millionth,
                }
            })
            .collect();

        // Sort by fee rate by default
        node_channels.sort_by(|a, b| a.fee_rate_ppm.cmp(&b.fee_rate_ppm));

        let node_content = html! {
            (create_page_header("Node Details", true))

            div class="info-card" {
                h2 { "Node Information" }

                div class="info-item" {
                    span class="label" { "Node ID: " }
                    span class="value" {
                        a href={(format!("https://mempool.space/lightning/node/{}", nodeid))} target="_blank" {
                            (nodeid)
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "Alias: " }
                    span class="value" { (alias) }
                }

                div class="info-item" {
                    span class="label" { "Connected: " }
                    span class="value" {
                        @if connected {
                            "Yes"
                        } @else {
                            "No"
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "Total Channels: " }
                    span class="value" { (num_channels) }
                }

                @if let Some(note_val) = note {
                    div class="info-item" {
                        span class="label" { "Note: " }
                        span class="value" { (note_val) }
                    }
                }

                @if let Some(last_timestamp) = node.last_timestamp {
                    div class="info-item" {
                        span class="label" { "Last Seen: " }
                        span class="value" {
                            (chrono::DateTime::from_timestamp(last_timestamp as i64, 0)
                                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                .unwrap_or_else(|| "Unknown".to_string()))
                        }
                    }
                }
            }

            // Outgoing Fee Distribution Chart
            div class="info-card" {
                h2 { "Outgoing Fee Distribution" }

                div class="info-item" {
                    span class="label" { "Mean Fee: " }
                    span class="value" { (format!("{} ppm", fee_stats.outgoing_mean)) }
                }

                div class="info-item" {
                    span class="label" { "Median Fee: " }
                    span class="value" { (format!("{} ppm", fee_stats.outgoing_median)) }
                }

                div class="fee-chart-container" {
                    div class="fee-chart-y-axis" {
                        @for i in (0..=4).rev() {
                            @let value = (max_amount as f64 * i as f64 / 4.0) as u64;
                            div class="fee-chart-y-label" {
                                @if value >= 1_000_000 {
                                    (format!("{}M", value / 1_000_000))
                                } @else if value >= 1_000 {
                                    (format!("{}K", value / 1_000))
                                } @else {
                                    (format!("{}", value))
                                }
                            }
                        }
                    }
                    div class="fee-chart-bars-container" {
                        @for (i, _label) in fee_dist.labels.iter().enumerate() {
                            @let outgoing = fee_dist.outgoing_amounts[i];
                            div class="fee-chart-group" {
                                div class="fee-bar-wrapper" {
                                    @if outgoing > 0 {
                                        div class="fee-bar outgoing" style={
                                            (format!("height: {}px", (outgoing as f64 / max_amount as f64 * 120.0) as u64))
                                        } {}
                                    } @else {
                                        div class="fee-bar-empty" {}
                                    }
                                }
                            }
                        }
                    }
                    div class="fee-chart-x-axis" {
                        @for (_i, label) in fee_dist.labels.iter().enumerate() {
                            div class="fee-chart-x-label" {
                                (label)
                            }
                        }
                    }
                }
            }

            // Incoming Fee Distribution Chart
            div class="info-card" {
                h2 { "Incoming Fee Distribution" }

                div class="info-item" {
                    span class="label" { "Mean Fee: " }
                    span class="value" { (format!("{} ppm", fee_stats.incoming_mean)) }
                }

                div class="info-item" {
                    span class="label" { "Median Fee: " }
                    span class="value" { (format!("{} ppm", fee_stats.incoming_median)) }
                }

                div class="fee-chart-container" {
                    div class="fee-chart-y-axis" {
                        @for i in (0..=4).rev() {
                            @let value = (max_amount as f64 * i as f64 / 4.0) as u64;
                            div class="fee-chart-y-label" {
                                @if value >= 1_000_000 {
                                    (format!("{}M", value / 1_000_000))
                                } @else if value >= 1_000 {
                                    (format!("{}K", value / 1_000))
                                } @else {
                                    (format!("{}", value))
                                }
                            }
                        }
                    }
                    div class="fee-chart-bars-container" {
                        @for (i, _label) in fee_dist.labels.iter().enumerate() {
                            @let incoming = fee_dist.incoming_amounts[i];
                            div class="fee-chart-group" {
                                div class="fee-bar-wrapper" {
                                    @if incoming > 0 {
                                        div class="fee-bar incoming" style={
                                            (format!("height: {}px", (incoming as f64 / max_amount as f64 * 120.0) as u64))
                                        } {}
                                    } @else {
                                        div class="fee-bar-empty" {}
                                    }
                                }
                            }
                        }
                    }
                    div class="fee-chart-x-axis" {
                        @for (_i, label) in fee_dist.labels.iter().enumerate() {
                            div class="fee-chart-x-label" {
                                (label)
                            }
                        }
                    }
                }
            }

            @if !node_channels.is_empty() {
                div class="info-card" {
                    h2 { "Connected Channels" }
                    p { "Total channels: " (node_channels.len()) }

                    table class="sortable" {
                        thead {
                            tr {
                                th data-sort="string" { "Other Node" }
                                th data-sort="string" { "Short Channel ID" }
                                th data-sort="number" { "Fee Rate (ppm)" }
                            }
                        }
                        tbody {
                            @for channel in &node_channels {
                                tr {
                                    td {
                                        a href={(format!("{}.html", channel.other_node_id))} {
                                            (channel.other_node_alias)
                                        }
                                    }
                                    td {
                                        a href={(format!("../../../charts/channels/{}.html", channel.short_channel_id))} {
                                            (channel.short_channel_id)
                                        }
                                    }
                                    td class="number-cell" { (channel.fee_rate_ppm) }
                                }
                            }
                        }
                    }
                }
            }



            style {
                r#"
                .fee-chart-container {
                    display: flex;
                    flex-direction: column;
                    padding: 20px;
                    background-color: #1a202c;
                    border-radius: 6px;
                    position: relative;
                }

                .fee-chart-y-axis {
                    position: absolute;
                    left: 20px;
                    top: 20px;
                    height: 120px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                    z-index: 1;
                }

                .fee-chart-y-label {
                    color: #a0aec0;
                    font-size: 10px;
                    text-align: right;
                    padding-right: 8px;
                    min-width: 40px;
                    height: 0;
                    position: relative;
                }

                .fee-chart-y-label::after {
                    content: '';
                    position: absolute;
                    left: 48px;
                    top: 0;
                    width: calc(100vw - 100px);
                    height: 1px;
                    background-color: #2d3748;
                    z-index: 0;
                }

                .fee-chart-bars-container {
                    display: flex;
                    justify-content: space-between;
                    align-items: flex-end;
                    min-height: 120px;
                    padding: 0 5px 0 55px;
                    position: relative;
                }

                .fee-chart-group {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    flex: 1;
                    position: relative;
                    z-index: 2;
                }

                .fee-bar-wrapper {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: flex-end;
                    height: 100%;
                }

                .fee-bar {
                    width: 16px;
                    min-height: 2px;
                    position: relative;
                    transition: all 0.3s ease;
                    border-radius: 3px 3px 0 0;
                }

                .fee-bar.outgoing {
                    background-color: #63b3ed;
                }

                .fee-bar.incoming {
                    background-color: #48bb78;
                }

                .fee-bar:hover {
                    opacity: 0.8;
                }

                .fee-bar-empty {
                    width: 16px;
                    height: 2px;
                    background-color: #2d3748;
                }

                .fee-chart-x-axis {
                    display: flex;
                    justify-content: space-between;
                    padding: 5px 5px 0 55px;
                    margin-top: 5px;
                    border-top: 1px solid #4a5568;
                }

                .fee-chart-x-label {
                    color: #a0aec0;
                    font-size: 10px;
                    text-align: center;
                    min-height: 14px;
                    flex: 1;
                    padding-top: 5px;
                }

                .node-links {
                    margin-top: 10px;
                    padding: 10px;
                    background-color: #2d3748;
                    border-radius: 4px;
                    word-break: break-all;
                }
                .node-links a {
                    color: #63b3ed;
                }
                "#
            }
        };

        let node_html = wrap_in_html_page(
            &format!("Node {}", &nodeid[..8]),
            node_content,
            &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        );
        let node_file_path = format!("{}/{}.html", nodes_dir, nodeid);

        match fs::write(&node_file_path, node_html.into_string()) {
            Ok(_) => log::debug!("Node page generated: {}", node_file_path),
            Err(e) => log::debug!("Error writing node page {}: {}", node_file_path, e),
        }
    }
}

fn create_weekday_chart_page(directory: &str, store: &Store, now: &chrono::DateTime<chrono::Utc>) {
    let weekday_counts = store.forwards_by_weekday();

    let weekday_names = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    let max_count = *weekday_counts.iter().max().unwrap_or(&0);

    let chart_content = html! {
        (create_page_header("Forwards by Weekday", false))

        div class="info-card" {
            h2 { "Settled Forwards Distribution by Day of Week" }

            div class="chart-container" {
                @for (i, &count) in weekday_counts.iter().enumerate() {
                    div class="chart-bar" {
                        div class="bar-label" { (weekday_names[i]) ": " (count) }
                        div class="bar-fill" style={
                            @if max_count > 0 {
                                (format!("width: {:.1}%", (count as f64 / max_count as f64) * 100.0))
                            } @else {
                                "width: 0%"
                            }
                        } {
                            div class="bar-value" { (count) }
                        }
                    }
                }
            }

            div class="info-card" {
                h3 { "Statistics" }
                p { "Total settled forwards: " (store.settled_forwards().len()) }
                @if let Some(most_active) = weekday_counts.iter().enumerate().max_by_key(|(_, &count)| count) {
                    p { "Most active day: " (weekday_names[most_active.0]) " (" (most_active.1) " forwards)" }
                }
                @if let Some(least_active) = weekday_counts.iter().enumerate().min_by_key(|(_, &count)| count) {
                    p { "Least active day: " (weekday_names[least_active.0]) " (" (least_active.1) " forwards)" }
                }
            }
        }

        style {
            r#"
            .chart-container {
                margin: 20px 0;
                padding: 20px;
                background-color: #2d3748;
                border-radius: 8px;
            }

            .chart-bar {
                display: flex;
                align-items: center;
                margin-bottom: 15px;
                padding: 10px;
                background-color: #1a202c;
                border-radius: 6px;
            }

            .bar-label {
                width: 150px;
                font-weight: bold;
                color: #63b3ed;
                flex-shrink: 0;
            }

            .bar-fill {
                height: 30px;
                background-color: #63b3ed;
                border-radius: 4px;
                position: relative;
                margin-left: 20px;
                transition: width 0.3s ease;
                min-width: 40px;
                display: flex;
                align-items: center;
                justify-content: flex-end;
                padding-right: 10px;
            }

            .bar-value {
                color: #1a202c;
                font-weight: bold;
                font-size: 14px;
            }

            .info-card h3 {
                color: #63b3ed;
                margin-top: 0;
                margin-bottom: 15px;
            }

            .info-card p {
                margin: 8px 0;
                color: #f8f8f2;
            }
            "#
        }
    };

    let chart_html = wrap_in_html_page(
        "Forwards by Weekday",
        chart_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let chart_file_path = format!("{}/weekday-chart.html", directory);
    match fs::write(&chart_file_path, chart_html.into_string()) {
        Ok(_) => log::debug!("Weekday chart page generated: {}", chart_file_path),
        Err(e) => log::debug!("Error writing weekday chart page: {}", e),
    }
}

/// Create HTML content for a forwards page with the given forwards data
fn create_forwards_html_content(
    forwards: &[crate::cmd::SettledForward],
    title: &str,
    store: &Store,
    our_node_id: &String,
) -> Markup {
    // Helper function to get node alias and ID for a channel
    let get_channel_info = |channel_id: &str| -> (String, Option<String>) {
        let channel_id_string = channel_id.to_string();

        // Try to find the channel in our channels (we're the source)
        if let Some(channel) = store.get_channel(&channel_id_string, our_node_id) {
            // We're the source, so the destination is the remote node
            return (
                store.get_node_alias(&channel.destination),
                Some(channel.destination.clone()),
            );
        }

        // If we can't find the channel, return the original channel ID
        (channel_id.to_string(), None)
    };

    html! {
        (create_page_header(title, false))

        div class="info-card" {
            h2 { "Settled Forward Payments" }
            p { "Total settled forwards: " (forwards.len()) }

            @if !forwards.is_empty() {
                table class="sortable" {
                    thead {
                        tr {
                            th { "In Node" }
                            th { "Out Node" }
                            th { "Fee (sats)" }
                            th { "Out Amount (sats)" }
                            th { "Fee PPM" }
                            th { "Received Time" }
                            th { "Elapsed (s)" }
                        }
                    }
                    tbody {
                        @for forward in forwards {
                            @let (in_alias, in_node_id) = get_channel_info(&forward.in_channel);
                            @let (out_alias, out_node_id) = get_channel_info(&forward.out_channel);
                            tr {
                                td {
                                    @if let Some(node_id) = in_node_id {
                                        a href={(format!("nodes/{}.html", node_id))} { (in_alias) }
                                    } @else {
                                        (in_alias)
                                    }
                                    " "
                                    a href={(format!("channels/{}.html", forward.in_channel))} { "(C)" }
                                }
                                td {
                                    @if let Some(node_id) = out_node_id {
                                        a href={(format!("nodes/{}.html", node_id))} { (out_alias) }
                                    } @else {
                                        (out_alias)
                                    }
                                    " "
                                    a href={(format!("channels/{}.html", forward.out_channel))} { "(C)" }
                                }
                                td class="align-right" {
                                    (forward.fee_sat)
                                }
                                td class="align-right" {
                                    (forward.out_sat)
                                }
                                td class="align-right" {
                                    (forward.fee_ppm)
                                }
                                td {
                                    (forward.received_time.format("%Y-%m-%d %H:%M:%S").to_string())
                                }
                                td class="align-right" {
                                    (format!("{:.1}", (forward.resolved_time - forward.received_time).num_seconds() as f64))
                                }
                            }
                        }
                    }
                }
            } @else {
                p { "No settled forwards found." }
            }
        }

        style {
            r#"
            .align-right {
                text-align: right;
            }
            "#
        }
    }
}

fn create_forwards_page(
    directory: &str,
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
) {
    let settled_forwards = store.settled_forwards();

    let forwards_content =
        create_forwards_html_content(&settled_forwards, "Settled Forwards", store, our_node_id);

    let forwards_html = wrap_in_html_page(
        "Settled Forwards",
        forwards_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let forwards_file_path = format!("{}/forwards.html", directory);
    match fs::write(&forwards_file_path, forwards_html.into_string()) {
        Ok(_) => log::debug!("Forwards page generated: {}", forwards_file_path),
        Err(e) => log::debug!("Error writing forwards page: {}", e),
    }
}

fn create_forwards_week_page(
    directory: &str,
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
) {
    let settled_forwards = store.filter_settled_forwards_by_days(7);

    let forwards_content = create_forwards_html_content(
        &settled_forwards,
        "Settled Forwards - Last Week",
        store,
        our_node_id,
    );

    let forwards_html = wrap_in_html_page(
        "Settled Forwards - Last Week",
        forwards_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let forwards_file_path = format!("{}/forwards-week.html", directory);
    match fs::write(&forwards_file_path, forwards_html.into_string()) {
        Ok(_) => log::debug!("Weekly forwards page generated: {}", forwards_file_path),
        Err(e) => log::debug!("Error writing weekly forwards page: {}", e),
    }
}

fn create_forwards_year_page(
    directory: &str,
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
) {
    let settled_forwards = store.filter_settled_forwards_by_days(365);

    let forwards_content = create_forwards_html_content(
        &settled_forwards,
        "Settled Forwards - Last Year",
        store,
        our_node_id,
    );

    let forwards_html = wrap_in_html_page(
        "Settled Forwards - Last Year",
        forwards_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let forwards_file_path = format!("{}/forwards-year.html", directory);
    match fs::write(&forwards_file_path, forwards_html.into_string()) {
        Ok(_) => log::debug!("Yearly forwards page generated: {}", forwards_file_path),
        Err(e) => log::debug!("Error writing yearly forwards page: {}", e),
    }
}

fn create_failures_page(
    directory: &str,
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
) {
    // Helper function to get node alias and ID for a channel
    let get_channel_info = |channel_id: &str| -> (String, Option<String>) {
        let channel_id_string = channel_id.to_string();

        // Try to find the channel in our channels (we're the source)
        if let Some(channel) = store.get_channel(&channel_id_string, our_node_id) {
            // We're the source, so the destination is the remote node
            return (
                store.get_node_alias(&channel.destination),
                Some(channel.destination.clone()),
            );
        }

        // If we can't find the channel, return the original channel ID
        (channel_id.to_string(), None)
    };

    let stats = store.get_forward_statistics();

    let failures_content = html! {
        (create_page_header("Forward Failures Analysis", false))

        // Statistics Overview
        div class="info-card" {
            h2 { "Forward Statistics Overview" }
            p { "Track your routing success ratio over time to measure the impact of channel management decisions." }

            table {
                thead {
                    tr {
                        th { "Time Period" }
                        th { "Settled" }
                        th { "Failed" }
                        th { "Local Failed" }
                        th { "Total Attempts" }
                        th { "Success Ratio" }
                        th { "Settled/Day" }
                        th { "Failed/Day" }
                    }
                }
                tbody {
                    tr {
                        td { strong { "Day" } }
                        td class="align-right" { (stats.day_settled) }
                        td class="align-right" { (stats.day_failed) }
                        td class="align-right" { (stats.day_local_failed) }
                        td class="align-right" { (stats.day_all) }
                        td class="align-right" { (format!("{:.2}%", stats.day_success_ratio())) }
                        td class="align-right" { (format!("{:.1}", stats.day_per_day(stats.day_settled))) }
                        td class="align-right" { (format!("{:.1}", stats.day_per_day(stats.day_failed + stats.day_local_failed))) }
                    }
                    tr {
                        td { strong { "Week" } }
                        td class="align-right" { (stats.week_settled) }
                        td class="align-right" { (stats.week_failed) }
                        td class="align-right" { (stats.week_local_failed) }
                        td class="align-right" { (stats.week_all) }
                        td class="align-right" { (format!("{:.2}%", stats.week_success_ratio())) }
                        td class="align-right" { (format!("{:.1}", stats.week_per_day(stats.week_settled))) }
                        td class="align-right" { (format!("{:.1}", stats.week_per_day(stats.week_failed + stats.week_local_failed))) }
                    }
                    tr {
                        td { strong { "Month" } }
                        td class="align-right" { (stats.month_settled) }
                        td class="align-right" { (stats.month_failed) }
                        td class="align-right" { (stats.month_local_failed) }
                        td class="align-right" { (stats.month_all) }
                        td class="align-right" { (format!("{:.2}%", stats.month_success_ratio())) }
                        td class="align-right" { (format!("{:.1}", stats.month_per_day(stats.month_settled))) }
                        td class="align-right" { (format!("{:.1}", stats.month_per_day(stats.month_failed + stats.month_local_failed))) }
                    }

                }
            }
        }

        // Table 1: Local Failed Forwards with WIRE_TEMPORARY_CHANNEL_FAILURE (liquidity issues on our side)
        div class="info-card" {
            h2 { "Local Failed Forwards" }
            p { "Local failures happen mostly because: not enough liquidity in the outbound channel, or the other node is offline." }

            @let local_failed_data = store.local_failed_temp_channel_failure_by_out_channel();
            @if !local_failed_data.is_empty() {
                p { "Total channels with local failures: " (local_failed_data.len()) }
                table class="sortable" {
                    thead {
                        tr {
                            th { "Day" }
                            th { "Week" }
                            th { "Month" }
                            th { "Channel ID" }
                            th { "Node Alias" }
                        }
                    }
                    tbody {
                        @for channel_data in local_failed_data.iter() {
                            @let (alias, node_id) = get_channel_info(&channel_data.channel_id);
                            tr {
                                td class="align-right" { (channel_data.counts.day) }
                                td class="align-right" { (channel_data.counts.week) }
                                td class="align-right" { (channel_data.counts.month) }
                                td {
                                    a href={(format!("channels/{}.html", channel_data.channel_id))} { (&channel_data.channel_id) }
                                }
                                td {
                                    @if let Some(node_id) = node_id {
                                        a href={(format!("nodes/{}.html", node_id))} { (alias) }
                                    } @else {
                                        (alias)
                                    }
                                }
                            }
                        }
                    }
                }
            } @else {
                p { "No local failed forwards with WIRE_TEMPORARY_CHANNEL_FAILURE found." }
            }
        }

        // Table 2: All Failed Forwards (failures on the remote node's side)
        div class="info-card" {
            h2 { "All Failed Forwards - Remote Node Issues" }
            p { "These are payments that failed not due to our fault but due to connected nodes. The channel showed is always the outbound channel." }

            @let failed_data = store.failed_forwards_by_out_channel();
            @if !failed_data.is_empty() {
                p { "Total channels with failed forwards: " (failed_data.len()) }
                table class="sortable" {
                    thead {
                        tr {
                            th { "Day" }
                            th { "Week" }
                            th { "Month" }
                            th { "Channel ID" }
                            th { "Node Alias" }
                        }
                    }
                    tbody {
                        @for channel_data in failed_data.iter() {
                            @let (alias, node_id) = get_channel_info(&channel_data.channel_id);
                            tr {
                                td class="align-right" { (channel_data.counts.day) }
                                td class="align-right" { (channel_data.counts.week) }
                                td class="align-right" { (channel_data.counts.month) }
                                td {
                                    a href={(format!("channels/{}.html", channel_data.channel_id))} { (&channel_data.channel_id) }
                                }
                                td {
                                    @if let Some(node_id) = node_id {
                                        a href={(format!("nodes/{}.html", node_id))} { (alias) }
                                    } @else {
                                        (alias)
                                    }
                                }
                            }
                        }
                    }
                }
            } @else {
                p { "No failed forwards found." }
            }
        }

        style {
            r#"
            .align-right {
                text-align: right;
            }
            "#
        }
    };

    let failures_html = wrap_in_html_page(
        "Forward Failures Analysis",
        failures_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let failures_file_path = format!("{}/failures.html", directory);
    match fs::write(&failures_file_path, failures_html.into_string()) {
        Ok(_) => log::debug!("Failures page generated: {}", failures_file_path),
        Err(e) => log::debug!("Error writing failures page: {}", e),
    }
}

fn create_channel_pages(
    directory: &str,
    channels: &[crate::cmd::Fund],
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
    per_channel_last_forward_in: &HashMap<String, DateTime<Utc>>,
    per_channel_last_forward_out: &HashMap<String, DateTime<Utc>>,
    avail_map: &HashMap<String, f64>,
) {
    let channels_dir = format!("{}/channels", directory);

    // Create channels directory
    if let Err(e) = fs::create_dir_all(&channels_dir) {
        log::debug!("Error creating channels directory {}: {}", channels_dir, e);
        return;
    }

    log::debug!("Creating channel pages in: {}", channels_dir);

    // Sort channels by balance percentage (lower first)
    let mut sorted_channels = channels.to_vec();
    sorted_channels.sort_by(|a, b| a.perc_float().partial_cmp(&b.perc_float()).unwrap());

    // Sort channels by sats/day (descending) - only channels 1+ year old AND no forwards in last 2 months
    // Also exclude channels where the peer has a note (those are intentionally kept)
    let mut sorted_channels_by_sats_per_day = channels
        .iter()
        .filter(|c| {
            // Exclude peers with notes
            if store.get_peer_note(&c.peer_id).is_some() {
                return false;
            }

            if let Some(scid) = &c.short_channel_id {
                let is_old_enough = store.get_channel_age_days(scid).unwrap_or(0) >= 365;
                if !is_old_enough {
                    return false;
                }

                // Check if channel has had any forwards in the last 60 days
                let channel_forwards = store.get_channel_forwards(scid);
                let has_recent_forwards = channel_forwards
                    .iter()
                    .any(|f| now.signed_duration_since(f.resolved_time).num_days() <= 60);

                !has_recent_forwards
            } else {
                false
            }
        })
        .cloned()
        .collect::<Vec<_>>();
    sorted_channels_by_sats_per_day.sort_by(|a, b| {
        let a_sats = a
            .short_channel_id
            .as_ref()
            .and_then(|scid| store.get_channel_sats_per_day(scid))
            .unwrap_or(0.0);
        let b_sats = b
            .short_channel_id
            .as_ref()
            .and_then(|scid| store.get_channel_sats_per_day(scid))
            .unwrap_or(0.0);
        b_sats.partial_cmp(&a_sats).unwrap() // descending order
    });

    // Calculate global channel statistics
    let total_inbound: u64 = channels.iter().map(|c| c.our_amount_msat / 1000).sum();
    let total_outbound: u64 = channels
        .iter()
        .map(|c| (c.amount_msat - c.our_amount_msat) / 1000)
        .sum();
    let total_liquidity = total_inbound + total_outbound;
    let global_balance_percentage = if total_liquidity > 0 {
        (total_inbound as f64 / total_liquidity as f64) * 100.0
    } else {
        0.0
    };

    // Create channels index page
    let channels_index_content = html! {
        (create_page_header("Channels", true))

        div class="info-card" {
            h2 { "Channel Liquidity Summary" }
            div class="info-item" {
                span class="label" { "Total Inbound Available: " }
                span class="value" { (format!("{} sats", total_inbound)) }
            }
            div class="info-item" {
                span class="label" { "Total Outbound Available: " }
                span class="value" { (format!("{} sats", total_outbound)) }
            }
            div class="info-item" {
                span class="label" { "Global Balance: " }
                span class="value" { (format!("{:.1}% inbound", global_balance_percentage)) }
            }
            @let (avg_fee, median_fee) = store.network_channel_fees();
            div class="info-item" {
                span class="label" { "Network Avg Fee: " }
                span class="value" { (format!("{} ppm ({:.3}%)", avg_fee as u64, avg_fee / 10000.0)) }
            }
            div class="info-item" {
                span class="label" { "Network Median Fee: " }
                span class="value" { (format!("{} ppm ({:.3}%)", median_fee as u64, median_fee / 10000.0)) }
            }
            @let (node_avg_fee, node_median_fee) = store.node_channel_fees();
            div class="info-item" {
                span class="label" { "Node Avg Fee: " }
                span class="value" { (format!("{} ppm ({:.3}%)", node_avg_fee as u64, node_avg_fee / 10000.0)) }
            }
            div class="info-item" {
                span class="label" { "Node Median Fee: " }
                span class="value" { (format!("{} ppm ({:.3}%)", node_median_fee as u64, node_median_fee / 10000.0)) }
            }
            div class="progress-bar" {
                div class="progress-fill" style={
                    (format!("width: {:.1}%", global_balance_percentage))
                } {}
            }
        }

        div class="info-card" {
            h2 { "Channel List" }
            p { "Total channels: " (channels.len()) }

            table class="sortable" {
                thead {
                    tr {
                        th { "Channel ID" }
                        th { "Node Alias" }
                        th style="text-align: right;" { "Uptime" }
                        th style="text-align: right;" { "Balance %" }
                        th style="text-align: right;" { "Amount (sats)" }
                        th style="text-align: right;" { "My PPM" }
                        th style="text-align: right;" { "Inbound PPM" }
                        th style="text-align: right;" { "Sats/Day" }
                    }
                }
                tbody {
                    @for channel in sorted_channels {
                        tr {
                            td {
                                @if let Some(scid) = &channel.short_channel_id {
                                    a href={(format!("{}.html", scid))} {
                                        (scid)
                                    }
                                } @else {
                                    a href={(format!("{}.html", channel.channel_id))} {
                                        (&channel.channel_id[..16])
                                    }
                                }
                            }
                            td {
                                a href={(format!("../nodes/{}.html", channel.peer_id))} {
                                    (store.get_node_alias(&channel.peer_id))
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(avail) = avail_map.get(&channel.peer_id) {
                                    (format!("{:.0}%", avail * 100.0))
                                } @else {
                                    "N/A"
                                }
                            }
                            td style="text-align: right;" {
                                (format!("{:.1}", channel.perc_float() * 100.0))
                            }
                            td style="text-align: right;" {
                                (channel.amount_msat / 1000)
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(channel_info) = store.get_channel(scid, &store.info.id) {
                                        (channel_info.fee_per_millionth)
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(channel_info) = store.get_channel(scid, &channel.peer_id) {
                                        (channel_info.fee_per_millionth)
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(sats_per_day) = store.get_channel_sats_per_day(scid) {
                                        (format!("{:.0}", sats_per_day))
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                        }
                    }
                }
            }
        }

        div class="info-card" {
            h2 { "Mature Inactive Channels" }
            p { "Channels 1+ year old with no forwards in last 2 months: " (sorted_channels_by_sats_per_day.len()) }

            table class="sortable" {
                thead {
                    tr {
                        th { "Channel ID" }
                        th { "Node Alias" }
                        th style="text-align: right;" { "Uptime" }
                        th style="text-align: right;" { "Balance %" }
                        th style="text-align: right;" { "Amount (sats)" }
                        th style="text-align: right;" { "My PPM" }
                        th style="text-align: right;" { "Inbound PPM" }
                        th style="text-align: right;" { "Sats/Day" }
                        th style="text-align: right;" { "Age (days)" }
                    }
                }
                tbody {
                    @for channel in sorted_channels_by_sats_per_day {
                        tr {
                            td {
                                @if let Some(scid) = &channel.short_channel_id {
                                    a href={(format!("{}.html", scid))} {
                                        (scid)
                                    }
                                } @else {
                                    a href={(format!("{}.html", channel.channel_id))} {
                                        (&channel.channel_id[..16])
                                    }
                                }
                            }
                            td {
                                a href={(format!("../nodes/{}.html", channel.peer_id))} {
                                    (store.get_node_alias(&channel.peer_id))
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(avail) = avail_map.get(&channel.peer_id) {
                                    (format!("{:.0}%", avail * 100.0))
                                } @else {
                                    "N/A"
                                }
                            }
                            td style="text-align: right;" {
                                (format!("{:.1}", channel.perc_float() * 100.0))
                            }
                            td style="text-align: right;" {
                                (channel.amount_msat / 1000)
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(channel_info) = store.get_channel(scid, &store.info.id) {
                                        (channel_info.fee_per_millionth)
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(channel_info) = store.get_channel(scid, &channel.peer_id) {
                                        (channel_info.fee_per_millionth)
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(sats_per_day) = store.get_channel_sats_per_day(scid) {
                                        (format!("{:.0}", sats_per_day))
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                            td style="text-align: right;" {
                                @if let Some(scid) = &channel.short_channel_id {
                                    @if let Some(age_days) = store.get_channel_age_days(scid) {
                                        (age_days)
                                    } @else {
                                        "-"
                                    }
                                } @else {
                                    "-"
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    let channels_index_html = wrap_in_html_page(
        "Channels Directory",
        channels_index_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );
    let channels_index_path = format!("{}/index.html", channels_dir);

    match fs::write(&channels_index_path, channels_index_html.into_string()) {
        Ok(_) => log::debug!("Channels index page generated: {}", channels_index_path),
        Err(e) => log::debug!("Error writing channels index page: {}", e),
    }

    // Create individual channel pages
    for channel in channels {
        let channel_content = html! {
            (create_page_header("Channel", true))

            div class="info-card" {
                h2 { "Channel Information" }

                div class="info-item" {
                    span class="label" { "Channel ID: " }
                    span class="value" { (channel.channel_id) }
                }

                @if let Some(scid) = &channel.short_channel_id {
                    div class="info-item" {
                        span class="label" { "Short Channel ID: " }
                        span class="value" { (scid) }
                    }
                }

                div class="info-item" {
                    span class="label" { "Peer ID: " }
                    span class="value" {
                        a href={(format!("https://mempool.space/lightning/node/{}", channel.peer_id))} target="_blank" {
                            (channel.peer_id)
                        }
                    }
                }

                @if let Some(node_info) = store.nodes.nodes.iter().find(|n| n.nodeid == channel.peer_id) {
                    div class="info-item" {
                        span class="label" { "Peer Alias: " }
                        span class="value" {
                            @if let Some(alias) = &node_info.alias {
                                (alias)
                            } @else {
                                "N/A"
                            }
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "Node Uptime: " }
                    span class="value" {
                        @if let Some(avail) = avail_map.get(&channel.peer_id) {
                            (format!("{:.2}%", avail * 100.0))
                        } @else {
                            "N/A"
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "State: " }
                    span class="value" { (channel.state) }
                }

                div class="info-item" {
                    span class="label" { "Connected: " }
                    span class="value" {
                        @if channel.connected {
                            "Yes"
                        } @else {
                            "No"
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "Funding TXID: " }
                    span class="value" { (channel.funding_txid) }
                }

                div class="info-item" {
                    span class="label" { "Funding Output: " }
                    span class="value" { (channel.funding_output) }
                }
            }

            div class="info-card" {
                h2 { "Balance Information" }

                div class="info-item" {
                    span class="label" { "Total Amount: " }
                    span class="value" { (format!("{} sats", channel.amount_msat / 1000)) }
                }

                div class="info-item" {
                    span class="label" { "Our Amount: " }
                    span class="value" { (format!("{} sats", channel.our_amount_msat / 1000)) }
                }

                div class="info-item" {
                    span class="label" { "Peer Amount: " }
                    span class="value" {
                        (format!("{} sats", (channel.amount_msat - channel.our_amount_msat) / 1000))
                    }
                }

                div class="info-item" {
                    span class="label" { "Balance Ratio: " }
                    span class="value" { (format!("{:.1}%", channel.perc_float() * 100.0)) }
                }

                div class="progress-bar" {
                    div class="progress-fill" style={
                        (format!("width: {:.1}%", channel.perc_float() * 100.0))
                    } {}
                }
            }

            // Channel Fee History Chart
            @if let Some(scid) = &channel.short_channel_id {
                div class="info-card" {
                    h2 { "Channel Fee History" }
                    div class="chart-container" {
                        object data={(format!("/charts/fees/{}.svgz", scid))} type="image/svg+xml" style="width: 100%; background-color:rgb(235, 230, 230); margin:10px" {
                            p { "Chart not available for this channel." }
                        }
                    }

                    h2 { "Channel HTLC max History" }
                    div class="chart-container" {
                        object data={(format!("/charts/htlc-max/{}.svgz", scid))} type="image/svg+xml" style="width: 100%; background-color:rgb(235, 230, 230); margin:10px" {
                            p { "Chart not available for this channel." }
                        }
                    }
                }
            }

            @if let Some(scid) = &channel.short_channel_id {
                @if let Some(channel_info) = store.get_channel(scid, our_node_id) {
                    div class="info-card" {
                        h2 { "Network Channel Information" }

                        div class="info-item" {
                            span class="label" { "Fee Rate: " }
                            span class="value" { (format!("{} ppm", channel_info.fee_per_millionth)) }
                        }

                        div class="info-item" {
                            span class="label" { "Base Fee: " }
                            span class="value" { (format!("{} sat", channel_info.base_fee_millisatoshi/1000)) }
                        }

                        div class="info-item" {
                            span class="label" { "Min HTLC: " }
                            span class="value" { (format!("{} sat", channel_info.htlc_minimum_msat/1000)) }
                        }

                        div class="info-item" {
                            span class="label" { "Max HTLC: " }
                            span class="value" { (format!("{} sat", channel_info.htlc_maximum_msat/1000)) }
                        }

                        div class="info-item" {
                            span class="label" { "Delay: " }
                            span class="value" { (format!("{} blocks", channel_info.delay)) }
                        }
                    }
                }
            }

            // Forward Activity Information
            div class="info-card" {
                h2 { "Forward Activity" }

                @if let Some(scid) = &channel.short_channel_id {
                    @if let Some(last_inbound) = per_channel_last_forward_in.get(scid) {
                        div class="info-item" {
                            span class="label" { "Last Inbound Forward: " }
                            span class="value" {
                                (format!("{} ago", format_duration(now.signed_duration_since(*last_inbound))))
                                br;
                                (format!("({})", last_inbound.format("%Y-%m-%d %H:%M:%S UTC")))
                            }
                        }
                    } @else {
                        div class="info-item" {
                            span class="label" { "Last Inbound Forward: " }
                            span class="value" { "Never" }
                        }
                    }

                    @if let Some(last_outbound) = per_channel_last_forward_out.get(scid) {
                        div class="info-item" {
                            span class="label" { "Last Outbound Forward: " }
                            span class="value" {
                                (format!("{} ago", format_duration(now.signed_duration_since(*last_outbound))))
                                br;
                                (format!("({})", last_outbound.format("%Y-%m-%d %H:%M:%S UTC")))
                            }
                        }
                    } @else {
                        div class="info-item" {
                            span class="label" { "Last Outbound Forward: " }
                            span class="value" { "Never" }
                        }
                    }

                    // Channel Forward Statistics
                    div class="info-item" {
                        span class="label" { "Total Forwards: " }
                        span class="value" { (store.get_channel_total_forwards(scid)) }
                    }

                    div class="info-item" {
                        span class="label" { "Total Fees Earned: " }
                        span class="value" { (format!("{} sats", store.get_channel_total_fees(scid))) }
                    }

                    @if let Some(sats_per_day) = store.get_channel_sats_per_day(scid) {
                        div class="info-item" {
                            span class="label" { "Avg. Sat/Day Earned: " }
                            span class="value" {
                                (format!("{:.0} sats/day", sats_per_day))
                            }
                        }
                    }
                } @else {
                    div class="info-item" {
                        span class="label" { "Forward Activity: " }
                        span class="value" { "Channel ID not available" }
                    }
                }
            }

            // Channel Management Information
            div class="info-card" {
                h2 { "Channel Management" }

                @if let Some(scid) = &channel.short_channel_id {
                    @if let Some(timestamp) = store.get_setchannel_timestamp(scid) {
                        @if let Some(datetime) = chrono::DateTime::from_timestamp(timestamp, 0) {
                            div class="info-item" {
                                span class="label" { "Last Fee Adjustment: " }
                                span class="value" {
                                    (format!("{} ago", format_duration(now.signed_duration_since(datetime))))
                                    br;
                                    (format!("({})", datetime.format("%Y-%m-%d %H:%M:%S UTC")))
                                }
                            }
                        } @else {
                            div class="info-item" {
                                span class="label" { "Last Fee Adjustment: " }
                                span class="value" { "Invalid timestamp" }
                            }
                        }
                    } @else {
                        div class="info-item" {
                            span class="label" { "Last Fee Adjustment: " }
                            span class="value" { "Never" }
                        }
                    }
                } @else {
                    div class="info-item" {
                        span class="label" { "Channel Management: " }
                        span class="value" { "Channel ID not available" }
                    }
                }
            }

            // Channel Forwards Section
            @if let Some(scid) = &channel.short_channel_id {
                @let channel_forwards = store.get_channel_forwards(scid);
                @if !channel_forwards.is_empty() {
                    div class="info-card" {
                        h2 { "Channel Forwards" }
                        p { "Total forwards: " (channel_forwards.len()) }

                        table class="sortable" {
                            thead {
                                tr {
                                    th { "Direction" }
                                    th { "Amount (sats)" }
                                    th { "Fee (sats)" }
                                    th { "Fee PPM" }
                                    th { "Received Time" }
                                    th { "Elapsed (s)" }
                                }
                            }
                            tbody {
                                @for forward in channel_forwards.iter().take(100) {
                                    tr {
                                        td {
                                            @if forward.out_channel == *scid {
                                                span style="color: #48bb78;" { "→ Outbound" }
                                            } @else {
                                                span style="color: #63b3ed;" { "← Inbound" }
                                            }
                                        }
                                        td style="text-align: right;" {
                                            (forward.out_sat)
                                        }
                                        td style="text-align: right;" {
                                            @if forward.out_channel == *scid {
                                                (forward.fee_sat)
                                            } @else {
                                                (format!("({})", forward.fee_sat))
                                            }
                                        }
                                        td style="text-align: right;" {
                                            @if forward.out_channel == *scid {
                                                (forward.fee_ppm)
                                            } @else {
                                                (format!("({})", forward.fee_ppm))
                                            }
                                        }
                                        td {
                                            (forward.received_time.format("%Y-%m-%d %H:%M:%S").to_string())
                                        }
                                        td style="text-align: right;" {
                                            (format!("{:.1}", (forward.resolved_time - forward.received_time).num_seconds() as f64))
                                        }
                                    }
                                }
                            }
                        }

                        @if channel_forwards.len() > 100 {
                            p style="font-style: italic; color: #a0aec0;" {
                                "Showing latest 100 forwards out of " (channel_forwards.len()) " total."
                            }
                        }
                    }
                } @else {
                    div class="info-card" {
                        h2 { "Channel Forwards" }
                        p { "No forwards found for this channel." }
                    }
                }

                // Channel Local Failed Forwards Section
                @let channel_local_failed = store.get_channel_local_failed_forwards(scid);
                @if !channel_local_failed.is_empty() {
                    div class="info-card" {
                        h2 { "Channel Local Failed Forwards" }
                        p { "Total local failures: " (channel_local_failed.len()) }

                        table class="sortable" {
                            thead {
                                tr {
                                    th { "Direction" }
                                    th { "Other" }
                                    th { "Amount (sats)" }
                                    th { "Received Time" }
                                    th { "Fail Reason" }
                                    th { "Fail Code" }
                                }
                            }
                            tbody {
                                @for forward in channel_local_failed.iter().take(100) {
                                    tr {
                                        td {
                                            @if forward.out_channel.as_deref() == Some(scid) {
                                                span style="color: #48bb78;" { "→ Outbound" }
                                            } @else {
                                                span style="color: #63b3ed;" { "← Inbound" }
                                            }
                                        }
                                        td {
                                            @if forward.out_channel.as_deref() == Some(scid) {
                                                // Current channel is out_channel, show in_channel
                                                a href={(format!("../channels/{}.html", &forward.in_channel))} { (&forward.in_channel) }
                                            } @else {
                                                // Current channel is in_channel, show out_channel
                                                @if let Some(out_scid) = &forward.out_channel {
                                                    a href={(format!("../channels/{}.html", out_scid))} { (out_scid) }
                                                } @else {
                                                    "N/A"
                                                }
                                            }
                                        }
                                        td style="text-align: right;" {
                                            (forward.in_msat / 1000)
                                        }
                                        td {
                                            @if let Some(dt) = chrono::DateTime::from_timestamp(forward.received_time as i64, 0) {
                                                (dt.format("%Y-%m-%d %H:%M:%S").to_string())
                                            } @else {
                                                "N/A"
                                            }
                                        }
                                        td {
                                            @if let Some(reason) = &forward.failreason {
                                                (reason)
                                            } @else {
                                                "N/A"
                                            }
                                        }
                                        td style="text-align: right;" {
                                            @if let Some(code) = forward.failcode {
                                                (code)
                                            } @else {
                                                "N/A"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        @if channel_local_failed.len() > 100 {
                            p style="font-style: italic; color: #a0aec0;" {
                                "Showing latest 100 local failures out of " (channel_local_failed.len()) " total."
                            }
                        }
                    }
                } @else {
                    div class="info-card" {
                        h2 { "Channel Local Failed Forwards" }
                        p { "No local failures found for this channel." }
                    }
                }

                // Channel Failed Forwards Section
                @let channel_failed = store.get_channel_failed_forwards(scid);
                @if !channel_failed.is_empty() {
                    div class="info-card" {
                        h2 { "Channel Failed Forwards" }
                        p { "Total failures: " (channel_failed.len()) }

                        table class="sortable" {
                            thead {
                                tr {
                                    th { "Direction" }
                                    th { "Other" }
                                    th { "Amount (sats)" }
                                    th { "Received Time" }
                                }
                            }
                            tbody {
                                @for forward in channel_failed.iter().take(100) {
                                    tr {
                                        td {
                                            @if forward.out_channel.as_deref() == Some(scid) {
                                                span style="color: #48bb78;" { "→ Outbound" }
                                            } @else {
                                                span style="color: #63b3ed;" { "← Inbound" }
                                            }
                                        }
                                        td {
                                            @if forward.out_channel.as_deref() == Some(scid) {
                                                // Current channel is out_channel, show in_channel
                                                a href={(format!("../channels/{}.html", &forward.in_channel))} { (&forward.in_channel) }
                                            } @else {
                                                // Current channel is in_channel, show out_channel
                                                @if let Some(out_scid) = &forward.out_channel {
                                                    a href={(format!("../channels/{}.html", out_scid))} { (out_scid) }
                                                } @else {
                                                    "N/A"
                                                }
                                            }
                                        }
                                        td style="text-align: right;" {
                                            (forward.in_msat / 1000)
                                        }
                                        td {
                                            @if let Some(dt) = chrono::DateTime::from_timestamp(forward.received_time as i64, 0) {
                                                (dt.format("%Y-%m-%d %H:%M:%S").to_string())
                                            } @else {
                                                "N/A"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        @if channel_failed.len() > 100 {
                            p style="font-style: italic; color: #a0aec0;" {
                                "Showing latest 100 failures out of " (channel_failed.len()) " total."
                            }
                        }
                    }
                } @else {
                    div class="info-card" {
                        h2 { "Channel Failed Forwards" }
                        p { "No failures found for this channel." }
                    }
                }
            }
        };

        let channel_file_name = if let Some(scid) = &channel.short_channel_id {
            format!("{}.html", scid)
        } else {
            format!("{}.html", channel.channel_id)
        };

        let channel_html = wrap_in_html_page(
            &format!(
                "Channel {}",
                channel
                    .short_channel_id
                    .as_deref()
                    .unwrap_or(&channel.channel_id[..16])
            ),
            channel_content,
            &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        );
        let channel_file_path = format!("{}/{}", channels_dir, channel_file_name);

        match fs::write(&channel_file_path, channel_html.into_string()) {
            Ok(_) => log::debug!("Channel page generated: {}", channel_file_path),
            Err(e) => log::debug!("Error writing channel page {}: {}", channel_file_path, e),
        }
    }
}

fn create_apy_page(directory: &str, store: &Store, now: &chrono::DateTime<chrono::Utc>) {
    let apy_data = store.get_apy_data();

    let apy_content = html! {
        (create_page_header("APY Analysis", false))

        div class="info-card" {
            h2 { "Annual Percentage Yield (APY) Analysis" }

            div class="section" {
                h3 class="section-title" { "Fee Income Summary" }

                table {
                    thead {
                        tr {
                            th { "Period" }
                            th style="text-align: right;" { "Fees Earned (sats)" }
                            th style="text-align: right;" { "Projected Yearly APY %" }
                        }
                    }
                    tbody {
                        tr {
                            td { "Last 1 Month" }
                            td style="text-align: right;" { (apy_data.fees_1_month) }
                            td style="text-align: right;" { (format!("{:.3}", apy_data.apy_1_month)) }
                        }
                        tr {
                            td { "Last 3 Months" }
                            td style="text-align: right;" { (apy_data.fees_3_months) }
                            td style="text-align: right;" { (format!("{:.3}", apy_data.apy_3_months)) }
                        }
                        tr {
                            td { "Last 6 Months" }
                            td style="text-align: right;" { (apy_data.fees_6_months) }
                            td style="text-align: right;" { (format!("{:.3}", apy_data.apy_6_months)) }
                        }
                        tr {
                            td { "Last 12 Months" }
                            td style="text-align: right;" { (apy_data.fees_12_months) }
                            td style="text-align: right;" { (format!("{:.3}", apy_data.apy_12_months)) }
                        }
                    }
                }
            }

            div class="section" {
                h3 class="section-title" { "Fund Information" }

                div class="info-item" {
                    span class="label" { "Total Channel Funds: " }
                    span class="value" { (format!("{} sats", apy_data.total_funds)) }
                }

                div class="info-item" {
                    span class="label" { "Transacted Last Month: " }
                    span class="value" { (format!("{} sats", apy_data.transacted_last_month)) }
                }
            }

            div class="section" {
                h3 class="section-title" { "APY Methodology" }
                p {
                    "APY (Annual Percentage Yield) is calculated by taking the fees earned over a specific period, "
                    "annualizing them (multiplying by 12/months), and dividing by the total channel funds. "
                    "This gives a projected yearly return rate as a percentage."
                }
                p {
                    "Formula: APY% = (Fees Earned × 12 ÷ Period in Months × 100) ÷ Total Funds"
                }
            }
        }

        style {
            r#"
            .info-item {
                display: flex;
                margin: 10px 0;
                padding: 8px 0;
                border-bottom: 1px solid #4a5568;
            }
            
            .info-item:last-child {
                border-bottom: none;
            }

            .label {
                font-weight: bold;
                color: #63b3ed;
                min-width: 200px;
            }

            .value {
                color: #f8f8f2;
            }

            .section {
                margin: 25px 0;
            }

            .section:first-child {
                margin-top: 0;
            }

            .section p {
                margin: 10px 0;
                line-height: 1.6;
            }
            "#
        }
    };

    let apy_html = wrap_in_html_page(
        "APY Analysis",
        apy_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let apy_file_path = format!("{}/apy.html", directory);
    match fs::write(&apy_file_path, apy_html.into_string()) {
        Ok(_) => log::debug!("APY page generated: {}", apy_file_path),
        Err(e) => log::debug!("Error writing APY page: {}", e),
    }
}

fn create_closed_channels_page(
    directory: &str,
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
) {
    let closed_channels_info = store.get_closed_channels_info();

    // Calculate close cause counts
    let mut cause_counts = std::collections::HashMap::new();
    for channel_info in &closed_channels_info {
        *cause_counts
            .entry(&channel_info.channel.close_cause)
            .or_insert(0) += 1;
    }
    let mut causes: Vec<_> = cause_counts.into_iter().collect();
    causes.sort_by(|a, b| b.1.cmp(&a.1));

    let closed_channels_content = html! {
        (create_page_header("Closed Channels", false))

        div class="info-card" {
            h2 { "Closed Channels Analysis" }
            p { "Total closed channels: " (closed_channels_info.len()) }

            @if !closed_channels_info.is_empty() {
                table class="sortable" {
                    thead {
                        tr {
                            th { "Short Channel ID" }
                            th { "Peer Alias" }
                            th { "Peer ID" }
                            th { "Close Cause" }
                            th { "Opener" }
                            th { "Closer" }
                            th { "Funding / Closing" }
                            th style="text-align: right;" { "Opening Block" }
                            th style="text-align: right;" { "Final Amount (sats)" }
                            th style="text-align: right;" { "Total HTLCs Sent" }
                        }
                    }
                    tbody {
                        @for channel_info in &closed_channels_info {
                            tr {
                                td { (channel_info.channel.short_channel_id_display()) }
                                td {
                                    @if let Some(peer_id) = &channel_info.channel.peer_id {
                                        a href={(format!("../nodes/{}.html", peer_id))} {
                                            (channel_info.alias)
                                        }
                                    } @else {
                                        (channel_info.alias)
                                    }
                                }
                                td {
                                    @if let Some(peer_id) = &channel_info.channel.peer_id {
                                        a href={(format!("https://mempool.space/lightning/node/{}", peer_id))} target="_blank" {
                                            (format!("{}...", &peer_id[..16]))
                                        }
                                    } @else {
                                        "N/A"
                                    }
                                }
                                td { (channel_info.channel.close_cause) }
                                td { (channel_info.channel.opener) }
                                td {
                                    @if let Some(closer) = &channel_info.channel.closer {
                                        (closer)
                                    } @else {
                                        "N/A"
                                    }
                                }
                                td {
                                    a href={(format!("https://fbbe.info/t/{}", channel_info.channel.funding_txid))}  {
                                        "F"
                                    }
                                    " / "
                                    @if let Some(last_commitment_txid) = &channel_info.channel.last_commitment_txid {
                                        a href={(format!("https://fbbe.info/t/{}", last_commitment_txid))} {
                                            "C"
                                        }
                                    } @else {
                                        "C"
                                    }
                                }
                                td style="text-align: right;" {
                                    @if let Some(block) = channel_info.opening_block {
                                        (block)
                                    } @else {
                                        "N/A"
                                    }
                                }
                                td style="text-align: right;" {
                                    (channel_info.channel.final_to_us_msat / 1000)
                                }
                                td style="text-align: right;" {
                                    @if let Some(htlcs_sent) = channel_info.channel.total_htlcs_sent {
                                        (htlcs_sent)
                                    } @else {
                                        "N/A"
                                    }
                                }
                            }
                        }
                    }
                }
            } @else {
                p { "No closed channels found." }
            }

            div class="section" {
                h3 class="section-title" { "Close Cause Summary" }
                @if !closed_channels_info.is_empty() {
                    table class="sortable" {
                        thead {
                            tr {
                                th { "Close Cause" }
                                th style="text-align: right;" { "Count" }
                            }
                        }
                        tbody {
                            @for (cause, count) in causes {
                                tr {
                                    td { (cause) }
                                    td style="text-align: right;" { (count) }
                                }
                            }
                        }
                    }
                } @else {
                    p { "No data available." }
                }
            }

        }

        style {
            r#"
            .info-item {
                display: flex;
                margin: 10px 0;
                padding: 8px 0;
                border-bottom: 1px solid #4a5568;
            }
            
            .info-item:last-child {
                border-bottom: none;
            }

            .label {
                font-weight: bold;
                color: #63b3ed;
                min-width: 200px;
            }

            .value {
                color: #f8f8f2;
            }

            .section {
                margin: 25px 0;
            }

            .section:first-child {
                margin-top: 0;
            }
            "#
        }
    };

    let closed_channels_html = wrap_in_html_page(
        "Closed Channels",
        closed_channels_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    let closed_channels_file_path = format!("{}/closed-channels.html", directory);
    match fs::write(
        &closed_channels_file_path,
        closed_channels_html.into_string(),
    ) {
        Ok(_) => log::debug!(
            "Closed channels page generated: {}",
            closed_channels_file_path
        ),
        Err(e) => log::debug!("Error writing closed channels page: {}", e),
    }
}

/// Run the Lightning Network dashboard generator
///
/// This function generates a comprehensive HTML dashboard for Lightning Network
/// channel management. It creates the following files and directories:
///
/// # Generated Structure
/// ```text
/// directory/
/// ├── index.html              # Main overview page with navigation links
/// ├── peers/
/// │   ├── index.html         # Peer directory listing with connection status
/// │   └── *.html             # Individual peer detail pages
/// └── channels/
///     ├── index.html         # Channel directory listing with balances
///     └── *.html             # Individual channel detail pages
/// ```
///
/// # Features
/// - **Main Dashboard**: Overview with node information and navigation
/// - **Peer Directory**: Individual pages for each connected peer
/// - **Channel Directory**: Individual pages for each owned channel
/// - **Navigation**: Consistent back/forward navigation between all pages
/// - **Real-time Data**: Live Lightning Network node information
///
/// # Parameters
/// * `directory` - The directory path where HTML files will be generated
/// * `min_channels` - Minimum number of channels a node must have to be included
///
/// # Panics
/// Panics if unable to create the output directory or write HTML files
pub fn run_dashboard(store: &Store, directory: String, min_channels: usize) {
    let now = Utc::now();
    log::debug!("{}", now);
    log::debug!("my id:{}", store.info.id);
    let current_block = store.info.blockheight;
    log::info!("Fetching normal channels and settled forwards");
    let normal_channels = store.normal_channels();
    let settled = store.settled_forwards();
    log::info!(
        "Fetched {} normal channels and {} settled forwards",
        normal_channels.len(),
        settled.len()
    );

    // Generate index.html content
    let index_content = html! {
        (create_page_header("Lightning Network Dashboard", false))

        div class="info-card" {
            h2 { "Node Information" }

            div class="info-item" {
                span class="label" { "Node ID: " }
                span class="value" { (store.info.id) }
            }

            div class="info-item" {
                span class="label" { "Block Height: " }
                span class="value" { (store.info.blockheight) }
            }

            div class="info-item" {
                span class="label" { "Onchain Balance: " }
                span class="value" { (format!("{:.8} BTC", store.onchain_balance_btc())) }
            }

        }

        div class="info-card" {
            h3 {
                a href="nodes/" { "Nodes" }
            }
            h3 {
                a href="channels/" {
                    (format!("{} Channels", normal_channels.len()))
                }
            }
            h3 {
                a href="forwards.html" {
                    (format!("{} Settled Forwards (Ever)", settled.len()))
                }
            }
            h3 {
                a href="forwards-week.html" {
                    (format!("{} Forwards (Last Week)", store.filter_settled_forwards_by_days(7).len()))
                }
            }
            h3 {
                a href="forwards-year.html" {
                    (format!("{} Forwards (Last Year)", store.filter_settled_forwards_by_days(365).len()))
                }
            }
            h3 {
                a href="weekday-chart.html" {
                    "📊 Forwards by Weekday"
                }
            }
            h3 {
                a href="apy.html" {
                    "📈 APY Analysis"
                }
            }
            h3 {
                a href="closed-channels.html" {
                    (format!("🔒 {} Closed Channels", store.closed_channels_len()))
                }
            }
        }
    };
    log::info!("Generated index HTML content");

    let mut output_content = String::new();
    output_content.push_str(&format!(
        "network channels:{} nodes:{} nodes:{}\n",
        store.channels_len(),
        store.nodes_len(),
        store.nodes_len(),
    ));

    log::debug!(
        "network channels:{} nodes:{} nodes:{}",
        store.channels_len(),
        store.nodes_len(),
        store.nodes_len(),
    );

    let mut chan_meta_per_node = HashMap::new();

    log::info!("Processing network channels metadata");
    for c in store.channels() {
        let meta: &mut ChannelFee = chan_meta_per_node.entry(&c.source).or_default();
        meta.count += 1;
        meta.fee_sum += c.fee_per_millionth;
        meta.fee_rates.insert(c.fee_per_millionth);
    }
    log::info!("Processed {} network channels", store.channels_len());

    let total_forwards = store.forwards_len();

    // let jobs = sling_jobsettings();
    let forwards_perc = (settled.len() as f64 / total_forwards as f64) * 100.0;

    output_content.push_str(&format!(
        "forwards: {}/{} {:.1}%\n",
        settled.len(),
        total_forwards,
        forwards_perc
    ));

    log::debug!(
        "forwards: {}/{} {:.1}%",
        settled.len(),
        total_forwards,
        forwards_perc
    );
    let mut last_year = 0f64;
    let mut last_month = 0f64;
    let mut last_week = 0f64;
    let mut first = now;

    let mut per_channel_ever_forwards: HashMap<String, u64> = HashMap::new();
    let mut per_channel_ever_fee_sat: HashMap<String, u64> = HashMap::new();
    let mut per_channel_ever_incoming_fee_sat: HashMap<String, i64> = HashMap::new();

    let mut per_channel_forwards_in: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out: HashMap<String, u64> = HashMap::new();

    let mut per_channel_forwards_in_last_month: HashMap<String, u64> = HashMap::new();
    let mut per_channel_forwards_out_last_month: HashMap<String, u64> = HashMap::new();

    let mut per_channel_last_forward_in: HashMap<String, DateTime<Utc>> = HashMap::new();
    let mut per_channel_last_forward_out: HashMap<String, DateTime<Utc>> = HashMap::new();

    log::info!(
        "Processing {} settled forwards to compute per-channel statistics",
        settled.len()
    );
    for s in settled.iter() {
        let d = s.resolved_time;
        first = first.min(d);
        let days_elapsed = now.signed_duration_since(d).num_days();
        *per_channel_forwards_in
            .entry(s.in_channel.to_string())
            .or_default() += 1;
        *per_channel_forwards_out
            .entry(s.out_channel.to_string())
            .or_default() += 1;

        // Track the most recent forward timestamp for each channel
        per_channel_last_forward_in
            .entry(s.in_channel.to_string())
            .and_modify(|existing| {
                if s.resolved_time > *existing {
                    *existing = s.resolved_time;
                }
            })
            .or_insert(s.resolved_time);

        per_channel_last_forward_out
            .entry(s.out_channel.to_string())
            .and_modify(|existing| {
                if s.resolved_time > *existing {
                    *existing = s.resolved_time;
                }
            })
            .or_insert(s.resolved_time);

        *per_channel_ever_forwards
            .entry(s.out_channel.to_string())
            .or_default() += 1;
        *per_channel_ever_fee_sat
            .entry(s.out_channel.to_string())
            .or_default() += s.fee_sat;
        *per_channel_ever_incoming_fee_sat
            .entry(s.in_channel.to_string())
            .or_default() -= s.fee_sat as i64;

        if days_elapsed < 365 {
            last_year += 1.0;
            if days_elapsed < 30 {
                last_month += 1.0;

                *per_channel_forwards_in_last_month
                    .entry(s.in_channel.to_string())
                    .or_default() += 1;
                *per_channel_forwards_out_last_month
                    .entry(s.out_channel.to_string())
                    .or_default() += 1;

                if days_elapsed < 7 {
                    last_week += 1.0;
                }
            }
        }
    }
    log::info!("Finished processing settled forwards");

    let el = now.signed_duration_since(first).num_days();
    output_content.push_str(&format!(
        "settled frequency ever:{:.2} year:{:.2} month:{:.2} week:{:.2}\n",
        settled.len() as f64 / el as f64,
        last_year / 365.0,
        last_month / 30.0,
        last_week / 7.0
    ));

    log::debug!(
        "settled frequency ever:{:.2} year:{:.2} month:{:.2} week:{:.2}",
        settled.len() as f64 / el as f64,
        last_year / 365.0,
        last_month / 30.0,
        last_week / 7.0
    );

    log::info!("Calculating network average fee");
    let mut sum_fee_rate = 0u128;
    let mut count = 0u128;
    for c in store.channels() {
        if c.base_fee_millisatoshi != 0 {
            continue;
        }
        if c.fee_per_millionth > 10000 {
            continue;
        }
        sum_fee_rate += c.fee_per_millionth as u128;
        count += 1;
    }
    let network_average = (sum_fee_rate / count) as u64;
    log::info!("Network average fee calculated: {} ppm", network_average);
    output_content.push_str(&format!(
        "network average fee: {network_average} per millionth {:.3}%\n",
        network_average as f64 / 10000.0
    ));

    log::debug!(
        "network average fee: {network_average} per millionth {:.3}% ",
        network_average as f64 / 10000.0
    );

    let zero_fees = normal_channels.iter().all(|c| {
        store
            .get_channel(&c.short_channel_id(), &store.info.id)
            .map(|e| e.base_fee_millisatoshi)
            .unwrap_or(1)
            == 0
    });
    output_content.push_str(&format!(
        "my channels: {} - zero base fees? {}\n",
        normal_channels.len(),
        zero_fees
    ));

    log::debug!(
        "my channels: {} - zero base fees? {}",
        normal_channels.len(),
        zero_fees
    );

    let mut lines = vec![];
    let mut sling_lines = vec![];

    let mut perces = vec![];

    let mut channels = vec![];

    // Compute ChannelMeta
    log::info!(
        "Computing ChannelMeta for {} normal channels",
        normal_channels.len()
    );
    for fund in normal_channels.iter() {
        let short_channel_id = fund.short_channel_id();

        let ever_forw_in = *per_channel_forwards_in
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_out = *per_channel_forwards_out
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forward_in_out = ever_forw_out + ever_forw_in;

        // 100% is sink, 0% is source
        let is_sink = if ever_forward_in_out == 0 {
            // Avoid resulting in NaN
            0.5
        } else {
            (ever_forw_out as f64) / (ever_forward_in_out as f64)
        };

        let last_month_forw_in = *per_channel_forwards_in_last_month
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let last_month_forw_out = *per_channel_forwards_out_last_month
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let last_month_forward_in_out = last_month_forw_out + last_month_forw_in;

        // 100% is sink, 0% is source
        let is_sink_last_month = if last_month_forward_in_out == 0 {
            // Avoid resulting in NaN
            0.5
        } else {
            (last_month_forw_out as f64) / (last_month_forward_in_out as f64)
        };

        let perc = fund.perc_float();
        let rebalance = if perc < 0.3 && is_sink_last_month >= 0.5 {
            Rebalance::PullIn
        } else if perc > 0.7 && is_sink_last_month <= 0.5 {
            Rebalance::PushOut
        } else {
            Rebalance::Nothing
        };

        let alias_or_id = store.get_node_alias(&fund.peer_id);

        let c = ChannelMeta {
            fund: fund.clone(),
            is_sink,
            rebalance,
            alias_or_id,
            is_sink_last_month,
            block_born: fund.block_born().unwrap_or(0),
        };
        channels.push(c);
    }
    log::info!("Finished computing ChannelMeta");

    let pull_in: Vec<_> = channels
        .iter()
        .filter(|e| e.rebalance == Rebalance::PullIn)
        .map(|e| e.fund.short_channel_id())
        .collect();
    let push_out: Vec<_> = channels
        .iter()
        .filter(|e| e.rebalance == Rebalance::PushOut)
        .map(|e| e.fund.short_channel_id())
        .collect();
    output_content.push_str(&format!(
        "pull_in:{} push_out:{}\n",
        pull_in.len(),
        push_out.len()
    ));

    log::debug!("pull_in:{} push_out:{}", pull_in.len(), push_out.len());

    log::info!("Processing channels for output and sling jobs");
    for channel in channels {
        let fund = &channel.fund;
        let perc = fund.perc();
        perces.push(fund.perc_float());
        let short_channel_id = fund.short_channel_id();
        let our = store.get_channel(&short_channel_id, &store.info.id);
        let our_fee = our
            .map(|e| e.fee_per_millionth.to_string())
            .unwrap_or("".to_string());
        let our_base_fee = our
            .map(|e| (e.base_fee_millisatoshi / 1000).to_string())
            .unwrap_or("".to_string());
        let our_min = our
            .map(|e| (e.htlc_minimum_msat / 1000).to_string())
            .unwrap_or("".to_string());
        let our_max = our
            .map(|e| (e.htlc_maximum_msat / 1000).to_string())
            .unwrap_or("".to_string());

        let amount = fund.amount_msat / 1000;

        let their = store.get_channel(&short_channel_id, &fund.peer_id);
        let their_fee = their
            .map(|e| e.fee_per_millionth.to_string())
            .unwrap_or("".to_string());
        let their_base_fee = their
            .map(|e| (e.base_fee_millisatoshi / 1000).to_string())
            .unwrap_or("".to_string());
        let min_max = format!("{our_min}/{our_max}");

        let last_timestamp = store
            .nodes
            .nodes
            .iter()
            .find(|n| n.nodeid == fund.peer_id)
            .map(|e| DateTime::from_timestamp(e.last_timestamp.unwrap_or(0) as i64, 0).unwrap())
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        let last_timestamp_delta = cut_days(now.signed_duration_since(last_timestamp).num_days());

        let last_update = their
            .map(|e| DateTime::from_timestamp(e.last_update as i64, 0).unwrap())
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
        let last_update_delta = cut_days(now.signed_duration_since(last_update).num_days());
        let short_channel_id = fund.short_channel_id();

        let ever_forw = *per_channel_ever_forwards
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_fee = *per_channel_ever_fee_sat
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_fee_incom = *per_channel_ever_incoming_fee_sat
            .get(&short_channel_id)
            .unwrap_or(&0i64);

        let ever_forw_in_out = ever_forw_fee + ever_forw_fee_incom.abs() as u64;

        // gain is millisat "gained" per block, a millisat is gained is it an effective fee from outgoing forward, but also if it's an ineffective fee as incoming forward.
        let blocks_alive = current_block.saturating_sub(channel.block_born).max(1); // Prevent division by zero and overflow
        let gain = ((ever_forw_in_out as f64 / blocks_alive as f64) * 1000.0) as u64;

        let ever_forw_in = *per_channel_forwards_in
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forw_out = *per_channel_forwards_out
            .get(&short_channel_id)
            .unwrap_or(&0u64);

        let ever_forward_in_out = ever_forw_out + ever_forw_in;

        let is_sink_perc = channel.is_sink_perc();
        let is_sink_last_month_perc = channel.is_sink_last_month_perc();
        let alias_or_id = channel.alias_or_id();

        if let Some(l) = calc_slingjobs(
            short_channel_id.clone(),
            fund.perc_float(),
            ever_forward_in_out,
            &alias_or_id,
            &channel,
            &pull_in,
            &push_out,
        ) {
            sling_lines.push(l);
        }

        let push_pull = if push_out.contains(&short_channel_id) {
            "push"
        } else if pull_in.contains(&short_channel_id) {
            "pull"
        } else {
            ""
        };

        let s = format!(
            "{min_max:>12} {our_base_fee:1} {our_fee:>5} {short_channel_id:>15} {amount:8} {perc:>3}% {their_fee:>5} {their_base_fee:>3} {last_timestamp_delta:>3} {last_update_delta:>3} {ever_forw:>3} {ever_forw_fee:>5}s {ever_forw_fee_incom:>5}s {gain:>5}g {is_sink_perc:>4} {is_sink_last_month_perc:>4}  {push_pull:4}  {alias_or_id}"
        );
        lines.push((perc, s));
    }
    log::info!("Finished processing channels for output");

    let sum_perces: f64 = perces.iter().sum();
    let mean_perces = sum_perces / perces.len() as f64;
    let quad_diff_perces: f64 = perces
        .iter()
        .map(|e| (mean_perces - e) * (mean_perces - e))
        .sum();
    let variance = quad_diff_perces / (perces.len() as f64 - 1.0);
    output_content.push_str(&format!(
        "mean_perces:{:.1} variance:{:.1}\n",
        mean_perces * 100.0,
        variance * 100.0
    ));

    log::debug!(
        "mean_perces:{:.1} variance:{:.1}",
        mean_perces * 100.0,
        variance * 100.0
    );

    lines.sort_by(|a, b| a.0.cmp(&b.0));
    output_content.push_str("min_max our_base_fee our_fee scid amount perc their_fee their_base_fee last_tstamp_delta last_upd_delta monthly_forw monthly_forw_fee is_sink push/pull alias_or_id\n");

    log::debug!("min_max our_base_fee our_fee scid amount perc their_fee their_base_fee last_tstamp_delta last_upd_delta monthly_forw monthly_forw_fee is_sink push/pull alias_or_id");

    log::info!("Writing {} channel lines to debug output", lines.len());
    for (_, l1) in lines.iter() {
        output_content.push_str(&format!("{l1}\n"));
        log::debug!("{l1}");
    }

    // Display sling jobs without executing
    for (cmd, details) in sling_lines.iter() {
        output_content.push_str(&format!("`{cmd}` {details}\n"));
        log::debug!("`{cmd}` {details}");
    }
    log::info!("Finished writing debug output");

    // Generate HTML files after all output is collected
    log::info!("Wrapping index content in HTML page");
    let html_content = wrap_in_html_page(
        "Lightdash Dashboard",
        index_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );

    // Create the directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(&directory) {
        log::debug!("Error creating directory {}: {}", directory, e);
        return;
    }

    // Create the index.html file
    let html_file_path = format!("{}/index.html", directory);
    match fs::write(&html_file_path, html_content.into_string()) {
        Ok(_) => log::debug!("HTML dashboard generated: {}", html_file_path),
        Err(e) => log::debug!("Error writing HTML file: {}", e),
    }
    log::info!("Index HTML file written");

    log::info!("Creating channels directory and individual channel pages");
    create_channel_pages(
        &directory,
        &normal_channels,
        &store,
        &now,
        &store.info.id,
        &per_channel_last_forward_in,
        &per_channel_last_forward_out,
        &store.avail_map,
    );

    log::info!("Creating forwards page");
    create_forwards_page(&directory, &store, &now, &store.info.id);

    log::info!("Creating weekly forwards page");
    create_forwards_week_page(&directory, &store, &now, &store.info.id);

    log::info!("Creating yearly forwards page");
    create_forwards_year_page(&directory, &store, &now, &store.info.id);

    log::info!("Creating failures page");
    create_failures_page(&directory, &store, &now, &store.info.id);

    log::info!("Creating weekday chart page");
    create_weekday_chart_page(&directory, &store, &now);

    log::info!("Creating APY page");
    create_apy_page(&directory, &store, &now);

    log::info!("Creating closed channels page");
    create_closed_channels_page(&directory, &store, &now);

    log::info!("Creating node pages");
    create_node_pages(&directory, &store, &now, min_channels);

    log::info!("Dashboard generated successfully");
}
