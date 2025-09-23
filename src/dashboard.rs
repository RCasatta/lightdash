use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs;

use crate::{common::*, store::Store};
use maud::{html, Markup, DOCTYPE};

/// Create common HTML header with title
fn create_html_header(title: &str) -> Markup {
    html! {
        head {
            title { (title) }
            meta charset="utf-8";
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
                "#
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

fn create_peer_pages(directory: &str, store: &Store, now: &chrono::DateTime<chrono::Utc>) {
    let peers_dir = format!("{}/peers", directory);

    // Create peers directory
    if let Err(e) = fs::create_dir_all(&peers_dir) {
        log::debug!("Error creating peers directory {}: {}", peers_dir, e);
        return;
    }

    log::debug!("Creating peer pages in: {}", peers_dir);

    // Create peers index page
    let peers_index_content = html! {
        div class="header" {
            h1 { "Peers" }
            div class="back-link" {
                a href="../index.html" { "Home" }
            }
            p class="timestamp" { "Generated at: " (now.format("%Y-%m-%d %H:%M:%S UTC")) }
        }

        div class="info-card" {
            h2 { "Peer List" }
            p { "Total peers: " (store.peers_len()) }

            @for peer in store.peers() {
                div class="section" {
                    div class="info-item" {
                        span class="label" {
                            @if peer.connected {
                                "🟢 "
                            } @else {
                                "🔴 "
                            }
                            "Peer: "
                        }
                        span class="value" {
                            a href={(format!("{}.html", peer.id))} {
                                (store.get_node_alias(&peer.id))
                            }
                        }
                    }

                    div class="info-item" {
                        span class="label" { "ID: " }
                        span class="value" {
                            a href={(format!("https://mempool.space/lightning/node/{}", peer.id))} target="_blank" {
                                (&peer.id)
                            }
                        }
                    }

                    div class="info-item" {
                        span class="label" { "Channels: " }
                        span class="value" { (peer.num_channels) }
                    }

                    div class="info-item" {
                        span class="label" { "Connected: " }
                        span class="value" {
                            @if peer.connected {
                                "Yes"
                            } @else {
                                "No"
                            }
                        }
                    }
                }
            }
        }
    };

    let peers_index_html = wrap_in_html_page(
        "Peers Directory",
        peers_index_content,
        &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    );
    let peers_index_path = format!("{}/index.html", peers_dir);

    match fs::write(&peers_index_path, peers_index_html.into_string()) {
        Ok(_) => log::debug!("Peers index page generated: {}", peers_index_path),
        Err(e) => log::debug!("Error writing peers index page: {}", e),
    }

    for peer in store.peers() {
        let peer_content = html! {
            div class="header" {
                h1 { "Peer Details" }
                div class="back-link" {
                    a href="../index.html" { "Home" } " | " a href="index.html" { "Peers" }
                }
                p class="timestamp" { "Generated at: " (now.format("%Y-%m-%d %H:%M:%S UTC")) }
            }

            div class="info-card" {
                h2 { "Peer Information" }

                div class="info-item" {
                    span class="label" { "Peer ID: " }
                    span class="value" {
                        a href={(format!("https://mempool.space/lightning/node/{}", peer.id))} target="_blank" {
                            (peer.id)
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "Connected: " }
                    span class="value" {
                        @if peer.connected {
                            "Yes"
                        } @else {
                            "No"
                        }
                    }
                }

                div class="info-item" {
                    span class="label" { "Number of Channels: " }
                    span class="value" { (peer.num_channels) }
                }

                div class="info-item" {
                    span class="label" { "Features: " }
                    span class="value" { (peer.features) }
                }

                @if let Some(node_info) = store.nodes.nodes.iter().find(|n| n.nodeid == peer.id) {
                    div class="info-item" {
                        span class="label" { "Alias: " }
                        span class="value" { (node_info.alias.as_deref().unwrap_or("N/A")) }
                    }

                    @if let Some(last_timestamp) = node_info.last_timestamp {
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
            }

            @if !peer.channels.is_empty() {
                div class="info-card" {
                    h2 { "Channels" }
                    @for channel in &peer.channels {
                        div class="section" {
                            h3 { "Channel" }

                            div class="info-item" {
                                span class="label" { "State: " }
                                span class="value" { (channel.state) }
                            }

                            @if let Some(scid) = &channel.short_channel_id {
                                div class="info-item" {
                                    span class="label" { "Short Channel ID: " }
                                    span class="value" { (scid) }
                                }
                            }

                            @if let Some(direction) = channel.direction {
                                div class="info-item" {
                                    span class="label" { "Direction: " }
                                    span class="value" { (direction) }
                                }
                            }

                            @if let Some(channel_id) = &channel.channel_id {
                                div class="info-item" {
                                    span class="label" { "Channel ID: " }
                                    span class="value" { (channel_id) }
                                }
                            }

                            @if let Some(funding_txid) = &channel.funding_txid {
                                div class="info-item" {
                                    span class="label" { "Funding TXID: " }
                                    span class="value" { (funding_txid) }
                                }
                            }
                        }
                    }
                }
            }
        };

        let peer_html = wrap_in_html_page(
            &format!("Peer {}", &peer.id[..8]),
            peer_content,
            &now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        );
        let peer_file_path = format!("{}/{}.html", peers_dir, peer.id);

        match fs::write(&peer_file_path, peer_html.into_string()) {
            Ok(_) => log::debug!("Peer page generated: {}", peer_file_path),
            Err(e) => log::debug!("Error writing peer page {}: {}", peer_file_path, e),
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
        div class="header" {
            h1 { "Forwards by Weekday" }
            div class="back-link" {
                a href="index.html" { "Home" }
            }
            p class="timestamp" { "Generated at: " (now.format("%Y-%m-%d %H:%M:%S UTC")) }
        }

        div class="content" {
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
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
) -> Markup {
    // Helper function to get node alias for a channel
    let get_channel_alias = |channel_id: &str| -> String {
        let channel_id_string = channel_id.to_string();

        // Try to find the channel in our channels (we're the source)
        if let Some(channel) = store.get_channel(&channel_id_string, our_node_id) {
            // We're the source, so the destination is the remote node
            return store.get_node_alias(&channel.destination);
        }

        // If we can't find the channel, return the original channel ID
        channel_id.to_string()
    };

    html! {
        div class="header" {
            h1 { (title) }
            div class="back-link" {
                a href="index.html" { "Home" }
            }
            p class="timestamp" { "Generated at: " (now.format("%Y-%m-%d %H:%M:%S UTC")) }
        }

        div class="content" {
            h2 { "Settled Forward Payments" }
            p { "Total settled forwards: " (forwards.len()) }

            @if !forwards.is_empty() {
                table {
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
                            tr {
                                td { (get_channel_alias(&forward.in_channel)) }
                                td { (get_channel_alias(&forward.out_channel)) }
                                td class="align-right" {
                                    (format!("{:.1}", forward.fee_sat as f64))
                                }
                                td class="align-right" {
                                    (format!("{:.1}", forward.out_sat as f64))
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

    let forwards_content = create_forwards_html_content(
        &settled_forwards,
        "Settled Forwards",
        store,
        now,
        our_node_id,
    );

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
        now,
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
        now,
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

fn create_channel_pages(
    directory: &str,
    channels: &[crate::cmd::Fund],
    store: &Store,
    now: &chrono::DateTime<chrono::Utc>,
    our_node_id: &String,
) {
    let channels_dir = format!("{}/channels", directory);

    // Create channels directory
    if let Err(e) = fs::create_dir_all(&channels_dir) {
        log::debug!("Error creating channels directory {}: {}", channels_dir, e);
        return;
    }

    log::debug!("Creating channel pages in: {}", channels_dir);

    // Create channels index page
    let channels_index_content = html! {
        div class="header" {
            h1 { "Channels" }
            div class="back-link" {
                a href="../index.html" { "Home" }
            }
            p class="timestamp" { "Generated at: " (now.format("%Y-%m-%d %H:%M:%S UTC")) }
        }

        div class="info-card" {
            h2 { "Channel List" }
            p { "Total channels: " (channels.len()) }

            @for channel in channels {
                div class="section" {
                    div class="info-item" {
                        span class="label" { "Channel: " }
                        span class="value" {
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
                    }

                    div class="info-item" {
                        span class="label" { "Peer: " }
                        span class="value" {
                            (store.get_node_alias(&channel.peer_id))
                        }
                    }

                    div class="info-item" {
                        span class="label" { "State: " }
                        span class="value" { (channel.state) }
                    }

                    div class="info-item" {
                        span class="label" { "Balance: " }
                        span class="value" {
                            (format!("{:.1}%", channel.perc_float() * 100.0))
                        }
                    }

                    div class="info-item" {
                        span class="label" { "Amount: " }
                        span class="value" {
                            (format!("{} sats", channel.amount_msat / 1000))
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
            div class="header" {
                h1 { "Channel" }
                div class="back-link" {
                    a href="../index.html" { "Home" } " | " a href="index.html" { "Channels" }
                }
                p class="timestamp" { "Generated at: " (now.format("%Y-%m-%d %H:%M:%S UTC")) }
            }

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
                            span class="value" { (format!("{} msat", channel_info.base_fee_millisatoshi)) }
                        }

                        div class="info-item" {
                            span class="label" { "Min HTLC: " }
                            span class="value" { (format!("{} msat", channel_info.htlc_minimum_msat)) }
                        }

                        div class="info-item" {
                            span class="label" { "Max HTLC: " }
                            span class="value" { (format!("{} msat", channel_info.htlc_maximum_msat)) }
                        }

                        div class="info-item" {
                            span class="label" { "Delay: " }
                            span class="value" { (format!("{} blocks", channel_info.delay)) }
                        }
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
///
/// # Panics
/// Panics if unable to create the output directory or write HTML files
pub fn run_dashboard(store: &Store, directory: String) {
    let now = Utc::now();
    log::debug!("{}", now);
    log::debug!("my id:{}", store.info.id);
    let current_block = store.info.blockheight;
    let normal_channels = store.normal_channels();
    let settled = store.settled_forwards();

    // Generate index.html content
    let index_content = html! {
        div class="header" {
            h1 { "Lightning Network Dashboard" }
        }

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

        }

        div class="info-card" {
            h3 {
                a href="peers/" {
                    (format!("{} Peers", store.peers_len()))
                }
            }
            h3 {
                a href="channels/" {
                    (format!("{} Channels", normal_channels.len()))
                }
            }
            h3 {
                a href="forwards.html" {
                    (format!("{} Settled Forwards", settled.len()))
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
        }
    };

    let mut output_content = String::new();
    output_content.push_str(&format!(
        "network channels:{} nodes:{} peers:{}\n",
        store.channels_len(),
        store.nodes_len(),
        store.peers_len(),
    ));

    log::debug!(
        "network channels:{} nodes:{} peers:{}",
        store.channels_len(),
        store.nodes_len(),
        store.peers_len(),
    );

    let mut chan_meta_per_node = HashMap::new();

    for c in store.channels() {
        let meta: &mut ChannelFee = chan_meta_per_node.entry(&c.source).or_default();
        meta.count += 1;
        meta.fee_sum += c.fee_per_millionth;
        meta.fee_rates.insert(c.fee_per_millionth);
    }

    let total_forwards = store.forwards_len();
    let settled_24h = store.filter_settled_forwards_by_hours(24);

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

        let (_new_fee, cmd) = calc_setchannel(
            &short_channel_id,
            &channel.alias_or_id(),
            &fund,
            our.as_ref(),
            &settled_24h,
        );

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
        let gain = ((ever_forw_in_out as f64 / (current_block - channel.block_born) as f64)
            * 1000.0) as u64;

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
        lines.push((perc, s, cmd));
    }

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

    for (_, l1, _) in lines.iter() {
        output_content.push_str(&format!("{l1}\n"));
        log::debug!("{l1}");
    }

    for (_, _, l2) in lines {
        if let Some(l) = l2 {
            output_content.push_str(&format!("{l}\n"));
            log::debug!("{l}");
        }
    }

    // Display sling jobs without executing
    for (cmd, details) in sling_lines.iter() {
        output_content.push_str(&format!("`{cmd}` {details}\n"));
        log::debug!("`{cmd}` {details}");
    }

    // Generate HTML files after all output is collected
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

    // Create peers directory and individual peer pages
    create_peer_pages(&directory, &store, &now);

    // Create channels directory and individual channel pages
    create_channel_pages(&directory, &normal_channels, &store, &now, &store.info.id);

    // Create forwards page
    create_forwards_page(&directory, &store, &now, &store.info.id);

    // Create weekly forwards page
    create_forwards_week_page(&directory, &store, &now, &store.info.id);

    // Create yearly forwards page
    create_forwards_year_page(&directory, &store, &now, &store.info.id);

    // Create weekday chart page
    create_weekday_chart_page(&directory, &store, &now);
}
