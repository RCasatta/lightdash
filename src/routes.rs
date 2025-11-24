use chrono::Utc;
use maud::{html, Markup, DOCTYPE};
use std::collections::HashMap;
use std::fs;

use crate::cmd::*;
use crate::store::Store;

pub fn run_routes(store: &Store, directory: &str, amount_msat: u64) {
    let chan_meta = store.chan_meta_per_node();
    let peers_ids = store.peers_ids();
    let nodes_by_id_keys = store.node_ids_with_aliases();

    let mut counters = HashMap::new();
    let mut hop_sum = 0usize;
    let mut total = 0;

    for id in &nodes_by_id_keys {
        // Skip nodes that have less than 2 channels
        if chan_meta
            .get(id.as_str())
            .map_or(true, |chan_info| chan_info.count < 2)
        {
            continue;
        }
        if let Some(route) = get_route(id, amount_msat) {
            let mut nodes = route.route;
            hop_sum += nodes.len();
            total += 1;
            nodes.pop(); // remove the random destination
            for n in nodes.iter() {
                if !peers_ids.contains(&n.id) {
                    *counters.entry(n.id.to_string()).or_insert(0u64) += 1;
                }
            }
        }
    }
    let mut counters_vec: Vec<_> = counters.into_iter().filter(|e| e.1 > 2).collect();
    counters_vec.sort_by(|a, b| b.1.cmp(&a.1));

    let route_entries: Vec<RouteEntry> = counters_vec
        .into_iter()
        .filter_map(|(id, count)| {
            let chan_info = chan_meta.get(id.as_str())?;
            Some(RouteEntry {
                node_id: id.clone(),
                alias: store.get_node_alias(&id),
                appearances: count,
                avg_fee: chan_info.avg_fee(),
                fee_diversity: chan_info.fee_diversity(),
                channel_count: chan_info.count,
            })
        })
        .collect();

    let average_hops = if total == 0 {
        0.0
    } else {
        hop_sum as f64 / total as f64
    };

    let summary = RoutesSummary {
        scanned_nodes: nodes_by_id_keys.len(),
        evaluated_routes: total,
        candidate_nodes: route_entries.len(),
        average_hops,
    };

    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let routes_html = render_routes_page(&route_entries, &summary, &timestamp, amount_msat);

    if let Err(e) = fs::create_dir_all(directory) {
        log::error!("Error creating directory {}: {}", directory, e);
        return;
    }

    let amount_sat = amount_msat / 1000;
    let routes_file_path = format!("{}/routes-{}.html", directory, amount_sat);

    match fs::write(&routes_file_path, routes_html.into_string()) {
        Ok(_) => {
            log::info!("Routes page generated: {}", routes_file_path);
        }
        Err(e) => log::error!("Error writing routes page: {}", e),
    }
}

struct RouteEntry {
    node_id: String,
    alias: String,
    appearances: u64,
    avg_fee: f64,
    fee_diversity: f64,
    channel_count: u64,
}

struct RoutesSummary {
    scanned_nodes: usize,
    evaluated_routes: usize,
    candidate_nodes: usize,
    average_hops: f64,
}

fn render_routes_page(
    entries: &[RouteEntry],
    summary: &RoutesSummary,
    timestamp: &str,
    amount_msat: u64,
) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                title { "Routing Insights" }
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
                        white-space: pre-wrap;
                    }
                    section {
                        background-color: #2d3748;
                        padding: 20px;
                        border-radius: 8px;
                        margin-bottom: 20px;
                    }
                    a {
                        color: #63b3ed;
                        text-decoration: none;
                    }
                    a:hover {
                        text-decoration: underline;
                    }
                    section h2 {
                        color: #63b3ed;
                        margin-top: 0;
                    }
                    section p {
                        color: #a0aec0;
                        margin: 10px 0;
                    }
                    .back-link {
                        display: inline-block;
                        margin-top: 10px;
                        color: #63b3ed;
                    }
                    table {
                        width: 100%;
                        border-collapse: collapse;
                        margin-top: 10px;
                    }
                    th, td {
                        border: 1px solid #4a5568;
                        padding: 8px 12px;
                        text-align: left;
                    }
                    th {
                        background-color: #2d3748;
                        color: #63b3ed;
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
                    .align-right {
                        text-align: right;
                    }
                    footer {
                        text-align: center;
                        color: #a0aec0;
                        margin-top: 30px;
                    }
                    "#
                }
            }
            body {
                div class="container" {
                    div class="header" {
                        h1 {
                            "Routing Insights - "
                            (format!("{} sats", amount_msat / 1000))
                        }
                        div class="back-link" {
                            a href="index.html" { "Home" } " | "
                            a href="nodes/" { "Nodes" } " | "
                            a href="channels/" { "Channels" } " | "
                            a href="forwards-week.html" { "Forwards" } " | "
                            a href="routes-10000.html" { "Routes" } " | "
                            a href="failures.html" { "Failures" } " | "
                            a href="apy.html" { "APY" } " | "
                            a href="closed-channels.html" { "Closed" }
                        }
                    }

                    section {
                        h2 { "Route Amount Variants" }
                        p {
                            "Analysis performed for different payment amounts:"
                        }
                        ul {
                            li { a href="routes-1000.html" { "1,000 sats (0.00001 BTC)" } }
                            li { a href="routes-10000.html" { "10,000 sats (0.0001 BTC)" } }
                            li { a href="routes-100000.html" { "100,000 sats (0.001 BTC)" } }
                            li { a href="routes-1000000.html" { "1,000,000 sats (0.01 BTC)" } }
                        }
                    }

                    section {
                        h2 { "Random Route Coverage" }
                        p {
                            "Average hops per route: "
                            (format!("{:.2}", summary.average_hops))
                        }
                        p {
                            "Routes evaluated: " (summary.evaluated_routes)
                            " | Nodes scanned: " (summary.scanned_nodes)
                            " | Candidate relays: " (summary.candidate_nodes)
                        }
                        p {
                            "Nodes listed below appeared at least three times in random routes and are not currently direct peers."
                        }
                    }

                    section {
                        h2 { "Top Potential Relay Partners" }
                        @if entries.is_empty() {
                            p {
                                "No recurring third-party relay nodes detected. Try increasing the number of eligible nodes or ensure your node has sufficient channels."
                            }
                        } @else {
                            table {
                                thead {
                                    tr {
                                        th { "Rank" }
                                        th { "Alias" }
                                        th { "Appearances" }
                                        th { "Avg Fee (ppm)" }
                                        th { "Fee Diversity" }
                                        th { "Channels" }
                                    }
                                }
                                tbody {
                                    @for (idx, entry) in entries.iter().enumerate() {
                                        tr {
                                            td class="align-right" { (idx + 1) }
                                            td {
                                                a href={(format!("nodes/{}.html", entry.node_id))} { (&entry.alias) }
                                            }
                                            td class="align-right" { (entry.appearances) }
                                            td class="align-right" { (format!("{:.1}", entry.avg_fee)) }
                                            td class="align-right" { (format!("{:.3}", entry.fee_diversity)) }
                                            td class="align-right" { (entry.channel_count) }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    footer {
                        "Generated at: " (timestamp)
                    }
                }
            }
        }
    }
}
