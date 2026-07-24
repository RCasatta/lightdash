use std::fs;
use std::path::{Component, Path, PathBuf};

use maud::{html, Markup, DOCTYPE};
use serde::de::DeserializeOwned;

use crate::snapshot::{SnapshotManifest, SummarySnapshot, SCHEMA_VERSION};

const APP_CSS: &str = include_str!("dashboard2.css");
const APP_JS: &str = include_str!("dashboard2.js");

pub fn run_dashboard2(snapshot_directory: &str, output_directory: &str) -> Result<(), String> {
    let snapshot_directory = Path::new(snapshot_directory);
    let output_directory = Path::new(output_directory);

    let manifest: SnapshotManifest = read_json(
        &snapshot_directory.join("manifest.json"),
        "snapshot manifest",
    )?;
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported snapshot schema version {}; dashboard2 supports version {SCHEMA_VERSION}",
            manifest.schema_version
        ));
    }

    let summary_path = snapshot_file(snapshot_directory, &manifest.files.summary)?;
    let channels_path = snapshot_file(snapshot_directory, &manifest.files.channels)?;
    let closed_channels_path = snapshot_file(snapshot_directory, &manifest.files.closed_channels)?;
    let forwards_path = snapshot_file(snapshot_directory, &manifest.files.settled_forwards)?;
    let rebalances_path = snapshot_file(snapshot_directory, &manifest.files.rebalances)?;
    let rebalance_status_path =
        snapshot_file(snapshot_directory, &manifest.files.rebalance_status)?;
    let summary: SummarySnapshot = read_json(&summary_path, "snapshot summary")?;

    let assets_directory = output_directory.join("assets");
    let data_directory = output_directory.join("data");
    fs::create_dir_all(&assets_directory)
        .map_err(|e| format!("creating `{}` failed: {e}", assets_directory.display()))?;
    fs::create_dir_all(&data_directory)
        .map_err(|e| format!("creating `{}` failed: {e}", data_directory.display()))?;

    write_file(&assets_directory.join("app.css"), APP_CSS)?;
    write_file(&assets_directory.join("app.js"), APP_JS)?;
    copy_file(&summary_path, &data_directory.join("summary.json"))?;
    copy_file(&channels_path, &data_directory.join("channels.json"))?;
    copy_file(
        &closed_channels_path,
        &data_directory.join("closed-channels.json"),
    )?;
    copy_file(
        &forwards_path,
        &data_directory.join("settled-forwards.jsonl"),
    )?;
    copy_file(&rebalances_path, &data_directory.join("rebalances.jsonl"))?;
    copy_file(
        &rebalance_status_path,
        &data_directory.join("rebalance-status.json"),
    )?;
    copy_file(
        &snapshot_directory.join("manifest.json"),
        &data_directory.join("manifest.json"),
    )?;
    for dataset_key in [
        "summary",
        "channels",
        "closed_channels",
        "settled_forwards",
        "rebalances",
        "rebalance_status",
    ] {
        let dataset = manifest
            .datasets
            .get(dataset_key)
            .ok_or_else(|| format!("snapshot manifest is missing dataset `{dataset_key}`"))?;
        let schema_source = snapshot_file(snapshot_directory, &dataset.schema_path)?;
        let schema_destination = snapshot_file(&data_directory, &dataset.schema_path)?;
        copy_file(&schema_source, &schema_destination)?;
    }
    for dataset_key in ["channel_policy_history", "channel_liquidity_history"] {
        let Some(dataset) = manifest.datasets.get(dataset_key) else {
            continue;
        };
        let data_source = snapshot_file(snapshot_directory, &dataset.path)?;
        let data_destination = snapshot_file(&data_directory, &dataset.path)?;
        let schema_source = snapshot_file(snapshot_directory, &dataset.schema_path)?;
        let schema_destination = snapshot_file(&data_directory, &dataset.schema_path)?;
        copy_file(&data_source, &data_destination)?;
        copy_file(&schema_source, &schema_destination)?;
    }

    let overview = render_overview_page(&manifest, &summary);
    write_file(&output_directory.join("index.html"), &overview)?;
    let channels_page = render_channels_page(&manifest);
    write_file(&output_directory.join("channels.html"), &channels_page)?;
    let channel_page = render_channel_page(&manifest);
    write_file(&output_directory.join("channel.html"), &channel_page)?;
    let forwards_page = render_forwards_page(&manifest);
    write_file(&output_directory.join("forwards.html"), &forwards_page)?;
    let rebalances_page = render_rebalances_page(&manifest);
    write_file(&output_directory.join("rebalances.html"), &rebalances_page)?;

    log::info!(
        "Dashboard2 generated successfully in {} from snapshot {}",
        output_directory.display(),
        snapshot_directory.display()
    );
    Ok(())
}

fn snapshot_file(directory: &Path, file_name: &str) -> Result<PathBuf, String> {
    let path = Path::new(file_name);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("unsafe snapshot file path `{file_name}`"));
    }
    Ok(directory.join(path))
}

fn read_json<T: DeserializeOwned>(path: &Path, description: &str) -> Result<T, String> {
    let content =
        fs::read_to_string(path).map_err(|e| format!("reading {description} failed: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("parsing {description} failed: {e}"))
}

fn copy_file(source: &Path, destination: &Path) -> Result<(), String> {
    fs::copy(source, destination).map_err(|e| {
        format!(
            "copying `{}` to `{}` failed: {e}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn write_file(path: &Path, content: &str) -> Result<(), String> {
    fs::write(path, content).map_err(|e| format!("writing `{}` failed: {e}", path.display()))
}

fn render_overview_page(manifest: &SnapshotManifest, summary: &SummarySnapshot) -> String {
    let period_12_months = summary
        .roic
        .periods
        .iter()
        .find(|period| period.months == 12);
    let gross_roic_12_months = period_12_months.map(|period| period.annualized_gross_roic_percent);
    let content = html! {
        section class="hero" {
            div {
                p class="eyebrow" { "Overview" }
                h1 { "Lightning operations at a glance" }
                p class="hero-copy" {
                    "A compact view generated entirely from snapshot schema "
                    (manifest.schema_version) "."
                }
            }
            a class="primary-action" href="channels.html" { "Explore channels" }
        }

        section class="metric-grid" aria-label="Node summary" {
            (metric_card(
                "Local liquidity",
                &format_optional_percent(summary.channel_funds_percent_of_capacity),
                &format!(
                    "{} of {} sats normal-channel capacity",
                    format_number(summary.channel_funds_sat),
                    format_number(summary.normal_channel_capacity_sat),
                ),
            ))
            (metric_card(
                "On-chain balance",
                &format!("{} sats", format_number(summary.onchain_balance_msat / 1000)),
                "Spendable wallet outputs",
            ))
            (metric_card("Current channels", &format_number(summary.current_channel_count), &format!("{} normal", format_number(summary.normal_channel_count))))
            (metric_card("Settled forwards", &format_number(summary.settled_forward_count), &format!("{} attempts recorded", format_number(summary.forward_attempt_count))))
            (metric_card("Forwarding fees", &format!("{} sats", format_number(summary.total_forwarding_fees_sat)), "All-time settled forwarding revenue"))
            (metric_card("Gross ROIC", &format_optional_percent(gross_roic_12_months), "Forwarding + lease earnings, trailing 12 months"))
            (metric_card("Net ROIC", &format!("{:.2}%", summary.roic.net_roic_12_months_percent), "After lease and rebalance costs"))
        }

        section class="panel split-panel" {
            div {
                p class="eyebrow" { "Benchmarks" }
                h2 { "Liquidity and fee positioning" }
                p class="muted" {
                    "Current channel balance distribution and announced variable routing fees."
                }
            }
            dl class="detail-list" {
                div {
                    dt { "Channel balance target std dev" }
                    dd { (format!("{:.2} pp from 50%", summary.channel_balance_target_stddev_percentage_points)) }
                }
                div {
                    dt { "Network average fee" }
                    dd { (format_fee_ppm(summary.network_average_fee_ppm)) }
                }
                div {
                    dt { "Network median fee" }
                    dd { (format_fee_ppm(summary.network_median_fee_ppm)) }
                }
                div {
                    dt { "Node average fee" }
                    dd { (format_fee_ppm(summary.node_average_fee_ppm)) }
                }
                div {
                    dt { "Node median fee" }
                    dd { (format_fee_ppm(summary.node_median_fee_ppm)) }
                }
            }
        }

        section class="panel split-panel" {
            div {
                p class="eyebrow" { "Capital efficiency" }
                h2 { "ROIC decomposition" }
                p class="muted" {
                    "The snapshot keeps raw values numeric and applies presentation only in this site."
                }
            }
            dl class="detail-list" {
                div {
                    dt { "Average channel funds, 12 months" }
                    dd { (period_12_months.map(|period| format!("{} sats", format_number(period.average_channel_funds_sat.round() as u64))).unwrap_or_else(|| "—".to_string())) }
                }
                div {
                    dt { "Capital-history coverage" }
                    dd { (period_12_months.map(|period| format!("{:.1}%", period.capital_history_coverage_ratio * 100.0)).unwrap_or_else(|| "—".to_string())) }
                }
                div {
                    dt { "Routed, 12 months" }
                    dd { (format!("{} sats", format_number(summary.roic.routed_12_months_sat))) }
                }
                div {
                    dt { "Effective fee rate" }
                    dd { (format!("{:.2} bps", summary.roic.effective_fee_rate_12_months_bps)) }
                }
                div {
                    dt { "Capital velocity" }
                    dd { (format!("{:.2}×", summary.roic.capital_velocity_12_months)) }
                }
                div {
                    dt { "Lease earnings, 12 months" }
                    dd { (format!("{} sats", format_number(summary.roic.lease_fee_earnings_12_months_msat / 1000))) }
                }
                div {
                    dt { "Lease costs, 12 months" }
                    dd { (format!("{} sats", format_number(summary.roic.lease_fee_cost_12_months_msat / 1000))) }
                }
                div {
                    dt { "Rebalance cost, 12 months" }
                    dd { (format!("{} sats", format_number(summary.roic.rebalance_cost_12_months_msat / 1000))) }
                }
            }
        }
    };
    page_shell("Overview", "overview", manifest, content)
}

fn render_channels_page(manifest: &SnapshotManifest) -> String {
    let content = html! {
        (dynamic_table_panel(
            "Channels",
            "channels",
            "data/channels.json",
            "json",
            "Loading channel data…",
            false,
            html! {
                div class="channel-view-controls" {
                    nav class="dataset-switch" aria-label="Channel status" {
                        a href="channels.html" data-channel-view-link="open" { "Open" }
                        a href="channels.html?view=closed" data-channel-view-link="closed" { "Closed" }
                    }
                    div class="preset-group" role="group" aria-label="Open channel views" data-channel-view-presets="open" {
                        button type="button" class="preset-button" data-view="all" { "All" }
                        button type="button" class="preset-button" data-view="mature" { "Age 1y+" }
                        button type="button" class="preset-button" data-view="low-balance" { "Low balance" }
                        button type="button" class="preset-button" data-view="negative-capacity-return" { "Negative capacity return" }
                        button type="button" class="preset-button" data-view="disconnected" { "Disconnected" }
                        button type="button" class="preset-button" data-view="no-forwards" { "No forwards" }
                    }
                    div class="preset-group" role="group" aria-label="Closed channel views" data-channel-view-presets="closed" hidden {
                        button type="button" class="preset-button" data-view="all" { "All" }
                        button type="button" class="preset-button" data-view="mature" { "Age 1y+" }
                        button type="button" class="preset-button" data-view="local-close" { "Closed locally" }
                        button type="button" class="preset-button" data-view="remote-close" { "Closed remotely" }
                        button type="button" class="preset-button" data-view="negative-capacity-return" { "Negative capacity return" }
                    }
                }
            }
        ))
    };
    page_shell("Channels", "channels", manifest, content)
}

fn render_channel_page(manifest: &SnapshotManifest) -> String {
    let content = html! {
        section data-channel-root {
            noscript {
                div class="error-banner" { "Channel details require JavaScript." }
            }
            div id="channel-error" class="error-banner" hidden {}
            div id="channel-content" hidden {
                div class="detail-heading" {
                    div {
                        p class="eyebrow" { "Channel detail" }
                        h1 id="channel-title" { "Channel" }
                        p id="channel-subtitle" class="muted monospace" {}
                    }
                    a class="secondary-link" href="channels.html" { "Back to channels" }
                }

                div id="channel-metrics" class="metric-grid channel-metrics" {}

                div class="detail-grid" {
                    section class="panel detail-panel" aria-labelledby="identity-title" {
                        h2 id="identity-title" { "Channel information" }
                        dl id="channel-identity" class="detail-list" {}
                    }
                    section class="panel detail-panel" aria-labelledby="policy-title" {
                        h2 id="policy-title" { "Current outbound policy" }
                        dl id="channel-policy" class="detail-list" {}
                    }
                    section class="panel detail-panel" aria-labelledby="activity-title" {
                        h2 id="activity-title" { "Routing and rebalancing" }
                        dl id="channel-activity" class="detail-list" {}
                    }
                }

                section class="panel detail-panel chart-section" {
                    h2 { "Channel history" }
                    p id="history-note" class="muted" { "Loading historical channel data…" }
                    div class="chart-grid" {
                        figure class="channel-chart" {
                            figcaption { "Liquidity ratio" }
                            div id="liquidity-chart" class="chart-host" {}
                        }
                        figure class="channel-chart" {
                            figcaption { "Proportional fee" }
                            div id="fee-chart" class="chart-host" {}
                        }
                        figure class="channel-chart" {
                            figcaption { "Maximum HTLC" }
                            div id="htlc-chart" class="chart-host" {}
                        }
                    }
                }

                (channel_activity_table("Settled forwards", "channel-forwards", "No settled forwards involve this channel."))
                (channel_activity_table("Rebalances", "channel-rebalances", "No rebalances involve this channel."))
            }
        }
    };
    page_shell("Channel", "channels", manifest, content)
}

fn channel_activity_table(title: &str, id: &str, empty_message: &str) -> Markup {
    html! {
        section class="panel detail-panel activity-table-panel" {
            h2 { (title) }
            p id={(format!("{id}-status"))} class="muted" { "Loading…" }
            div class="table-scroll" {
                table id=(id) class="data-table compact-table" {
                    thead {}
                    tbody {}
                }
            }
            p id={(format!("{id}-empty"))} class="muted" hidden { (empty_message) }
        }
    }
}

fn render_forwards_page(manifest: &SnapshotManifest) -> String {
    let content = html! {
        (dynamic_table_panel(
            "Forwards",
            "forwards",
            "data/settled-forwards.jsonl",
            "jsonl",
            "Streaming forward data…",
            true,
            html! {
                div class="preset-group" role="group" aria-label="Forward views" {
                    button type="button" class="preset-button" data-view="all" { "All" }
                    button type="button" class="preset-button" data-view="last-day" { "Last day" }
                    button type="button" class="preset-button" data-view="last-week" { "Last week" }
                    button type="button" class="preset-button" data-view="last-month" { "Last month" }
                    button type="button" class="preset-button" data-view="last-year" { "Last year" }
                }
            }
        ))
    };
    page_shell("Forwards", "forwards", manifest, content)
}

fn render_rebalances_page(manifest: &SnapshotManifest) -> String {
    let content = html! {
        div id="rebalance-summary" class="metric-grid channel-metrics" aria-live="polite" {}
        (dynamic_table_panel(
            "Rebalances",
            "rebalances",
            "data/rebalance-status.json",
            "json",
            "Loading rebalance data…",
            true,
            html! {
                div class="channel-view-controls" {
                    nav class="dataset-switch" aria-label="Rebalance dataset" {
                        a href="rebalances.html" data-rebalance-view-link="status" { "Latest status" }
                        a href="rebalances.html?view=history" data-rebalance-view-link="history" { "Successful parts" }
                    }
                    div class="preset-group" role="group" aria-label="Rebalance status views" data-rebalance-view-presets="status" {
                        button type="button" class="preset-button" data-view="all" { "All" }
                        button type="button" class="preset-button" data-view="balanced" { "Balanced" }
                        button type="button" class="preset-button" data-view="no-cheap-route" { "No cheap route" }
                    }
                    div class="preset-group" role="group" aria-label="Successful rebalance views" data-rebalance-view-presets="history" hidden {
                        button type="button" class="preset-button" data-view="all" { "All" }
                        button type="button" class="preset-button" data-view="last-month" { "Last month" }
                        button type="button" class="preset-button" data-view="last-year" { "Last year" }
                    }
                }
            }
        ))
    };
    page_shell("Rebalances", "rebalances", manifest, content)
}

fn dynamic_table_panel(
    page_title: &str,
    table_kind: &str,
    data_source: &str,
    data_format: &str,
    loading_label: &str,
    paginated: bool,
    presets: Markup,
) -> Markup {
    html! {
        section
            class="panel table-panel"
            aria-labelledby="table-page-title"
            data-table-root
            data-table-kind=(table_kind)
            data-source=(data_source)
            data-source-format=(data_format)
            data-paginated=(paginated) {
            h1 id="table-page-title" class="sr-only" { (page_title) }
            noscript {
                div class="error-banner" {
                    "This dynamic table requires JavaScript."
                }
            }
            div class="table-toolbar" {
                (presets)
                div class="toolbar-actions" {
                    label class="search-field" {
                        span class="sr-only" { "Search table" }
                        input id="table-search" type="search" placeholder="Search…" autocomplete="off";
                    }
                    details class="toolbar-menu" {
                        summary { "Filters" }
                        div id="filter-panel" class="filter-panel" {}
                    }
                    details class="toolbar-menu" {
                        summary { "Columns" }
                        div id="column-panel" class="column-panel" {}
                    }
                    button type="button" class="secondary-button" id="reset-table" { "Reset" }
                }
            }

            div class="table-status-row" {
                p id="table-status" aria-live="polite" { (loading_label) }
                div class="export-actions" {
                    button type="button" class="text-button" id="export-csv" { "Export CSV" }
                    button type="button" class="text-button" id="export-json" { "Export JSON" }
                }
            }

            div id="table-error" class="error-banner" hidden {}
            div class="table-scroll" {
                table id="data-table" class="data-table" {
                    thead {}
                    tbody {}
                }
            }
            @if paginated {
                div id="table-pagination" class="pagination" {
                    button type="button" class="secondary-button" id="previous-page" { "Previous" }
                    span id="page-status" { "Page 1" }
                    label {
                        "Rows "
                        select id="page-size" {
                            option value="50" { "50" }
                            option value="100" selected { "100" }
                            option value="250" { "250" }
                            option value="500" { "500" }
                        }
                    }
                    button type="button" class="secondary-button" id="next-page" { "Next" }
                }
            }
        }
    }
}

fn metric_card(label: &str, value: &str, note: &str) -> Markup {
    html! {
        article class="metric-card" {
            p class="metric-label" { (label) }
            p class="metric-value" { (value) }
            p class="metric-note" { (note) }
        }
    }
}

fn page_shell(
    title: &str,
    active_page: &str,
    manifest: &SnapshotManifest,
    content: Markup,
) -> String {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                meta name="color-scheme" content="dark";
                title { (title) " · Lightdash" }
                link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'%3E%3Ctext y='.9em' font-size='90'%3E%E2%9A%A1%3C/text%3E%3C/svg%3E";
                link rel="stylesheet" href="assets/app.css";
                script defer src="assets/app.js" {}
            }
            body data-snapshot-time=(manifest.generated_at) {
                header class="site-header" {
                    a class="brand" href="index.html" aria-label="Lightdash overview" {
                        span class="brand-mark" aria-hidden="true" { "⚡" }
                        span { "Lightdash" }
                        span class="version-pill" { "dashboard2" }
                    }
                    nav class="site-nav" aria-label="Primary navigation" {
                        a href="index.html" aria-current=(if active_page == "overview" { "page" } else { "false" }) { "Overview" }
                        a href="channels.html" aria-current=(if active_page == "channels" { "page" } else { "false" }) { "Channels" }
                        a href="forwards.html" aria-current=(if active_page == "forwards" { "page" } else { "false" }) { "Forwards" }
                        a href="rebalances.html" aria-current=(if active_page == "rebalances" { "page" } else { "false" }) { "Rebalances" }
                    }
                    div class="freshness" {
                        span { "Snapshot" }
                        time datetime=(manifest.generated_at) { (&manifest.generated_at) }
                    }
                }
                main class="site-main" {
                    (content)
                }
                footer class="site-footer" {
                    span { "Node " (abbreviate(&manifest.node_id)) }
                    span { "Block " (format_number(manifest.block_height)) }
                    span { "Schema v" (manifest.schema_version) }
                }
            }
        }
    }
    .into_string()
}

fn abbreviate(value: &str) -> String {
    if value.len() <= 20 {
        return value.to_string();
    }
    format!("{}…{}", &value[..10], &value[value.len() - 8..])
}

fn format_optional_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}%"))
        .unwrap_or_else(|| "—".to_string())
}

fn format_fee_ppm(value: f64) -> String {
    format!("{} ppm ({:.3}%)", value as u64, value / 10_000.0)
}

fn format_number<T: ToString>(value: T) -> String {
    let value = value.to_string();
    let (sign, digits) = value
        .strip_prefix('-')
        .map(|digits| ("-", digits))
        .unwrap_or(("", value.as_str()));
    let mut result = String::with_capacity(value.len() + value.len() / 3);
    result.push_str(sign);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            result.push(',');
        }
        result.push(character);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::snapshot::{
        RoicPeriodSnapshot, RoicSnapshot, SnapshotFiles, SnapshotManifest, SummarySnapshot,
        SCHEMA_VERSION,
    };
    use crate::snapshot_metadata::{build_dataset_metadata, DatasetCounts};

    use super::{format_number, run_dashboard2, snapshot_file};

    #[test]
    fn formats_grouped_numbers() {
        assert_eq!(format_number(1_234_567), "1,234,567");
        assert_eq!(format_number(-12_345), "-12,345");
    }

    #[test]
    fn snapshot_paths_cannot_escape_the_snapshot_directory() {
        assert!(snapshot_file(Path::new("/tmp/snapshot"), "channels.json").is_ok());
        assert!(snapshot_file(Path::new("/tmp/snapshot"), "../channels.json").is_err());
        assert!(snapshot_file(Path::new("/tmp/snapshot"), "/tmp/channels.json").is_err());
    }

    #[test]
    fn generates_site_from_snapshot_contract() {
        let root = temporary_test_directory();
        let snapshot = root.join("snapshot");
        let output = root.join("site");
        fs::create_dir_all(&snapshot).unwrap();

        let files = SnapshotFiles {
            summary: "summary.json".to_string(),
            channels: "channels.json".to_string(),
            closed_channels: "closed-channels.json".to_string(),
            settled_forwards: "settled-forwards.jsonl".to_string(),
            other_forwards: "other-forwards.jsonl".to_string(),
            rebalances: "rebalances.jsonl".to_string(),
            rebalance_status: "rebalance-status.json".to_string(),
            history_manifest: None,
        };
        let manifest = SnapshotManifest {
            schema_version: SCHEMA_VERSION,
            generated_at: "2026-07-16T10:00:00Z".to_string(),
            node_id: "02testnode".to_string(),
            block_height: 950_000,
            datasets: build_dataset_metadata(
                &files,
                DatasetCounts {
                    channels: 0,
                    closed_channels: 0,
                    settled_forwards: 0,
                    other_forwards: 0,
                    rebalances: 0,
                    rebalance_status: 0,
                },
            ),
            files,
        };
        let summary = SummarySnapshot {
            node_id: manifest.node_id.clone(),
            block_height: manifest.block_height,
            peer_count: 1,
            network_channel_count: 1,
            current_channel_count: 0,
            normal_channel_count: 0,
            closed_channel_count: 0,
            forward_attempt_count: 0,
            settled_forward_count: 0,
            onchain_balance_msat: 123_456_000,
            channel_funds_sat: 100,
            normal_channel_capacity_sat: 200,
            channel_funds_percent_of_capacity: Some(50.0),
            channel_balance_target_stddev_percentage_points: 38.83,
            network_average_fee_ppm: 728.0,
            network_median_fee_ppm: 220.0,
            node_average_fee_ppm: 748.0,
            node_median_fee_ppm: 388.0,
            total_forwarding_fees_sat: 0,
            total_rebalance_cost_msat: 0,
            net_routing_revenue_msat: 0,
            roic: RoicSnapshot {
                periods: vec![RoicPeriodSnapshot {
                    months: 12,
                    forwarding_fees_sat: 0,
                    lease_fee_earnings_msat: 0,
                    average_channel_funds_sat: 0.0,
                    capital_history_coverage_ratio: 0.0,
                    annualized_gross_roic_percent: 0.0,
                }],
                routed_12_months_sat: 0,
                capital_velocity_12_months: 0.0,
                effective_fee_rate_12_months_bps: 0.0,
                lease_fee_earnings_12_months_msat: 0,
                lease_fee_cost_12_months_msat: 0,
                rebalance_cost_12_months_msat: 0,
                net_roic_12_months_percent: 0.0,
            },
        };

        fs::write(
            snapshot.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        fs::write(
            snapshot.join("summary.json"),
            serde_json::to_vec(&summary).unwrap(),
        )
        .unwrap();
        fs::write(snapshot.join("channels.json"), b"[]").unwrap();
        fs::write(snapshot.join("closed-channels.json"), b"[]").unwrap();
        fs::write(snapshot.join("settled-forwards.jsonl"), b"").unwrap();
        fs::write(snapshot.join("rebalances.jsonl"), b"").unwrap();
        fs::write(snapshot.join("rebalance-status.json"), b"[]").unwrap();
        for dataset in manifest.datasets.values() {
            fs::write(
                snapshot.join(&dataset.schema_path),
                serde_json::to_vec(dataset).unwrap(),
            )
            .unwrap();
        }

        run_dashboard2(snapshot.to_str().unwrap(), output.to_str().unwrap()).unwrap();

        assert!(output.join("index.html").is_file());
        assert!(output.join("channels.html").is_file());
        assert!(output.join("channel.html").is_file());
        assert!(output.join("forwards.html").is_file());
        assert!(output.join("rebalances.html").is_file());
        assert!(output.join("assets/app.css").is_file());
        assert!(output.join("assets/app.js").is_file());
        assert_eq!(
            fs::read_to_string(output.join("data/channels.json")).unwrap(),
            "[]"
        );
        assert!(output.join("data/closed-channels.json").is_file());
        assert!(output.join("data/settled-forwards.jsonl").is_file());
        assert!(output.join("data/rebalances.jsonl").is_file());
        assert!(output.join("data/rebalance-status.json").is_file());
        assert!(output.join("data/summary.schema.json").is_file());
        assert!(output.join("data/channels.schema.json").is_file());
        assert!(output.join("data/closed-channels.schema.json").is_file());
        assert!(output.join("data/settled-forwards.schema.json").is_file());
        let overview = fs::read_to_string(output.join("index.html")).unwrap();
        assert!(overview.contains("Local liquidity"));
        assert!(overview.contains("50.00%"));
        assert!(overview.contains("100 of 200 sats normal-channel capacity"));
        assert!(overview.contains("On-chain balance"));
        assert!(overview.contains("123,456 sats"));
        assert!(overview.contains("38.83 pp from 50%"));
        assert!(overview.contains("728 ppm (0.073%)"));
        assert!(overview.contains("220 ppm (0.022%)"));
        assert!(overview.contains("748 ppm (0.075%)"));
        assert!(overview.contains("388 ppm (0.039%)"));

        fs::remove_dir_all(root).unwrap();
    }

    fn temporary_test_directory() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lightdash-dashboard2-test-{}-{nonce}",
            std::process::id()
        ))
    }
}
