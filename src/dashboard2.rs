use std::fs;
use std::path::{Component, Path, PathBuf};

use maud::{html, Markup, DOCTYPE};
use serde::de::DeserializeOwned;

use crate::snapshot::{ChannelSnapshot, SnapshotManifest, SummarySnapshot, SCHEMA_VERSION};

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
    let forwards_path = snapshot_file(snapshot_directory, &manifest.files.settled_forwards)?;
    let summary: SummarySnapshot = read_json(&summary_path, "snapshot summary")?;
    let channels: Vec<ChannelSnapshot> = read_json(&channels_path, "snapshot channels")?;

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
        &forwards_path,
        &data_directory.join("settled-forwards.jsonl"),
    )?;
    copy_file(
        &snapshot_directory.join("manifest.json"),
        &data_directory.join("manifest.json"),
    )?;
    for dataset_key in ["summary", "channels", "settled_forwards"] {
        let dataset = manifest
            .datasets
            .get(dataset_key)
            .ok_or_else(|| format!("snapshot manifest is missing dataset `{dataset_key}`"))?;
        let schema_source = snapshot_file(snapshot_directory, &dataset.schema_path)?;
        let schema_destination = snapshot_file(&data_directory, &dataset.schema_path)?;
        copy_file(&schema_source, &schema_destination)?;
    }

    let overview = render_overview_page(&manifest, &summary);
    write_file(&output_directory.join("index.html"), &overview)?;
    let channels_page = render_channels_page(&manifest, channels.len());
    write_file(&output_directory.join("channels.html"), &channels_page)?;
    let forwards_page = render_forwards_page(&manifest, summary.settled_forward_count);
    write_file(&output_directory.join("forwards.html"), &forwards_page)?;

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
    let gross_roic_12_months = summary
        .roic
        .periods
        .iter()
        .find(|period| period.months == 12)
        .map(|period| period.annualized_gross_roic_percent);
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
            (metric_card("Channel funds", &format!("{} sats", format_number(summary.channel_funds_sat)), "Capital currently deployed"))
            (metric_card("Current channels", &format_number(summary.current_channel_count), &format!("{} normal", format_number(summary.normal_channel_count))))
            (metric_card("Settled forwards", &format_number(summary.settled_forward_count), &format!("{} attempts recorded", format_number(summary.forward_attempt_count))))
            (metric_card("Forwarding fees", &format!("{} sats", format_number(summary.total_forwarding_fees_sat)), "All-time settled forwarding revenue"))
            (metric_card("Gross ROIC", &format_optional_percent(gross_roic_12_months), "Trailing 12 months"))
            (metric_card("Net ROIC", &format!("{:.2}%", summary.roic.net_roic_12_months_percent), "After trailing rebalance cost"))
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
                    dt { "Rebalance cost, 12 months" }
                    dd { (format!("{} sats", format_number(summary.roic.rebalance_cost_12_months_msat / 1000))) }
                }
            }
        }
    };
    page_shell("Overview", "overview", manifest, content)
}

fn render_channels_page(manifest: &SnapshotManifest, channel_count: usize) -> String {
    let content = html! {
        section class="hero compact-hero" {
            div {
                p class="eyebrow" { "Channels" }
                h1 { "One table, many views" }
                p class="hero-copy" {
                    "Filter, sort, and reshape " (format_number(channel_count))
                    " channel records without regenerating the site."
                }
            }
        }

        (dynamic_table_panel(
            "channels",
            "data/channels.json",
            "json",
            "Loading channel data…",
            false,
            html! {
                div class="preset-group" role="group" aria-label="Channel views" {
                    button type="button" class="preset-button" data-view="all" { "All" }
                    button type="button" class="preset-button" data-view="mature" { "Age 1y+" }
                    button type="button" class="preset-button" data-view="low-balance" { "Low balance" }
                    button type="button" class="preset-button" data-view="negative-roic" { "Negative ROIC" }
                    button type="button" class="preset-button" data-view="disconnected" { "Disconnected" }
                    button type="button" class="preset-button" data-view="no-forwards" { "No forwards" }
                }
            }
        ))
    };
    page_shell("Channels", "channels", manifest, content)
}

fn render_forwards_page(manifest: &SnapshotManifest, forward_count: usize) -> String {
    let content = html! {
        section class="hero compact-hero" {
            div {
                p class="eyebrow" { "Forwards" }
                h1 { "Settled forwarding history" }
                p class="hero-copy" {
                    "Explore " (format_number(forward_count))
                    " settled forwards with time presets. Only the current page is rendered."
                }
            }
        }

        (dynamic_table_panel(
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

fn dynamic_table_panel(
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
            data-table-root
            data-table-kind=(table_kind)
            data-source=(data_source)
            data-source-format=(data_format)
            data-paginated=(paginated) {
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
            result.push('\u{202f}');
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
        assert_eq!(format_number(1_234_567), "1\u{202f}234\u{202f}567");
        assert_eq!(format_number(-12_345), "-12\u{202f}345");
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
            onchain_balance_msat: 0,
            channel_funds_sat: 0,
            total_forwarding_fees_sat: 0,
            total_rebalance_cost_msat: 0,
            net_routing_revenue_msat: 0,
            roic: RoicSnapshot {
                periods: vec![RoicPeriodSnapshot {
                    months: 12,
                    forwarding_fees_sat: 0,
                    annualized_gross_roic_percent: 0.0,
                }],
                routed_12_months_sat: 0,
                capital_velocity_12_months: 0.0,
                effective_fee_rate_12_months_bps: 0.0,
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
        fs::write(snapshot.join("settled-forwards.jsonl"), b"").unwrap();
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
        assert!(output.join("forwards.html").is_file());
        assert!(output.join("assets/app.css").is_file());
        assert!(output.join("assets/app.js").is_file());
        assert_eq!(
            fs::read_to_string(output.join("data/channels.json")).unwrap(),
            "[]"
        );
        assert!(output.join("data/settled-forwards.jsonl").is_file());
        assert!(output.join("data/summary.schema.json").is_file());
        assert!(output.join("data/channels.schema.json").is_file());
        assert!(output.join("data/settled-forwards.schema.json").is_file());

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
