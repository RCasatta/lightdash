use chrono::{DateTime, Duration, SecondsFormat, Utc};
use maud::{html, Markup, DOCTYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Component, Path};
use std::time::{Duration as StdDuration, Instant};

use crate::cmd::*;
use crate::common::format_sats;
use crate::snapshot_metadata::{DatasetMetadata, FieldMetadata};
use crate::store::Store;

const ROUTE_MAX_FEE_PPM: u64 = 10_000;
const ROUTE_MIN_MAX_FEE_MSAT: u64 = 5_000;
const ROUTES_SCHEMA_VERSION: u32 = 6;
const ROUTES_MAX_AGE_SECONDS: i64 = 24 * 60 * 60;
const ROUTE_AMOUNTS_SAT: [u64; 5] = [1_000, 10_000, 100_000, 1_000_000, 10_000_000];
const ROUTE_AMOUNT_BUDGET: StdDuration = StdDuration::from_secs(10 * 60);
const ROUTE_TOTAL_BUDGET: StdDuration = StdDuration::from_secs(55 * 60);
const ROUTE_PROGRESS_INTERVAL: usize = 100;
const ROUTE_SLOW_QUERY: StdDuration = StdDuration::from_secs(2);
pub(crate) const DEFAULT_PROCESSED_DIRECTORY: &str = "/var/lib/lightdash/routes/processed";
pub(crate) const SNAPSHOT_ROUTES_MANIFEST: &str = "routes-manifest.json";

pub(crate) struct ImportedRoutes {
    pub manifest_file: String,
    pub datasets: BTreeMap<String, DatasetMetadata>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct RoutesManifest {
    schema_version: u32,
    pub generated_at: String,
    node_id: String,
    source: RoutesSource,
    pub datasets: BTreeMap<String, DatasetMetadata>,
}

#[derive(Clone, Deserialize, Serialize)]
struct RoutesSource {
    amounts_sat: Vec<u64>,
    sample_seed_utc_day: u64,
    randomized_destination_order: bool,
    per_amount_budget_seconds: u64,
    total_budget_seconds: u64,
    single_path_endpoint_capacity_filter: bool,
    max_fee_ppm: u64,
    minimum_max_fee_msat: u64,
    layers: Vec<String>,
    final_cltv: u32,
    maxdelay: u32,
    maxparts: u32,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct RouteRun {
    pub amount_sat: u64,
    pub max_fee_msat: u64,
    pub scanned_nodes: usize,
    pub eligible_destinations: usize,
    pub processed_destinations: usize,
    pub queried_destinations: usize,
    pub capacity_filtered_destinations: usize,
    pub evaluated_routes: usize,
    pub failed_routes: usize,
    pub timed_out_routes: usize,
    pub budget_exhausted: bool,
    pub elapsed_seconds: f64,
    pub candidate_nodes: usize,
    pub recurring_candidate_nodes: usize,
    pub average_hops: f64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct RouteCandidate {
    pub amount_sat: u64,
    pub rank: usize,
    pub node_id: String,
    pub alias: String,
    pub connectable: bool,
    pub appearances: u64,
    pub appearance_ratio: Option<f64>,
    pub average_fee_ppm: f64,
    pub fee_diversity: f64,
    pub channel_count: u64,
}

struct RouteAnalysis {
    run: RouteRun,
    candidates: Vec<RouteCandidate>,
}

struct RouteAnalysisBatch {
    analyses: Vec<RouteAnalysis>,
    sample_seed_utc_day: u64,
}

#[derive(Deserialize, Serialize)]
struct RoutesExportBundle {
    manifest: RoutesManifest,
    files: BTreeMap<String, Value>,
}

pub fn run_routes(store: &Store, directory: &str) {
    for analysis in analyze_route_amounts(store).analyses {
        write_routes_page(directory, &analysis);
    }
}

fn write_routes_page(directory: &str, analysis: &RouteAnalysis) {
    let summary = RoutesSummary::from(&analysis.run);
    let route_entries: Vec<_> = analysis
        .candidates
        .iter()
        .filter(|candidate| candidate.appearances >= 3)
        .cloned()
        .collect();
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let amount_sat = analysis.run.amount_sat;
    let routes_html = render_routes_page(&route_entries, &summary, &timestamp, amount_sat * 1000);

    if let Err(e) = fs::create_dir_all(directory) {
        log::error!("Error creating directory {directory}: {e}");
        return;
    }

    let routes_file_path = format!("{directory}/routes-{amount_sat}.html");

    match fs::write(&routes_file_path, routes_html.into_string()) {
        Ok(_) => log::info!("Routes page generated: {routes_file_path}"),
        Err(e) => log::error!("Error writing routes page {routes_file_path}: {e}"),
    }
}

fn analyze_route_amounts(store: &Store) -> RouteAnalysisBatch {
    let started = Instant::now();
    let total_deadline = started + ROUTE_TOTAL_BUDGET;
    let sample_seed = Utc::now().timestamp().div_euclid(24 * 60 * 60) as u64;
    let mut analyses = Vec::with_capacity(ROUTE_AMOUNTS_SAT.len());

    for amount_sat in ROUTE_AMOUNTS_SAT {
        let amount_deadline = (Instant::now() + ROUTE_AMOUNT_BUDGET).min(total_deadline);
        analyses.push(analyze_routes(
            store,
            amount_sat,
            sample_seed,
            amount_deadline,
        ));
    }

    log::info!(
        "Route analysis completed all {} amounts in {:.1} minutes",
        analyses.len(),
        started.elapsed().as_secs_f64() / 60.0
    );
    RouteAnalysisBatch {
        analyses,
        sample_seed_utc_day: sample_seed,
    }
}

fn analyze_routes(
    store: &Store,
    amount_sat: u64,
    sample_seed: u64,
    deadline: Instant,
) -> RouteAnalysis {
    let started = Instant::now();
    let chan_meta = store.chan_meta_per_node();
    let peers_ids = store.peers_ids();
    let nodes_by_id_keys = store.node_ids_with_aliases();

    let mut counters = HashMap::new();
    let mut hop_sum = 0usize;
    let mut total = 0;
    let mut timed_out_routes = 0;
    let mut queried_destinations = 0;
    let mut capacity_filtered_destinations = 0;
    let amount_msat = amount_sat * 1000;
    let max_fee_msat = route_max_fee_msat(amount_msat);
    let local_single_path_capacity_msat = store
        .peer_channels
        .channels
        .iter()
        .filter(|channel| channel.state == "CHANNELD_NORMAL")
        .map(|channel| channel.spendable_msat.min(channel.maximum_htlc_out_msat))
        .max()
        .unwrap_or(0);
    let mut destination_inbound_capacity_msat: HashMap<&str, u64> = HashMap::new();
    for channel in store
        .channels
        .channels
        .iter()
        .filter(|channel| channel.active != Some(false))
    {
        let single_htlc_capacity = channel.amount_msat.min(channel.htlc_maximum_msat);
        destination_inbound_capacity_msat
            .entry(&channel.destination)
            .and_modify(|capacity| *capacity = (*capacity).max(single_htlc_capacity))
            .or_insert(single_htlc_capacity);
    }
    let mut eligible: Vec<_> = nodes_by_id_keys
        .iter()
        .filter(|id| {
            chan_meta
                .get(id.as_str())
                .is_some_and(|chan_info| chan_info.count >= 2)
        })
        .collect();
    eligible.sort_unstable_by_key(|id| destination_sample_score(id, sample_seed));
    let eligible_destinations = eligible.len();
    let mut processed_destinations = 0;

    log::info!(
        "Analyzing {amount_sat} sat routes: {eligible_destinations} eligible destinations in randomized daily order; maximum local single-path capacity {:.0} sats; budget {:.1} minutes",
        local_single_path_capacity_msat as f64 / 1_000.0,
        deadline.saturating_duration_since(Instant::now()).as_secs_f64() / 60.0
    );

    for id in eligible {
        if Instant::now() >= deadline {
            log::warn!(
                "Stopping {amount_sat} sat analysis after {processed_destinations}/{eligible_destinations} eligible destinations: amount time budget exhausted"
            );
            break;
        }
        processed_destinations += 1;
        let destination_capacity_msat = destination_inbound_capacity_msat
            .get(id.as_str())
            .copied()
            .unwrap_or(0);
        if local_single_path_capacity_msat < amount_msat || destination_capacity_msat < amount_msat
        {
            capacity_filtered_destinations += 1;
        } else {
            queried_destinations += 1;
            let query_started = Instant::now();
            let route_result = get_routes(&store.info.id, id, amount_msat, max_fee_msat);
            let query_elapsed = query_started.elapsed();
            if query_elapsed >= ROUTE_SLOW_QUERY {
                log::info!(
                    "Slow {amount_sat} sat route query {queried_destinations} (destination {processed_destinations}/{eligible_destinations}) to {id}: {:.2}s",
                    query_elapsed.as_secs_f64()
                );
            }
            let response = match route_result {
                Ok(GetRoutesOutcome::Found(response)) => Some(response),
                Ok(GetRoutesOutcome::TimedOut) => {
                    timed_out_routes += 1;
                    None
                }
                Ok(GetRoutesOutcome::NotFound) => None,
                Err(error) => {
                    log::warn!("Route query to {id} failed: {error}");
                    None
                }
            };
            if let Some(route) = response.and_then(|response| response.routes.into_iter().next()) {
                let mut nodes = route.path;
                hop_sum += nodes.len();
                total += 1;
                nodes.pop(); // remove the random destination
                for n in nodes.iter() {
                    let Some(node_id) = n.outgoing_node_id() else {
                        continue;
                    };
                    if !peers_ids.contains(node_id) {
                        *counters.entry(node_id.to_string()).or_insert(0u64) += 1;
                    }
                }
            }
        }
        if processed_destinations % ROUTE_PROGRESS_INTERVAL == 0
            || processed_destinations == eligible_destinations
        {
            let failed = queried_destinations - total;
            let average_query_seconds =
                started.elapsed().as_secs_f64() / queried_destinations.max(1) as f64;
            let remaining_destinations = eligible_destinations - processed_destinations;
            let estimated_remaining_seconds = average_query_seconds * remaining_destinations as f64;
            log::info!(
                "Route progress for {amount_sat} sats: {processed_destinations}/{eligible_destinations} eligible destinations processed, {queried_destinations} queried, {capacity_filtered_destinations} capacity-filtered, {total} routes found, {failed} failed, {timed_out_routes} timed out, {:.2}s/query average, {:.1}s estimated remaining, {:.1}s elapsed, {:.1}s budget left",
                average_query_seconds,
                estimated_remaining_seconds,
                started.elapsed().as_secs_f64(),
                deadline.saturating_duration_since(Instant::now()).as_secs_f64()
            );
        }
    }
    let mut counters_vec: Vec<_> = counters.into_iter().collect();
    counters_vec.sort_by(|a, b| b.1.cmp(&a.1));

    let mut candidates: Vec<RouteCandidate> = counters_vec
        .into_iter()
        .filter_map(|(id, count)| {
            let chan_info = chan_meta.get(id.as_str())?;
            Some(RouteCandidate {
                amount_sat,
                rank: 0,
                node_id: id.clone(),
                alias: store.get_node_alias(&id),
                connectable: store.is_node_connectable(&id),
                appearances: count,
                appearance_ratio: (total != 0).then_some(count as f64 / total as f64),
                average_fee_ppm: chan_info.avg_fee(),
                fee_diversity: chan_info.fee_diversity(),
                channel_count: chan_info.count,
            })
        })
        .collect();
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = index + 1;
    }

    let average_hops = if total == 0 {
        0.0
    } else {
        hop_sum as f64 / total as f64
    };

    RouteAnalysis {
        run: RouteRun {
            amount_sat,
            max_fee_msat,
            scanned_nodes: nodes_by_id_keys.len(),
            eligible_destinations,
            processed_destinations,
            queried_destinations,
            capacity_filtered_destinations,
            evaluated_routes: total,
            failed_routes: queried_destinations - total,
            timed_out_routes,
            budget_exhausted: processed_destinations < eligible_destinations,
            elapsed_seconds: started.elapsed().as_secs_f64(),
            candidate_nodes: candidates.len(),
            recurring_candidate_nodes: candidates
                .iter()
                .filter(|candidate| candidate.appearances >= 3)
                .count(),
            average_hops,
        },
        candidates,
    }
}

fn destination_sample_score(node_id: &str, seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64 ^ seed;
    for byte in node_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

struct RoutesSummary {
    scanned_nodes: usize,
    eligible_destinations: usize,
    processed_destinations: usize,
    queried_destinations: usize,
    capacity_filtered_destinations: usize,
    evaluated_routes: usize,
    failed_routes: usize,
    timed_out_routes: usize,
    budget_exhausted: bool,
    elapsed_seconds: f64,
    candidate_nodes: usize,
    average_hops: f64,
    max_fee_msat: u64,
}

impl From<&RouteRun> for RoutesSummary {
    fn from(run: &RouteRun) -> Self {
        Self {
            scanned_nodes: run.scanned_nodes,
            eligible_destinations: run.eligible_destinations,
            processed_destinations: run.processed_destinations,
            queried_destinations: run.queried_destinations,
            capacity_filtered_destinations: run.capacity_filtered_destinations,
            evaluated_routes: run.evaluated_routes,
            failed_routes: run.failed_routes,
            timed_out_routes: run.timed_out_routes,
            budget_exhausted: run.budget_exhausted,
            elapsed_seconds: run.elapsed_seconds,
            candidate_nodes: run.recurring_candidate_nodes,
            average_hops: run.average_hops,
            max_fee_msat: run.max_fee_msat,
        }
    }
}

fn route_max_fee_msat(amount_msat: u64) -> u64 {
    let proportional_fee = (amount_msat as u128 * ROUTE_MAX_FEE_PPM as u128 / 1_000_000) as u64;
    proportional_fee.max(ROUTE_MIN_MAX_FEE_MSAT)
}

pub fn run_cache_refresh(directory: &str) -> Result<(), String> {
    let store = Store::new(None);
    ensure_cached_routes(&store, Path::new(directory), true)?;
    Ok(())
}

pub fn run_export(directory: &str, refresh_if_stale: bool) -> Result<(), String> {
    let directory = Path::new(directory);
    if refresh_if_stale {
        let node_id = get_info().id;
        if !cached_routes_are_fresh(directory, &node_id) {
            let store = Store::new(None);
            ensure_cached_routes(&store, directory, false)?;
        }
    }

    let bundle = export_bundle(directory)?;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut writer, &bundle)
        .map_err(|e| format!("serializing routes export failed: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("flushing routes export failed: {e}"))
}

pub(crate) fn import_for_snapshot(
    store: &Store,
    snapshot_directory: &Path,
    configured_directory: Option<&str>,
) -> Result<ImportedRoutes, String> {
    let processed_directory = configured_directory.unwrap_or(DEFAULT_PROCESSED_DIRECTORY);
    if using_ssh() {
        log::info!("Fetching cached route analysis from remote node");
        let bytes = remote_command_output(
            "lightdash",
            &[
                "routes",
                "export",
                "--directory",
                processed_directory,
                "--refresh-if-stale",
            ],
        )?;
        let bundle: RoutesExportBundle = serde_json::from_slice(&bytes)
            .map_err(|e| format!("parsing remote routes export failed: {e}"))?;
        import_bundle(bundle, snapshot_directory, &store.info.id)
    } else {
        let processed_directory = Path::new(processed_directory);
        log::info!(
            "Ensuring cached route analysis in {}",
            processed_directory.display()
        );
        let manifest = ensure_cached_routes(store, processed_directory, false)?;
        import_directory(processed_directory, snapshot_directory, manifest)
    }
}

fn ensure_cached_routes(
    store: &Store,
    directory: &Path,
    force: bool,
) -> Result<RoutesManifest, String> {
    let existing = read_valid_manifest(directory, Some(&store.info.id)).ok();
    if !force
        && existing
            .as_ref()
            .is_some_and(|manifest| manifest_is_fresh(manifest, Utc::now()))
    {
        log::info!("Reusing route analysis less than 24 hours old");
        return Ok(existing.expect("fresh manifest exists"));
    }

    log::info!("Refreshing cached route analysis");
    match rebuild_cache(store, directory) {
        Ok(manifest) => Ok(manifest),
        Err(error) => {
            if let Some(existing) = existing {
                log::warn!(
                    "Refreshing route analysis failed; reusing cached result from {}: {error}",
                    existing.generated_at
                );
                Ok(existing)
            } else {
                Err(error)
            }
        }
    }
}

fn rebuild_cache(store: &Store, directory: &Path) -> Result<RoutesManifest, String> {
    let mut runs = Vec::new();
    let mut candidates = Vec::new();
    let batch = analyze_route_amounts(store);
    for analysis in batch.analyses {
        runs.push(analysis.run);
        candidates.extend(analysis.candidates);
    }
    if runs.iter().all(|run| run.evaluated_routes == 0) {
        return Err("route analysis did not find any routes for any probe amount".to_string());
    }

    fs::create_dir_all(directory).map_err(|e| {
        format!(
            "creating routes cache directory `{}` failed: {e}",
            directory.display()
        )
    })?;
    let generated_at = Utc::now();
    let generation = generated_at.format("%Y%m%dT%H%M%S%.fZ").to_string();
    let runs_file = format!("route-runs-{generation}.json");
    let runs_schema_file = format!("route-runs-{generation}.schema.json");
    let candidates_file = format!("route-candidates-{generation}.json");
    let candidates_schema_file = format!("route-candidates-{generation}.schema.json");
    let datasets = route_dataset_metadata(
        &runs_file,
        &runs_schema_file,
        &candidates_file,
        &candidates_schema_file,
        runs.len(),
        candidates.len(),
    );

    write_json_atomic(&directory.join(&runs_file), &runs)?;
    write_json_atomic(&directory.join(&candidates_file), &candidates)?;
    write_json_atomic(
        &directory.join(&runs_schema_file),
        datasets
            .get("route_runs")
            .expect("route runs metadata exists"),
    )?;
    write_json_atomic(
        &directory.join(&candidates_schema_file),
        datasets
            .get("route_candidates")
            .expect("route candidates metadata exists"),
    )?;

    let manifest = RoutesManifest {
        schema_version: ROUTES_SCHEMA_VERSION,
        generated_at: generated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        node_id: store.info.id.clone(),
        source: RoutesSource {
            amounts_sat: ROUTE_AMOUNTS_SAT.to_vec(),
            sample_seed_utc_day: batch.sample_seed_utc_day,
            randomized_destination_order: true,
            per_amount_budget_seconds: ROUTE_AMOUNT_BUDGET.as_secs(),
            total_budget_seconds: ROUTE_TOTAL_BUDGET.as_secs(),
            single_path_endpoint_capacity_filter: true,
            max_fee_ppm: ROUTE_MAX_FEE_PPM,
            minimum_max_fee_msat: ROUTE_MIN_MAX_FEE_MSAT,
            layers: vec!["auto.localchans".to_string(), "auto.sourcefree".to_string()],
            final_cltv: 9,
            maxdelay: 2016,
            maxparts: 1,
        },
        datasets,
    };
    write_json_atomic(&directory.join("manifest.json"), &manifest)?;
    log::info!(
        "Cached {} route runs and {} candidates in {}",
        runs.len(),
        candidates.len(),
        directory.display()
    );
    Ok(manifest)
}

fn cached_routes_are_fresh(directory: &Path, expected_node_id: &str) -> bool {
    read_valid_manifest(directory, Some(expected_node_id))
        .is_ok_and(|manifest| manifest_is_fresh(&manifest, Utc::now()))
}

fn manifest_is_fresh(manifest: &RoutesManifest, now: DateTime<Utc>) -> bool {
    let Ok(generated_at) = DateTime::parse_from_rfc3339(&manifest.generated_at) else {
        return false;
    };
    let age = now.signed_duration_since(generated_at.with_timezone(&Utc));
    age >= Duration::zero() && age < Duration::seconds(ROUTES_MAX_AGE_SECONDS)
}

fn read_valid_manifest(
    directory: &Path,
    expected_node_id: Option<&str>,
) -> Result<RoutesManifest, String> {
    let path = directory.join("manifest.json");
    let bytes = fs::read(&path)
        .map_err(|e| format!("reading routes manifest `{}` failed: {e}", path.display()))?;
    let manifest: RoutesManifest = serde_json::from_slice(&bytes)
        .map_err(|e| format!("parsing routes manifest `{}` failed: {e}", path.display()))?;
    validate_manifest(&manifest, expected_node_id)?;
    for dataset in manifest.datasets.values() {
        for relative_path in [&dataset.path, &dataset.schema_path] {
            validate_relative_path(relative_path)?;
            if !directory.join(relative_path).is_file() {
                return Err(format!(
                    "routes cache is missing manifest file `{relative_path}`"
                ));
            }
        }
    }
    Ok(manifest)
}

fn validate_manifest(
    manifest: &RoutesManifest,
    expected_node_id: Option<&str>,
) -> Result<(), String> {
    if manifest.schema_version != ROUTES_SCHEMA_VERSION {
        return Err(format!(
            "unsupported routes schema version {}; expected {ROUTES_SCHEMA_VERSION}",
            manifest.schema_version
        ));
    }
    if let Some(expected_node_id) = expected_node_id {
        if manifest.node_id != expected_node_id {
            return Err(format!(
                "cached routes belong to node {}, but snapshot node is {expected_node_id}",
                manifest.node_id
            ));
        }
    }
    for required in ["route_runs", "route_candidates"] {
        if !manifest.datasets.contains_key(required) {
            return Err(format!("routes manifest is missing dataset `{required}`"));
        }
    }
    Ok(())
}

fn import_directory(
    processed_directory: &Path,
    snapshot_directory: &Path,
    mut manifest: RoutesManifest,
) -> Result<ImportedRoutes, String> {
    for dataset_key in ["route_runs", "route_candidates"] {
        let dataset = manifest
            .datasets
            .get_mut(dataset_key)
            .expect("validated routes dataset exists");
        let source_path = dataset.path.clone();
        validate_relative_path(&source_path)?;
        let (snapshot_path, snapshot_schema_path) = snapshot_dataset_paths(dataset_key);
        fs::copy(
            processed_directory.join(&source_path),
            snapshot_directory.join(snapshot_path),
        )
        .map_err(|e| format!("copying cached routes file `{source_path}` failed: {e}"))?;
        dataset.path = snapshot_path.to_string();
        dataset.schema_path = snapshot_schema_path.to_string();
        write_json(&snapshot_directory.join(snapshot_schema_path), dataset)?;
    }
    write_json(
        &snapshot_directory.join(SNAPSHOT_ROUTES_MANIFEST),
        &manifest,
    )?;
    Ok(ImportedRoutes {
        manifest_file: SNAPSHOT_ROUTES_MANIFEST.to_string(),
        datasets: manifest.datasets,
    })
}

fn import_bundle(
    mut bundle: RoutesExportBundle,
    snapshot_directory: &Path,
    expected_node_id: &str,
) -> Result<ImportedRoutes, String> {
    validate_manifest(&bundle.manifest, Some(expected_node_id))?;
    for dataset_key in ["route_runs", "route_candidates"] {
        let dataset = bundle
            .manifest
            .datasets
            .get_mut(dataset_key)
            .expect("validated routes dataset exists");
        let source_path = dataset.path.clone();
        validate_relative_path(&source_path)?;
        let value = bundle
            .files
            .get(&source_path)
            .ok_or_else(|| format!("routes export is missing `{source_path}`"))?;
        let (snapshot_path, snapshot_schema_path) = snapshot_dataset_paths(dataset_key);
        write_json(&snapshot_directory.join(snapshot_path), value)?;
        dataset.path = snapshot_path.to_string();
        dataset.schema_path = snapshot_schema_path.to_string();
        write_json(&snapshot_directory.join(snapshot_schema_path), dataset)?;
    }
    write_json(
        &snapshot_directory.join(SNAPSHOT_ROUTES_MANIFEST),
        &bundle.manifest,
    )?;
    Ok(ImportedRoutes {
        manifest_file: SNAPSHOT_ROUTES_MANIFEST.to_string(),
        datasets: bundle.manifest.datasets,
    })
}

fn snapshot_dataset_paths(dataset_key: &str) -> (&'static str, &'static str) {
    match dataset_key {
        "route_runs" => ("route-runs.json", "route-runs.schema.json"),
        "route_candidates" => ("route-candidates.json", "route-candidates.schema.json"),
        _ => unreachable!("validated route dataset key"),
    }
}

fn export_bundle(directory: &Path) -> Result<RoutesExportBundle, String> {
    let manifest = read_valid_manifest(directory, None)?;
    let mut files = BTreeMap::new();
    for dataset in manifest.datasets.values() {
        for relative_path in [&dataset.path, &dataset.schema_path] {
            let path = directory.join(relative_path);
            let bytes = fs::read(&path).map_err(|e| {
                format!(
                    "reading routes export file `{}` failed: {e}",
                    path.display()
                )
            })?;
            let value = serde_json::from_slice(&bytes).map_err(|e| {
                format!(
                    "parsing routes export file `{}` failed: {e}",
                    path.display()
                )
            })?;
            files.insert(relative_path.clone(), value);
        }
    }
    Ok(RoutesExportBundle { manifest, files })
}

fn validate_relative_path(value: &str) -> Result<(), String> {
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("unsafe routes artifact path `{value}`"));
    }
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let file = File::create(path)
        .map_err(|e| format!("creating routes artifact `{}` failed: {e}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .map_err(|e| format!("writing routes artifact `{}` failed: {e}", path.display()))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("finishing routes artifact `{}` failed: {e}", path.display()))?;
    writer
        .flush()
        .map_err(|e| format!("flushing routes artifact `{}` failed: {e}", path.display()))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("routes artifact path `{}` has no filename", path.display()))?;
    let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    write_json(&temporary, value)?;
    fs::rename(&temporary, path).map_err(|e| {
        format!(
            "replacing routes artifact `{}` with `{}` failed: {e}",
            path.display(),
            temporary.display()
        )
    })
}

fn route_dataset_metadata(
    runs_file: &str,
    runs_schema_file: &str,
    candidates_file: &str,
    candidates_schema_file: &str,
    run_count: usize,
    candidate_count: usize,
) -> BTreeMap<String, DatasetMetadata> {
    BTreeMap::from([
        (
            "route_runs".to_string(),
            DatasetMetadata {
                path: runs_file.to_string(),
                schema_path: runs_schema_file.to_string(),
                format: "json-array".to_string(),
                description: "Coverage and outcome summary for each amount used to probe single-part routes from the local node.".to_string(),
                record_count: run_count,
                primary_key: Some("amount_sat".to_string()),
                fields: route_run_fields(),
            },
        ),
        (
            "route_candidates".to_string(),
            DatasetMetadata {
                path: candidates_file.to_string(),
                schema_path: candidates_schema_file.to_string(),
                format: "json-array".to_string(),
                description: "Non-peer intermediary nodes appearing in route probes, ranked separately for each payment amount.".to_string(),
                record_count: candidate_count,
                primary_key: Some("amount_sat,node_id".to_string()),
                fields: route_candidate_fields(),
            },
        ),
    ])
}

fn metadata_field(
    json_type: &str,
    nullable: bool,
    unit: Option<&str>,
    description: &str,
    source: Option<&str>,
    formula: Option<&str>,
) -> FieldMetadata {
    FieldMetadata {
        json_type: json_type.to_string(),
        nullable,
        unit: unit.map(str::to_string),
        description: description.to_string(),
        formula: formula.map(str::to_string),
        source: source.map(str::to_string),
        aggregation: None,
        warning: None,
    }
}

fn route_run_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("amount_sat".into(), metadata_field("integer", false, Some("sat"), "Payment amount delivered to each probed destination.", None, None)),
        ("max_fee_msat".into(), metadata_field("integer", false, Some("msat"), "Maximum total route fee accepted for this probe amount.", None, Some("max(5000 msat, amount_sat * 1000 * 10000 / 1000000)"))),
        ("scanned_nodes".into(), metadata_field("integer", false, Some("node"), "Gossip nodes considered before destination eligibility filtering.", Some("listnodes"), None)),
        ("eligible_destinations".into(), metadata_field("integer", false, Some("node"), "Destinations with metadata for at least two public channels.", Some("listnodes and listchannels"), None)),
        ("processed_destinations".into(), metadata_field("integer", false, Some("node"), "Eligible destinations processed in deterministic daily randomized order before the per-amount time budget expired.", Some("listnodes and listchannels"), None)),
        ("queried_destinations".into(), metadata_field("integer", false, Some("node"), "Processed destinations passed to getroutes after known single-path endpoint capacity limits were checked.", Some("listpeerchannels, listchannels, and getroutes"), None)),
        ("capacity_filtered_destinations".into(), metadata_field("integer", false, Some("node"), "Processed destinations skipped because no single local outbound channel or advertised destination inbound channel could carry the full amount in one HTLC.", Some("listpeerchannels and listchannels"), None)),
        ("evaluated_routes".into(), metadata_field("integer", false, Some("route"), "Destinations for which getroutes returned a single-part route within the fee and delay budgets.", Some("getroutes"), None)),
        ("failed_routes".into(), metadata_field("integer", false, Some("route"), "Queried destinations for which no acceptable route was returned.", None, Some("queried_destinations - evaluated_routes"))),
        ("timed_out_routes".into(), metadata_field("integer", false, Some("route"), "Route queries for which getroutes reported that its internal Askrene deadline expired.", Some("getroutes"), None)),
        ("budget_exhausted".into(), metadata_field("boolean", false, None, "Whether the per-amount time budget expired before every eligible destination was processed.", None, None)),
        ("elapsed_seconds".into(), metadata_field("number", false, Some("second"), "Wall-clock duration of this amount's route analysis.", None, None)),
        ("candidate_nodes".into(), metadata_field("integer", false, Some("node"), "Distinct non-peer intermediary nodes appearing in at least one returned route.", None, None)),
        ("recurring_candidate_nodes".into(), metadata_field("integer", false, Some("node"), "Candidate nodes appearing in at least three returned routes.", None, None)),
        ("average_hops".into(), metadata_field("number", false, Some("hop_per_route"), "Mean hop count across successfully evaluated routes, including the destination hop.", None, Some("sum(route hop counts) / evaluated_routes"))),
    ])
}

fn route_candidate_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("amount_sat".into(), metadata_field("integer", false, Some("sat"), "Probe payment amount for this ranking.", None, None)),
        ("rank".into(), metadata_field("integer", false, Some("rank"), "Rank within the probe amount ordered by descending appearances.", None, None)),
        ("node_id".into(), metadata_field("string", false, None, "Public key of the non-peer intermediary node.", Some("getroutes path.node_id_out"), None)),
        ("alias".into(), metadata_field("string", false, None, "Gossip alias advertised by the candidate node.", Some("listnodes"), None)),
        ("connectable".into(), metadata_field("boolean", false, None, "Whether the candidate advertises at least one network address in its current node announcement.", Some("listnodes.addresses"), None)),
        ("appearances".into(), metadata_field("integer", false, Some("route"), "Number of successful destination probes whose path contained this node.", Some("getroutes"), None)),
        ("appearance_ratio".into(), metadata_field("number", true, Some("ratio"), "Share of successfully evaluated routes containing this candidate.", None, Some("appearances / evaluated_routes for amount_sat"))),
        ("average_fee_ppm".into(), metadata_field("number", false, Some("ppm"), "Mean advertised proportional fee across the candidate's public channel directions.", Some("listchannels"), None)),
        ("fee_diversity".into(), metadata_field("number", false, Some("ratio"), "Existing fee-diversity score derived from the candidate's public channel policies.", Some("listchannels"), None)),
        ("channel_count".into(), metadata_field("integer", false, Some("channel"), "Number of public channel records associated with the candidate.", Some("listchannels"), None)),
    ])
}

fn render_routes_page(
    entries: &[RouteCandidate],
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
                            (format!("{} sats", format_sats(amount_msat / 1000)))
                        }
                        div class="back-link" {
                            a href="index.html" { "Home" } " | "
                            a href="nodes/" { "Nodes" } " | "
                            a href="channels/" { "Channels" } " | "
                            a href="forwards-week.html" { "Forwards" } " | "
                            a href="routes-10000.html" { "Routes" } " | "
                            a href="failures.html" { "Failures" } " | "
                            a href="roic.html" { "ROIC" } " | "
                            a href="closed-channels.html" { "Closed" }
                        }
                    }

                    section {
                        h2 { "Route Amount Variants" }
                        p {
                            "Analysis performed for different payment amounts:"
                        }
                        ul {
                            li { a href="routes-1000.html" { (format!("{} sats (0.00001 BTC)", format_sats(1_000))) } }
                            li { a href="routes-10000.html" { (format!("{} sats (0.0001 BTC)", format_sats(10_000))) } }
                            li { a href="routes-100000.html" { (format!("{} sats (0.001 BTC)", format_sats(100_000))) } }
                            li { a href="routes-1000000.html" { (format!("{} sats (0.01 BTC)", format_sats(1_000_000))) } }
                            li { a href="routes-10000000.html" { (format!("{} sats (0.1 BTC)", format_sats(10_000_000))) } }
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
                            "Destinations: " (summary.processed_destinations) " processed of "
                            (summary.eligible_destinations) " eligible | Queried: "
                            (summary.queried_destinations) " | Capacity-filtered: "
                            (summary.capacity_filtered_destinations) " | Failed: "
                            (summary.failed_routes) " | Timed out: " (summary.timed_out_routes)
                            " | Elapsed: " (format!("{:.1} seconds", summary.elapsed_seconds))
                        }
                        @if summary.budget_exhausted {
                            p { "The per-amount time budget expired before all eligible destinations were processed." }
                        }
                        p {
                            "Maximum route fee: "
                            (format!("{} sats", format_sats(summary.max_fee_msat / 1000)))
                            " (1% of the payment amount, with a 5 sat minimum)."
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
                                        th { "Connectable" }
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
                                            td { (if entry.connectable { "Yes" } else { "No" }) }
                                            td class="align-right" { (entry.appearances) }
                                            td class="align-right" { (format!("{:.1}", entry.average_fee_ppm)) }
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::{Duration, SecondsFormat, Utc};

    use super::*;

    #[test]
    fn route_fee_budget_matches_xpay_default() {
        assert_eq!(route_max_fee_msat(100_000), 5_000);
        assert_eq!(route_max_fee_msat(1_000_000), 10_000);
        assert_eq!(route_max_fee_msat(10_000_000), 100_000);
        assert_eq!(route_max_fee_msat(10_000_000_000), 100_000_000);
    }

    #[test]
    fn destination_sampling_is_stable_within_a_utc_day() {
        let node = "02c095d069538f96bf14c5f90f6c0851bdf354a0ec86039a24bf38a73f705adc2c";
        assert_eq!(
            destination_sample_score(node, 20_000),
            destination_sample_score(node, 20_000)
        );
        assert_ne!(
            destination_sample_score(node, 20_000),
            destination_sample_score(node, 20_001)
        );
    }

    #[test]
    fn route_cache_uses_a_rolling_24_hour_ttl() {
        let now = Utc::now();
        let mut manifest = test_manifest(now - Duration::hours(23));
        assert!(manifest_is_fresh(&manifest, now));

        manifest.generated_at =
            (now - Duration::hours(24)).to_rfc3339_opts(SecondsFormat::Secs, true);
        assert!(!manifest_is_fresh(&manifest, now));

        manifest.generated_at =
            (now + Duration::seconds(1)).to_rfc3339_opts(SecondsFormat::Secs, true);
        assert!(!manifest_is_fresh(&manifest, now));
    }

    #[test]
    fn cached_routes_export_imports_with_stable_snapshot_paths() {
        let root = temporary_test_directory();
        let cache = root.join("cache");
        let snapshot = root.join("snapshot");
        fs::create_dir_all(&cache).unwrap();
        fs::create_dir_all(&snapshot).unwrap();

        let datasets = route_dataset_metadata(
            "route-runs-generation.json",
            "route-runs-generation.schema.json",
            "route-candidates-generation.json",
            "route-candidates-generation.schema.json",
            1,
            0,
        );
        let manifest = RoutesManifest {
            datasets: datasets.clone(),
            ..test_manifest(Utc::now())
        };
        let runs = vec![RouteRun {
            amount_sat: 1_000,
            max_fee_msat: 10_000,
            scanned_nodes: 10,
            eligible_destinations: 8,
            processed_destinations: 8,
            queried_destinations: 7,
            capacity_filtered_destinations: 1,
            evaluated_routes: 7,
            failed_routes: 0,
            timed_out_routes: 0,
            budget_exhausted: false,
            elapsed_seconds: 2.5,
            candidate_nodes: 0,
            recurring_candidate_nodes: 0,
            average_hops: 2.5,
        }];
        write_json(&cache.join("route-runs-generation.json"), &runs).unwrap();
        write_json(
            &cache.join("route-candidates-generation.json"),
            &Vec::<RouteCandidate>::new(),
        )
        .unwrap();
        write_json(
            &cache.join("route-runs-generation.schema.json"),
            datasets.get("route_runs").unwrap(),
        )
        .unwrap();
        write_json(
            &cache.join("route-candidates-generation.schema.json"),
            datasets.get("route_candidates").unwrap(),
        )
        .unwrap();
        write_json(&cache.join("manifest.json"), &manifest).unwrap();

        let imported =
            import_bundle(export_bundle(&cache).unwrap(), &snapshot, "test-node").unwrap();
        assert!(snapshot.join("route-runs.json").is_file());
        assert!(snapshot.join("route-runs.schema.json").is_file());
        assert!(snapshot.join("route-candidates.json").is_file());
        assert!(snapshot.join(SNAPSHOT_ROUTES_MANIFEST).is_file());
        assert_eq!(imported.datasets["route_runs"].path, "route-runs.json");
        assert_eq!(
            imported.datasets["route_candidates"].schema_path,
            "route-candidates.schema.json"
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn test_manifest(generated_at: DateTime<Utc>) -> RoutesManifest {
        RoutesManifest {
            schema_version: ROUTES_SCHEMA_VERSION,
            generated_at: generated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            node_id: "test-node".to_string(),
            source: RoutesSource {
                amounts_sat: ROUTE_AMOUNTS_SAT.to_vec(),
                sample_seed_utc_day: 20_000,
                randomized_destination_order: true,
                per_amount_budget_seconds: ROUTE_AMOUNT_BUDGET.as_secs(),
                total_budget_seconds: ROUTE_TOTAL_BUDGET.as_secs(),
                single_path_endpoint_capacity_filter: true,
                max_fee_ppm: ROUTE_MAX_FEE_PPM,
                minimum_max_fee_msat: ROUTE_MIN_MAX_FEE_MSAT,
                layers: vec!["auto.localchans".to_string(), "auto.sourcefree".to_string()],
                final_cltv: 9,
                maxdelay: 2016,
                maxparts: 1,
            },
            datasets: BTreeMap::new(),
        }
    }

    fn temporary_test_directory() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lightdash-routes-test-{}-{nonce}",
            std::process::id()
        ))
    }
}
