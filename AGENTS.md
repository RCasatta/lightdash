# AGENTS.md - Lightdash Development Guide

## Project Overview

Lightdash is a Rust CLI tool for Lightning Network channel management and
dashboard generation. It interfaces with a Core Lightning node through
`lightning-cli`, either locally, through SSH, or from bundled test data.

The dashboard is a two-stage flow: `snapshot` exports a versioned,
self-descriptive analytical dataset, then `dashboard` generates a dynamic site
using only those files.

Do not make Dashboard query `Store` or invoke `lightning-cli`; it must remain a
pure snapshot consumer.

## Local Core Lightning Reference

A local checkout of the Core Lightning API and command reference may be
available at `~/references/apis/core-lightning`. Check this reference when
working with `lightning-cli`, RPC methods, command syntax, or version-specific
Core Lightning behavior before consulting external documentation.

## Build Commands

```bash
# Check the project
direnv exec . cargo check

# Build in release mode
direnv exec . cargo build --release

# Run with custom arguments
direnv exec . cargo run -- <command> [args]
```

## Lint and Format Commands

```bash
# Run clippy for linting
direnv exec . cargo clippy -- -D warnings

# Format code
direnv exec . cargo fmt

# Check formatting
direnv exec . cargo fmt --check
```

## Test Commands

```bash
# Run all tests
direnv exec . cargo test --quiet

# Run a single test by name
direnv exec . cargo test test_name

# Run doc tests
direnv exec . cargo test --doc

# Run tests with output
direnv exec . cargo test -- --nocapture
```

## Development Environment

This project uses Nix flakes. To enter the development shell:

```bash
nix develop
```

Or with direnv (already configured):

```bash
direnv allow
```

The dev shell includes: rust-toolchain, miniserve, just.

All build, test, formatting, and CLI commands must be run through
`direnv exec .` on NixOS.

## Snapshot and Dashboard Architecture

Generate the analytical snapshot first, then render Dashboard:

```bash
direnv exec . cargo run -- snapshot target/snapshot
direnv exec . cargo run -- dashboard target/snapshot target/site
```

For a remote node, `--ssh` is a global argument and belongs before the
subcommand:

```bash
direnv exec . cargo run -- --ssh name@host snapshot target/snapshot
direnv exec . cargo run -- dashboard target/snapshot target/site
```

The snapshot contract is versioned by `SCHEMA_VERSION` in `src/snapshot.rs`.
Dashboard intentionally rejects unsupported versions. When changing exported
field names, types, meaning, or file layout:

1. Update the serialized snapshot structs and generation logic.
2. Update the canonical catalog in `src/snapshot_metadata.rs`.
3. Increment `SCHEMA_VERSION`.
4. Update Dashboard to consume the new contract.
5. Regenerate fixtures or validation snapshots rather than expecting old
   snapshots to work.

`manifest.json` contains node and snapshot identity, dataset paths, record
counts, and the full field catalog. Each dataset also has a matching
`*.schema.json` companion containing its description and field metadata. Keep
these files suitable for analysis by people and AI agents: document exact
units, formulas, sources, aggregation rules, and important caveats.

Snapshot datasets currently include:

- `summary.json`: node-level balances, counts, revenue, and ROIC.
- `channels.json`: current channels with routing, rebalance, and ROIC metrics.
- `closed-channels.json`: closed-channel history and return attribution.
- `settled-forwards.jsonl`: successful forwards used by Dashboard.
- `other-forwards.jsonl`: failed, offered, pending, and other noisy attempts.
- `rebalances.jsonl`: matched bookkeeper rebalance parts.
- `route-runs.json`: coverage summaries for cached single-part route probes.
- `route-candidates.json`: non-peer intermediaries ranked as potential channel
  partners, enriched at snapshot time with a node-level `weighted_route_score`
  and joined LN+ Liquidity Pool offer fields.
- `route-partners.json`: one row per route candidate node with appearances per
  probe amount, the weighted score, aggregated past-channel results, and LN+
  offer fields. This is the default Dashboard Routes view.
- `lnplus-pools.json`: every node in the public LN+ Liquidity Pool when the
  snapshot was taken.

Historical archives are a separate server-side source under
`/var/lib/lightdash/history/raw/{channels,funds}`. Rebuild their normalized
change-point datasets with:

```bash
direnv exec . cargo run -- history rebuild
```

The command writes a self-descriptive manifest, schema companions, and gzip
JSONL datasets to `/var/lib/lightdash/history/processed`. It intentionally
rescans the complete raw archive so processed-schema changes remain easy to
rebuild. Keep raw archives as the source of truth; do not make Dashboard read
the raw `listchannels` or `listfunds` files.

`lightdash history export` streams a tar archive to stdout containing only the
manifest and files referenced by it. This is the transport for retrieving
processed history over SSH; do not rsync the raw archives into a snapshot.

Snapshots import processed history by default. In local mode it reads
the processed directory directly; with `--ssh` it invokes the remote history
export and validates both the history schema version and node ID before merging
the datasets into the snapshot manifest. Use `--without-history` only when an
incomplete snapshot is intentional. Test-data mode omits history unless
`--history-directory` is supplied.

Snapshots also ensure a durable route-analysis cache by default. Local
snapshots reuse `/var/lib/lightdash/routes/processed` while it is less than 24
hours old and otherwise recompute it. Remote snapshots invoke `lightdash routes
export --refresh-if-stale` on the node so thousands of `getroutes` calls do not
cross individual SSH processes. Use `--routes-directory` to override the cache
path or `--without-routes` to intentionally omit it. Test-data mode omits
routes unless `--routes-directory` is supplied. Cache refreshes write
generation-specific datasets and schemas before atomically replacing the
manifest; a failed refresh reuses the last valid cache when available.

Snapshots also fetch the public LN+ `get_pool_nodes` endpoint (all pages, no
filters) from the machine running `snapshot`, not the node. Pool offers go
stale within hours, so they are never cached; the fetch time is recorded in the
manifest `lnplus_pools_source`. A failed fetch logs a warning and omits the
dataset, leaving the joined route-candidate fields null. Use `--without-lnplus`
to skip it. The route-candidate enrichment happens during snapshot import, so
it does not change the routes cache schema or require redeploying the node
binary.

Keep settled and non-settled forwards separate. The Dashboard forwards page
must load only `settled-forwards.jsonl`; failed forwards are high-volume,
spammy, and not economically meaningful enough for the default interactive
view.

Derived values that are part of the analytical contract, such as `fee_ppm` and
`elapsed_seconds`, should be computed during snapshot generation. Avoid
reimplementing metric formulas independently in browser JavaScript.

Dashboard copies only the data it uses into its `data/` directory. Its tables
are client-side and support filtering, sorting, presets, column visibility, URL
state, pagination where appropriate, and filtered exports. Column descriptions
and tooltips must come from snapshot metadata instead of duplicated prose in
HTML or JavaScript.

For Dashboard presentation, truncate sats and PPM to whole numbers, format
numbers with `en-US` comma grouping, and right-align numeric columns with
tabular digits. Keep sorting and filtering based on raw numeric values.

## Code Style Guidelines

### General Conventions

- **Edition**: Rust 2021
- **Rust toolchain**: 1.85.0 (specified in rust-toolchain.toml)
- **Line length**: Default (typically 100 characters)
- **Indentation**: 4 spaces

### Naming Conventions

- **Structs/Enums**: `PascalCase` (e.g., `Store`, `ListChannels`)
- **Functions/Methods**: `snake_case` (e.g., `run_dashboard`, `list_channels`)
- **Variables**: `snake_case` (e.g., `min_channels`, `avail_map`)
- **Constants**: `SCREAMING_SNAKE_CASE` for true constants, `snake_case` otherwise
- **Modules**: `snake_case` (e.g., `mod channels;`)

### Import Organization

Standard import order:
1. Standard library imports (`std::`)
2. External crate imports (alphabetical)
3. Local crate imports (`crate::`)

```rust
use std::collections::{HashMap, HashSet};
use std::fs;

use chrono::{DateTime, Datelike, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::cmd::{self, DatastoreMode};
use crate::store::Store;
```

### Error Handling

- Use the custom `error_panic!` macro for fatal errors that should log and panic:
  ```rust
  error_panic!("executing `{cmd}` returned {s} with error {e:?}");
  ```
- Use `Result<T, E>` for recoverable errors
- Use `Option<T>` for optional values
- Use `?` operator for propagating errors
- Return meaningful error messages

### Documentation

- Use doc comments (`///`) for public APIs
- Include examples in doc comments where helpful
- Document struct fields when behavior is non-obvious

### Testing Patterns

Debug builds read bundled fixtures instead of calling `lightning-cli`, unless
`--ssh` is given. `cmd::using_test_data()` makes that decision:

```rust
pub fn list_funds() -> ListFunds {
    let v = if using_test_data() {
        gz_json_file("test-json/listfunds.gz")
    } else {
        cmd_result("lightning-cli", &["listfunds"])
    };
    serde_json::from_value(v).unwrap()
}
```

Test data is located in the `test-json/` directory. Remove fixtures that no
code reads anymore.

### Struct and Enum Patterns

- Use `#[derive(Debug, Deserialize, Clone)]` for most data structs
- Use `#[serde(default)]` for optional fields that default to empty/zero
- Use `#[serde(rename = "...")]` for JSON field renaming
- Group related structs in the same file when possible

### Logging

- Use `log::debug!` for verbose information
- Use `log::info!` for important operational info
- Use `log::error!` for errors (pair with error_panic! for fatal errors)

### CLI Patterns

Uses `clap` with derive macros:

```rust
#[derive(Parser)]
#[command(name = "lightdash")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Dashboard {
        #[arg(long, default_value = "10")]
        min_channels: usize,
    },
}
```

### Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | CLI entry point, command routing |
| `src/cmd.rs` | Lightning CLI command wrappers |
| `src/store.rs` | Data store for fetched node data |
| `src/snapshot.rs` | Versioned JSON/JSONL analytical snapshot generation |
| `src/snapshot_metadata.rs` | Canonical dataset and metric descriptions |
| `src/dashboard.rs` | Snapshot-driven site generation and shared HTML shell |
| `src/dashboard.js` | Dynamic Dashboard tables and metadata tooltips |
| `src/dashboard.css` | Dashboard shared styling |
| `src/history.rs` | Full rebuild of normalized historical channel datasets |
| `src/routes.rs` | Cached route analysis used by snapshots |
| `src/lnplus.rs` | LN+ Liquidity Pool fetcher used by snapshots |
| `src/sling.rs` | Sling job execution |
| `src/fees.rs` | Fee adjustments |
| `src/htlc.rs` | HTLC maximum adjustments |

### Common Development Tasks

```bash
# Generate a test-data snapshot and Dashboard site
direnv exec . cargo run -- snapshot target/snapshot
direnv exec . cargo run -- dashboard target/snapshot target/site

# Serve Dashboard locally; opening through file:// will not load JSON data
direnv exec . miniserve --index index.html --port 3535 \
  --interfaces 127.0.0.1 target/site

# Or use just
direnv exec . just serve
```

Before completing changes to snapshots or Dashboard, run:

```bash
direnv exec . cargo fmt --check
direnv exec . cargo check --quiet
direnv exec . cargo clippy --quiet -- -D warnings
direnv exec . cargo test --quiet
direnv exec . node --check src/dashboard.js
```

For contract changes, also generate a fresh snapshot and Dashboard site, then
inspect `manifest.json`, the companion schema files, and at least one record
from each affected dataset. Browser-test dynamic tables over HTTP when their
JavaScript or metadata integration changes.

### Configuration Files

- `Cargo.toml` - Rust dependencies
- `rust-toolchain.toml` - Rust version and components
- `flake.nix` - Nix development environment
- `justfile` - Common development tasks
- `.env` - Environment variables
