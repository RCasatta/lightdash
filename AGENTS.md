# AGENTS.md - Lightdash Development Guide

## Project Overview

Lightdash is a Rust CLI tool for Lightning Network channel management and
dashboard generation. It interfaces with a Core Lightning node through
`lightning-cli`, either locally, through SSH, or from bundled test data.

There are currently two dashboard flows:

- `dashboard` is the existing direct flow: query the node and generate the
  complete legacy HTML site in one process.
- `snapshot` + `dashboard2` is the new two-stage flow: first export a versioned,
  self-descriptive analytical dataset, then generate a simpler dynamic site
  using only those files.

Prefer extending the snapshot-driven flow for new Dashboard2 features. Do not
make Dashboard2 query `Store` or invoke `lightning-cli`; it must remain a pure
snapshot consumer.

## Build Commands

```bash
# Check the project
direnv exec . cargo check

# Build in release mode
direnv exec . cargo build --release

# Run the legacy dashboard
direnv exec . cargo run -- dashboard target/site --min-channels 100 \
  --availdb test-json/availdb.json

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

## Snapshot and Dashboard2 Architecture

Generate the analytical snapshot first, then render Dashboard2:

```bash
direnv exec . cargo run -- snapshot target/snapshot
direnv exec . cargo run -- dashboard2 target/snapshot target/site2
```

For a remote node, `--ssh` is a global argument and belongs before the
subcommand:

```bash
direnv exec . cargo run -- --ssh name@host snapshot target/snapshot
direnv exec . cargo run -- dashboard2 target/snapshot target/site2
```

The snapshot contract is versioned by `SCHEMA_VERSION` in `src/snapshot.rs`.
Dashboard2 intentionally rejects unsupported versions. When changing exported
field names, types, meaning, or file layout:

1. Update the serialized snapshot structs and generation logic.
2. Update the canonical catalog in `src/snapshot_metadata.rs`.
3. Increment `SCHEMA_VERSION`.
4. Update Dashboard2 to consume the new contract.
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
- `settled-forwards.jsonl`: successful forwards used by Dashboard2.
- `other-forwards.jsonl`: failed, offered, pending, and other noisy attempts.
- `rebalances.jsonl`: matched bookkeeper rebalance parts.

Historical archives are a separate server-side source under
`/var/lib/lightdash/history/raw/{channels,funds}`. Rebuild their normalized
change-point datasets with:

```bash
direnv exec . cargo run -- history rebuild
```

The command writes a self-descriptive manifest, schema companions, and gzip
JSONL datasets to `/var/lib/lightdash/history/processed`. It intentionally
rescans the complete raw archive so processed-schema changes remain easy to
rebuild. Keep raw archives as the source of truth; do not make Dashboard2 read
the raw `listchannels` or `listfunds` files.

`lightdash history export` streams a tar archive to stdout containing only the
manifest and files referenced by it. This is the transport for retrieving
processed history over SSH; do not rsync the raw archives into a snapshot.

Snapshot schema v4 imports processed history by default. In local mode it reads
the processed directory directly; with `--ssh` it invokes the remote history
export and validates both the history schema version and node ID before merging
the datasets into the snapshot manifest. Use `--without-history` only when an
incomplete snapshot is intentional. Test-data mode omits history unless
`--history-directory` is supplied.

Keep settled and non-settled forwards separate. The Dashboard2 forwards page
must load only `settled-forwards.jsonl`; failed forwards are high-volume,
spammy, and not economically meaningful enough for the default interactive
view.

Derived values that are part of the analytical contract, such as `fee_ppm` and
`elapsed_seconds`, should be computed during snapshot generation. Avoid
reimplementing metric formulas independently in browser JavaScript.

Dashboard2 copies only the data it uses into its `data/` directory. Its tables
are client-side and support filtering, sorting, presets, column visibility, URL
state, pagination where appropriate, and filtered exports. Column descriptions
and tooltips must come from snapshot metadata instead of duplicated prose in
HTML or JavaScript.

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
use crate::common::ChannelFee;
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

The codebase uses `cfg!(debug_assertions)` to switch between test and production
modes:

```rust
pub fn list_channels() -> ListChannels {
    let v = if cfg!(debug_assertions) {
        cmd_result("zcat", &["test-json/listchannels.gz"])
    } else {
        cmd_result("lightning-cli", &["listchannels"])
    };
    serde_json::from_value(v).unwrap()
}
```

Test data is located in `test-json/` directory.

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
| `src/dashboard.rs` | Legacy direct HTML dashboard generation |
| `src/snapshot.rs` | Versioned JSON/JSONL analytical snapshot generation |
| `src/snapshot_metadata.rs` | Canonical dataset and metric descriptions |
| `src/dashboard2.rs` | Snapshot-driven site generation and shared HTML shell |
| `src/dashboard2.js` | Dynamic Dashboard2 tables and metadata tooltips |
| `src/dashboard2.css` | Dashboard2 shared styling |
| `src/history.rs` | Full rebuild of normalized historical channel datasets |
| `src/routes.rs` | Routing analysis |
| `src/sling.rs` | Sling job execution |
| `src/fees.rs` | Fee adjustments |

### Common Development Tasks

```bash
# Generate a test-data snapshot and Dashboard2 site
direnv exec . cargo run -- snapshot target/snapshot
direnv exec . cargo run -- dashboard2 target/snapshot target/site2

# Generate the legacy dashboard
direnv exec . cargo run -- dashboard target/site --min-channels 100 \
  --availdb test-json/availdb.json

# Serve Dashboard2 locally; opening through file:// will not load JSON data
direnv exec . miniserve --index index.html --port 3535 \
  --interfaces 127.0.0.1 target/site2

# Or use just
direnv exec . just serve
```

Before completing changes to snapshots or Dashboard2, run:

```bash
direnv exec . cargo fmt --check
direnv exec . cargo check --quiet
direnv exec . cargo clippy --quiet -- -D warnings
direnv exec . cargo test --quiet
direnv exec . node --check src/dashboard2.js
```

For contract changes, also generate a fresh snapshot and Dashboard2 site, then
inspect `manifest.json`, the companion schema files, and at least one record
from each affected dataset. Browser-test dynamic tables over HTTP when their
JavaScript or metadata integration changes.

### Configuration Files

- `Cargo.toml` - Rust dependencies
- `rust-toolchain.toml` - Rust version and components
- `flake.nix` - Nix development environment
- `justfile` - Common development tasks
- `.env` - Environment variables
