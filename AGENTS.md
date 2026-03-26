# AGENTS.md - Lightdash Development Guide

## Project Overview

Lightdash is a Rust CLI tool for Lightning Network channel management dashboard generation. It interfaces with a Core Lightning node via `lightning-cli` and generates HTML dashboards.

## Build Commands

```bash
# Build the project
cargo build

# Build in release mode
cargo build --release

# Run the CLI
cargo run -- dashboard target --min-channels 100 --availdb test-json/availdb.json

# Run with custom arguments
cargo run -- <command> [args]
```

## Lint and Format Commands

```bash
# Run clippy for linting
cargo clippy -- -D warnings

# Format code
cargo fmt

# Check formatting
cargo fmt --check
```

## Test Commands

```bash
# Run all tests
cargo test

# Run a single test by name
cargo test test_name

# Run doc tests
cargo test --doc

# Run tests with output
cargo test -- --nocapture
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

The codebase uses `cfg!(debug_assertions)` to switch between test and production modes:

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
        #[arg(long, default_value = "1")]
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
| `src/dashboard.rs` | HTML dashboard generation |
| `src/routes.rs` | Routing analysis |
| `src/sling.rs` | Sling job execution |
| `src/fees.rs` | Fee adjustments |

### Common Development Tasks

```bash
# Generate dashboard
cargo run -- dashboard target --min-channels 100 --availdb test-json/availdb.json

# Serve dashboard locally
miniserve --index index.html --port 3535 target/ -i 127.0.0.1

# Or use just
just serve
```

### Configuration Files

- `Cargo.toml` - Rust dependencies
- `rust-toolchain.toml` - Rust version and components
- `flake.nix` - Nix development environment
- `justfile` - Common development tasks
- `.env` - Environment variables
