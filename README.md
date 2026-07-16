
# Lightdash

## Analytical snapshots

Export a versioned snapshot without generating the HTML dashboard:

```bash
lightdash snapshot target/snapshot --availdb test-json/availdb.json
```

The output contains a manifest and summary, current and closed channel JSON files,
plus forward and rebalance event streams in JSONL format.

## Remote Core Lightning node

Use the global `--ssh` option to execute every `lightning-cli` command on a
remote node. SSH host aliases are supported, so ports and identity files can be
configured in `~/.ssh/config`.

```bash
lightdash --ssh name@host snapshot target/snapshot
lightdash --ssh production-node dashboard target
```

In debug builds, specifying `--ssh` overrides the bundled test data.

## Project Structure

```
src/
├── main.rs      # CLI entry point and command routing
├── cmd.rs       # Lightning CLI command wrappers
├── common.rs    # Shared constants, structs, and utilities
├── dashboard.rs # Main dashboard display
├── routes.rs    # Routing analysis
├── sling.rs     # Sling job execution
└── fees.rs      # Fee adjustments
```


## Dashboard created HTML pages

```
directory/
├── index.html              # Main overview page with navigation links
├── dashboard.html          # Detailed dashboard output (terminal-style)
├── peers/
│   ├── index.html         # Peer directory listing with connection status
│   └── *.html             # Individual peer detail pages
└── channels/
    ├── index.html         # Channel directory listing with balances
    └── *.html             # Individual channel detail pages
```
