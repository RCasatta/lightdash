
# Lightdash

## Analytical snapshots

Export a versioned snapshot without generating the HTML dashboard:

```bash
lightdash snapshot target/snapshot
```

The output contains a manifest and summary, current and closed channel JSON
files, separate settled and non-settled forward streams, and rebalance events in
JSONL format. The manifest embeds a dataset and field catalog with units,
definitions, formulas, sources, aggregation guidance, and warnings. Matching
`*.schema.json` companion files make each data file understandable when shared
without the rest of the snapshot.

Snapshots include processed channel policy and liquidity history by default.
When `--ssh` is used, Lightdash runs `lightdash history export` on the remote
node and imports the resulting tar stream. Local snapshots read
`/var/lib/lightdash/history/processed` directly. Use `--history-directory` to
override that path or `--without-history` to intentionally create a snapshot
without historical datasets. Debug test-data snapshots omit history unless a
history directory is explicitly supplied.

Snapshots also include cached route analysis by default. The cache lives at
`/var/lib/lightdash/routes/processed` and is refreshed when it is at least 24
hours old. Each refresh probes the configured payment amounts with `getroutes`,
a single part, and the same fee ceiling as xpay: 1% with a 5 sat minimum. Use
`--routes-directory` to override the cache location or `--without-routes` to
omit it intentionally. With `--ssh`, the remote Lightdash process refreshes and
exports the cache in one operation, avoiding a separate SSH process per route
probe.

Lightdash automatically reads Summars availability data from
`~/.lightning/bitcoin/summars/availdb.json`. Use `--availdb PATH` or the
`AVAILDB_PATH` environment variable to override it. With `--ssh`, the path is
read from the remote host.

Generate the experimental snapshot-driven site in a separate step:

```bash
lightdash dashboard2 target/snapshot target/site2
```

Dashboard2 currently provides a shared site shell, an overview, and dynamic
channel, forward, rebalance, and route-candidate tables with presets, generic
filters, sorting, column visibility, URL state, and filtered CSV/JSON exports.
The forwards page streams only `settled-forwards.jsonl` and paginates the result
instead of loading noisy failed attempts or rendering the complete history into
the DOM. Serve the output over HTTP so the browser can load its data files.

## Historical channel data

Rebuild normalized channel policy and liquidity histories from the raw
`listchannels` and `listfunds` archives:

```bash
lightdash history rebuild
```

The default source is `/var/lib/lightdash/history/raw`, containing `channels/`
and `funds/`. Processed data is atomically written under
`/var/lib/lightdash/history/processed` as:

```text
manifest.json
channel-policy-history.jsonl.gz
channel-policy-history.schema.json
channel-liquidity-history.jsonl.gz
channel-liquidity-history.schema.json
```

The rebuild scans all raw archives but emits change points rather than
repeating identical consecutive observations. Policy history is restricted to
channels involving the local node. Use `--raw-directory` and
`--output-directory` to override the defaults for development or migration.

Export exactly the files referenced by the processed manifest as a tar stream:

```bash
lightdash history export > history.tar
ssh casatta@unique lightdash history export > history.tar
tar -xf history.tar
```

The JSONL datasets are already gzip-compressed, so the surrounding tar stream
is intentionally uncompressed.

## Cached route analysis

Refresh the processed route cache explicitly:

```bash
lightdash routes refresh
```

Export it as a self-contained JSON bundle, refreshing only when stale:

```bash
lightdash routes export --refresh-if-stale > routes.json
```

The legacy HTML generator remains available as `lightdash routes DIRECTORY`.
The cache contains a versioned manifest, route-run summaries, candidate rows,
and matching schema companions. Snapshot import gives these files stable names
while preserving the route-analysis generation time separately from the
snapshot generation time.

## Remote Core Lightning node

Use the global `--ssh` option to execute every `lightning-cli` command on a
remote node. SSH host aliases are supported, so ports and identity files can be
configured in `~/.ssh/config`. Lightdash enables SSH compression automatically
to reduce bandwidth usage for the JSON responses.

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
├── dashboard2.rs # Experimental snapshot-driven site renderer
├── snapshot.rs  # Versioned analytical snapshot export
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
