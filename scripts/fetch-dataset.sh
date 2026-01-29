#!/usr/bin/env bash
set -euo pipefail

: "${NODE_SSH:?NODE_SSH environment variable is required}"
: "${AVAILDB_PATH:?AVAILDB_PATH environment variable is required}"

ssh "$NODE_SSH" bash -s "$AVAILDB_PATH" << 'REMOTE_SCRIPT'
set -euo pipefail
AVAILDB_PATH="$1"

jq -n \
    --slurpfile info <(lightning-cli getinfo) \
    --slurpfile peers <(lightning-cli listpeers) \
    --slurpfile channels <(lightning-cli listpeerchannels) \
    --slurpfile forwards <(lightning-cli listforwards) \
    --slurpfile availdb "$AVAILDB_PATH" \
    '{
        info: $info[0],
        peers: $peers[0].peers,
        channels: $channels[0].channels,
        forwards: $forwards[0].forwards,
        availdb: $availdb[0]
    }' | xz -9 -c
REMOTE_SCRIPT
