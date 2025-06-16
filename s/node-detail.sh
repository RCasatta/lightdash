#!/bin/sh

NODE_ID=$1

[ -z "$NODE_ID" ] && echo "first program argument should be a node id (pubkey) and it is unset or set to the empty string" && exit 1



lightning-cli listnodes | jq ".nodes[] | select(.nodeid==\"$NODE_ID\")"
