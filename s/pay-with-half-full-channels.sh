#!/bin/sh



BOLT11=$1

[ -z "$BOLT11" ] && echo "first program argument should be a bolt11 invoice and it is unset or set to the empty string" && exit 1

EXCLUDE=$(./half-empty-channels.sh)

A=$(mktemp)
B=$(mktemp)

lightning-cli summars summars-columns=SCID,PERC_US,ALIAS >$A

lightning-cli pay -k bolt11=$BOLT11 exclude="${EXCLUDE}" # riskfactor=20

sleep 3

lightning-cli summars summars-columns=SCID,PERC_US,ALIAS >$B

diff $A $B
