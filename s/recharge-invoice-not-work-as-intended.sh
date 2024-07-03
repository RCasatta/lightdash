#!/bin/sh

SATOSHI=$1
SHORT_CHANNEL=$2

[ -z "$SATOSHI" ] && echo "first program argument should be satoshis and it is unset or set to the empty string" && exit 1
[ -z "$SHORT_CHANNEL" ] && echo "second program argument should be a short channel id and it is unset or set to the empty string" && exit 1

LABEL=recharge$(date)

lightning-cli invoice -k amount_msat=${SATOSHI}sat label="$LABEL" description=recharge exposeprivatechannels=$SHORT_CHANNEL
