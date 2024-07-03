#!/bin/sh

CHANNEL=$1
BOLT11=$2

[ -z "$CHANNEL" ] && echo "first program argument should be a short channel id and it is unset or set to the empty string" && exit 1
[ -z "$BOLT11" ] && echo "second program argument should be a bolt11 invoice and it is unset or set to the empty string" && exit 1

EXCLUDE=$(./exclude-all-channels-but.sh $CHANNEL)

lightning-cli pay -k bolt11=$BOLT11 exclude=${EXCLUDE}
