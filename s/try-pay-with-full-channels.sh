#!/bin/sh


// TODO use instead exclude with only channels <60%

BOLT11=$1

[ -z "$BOLT11" ] && echo "first program argument should be a bolt11 invoice and it is unset or set to the empty string" && exit 1

CHANNELS=$(./channels-balance.sh | jq '[.[] | select(.perc>50) | .short_channel_id] | reverse' | jq -r '.[]')

while IFS= read -r line; do
  echo "... $line ..."
  ./pay-using-channel.sh "$line" "$BOLT11" && exit 0
done <<< "$CHANNELS"


echo "all channels failed"
