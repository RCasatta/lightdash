#!/bin/sh

CMD=${TEST_CMD:-lightning-cli listfunds}  # eg: `TEST_CMD="cat test-json/listfunds" ./channels-balance.sh`
$CMD | jq '.channels.[] | select(.state=="CHANNELD_NORMAL") | {short_channel_id, peer_id, our_btc:(.our_amount_msat/100000000000), our_sats:(.our_amount_msat/1000 | floor), tot_sats:(.amount_msat/1000), perc:(.our_amount_msat/.amount_msat*100 | floor)}'  | jq -s 'sort_by(.perc)'
