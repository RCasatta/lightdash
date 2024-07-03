
# the capacity is divided by 2(directions)*1000(millibtc/sat)*1000000(sat/btc) 

lightning-cli listchannels | jq '{ capacity_btc:([.channels[].amount_msat] | add / 200000000000), nodes:([.channels[].source] | unique | length), channels:([.channels[].short_channel_id] | unique | length)}'
