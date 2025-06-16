lightning-cli listforwards | jq '[.forwards.[] | select(.status != "settled")] | [group_by(.out_channel)[] | { out_channel:.[0].out_channel, tot: length}] | sort_by(.tot)'
