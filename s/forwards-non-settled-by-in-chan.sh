lightning-cli listforwards | jq '[.forwards.[] | select(.status != "settled")] | [group_by(.in_channel)[] | { in_channel:.[0].in_channel, tot: length}] | sort_by(.tot)'
