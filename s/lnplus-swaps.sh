#!/bin/sh

curl -s https://lightningnetwork.plus/api/2/get_swaps/ | jq '.[] | select(.capacity_sats>1000000) | select(.participant_max_count==3) | select(.status!="completed") | select(.participant_applied_count<3) | select(.participant_min_capacity_sats<150000000)  | { web_url:.web_url,  peers:[{pubkey:.participants[].pubkey, rank:.participants[].lnplus_rank_number }] }'
