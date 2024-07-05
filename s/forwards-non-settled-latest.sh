lightning-cli listforwards | jq '[.forwards.[] | select(.status != "settled")] | .[-30:][] |  {in_channel, out_channel, fee_sat: ((.fee_msat//0)/1000|floor), in_sat: ((.in_msat//0)/1000 | floor ), out_sat: ((.out_msat//0)/1000 | floor), received: (.received_time | floor | todate), status, style, failreason }'
