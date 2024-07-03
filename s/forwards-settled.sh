lightning-cli listforwards | jq '.forwards.[] | select(.status=="settled") | {in_channel, out_channel, fee_sat:(.fee_msat/1000 | floor), amount: (.out_msat/1000|floor) , resolved:
 (.resolved_time | floor | todate)}'
