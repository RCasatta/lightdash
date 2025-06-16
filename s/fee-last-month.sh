A=$(mktemp)
lightning-cli listforwards | jq '[.forwards.[] | select(.status=="settled")]' >$A
echo "1 3 6 12 months"
cat $A | jq '[.[] | select(.resolved_time > (now-(60*60*24*30))) | .fee_msat] | add | . / 1000'
cat $A | jq '[.[] | select(.resolved_time > (now-(3*60*60*24*30))) | .fee_msat] | add | . / 1000'
cat $A | jq '[.[] | select(.resolved_time > (now-(6*60*60*24*30))) | .fee_msat] | add | . / 1000'
cat $A | jq '[.[] | select(.resolved_time > (now-(12*60*60*24*30))) | .fee_msat] | add | . / 1000'

