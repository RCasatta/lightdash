A=$(mktemp)
lightning-cli listforwards | jq '[.forwards.[] | select(.status=="settled")]' >$A
echo "1 3 6 12 months"
M1=$(cat $A | jq '[.[] | select(.resolved_time > (now-(60*60*24*30))) | .fee_msat] | add | . / 1000')
M3=$(cat $A | jq '[.[] | select(.resolved_time > (now-(3*60*60*24*30))) | .fee_msat] | add | . / 1000')
M6=$(cat $A | jq '[.[] | select(.resolved_time > (now-(6*60*60*24*30))) | .fee_msat] | add | . / 1000')
M12=$(cat $A | jq '[.[] | select(.resolved_time > (now-(12*60*60*24*30))) | .fee_msat] | add | . / 1000')
TOT=$(lightning-cli listfunds | jq '[.channels.[].our_amount_msat] | add | . /1000')


echo $M1
echo $M3
echo $M6
echo $M12

echo

echo our funds: $TOT

echo 

echo Projected year APY for the last 1,3,6,12 months:

echo "($M1*100*12)/$TOT" | bc -l
echo "($M3*100*4)/$TOT" | bc -l
echo "($M6*100*2)/$TOT" | bc -l
echo "($M12*100)/$TOT" | bc -l

TRANSACTED=$(lightning-cli listforwards | jq '[.forwards.[] | select(.status=="settled") | select(.resolved_time > (now-(60*60*24*30))) | {in_channel, out_channel, fee_sat:(.fee_msat/1000 | floor), amount: (.out_msat/1000|floor) , resolved: (.resolved_time | floor | todate)}] | [.[].amount] | add')

echo
echo Transacted last month:
echo "$TRANSACTED"
