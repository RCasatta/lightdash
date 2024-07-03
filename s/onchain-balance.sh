lightning-cli listfunds | jq '[.outputs.[].amount_msat] | add/1000 | floor/100000000'
