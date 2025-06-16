lightning-cli listnodes | jq '[.nodes[] | select(.option_will_fund != null)]'
