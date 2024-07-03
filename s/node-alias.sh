lightning-cli listnodes | jq -r '.nodes.[] | (.nodeid + " " +.alias) '
