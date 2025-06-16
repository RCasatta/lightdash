lightning-cli listpeerchannels | jq '.channels[] | select(.state!="CHANNELD_NORMAL")'
