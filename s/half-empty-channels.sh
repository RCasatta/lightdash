#!/bin/sh

CMD=${TEST_CMD:-lightning-cli listfunds}  # eg: `TEST_CMD="cat test-json/listfunds" ./exclude-all-channels-but.sh 032b01b7585f781420cd4148841a82831ba37fa952342052cec16750852d4f2dd9`

$CMD | jq -c "[.channels.[] | select(.state==\"CHANNELD_NORMAL\") | select(.our_amount_msat/.amount_msat<0.6) | (.short_channel_id + \"/0\"), (.short_channel_id + \"/1\")  ]"
