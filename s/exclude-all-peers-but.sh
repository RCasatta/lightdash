#!/bin/sh

CMD=${TEST_CMD:-lightning-cli listfunds}  # eg: `TEST_CMD="cat test-json/listfunds" ./exclude-all-peers-but.sh 032b01b7585f781420cd4148841a82831ba37fa952342052cec16750852d4f2dd9`

$CMD | jq -c "[.channels.[] | select(.peer_id!=\"$1\") .peer_id]"
