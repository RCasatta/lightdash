lightning-cli listnodes | jq '[.nodes[] | {nodeid, alias}]' >/tmp/nodes.json
lightning-cli listclosedchannels | jq '[.closedchannels[] | {short_channel_id, peer_id}]' >/tmp/closed.json


jq -s '[ .[0][] as $node | .[1][] | select(.peer_id == $node.nodeid) | {short_channel_id, peer_id, alias: $node.alias} ] | sort_by(.short_channel_id)' /tmp/nodes.json /tmp/closed.json

