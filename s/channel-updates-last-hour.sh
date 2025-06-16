lightning-cli listchannels | jq -r '
        [.channels[] | select(.last_update > (now - 3600)) | .source]
        | group_by(.)
        | map({node_id: .[0], count: length})
        | sort_by(-.count)
	| .[]
	| "\(.node_id) \(.count)"'
