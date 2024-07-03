
CHANNEL=$1

[ -z "$CHANNEL" ] && echo "first program argument should be a short channel id (eg 787383x408x1)" && exit 1



lightning-cli listchannels | jq ".channels.[] | select(.short_channel_id==\"${CHANNEL}\")"
