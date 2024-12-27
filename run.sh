#!/bin/sh

set -eux
export PORT=9032
export DEBUG=1
export SCRIPTABLE_FOLLOWING_FEED_ENABLE=1
export SCRIPTABLE_FOLLOWING_FEED_ACTOR_DID=did:plc:AAA
export FEED_ACTOR_DID=did:plc:BBB
export SCRIPTABLE_FOLLOWING_FEED_NAME=scriptable_following
export SERVICE_ENDPOINT=https://feedgen.bsky.ln4.net
export RELAY_WEBSOCKET_ADDRESS=wss://bsky.network
export APPVIEW_URL=https://public.api.bsky.app

make build
./feedgen
