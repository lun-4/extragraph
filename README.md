# extragraph

quick lua-scriptable feedgen

takes you from
```lua
return {
  filter = function(ctx)
    local hasSubstring = string.find(string.lower(ctx.post.text), "geans") ~= nil
    return hasSubstring
  end,
}
```

to

![](https://smooch.computer/i/hn6dje7k9xdei.png)

based on very good work by jaz: https://github.com/ericvolp12/go-bsky-feed-generator

## how

### how build

- get go

```
make build
```

or

```
docker build -t extragraph:0.0.1 .
```

### how run

```sh
export PORT=9032
export SCRIPTABLE_FOLLOWING_FEED_ENABLE=1

# create an account on bluesky. set its did plc here
export SCRIPTABLE_FOLLOWING_FEED_ACTOR_DID=did:plc:AAA

# this is just base name. you will have scriptable_1, scriptable_2, scriptable_3, etc...
export SCRIPTABLE_FOLLOWING_FEED_NAME=scriptable

# server address
export SERVICE_ENDPOINT=https://feedgen.bsky.ln4.net

# using the main bluesky llc appview and relay:
export RELAY_WEBSOCKET_ADDRESS=wss://bsky.network
export APPVIEW_URL=https://public.api.bsky.app

make build
./feedgen
```

### how use

first you need to publish this feed somewhere. i use the scripts from
https://github.com/bluesky-social/feed-generator/tree/main/scripts. if i remember its something like this
```sh
git clone https://github.com/bluesky-social/feed-generator
cd feed-generator
npm i
npm publishFeed
```

after publishing, you'll have to go to pdsls.dev and fix the record. the feedgen itself lives as a did:web, not did:plc.

example: https://pdsls.dev/at/did:plc:cxnj3lvzbel4zbzf3i3fmnjn/app.bsky.feed.generator/scriptable_1

go to the one made for yours, login to pdsls.dev (works with 3rd-party pds) and edit it to `did:web:feedgen.bsky.ln4.net`

the feed should then be available and working on bluesky frontend, by default nothing is done about posts coming from firehose.
to do something about that, write your filter script

```lua
local debug = 0
return {
  filter = function(ctx)
    local hasSubstring = string.find(string.lower(ctx.post.text), "amogus") ~= nil
    return hasSubstring
  end,
}
```

save it somewhere in the server, then run

```sh
./feedgen-admin adduser did:plc:your_did_plc
./feedgen-admin setscript did:plc:your_did_plc 1 ./yourscript.lua
```

there is no backfill. script changes happen in real time, but they don't backfill either.

have fun!
