#!/bin/sh

export NODE_OPTIONS=--use-openssl-ca
export FEEDGEN_HOSTNAME=feedgen.gsky.ln4.net

# example prompts:

# ✔ Enter your Bluesky handle feedgen.pds.gsky.ln4.net
# ✔ Enter your Bluesky password (preferably an App Password):
# ✔ Optionally, enter a custom PDS service to sign in with: https://pds.gsky.ln4.net
# ✔ Enter the short name for the record you want to delete: following
# ✔ Are you sure you want to delete this record? Any likes that your feed has will be lost: yes


echo "USER: feedgen.pds.gsky.ln4.net"
echo "PASS: password"
echo "PDS: https://pds.gsky.ln4.net"
echo "RECORD: following"
echo "YES: yes"
cd ../feedgen && npm run build
cd ../feedgen && npm run unpublishFeed
