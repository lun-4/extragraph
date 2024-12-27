#!/bin/sh

export NODE_OPTIONS=--use-openssl-ca
export FEEDGEN_HOSTNAME=feedgen.gsky.ln4.net

export HANDLE=feedgen.pds.gsky.ln4.net
export PASSWORD=123
export SERVICE=https://pds.gsky.ln4.net
export RECORD_NAME=following
export DISPLAY_NAME=Following
export DESCRIPTION="Following feed repro"
export AVATAR=/home/luna/Screenshots/screenie-2024-11-15-17_47_47.png

cd ../feedgen && npm run build
cd ../feedgen && npm run publishFeedEnv
