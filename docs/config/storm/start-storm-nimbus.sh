#!/usr/bin/env bash

current=$(cd "$(dirname "$0")";pwd)

nohup ${current}/storm ui >/dev/null 2>&1 &
nohup ${current}/storm nimbus >/dev/null 2>&1 &
