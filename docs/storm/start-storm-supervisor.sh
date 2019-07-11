#!/usr/bin/env bash

current=$(cd "$(dirname "$0")";pwd)
nohup ${current}/storm supervisor >/dev/null 2>&1 &
