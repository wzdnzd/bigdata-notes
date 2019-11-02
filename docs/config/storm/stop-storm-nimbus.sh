#!/usr/bin/env bash

count=`ps -ef | grep daemon.nimbus | grep -v "grep" | wc -l`
if [[ ${count} -gt 0 ]]; then
    kill -9 `ps -ef | grep daemon.nimbus| awk '{print $2}'`
fi

count=`ps -ef | grep daemon.ui.UIServer | grep -v "grep" | wc -l`
if [[ ${count} -gt 0 ]]; then
    kill -9 `ps -ef | grep daemon.ui.UIServer | awk '{print $2}'`
fi
