#!/usr/bin/env bash

count=`ps -ef | grep daemon.supervisor.Supervisor | grep -v "grep" | wc -l`
if [[ ${count} -gt 0 ]]; then
    kill -9 `ps -ef | grep daemon.supervisor.Supervisor| awk '{print $2}'`
fi

