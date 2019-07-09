#!/usr/bin/env bash

flume-ng agent -c /home/hadoop/flume/conf -f /home/hadoop/flume/conf/flume-kafka.properties -n a1
