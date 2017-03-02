#!/bin/sh
#
# simple shell script for auto-restarting backend processes.
#
# Copyright 2016 Tekuma Inc.
# All rights reserved.
# created by Scott C. Livingston

export NODE_ENV=production

kill_bk () {
    kill ${child_pid}
    exit
}
trap kill_bk INT

while true; do
    node curator-daemon.js&
    child_pid=$!
    echo PID: ${child_pid}
    wait $!
done
