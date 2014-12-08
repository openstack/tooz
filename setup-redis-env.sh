#!/bin/bash
set -x -e

function clean_exit(){
    local error_code="$?"
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $(jobs -p)
    fi
    return $error_code
}

trap "clean_exit" EXIT

redis_bin=$(which redis-server || true)
if [ -n "$redis_bin" ]; then
    $redis_bin --port 6380 &
fi

export TOOZ_TEST_REDIS_URL="redis://localhost:6380?timeout=5"
# Yield execution to venv command
$*
