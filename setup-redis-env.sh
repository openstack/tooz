#!/bin/bash
set -x -e

. functions.sh

function clean_exit(){
    local error_code="$?"
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $(jobs -p)
    fi
    return $error_code
}

trap "clean_exit" EXIT

if ! check_port 6379; then
    redis_bin=$(which redis-server || true)
    if [ -n "$redis_bin" ]; then
        $redis_bin --port 6379 &
    else
        echo "Redis server not available"
        exit 1
    fi
fi

export TOOZ_TEST_REDIS_URL="redis://localhost:6379?timeout=5"
# Yield execution to venv command
$*
