#!/bin/bash -x -e

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
        echo "Redis server not available, testing being skipped..."
    fi
fi

# Yield execution to venv command
$*
