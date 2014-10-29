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

if ! check_port 11211; then
    memcached_bin=$(which memcached || true)
    if [ -n "$memcached_bin" ]; then
        $memcached_bin &
    else
        echo "Memcached server not available, testing being skipped..."
    fi
fi

# Yield execution to venv command
$*
