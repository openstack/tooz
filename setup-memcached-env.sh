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

if ! check_port 11211; then
    memcached_bin=$(which memcached || true)
    if [ -n "$memcached_bin" ]; then
        $memcached_bin &
    else
        echo "Memcached server not available"
        exit 1
    fi
fi

export TOOZ_TEST_MEMCACHED_URL="memcached://?timeout=5"
# Yield execution to venv command
$*
