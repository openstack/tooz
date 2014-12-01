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

memcached_bin=$(which memcached || true)
if [ -n "$memcached_bin" ]; then
    $memcached_bin -p 11212 &
fi

export TOOZ_TEST_MEMCACHED_URL="memcached://localhost:11212?timeout=5"
# Yield execution to venv command
$*
