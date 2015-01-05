#!/bin/bash
set -x -e

CONFFILE=$(mktemp -t tooz-sentinel-XXXXXX)

function clean_exit(){
    local error_code="$?"
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $(jobs -p)
    fi
    wait $spawned
    rm $CONFFILE || true
    rm /tmp/sentinel.26381.log || true
    return $error_code
}

cat > $CONFFILE <<EOF
port 26381
dir /tmp
sentinel monitor mainbarn 127.0.0.1 6381 1
sentinel down-after-milliseconds mainbarn 80000
sentinel parallel-syncs mainbarn 1
sentinel failover-timeout mainbarn 180000
logfile /tmp/sentinel.26381.log
EOF

trap "clean_exit" EXIT

# If we can't find either redis-server or redis-sentinel, exit.
redis_bin=$(which redis-server)
sentinel_bin=$(which redis-sentinel)
$redis_bin --port 6381 &
$sentinel_bin $CONFFILE &

export TOOZ_TEST_REDIS_URL="redis://localhost:26381?sentinel=mainbarn&timeout=5"
# Yield execution to venv command
$*
