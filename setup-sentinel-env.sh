#!/bin/bash
set -x -e

SENTINEL_PORTS="26381 26382 26383"
CONFFILES=()
TMPDIR=${TMPDIR:/tmp}

function clean_exit(){
    local error_code="$?"
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $(jobs -p)
    fi
    wait $spawned
    rm /$TMPDIR/sentinel.2638[123].log || true
    rm ${CONFFILES[@]} || true
    return $error_code
}

function write_conf_file() {
    local port=$1
    local conffile=$(mktemp -t tooz-sentinel-$port-XXXXXX)
    cat > $conffile <<EOF
port $port
dir $TMPDIR
sentinel monitor mainbarn 127.0.0.1 6381 2
sentinel down-after-milliseconds mainbarn 80000
sentinel parallel-syncs mainbarn 1
sentinel failover-timeout mainbarn 180000
logfile $TMPDIR/sentinel.$port.log
EOF
    echo $conffile
}

trap "clean_exit" EXIT

# If we can't find either redis-server or redis-sentinel, exit.
redis_bin=$(which redis-server)
sentinel_bin=$(which redis-sentinel)

# start redis
$redis_bin --port 6381 &

# start the sentinels
for port in $SENTINEL_PORTS; do
    conffile=$(write_conf_file $port)
    $sentinel_bin $conffile &
    CONFFILES+=($conffile)
done

# Test a first time without sentinel fallbacks
export TOOZ_TEST_REDIS_URL="redis://localhost:26381?sentinel=mainbarn&timeout=5"
$*
# Test a second time with sentinel fallbacks
export TOOZ_TEST_REDIS_URL="redis://localhost:26381?sentinel=mainbarn&sentinel_fallback=localhost:26382&sentinel_fallback=localhost:26383&timeout=5"
$*
