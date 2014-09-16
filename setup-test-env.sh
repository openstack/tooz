#!/bin/bash
set -e

ZOO_CONF=/etc/zookeeper
ZOO_DIR=/usr/share/zookeeper
ZOO_BIN=$ZOO_DIR/bin
ZOO_TMP_DIR=$(mktemp -d /tmp/ZOO-TMP-XXXXX)
ZOOKEEPER_STARTED=0

mkdir $ZOO_TMP_DIR/bin

function clean_exit(){
    local error_code="$?"
    if [ -d $ZOO_CONF ] && [ $ZOOKEEPER_STARTED -eq 1 ]; then
        stop_zookeeper_server
    fi
    rm -rf ${ZOO_TMP_DIR}
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $(jobs -p)
    fi
    return $error_code
}


function start_zookeeper_server(){
    #Copy zookeeper scripts in temporary directory
    cp $ZOO_BIN/* $ZOO_TMP_DIR/bin

    #Copy zookeeper conf and set dataDir variable to the zookeeper temporary
    #directory
    cp $ZOO_CONF/conf/zoo.cfg $ZOO_TMP_DIR
    sed -i -r "s@(dataDir *= *).*@\1$ZOO_TMP_DIR@" $ZOO_TMP_DIR/zoo.cfg

    #Replace some variables by the zookeeper temporary directory
    sed -i -r "s@(ZOOCFGDIR *= *).*@\1$ZOO_TMP_DIR@" $ZOO_TMP_DIR/bin/zkEnv.sh

    mkdir $ZOO_TMP_DIR/log
    sed -i -r "s@(ZOO_LOG_DIR *= *).*@\1$ZOO_TMP_DIR/log@" $ZOO_TMP_DIR/bin/zkEnv.sh

    $ZOO_TMP_DIR/bin/zkServer.sh start
}


function check_port(){
    netstat -an 2>/dev/null | grep -q "$1"
    return $?
}

function stop_zookeeper_server(){
    $ZOO_TMP_DIR/bin/zkServer.sh stop
}

trap "clean_exit" EXIT

if ! check_port 2181 && [ -d $ZOO_CONF ]; then
    start_zookeeper_server
    if [ $? -eq 0 ]; then
        ZOOKEEPER_STARTED=1
    fi
fi

if ! check_port 11211; then
    memcached &
fi

# Yield execution to venv command
$*
