#!/bin/bash
set -e

ZOO_CONF=/etc/zookeeper
ZOO_DIR=/usr/share/zookeeper
ZOO_BIN=$ZOO_DIR/bin
ZOO_TMP_DIR=$(mktemp -d /tmp/ZOO-TMP-XXXXX)

mkdir $ZOO_TMP_DIR/bin

function clean_exit(){
    local error_code="$?"
    if [ -d $ZOO_CONF ]; then
        stop_zookeeper_server
    fi
    rm -rf ${ZOO_TMP_DIR}
    kill $(jobs -p)
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


function stop_zookeeper_server(){

    $ZOO_TMP_DIR/bin/zkServer.sh stop
}

trap "clean_exit" EXIT

if [ -d $ZOO_CONF ]; then
    start_zookeeper_server
fi

memcached &

python setup.py testr --slowest
