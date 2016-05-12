#!/bin/bash

set -x
set -e

CONSUL_DOWN_DIR=`mktemp -d`
CONSUL_BIN_DIR=`mktemp -d`
CONSUL_TMP_DATA_DIR=`mktemp -d`
CONSUL_VERSION="0.6.3"
CONSUL_RELEASE_URL="https://releases.hashicorp.com/consul"

if [ ! -d "$CONSUL_DOWN_DIR" ]; then
    mkdir -p $CONSUL_DOWN_DIR
fi
if [ ! -d "$CONSUL_BIN_DIR" ]; then
    mkdir -p $CONSUL_BIN_DIR
fi
if [ ! -d "$CONSUL_TMP_DATA_DIR" ]; then
    mkdir -p $CONSUL_TMP_DATA_DIR
fi

function clean_exit(){
    local error_code="$?"
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $spawned
    fi
    rm -rf $CONSUL_TMP_DATA_DIR
    rm -rf $CONSUL_BIN_DIR
    rm -rf $CONSUL_DOWN_DIR
    return $error_code
}

function get_leader(){
    local leader=""
    leader=$(curl -s http://127.0.0.1:8500/v1/status/leader)
    if [ $? -ne 0 ]; then
        return 1
    else
        echo $leader | python -c "import sys;\
                                  import json;\
                                  print(json.loads(sys.stdin.read()))"
        return 0
    fi
}

function wait_until_up(){
    local leader=`get_leader`
    while [ -z "$leader" ]; do
        echo "Waiting for consul to respond to a leader http request"
        sleep 1
        leader=`get_leader`
    done
}

function download_and_expand_consul() {
    if [ "$(uname)" == "Darwin" ]; then
        local consul_file="consul_${CONSUL_VERSION}_darwin_amd64.zip"
    elif [ "$(uname -a | cut -d" " -f1)" == "Linux" ]; then
        local consul_file="consul_${CONSUL_VERSION}_linux_amd64.zip"
    else
        echo "Unknown operating system '$(uname -a)'"
        exit 1
    fi
    wget $CONSUL_RELEASE_URL/$CONSUL_VERSION/$consul_file \
                            --directory-prefix $CONSUL_DOWN_DIR
    if [ $? -ne 0 ]; then
        echo "Unable to download consul"
        exit 1
    fi
    unzip $CONSUL_DOWN_DIR/$consul_file -d $CONSUL_BIN_DIR
}

function get_consul_version() {
    local consul_bin="$1"
    local consul_version=`$consul_bin --version | \
                          head -n1 | cut -d" " -f2 | \
                          cut -d"v" -f2`
    echo $consul_version
}

trap "clean_exit" EXIT

CONSUL_BIN=`which consul || true`
if [ -z "$CONSUL_BIN" ]; then
    echo "Downloading consul $CONSUL_VERSION"
    download_and_expand_consul
    CONSUL_BIN=$CONSUL_BIN_DIR/consul
    if [ ! -e "$CONSUL_BIN" ]; then
        echo "Consul executable does not exist (even after downloading it)"
        exit 1
    fi
else
    CONSUL_VERSION=`get_consul_version "$CONSUL_BIN"`
    echo "Consul $CONSUL_VERSION is already installed"
fi

$CONSUL_BIN agent -server -bootstrap-expect 1 -data-dir $CONSUL_TMP_DATA_DIR -node=agent-one -bind=127.0.0.1 &

# Give some time for the agent to elect a leader, and startup...
# TODO(harlowja): there has to be a better way to do this, there doesn't
# seem
wait_until_up

export TOOZ_TEST_CONSUL_URL="consul://localhost:8500/v1"

# Yield execution to venv command
$*
