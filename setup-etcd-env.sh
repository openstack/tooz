#!/bin/bash
set -eux

clean_exit() {
    local error_code="$?"
    kill $(jobs -p)
    return $error_code
}

trap clean_exit EXIT
if [ -n "$(which etcd)" ]; then
    etcd &
else
    ETCD_VERSION=2.2.2
    case `uname -s` in
        Darwin)
            OS=darwin
            SUFFIX=zip
            ;;
        Linux)
            OS=linux
            SUFFIX=tar.gz
            ;;
        *)
            echo "Unsupported OS"
            exit 1
    esac
    case `uname -m` in
         x86_64)
             MACHINE=amd64
             ;;
         *)
            echo "Unsupported machine"
            exit 1
    esac
    TARBALL_NAME=etcd-v${ETCD_VERSION}-$OS-$MACHINE
    test ! -d "$TARBALL_NAME" && curl -L https://github.com/coreos/etcd/releases/download/v${ETCD_VERSION}/${TARBALL_NAME}.${SUFFIX} | tar xz
    $TARBALL_NAME/etcd &
fi

export TOOZ_TEST_ETCD_URL="etcd://localhost:4001"
# Yield execution to venv command
$*
