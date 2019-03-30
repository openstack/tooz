#!/bin/bash
set -eux

if [ -z "$(which consul)" ]; then
    # originally we used 0.6.3 (released in Jan 2016).
    # updated to 1.7.4 in Change-Id: Iaddf21f14c434129541e7c9ec7134e0661f7be52
    # 1.4.0 (released Nov 2018) has a new ACL system.
    # 1.6.1 (released Sep 2019) is the version used by python-consul2 for testing.
    # 1.7.0 (released Feb 2020) changes standards enforcement.
    # For details see upgrade notes in Change-Id: I98fc96468b21368ce66365e3fc38c495b1f2918a
    CONSUL_VERSION=1.7.4
    CONSUL_RELEASE_URL=https://releases.hashicorp.com/consul
    case `uname -s` in
        Darwin)
            consul_file="consul_${CONSUL_VERSION}_darwin_amd64.zip"
            ;;
        Linux)
            consul_file="consul_${CONSUL_VERSION}_linux_amd64.zip"
            ;;
        *)
            echo "Unknown operating system"
            exit 1
            ;;
    esac
    consul_dir=`basename $consul_file .zip`
    mkdir -p $consul_dir
    curl -L $CONSUL_RELEASE_URL/$CONSUL_VERSION/$consul_file > $consul_dir/$consul_file
    unzip $consul_dir/$consul_file -d $consul_dir
    export PATH=$PATH:$consul_dir
fi

# Yield execution to venv command
$*
