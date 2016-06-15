#!/bin/bash
set -eux

if [ -z "$(which consul)" ]; then
    CONSUL_VERSION=0.6.3
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
