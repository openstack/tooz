#!/usr/bin/env bash

set -o pipefail

TESTRARGS=$1

# --until-failure is not compatible with --subunit see:
#
# https://bugs.launchpad.net/testrepository/+bug/1411804
#
# this work around exists until that is addressed
if [[ "$TESTARGS" =~ "until-failure" ]]; then
    stestr run --slowest "$TESTRARGS"
else
    stestr run --slowest --subunit "$TESTRARGS" | subunit-trace -f
fi
