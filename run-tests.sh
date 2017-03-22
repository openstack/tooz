#!/bin/sh
set -e
set -x

for d in $TOOZ_TEST_URLS
do
    TOOZ_TEST_URL=$d $*
done
