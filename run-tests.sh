#!/bin/bash

set -e
set -x

if [ -n "$TOOZ_TEST_DRIVERS" ]
then
    IFS=","
    for TOOZ_TEST_DRIVER in $TOOZ_TEST_DRIVERS
    do
        IFS=" "
        TOOZ_TEST_DRIVER=(${TOOZ_TEST_DRIVER})
        pifpaf -e TOOZ_TEST run "${TOOZ_TEST_DRIVER[@]}" -- $*
    done
    unset IFS
else
    for d in $TOOZ_TEST_URLS
    do
        TOOZ_TEST_URL=$d $*
    done
fi
