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
        SETUP_ENV_SCRIPT="./setup-${TOOZ_TEST_DRIVER[0]}-env.sh"
        [ -x  $SETUP_ENV_SCRIPT ] || unset SETUP_ENV_SCRIPT
        $SETUP_ENV_SCRIPT pifpaf -e TOOZ_TEST run "${TOOZ_TEST_DRIVER[@]}" -- $*
    done
    unset IFS
else
    for d in $TOOZ_TEST_URLS
    do
        TOOZ_TEST_URL=$d $*
    done
fi
