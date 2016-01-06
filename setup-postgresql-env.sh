#!/bin/bash
set -x -e

function clean_exit() {
    local error_code="$?"
    ${PGSQL_PATH}/pg_ctl -w -D ${PGSQL_DATA} -o "-p $PGSQL_PORT" stop
    rm -rf ${PGSQL_DATA}
    return $error_code
}

trap "clean_exit" EXIT

# Start PostgreSQL process for tests
PGSQL_DATA=`mktemp -d -t tooz-pgsql-XXXXX`
PGSQL_PATH=`pg_config --bindir`
PGSQL_PORT=9825
${PGSQL_PATH}/initdb ${PGSQL_DATA}
${PGSQL_PATH}/pg_ctl -w -D ${PGSQL_DATA} -o "-k ${PGSQL_DATA} -p ${PGSQL_PORT}" start
# Wait for PostgreSQL to start listening to connections
export TOOZ_TEST_PGSQL_URL="postgresql:///?host=${PGSQL_DATA}&port=${PGSQL_PORT}&dbname=template1"

# Yield execution to venv command
$*
