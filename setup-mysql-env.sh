#!/bin/bash
set -x -e

clean_exit () {
    local error_code="$?"
    kill $(jobs -p)
    rm -rf ${MYSQL_DATA}
    return $error_code
}

wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

trap "clean_exit" EXIT

# On systems like Fedora here's where mysqld can be found
export PATH=$PATH:/usr/libexec

# Start MySQL process for tests
MYSQL_DATA=`mktemp -d /tmp/tooz-mysql-XXXXX`
mkfifo ${MYSQL_DATA}/out
mysqld --datadir=${MYSQL_DATA} --pid-file=${MYSQL_DATA}/mysql.pid --socket=${MYSQL_DATA}/mysql.socket --skip-networking --skip-grant-tables &> ${MYSQL_DATA}/out &
# Wait for MySQL to start listening to connections
wait_for_line "mysqld: ready for connections." ${MYSQL_DATA}/out
mysql -S ${MYSQL_DATA}/mysql.socket -e 'CREATE DATABASE test;'
export TOOZ_TEST_MYSQL_URL="mysql://root@localhost/test?unix_socket=${MYSQL_DATA}/mysql.socket"

# Yield execution to venv command
$*
