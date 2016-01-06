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

get_random_port () {
    PORT=13306
    while netstat -atwn | grep "^.*:${PORT}.*:\*\s*LISTEN\s*$"
    do
        PORT=$(( ${PORT} + 1 ))
    done
}

trap "clean_exit" EXIT

# On systems like Fedora here's where mysqld can be found
export PATH=$PATH:/usr/libexec

# Start MySQL process for tests
MYSQL_DATA=`mktemp -d -t tooz-mysql-XXXXX`
mkfifo ${MYSQL_DATA}/out
# Initialize MySQL Data Directory
mysql_install_db --user=${USER} --ldata=${MYSQL_DATA}
# Get random unused port for mysql
get_random_port
# Start mysqld with networking (i.e. - allow connection with TCP/IP)
mysqld --no-defaults --datadir=${MYSQL_DATA} --port=${PORT} --pid-file=${MYSQL_DATA}/mysql.pid --socket=${MYSQL_DATA}/mysql.socket --slow-query-log-file=${MYSQL_DATA}/mysql-slow.log --skip-grant-tables &> ${MYSQL_DATA}/out &
# Wait for MySQL to start listening to connections
wait_for_line "mysqld: ready for connections." ${MYSQL_DATA}/out
# The default root password is blank
mysql --host=localhost -S ${MYSQL_DATA}/mysql.socket -e 'CREATE DATABASE IF NOT EXISTS test;'
export TOOZ_TEST_MYSQL_URL="mysql://root@localhost:${PORT}/test"

# Yield execution to venv command
$*
