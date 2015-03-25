# -*- coding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import pymysql

import tooz
from tooz import coordination
from tooz.drivers import _retry
from tooz import locking
from tooz import utils


class MySQLLock(locking.Lock):
    """A MySQL based lock."""

    def __init__(self, name, parsed_url, options):
        super(MySQLLock, self).__init__(name)
        self._conn = MySQLDriver.get_connection(parsed_url, options)

    def acquire(self, blocking=True):
        def _acquire(retry=False):
            try:
                with self._conn as cur:
                    cur.execute("SELECT GET_LOCK(%s, 0);", self.name)
                    # Can return NULL on error
                    if cur.fetchone()[0] is 1:
                        return True
            except pymysql.MySQLError as e:
                raise coordination.ToozError(utils.exception_message(e))
            if retry:
                raise _retry.Retry
            else:
                return False

        if blocking is False:
            return _acquire()
        else:
            kwargs = _retry.RETRYING_KWARGS.copy()
            if blocking is not True:
                kwargs['stop_max_delay'] = blocking
            return _retry.Retrying(**kwargs).call(_acquire, retry=True)

    def release(self):
        try:
            with self._conn as cur:
                cur.execute("SELECT RELEASE_LOCK(%s);", self.name)
                return cur.fetchone()[0]
        except pymysql.MySQLError as e:
            raise coordination.ToozError(utils.exception_message(e))


class MySQLDriver(coordination.CoordinationDriver):
    """A mysql based driver."""

    def __init__(self, member_id, parsed_url, options):
        """Initialize the MySQL driver."""
        super(MySQLDriver, self).__init__()
        self._parsed_url = parsed_url
        self._options = options

    def _start(self):
        self._conn = MySQLDriver.get_connection(self._parsed_url,
                                                self._options)

    def _stop(self):
        self._conn.close()

    def get_lock(self, name):
        return locking.WeakLockHelper(
            self._parsed_url.geturl(),
            MySQLLock, name, self._parsed_url, self._options)

    @staticmethod
    def watch_join_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_join_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def watch_leave_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_leave_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def get_connection(parsed_url, options):
        host = parsed_url.hostname
        port = parsed_url.port
        dbname = parsed_url.path[1:]
        username = parsed_url.username
        password = parsed_url.password
        unix_socket = options.get("unix_socket", [None])[-1]

        try:
            if unix_socket:
                return pymysql.Connect(unix_socket=unix_socket,
                                       port=port,
                                       user=username,
                                       passwd=password,
                                       database=dbname)
            else:
                return pymysql.Connect(host=host,
                                       port=port,
                                       user=username,
                                       passwd=password,
                                       database=dbname)
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
            raise coordination.ToozConnectionError(utils.exception_message(e))
