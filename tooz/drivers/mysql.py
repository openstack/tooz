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

    def __init__(self, name, connection):
        super(MySQLLock, self).__init__(name)
        self._conn = connection

    def acquire(self, blocking=True):
        if blocking is False:
            try:
                cur = self._conn.cursor()
                cur.execute("SELECT GET_LOCK(%s, 0);", self.name)
                # Can return NULL on error
                if cur.fetchone()[0] is 1:
                    return True
                return False
            except pymysql.MySQLError as e:
                raise coordination.ToozError(utils.exception_message(e))
        else:
            def _acquire():
                try:
                    cur = self._conn.cursor()
                    cur.execute("SELECT GET_LOCK(%s, 0);", self.name)
                    if cur.fetchone()[0] is 1:
                        return True
                except pymysql.MySQLError as e:
                    raise coordination.ToozError(utils.exception_message(e))
                raise _retry.Retry
            kwargs = _retry.RETRYING_KWARGS.copy()
            if blocking is not True:
                kwargs['stop_max_delay'] = blocking
            return _retry.Retrying(**kwargs).call(_acquire)

    def release(self):
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT RELEASE_LOCK(%s);", self.name)
            return cur.fetchone()[0]
        except pymysql.MySQLError as e:
            raise coordination.ToozError(utils.exception_message(e))


class MySQLDriver(coordination.CoordinationDriver):
    """A mysql based driver."""

    def __init__(self, member_id, parsed_url, options):
        """Initialize the MySQL driver."""
        super(MySQLDriver, self).__init__()
        self._host = parsed_url.netloc
        self._port = parsed_url.port
        self._dbname = parsed_url.path[1:]
        self._username = parsed_url.username
        self._password = parsed_url.password
        self._unix_socket = options.get("unix_socket", [None])[-1]

    def _start(self):
        try:
            if self._unix_socket:
                self._conn = pymysql.Connect(unix_socket=self._unix_socket,
                                             port=self._port,
                                             user=self._username,
                                             passwd=self._password,
                                             database=self._dbname)
            else:
                self._conn = pymysql.Connect(host=self._host,
                                             port=self._port,
                                             user=self._username,
                                             passwd=self._password,
                                             database=self._dbname)
        except pymysql.err.OperationalError as e:
            raise coordination.ToozConnectionError(utils.exception_message(e))

    def _stop(self):
        self._conn.close()

    def get_lock(self, name):
        return MySQLLock(name, self._conn)

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
