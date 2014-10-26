# -*- coding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Author: Julien Danjou <julien@danjou.info>
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
import hashlib

import psycopg2
import six

import tooz
from tooz import coordination
from tooz.drivers import _retry
from tooz import locking


class PostgresLock(locking.Lock):
    """A PostgreSQL based lock."""

    def __init__(self, name, connection):
        super(PostgresLock, self).__init__(name)
        self._conn = connection
        h = hashlib.md5()
        h.update(name)
        if six.PY2:
            self.key = list(map(ord, h.digest()[0:2]))
        else:
            self.key = h.digest()[0:2]

    def acquire(self, blocking=True):
        if blocking is True:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s, %s);", self.key)
            return True
        elif blocking is False:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s, %s);", self.key)
                return cur.fetchone()[0]
        else:
            def _acquire():
                with self._conn.cursor() as cur:
                    cur.execute("SELECT pg_try_advisory_lock(%s, %s);",
                                self.key)
                    if cur.fetchone()[0] is True:
                        return True
                    raise _retry.Retry
            kwargs = _retry.RETRYING_KWARGS.copy()
            kwargs['stop_max_delay'] = blocking
            return _retry.Retrying(**kwargs).call(_acquire)

    def release(self):
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s);", self.key)
            return cur.fetchone()[0]


class PostgresDriver(coordination.CoordinationDriver):

    def __init__(self, member_id, parsed_url, options):
        """Initialize the PostgreSQL driver."""
        super(PostgresDriver, self).__init__()
        self._host = options.get("host", [None])[-1]
        self._port = parsed_url.port or options.get("port", [None])[-1]
        self._dbname = parsed_url.path[1:] or options.get("dbname", [None])[-1]
        self._username = parsed_url.username
        self._password = parsed_url.password

    def _start(self):
        self._conn = psycopg2.connect(host=self._host,
                                      port=self._port,
                                      user=self._username,
                                      password=self._password,
                                      database=self._dbname)

    def _stop(self):
        self._conn.close()

    def get_lock(self, name):
        return PostgresLock(name, self._conn)

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
