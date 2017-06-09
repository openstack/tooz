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
import logging

from oslo_utils import encodeutils
import pymysql

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils

LOG = logging.getLogger(__name__)


class MySQLLock(locking.Lock):
    """A MySQL based lock."""

    MYSQL_DEFAULT_PORT = 3306

    def __init__(self, name, parsed_url, options):
        super(MySQLLock, self).__init__(name)
        self.acquired = False
        self._conn = MySQLDriver.get_connection(parsed_url, options, True)

    def acquire(self, blocking=True, shared=False):

        if shared:
            raise tooz.NotImplemented

        @_retry.retry(stop_max_delay=blocking)
        def _lock():
            # NOTE(sileht): mysql-server (<5.7.5) allows only one lock per
            # connection at a time:
            #  select GET_LOCK("a", 0);
            #  select GET_LOCK("b", 0); <-- this release lock "a" ...
            # Or
            #  select GET_LOCK("a", 0);
            #  select GET_LOCK("a", 0); release and lock again "a"
            #
            # So, we track locally the lock status with self.acquired
            if self.acquired is True:
                if blocking:
                    raise _retry.TryAgain
                return False

            try:
                if not self._conn.open:
                    self._conn.connect()
                with self._conn as cur:
                    cur.execute("SELECT GET_LOCK(%s, 0);", self.name)
                    # Can return NULL on error
                    if cur.fetchone()[0] is 1:
                        self.acquired = True
                        return True
            except pymysql.MySQLError as e:
                utils.raise_with_cause(
                    tooz.ToozError,
                    encodeutils.exception_to_unicode(e),
                    cause=e)

            if blocking:
                raise _retry.TryAgain
            self._conn.close()
            return False

        try:
            return _lock()
        except Exception:
            # Close the connection if we tried too much and finally failed, or
            # anything else bad happened.
            self._conn.close()
            raise

    def release(self):
        if not self.acquired:
            return False
        try:
            with self._conn as cur:
                cur.execute("SELECT RELEASE_LOCK(%s);", self.name)
                cur.fetchone()
            self.acquired = False
            self._conn.close()
            return True
        except pymysql.MySQLError as e:
            utils.raise_with_cause(tooz.ToozError,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)

    def __del__(self):
        if self.acquired:
            LOG.warning("unreleased lock %s garbage collected", self.name)


class MySQLDriver(coordination.CoordinationDriver):
    """A `MySQL`_ based driver.

    This driver users `MySQL`_ database tables to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.

    .. _MySQL: http://dev.mysql.com/
    """

    CHARACTERISTICS = (
        coordination.Characteristics.NON_TIMEOUT_BASED,
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
        coordination.Characteristics.DISTRIBUTED_ACROSS_HOSTS,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    def __init__(self, member_id, parsed_url, options):
        """Initialize the MySQL driver."""
        super(MySQLDriver, self).__init__(member_id)
        self._parsed_url = parsed_url
        self._options = utils.collapse(options)

    def _start(self):
        self._conn = MySQLDriver.get_connection(self._parsed_url,
                                                self._options)

    def _stop(self):
        self._conn.close()

    def get_lock(self, name):
        return MySQLLock(name, self._parsed_url, self._options)

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
    def get_connection(parsed_url, options, defer_connect=False):
        host = parsed_url.hostname
        port = parsed_url.port or MySQLLock.MYSQL_DEFAULT_PORT
        dbname = parsed_url.path[1:]
        username = parsed_url.username
        password = parsed_url.password
        unix_socket = options.get("unix_socket")

        try:
            if unix_socket:
                return pymysql.Connect(unix_socket=unix_socket,
                                       port=port,
                                       user=username,
                                       passwd=password,
                                       database=dbname,
                                       defer_connect=defer_connect)
            else:
                return pymysql.Connect(host=host,
                                       port=port,
                                       user=username,
                                       passwd=password,
                                       database=dbname,
                                       defer_connect=defer_connect)
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
            utils.raise_with_cause(coordination.ToozConnectionError,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)
