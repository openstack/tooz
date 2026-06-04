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

from __future__ import annotations

import logging
from typing import Any

from oslo_utils import strutils
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

    def __init__(
        self, member_id: bytes, parsed_url: Any, options: dict[str, Any]
    ) -> None:
        super().__init__(member_id)
        self.acquired = False
        self._conn = MySQLDriver.get_connection(parsed_url, options, True)

    def acquire(
        self,
        blocking: bool = True,
        shared: bool = False,
        timeout: int | None = None,
    ) -> bool:
        if shared:
            raise tooz.NotImplemented("not implemented")

        @_retry.retry(stop_max_delay=blocking)
        def _lock(timeout: int | None) -> bool:
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

            _, timeout_ = utils.convert_blocking(blocking, timeout)
            timeout = int(timeout_) if timeout_ is not None else 0
            try:
                if not self._conn.open:
                    self._conn.connect()
                cur = self._conn.cursor()
                # FIXME(stephenfin): Should self.name be decoded?
                cur.execute("SELECT GET_LOCK(%s, %s);", (self.name, timeout))
                # Can return NULL on error
                ret = cur.fetchone()
                if ret is None:
                    raise tooz.ToozError("Got null response from database")
                if ret[0] == 1:
                    self.acquired = True
                    return True
            except pymysql.MySQLError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

            if blocking:
                raise _retry.TryAgain
            self._conn.close()
            return False

        try:
            return _lock(timeout)
        except Exception:
            # Close the connection if we tried too much and finally failed, or
            # anything else bad happened.
            self._conn.close()
            raise

    def release(self) -> bool:
        if not self.acquired:
            return False
        try:
            cur = self._conn.cursor()
            # FIXME(stephenfin): Should self.name be decoded?
            cur.execute("SELECT RELEASE_LOCK(%s);", self.name)
            cur.fetchone()
            self.acquired = False
            self._conn.close()
            return True
        except pymysql.MySQLError as e:
            utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

    def __del__(self) -> None:
        if self.acquired:
            LOG.warning("unreleased lock %s garbage collected", self.name)


class MySQLDriver(coordination.CoordinationDriver):
    """A `MySQL`_ based driver.

    This driver users `MySQL`_ database tables to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.

    The MySQL driver connection URI should look like::

      mysql://USERNAME:PASSWORD@HOST[:PORT]/DBNAME[?OPTION1=VALUE1[&OPTION2=VALUE2[&...]]]

    If not specified, PORT defaults to 3306.
    Available options are:

    ==================  =======
    Name                Default
    ==================  =======
    ssl_ca              None
    ssl_capath          None
    ssl_cert            None
    ssl_key             None
    ssl_cipher          None
    ssl_verify_mode     None
    ssl_check_hostname  True
    unix_socket         None
    ==================  =======

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

    def __init__(
        self, member_id: bytes, parsed_url: Any, options: Any
    ) -> None:
        """Initialize the MySQL driver."""
        super().__init__(member_id, parsed_url, options)
        self._parsed_url = parsed_url
        self._options = utils.collapse(options)

    def _start(self) -> None:
        self._conn = MySQLDriver.get_connection(
            self._parsed_url, self._options
        )

    def _stop(self) -> None:
        self._conn.close()

    def get_lock(self, name: bytes) -> MySQLLock:
        return MySQLLock(name, self._parsed_url, self._options)

    def watch_join_group(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.MemberJoinedGroup],
    ) -> None:
        raise tooz.NotImplemented("not implemented")

    def unwatch_join_group(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.MemberJoinedGroup],
    ) -> None:
        raise tooz.NotImplemented("not implemented")

    def watch_leave_group(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.MemberLeftGroup],
    ) -> None:
        raise tooz.NotImplemented("not implemented")

    def unwatch_leave_group(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.MemberLeftGroup],
    ) -> None:
        raise tooz.NotImplemented("not implemented")

    def watch_elected_as_leader(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.LeaderElected],
    ) -> None:
        raise tooz.NotImplemented("not implemented")

    def unwatch_elected_as_leader(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.LeaderElected],
    ) -> None:
        raise tooz.NotImplemented("not implemented")

    @staticmethod
    def get_connection(
        parsed_url: Any, options: Any, defer_connect: bool = False
    ) -> pymysql.Connection:
        host = parsed_url.hostname
        port = parsed_url.port or MySQLLock.MYSQL_DEFAULT_PORT
        dbname = parsed_url.path[1:]
        username = parsed_url.username
        password = parsed_url.password
        unix_socket = options.get("unix_socket")
        ssl_opt_names = (
            "ca",
            "capath",
            "cert",
            "key",
            "cipher",
            "verify_mode",
        )
        ssl_args = {}
        for o in ssl_opt_names:
            value = options.get("ssl_" + o)
            if value:
                ssl_args[o] = value
        check_hostname = options.get("ssl_check_hostname")
        # https://review.opendev.org/c/openstack/oslo.utils/+/980367
        check_hostname = strutils.bool_from_string(
            check_hostname,
            default=None,
        )
        if check_hostname is not None:
            ssl_args['check_hostname'] = check_hostname

        try:
            if unix_socket:
                return pymysql.Connect(
                    unix_socket=unix_socket,
                    port=port,
                    user=username,
                    passwd=password,
                    database=dbname,
                    ssl=ssl_args,
                    defer_connect=defer_connect,
                )
            else:
                return pymysql.Connect(
                    host=host,
                    port=port,
                    user=username,
                    passwd=password,
                    database=dbname,
                    ssl=ssl_args,
                    defer_connect=defer_connect,
                )
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
            utils.raise_with_cause(
                coordination.ToozConnectionError, str(e), cause=e
            )
