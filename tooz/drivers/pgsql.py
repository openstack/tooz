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

from collections.abc import Generator
import contextlib
import hashlib
import logging
import warnings

import psycopg2
import psycopg2.extensions
from typing import Any, cast

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils

LOG = logging.getLogger(__name__)

# See: psycopg/diagnostics_type.c for what kind of fields these
# objects may have (things like 'schema_name', 'internal_query'
# and so-on which are useful for figuring out what went wrong...)
_DIAGNOSTICS_ATTRS = tuple(
    [
        'column_name',
        'constraint_name',
        'context',
        'datatype_name',
        'internal_position',
        'internal_query',
        'message_detail',
        'message_hint',
        'message_primary',
        'schema_name',
        'severity',
        'source_file',
        'source_function',
        'source_line',
        'sqlstate',
        'statement_position',
        'table_name',
    ]
)


def _format_exception(e: Exception) -> str:
    lines = [
        f"{type(e).__name__}: {str(e).strip()}",
    ]
    if hasattr(e, 'pgcode') and e.pgcode is not None:
        lines.append(f"Error code: {e.pgcode}")
    # The reason this hasattr check is done is that the 'diag' may not always
    # be present, depending on how new of a psycopg is installed... so better
    # to be safe than sorry...
    if hasattr(e, 'diag') and e.diag is not None:
        diagnostic_lines = []
        for attr_name in _DIAGNOSTICS_ATTRS:
            if not hasattr(e.diag, attr_name):
                continue
            attr_value = getattr(e.diag, attr_name)
            if attr_value is None:
                continue
            diagnostic_lines.append(f"  {attr_name} = {attr_value}")
        if diagnostic_lines:
            lines.append('Diagnostics:')
            lines.extend(diagnostic_lines)
    return "\n".join(lines)


@contextlib.contextmanager
def _translating_cursor(
    conn: psycopg2.extensions.connection,
) -> Generator[psycopg2.extensions.cursor, None, None]:
    try:
        with conn.cursor() as cur:
            yield cur
    except psycopg2.Error as e:
        utils.raise_with_cause(tooz.ToozError, _format_exception(e), cause=e)


class PostgresLock(locking.Lock):
    """A PostgreSQL based lock."""

    def __init__(
        self, member_id: bytes, parsed_url: Any, options: dict[str, Any]
    ) -> None:
        super().__init__(member_id)
        self.acquired = False
        self._conn: psycopg2.extensions.connection | None = None
        self._parsed_url = parsed_url
        self._options = options
        h = hashlib.md5(usedforsecurity=False)
        h.update(member_id)
        self.key = h.digest()[0:2]

    def acquire(
        self,
        blocking: bool = True,
        shared: bool = False,
        timeout: int | None = None,
    ) -> bool:
        if shared:
            raise tooz.NotImplemented("not implemented")

        if timeout is not None:
            raise tooz.NotImplemented("not implemented")

        @_retry.retry(stop_max_delay=blocking)
        def _lock() -> bool:
            # NOTE(sileht) One the same session the lock is not exclusive
            # so we track it internally if the process already has the lock.
            if self.acquired is True:
                if blocking:
                    raise _retry.TryAgain
                return False

            if not self._conn or self._conn.closed:
                self._conn = PostgresDriver.get_connection(
                    self._parsed_url, self._options
                )

            with _translating_cursor(self._conn) as cur:
                if blocking is True:
                    cur.execute("SELECT pg_advisory_lock(%s, %s);", self.key)
                    cur.fetchone()
                    self.acquired = True
                    return True
                else:
                    cur.execute(
                        "SELECT pg_try_advisory_lock(%s, %s);", self.key
                    )
                    row = cur.fetchone()
                    if row is not None and row[0] is True:
                        self.acquired = True
                        return True
                    elif blocking is False:
                        self._conn.close()
                        return False
                    else:
                        raise _retry.TryAgain

        try:
            return _lock()
        except Exception:
            if self._conn:
                self._conn.close()
            raise

    def release(self) -> bool:
        if not self.acquired:
            return False

        assert self._conn is not None  # narrow type

        with _translating_cursor(self._conn) as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s);", self.key)
            cur.fetchone()
        self.acquired = False
        self._conn.close()
        return True

    def __del__(self) -> None:
        if self.acquired:
            LOG.warning("unreleased lock %s garbage collected", self.name)


class PostgresDriver(coordination.CoordinationDriver):
    """A `PostgreSQL`_ based driver.

    This driver users `PostgreSQL`_ database tables to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.

    The PostgreSQL driver connection URI should look like::

      postgresql://[USERNAME[:PASSWORD]@]HOST:PORT?dbname=DBNAME

    .. _PostgreSQL: http://www.postgresql.org/
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
        """Initialize the PostgreSQL driver."""
        warnings.warn(
            'PostgreSQL driver is deprecated and will be removed in '
            'a future release',
            category=DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(member_id, parsed_url, options)
        self._parsed_url = parsed_url
        self._options = utils.collapse(options)

    def _start(self) -> None:
        self._conn = self.get_connection(self._parsed_url, self._options)

    def _stop(self) -> None:
        self._conn.close()

    def get_lock(self, name: bytes) -> PostgresLock:
        return PostgresLock(name, self._parsed_url, self._options)

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

    # the connection object returned by psycopg2 is not part of the public API
    @staticmethod
    def get_connection(
        parsed_url: Any, options: Any
    ) -> psycopg2.extensions.connection:
        host = options.get("host") or parsed_url.hostname
        port = options.get("port") or parsed_url.port
        dbname = options.get("dbname") or parsed_url.path[1:]
        kwargs = {}
        if parsed_url.username is not None:
            kwargs["user"] = parsed_url.username
        if parsed_url.password is not None:
            kwargs["password"] = parsed_url.password

        try:
            return cast(
                psycopg2.extensions.connection,
                psycopg2.connect(
                    host=host, port=port, database=dbname, **kwargs
                ),
            )
        except psycopg2.Error as e:
            utils.raise_with_cause(
                coordination.ToozConnectionError, _format_exception(e), cause=e
            )
