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

import contextlib
import hashlib
import logging

from oslo_utils import encodeutils
import psycopg2
import six

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils

LOG = logging.getLogger(__name__)

# See: psycopg/diagnostics_type.c for what kind of fields these
# objects may have (things like 'schema_name', 'internal_query'
# and so-on which are useful for figuring out what went wrong...)
_DIAGNOSTICS_ATTRS = tuple([
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
])


def _format_exception(e):
    lines = [
        "%s: %s" % (type(e).__name__,
                    encodeutils.exception_to_unicode(e).strip()),
    ]
    if hasattr(e, 'pgcode') and e.pgcode is not None:
        lines.append("Error code: %s" % e.pgcode)
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
            diagnostic_lines.append("  %s = %s" (attr_name, attr_value))
        if diagnostic_lines:
            lines.append('Diagnostics:')
            lines.extend(diagnostic_lines)
    return "\n".join(lines)


@contextlib.contextmanager
def _translating_cursor(conn):
    try:
        with conn.cursor() as cur:
            yield cur
    except psycopg2.Error as e:
        utils.raise_with_cause(tooz.ToozError,
                               _format_exception(e),
                               cause=e)


class PostgresLock(locking.Lock):
    """A PostgreSQL based lock."""

    def __init__(self, name, parsed_url, options):
        super(PostgresLock, self).__init__(name)
        self.acquired = False
        self._conn = None
        self._parsed_url = parsed_url
        self._options = options
        h = hashlib.md5()
        h.update(name)
        if six.PY2:
            self.key = list(map(ord, h.digest()[0:2]))
        else:
            self.key = h.digest()[0:2]

    def acquire(self, blocking=True, shared=False):

        if shared:
            raise tooz.NotImplemented

        @_retry.retry(stop_max_delay=blocking)
        def _lock():
            # NOTE(sileht) One the same session the lock is not exclusive
            # so we track it internally if the process already has the lock.
            if self.acquired is True:
                if blocking:
                    raise _retry.TryAgain
                return False

            if not self._conn or self._conn.closed:
                self._conn = PostgresDriver.get_connection(self._parsed_url,
                                                           self._options)

            with _translating_cursor(self._conn) as cur:
                if blocking is True:
                    cur.execute("SELECT pg_advisory_lock(%s, %s);",
                                self.key)
                    cur.fetchone()
                    self.acquired = True
                    return True
                else:
                    cur.execute("SELECT pg_try_advisory_lock(%s, %s);",
                                self.key)
                    if cur.fetchone()[0] is True:
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
            self._conn.close()
            raise

    def release(self):
        if not self.acquired:
            return False

        with _translating_cursor(self._conn) as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s);", self.key)
            cur.fetchone()
        self.acquired = False
        self._conn.close()
        return True

    def __del__(self):
        if self.acquired:
            LOG.warning("unreleased lock %s garbage collected", self.name)


class PostgresDriver(coordination.CoordinationDriver):
    """A `PostgreSQL`_ based driver.

    This driver users `PostgreSQL`_ database tables to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.

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

    def __init__(self, member_id, parsed_url, options):
        """Initialize the PostgreSQL driver."""
        super(PostgresDriver, self).__init__(member_id)
        self._parsed_url = parsed_url
        self._options = utils.collapse(options)

    def _start(self):
        self._conn = self.get_connection(self._parsed_url, self._options)

    def _stop(self):
        self._conn.close()

    def get_lock(self, name):
        return PostgresLock(name, self._parsed_url, self._options)

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
        host = options.get("host") or parsed_url.hostname
        port = options.get("port") or parsed_url.port
        dbname = options.get("dbname") or parsed_url.path[1:]
        kwargs = {}
        if parsed_url.username is not None:
            kwargs["user"] = parsed_url.username
        if parsed_url.password is not None:
            kwargs["password"] = parsed_url.password

        try:
            return psycopg2.connect(host=host,
                                    port=port,
                                    database=dbname,
                                    **kwargs)
        except psycopg2.Error as e:
            utils.raise_with_cause(coordination.ToozConnectionError,
                                   _format_exception(e),
                                   cause=e)
