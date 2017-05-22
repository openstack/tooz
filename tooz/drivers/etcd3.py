# -*- coding: utf-8 -*-
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

from __future__ import absolute_import
import uuid

import etcd3
from etcd3 import exceptions as etcd3_exc
from oslo_utils import encodeutils
import six

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils


def _translate_failures(func):
    """Translates common requests exceptions into tooz exceptions."""

    @six.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except etcd3_exc.ConnectionFailedError as e:
            utils.raise_with_cause(coordination.ToozConnectionError,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)
        except etcd3_exc.ConnectionTimeoutError as e:
            utils.raise_with_cause(coordination.OperationTimedOut,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)
        except etcd3_exc.Etcd3Exception as e:
            utils.raise_with_cause(coordination.ToozError,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)

    return wrapper


class Etcd3Lock(locking.Lock):
    """An etcd3-specific lock.

    Thin wrapper over etcd3's lock object basically to provide the heartbeat()
    semantics for the coordination driver.
    """

    LOCK_PREFIX = b"/tooz/locks"

    def __init__(self, coord, name, timeout):
        super(Etcd3Lock, self).__init__(name)
        self._timeout = timeout
        self._coord = coord
        self._key = self.LOCK_PREFIX + name
        self._uuid = uuid.uuid4().bytes
        self._lease = self._coord.client.lease(self._timeout)

    @_translate_failures
    def acquire(self, blocking=True, shared=False):
        if shared:
            raise tooz.NotImplemented

        @_retry.retry(stop_max_delay=blocking)
        def _acquire():
            # TODO(jd): save the created revision so we can check it later to
            # make sure we still have the lock
            success, _ = self._coord.client.transaction(
                compare=[
                    self._coord.client.transactions.create(self._key) == 0
                ],
                success=[
                    self._coord.client.transactions.put(self._key, self._uuid,
                                                        lease=self._lease)
                ],
                failure=[
                    self._coord.client.transactions.get(self._key)
                ],
            )
            if success is not True:
                if blocking is False:
                    return False
                raise _retry.TryAgain
            self._coord._acquired_locks.add(self)
            return True

        return _acquire()

    @_translate_failures
    def release(self):
        success, _ = self._coord.client.transaction(
            compare=[
                self._coord.client.transactions.value(self._key) == self._uuid
            ],
            success=[self._coord.client.transactions.delete(self._key)],
            failure=[],
        )
        if success:
            self._coord._acquired_locks.remove(self)
            return True
        return False

    @_translate_failures
    def break_(self):
        # FIXME(jd) when pyetcd3 returns the status
        # https://github.com/kragniz/python-etcd3/pull/126
        self._coord.client.delete(self._key)
        self._coord._acquired_locks.discard(self)
        return True

    @_translate_failures
    def heartbeat(self):
        self._lease.refresh()


class Etcd3Driver(coordination.CoordinationDriver):
    """An etcd based driver.

    This driver uses etcd provide the coordination driver semantics and
    required API(s).
    """

    #: Default socket/lock/member/leader timeout used when none is provided.
    DEFAULT_TIMEOUT = 30

    #: Default hostname used when none is provided.
    DEFAULT_HOST = "localhost"

    #: Default port used if none provided (4001 or 2379 are the common ones).
    DEFAULT_PORT = 2379

    def __init__(self, member_id, parsed_url, options):
        super(Etcd3Driver, self).__init__(member_id)
        host = parsed_url.hostname or self.DEFAULT_HOST
        port = parsed_url.port or self.DEFAULT_PORT
        options = utils.collapse(options)
        timeout = int(options.get('timeout', self.DEFAULT_TIMEOUT))
        self.client = etcd3.client(host=host, port=port, timeout=timeout)
        self.lock_timeout = int(options.get('lock_timeout', timeout))
        self._acquired_locks = set()

    def get_lock(self, name):
        return Etcd3Lock(self, name, self.lock_timeout)

    def heartbeat(self):
        # NOTE(jaypipes): Copying because set can mutate during iteration
        for lock in self._acquired_locks.copy():
            lock.heartbeat()
        return self.lock_timeout

    def watch_join_group(self, group_id, callback):
        raise tooz.NotImplemented

    def unwatch_join_group(self, group_id, callback):
        raise tooz.NotImplemented

    def watch_leave_group(self, group_id, callback):
        raise tooz.NotImplemented

    def unwatch_leave_group(self, group_id, callback):
        raise tooz.NotImplemented
