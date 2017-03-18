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

import etcd3
from etcd3 import exceptions as etcd3_exc
from oslo_utils import encodeutils
import six

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

    def __init__(self, etcd3_lock, coord):
        super(Etcd3Lock, self).__init__(etcd3_lock.name)
        self._lock = etcd3_lock
        self._coord = coord

    @_translate_failures
    def acquire(self, *args, **kwargs):
        res = self._lock.acquire()
        if res:
            self._coord._acquired_locks.add(self)
        return res

    @_translate_failures
    def release(self, *args, **kwargs):
        if self._lock.is_acquired():
            res = self._lock.release()
            if res:
                self._coord._acquired_locks.remove(self)

    @_translate_failures
    def break_(self):
        res = self._lock.release()
        if res:
            self._coord._acquired_locks.remove(self)

    @_translate_failures
    def heartbeat(self):
        return self._lock.refresh()


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
        etcd3_lock = self.client.lock(name, ttl=self.lock_timeout)
        return Etcd3Lock(etcd3_lock, self)

    def heartbeat(self):
        # NOTE(jaypipes): Copying because set can mutate during iteration
        for lock in self._acquired_locks.copy():
            lock.heartbeat()
        return self.lock_timeout
