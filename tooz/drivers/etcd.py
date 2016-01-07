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

import logging

from oslo_utils import encodeutils
from oslo_utils import timeutils
import requests
import six

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils


LOG = logging.getLogger(__name__)


def _translate_failures(func):
    """Translates common requests exceptions into tooz exceptions."""

    @six.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            coordination.raise_with_cause(coordination.ToozConnectionError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    return wrapper


class _Client(object):
    def __init__(self, host, port, protocol):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.session = requests.Session()

    @property
    def base_url(self):
        return self.protocol + '://' + self.host + ':' + str(self.port)

    def get_url(self, path):
        return self.base_url + '/v2/' + path.lstrip("/")

    def get(self, url, **kwargs):
        return self.session.get(self.get_url(url), **kwargs).json()

    def put(self, url, **kwargs):
        return self.session.put(self.get_url(url), **kwargs).json()

    def delete(self, url, **kwargs):
        return self.session.delete(self.get_url(url), **kwargs).json()

    def self_stats(self):
        return self.session.get(self.get_url("/stats/self"))


class EtcdLock(locking.Lock):

    _TOOZ_LOCK_PREFIX = "tooz_locks"

    def __init__(self, name, coord, client, ttl=60):
        super(EtcdLock, self).__init__(name)
        self.client = client
        self.coord = coord
        self.lock = None
        self.ttl = ttl
        # NOTE(jd) sha1 of the name to be sure it works with any string?
        self._lock_url = (
            "/keys/" + self._TOOZ_LOCK_PREFIX + "/" + name.decode('ascii')
        )

    def acquire(self, blocking=True):
        if isinstance(blocking, bool):
            watch = None
        else:
            watch = timeutils.StopWatch(duration=blocking)
            watch.start()

        while True:
            try:
                reply = self.client.put(
                    self._lock_url,
                    timeout=watch.leftover() if watch else None,
                    data={"ttl": self.ttl,
                          "prevExist": "false"})
            except requests.exceptions.RequestException:
                if watch and watch.leftover() == 0:
                    return False

            # We got the lock!
            if reply.get("errorCode") is None:
                self.coord._acquired_locks.append(self)
                return True

            # We didn't get the lock and we don't want to wait
            if blocking is False:
                return False

            # Ok, so let's wait a bit (or forever!)
            try:
                reply = self.client.get(
                    self._lock_url +
                    "?wait=true&waitIndex=%d" % reply['index'],
                    timeout=watch.leftover() if watch else None)
            except requests.exceptions.RequestException:
                if watch and watch.expired():
                    return False

    @_translate_failures
    def release(self):
        if self in self.coord._acquired_locks:
            reply = self.client.delete(self._lock_url)
            if reply.get("errorCode") is None:
                self.coord._acquired_locks.remove(self)
                return True
        return False

    @_translate_failures
    def heartbeat(self):
        """Keep the lock alive."""
        poked = self.client.put(self._lock_url,
                                data={"ttl": self.ttl,
                                      "prevExist": "true"})
        errorcode = poked.get("errorCode")
        if errorcode:
            LOG.warn("Unable to heartbeat by updating key '%s' with extended"
                     " expiry of %s seconds: %d, %s", self.name, self.ttl,
                     errorcode, poked.get("message"))


class EtcdDriver(coordination.CoordinationDriver):
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
        super(EtcdDriver, self).__init__()
        host = parsed_url.hostname or self.DEFAULT_HOST
        port = parsed_url.port or self.DEFAULT_PORT
        options = utils.collapse(options)
        self.client = _Client(host=host, port=port,
                              protocol=options.get('protocol', 'http'))
        default_timeout = options.get('timeout', self.DEFAULT_TIMEOUT)
        self.lock_timeout = int(options.get(
            'lock_timeout', default_timeout))
        self._acquired_locks = []

    def _start(self):
        try:
            self.client.self_stats()
        except requests.exceptions.ConnectionError as e:
            raise coordination.ToozConnectionError(
                encodeutils.exception_to_unicode(e))

    def get_lock(self, name):
        return EtcdLock(name, self, self.client, self.lock_timeout)

    def heartbeat(self):
        for lock in self._acquired_locks:
            lock.heartbeat()

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
