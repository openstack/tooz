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
import contextlib
import functools
import threading

import etcd3
from etcd3 import exceptions as etcd3_exc
from oslo_utils import encodeutils
import six

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils


@contextlib.contextmanager
def _failure_translator():
    """Translates common requests exceptions into tooz exceptions."""
    try:
        yield
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


def _translate_failures(func):

    @six.wraps(func)
    def wrapper(*args, **kwargs):
        with _failure_translator():
            return func(*args, **kwargs)

    return wrapper


class Etcd3Lock(locking.Lock):
    """An etcd3-specific lock.

    Thin wrapper over etcd3's lock object basically to provide the heartbeat()
    semantics for the coordination driver.
    """

    LOCK_PREFIX = b"/tooz/locks"

    def __init__(self, coord, name, timeout):
        super(Etcd3Lock, self).__init__(name)
        self._coord = coord
        self._lock = coord.client.lock(name.decode(), timeout)
        self._exclusive_access = threading.Lock()

    @_translate_failures
    def acquire(self, blocking=True, shared=False):
        if shared:
            raise tooz.NotImplemented

        blocking, timeout = utils.convert_blocking(blocking)
        if blocking is False:
            timeout = 0

        if self._lock.acquire(timeout):
            self._coord._acquired_locks.add(self)
            return True

        return False

    @property
    def acquired(self):
        return self in self._coord._acquired_locks

    @_translate_failures
    def release(self):
        with self._exclusive_access:
            if self.acquired and self._lock.release():
                self._coord._acquired_locks.discard(self)
                return True
        return False

    @_translate_failures
    def heartbeat(self):
        with self._exclusive_access:
            if self.acquired:
                self._lock.refresh()
                return True
        return False


class Etcd3Driver(coordination.CoordinationDriverCachedRunWatchers,
                  coordination.CoordinationDriverWithExecutor):
    """An etcd based driver.

    This driver uses etcd provide the coordination driver semantics and
    required API(s).

    The Etcd driver connection URI should look like::

      etcd3://[HOST[:PORT]][?OPTION1=VALUE1[&OPTION2=VALUE2[&...]]]

    If not specified, HOST defaults to localhost and PORT defaults to 2379.
    Available options are:

    ==================  =======
    Name                Default
    ==================  =======
    ca_cert             None
    cert_key            None
    cert_cert           None
    timeout             30
    lock_timeout        30
    membership_timeout  30
    ==================  =======
    """

    #: Default socket/lock/member/leader timeout used when none is provided.
    DEFAULT_TIMEOUT = 30

    #: Default hostname used when none is provided.
    DEFAULT_HOST = "localhost"

    #: Default port used if none provided (4001 or 2379 are the common ones).
    DEFAULT_PORT = 2379

    def __init__(self, member_id, parsed_url, options):
        super(Etcd3Driver, self).__init__(member_id, parsed_url, options)
        host = parsed_url.hostname or self.DEFAULT_HOST
        port = parsed_url.port or self.DEFAULT_PORT
        options = utils.collapse(options)
        ca_cert = options.get('ca_cert')
        cert_key = options.get('cert_key')
        cert_cert = options.get('cert_cert')
        timeout = int(options.get('timeout', self.DEFAULT_TIMEOUT))
        self.client = etcd3.client(host=host,
                                   port=port,
                                   ca_cert=ca_cert,
                                   cert_key=cert_key,
                                   cert_cert=cert_cert,
                                   timeout=timeout)
        self.lock_timeout = int(options.get('lock_timeout', timeout))
        self.membership_timeout = int(options.get(
            'membership_timeout', timeout))
        self._acquired_locks = set()

    def _start(self):
        super(Etcd3Driver, self)._start()
        self._membership_lease = self.client.lease(self.membership_timeout)

    def _stop(self):
        super(Etcd3Driver, self)._stop()
        self._membership_lease.revoke()

    def get_lock(self, name):
        return Etcd3Lock(self, name, self.lock_timeout)

    def heartbeat(self):
        # NOTE(jaypipes): Copying because set can mutate during iteration
        for lock in self._acquired_locks.copy():
            lock.heartbeat()
        # TODO(jd) use the same lease for locks?
        self._membership_lease.refresh()
        return min(self.lock_timeout, self.membership_timeout)

    GROUP_PREFIX = b"tooz/groups/"

    def _encode_group_id(self, group_id):
        return self.GROUP_PREFIX + utils.to_binary(group_id) + b"/"

    def _encode_group_member_id(self, group_id, member_id):
        return self._encode_group_id(group_id) + utils.to_binary(member_id)

    def create_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        @_translate_failures
        def _create_group():
            status, results = self.client.transaction(
                compare=[
                    self.client.transactions.version(
                        encoded_group) == 0
                ],
                success=[
                    self.client.transactions.put(encoded_group, b"")
                ],
                failure=[],
            )
            if not status:
                raise coordination.GroupAlreadyExist(group_id)

        return EtcdFutureResult(self._executor.submit(_create_group))

    def _destroy_group(self, group_id):
        self.client.delete(self._encode_group_id(group_id))

    def delete_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        @_translate_failures
        def _delete_group():
            members = list(self.client.get_prefix(encoded_group))
            if len(members) > 1:
                raise coordination.GroupNotEmpty(group_id)

            # Warning: as of this writing python-etcd3 does not support the
            # NOT_EQUAL operator so we use the EQUAL operator and will retry on
            # success, hihi
            status, results = self.client.transaction(
                compare=[
                    self.client.transactions.version(encoded_group) == 0
                ],
                success=[],
                failure=[
                    self.client.transactions.delete(encoded_group)
                ],
            )
            if status:
                raise coordination.GroupNotCreated(group_id)

        return EtcdFutureResult(self._executor.submit(_delete_group))

    def join_group(self, group_id, capabilities=b""):
        encoded_group = self._encode_group_id(group_id)

        @_retry.retry()
        @_translate_failures
        def _join_group():
            members = list(self.client.get_prefix(encoded_group))

            encoded_member = self._encode_group_member_id(
                group_id, self._member_id)

            group_metadata = None
            for cap, metadata in members:
                if metadata.key == encoded_member:
                    raise coordination.MemberAlreadyExist(group_id,
                                                          self._member_id)
                if metadata.key == encoded_group:
                    group_metadata = metadata

            if group_metadata is None:
                raise coordination.GroupNotCreated(group_id)

            status, results = self.client.transaction(
                # This comparison makes sure the group has not been deleted in
                # the mean time
                compare=[
                    self.client.transactions.version(encoded_group) ==
                    group_metadata.version
                ],
                success=[
                    self.client.transactions.put(encoded_member,
                                                 utils.dumps(capabilities),
                                                 lease=self._membership_lease)
                ],
                failure=[],
            )
            if not status:
                # TODO(jd) There's a small optimization doable by getting the
                # current range on failure and passing it to this function as
                # the first arg when retrying to avoid redoing a get_prefix()
                raise _retry.TryAgain

        return EtcdFutureResult(self._executor.submit(_join_group))

    def leave_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        @_translate_failures
        def _leave_group():
            members = list(self.client.get_prefix(encoded_group))

            encoded_member = self._encode_group_member_id(
                group_id, self._member_id)

            for capabilities, metadata in members:
                if metadata.key == encoded_member:
                    break
            else:
                raise coordination.MemberNotJoined(group_id,
                                                   self._member_id)

            self.client.delete(encoded_member)

        return EtcdFutureResult(self._executor.submit(_leave_group))

    def get_members(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        @_translate_failures
        def _get_members():
            members = set()
            group_found = False

            for cap, metadata in self.client.get_prefix(encoded_group):
                if metadata.key == encoded_group:
                    group_found = True
                else:
                    members.add(metadata.key[len(encoded_group):])

            if not group_found:
                raise coordination.GroupNotCreated(group_id)

            return members

        return EtcdFutureResult(self._executor.submit(_get_members))

    def get_member_capabilities(self, group_id, member_id):
        encoded_member = self._encode_group_member_id(
            group_id, member_id)

        @_translate_failures
        def _get_member_capabilities():
            capabilities, metadata = self.client.get(encoded_member)
            if capabilities is None:
                raise coordination.MemberNotJoined(group_id, member_id)
            return utils.loads(capabilities)

        return EtcdFutureResult(
            self._executor.submit(_get_member_capabilities))

    def update_capabilities(self, group_id, capabilities):
        encoded_member = self._encode_group_member_id(
            group_id, self._member_id)

        @_translate_failures
        def _update_capabilities():
            cap, metadata = self.client.get(encoded_member)
            if cap is None:
                raise coordination.MemberNotJoined(group_id, self._member_id)

            self.client.put(encoded_member, utils.dumps(capabilities),
                            lease=self._membership_lease)

        return EtcdFutureResult(
            self._executor.submit(_update_capabilities))

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented


EtcdFutureResult = functools.partial(coordination.CoordinatorResult,
                                     failure_translator=_failure_translator)
