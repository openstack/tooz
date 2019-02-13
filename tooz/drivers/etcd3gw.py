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
import base64
import threading
import uuid

import etcd3gw
from etcd3gw import exceptions as etcd3_exc
from oslo_utils import encodeutils
import six

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils


def _encode(data):
    """Safely encode data for consumption of the gateway."""
    return base64.b64encode(data).decode("ascii")


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
        self._key_b64 = _encode(self._key)
        self._uuid = _encode(uuid.uuid4().bytes)
        self._exclusive_access = threading.Lock()

    @_translate_failures
    def acquire(self, blocking=True, shared=False):
        if shared:
            raise tooz.NotImplemented

        @_retry.retry(stop_max_delay=blocking)
        def _acquire():
            # TODO(jd): save the created revision so we can check it later to
            # make sure we still have the lock
            self._lease = self._coord.client.lease(self._timeout)
            txn = {
                'compare': [{
                    'key': self._key_b64,
                    'result': 'EQUAL',
                    'target': 'CREATE',
                    'create_revision': 0
                }],
                'success': [{
                    'request_put': {
                        'key': self._key_b64,
                        'value': self._uuid,
                        'lease': self._lease.id
                    }
                }],
                'failure': [{
                    'request_range': {
                        'key': self._key_b64
                    }
                }]
            }
            result = self._coord.client.transaction(txn)
            success = result.get('succeeded', False)

            if success is not True:
                if blocking is False:
                    return False
                raise _retry.TryAgain
            self._coord._acquired_locks.add(self)
            return True

        return _acquire()

    @_translate_failures
    def release(self):
        txn = {
            'compare': [{
                'key': self._key_b64,
                'result': 'EQUAL',
                'target': 'VALUE',
                'value': self._uuid
            }],
            'success': [{
                'request_delete_range': {
                    'key': self._key_b64
                }
            }]
        }

        with self._exclusive_access:
            result = self._coord.client.transaction(txn)
            success = result.get('succeeded', False)
            if success:
                self._coord._acquired_locks.remove(self)
                return True
        return False

    @_translate_failures
    def break_(self):
        if self._coord.client.delete(self._key):
            self._coord._acquired_locks.discard(self)
            return True
        return False

    @property
    def acquired(self):
        return self in self._coord._acquired_locks

    @_translate_failures
    def heartbeat(self):
        with self._exclusive_access:
            if self.acquired:
                self._lease.refresh()
                return True
        return False


class Etcd3Driver(coordination.CoordinationDriverWithExecutor):
    """An etcd based driver.

    This driver uses etcd provide the coordination driver semantics and
    required API(s).

    The Etcd driver connection URI should look like::

      etcd3+http://[HOST[:PORT]][?OPTION1=VALUE1[&OPTION2=VALUE2[&...]]]

    If not specified, HOST defaults to localhost and PORT defaults to 2379.
    Available options are:

    ==================  =======
    Name                Default
    ==================  =======
    protocol            http
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

    GROUP_PREFIX = b"tooz/groups/"

    def __init__(self, member_id, parsed_url, options):
        super(Etcd3Driver, self).__init__(member_id, parsed_url, options)
        host = parsed_url.hostname or self.DEFAULT_HOST
        port = parsed_url.port or self.DEFAULT_PORT
        options = utils.collapse(options)
        timeout = int(options.get('timeout', self.DEFAULT_TIMEOUT))
        self.client = etcd3gw.client(host=host, port=port, timeout=timeout)
        self.lock_timeout = int(options.get('lock_timeout', timeout))
        self.membership_timeout = int(options.get(
            'membership_timeout', timeout))
        self._acquired_locks = set()

    def _start(self):
        super(Etcd3Driver, self)._start()
        self._membership_lease = self.client.lease(self.membership_timeout)

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

    def _encode_group_id(self, group_id):
        return _encode(self._prefix_group(group_id))

    def _prefix_group(self, group_id):
        return b"%s%s/" % (self.GROUP_PREFIX, group_id)

    def create_group(self, group_id):
        @_translate_failures
        def _create_group():
            encoded_group = self._encode_group_id(group_id)
            txn = {
                'compare': [{
                    'key': encoded_group,
                    'result': 'EQUAL',
                    'target': 'VERSION',
                    'version': 0
                }],
                'success': [{
                    'request_put': {
                        'key': encoded_group,
                        # We shouldn't need a value, but etcd3gw needs it for
                        # now
                        'value': encoded_group
                    }
                }],
                'failure': []
            }
            result = self.client.transaction(txn)
            if not result.get("succeeded"):
                raise coordination.GroupAlreadyExist(group_id)

        return coordination.CoordinatorResult(
            self._executor.submit(_create_group))

    def _destroy_group(self, group_id):
        self.client.delete(group_id)

    def delete_group(self, group_id):
        @_translate_failures
        def _delete_group():
            prefix_group = self._prefix_group(group_id)
            members = self.client.get_prefix(prefix_group)
            if len(members) > 1:
                raise coordination.GroupNotEmpty(group_id)

            encoded_group = self._encode_group_id(group_id)
            txn = {
                'compare': [{
                    'key': encoded_group,
                    'result': 'NOT_EQUAL',
                    'target': 'VERSION',
                    'version': 0
                }],
                'success': [{
                    'request_delete_range': {
                        'key': encoded_group,
                    }
                }],
                'failure': []
            }
            result = self.client.transaction(txn)

            if not result.get("succeeded"):
                raise coordination.GroupNotCreated(group_id)

        return coordination.CoordinatorResult(
            self._executor.submit(_delete_group))

    def join_group(self, group_id, capabilities=b""):
        @_retry.retry()
        @_translate_failures
        def _join_group():
            prefix_group = self._prefix_group(group_id)
            prefix_member = prefix_group + self._member_id
            members = self.client.get_prefix(prefix_group)

            encoded_member = _encode(prefix_member)

            group_metadata = None
            for cap, metadata in members:
                if metadata['key'] == prefix_member:
                    raise coordination.MemberAlreadyExist(group_id,
                                                          self._member_id)
                if metadata['key'] == prefix_group:
                    group_metadata = metadata

            if group_metadata is None:
                raise coordination.GroupNotCreated(group_id)

            encoded_group = self._encode_group_id(group_id)
            txn = {
                'compare': [{
                    'key': encoded_group,
                    'result': 'EQUAL',
                    'target': 'VERSION',
                    'version': int(group_metadata['version'])
                }],
                'success': [{
                    'request_put': {
                        'key': encoded_member,
                        'value': _encode(utils.dumps(capabilities)),
                        'lease': self._membership_lease.id
                    }
                }],
                'failure': []
            }
            result = self.client.transaction(txn)
            if not result.get('succeeded'):
                raise _retry.TryAgain
            else:
                self._joined_groups.add(group_id)

        return coordination.CoordinatorResult(
            self._executor.submit(_join_group))

    def leave_group(self, group_id):
        @_translate_failures
        def _leave_group():
            prefix_group = self._prefix_group(group_id)
            prefix_member = prefix_group + self._member_id
            members = self.client.get_prefix(prefix_group)
            for capabilities, metadata in members:
                if metadata['key'] == prefix_member:
                    break
            else:
                raise coordination.MemberNotJoined(group_id,
                                                   self._member_id)

            self.client.delete(prefix_member)
            self._joined_groups.discard(group_id)

        return coordination.CoordinatorResult(
            self._executor.submit(_leave_group))

    def get_members(self, group_id):
        @_translate_failures
        def _get_members():
            prefix_group = self._prefix_group(group_id)
            members = set()
            group_found = False

            for cap, metadata in self.client.get_prefix(prefix_group):
                if metadata['key'] == prefix_group:
                    group_found = True
                else:
                    members.add(metadata['key'][len(prefix_group):])

            if not group_found:
                raise coordination.GroupNotCreated(group_id)

            return members

        return coordination.CoordinatorResult(
            self._executor.submit(_get_members))

    def get_member_capabilities(self, group_id, member_id):
        @_translate_failures
        def _get_member_capabilities():
            prefix_member = self._prefix_group(group_id) + member_id
            result = self.client.get(prefix_member)
            if not result:
                raise coordination.MemberNotJoined(group_id, member_id)
            return utils.loads(result[0])

        return coordination.CoordinatorResult(
            self._executor.submit(_get_member_capabilities))

    def update_capabilities(self, group_id, capabilities):
        @_translate_failures
        def _update_capabilities():
            prefix_member = self._prefix_group(group_id) + self._member_id
            result = self.client.get(prefix_member)
            if not result:
                raise coordination.MemberNotJoined(group_id, self._member_id)

            self.client.put(prefix_member, utils.dumps(capabilities),
                            lease=self._membership_lease)

        return coordination.CoordinatorResult(
            self._executor.submit(_update_capabilities))

    def get_groups(self):
        @_translate_failures
        def _get_groups():
            groups = self.client.get_prefix(self.GROUP_PREFIX)
            return [
                group[1]['key'][len(self.GROUP_PREFIX):-1] for group in groups]
        return coordination.CoordinatorResult(
            self._executor.submit(_get_groups))
