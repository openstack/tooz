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

import collections
import logging

from concurrent import futures
import pymemcache.client
import six

from tooz import coordination
from tooz.drivers import _retry
from tooz import locking
from tooz import utils


LOG = logging.getLogger(__name__)


class MemcachedLock(locking.Lock):
    _LOCK_PREFIX = b'__TOOZ_LOCK_'

    def __init__(self, coord, name, timeout):
        super(MemcachedLock, self).__init__(self._LOCK_PREFIX + name)
        self.coord = coord
        self.timeout = timeout

    def acquire(self, blocking=True):
        @_retry.retry(stop_max_delay=blocking)
        def _acquire():
            if self.coord.client.add(
                    self.name,
                    self.coord._member_id,
                    expire=self.timeout,
                    noreply=False):
                self.coord._acquired_locks.append(self)
                return True
            if blocking is False:
                return False
            raise _retry.Retry
        return _acquire()

    def release(self):
        if self.coord.client.delete(self.name, noreply=False):
            self.coord._acquired_locks.remove(self)
            return True
        else:
            return False

    def heartbeat(self):
        """Keep the lock alive."""
        poked = self.coord.client.touch(self.name,
                                        expire=self.timeout,
                                        noreply=False)
        if not poked:
            LOG.warn("Unable to heartbeat by updating key '%s' with extended"
                     " expiry of %s seconds", self.name, self.timeout)

    def get_owner(self):
        return self.coord.client.get(self.name)


class MemcachedDriver(coordination.CoordinationDriver):
    """A memcached based driver."""

    _GROUP_PREFIX = b'_TOOZ_GROUP_'
    _GROUP_LEADER_PREFIX = b'_TOOZ_GROUP_LEADER_'
    _MEMBER_PREFIX = b'_TOOZ_MEMBER_'
    _GROUP_LIST_KEY = b'_TOOZ_GROUP_LIST'

    def __init__(self, member_id, parsed_url, options):
        super(MemcachedDriver, self).__init__()
        self._member_id = member_id
        self._groups = set()
        self._executor = None
        self.host = (parsed_url.hostname or "localhost",
                     parsed_url.port or 11211)
        default_timeout = options.get('timeout', ['30'])
        self.timeout = int(default_timeout[-1])
        self.membership_timeout = int(options.get(
            'membership_timeout', default_timeout)[-1])
        self.lock_timeout = int(options.get(
            'lock_timeout', default_timeout)[-1])
        self.leader_timeout = int(options.get(
            'leader_timeout', default_timeout)[-1])
        self._acquired_locks = []

    @staticmethod
    def _msgpack_serializer(key, value):
        if isinstance(value, six.binary_type):
            return value, 1
        return utils.dumps(value), 2

    @staticmethod
    def _msgpack_deserializer(key, value, flags):
        if flags == 1:
            return value
        if flags == 2:
            return utils.loads(value)
        raise Exception("Unknown serialization format '%s'" % flags)

    def _start(self):
        try:
            self.client = pymemcache.client.Client(
                self.host,
                serializer=self._msgpack_serializer,
                deserializer=self._msgpack_deserializer,
                timeout=self.timeout,
                connect_timeout=self.timeout)
            # Run heartbeat here because pymemcache use a lazy connection
            # method and only connect once you do an operation.
            self.heartbeat()
        except Exception as e:
            raise coordination.ToozConnectionError(utils.exception_message(e))
        self._group_members = collections.defaultdict(set)
        self._executor = futures.ThreadPoolExecutor(max_workers=1)

    def _stop(self):
        for lock in list(self._acquired_locks):
            lock.release()
        self.client.delete(self._encode_member_id(self._member_id))
        for g in list(self._groups):
            try:
                self.leave_group(g).get()
            except coordination.ToozError:
                LOG.warning("Unable to leave group '%s'", g, exc_info=True)
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        self.client.close()

    def _encode_group_id(self, group_id):
        return self._GROUP_PREFIX + group_id

    def _encode_member_id(self, member_id):
        return self._MEMBER_PREFIX + member_id

    def _encode_group_leader(self, group_id):
        return self._GROUP_LEADER_PREFIX + group_id

    @_retry.retry()
    def _add_group_to_group_list(self, group_id):
        """Add group to the group list.

        :param group_id: The group id
        """
        group_list, cas = self.client.gets(self._GROUP_LIST_KEY)
        if cas:
            group_list = set(group_list)
            group_list.add(group_id)
            if not self.client.cas(self._GROUP_LIST_KEY,
                                   list(group_list), cas):
                # Someone updated the group list before us, try again!
                raise _retry.Retry
        else:
            if not self.client.add(self._GROUP_LIST_KEY,
                                   [group_id], noreply=False):
                # Someone updated the group list before us, try again!
                raise _retry.Retry

    @_retry.retry()
    def _remove_from_group_list(self, group_id):
        """Remove group from the group list.

        :param group_id: The group id
        """
        group_list, cas = self.client.gets(self._GROUP_LIST_KEY)
        group_list = set(group_list)
        group_list.remove(group_id)
        if not self.client.cas(self._GROUP_LIST_KEY,
                               list(group_list), cas):
            # Someone updated the group list before us, try again!
            raise _retry.Retry

    def create_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        def _create_group():
            if not self.client.add(encoded_group, {}, noreply=False):
                raise coordination.GroupAlreadyExist(group_id)
            self._add_group_to_group_list(group_id)

        return MemcachedFutureResult(self._executor.submit(_create_group))

    def get_groups(self):
        def _get_groups():
            return self.client.get(self._GROUP_LIST_KEY) or []
        return MemcachedFutureResult(self._executor.submit(_get_groups))

    def join_group(self, group_id, capabilities=b""):
        encoded_group = self._encode_group_id(group_id)

        @_retry.retry()
        def _join_group():
            group_members, cas = self.client.gets(encoded_group)
            if group_members is None:
                raise coordination.GroupNotCreated(group_id)
            if self._member_id in group_members:
                raise coordination.MemberAlreadyExist(group_id,
                                                      self._member_id)
            group_members[self._member_id] = {
                b"capabilities": capabilities,
            }
            if not self.client.cas(encoded_group, group_members, cas):
                # It changed, let's try again
                raise _retry.Retry
            self._groups.add(group_id)

        return MemcachedFutureResult(self._executor.submit(_join_group))

    def leave_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        @_retry.retry()
        def _leave_group():
            group_members, cas = self.client.gets(encoded_group)
            if group_members is None:
                raise coordination.GroupNotCreated(group_id)
            if self._member_id not in group_members:
                raise coordination.MemberNotJoined(group_id, self._member_id)
            del group_members[self._member_id]
            if not self.client.cas(encoded_group, group_members, cas):
                # It changed, let's try again
                raise _retry.Retry
            self._groups.discard(group_id)

        return MemcachedFutureResult(self._executor.submit(_leave_group))

    def _destroy_group(self, group_id):
        self.client.delete(self._encode_group_id(group_id))

    def delete_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        @_retry.retry()
        def _delete_group():
            group_members, cas = self.client.gets(encoded_group)
            if group_members is None:
                raise coordination.GroupNotCreated(group_id)
            if group_members != {}:
                raise coordination.GroupNotEmpty(group_id)
            # Delete is not atomic, so we first set the group to
            # using CAS, and then we delete it, to avoid race conditions.
            if not self.client.cas(encoded_group, None, cas):
                raise _retry.Retry
            self.client.delete(encoded_group)
            self._remove_from_group_list(group_id)

        return MemcachedFutureResult(self._executor.submit(_delete_group))

    @_retry.retry()
    def _get_members(self, group_id):
        encoded_group = self._encode_group_id(group_id)
        group_members, cas = self.client.gets(encoded_group)
        if group_members is None:
            raise coordination.GroupNotCreated(group_id)
        actual_group_members = {}
        for m, v in six.iteritems(group_members):
            # Never kick self from the group, we know we're alive
            if (m == self._member_id
               or self.client.get(self._encode_member_id(m))):
                actual_group_members[m] = v
        if group_members != actual_group_members:
            # There are some dead members, update the group
            if not self.client.cas(encoded_group, actual_group_members, cas):
                # It changed, let's try again
                raise _retry.Retry
        return actual_group_members

    def get_members(self, group_id):
        def _get_members():
            return self._get_members(group_id).keys()
        return MemcachedFutureResult(self._executor.submit(_get_members))

    def get_member_capabilities(self, group_id, member_id):
        def _get_member_capabilities():
            group_members = self._get_members(group_id)
            if member_id not in group_members:
                raise coordination.MemberNotJoined(group_id, member_id)
            return group_members[member_id][b'capabilities']
        return MemcachedFutureResult(
            self._executor.submit(_get_member_capabilities))

    def update_capabilities(self, group_id, capabilities):
        encoded_group = self._encode_group_id(group_id)

        @_retry.retry()
        def _update_capabilities():
            group_members, cas = self.client.gets(encoded_group)
            if group_members is None:
                raise coordination.GroupNotCreated(group_id)
            if self._member_id not in group_members:
                raise coordination.MemberNotJoined(group_id, self._member_id)
            group_members[self._member_id][b'capabilities'] = capabilities
            if not self.client.cas(encoded_group, group_members, cas):
                # It changed, try again
                raise _retry.Retry

        return MemcachedFutureResult(
            self._executor.submit(_update_capabilities))

    def get_leader(self, group_id):
        def _get_leader():
            return self._get_leader_lock(group_id).get_owner()
        return MemcachedFutureResult(self._executor.submit(_get_leader))

    def heartbeat(self):
        self.client.set(self._encode_member_id(self._member_id),
                        "It's alive!",
                        expire=self.membership_timeout)
        # Reset the acquired locks
        for lock in self._acquired_locks:
            lock.heartbeat()

    def _init_watch_group(self, group_id):
        members = self.client.get(self._encode_group_id(group_id))
        if members is None:
            raise coordination.GroupNotCreated(group_id)
        # Initialize with the current group member list
        if group_id not in self._group_members:
            self._group_members[group_id] = set(members.keys())

    def watch_join_group(self, group_id, callback):
        self._init_watch_group(group_id)
        return super(MemcachedDriver, self).watch_join_group(
            group_id, callback)

    def unwatch_join_group(self, group_id, callback):
        return super(MemcachedDriver, self).unwatch_join_group(
            group_id, callback)

    def watch_leave_group(self, group_id, callback):
        self._init_watch_group(group_id)
        return super(MemcachedDriver, self).watch_leave_group(
            group_id, callback)

    def unwatch_leave_group(self, group_id, callback):
        return super(MemcachedDriver, self).unwatch_leave_group(
            group_id, callback)

    def watch_elected_as_leader(self, group_id, callback):
        return super(MemcachedDriver, self).watch_elected_as_leader(
            group_id, callback)

    def unwatch_elected_as_leader(self, group_id, callback):
        return super(MemcachedDriver, self).unwatch_elected_as_leader(
            group_id, callback)

    def get_lock(self, name):
        return MemcachedLock(self, name, self.lock_timeout)

    def _get_leader_lock(self, group_id):
        return MemcachedLock(self, self._encode_group_leader(group_id),
                             self.leader_timeout)

    def run_watchers(self):
        result = []
        for group_id in self.client.get(self._GROUP_LIST_KEY):
            try:
                group_members = set(self._get_members(group_id))
            except coordination.GroupNotCreated:
                group_members = set()
            old_group_members = self._group_members[group_id]

            for member_id in (group_members - old_group_members):
                result.extend(
                    self._hooks_join_group[group_id].run(
                        coordination.MemberJoinedGroup(group_id,
                                                       member_id)))

            for member_id in (old_group_members - group_members):
                result.extend(
                    self._hooks_leave_group[group_id].run(
                        coordination.MemberLeftGroup(group_id,
                                                     member_id)))

            self._group_members[group_id] = group_members

        for group_id, hooks in six.iteritems(self._hooks_elected_leader):
            # Try to grab the lock, if that fails, that means someone has it
            # already.
            if self._get_leader_lock(group_id).acquire(blocking=False):
                # We got the lock
                hooks.run(coordination.LeaderElected(
                    group_id,
                    self._member_id))

        return result


class MemcachedFutureResult(coordination.CoordAsyncResult):
    """Memcached asynchronous result that references a future."""
    def __init__(self, fut):
        self._fut = fut

    def get(self, timeout=10):
        try:
            return self._fut.result(timeout=timeout)
        except futures.TimeoutError as e:
            raise coordination.OperationTimedOut(utils.exception_message(e))

    def done(self):
        return self._fut.done()
