# -*- coding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Author: Julien Danjou <julien@danjou.info>
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

import msgpack
import pymemcache.client
import retrying
import six

from tooz import coordination
from tooz import locking


class Retry(Exception):
    """Exception raised if we need to retry."""


def retry_if_retry_raised(exception):
    return isinstance(exception, Retry)


def retry(f):
    return retrying.retry(
        retry_on_exception=retry_if_retry_raised,
        wait='exponential_sleep', wait_exponential_max=1)(f)


class MemcachedLock(locking.Lock):
    _LOCK_PREFIX = b'__TOOZ_LOCK_'

    def __init__(self, coord, name, timeout):
        self.coord = coord
        self.name = self._LOCK_PREFIX + name
        self.timeout = timeout

    @retry
    def acquire(self, blocking=True):
        if self.coord.client.add(
                self.name,
                self.coord._member_id,
                expire=self.timeout,
                noreply=False):
            self.coord._acquired_locks.append(self)
            return True
        if not blocking:
            return False
        raise Retry

    def release(self):
        self.coord._acquired_locks.remove(self)
        self.coord.client.delete(self.name)

    def heartbeat(self):
        """Keep the lock alive."""
        self.coord.client.touch(self.name,
                                expire=self.timeout)


class MemcachedDriver(coordination.CoordinationDriver):

    _GROUP_PREFIX = b'_TOOZ_GROUP_'
    _GROUP_LEADER_PREFIX = b'_TOOZ_GROUP_LEADER_'
    _MEMBER_PREFIX = b'_TOOZ_MEMBER_'
    _GROUP_LIST_KEY = b'_TOOZ_GROUP_LIST'

    def __init__(self, member_id, parsed_url, options):
        super(MemcachedDriver, self).__init__()
        self._member_id = member_id
        self._groups = set()
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

    @staticmethod
    def _msgpack_serializer(key, value):
        if isinstance(value, six.binary_type):
            return value, 1
        return msgpack.dumps(value), 2

    @staticmethod
    def _msgpack_deserializer(key, value, flags):
        if flags == 1:
            return value
        if flags == 2:
            return msgpack.loads(value)
        raise Exception("Unknown serialization format")

    def start(self):
        try:
            self.client = pymemcache.client.Client(
                self.host,
                serializer=self._msgpack_serializer,
                deserializer=self._msgpack_deserializer,
                timeout=self.timeout,
                connect_timeout=self.timeout)
        except Exception as e:
            raise coordination.ToozConnectionError(e)
        self._group_members = collections.defaultdict(set)
        self._acquired_locks = []
        self.heartbeat()

    def stop(self):
        self.client.delete(self._encode_member_id(self._member_id))
        map(self.leave_group, list(self._groups))

        for group_id in six.iterkeys(self._hooks_elected_leader):
            if self.get_leader(group_id).get() == self._member_id:
                self.client.delete(self._encode_group_leader(group_id))

        self.client.close()

    def _encode_group_id(self, group_id):
        return self._GROUP_PREFIX + group_id

    def _encode_member_id(self, member_id):
        return self._MEMBER_PREFIX + member_id

    def _encode_group_leader(self, group_id):
        return self._GROUP_LEADER_PREFIX + group_id

    @retry
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
                raise Retry
        else:
            if not self.client.add(self._GROUP_LIST_KEY,
                                   [group_id], noreply=False):
                # Someone updated the group list before us, try again!
                raise Retry

    def create_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)
        if not self.client.add(encoded_group, {}, noreply=False):
            return MemcachedAsyncError(
                coordination.GroupAlreadyExist(group_id))
        self._add_group_to_group_list(group_id)
        return MemcachedAsyncResult(None)

    def get_groups(self):
        return MemcachedAsyncResult(
            self.client.get(self._GROUP_LIST_KEY) or [])

    @retry
    def join_group(self, group_id, capabilities=b""):
        encoded_group = self._encode_group_id(group_id)
        group_members, cas = self.client.gets(encoded_group)
        if not cas:
            return MemcachedAsyncError(
                coordination.GroupNotCreated(group_id))
        if self._member_id in group_members:
            return MemcachedAsyncError(
                coordination.MemberAlreadyExist(group_id, self._member_id))
        group_members[self._member_id] = {
            "capabilities": capabilities,
        }
        if not self.client.cas(encoded_group,
                               group_members,
                               cas):
            # It changed, let's try again
            raise Retry
        self._groups.add(group_id)
        return MemcachedAsyncResult(None)

    @retry
    def leave_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)
        group_members, cas = self.client.gets(encoded_group)
        if not cas:
            return MemcachedAsyncError(
                coordination.GroupNotCreated(group_id))
        if self._member_id not in group_members:
            return MemcachedAsyncError(
                coordination.MemberNotJoined(group_id,
                                             self._member_id))
        del group_members[self._member_id]
        if not self.client.cas(encoded_group,
                               group_members,
                               cas):
            # It changed, let's try again
            raise Retry
        self._groups.remove(group_id)
        return MemcachedAsyncResult(None)

    def _get_members(self, group_id):
        group_members = self.client.get(self._encode_group_id(group_id))
        if group_members is None:
            raise coordination.GroupNotCreated(group_id)
        return dict((m, v) for m, v in six.iteritems(group_members)
                    if self.client.get(self._encode_member_id(m)))

    def get_members(self, group_id):
        try:
            return MemcachedAsyncResult(self._get_members(group_id).keys())
        except Exception as e:
            return MemcachedAsyncError(e)

    def get_member_capabilities(self, group_id, member_id):
        try:
            group_members = self._get_members(group_id)
        except Exception as e:
            return MemcachedAsyncError(e)
        if member_id not in group_members:
            return MemcachedAsyncError(
                coordination.MemberNotJoined(group_id, member_id))
        return MemcachedAsyncResult(group_members[member_id][b'capabilities'])

    @retry
    def update_capabilities(self, group_id, capabilities):
        encoded_group = self._encode_group_id(group_id)
        group_members, cas = self.client.gets(encoded_group)
        if cas is None:
            return MemcachedAsyncError(
                coordination.GroupNotCreated(group_id))
        if self._member_id not in group_members:
            return MemcachedAsyncError(
                coordination.MemberNotJoined(group_id, self._member_id))
        group_members[self._member_id][b'capabilities'] = capabilities
        if not self.client.cas(encoded_group, group_members, cas):
            # It changed, try again
            raise Retry
        return MemcachedAsyncResult(None)

    def get_leader(self, group_id):
        return MemcachedAsyncResult(
            self.client.get(self._encode_group_leader(group_id)))

    def heartbeat(self):
        self.client.set(self._encode_member_id(self._member_id),
                        "It's alive!",
                        expire=self.membership_timeout)
        # Reset the acquired locks
        for lock in self._acquired_locks:
            lock.heartbeat()

        for group_id in six.iterkeys(self._hooks_elected_leader):
            if self.get_leader(group_id).get() == self._member_id:
                self.client.touch(self._encode_group_leader(group_id),
                                  expire=self.leader_timeout)

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

    def run_watchers(self):
        result = []
        for group_id in self.client.get(self._GROUP_LIST_KEY):
            encoded_group = self._encode_group_id(group_id)
            group_members = set(self.client.get(encoded_group))
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

        for group_id in six.iterkeys(self._hooks_elected_leader):
            lock_id = self._encode_group_leader(group_id)
            # Try to grab the lock, if that fails, that means someone has it
            # already.
            if self.client.add(lock_id, self._member_id,
                               expire=self.leader_timeout,
                               noreply=False):
                # We got the lock
                self._hooks_elected_leader[group_id].run(
                    coordination.LeaderElected(
                        group_id,
                        self._member_id))

        return result


class MemcachedAsyncResult(coordination.CoordAsyncResult):
    """Memcached asynchronous result.

    Unfortunately, this is mostely a fake because our driver is not
    asynchronous at all. :-(.

    """
    def __init__(self, result):
        self.result = result

    def get(self, timeout=0):
        return self.result

    @staticmethod
    def done():
        return True


class MemcachedAsyncError(coordination.CoordAsyncResult):
    """Memcached asynchronous error.

    Unfortunately, this is mostely a fake because our driver is not
    asynchronous at all. :-(.

    """
    def __init__(self, error):
        self.error = error

    def get(self, timeout=0):
        raise self.error

    @staticmethod
    def done():
        return True
