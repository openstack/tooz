# -*- coding: utf-8 -*-
#
#    Copyright (C) 2013-2014 eNovance Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import copy
import threading

from kazoo import client
from kazoo import exceptions
from kazoo.protocol import paths
import six
import zake.fake_client
import zake.fake_storage

from tooz import coordination
from tooz import locking


class ZooKeeperLock(locking.Lock):
    def __init__(self, lock):
        self._lock = lock

    def acquire(self, blocking=True, timeout=None):
        return self._lock.acquire(blocking=blocking,
                                  timeout=timeout)

    def release(self):
        return self._lock.release()


class BaseZooKeeperDriver(coordination.CoordinationDriver):

    _TOOZ_NAMESPACE = b"tooz"

    def __init__(self, member_id, parsed_url, options):
        super(BaseZooKeeperDriver, self).__init__()
        self._member_id = member_id
        self.timeout = int(options.get('timeout', ['10'])[-1])

    def start(self):
        try:
            self._coord.start(timeout=self.timeout)
        except self._coord.handler.timeout_exception as e:
            raise coordination.ToozConnectionError("operation error: %s" % (e))

        try:
            self._coord.ensure_path(self.paths_join("/", self._TOOZ_NAMESPACE))
        except exceptions.KazooException as e:
            raise coordination.ToozError("operation error: %s" % (e))

        self._group_members = collections.defaultdict(set)
        self._watchers = six.moves.queue.Queue()
        self._leader_locks = {}

    def stop(self):
        self._coord.stop()

    @staticmethod
    def _create_group_handler(async_result, timeout, group_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NodeExistsError:
            raise coordination.GroupAlreadyExist(group_id)
        except exceptions.NoNodeError:
            raise coordination.ToozError("tooz namespace has not been created")
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))

    def create_group(self, group_id):
        group_path = self._path_group(group_id)
        async_result = self._coord.create_async(group_path)
        return ZooAsyncResult(async_result, self._create_group_handler,
                              group_id=group_id)

    @staticmethod
    def _join_group_handler(async_result, timeout, group_id, member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NodeExistsError:
            raise coordination.MemberAlreadyExist(group_id, member_id)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated(group_id)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))

    def join_group(self, group_id, capabilities=b""):
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.create_async(member_path,
                                                value=capabilities,
                                                ephemeral=True)
        return ZooAsyncResult(async_result, self._join_group_handler,
                              group_id=group_id, member_id=self._member_id)

    @staticmethod
    def _leave_group_handler(async_result, timeout, group_id, member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))

    def leave_group(self, group_id):
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.delete_async(member_path)
        return ZooAsyncResult(async_result, self._leave_group_handler,
                              group_id=group_id, member_id=self._member_id)

    @staticmethod
    def _get_members_handler(async_result, timeout, group_id):
        try:
            members_ids = async_result.get(block=True, timeout=timeout)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated(group_id)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))
        else:
            return set(m.encode('ascii') for m in members_ids)

    def get_members(self, group_id):
        group_path = self.paths_join("/", self._TOOZ_NAMESPACE, group_id)
        async_result = self._coord.get_children_async(group_path)
        return ZooAsyncResult(async_result, self._get_members_handler,
                              group_id=group_id)

    @staticmethod
    def _update_capabilities_handler(async_result, timeout, group_id,
                                     member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))

    def update_capabilities(self, group_id, capabilities):
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.set_async(member_path, capabilities)
        return ZooAsyncResult(async_result, self._update_capabilities_handler,
                              group_id=group_id, member_id=self._member_id)

    @staticmethod
    def _get_member_capabilities_handler(async_result, timeout, group_id,
                                         member_id):
        try:
            capabilities = async_result.get(block=True, timeout=timeout)[0]
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))
        else:
            return capabilities

    def get_member_capabilities(self, group_id, member_id):
        member_path = self._path_member(group_id, member_id)
        async_result = self._coord.get_async(member_path)
        return ZooAsyncResult(async_result,
                              self._get_member_capabilities_handler,
                              group_id=group_id, member_id=self._member_id)

    @staticmethod
    def _get_groups_handler(async_result, timeout):
        try:
            group_ids = async_result.get(block=True, timeout=timeout)
        except exceptions.NoNodeError:
            raise coordination.ToozError("tooz namespace has not been created")
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))
        else:
            return set(g.encode('ascii') for g in group_ids)

    def get_groups(self):
        tooz_namespace = self.paths_join("/", self._TOOZ_NAMESPACE)
        async_result = self._coord.get_children_async(tooz_namespace)
        return ZooAsyncResult(async_result, self._get_groups_handler)

    def _path_group(self, group_id):
        return self.paths_join("/", self._TOOZ_NAMESPACE, group_id)

    def _path_member(self, group_id, member_id):
        return self.paths_join("/", self._TOOZ_NAMESPACE,
                               group_id, member_id)

    @staticmethod
    def paths_join(*args):
        lpaths = []
        for arg in args:
            if isinstance(arg, six.binary_type):
                lpaths.append(arg.decode('ascii'))
            else:
                lpaths.append(arg)
        return paths.join(*lpaths)


class KazooDriver(BaseZooKeeperDriver):
    """The driver using the Kazoo client against real ZooKeeper servers."""

    def __init__(self, member_id, parsed_url, options):
        super(KazooDriver, self).__init__(member_id, parsed_url, options)
        self._coord = client.KazooClient(hosts=parsed_url.netloc)
        self._member_id = member_id

    def _watch_group(self, group_id):
        get_members_req = self.get_members(group_id)

        def on_children_change(children):
            # If we don't have any hook, stop watching
            if not self._has_hooks_for_group(group_id):
                return False
            children = set(children)
            last_children = self._group_members[group_id]

            for member_id in (children - last_children):
                # Copy function in case it's removed later from the
                # hook list
                hooks = copy.copy(self._hooks_join_group[group_id])
                self._watchers.put(
                    lambda: hooks.run(
                        coordination.MemberJoinedGroup(
                            group_id,
                            member_id.encode('ascii'))))

            for member_id in (last_children - children):
                # Copy function in case it's removed later from the
                # hook list
                hooks = copy.copy(self._hooks_leave_group[group_id])
                self._watchers.put(
                    lambda: hooks.run(
                        coordination.MemberLeftGroup(
                            group_id,
                            member_id.encode('ascii'))))

            self._group_members[group_id] = children

        # Initialize the current member list
        self._group_members[group_id] = get_members_req.get()

        try:
            self._coord.ChildrenWatch(self._path_group(group_id),
                                      on_children_change)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated(group_id)

    def watch_join_group(self, group_id, callback):
        # Check if we already have hooks for this group_id, if not, start
        # watching it.
        already_being_watched = self._has_hooks_for_group(group_id)

        # Add the hook before starting watching to avoid race conditions
        # as the watching executor can be in a thread
        super(BaseZooKeeperDriver, self).watch_join_group(
            group_id, callback)

        if not already_being_watched:
            try:
                self._watch_group(group_id)
            except Exception:
                # Rollback and unregister the hook
                self.unwatch_join_group(group_id, callback)
                raise

    def unwatch_join_group(self, group_id, callback):
        return super(BaseZooKeeperDriver, self).unwatch_join_group(
            group_id, callback)

    def watch_leave_group(self, group_id, callback):
        # Check if we already have hooks for this group_id, if not, start
        # watching it.
        already_being_watched = self._has_hooks_for_group(group_id)

        # Add the hook before starting watching to avoid race conditions
        # as the watching executor can be in a thread
        super(BaseZooKeeperDriver, self).watch_leave_group(
            group_id, callback)

        if not already_being_watched:
            try:
                self._watch_group(group_id)
            except Exception:
                # Rollback and unregister the hook
                self.unwatch_leave_group(group_id, callback)
                raise

    def unwatch_leave_group(self, group_id, callback):
        return super(BaseZooKeeperDriver, self).unwatch_leave_group(
            group_id, callback)

    def watch_elected_as_leader(self, group_id, callback):
        return super(BaseZooKeeperDriver, self).watch_elected_as_leader(
            group_id, callback)

    def unwatch_elected_as_leader(self, group_id, callback):
        return super(BaseZooKeeperDriver, self).unwatch_elected_as_leader(
            group_id, callback)

    def stand_down_group_leader(self, group_id):
        if group_id in self._leader_locks:
            self._leader_locks[group_id].release()
            return True
        return False

    def _get_group_leader_lock(self, group_id):
        if group_id not in self._leader_locks:
            self._leader_locks[group_id] = self._coord.Lock(
                self._path_group(group_id) + "/leader",
                self._member_id.decode('ascii'))
        return self._leader_locks[group_id]

    def get_leader(self, group_id):
        contenders = self._get_group_leader_lock(group_id).contenders()
        if contenders and contenders[0]:
            leader = contenders[0].encode('ascii')
        else:
            leader = None
        return ZooAsyncResult(None, lambda *args: leader)

    def get_lock(self, name):
        return ZooKeeperLock(
            self._coord.Lock(
                self.paths_join(b"/", self._TOOZ_NAMESPACE, b"locks", name),
                self._member_id.decode('ascii')))

    def run_watchers(self):
        ret = []
        while True:
            try:
                cb = self._watchers.get(block=False)
            except six.moves.queue.Empty:
                break
            ret.extend(cb())

        for group_id in six.iterkeys(self._hooks_elected_leader):
            if self._get_group_leader_lock(group_id).acquire(blocking=False):
                # We are now leader for this group
                self._hooks_elected_leader[group_id].run(
                    coordination.LeaderElected(
                        group_id,
                        self._member_id))

        return ret


class ZakeDriver(BaseZooKeeperDriver):
    """The driver using the Zake client which mimic a fake Kazoo client
    without the need of real ZooKeeper servers.
    """

    fake_storage = zake.fake_storage.FakeStorage(threading.RLock())

    def __init__(self, member_id, parsed_url, options):
        super(ZakeDriver, self).__init__(member_id, parsed_url, options)
        self._coord = zake.fake_client.FakeClient(storage=self.fake_storage)

    @staticmethod
    def watch_join_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def unwatch_join_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def watch_leave_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def unwatch_leave_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def run_watchers():
        raise NotImplementedError


class ZooAsyncResult(coordination.CoordAsyncResult):

    def __init__(self, kazooAsyncResult, handler, **kwargs):
        self.kazooAsyncResult = kazooAsyncResult
        self.handler = handler
        self.kwargs = kwargs

    def get(self, timeout=15):
        return self.handler(self.kazooAsyncResult, timeout, **self.kwargs)

    def done(self):
        return self.kazooAsyncResult.ready()
