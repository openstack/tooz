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

from kazoo import client
from kazoo import exceptions
from kazoo.handlers import eventlet as eventlet_handler
from kazoo.handlers import threading as threading_handler
from kazoo.protocol import paths
from oslo_utils import encodeutils
from oslo_utils import strutils
import six
from six.moves import filter as compat_filter

from tooz import coordination
from tooz import locking
from tooz import utils


class ZooKeeperLock(locking.Lock):
    def __init__(self, name, lock):
        super(ZooKeeperLock, self).__init__(name)
        self._lock = lock
        self._client = lock.client

    def is_still_owner(self):
        if not self.acquired:
            return False
        try:
            data, _znode = self._client.get(
                paths.join(self._lock.path, self._lock.node))
            return data == self._lock.data
        except (self._client.handler.timeout_exception,
                exceptions.ConnectionLoss,
                exceptions.ConnectionDropped,
                exceptions.NoNodeError):
            return False
        except exceptions.KazooException as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          "operation error: %s" % (e),
                                          cause=e)

    def acquire(self, blocking=True):
        blocking, timeout = utils.convert_blocking(blocking)
        return self._lock.acquire(blocking=blocking,
                                  timeout=timeout)

    def release(self):
        if self.acquired:
            self._lock.release()
            return True
        else:
            return False

    @property
    def acquired(self):
        return self._lock.is_acquired


class BaseZooKeeperDriver(coordination.CoordinationDriver):
    """Initialize the zookeeper driver.

    :param timeout: connection timeout to wait when first connecting to the
                    zookeeper server
    """
    #: Default namespace when none is provided.
    TOOZ_NAMESPACE = b"tooz"

    def __init__(self, member_id, parsed_url, options):
        super(BaseZooKeeperDriver, self).__init__()
        options = utils.collapse(options, exclude=['hosts'])
        self._options = options
        self._member_id = member_id
        self.timeout = int(options.get('timeout', '10'))
        self._namespace = options.get('namespace', self.TOOZ_NAMESPACE)

    def _start(self):
        try:
            self._coord.start(timeout=self.timeout)
        except self._coord.handler.timeout_exception as e:
            e_msg = encodeutils.exception_to_unicode(e)
            coordination.raise_with_cause(coordination.ToozConnectionError,
                                          "Operational error: %s" % e_msg,
                                          cause=e)
        try:
            self._coord.ensure_path(self._paths_join("/", self._namespace))
        except exceptions.KazooException as e:
            e_msg = encodeutils.exception_to_unicode(e)
            coordination.raise_with_cause(coordination.ToozError,
                                          "Operational error: %s" % e_msg,
                                          cause=e)
        self._group_members = collections.defaultdict(set)
        self._watchers = collections.deque()
        self._leader_locks = {}

    def _stop(self):
        self._coord.stop()

    @staticmethod
    def _dumps(data):
        return utils.dumps(data)

    @staticmethod
    def _loads(blob):
        return utils.loads(blob)

    def _create_group_handler(self, async_result, timeout,
                              timeout_exception, group_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NodeExistsError:
            raise coordination.GroupAlreadyExist(group_id)
        except exceptions.NoNodeError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          "Tooz namespace '%s' has not"
                                          " been created" % self._namespace,
                                          cause=e)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def create_group(self, group_id):
        group_path = self._path_group(group_id)
        async_result = self._coord.create_async(group_path)
        return ZooAsyncResult(async_result, self._create_group_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id)

    @staticmethod
    def _delete_group_handler(async_result, timeout,
                              timeout_exception, group_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated(group_id)
        except exceptions.NotEmptyError:
            raise coordination.GroupNotEmpty(group_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def delete_group(self, group_id):
        group_path = self._path_group(group_id)
        async_result = self._coord.delete_async(group_path)
        return ZooAsyncResult(async_result, self._delete_group_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id)

    @staticmethod
    def _join_group_handler(async_result, timeout,
                            timeout_exception, group_id, member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NodeExistsError:
            raise coordination.MemberAlreadyExist(group_id, member_id)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated(group_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def join_group(self, group_id, capabilities=b""):
        member_path = self._path_member(group_id, self._member_id)
        capabilities = self._dumps(capabilities)
        async_result = self._coord.create_async(member_path,
                                                value=capabilities,
                                                ephemeral=True)
        return ZooAsyncResult(async_result, self._join_group_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id, member_id=self._member_id)

    @staticmethod
    def _leave_group_handler(async_result, timeout,
                             timeout_exception, group_id, member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def heartbeat(self):
        # Just fetch the base path (and do nothing with it); this will
        # force any waiting heartbeat responses to be flushed, and also
        # ensures that the connection still works as expected...
        base_path = self._paths_join("/", self._namespace)
        try:
            self._coord.get(base_path)
        except self._timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            pass
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def leave_group(self, group_id):
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.delete_async(member_path)
        return ZooAsyncResult(async_result, self._leave_group_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id, member_id=self._member_id)

    @staticmethod
    def _get_members_handler(async_result, timeout,
                             timeout_exception, group_id):
        try:
            members_ids = async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated(group_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        else:
            return set(m.encode('ascii') for m in members_ids)

    def get_members(self, group_id):
        group_path = self._paths_join("/", self._namespace, group_id)
        async_result = self._coord.get_children_async(group_path)
        return ZooAsyncResult(async_result, self._get_members_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id)

    @staticmethod
    def _update_capabilities_handler(async_result, timeout,
                                     timeout_exception, group_id, member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def update_capabilities(self, group_id, capabilities):
        member_path = self._path_member(group_id, self._member_id)
        capabilities = self._dumps(capabilities)
        async_result = self._coord.set_async(member_path, capabilities)
        return ZooAsyncResult(async_result, self._update_capabilities_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id, member_id=self._member_id)

    @classmethod
    def _get_member_capabilities_handler(cls, async_result, timeout,
                                         timeout_exception, group_id,
                                         member_id):
        try:
            capabilities = async_result.get(block=True, timeout=timeout)[0]
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        else:
            return cls._loads(capabilities)

    def get_member_capabilities(self, group_id, member_id):
        member_path = self._path_member(group_id, member_id)
        async_result = self._coord.get_async(member_path)
        return ZooAsyncResult(async_result,
                              self._get_member_capabilities_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id, member_id=self._member_id)

    @classmethod
    def _get_member_info_handler(cls, async_result, timeout,
                                 timeout_exception, group_id,
                                 member_id):
        try:
            capabilities, znode_stats = async_result.get(block=True,
                                                         timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined(group_id, member_id)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        else:
            member_info = {
                'capabilities': cls._loads(capabilities),
                'created_at': utils.millis_to_datetime(znode_stats.ctime),
                'updated_at': utils.millis_to_datetime(znode_stats.mtime)
            }
            return member_info

    def get_member_info(self, group_id, member_id):
        member_path = self._path_member(group_id, member_id)
        async_result = self._coord.get_async(member_path)
        return ZooAsyncResult(async_result,
                              self._get_member_info_handler,
                              timeout_exception=self._timeout_exception,
                              group_id=group_id, member_id=self._member_id)

    def _get_groups_handler(self, async_result, timeout, timeout_exception):
        try:
            group_ids = async_result.get(block=True, timeout=timeout)
        except timeout_exception as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        except exceptions.NoNodeError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          "Tooz namespace '%s' has not"
                                          " been created" % self._namespace,
                                          cause=e)
        except exceptions.ZookeeperError as e:
            coordination.raise_with_cause(coordination.ToozError,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)
        else:
            return set(g.encode('ascii') for g in group_ids)

    def get_groups(self):
        tooz_namespace = self._paths_join("/", self._namespace)
        async_result = self._coord.get_children_async(tooz_namespace)
        return ZooAsyncResult(async_result, self._get_groups_handler,
                              timeout_exception=self._timeout_exception)

    def _path_group(self, group_id):
        return self._paths_join("/", self._namespace, group_id)

    def _path_member(self, group_id, member_id):
        return self._paths_join("/", self._namespace, group_id, member_id)

    @staticmethod
    def _paths_join(arg, *more_args):
        """Converts paths into a string (unicode)."""
        args = [arg]
        args.extend(more_args)
        cleaned_args = []
        for arg in args:
            if isinstance(arg, six.binary_type):
                cleaned_args.append(arg.decode('ascii'))
            else:
                cleaned_args.append(arg)
        return paths.join(*cleaned_args)


class KazooDriver(BaseZooKeeperDriver):
    """This driver uses the `kazoo`_ client against real `zookeeper`_ servers.

    It **is** fully functional and implements all of the coordination
    driver API(s). It stores data into `zookeeper`_ using znodes
    and `msgpack`_ encoded values.

    To configure the client to your liking a subset of the options defined at
    http://kazoo.readthedocs.org/en/latest/api/client.html
    will be extracted from the coordinator url (or any provided options),
    so that a specific coordinator can be created that will work for you.

    Currently the following options will be proxied to the contained client:

    ================  ===============================  ====================
    Name              Source                           Default
    ================  ===============================  ====================
    hosts             url netloc + 'hosts' option key  localhost:2181
    timeout           'timeout' options key            10.0 (kazoo default)
    connection_retry  'connection_retry' options key   None
    command_retry     'command_retry' options key      None
    randomize_hosts   'randomize_hosts' options key    True
    ================  ===============================  ====================

    .. _kazoo: http://kazoo.readthedocs.org/
    .. _zookeeper: http://zookeeper.apache.org/
    .. _msgpack: http://msgpack.org/
    """

    HANDLERS = {
        'eventlet': eventlet_handler.SequentialEventletHandler,
        'threading': threading_handler.SequentialThreadingHandler,
    }
    """
    Restricted immutable dict of handler 'kinds' -> handler classes that
    this driver can accept via 'handler' option key (the expected value for
    this option is one of the keys in this dictionary).
    """

    CHARACTERISTICS = (
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
        coordination.Characteristics.DISTRIBUTED_ACROSS_HOSTS,
        # Writes *always* go through a single leader process, but it may
        # take a while for those writes to propagate to followers (and =
        # during this time clients can read older values)...
        coordination.Characteristics.SEQUENTIAL,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    def __init__(self, member_id, parsed_url, options):
        super(KazooDriver, self).__init__(member_id, parsed_url, options)
        self._coord = self._make_client(parsed_url, self._options)
        self._member_id = member_id
        self._timeout_exception = self._coord.handler.timeout_exception

    def _make_client(self, parsed_url, options):
        # Creates a kazoo client,
        # See: https://github.com/python-zk/kazoo/blob/2.2.1/kazoo/client.py
        # for what options a client takes...
        maybe_hosts = [parsed_url.netloc] + list(options.get('hosts', []))
        hosts = list(compat_filter(None, maybe_hosts))
        if not hosts:
            hosts = ['localhost:2181']
        randomize_hosts = options.get('randomize_hosts', True)
        client_kwargs = {
            'hosts': ",".join(hosts),
            'timeout': float(options.get('timeout', self.timeout)),
            'connection_retry': options.get('connection_retry'),
            'command_retry': options.get('command_retry'),
            'randomize_hosts': strutils.bool_from_string(randomize_hosts),
        }
        handler_kind = options.get('handler')
        if handler_kind:
            try:
                handler_cls = self.HANDLERS[handler_kind]
            except KeyError:
                raise ValueError("Unknown handler '%s' requested"
                                 " valid handlers are %s"
                                 % (handler_kind,
                                    sorted(self.HANDLERS.keys())))
            client_kwargs['handler'] = handler_cls()
        return client.KazooClient(**client_kwargs)

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
                self._watchers.append(
                    lambda: hooks.run(
                        coordination.MemberJoinedGroup(
                            group_id,
                            utils.to_binary(member_id))))

            for member_id in (last_children - children):
                # Copy function in case it's removed later from the
                # hook list
                hooks = copy.copy(self._hooks_leave_group[group_id])
                self._watchers.append(
                    lambda: hooks.run(
                        coordination.MemberLeftGroup(
                            group_id,
                            utils.to_binary(member_id))))

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
        z_lock = self._coord.Lock(
            self._paths_join(b"/", self._namespace, b"locks", name),
            self._member_id.decode('ascii'))
        return ZooKeeperLock(name, z_lock)

    def run_elect_coordinator(self):
        for group_id in six.iterkeys(self._hooks_elected_leader):
            leader_lock = self._get_group_leader_lock(group_id)
            if leader_lock.is_acquired:
                # Previously acquired/still leader, leave it be...
                continue
            if leader_lock.acquire(blocking=False):
                # We are now leader for this group
                self._hooks_elected_leader[group_id].run(
                    coordination.LeaderElected(
                        group_id,
                        self._member_id))

    def run_watchers(self, timeout=None):
        results = []
        while self._watchers:
            cb = self._watchers.popleft()
            results.extend(cb())
        self.run_elect_coordinator()
        return results


class ZooAsyncResult(coordination.CoordAsyncResult):
    def __init__(self, kazoo_async_result, handler, **kwargs):
        self._kazoo_async_result = kazoo_async_result
        self._handler = handler
        self._kwargs = kwargs

    def get(self, timeout=10):
        return self._handler(self._kazoo_async_result, timeout, **self._kwargs)

    def done(self):
        return self._kazoo_async_result.ready()
