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

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Concatenate, ParamSpec, TypeVar, cast
import warnings

from kazoo import client
from kazoo import exceptions
from kazoo import security

try:
    from kazoo.handlers import eventlet as eventlet_handler
except ImportError:
    eventlet_handler = None
from kazoo.handlers import threading as threading_handler
from kazoo.protocol import paths
from kazoo.recipe import lock as kazoo_lock
from oslo_utils import encodeutils
from oslo_utils import strutils

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils


class ZooKeeperLock(locking.Lock):
    def __init__(self, name: bytes, lock: kazoo_lock.Lock) -> None:
        super().__init__(name)
        self._lock = lock
        self._client = lock.client

    def is_still_owner(self) -> bool:
        if not self.acquired:
            return False
        try:
            data, _znode = self._client.get(
                paths.join(self._lock.path, self._lock.node)
            )
            return bool(data == self._lock.data)
        except (
            self._client.handler.timeout_exception,
            exceptions.ConnectionLoss,
            exceptions.ConnectionDropped,
            exceptions.NoNodeError,
        ):
            return False
        except exceptions.KazooException as e:
            utils.raise_with_cause(
                tooz.ToozError, f"operation error: {e}", cause=e
            )

    def acquire(
        self,
        blocking: bool = True,
        shared: bool = False,
        timeout: float | None = None,
    ) -> bool:
        if shared:
            raise tooz.NotImplemented("not implemented")
        blocking, timeout = utils.convert_blocking(blocking, timeout)
        return bool(self._lock.acquire(blocking=blocking, timeout=timeout))

    def release(self) -> bool:
        if self.acquired:
            self._lock.release()
            return True
        else:
            return False

    @property
    def acquired(self) -> bool:
        return bool(self._lock.is_acquired)


class KazooDriver(coordination.CoordinationDriverCachedRunWatchers):
    """This driver uses the `kazoo`_ client against real `zookeeper`_ servers.

    It **is** fully functional and implements all of the coordination
    driver API(s). It stores data into `zookeeper`_ using znodes
    and `msgpack`_ encoded values.

    To configure the client to your liking a subset of the options defined at
    http://kazoo.readthedocs.org/en/latest/api/client.html
    will be extracted from the coordinator url (or any provided options),
    so that a specific coordinator can be created that will work for you.

    The Zookeeper coordinator url should look like::

      zookeeper://[USERNAME:PASSWORD@][HOST[:PORT]][?OPTION1=VALUE1[&OPTION2=VALUE2[&...]]]

    Currently the following options will be proxied to the contained client:

    ================  ===============================  ====================
    Name              Source                           Default
    ================  ===============================  ====================
    ca                'ca' options key                 None
    certfile          'certfile' options key           None
    connection_retry  'connection_retry' options key   None
    command_retry     'command_retry' options key      None
    hosts             url netloc + 'hosts' option key  localhost:2181
    keyfile           'keyfile' options key            None
    keyfile_password  'keyfile_password' options key   None
    randomize_hosts   'randomize_hosts' options key    True
    timeout           'timeout' options key            10.0 (kazoo default)
    use_ssl           'use_ssl' options key            False
    verify_certs      'verify_certs' options key       True
    ================  ===============================  ====================

    .. _kazoo: http://kazoo.readthedocs.org/
    .. _zookeeper: http://zookeeper.apache.org/
    .. _msgpack: http://msgpack.org/
    """

    #: Default namespace when none is provided.
    TOOZ_NAMESPACE = b"tooz"

    HANDLERS = {
        'threading': threading_handler.SequentialThreadingHandler,
    }

    if eventlet_handler:
        HANDLERS['eventlet'] = eventlet_handler.SequentialEventletHandler

    """
    Restricted immutable dict of handler 'kinds' -> handler classes that
    this driver can accept via 'handler' option key (the expected value for
    this option is one of the keys in this dictionary).
    """

    CHARACTERISTICS = (
        coordination.Characteristics.NON_TIMEOUT_BASED,
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

    def __init__(
        self, member_id: bytes, parsed_url: Any, options: dict[str, Any]
    ) -> None:
        super().__init__(member_id, parsed_url, options)
        options = utils.collapse(options, exclude=frozenset(['hosts']))
        self.timeout = int(options.get('timeout', '10'))
        self._namespace = options.get('namespace', self.TOOZ_NAMESPACE)
        self._coord = self._make_client(parsed_url, options)
        self._timeout_exception = self._coord.handler.timeout_exception

    def _start(self) -> None:
        try:
            self._coord.start(timeout=self.timeout)
        except self._coord.handler.timeout_exception as e:
            utils.raise_with_cause(
                coordination.ToozConnectionError,
                f"Operational error: {e}",
                cause=e,
            )
        try:
            self._coord.ensure_path(self._base_path())
        except exceptions.KazooException as e:
            utils.raise_with_cause(
                tooz.ToozError, f"Operational error: {e}", cause=e
            )
        self._leader_locks: dict[bytes, kazoo_lock.Lock] = {}

    def _stop(self) -> None:
        self._coord.stop()

    def create_group(self, group_id: bytes) -> ZooAsyncResult[None]:
        group_path = self._path_group(group_id)
        async_result = self._coord.create_async(group_path)

        def _create_group(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
        ) -> None:
            try:
                async_result.get(block=True, timeout=timeout)
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NodeExistsError:
                raise coordination.GroupAlreadyExist(group_id)
            except exceptions.NoNodeError as e:
                utils.raise_with_cause(
                    tooz.ToozError,
                    f"Tooz namespace '{self._namespace}' has not been created",
                    cause=e,
                )
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

        return ZooAsyncResult(
            async_result,
            _create_group,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
        )

    def delete_group(self, group_id: bytes) -> ZooAsyncResult[None]:
        group_path = self._path_group(group_id)
        async_result = self._coord.delete_async(group_path)

        def _delete_group(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
        ) -> None:
            try:
                async_result.get(block=True, timeout=timeout)
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError:
                raise coordination.GroupNotCreated(group_id)
            except exceptions.NotEmptyError:
                raise coordination.GroupNotEmpty(group_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

        return ZooAsyncResult(
            async_result,
            _delete_group,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
        )

    def join_group(
        self,
        group_id: bytes,
        capabilities: coordination.Capabilities | None = None,
    ) -> ZooAsyncResult[None]:
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.create_async(
            member_path, value=utils.dumps(capabilities), ephemeral=True
        )

        def _join_group(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
            member_id: bytes,
        ) -> None:
            try:
                async_result.get(block=True, timeout=timeout)
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NodeExistsError:
                raise coordination.MemberAlreadyExist(group_id, member_id)
            except exceptions.NoNodeError:
                raise coordination.GroupNotCreated(group_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

            self._joined_groups.add(group_id)

        return ZooAsyncResult(
            async_result,
            _join_group,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
            member_id=self._member_id,
        )

    def heartbeat(self) -> float:
        # Just fetch the base path (and do nothing with it); this will
        # force any waiting heartbeat responses to be flushed, and also
        # ensures that the connection still works as expected...
        base_path = self._base_path()
        try:
            self._coord.get(base_path)
        except self._timeout_exception as e:
            utils.raise_with_cause(
                coordination.OperationTimedOut, str(e), cause=e
            )
        except exceptions.NoNodeError:
            pass
        except exceptions.ZookeeperError as e:
            utils.raise_with_cause(tooz.ToozError, str(e), cause=e)
        return self.timeout

    def leave_group(self, group_id: bytes) -> ZooAsyncResult[None]:
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.delete_async(member_path)

        def _leave_group(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
            member_id: bytes,
        ) -> None:
            try:
                async_result.get(block=True, timeout=timeout)
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError:
                raise coordination.MemberNotJoined(group_id, member_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

            self._joined_groups.discard(group_id)

        return ZooAsyncResult(
            async_result,
            _leave_group,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
            member_id=self._member_id,
        )

    def get_members(self, group_id: bytes) -> ZooAsyncResult[set[bytes]]:
        group_path = self._path_group(group_id)
        async_result = self._coord.get_children_async(group_path)

        def _get_members(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
        ) -> set[bytes]:
            try:
                members_ids = async_result.get(block=True, timeout=timeout)
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError:
                raise coordination.GroupNotCreated(group_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)
            else:
                return {m.encode('ascii') for m in members_ids}

        return ZooAsyncResult(
            async_result,
            _get_members,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
        )

    def update_capabilities(
        self, group_id: bytes, capabilities: coordination.Capabilities | None
    ) -> ZooAsyncResult[None]:
        member_path = self._path_member(group_id, self._member_id)
        async_result = self._coord.set_async(
            member_path, value=utils.dumps(capabilities)
        )

        def _update_capabilities(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
            member_id: bytes,
        ) -> None:
            try:
                async_result.get(block=True, timeout=timeout)
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError:
                raise coordination.MemberNotJoined(group_id, member_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)

        return ZooAsyncResult(
            async_result,
            _update_capabilities,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
            member_id=self._member_id,
        )

    def get_member_capabilities(
        self, group_id: bytes, member_id: bytes
    ) -> ZooAsyncResult[coordination.Capabilities | None]:
        member_path = self._path_member(group_id, member_id)
        async_result = self._coord.get_async(member_path)

        def _get_member_capabilities(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
            member_id: bytes,
        ) -> coordination.Capabilities | None:
            try:
                capabilities = async_result.get(block=True, timeout=timeout)[0]
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError:
                raise coordination.MemberNotJoined(group_id, member_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)
            else:
                return cast(
                    coordination.Capabilities, utils.loads(capabilities)
                )

        return ZooAsyncResult(
            async_result,
            _get_member_capabilities,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
            member_id=self._member_id,
        )

    def get_member_info(
        self, group_id: bytes, member_id: bytes
    ) -> ZooAsyncResult[dict[str, Any]]:
        member_path = self._path_member(group_id, member_id)
        async_result = self._coord.get_async(member_path)

        def _get_member_info(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
            group_id: bytes,
            member_id: bytes,
        ) -> dict[str, Any]:
            try:
                capabilities, znode_stats = async_result.get(
                    block=True, timeout=timeout
                )
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError:
                raise coordination.MemberNotJoined(group_id, member_id)
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)
            else:
                member_info = {
                    'capabilities': utils.loads(capabilities),
                    'created_at': utils.millis_to_datetime(znode_stats.ctime),
                    'updated_at': utils.millis_to_datetime(znode_stats.mtime),
                }
                return member_info

        return ZooAsyncResult(
            async_result,
            _get_member_info,
            timeout_exception=self._timeout_exception,
            group_id=group_id,
            member_id=self._member_id,
        )

    def get_groups(self) -> ZooAsyncResult[list[bytes]]:
        base_path = self._base_path()
        async_result = self._coord.get_children_async(base_path)

        def _get_groups(
            async_result: Any,
            timeout: float | None,
            /,
            timeout_exception: type[Exception],
        ) -> list[bytes]:
            try:
                group_ids: list[str] = async_result.get(
                    block=True, timeout=timeout
                )
            except timeout_exception as e:
                utils.raise_with_cause(
                    coordination.OperationTimedOut, str(e), cause=e
                )
            except exceptions.NoNodeError as e:
                utils.raise_with_cause(
                    tooz.ToozError,
                    f"Tooz namespace '{self._namespace}' has not been created",
                    cause=e,
                )
            except exceptions.ZookeeperError as e:
                utils.raise_with_cause(tooz.ToozError, str(e), cause=e)
            else:
                return [g.encode('ascii') for g in group_ids]

        return ZooAsyncResult(
            async_result,
            _get_groups,
            timeout_exception=self._timeout_exception,
        )

    def _base_path(self) -> str:
        return self._paths_join("/", self._namespace)

    def _path_group(self, group_id: bytes) -> str:
        return self._paths_join("/", self._namespace, group_id)

    def _path_member(self, group_id: bytes, member_id: bytes) -> str:
        return self._paths_join("/", self._namespace, group_id, member_id)

    @staticmethod
    def _paths_join(arg: str | bytes, *more_args: str | bytes) -> str:
        """Converts paths into a string (unicode)."""
        args = [arg]
        args.extend(more_args)
        cleaned_args = []
        for arg in args:
            if isinstance(arg, bytes):
                cleaned_args.append(
                    encodeutils.safe_decode(arg, incoming='ascii')
                )
            else:
                cleaned_args.append(arg)
        return str(paths.join(*cleaned_args))

    def _make_client(
        self, parsed_url: Any, options: Any
    ) -> client.KazooClient:
        # Creates a kazoo client,
        # See: https://github.com/python-zk/kazoo/blob/2.2.1/kazoo/client.py
        # for what options a client takes...
        if parsed_url.username and parsed_url.password:
            username = parsed_url.username
            password = parsed_url.password

            digest_auth = f"{username}:{password}"
            digest_acl = security.make_digest_acl(username, password, all=True)
            default_acl = (digest_acl,)
            auth_data = [('digest', digest_auth)]
        else:
            default_acl = None
            auth_data = None

        maybe_hosts = [parsed_url.netloc] + list(options.get('hosts', []))
        hosts = list(filter(None, maybe_hosts))
        if not hosts:
            hosts = ['localhost:2181']
        randomize_hosts = options.get('randomize_hosts', True)
        use_ssl = options.get('use_ssl', False)
        verify_certs = options.get('verify_certs', True)
        client_kwargs = {
            'auth_data': auth_data,
            'ca': options.get('ca', None),
            'certfile': options.get('certfile', None),
            'connection_retry': options.get('connection_retry'),
            'command_retry': options.get('command_retry'),
            'default_acl': default_acl,
            'hosts': ",".join(hosts),
            'keyfile': options.get('keyfile', None),
            'keyfile_password': options.get('keyfile_password', None),
            'randomize_hosts': strutils.bool_from_string(randomize_hosts),
            'timeout': float(options.get('timeout', self.timeout)),
            'use_ssl': strutils.bool_from_string(use_ssl),
            'verify_certs': strutils.bool_from_string(verify_certs),
        }
        handler_kind = options.get('handler')
        if handler_kind == "eventlet":
            warnings.warn(
                "Eventlet support is deprecated and will be removed. "
                "Use threading handler instead.",
                DeprecationWarning,
            )
        if handler_kind:
            try:
                handler_cls = self.HANDLERS[handler_kind]
            except KeyError:
                raise ValueError(
                    f"Unknown handler '{handler_kind}' requested"
                    f" valid handlers are {sorted(self.HANDLERS.keys())}"
                )
            client_kwargs['handler'] = handler_cls()
        return client.KazooClient(**client_kwargs)

    def stand_down_group_leader(self, group_id: bytes) -> bool:
        if group_id in self._leader_locks:
            self._leader_locks[group_id].release()
            return True
        return False

    def _get_group_leader_lock(self, group_id: bytes) -> kazoo_lock.Lock:
        if group_id not in self._leader_locks:
            self._leader_locks[group_id] = self._coord.Lock(
                self._path_group(group_id) + "/leader",
                encodeutils.safe_decode(self._member_id, incoming='ascii'),
            )
        return self._leader_locks[group_id]

    def get_leader(self, group_id: bytes) -> ZooAsyncResult[bytes]:
        contenders = self._get_group_leader_lock(group_id).contenders()
        if contenders and contenders[0]:
            leader = contenders[0].encode('ascii')
        else:
            leader = None
        return ZooAsyncResult(None, lambda *args: leader)

    def get_lock(self, name: bytes) -> ZooKeeperLock:
        z_lock = self._coord.Lock(
            self._paths_join("/", self._namespace, "locks", name),
            encodeutils.safe_decode(self._member_id, incoming='ascii'),
        )
        return ZooKeeperLock(name, z_lock)

    def run_elect_coordinator(self) -> None:
        for group_id in self._hooks_elected_leader.keys():
            leader_lock = self._get_group_leader_lock(group_id)
            if leader_lock.is_acquired:
                # Previously acquired/still leader, leave it be...
                continue
            if leader_lock.acquire(blocking=False):
                # We are now leader for this group
                self._hooks_elected_leader[group_id].run(
                    coordination.LeaderElected(group_id, self._member_id)
                )

    def run_watchers(self, timeout: float | None = None) -> list[Any]:
        results = super().run_watchers(timeout)
        self.run_elect_coordinator()
        return results


P = ParamSpec('P')
T = TypeVar('T')


class ZooAsyncResult(coordination.CoordAsyncResult[T]):
    def __init__(
        self,
        kazoo_async_result: Any,
        handler: Callable[Concatenate[Any, float | None, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._kazoo_async_result = kazoo_async_result
        self._handler = handler
        self._args = args
        self._kwargs = kwargs

    def get(self, timeout: float | None = None) -> T:
        return self._handler(
            self._kazoo_async_result, timeout, *self._args, **self._kwargs
        )

    def done(self) -> bool:
        return bool(self._kazoo_async_result.ready())
