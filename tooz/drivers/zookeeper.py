# -*- coding: utf-8 -*-
#
#    Copyright (C) 2013 eNovance Inc. All Rights Reserved.
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

from kazoo import client
from kazoo import exceptions
from kazoo.protocol import paths
from zake import fake_client

from tooz import coordination

_TOOZ_NAMESPACE = "tooz"


class BaseZooKeeperDriver(coordination.CoordinationDriver):

    def start(self, timeout=10):
        try:
            self._coord.start(timeout=timeout)
        except self._coord.handler.timeout_exception as e:
            raise coordination.ToozConnectionError("operation error: %s" % (e))

        try:
            self._coord.ensure_path(paths.join("/", _TOOZ_NAMESPACE))
        except exceptions.KazooException as e:
            raise coordination.ToozError("operation error: %s" % (e))

    def stop(self):
        self._coord.stop()

    @staticmethod
    def _create_group_handler(async_result, timeout, group_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NodeExistsError:
            raise coordination.GroupAlreadyExist("group_id=%s" % group_id)
        except exceptions.NoNodeError:
            raise coordination.ToozError("tooz namespace has not been created")
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))

    def create_group(self, group_id):
        group_path = "/%s/%s" % (_TOOZ_NAMESPACE, group_id)
        async_result = self._coord.create_async(group_path)
        return ZooAsyncResult(async_result, self._create_group_handler,
                              group_id=group_id)

    @staticmethod
    def _join_group_handler(async_result, timeout, group_id, member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NodeExistsError:
            raise coordination.MemberAlreadyExist(str(member_id))
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated("group '%s' has not been "
                                               "created" % group_id)
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
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (member_id, group_id))
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
            raise coordination.GroupNotCreated("group '%s' does not exist" %
                                               group_id)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))
        else:
            return members_ids

    def get_members(self, group_id):
        group_path = paths.join("/", _TOOZ_NAMESPACE, group_id)
        async_result = self._coord.get_children_async(group_path)
        return ZooAsyncResult(async_result, self._get_members_handler,
                              group_id=group_id)

    @staticmethod
    def _update_capabilities_handler(async_result, timeout, group_id,
                                     member_id):
        try:
            async_result.get(block=True, timeout=timeout)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (member_id, group_id))
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
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (member_id, group_id))
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
            return group_ids

    def get_groups(self):
        tooz_namespace = paths.join("/", _TOOZ_NAMESPACE)
        async_result = self._coord.get_children_async(tooz_namespace)
        return ZooAsyncResult(async_result, self._get_groups_handler)

    @staticmethod
    def _path_member(group_id, member_id):
        return paths.join("/", _TOOZ_NAMESPACE, group_id, member_id)


class KazooDriver(BaseZooKeeperDriver):
    """The driver using the Kazoo client against real ZooKeeper servers."""

    def __init__(self, member_id, hosts="127.0.0.1:2181", handler=None,
                 **kwargs):
        """:param hosts: the list of zookeeper servers in the
        form "ip:port2, ip2:port2".

        :param handler: a kazoo async handler to use if provided, if not
        provided the default that kazoo uses internally will be used instead.
        """

        if not all((hosts, member_id)):
            raise KeyError("hosts=%r, member_id=%r" % hosts, member_id)
        self._member_id = member_id
        self._coord = client.KazooClient(hosts=hosts, handler=handler)
        super(KazooDriver, self).__init__()


class ZakeDriver(BaseZooKeeperDriver):
    """The driver using the Zake client which mimic a fake Kazoo client
    without the need of real ZooKeeper servers.
    """

    def __init__(self, member_id, storage=None, **kwargs):
        """:param storage: a fake storage object."""

        if not all((storage, member_id)):
            raise KeyError("storage=%r, member_id=%r" % storage, member_id)
        self._member_id = member_id
        self._coord = fake_client.FakeClient(storage=storage)
        super(ZakeDriver, self).__init__()


class ZooAsyncResult(coordination.CoordAsyncResult):

    def __init__(self, kazooAsyncResult, handler, **kwargs):
        self.kazooAsyncResult = kazooAsyncResult
        self.handler = handler
        self.kwargs = kwargs

    def get(self, timeout=15):
        return self.handler(self.kazooAsyncResult, timeout, **self.kwargs)

    def done(self):
        return self.kazooAsyncResult.ready()
