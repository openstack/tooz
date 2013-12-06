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
from kazoo.handlers import threading
from kazoo.protocol import paths

from tooz import coordination
from tooz import models

_TOOZ_NAMESPACE = "tooz"


class ZooKeeperDriver(coordination.CoordinationDriver):

    def __init__(self, member_id, **kwargs):
        """:param kwargs: it must contains the key "hosts" associated
        to the list of zookeeper servers in the form "ip:port2, ip2:port2".
        """
        if not all((kwargs["hosts"], member_id)):
            raise KeyError("hosts=%r, member_id=%r" % (kwargs["hosts"],
                           member_id))
        self._member_id = member_id
        self._coord = client.KazooClient(hosts=kwargs["hosts"])
        super(ZooKeeperDriver, self).__init__()

    def start(self, timeout=10):
        try:
            self._coord.start(timeout=timeout)
        except threading.TimeoutError as e:
            raise coordination.ToozConnectionError("operation error: %s" % (e))

        try:
            self._coord.ensure_path(paths.join("/", _TOOZ_NAMESPACE))
        except exceptions.KazooException as e:
            raise coordination.ToozError("operation error: %s" % (e))

    def stop(self):
        self._coord.stop()

    def create_group(self, group_id):
        try:
            group_path = "/%s/%s" % (_TOOZ_NAMESPACE, group_id)
            self._wrap_kazoo_call(self._coord.create, group_path)
        except exceptions.NodeExistsError:
            raise coordination.GroupAlreadyExist("group_id=%s" % group_id)
        except exceptions.NoNodeError:
            raise coordination.ToozError("tooz namespace has not been created")

    def join_group(self, group_id, capabilities=b""):
        try:
            member_path = self._path_member(group_id, self._member_id)
            self._wrap_kazoo_call(self._coord.create,
                                  member_path,
                                  value=capabilities,
                                  ephemeral=True)
        except exceptions.NodeExistsError:
            raise coordination.MemberAlreadyExist(str(self._member_id))
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated("group '%s' has not been "
                                               "created" % _TOOZ_NAMESPACE)

    def leave_group(self, group_id):
        try:
            member_path = self._path_member(group_id, self._member_id)
            self._wrap_kazoo_call(self._coord.delete, member_path)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (self._member_id, group_id))
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))

    def get_members(self, group_id):
        member_ids = []
        try:
            group_path = paths.join("/", _TOOZ_NAMESPACE, group_id)
            member_ids = self._wrap_kazoo_call(self._coord.get_children,
                                               group_path)
        except exceptions.NoNodeError:
            raise coordination.GroupNotCreated("group '%s' does not exist" %
                                               group_id)

        members = []
        capabilities = ""
        for member_id in member_ids:
            try:
                member_path = self._path_member(group_id, member_id)
                capabilities = self._wrap_kazoo_call(self._coord.get,
                                                     member_path)
            except exceptions.NoNodeError:
                #If the current node does not exist then it means that it
                #leaved the group just after the get_children() call above.
                pass
            members.append(models.Member(group_id, member_id, capabilities[0]))
        return members

    def get_member(self, group_id, member_id):
        capabilities = ""
        try:
            member_path = self._path_member(group_id, member_id)
            capabilities = self._wrap_kazoo_call(self._coord.get,
                                                 member_path)[0]
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (member_id, group_id))
        return models.Member(group_id, member_id, capabilities)

    def update_capabilities(self, group_id, capabilities):
        try:
            member_path = self._path_member(group_id, self._member_id)
            self._wrap_kazoo_call(self._coord.set, member_path, capabilities)
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (self._member_id, group_id))

    def get_member_capabilities(self, group_id, member_id):
        capabilities = ""
        try:
            member_path = self._path_member(group_id, member_id)
            capabilities = self._wrap_kazoo_call(self._coord.get,
                                                 member_path)[0]
        except exceptions.NoNodeError:
            raise coordination.MemberNotJoined("member '%s' has not joined "
                                               "the group '%s' or the group "
                                               "has not been created" %
                                               (self._member_id, group_id))
        return capabilities

    def get_all_groups_ids(self):
        group_ids = []
        try:
            group_ids = self._wrap_kazoo_call(self._coord.get_children,
                                              paths.join("/", _TOOZ_NAMESPACE))
        except exceptions.NoNodeError:
            raise coordination.ToozError("tooz namespace has "
                                         "not been created")
        return group_ids

    @staticmethod
    def _path_member(group_id, member_id):
        return paths.join("/", _TOOZ_NAMESPACE, group_id, member_id)

    def _wrap_kazoo_call(self, func, *args, **kwargs):
        """Call and catch ZooKeeperError."""
        try:
            return func(*args, **kwargs)
        except exceptions.ZookeeperError as e:
            raise coordination.ToozError(str(e))
