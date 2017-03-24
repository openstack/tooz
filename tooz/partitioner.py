# -*- coding: utf-8 -*-
#
#    Copyright (C) 2016 Red Hat, Inc.
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
import logging

from tooz import hashring
from tooz import utils


LOG = logging.getLogger(__name__)


class Partitioner(object):
    """Partition set of objects across several members.

    Objects to be partitioned should implement the __tooz_hash__ method to
    identify themselves across the consistent hashring. This should method
    return bytes.

    """

    DEFAULT_PARTITION_NUMBER = hashring.HashRing.DEFAULT_PARTITION_NUMBER

    def __init__(self, coordinator, group_id,
                 partitions=DEFAULT_PARTITION_NUMBER):
        members = coordinator.get_members(group_id)
        self.partitions = partitions
        self.group_id = group_id
        self._coord = coordinator
        caps = [(m, self._coord.get_member_capabilities(self.group_id, m))
                for m in members.get()]
        self._coord.watch_join_group(self.group_id, self._on_member_join)
        self._coord.watch_leave_group(self.group_id, self._on_member_leave)
        self.ring = hashring.HashRing([], partitions=self.partitions)
        for m_id, cap in caps:
            self.ring.add_node(m_id, utils.loads(cap.get()).get("weight", 1))

    def _on_member_join(self, event):
        try:
            weight = utils.loads(self._coord.get_member_capabilities(
                self.group_id, event.member_id).get()).get("weight", 1)
        except utils.SerializationError:
            # This node does not seem to have joined with the partitioner
            # system, so just ignore it.
            LOG.warning(
                "Node %s did not join group %s in partition mode, ignoring",
                self.group_id, event.member_id)
        else:
            self.ring.add_node(event.member_id, weight)

    def _on_member_leave(self, event):
        self.ring.remove_node(event.member_id)

    @staticmethod
    def _hash_object(obj):
        if hasattr(obj, "__tooz_hash__"):
            return obj.__tooz_hash__()
        return str(hash(obj)).encode('ascii')

    def members_for_object(self, obj, ignore_members=None, replicas=1):
        """Return the members responsible for an object.

        :param obj: The object to check owning for.
        :param member_id: The member to check if it owns the object.
        :param ignore_members: Group members to ignore.
        :param replicas: Number of replicas for the object.
        """
        return self.ring.get_nodes(self._hash_object(obj),
                                   ignore_nodes=ignore_members,
                                   replicas=replicas)

    def belongs_to_member(self, obj, member_id,
                          ignore_members=None, replicas=1):
        """Return whether an object belongs to a member.

        :param obj: The object to check owning for.
        :param member_id: The member to check if it owns the object.
        :param ignore_members: Group members to ignore.
        :param replicas: Number of replicas for the object.
        """
        return member_id in self.members_for_object(
            obj, ignore_members=ignore_members, replicas=replicas)

    def belongs_to_self(self, obj, ignore_members=None, replicas=1):
        """Return whether an object belongs to this coordinator.

        :param obj: The object to check owning for.
        :param ignore_members: Group members to ignore.
        :param replicas: Number of replicas for the object.
        """
        return self.belongs_to_member(obj, self._coord._member_id,
                                      ignore_members=ignore_members,
                                      replicas=replicas)

    def stop(self):
        """Stop the partitioner."""
        self._coord.unwatch_join_group(self.group_id, self._on_member_join)
        self._coord.unwatch_leave_group(self.group_id, self._on_member_leave)
