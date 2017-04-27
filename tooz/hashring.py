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
import bisect
import hashlib

import six

import tooz
from tooz import utils


class UnknownNode(tooz.ToozError):
    """Node is unknown."""
    def __init__(self, node):
        super(UnknownNode, self).__init__("Unknown node `%s'" % node)
        self.node = node


class HashRing(object):
    """Map objects onto nodes based on their consistent hash."""

    DEFAULT_PARTITION_NUMBER = 2**5

    def __init__(self, nodes, partitions=DEFAULT_PARTITION_NUMBER):
        """Create a new hashring.

        :param nodes: List of nodes where objects will be mapped onto.
        :param partitions: Number of partitions to spread objects onto.
        """
        self.nodes = {}
        self._ring = dict()
        self._partitions = []
        self._partition_number = partitions

        self.add_nodes(set(nodes))

    def add_node(self, node, weight=1):
        """Add a node to the hashring.

        :param node: Node to add.
        :param weight: How many resource instances this node should manage
        compared to the other nodes (default 1). Higher weights will be
        assigned more resources. Three nodes A, B and C with weights 1, 2 and 3
        will each handle 1/6, 1/3 and 1/2 of the resources, respectively.

        """
        return self.add_nodes((node,), weight)

    def add_nodes(self, nodes, weight=1):
        """Add nodes to the hashring with equal weight

        :param nodes: Nodes to add.
        :param weight: How many resource instances this node should manage
        compared to the other nodes (default 1). Higher weights will be
        assigned more resources. Three nodes A, B and C with weights 1, 2 and 3
        will each handle 1/6, 1/3 and 1/2 of the resources, respectively.
        """
        for node in nodes:
            key = utils.to_binary(node, 'utf-8')
            key_hash = hashlib.md5(key)
            for r in six.moves.range(self._partition_number * weight):
                key_hash.update(key)
                self._ring[self._hash2int(key_hash)] = node

            self.nodes[node] = weight

        self._partitions = sorted(self._ring.keys())

    def remove_node(self, node):
        """Remove a node from the hashring.

        Raises py:exc:`UnknownNode`

        :param node: Node to remove.
        """
        try:
            weight = self.nodes.pop(node)
        except KeyError:
            raise UnknownNode(node)

        key = utils.to_binary(node, 'utf-8')
        key_hash = hashlib.md5(key)
        for r in six.moves.range(self._partition_number * weight):
            key_hash.update(key)
            del self._ring[self._hash2int(key_hash)]

        self._partitions = sorted(self._ring.keys())

    @staticmethod
    def _hash2int(key):
        return int(key.hexdigest(), 16)

    def _get_partition(self, data):
        hashed_key = self._hash2int(hashlib.md5(data))
        position = bisect.bisect(self._partitions, hashed_key)
        return position if position < len(self._partitions) else 0

    def _get_node(self, partition):
        return self._ring[self._partitions[partition]]

    def get_nodes(self, data, ignore_nodes=None, replicas=1):
        """Get the set of nodes which the supplied data map onto.

        :param data: A byte identifier to be mapped across the ring.
        :param ignore_nodes: Set of nodes to ignore.
        :param replicas: Number of replicas to use.
        :return: A set of nodes whose length depends on the number of replicas.
        """
        partition = self._get_partition(data)

        ignore_nodes = set(ignore_nodes) if ignore_nodes else set()
        candidates = set(self.nodes.keys()) - ignore_nodes

        replicas = min(replicas, len(candidates))

        nodes = set()
        while len(nodes) < replicas:
            node = self._get_node(partition)
            if node not in ignore_nodes:
                nodes.add(node)
            partition = (partition + 1
                         if partition + 1 < len(self._partitions) else 0)
        return nodes

    def __getitem__(self, key):
        return self.get_nodes(key)

    def __len__(self):
        return len(self._partitions)
