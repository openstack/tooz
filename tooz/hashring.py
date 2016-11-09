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


class UnknownNode(tooz.ToozError):
    """Node is unknown."""
    def __init__(self, node):
        super(UnknownNode, self).__init__("Unknown node `%s'" % node)
        self.node = node


class HashRing(object):
    """Map objects onto nodes based on their consistent hash."""

    def __init__(self, nodes, partitions=2**5):
        """Create a new hashring.

        :param nodes: List of nodes where objects will be mapped onto.
        :param partitions: Number of partitions to spread objects onto.
        """
        self.nodes = set()
        self._ring = dict()
        self._partitions = []
        self._partition_number = partitions

        self.add_nodes(set(nodes))

    def add_node(self, node):
        """Add a node to the hashring.

        :param node: Node to add.
        """
        return self.add_nodes((node,))

    def add_nodes(self, nodes):
        """Add nodes to the hashring.

        :param nodes: Nodes to add.
        """
        for node in nodes:
            key = str(node).encode('utf-8')
            key_hash = hashlib.md5(key)
            for r in six.moves.range(self._partition_number):
                key_hash.update(key)
                self._ring[self._hash2int(key_hash)] = node

            self.nodes.add(node)

        self._partitions = sorted(self._ring.keys())

    def remove_node(self, node):
        """Remove a node from the hashring.

        Raises py:exc:`UnknownNode`

        :param node: Node to remove.
        """
        try:
            self.nodes.remove(node)
        except KeyError:
            raise UnknownNode(node)

        key = str(node).encode('utf-8')
        key_hash = hashlib.md5(key)
        for r in six.moves.range(self._partition_number):
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
        candidates = self.nodes - ignore_nodes

        replicas = min(replicas, len(candidates))

        nodes = set()
        for replica in six.moves.range(0, replicas):
            node = self._get_node(partition)
            while node in nodes or node in ignore_nodes:
                partition += 1
                if partition >= len(self._partitions):
                    partition = 0
                node = self._get_node(partition)
            nodes.add(node)

        return nodes

    def __getitem__(self, key):
        return self.get_nodes(key)

    def __len__(self):
        return len(self._partitions)
