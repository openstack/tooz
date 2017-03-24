# Copyright 2013 Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
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

import hashlib

import mock
from testtools import matchers
from testtools import testcase

from tooz import hashring


class HashRingTestCase(testcase.TestCase):

    # NOTE(deva): the mapping used in these tests is as follows:
    #             if nodes = [foo, bar]:
    #                fake -> foo, bar
    #             if nodes = [foo, bar, baz]:
    #                fake -> foo, bar, baz
    #                fake-again -> bar, baz, foo

    @mock.patch.object(hashlib, 'md5', autospec=True)
    def test_hash2int_returns_int(self, mock_md5):
        r1 = 32 * 'a'
        r2 = 32 * 'b'
        # 2**PARTITION_EXPONENT calls to md5.update per node
        # PARTITION_EXPONENT is currently always 5, so 32 calls each here
        mock_md5.return_value.hexdigest.side_effect = [r1] * 32 + [r2] * 32

        nodes = ['foo', 'bar']
        ring = hashring.HashRing(nodes)

        self.assertIn(int(r1, 16), ring._ring)
        self.assertIn(int(r2, 16), ring._ring)

    def test_create_ring(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * 2, len(ring))

    def test_add_node(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.add('baz')
        ring.add_node('baz')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))

    def test_add_node_bytes(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.add(b'Z\xe2\xfa\x90\x17EC\xac\xae\x88\xa7[\xa1}:E')
        ring.add_node(b'Z\xe2\xfa\x90\x17EC\xac\xae\x88\xa7[\xa1}:E')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))

    def test_add_node_unicode(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.add(u'\u0634\u0628\u06a9\u0647')
        ring.add_node(u'\u0634\u0628\u06a9\u0647')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))

    def test_add_node_weight(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.add('baz')
        ring.add_node('baz', weight=10)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * 12, len(ring))

    def test_add_nodes_weight(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.add('baz')
        nodes.add('baz2')
        ring.add_nodes(set(['baz', 'baz2']), weight=10)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * 22, len(ring))

    def test_remove_node(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.discard('bar')
        ring.remove_node('bar')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))

    def test_remove_node_bytes(self):
        nodes = {'foo', b'Z\xe2\xfa\x90\x17EC\xac\xae\x88\xa7[\xa1}:E'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.discard(b'Z\xe2\xfa\x90\x17EC\xac\xae\x88\xa7[\xa1}:E')
        ring.remove_node(b'Z\xe2\xfa\x90\x17EC\xac\xae\x88\xa7[\xa1}:E')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))

    def test_remove_node_unknown(self):
        nodes = ['foo', 'bar']
        ring = hashring.HashRing(nodes)
        self.assertRaises(
            hashring.UnknownNode,
            ring.remove_node, 'biz')

    def test_add_then_removenode(self):
        nodes = {'foo', 'bar'}
        ring = hashring.HashRing(nodes)
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.add('baz')
        ring.add_node('baz')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))
        nodes.discard('bar')
        ring.remove_node('bar')
        self.assertEqual(nodes, set(ring.nodes.keys()))
        self.assertEqual(2 ** 5 * len(nodes), len(ring))

    def test_distribution_one_replica(self):
        nodes = ['foo', 'bar', 'baz']
        ring = hashring.HashRing(nodes)
        fake_1_nodes = ring.get_nodes(b'fake')
        fake_2_nodes = ring.get_nodes(b'fake-again')
        # We should have one nodes for each thing
        self.assertEqual(1, len(fake_1_nodes))
        self.assertEqual(1, len(fake_2_nodes))
        # And they must not be the same answers even on this simple data.
        self.assertNotEqual(fake_1_nodes, fake_2_nodes)

    def test_distribution_more_replica(self):
        nodes = ['foo', 'bar', 'baz']
        ring = hashring.HashRing(nodes)
        fake_1_nodes = ring.get_nodes(b'fake', replicas=2)
        fake_2_nodes = ring.get_nodes(b'fake-again', replicas=2)
        # We should have one nodes for each thing
        self.assertEqual(2, len(fake_1_nodes))
        self.assertEqual(2, len(fake_2_nodes))
        fake_1_nodes = ring.get_nodes(b'fake', replicas=3)
        fake_2_nodes = ring.get_nodes(b'fake-again', replicas=3)
        # We should have one nodes for each thing
        self.assertEqual(3, len(fake_1_nodes))
        self.assertEqual(3, len(fake_2_nodes))
        self.assertEqual(fake_1_nodes, fake_2_nodes)

    def test_ignore_nodes(self):
        nodes = ['foo', 'bar', 'baz']
        ring = hashring.HashRing(nodes)
        equals_bar_or_baz = matchers.MatchesAny(
            matchers.Equals({'bar'}),
            matchers.Equals({'baz'}))
        self.assertThat(
            ring.get_nodes(b'fake', ignore_nodes=['foo']),
            equals_bar_or_baz)
        self.assertThat(
            ring.get_nodes(b'fake', ignore_nodes=['foo', 'bar']),
            equals_bar_or_baz)
        self.assertEqual(set(), ring.get_nodes(b'fake', ignore_nodes=nodes))

    @staticmethod
    def _compare_rings(nodes, conductors, ring, new_conductors, new_ring):
        delta = {}
        mapping = {
            'node': list(ring.get_nodes(node.encode('ascii')))[0]
            for node in nodes
        }
        new_mapping = {
            'node': list(new_ring.get_nodes(node.encode('ascii')))[0]
            for node in nodes
        }

        for key, old in mapping.items():
            new = new_mapping.get(key, None)
            if new != old:
                delta[key] = (old, new)
        return delta

    def test_rebalance_stability_join(self):
        num_services = 10
        num_nodes = 10000
        # Adding 1 service to a set of N should move 1/(N+1) of all nodes
        # Eg, for a cluster of 10 nodes, adding one should move 1/11, or 9%
        # We allow for 1/N to allow for rounding in tests.
        redistribution_factor = 1.0 / num_services

        nodes = [str(x) for x in range(num_nodes)]
        services = [str(x) for x in range(num_services)]
        new_services = services + ['new']
        delta = self._compare_rings(
            nodes, services, hashring.HashRing(services),
            new_services, hashring.HashRing(new_services))

        self.assertLess(len(delta), num_nodes * redistribution_factor)

    def test_rebalance_stability_leave(self):
        num_services = 10
        num_nodes = 10000
        # Removing 1 service from a set of N should move 1/(N) of all nodes
        # Eg, for a cluster of 10 nodes, removing one should move 1/10, or 10%
        # We allow for 1/(N-1) to allow for rounding in tests.
        redistribution_factor = 1.0 / (num_services - 1)

        nodes = [str(x) for x in range(num_nodes)]
        services = [str(x) for x in range(num_services)]
        new_services = services[:]
        new_services.pop()
        delta = self._compare_rings(
            nodes, services, hashring.HashRing(services),
            new_services, hashring.HashRing(new_services))

        self.assertLess(len(delta), num_nodes * redistribution_factor)

    def test_ignore_non_existent_node(self):
        nodes = ['foo', 'bar']
        ring = hashring.HashRing(nodes)
        self.assertEqual({'foo'}, ring.get_nodes(b'fake',
                                                 ignore_nodes=['baz']))
