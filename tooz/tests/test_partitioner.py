# -*- coding: utf-8 -*-
#
#    Copyright © 2016 Red Hat, Inc.
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
import six

from tooz import coordination
from tooz import tests


class TestPartitioner(tests.TestWithCoordinator):

    def setUp(self):
        super(TestPartitioner, self).setUp()
        self._extra_coords = []

    def tearDown(self):
        for c in self._extra_coords:
            c.stop()
        super(TestPartitioner, self).tearDown()

    def _add_members(self, number_of_members, weight=1):
        groups = []
        for _ in six.moves.range(number_of_members):
            m = tests.get_random_uuid()
            coord = coordination.get_coordinator(self.url, m)
            coord.start()
            groups.append(coord.join_partitioned_group(
                self.group_id, weight=weight))
            self._extra_coords.append(coord)
        self._coord.run_watchers()
        return groups

    def _remove_members(self, number_of_members):
        for _ in six.moves.range(number_of_members):
            c = self._extra_coords.pop()
            c.stop()
        self._coord.run_watchers()

    def test_join_partitioned_group(self):
        group_id = tests.get_random_uuid()
        self._coord.join_partitioned_group(group_id)

    def test_hashring_size(self):
        p = self._coord.join_partitioned_group(self.group_id)
        self.assertEqual(1, len(p.ring.nodes))
        self._add_members(1)
        self.assertEqual(2, len(p.ring.nodes))
        self._add_members(2)
        self.assertEqual(4, len(p.ring.nodes))
        self._remove_members(3)
        self.assertEqual(1, len(p.ring.nodes))
        p.stop()

    def test_hashring_weight(self):
        p = self._coord.join_partitioned_group(self.group_id, weight=5)
        self.assertEqual([5], list(p.ring.nodes.values()))
        p2 = self._add_members(1, weight=10)[0]
        self.assertEqual(set([5, 10]), set(p.ring.nodes.values()))
        self.assertEqual(set([5, 10]), set(p2.ring.nodes.values()))
        p.stop()

    def test_stop(self):
        p = self._coord.join_partitioned_group(self.group_id)
        p.stop()
        self.assertEqual(0, len(self._coord._hooks_join_group))
        self.assertEqual(0, len(self._coord._hooks_leave_group))

    def test_members_of_object_and_others(self):
        p = self._coord.join_partitioned_group(self.group_id)
        self._add_members(3)
        o = six.text_type(u"чупакабра")
        m = p.members_for_object(o)
        self.assertEqual(1, len(m))
        m = m.pop()
        self.assertTrue(p.belongs_to_member(o, m))
        self.assertFalse(p.belongs_to_member(o, b"chupacabra"))
        maybe = self.assertTrue if m == self.member_id else self.assertFalse
        maybe(p.belongs_to_self(o))
        p.stop()


class ZakeTestPartitioner(TestPartitioner):
    url = "zake://"


class IPCTestPartitioner(TestPartitioner):
    url = "ipc://"


class FileTestPartitioner(TestPartitioner):
    url = "file:///tmp"
