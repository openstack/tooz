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

import threading
import uuid

import testscenarios
from testtools import testcase

import tooz.coordination
from zake import fake_storage

# Real ZooKeeper server scenario
zookeeper_tests = ('zookeeper_tests', {'backend': 'kazoo',
                                       'kwargs': {'hosts': '127.0.0.1:2181'}})

# Fake Kazoo client scenario
fake_storage = fake_storage.FakeStorage(threading.RLock())
fake_zookeeper_tests = ('fake_zookeeper_tests', {'backend': 'zake',
                                                 'kwargs': {'storage':
                                                            fake_storage}})


class TestAPI(testscenarios.TestWithScenarios, testcase.TestCase):

    scenarios = [zookeeper_tests, fake_zookeeper_tests]

    def setUp(self):
        super(TestAPI, self).setUp()
        self.group_id = self._get_random_uuid()
        self.member_id = self._get_random_uuid()
        self._coord = tooz.coordination.get_coordinator(self.backend,
                                                        self.member_id,
                                                        **self.kwargs)
        try:
            self._coord.start(timeout=5)
        except tooz.coordination.ToozConnectionError as e:
            raise testcase.TestSkipped(str(e))

    def tearDown(self):
        self._coord.stop()
        super(TestAPI, self).tearDown()

    def test_create_group(self):
        self._coord.create_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertTrue(self.group_id in all_group_ids)

    def test_get_groups(self):
        groups_ids = [self._get_random_uuid() for _ in range(0, 5)]
        for group_id in groups_ids:
            self._coord.create_group(group_id).get()
        created_groups = self._coord.get_groups().get()
        for group_id in groups_ids:
            self.assertTrue(group_id in created_groups)

    def test_join_group(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        member_list = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in member_list)

    def test_leave_group(self):
        self._coord.create_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertTrue(self.group_id in all_group_ids)
        self._coord.join_group(self.group_id).get()
        member_list = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in member_list)
        member_ids = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in member_ids)
        self._coord.leave_group(self.group_id).get()
        new_member_objects = self._coord.get_members(self.group_id).get()
        new_member_list = [member.member_id for member in new_member_objects]
        self.assertTrue(self.member_id not in new_member_list)

    def test_get_members(self):
        group_id_test2 = self._get_random_uuid()
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.backend,
                                                    member_id_test2,
                                                    **self.kwargs)
        client2.start()

        self._coord.create_group(group_id_test2).get()
        self._coord.join_group(group_id_test2).get()
        client2.join_group(group_id_test2).get()
        members_ids = self._coord.get_members(group_id_test2).get()
        self.assertTrue(self.member_id in members_ids)
        self.assertTrue(member_id_test2 in members_ids)

    def test_get_member_capabilities(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id, b"test_capabilities")

        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id).get()
        self.assertEqual(capa, b"test_capabilities")

    def test_update_capabilities(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id, b"test_capabilities1").get()

        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id).get()
        self.assertEqual(capa, b"test_capabilities1")
        self._coord.update_capabilities(self.group_id,
                                        b"test_capabilities2").get()

        capa2 = self._coord.get_member_capabilities(self.group_id,
                                                    self.member_id).get()
        self.assertEqual(capa2, b"test_capabilities2")

    def _get_random_uuid(self):
        return str(uuid.uuid4())
