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
import time
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

    scenarios = [
        zookeeper_tests,
        fake_zookeeper_tests,
        ('memcached', {'backend': 'memcached',
                       'kwargs': {'membership_timeout': 5}}),
    ]

    def setUp(self):
        super(TestAPI, self).setUp()
        self.group_id = self._get_random_uuid()
        self.member_id = self._get_random_uuid()
        self._coord = tooz.coordination.get_coordinator(self.backend,
                                                        self.member_id,
                                                        **self.kwargs)
        # HACK(jd) Disable memcached on py33 for the time being, activate as
        # soon as https://github.com/pinterest/pymemcache/pull/16 is merged
        # and pymemcache is released
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

    def test_create_group_already_exist(self):
        self._coord.create_group(self.group_id).get()
        create_group = self._coord.create_group(self.group_id)
        self.assertRaises(tooz.coordination.GroupAlreadyExist,
                          create_group.get)

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

    def test_join_nonexistent_group(self):
        group_id_test = self._get_random_uuid()
        join_group = self._coord.join_group(group_id_test)
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          join_group.get)

    def test_join_group_with_member_id_already_exists(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        client = tooz.coordination.get_coordinator(self.backend,
                                                   self.member_id,
                                                   **self.kwargs)
        client.start()
        join_group = client.join_group(self.group_id)
        self.assertRaises(tooz.coordination.MemberAlreadyExist,
                          join_group.get)

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

    def test_leave_nonexistent_group(self):
        all_group_ids = self._coord.get_groups().get()
        self.assertTrue(self.group_id not in all_group_ids)
        leave_group = self._coord.leave_group(self.group_id)
        try:
            leave_group.get()
        # Drivers raise one of those depending on their capability
        except (tooz.coordination.MemberNotJoined,
                tooz.coordination.GroupNotCreated):
            pass
        else:
            self.fail("Exception not raised")

    def test_leave_group_not_joined_by_member(self):
        self._coord.create_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertTrue(self.group_id in all_group_ids)
        leave_group = self._coord.leave_group(self.group_id)
        self.assertRaises(tooz.coordination.MemberNotJoined,
                          leave_group.get)

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

    def test_get_member_capabilities_nonexistent_group(self):
        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id)
        try:
            capa.get()
        # Drivers raise one of those depending on their capability
        except (tooz.coordination.MemberNotJoined,
                tooz.coordination.GroupNotCreated):
            pass
        else:
            self.fail("Exception not raised")

    def test_get_member_capabilities_nonjoined_member(self):
        self._coord.create_group(self.group_id).get()
        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id)
        self.assertRaises(tooz.coordination.MemberNotJoined,
                          capa.get)

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

    def test_update_capabilities_with_group_id_nonexistent(self):
        update_cap = self._coord.update_capabilities(self.group_id,
                                                     b'test_capabilities')
        try:
            update_cap.get()
        # Drivers raise one of those depending on their capability
        except (tooz.coordination.MemberNotJoined,
                tooz.coordination.GroupNotCreated):
            pass
        else:
            self.fail("Exception not raised")

    def test_heartbeat(self):
        self._coord.heartbeat()

    def test_disconnect_leave_group(self):
        if self.backend == 'zake':
            self.skipTest("Zake has a bug that prevent this test from working")
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.backend,
                                                    member_id_test2,
                                                    **self.kwargs)
        client2.start()
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        client2.join_group(self.group_id).get()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in members_ids)
        self.assertTrue(member_id_test2 in members_ids)
        client2.stop()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in members_ids)
        self.assertTrue(member_id_test2 not in members_ids)

    def test_timeout(self):
        if self.backend != 'memcached':
            self.skipTest("This test only works with memcached for now")
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.backend,
                                                    member_id_test2,
                                                    **self.kwargs)
        client2.start()
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        client2.join_group(self.group_id).get()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in members_ids)
        self.assertTrue(member_id_test2 in members_ids)
        time.sleep(3)
        self._coord.heartbeat()
        time.sleep(3)
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertTrue(self.member_id in members_ids)
        self.assertTrue(member_id_test2 not in members_ids)

    @staticmethod
    def _get_random_uuid():
        return str(uuid.uuid4()).encode('ascii')
