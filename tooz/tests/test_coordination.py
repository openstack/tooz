# -*- coding: utf-8 -*-
#
#    Copyright © 2013-2014 eNovance Inc. All Rights Reserved.
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
import time
import uuid

import testscenarios
from testtools import testcase

import tooz.coordination
from tooz import tests


class TestAPI(testscenarios.TestWithScenarios,
              tests.TestCaseSkipNotImplemented):

    scenarios = [
        ('zookeeper', {'url': 'kazoo://127.0.0.1:2181?timeout=5'}),
        ('zake', {'url': 'zake://?timeout=5'}),
        ('memcached', {'url': 'memcached://?timeout=5'}),
        ('ipc', {'url': 'ipc://'}),
    ]

    def setUp(self):
        super(TestAPI, self).setUp()
        self.group_id = self._get_random_uuid()
        self.member_id = self._get_random_uuid()
        self._coord = tooz.coordination.get_coordinator(self.url,
                                                        self.member_id)
        try:
            self._coord.start()
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
        client = tooz.coordination.get_coordinator(self.url,
                                                   self.member_id)
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
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
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
        if self.url.startswith('zake://'):
            self.skipTest("Zake has a bug that prevent this test from working")
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
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
        if not self.url.startswith('memcached://'):
            self.skipTest("This test only works with memcached for now")
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
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

    def _set_event(self, event):
        self.event = event
        return 42

    def test_watch_group_join(self):
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        self._coord.create_group(self.group_id).get()

        # Watch the group
        self._coord.watch_join_group(self.group_id, self._set_event)

        # Join the group
        client2.join_group(self.group_id).get()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertTrue(member_id_test2 in members_ids)
        while True:
            if self._coord.run_watchers():
                break
        self.assertIsInstance(self.event,
                              tooz.coordination.MemberJoinedGroup)
        self.assertEqual(member_id_test2,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)

        # Stop watching
        self._coord.unwatch_join_group(self.group_id, self._set_event)
        self.event = None

        # Leave and rejoin group
        client2.leave_group(self.group_id).get()
        client2.join_group(self.group_id).get()
        self._coord.run_watchers()
        self.assertIsNone(self.event)

    def test_watch_leave_group(self):
        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        self._coord.create_group(self.group_id).get()

        # Watch the group: this can leads to race conditions in certain
        # driver that are not able to see all events, so we join, wait for
        # the join to be seen, and then we leave, and wait for the leave to
        # be seen.
        self._coord.watch_join_group(self.group_id, lambda children: True)
        self._coord.watch_leave_group(self.group_id, self._set_event)

        # Join and leave the group
        client2.join_group(self.group_id).get()
        # Consumes join event
        while True:
            if self._coord.run_watchers():
                break
        client2.leave_group(self.group_id).get()
        # Consumes leave event
        while True:
            if self._coord.run_watchers():
                break

        self.assertIsInstance(self.event,
                              tooz.coordination.MemberLeftGroup)
        self.assertEqual(member_id_test2,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)

        # Stop watching
        self._coord.unwatch_leave_group(self.group_id, self._set_event)
        self.event = None

        # Rejoin and releave group
        client2.join_group(self.group_id).get()
        client2.leave_group(self.group_id).get()
        self._coord.run_watchers()
        self.assertIsNone(self.event)

    def test_watch_join_group_non_existent(self):
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          self._coord.watch_join_group,
                          self.group_id,
                          lambda: None)
        self.assertEqual(0, len(self._coord._hooks_join_group[self.group_id]))

    def test_watch_leave_group_non_existent(self):
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          self._coord.watch_leave_group,
                          self.group_id,
                          lambda: None)
        self.assertEqual(0, len(self._coord._hooks_leave_group[self.group_id]))

    def test_run_for_election(self):
        self._coord.create_group(self.group_id).get()
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        self.assertIsInstance(self.event,
                              tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)

    def test_run_for_election_multiple_clients(self):
        self._coord.create_group(self.group_id).get()
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        client2.watch_elected_as_leader(self.group_id, self._set_event)
        client2.run_watchers()

        self.assertIsInstance(self.event,
                              tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)
        self.assertEqual(self._coord.get_leader(self.group_id).get(),
                         self.member_id)

        self.event = None

        self._coord.stop()
        client2.run_watchers()

        self.assertIsInstance(self.event,
                              tooz.coordination.LeaderElected)
        self.assertEqual(member_id_test2,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)
        self.assertEqual(client2.get_leader(self.group_id).get(),
                         member_id_test2)

    def test_get_leader(self):
        self._coord.create_group(self.group_id).get()

        leader = self._coord.get_leader(self.group_id).get()
        self.assertEqual(leader, None)

        self._coord.join_group(self.group_id).get()

        leader = self._coord.get_leader(self.group_id).get()
        self.assertEqual(leader, None)

        # Let's get elected
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        leader = self._coord.get_leader(self.group_id).get()
        self.assertEqual(leader, self.member_id)

    def test_run_for_election_multiple_clients_stand_down(self):
        self._coord.create_group(self.group_id).get()
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        member_id_test2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        client2.watch_elected_as_leader(self.group_id, self._set_event)
        client2.run_watchers()

        self.assertIsInstance(self.event,
                              tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)

        self.event = None

        self._coord.stand_down_group_leader(self.group_id)
        client2.run_watchers()

        self.assertIsInstance(self.event,
                              tooz.coordination.LeaderElected)
        self.assertEqual(member_id_test2,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)

        self.event = None

        client2.stand_down_group_leader(self.group_id)
        self._coord.run_watchers()

        self.assertIsInstance(self.event,
                              tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id,
                         self.event.member_id)
        self.assertEqual(self.group_id,
                         self.event.group_id)

    def test_get_lock(self):
        lock = self._coord.get_lock(self._get_random_uuid())
        self.assertEqual(True, lock.acquire())
        lock.release()
        with lock:
            pass

    def test_get_lock_multiple_coords(self):
        member_id2 = self._get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id2)
        client2.start()

        lock_name = self._get_random_uuid()
        lock = self._coord.get_lock(lock_name)
        self.assertEqual(True, lock.acquire())

        lock2 = client2.get_lock(lock_name)
        self.assertEqual(False, lock2.acquire(blocking=False))
        lock.release()
        self.assertEqual(True, lock2.acquire(blocking=True))

    @staticmethod
    def _get_random_uuid():
        return str(uuid.uuid4()).encode('ascii')


class TestHook(testcase.TestCase):
    def setUp(self):
        super(TestHook, self).setUp()
        self.hooks = tooz.coordination.Hooks()
        self.triggered = False

    def _trigger(self):
        self.triggered = True

    def test_register_hook(self):
        self.assertEqual(self.hooks.run(), [])
        self.assertFalse(self.triggered)
        self.hooks.append(self._trigger)
        self.assertEqual(self.hooks.run(), [None])
        self.assertTrue(self.triggered)

    def test_unregister_hook(self):
        self.hooks.append(self._trigger)
        self.assertEqual(self.hooks.run(), [None])
        self.assertTrue(self.triggered)
        self.triggered = False
        self.hooks.remove(self._trigger)
        self.assertEqual(self.hooks.run(), [])
        self.assertFalse(self.triggered)
