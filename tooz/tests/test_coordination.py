# -*- coding: utf-8 -*-
#
#    Copyright © 2013-2015 eNovance Inc. All Rights Reserved.
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
from unittest import mock
import urllib

from concurrent import futures
from testtools import matchers
from testtools import testcase

import tooz
import tooz.coordination
from tooz import tests


def try_to_lock_job(name, coord, url, member_id):
    if not coord:
        coord = tooz.coordination.get_coordinator(
            url, member_id)
        coord.start()
    lock2 = coord.get_lock(name)
    return lock2.acquire(blocking=False)


class TestAPI(tests.TestWithCoordinator):
    def assertRaisesAny(self, exc_classes, callable_obj, *args, **kwargs):
        checkers = [matchers.MatchesException(exc_class)
                    for exc_class in exc_classes]
        matcher = matchers.Raises(matchers.MatchesAny(*checkers))
        callable_obj = testcase.Nullary(callable_obj, *args, **kwargs)
        self.assertThat(callable_obj, matcher)

    def test_connection_error_bad_host(self):
        if (tooz.coordination.Characteristics.DISTRIBUTED_ACROSS_HOSTS
           not in self._coord.CHARACTERISTICS):
            self.skipTest("This driver is not distributed across hosts")
        scheme = urllib.parse.urlparse(self.url).scheme
        coord = tooz.coordination.get_coordinator(
            "%s://localhost:1/f00" % scheme,
            self.member_id)
        self.assertRaises(tooz.coordination.ToozConnectionError,
                          coord.start)

    def test_stop_first(self):
        c = tooz.coordination.get_coordinator(self.url,
                                              self.member_id)
        self.assertRaises(tooz.ToozError,
                          c.stop)

    def test_create_group(self):
        self._coord.create_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertIn(self.group_id, all_group_ids)

    def test_get_lock_release_broken(self):
        name = tests.get_random_uuid()
        memberid2 = tests.get_random_uuid()
        coord2 = tooz.coordination.get_coordinator(self.url,
                                                   memberid2)
        coord2.start()
        lock1 = self._coord.get_lock(name)
        lock2 = coord2.get_lock(name)
        self.assertTrue(lock1.acquire(blocking=False))
        self.assertFalse(lock2.acquire(blocking=False))
        self.assertTrue(lock2.break_())
        self.assertTrue(lock2.acquire(blocking=False))
        self.assertFalse(lock1.release())
        # Assert lock is not accidentally broken now
        memberid3 = tests.get_random_uuid()
        coord3 = tooz.coordination.get_coordinator(self.url,
                                                   memberid3)
        coord3.start()
        lock3 = coord3.get_lock(name)
        self.assertFalse(lock3.acquire(blocking=False))

    def test_create_group_already_exist(self):
        self._coord.create_group(self.group_id).get()
        create_group = self._coord.create_group(self.group_id)
        self.assertRaises(tooz.coordination.GroupAlreadyExist,
                          create_group.get)

    def test_get_groups(self):
        groups_ids = [tests.get_random_uuid() for _ in range(0, 5)]
        for group_id in groups_ids:
            self._coord.create_group(group_id).get()
        created_groups = self._coord.get_groups().get()
        for group_id in groups_ids:
            self.assertIn(group_id, created_groups)

    def test_delete_group(self):
        self._coord.create_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertIn(self.group_id, all_group_ids)
        self._coord.delete_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertNotIn(self.group_id, all_group_ids)
        join_group = self._coord.join_group(self.group_id)
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          join_group.get)

    def test_delete_group_non_existent(self):
        delete = self._coord.delete_group(self.group_id)
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          delete.get)

    def test_delete_group_non_empty(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        delete = self._coord.delete_group(self.group_id)
        self.assertRaises(tooz.coordination.GroupNotEmpty,
                          delete.get)
        self._coord.leave_group(self.group_id).get()
        self._coord.delete_group(self.group_id).get()

    def test_join_group(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        member_list = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, member_list)

    def test_join_nonexistent_group(self):
        join_group = self._coord.join_group(self.group_id)
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          join_group.get)

    def test_join_group_create(self):
        self._coord.join_group_create(self.group_id)
        member_list = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, member_list)

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
        self.assertIn(self.group_id, all_group_ids)
        self._coord.join_group(self.group_id).get()
        member_list = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, member_list)
        member_ids = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, member_ids)
        self._coord.leave_group(self.group_id).get()
        new_member_objects = self._coord.get_members(self.group_id).get()
        new_member_list = [member.member_id for member in new_member_objects]
        self.assertNotIn(self.member_id, new_member_list)

    def test_leave_nonexistent_group(self):
        all_group_ids = self._coord.get_groups().get()
        self.assertNotIn(self.group_id, all_group_ids)
        leave_group = self._coord.leave_group(self.group_id)
        # Drivers raise one of those depending on their capability
        self.assertRaisesAny([tooz.coordination.MemberNotJoined,
                              tooz.coordination.GroupNotCreated],
                             leave_group.get)

    def test_leave_group_not_joined_by_member(self):
        self._coord.create_group(self.group_id).get()
        all_group_ids = self._coord.get_groups().get()
        self.assertIn(self.group_id, all_group_ids)
        leave_group = self._coord.leave_group(self.group_id)
        self.assertRaises(tooz.coordination.MemberNotJoined,
                          leave_group.get)

    def test_get_lock_twice_locked_one_released_two(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock2 = self._coord.get_lock(name)
        self.assertTrue(lock1.acquire())
        self.assertFalse(lock2.acquire(blocking=False))
        self.assertFalse(lock2.release())
        self.assertTrue(lock1.release())
        self.assertFalse(lock2.release())

    def test_get_members(self):
        group_id_test2 = tests.get_random_uuid()
        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()

        self._coord.create_group(group_id_test2).get()
        self._coord.join_group(group_id_test2).get()
        client2.join_group(group_id_test2).get()
        members_ids = self._coord.get_members(group_id_test2).get()
        self.assertEqual({self.member_id, member_id_test2}, members_ids)

    def test_get_member_capabilities(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id, b"test_capabilities")

        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id).get()
        self.assertEqual(capa, b"test_capabilities")

    def test_get_member_capabilities_complex(self):
        self._coord.create_group(self.group_id).get()
        caps = {
            'type': 'warrior',
            'abilities': ['fight', 'flight', 'double-hit-damage'],
        }
        self._coord.join_group(self.group_id, caps).get()
        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id).get()
        self.assertEqual(capa, caps)
        self.assertEqual(capa['type'], caps['type'])

    def test_get_member_capabilities_nonexistent_group(self):
        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id)
        # Drivers raise one of those depending on their capability
        self.assertRaisesAny([tooz.coordination.MemberNotJoined,
                              tooz.coordination.GroupNotCreated],
                             capa.get)

    def test_get_member_capabilities_nonjoined_member(self):
        self._coord.create_group(self.group_id).get()
        capa = self._coord.get_member_capabilities(self.group_id,
                                                   self.member_id)
        self.assertRaises(tooz.coordination.MemberNotJoined,
                          capa.get)

    def test_get_member_info(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id, b"test_capabilities").get()

        member_info = self._coord.get_member_info(self.group_id,
                                                  self.member_id).get()
        self.assertEqual(member_info['capabilities'], b"test_capabilities")

    def test_get_member_info_complex(self):
        self._coord.create_group(self.group_id).get()
        caps = {
            'type': 'warrior',
            'abilities': ['fight', 'flight', 'double-hit-damage'],
        }
        member_info = {'capabilities': 'caps',
                       'created_at': '0',
                       'updated_at': '0'}
        self._coord.join_group(self.group_id, caps).get()
        member_info = self._coord.get_member_info(self.group_id,
                                                  self.member_id).get()
        self.assertEqual(member_info['capabilities'], caps)

    def test_get_member_info_nonexistent_group(self):
        member_info = self._coord.get_member_info(self.group_id,
                                                  self.member_id)
        # Drivers raise one of those depending on their capability
        self.assertRaisesAny([tooz.coordination.MemberNotJoined,
                              tooz.coordination.GroupNotCreated],
                             member_info.get)

    def test_get_member_info_nonjoined_member(self):
        self._coord.create_group(self.group_id).get()
        member_id = tests.get_random_uuid()
        member_info = self._coord.get_member_info(self.group_id,
                                                  member_id)
        self.assertRaises(tooz.coordination.MemberNotJoined,
                          member_info.get)

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
        # Drivers raise one of those depending on their capability
        self.assertRaisesAny([tooz.coordination.MemberNotJoined,
                              tooz.coordination.GroupNotCreated],
                             update_cap.get)

    def test_heartbeat(self):
        if not self._coord.requires_beating:
            raise testcase.TestSkipped("Test not applicable (heartbeating"
                                       " not required)")
        self._coord.heartbeat()

    def test_heartbeat_loop(self):
        if not self._coord.requires_beating:
            raise testcase.TestSkipped("Test not applicable (heartbeating"
                                       " not required)")

        heart = self._coord.heart
        self.assertFalse(heart.is_alive())
        heart.start()

        # This will timeout if nothing ever is done...
        try:
            while not heart.beats:
                time.sleep(1)
        finally:
            heart.stop()
            heart.wait()

    def test_disconnect_leave_group(self):
        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        client2.join_group(self.group_id).get()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, members_ids)
        self.assertIn(member_id_test2, members_ids)
        client2.stop()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, members_ids)
        self.assertNotIn(member_id_test2, members_ids)

    def test_timeout(self):
        if (tooz.coordination.Characteristics.NON_TIMEOUT_BASED
           in self._coord.CHARACTERISTICS):
            self.skipTest("This driver is not based on timeout")
        self._coord.stop()
        if "?" in self.url:
            sep = "&"
        else:
            sep = "?"
        url = self.url + sep + "timeout=5"
        self._coord = tooz.coordination.get_coordinator(url, self.member_id)
        self._coord.start()

        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(url, member_id_test2)
        client2.start()
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        client2.join_group(self.group_id).get()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertIn(self.member_id, members_ids)
        self.assertIn(member_id_test2, members_ids)

        # Watch the group, we want to be sure that when client2 is kicked out
        # we get an event.
        self._coord.watch_leave_group(self.group_id, self._set_event)

        # Run watchers to be sure we initialize the member cache and we *know*
        # client2 is a member now
        self._coord.run_watchers()

        time.sleep(3)
        self._coord.heartbeat()
        time.sleep(3)

        # Now client2 has timed out!

        members_ids = self._coord.get_members(self.group_id).get()
        while True:
            if self._coord.run_watchers():
                break
        self.assertIn(self.member_id, members_ids)
        self.assertNotIn(member_id_test2, members_ids)
        # Check that the event has been triggered
        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.MemberLeftGroup)
        self.assertEqual(member_id_test2, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

    def _set_event(self, event):
        if not hasattr(self, "events"):
            self.events = [event]
        else:
            self.events.append(event)
        return 42

    def test_watch_group_join(self):
        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        self._coord.create_group(self.group_id).get()

        # Watch the group
        self._coord.watch_join_group(self.group_id, self._set_event)

        # Join the group
        client2.join_group(self.group_id).get()
        members_ids = self._coord.get_members(self.group_id).get()
        self.assertIn(member_id_test2, members_ids)
        while True:
            if self._coord.run_watchers():
                break
        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.MemberJoinedGroup)
        self.assertEqual(member_id_test2, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

        # Stop watching
        self._coord.unwatch_join_group(self.group_id, self._set_event)
        self.events = []

        # Leave and rejoin group
        client2.leave_group(self.group_id).get()
        client2.join_group(self.group_id).get()
        self._coord.run_watchers()
        self.assertEqual([], self.events)

    def test_watch_leave_group(self):
        member_id_test2 = tests.get_random_uuid()
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

        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.MemberLeftGroup)
        self.assertEqual(member_id_test2, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

        # Stop watching
        self._coord.unwatch_leave_group(self.group_id, self._set_event)
        self.events = []

        # Rejoin and releave group
        client2.join_group(self.group_id).get()
        client2.leave_group(self.group_id).get()
        self._coord.run_watchers()
        self.assertEqual([], self.events)

    def test_watch_join_group_disappear(self):
        if not hasattr(self._coord, '_destroy_group'):
            self.skipTest("This test only works with coordinators"
                          " that have the ability to destroy groups.")

        self._coord.create_group(self.group_id).get()
        self._coord.watch_join_group(self.group_id, self._set_event)
        self._coord.watch_leave_group(self.group_id, self._set_event)

        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        client2.join_group(self.group_id).get()

        while True:
            if self._coord.run_watchers():
                break
        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.MemberJoinedGroup)
        self.events = []

        # Force the group to disappear...
        self._coord._destroy_group(self.group_id)

        while True:
            if self._coord.run_watchers():
                break

        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.MemberLeftGroup)

    def test_watch_join_group_non_existent(self):
        self.assertRaises(tooz.coordination.GroupNotCreated,
                          self._coord.watch_join_group,
                          self.group_id,
                          lambda: None)
        self.assertEqual(0, len(self._coord._hooks_join_group[self.group_id]))

    def test_watch_join_group_booted_out(self):
        self._coord.create_group(self.group_id).get()
        self._coord.join_group(self.group_id).get()
        self._coord.watch_join_group(self.group_id, self._set_event)
        self._coord.watch_leave_group(self.group_id, self._set_event)

        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        client2.join_group(self.group_id).get()

        while True:
            if self._coord.run_watchers():
                break

        client3 = tooz.coordination.get_coordinator(self.url, self.member_id)
        client3.start()
        client3.leave_group(self.group_id).get()

        # Only works for clients that have access to the groups they are part
        # of, to ensure that after we got booted out by client3 that this
        # client now no longer believes its part of the group.
        if (hasattr(self._coord, '_joined_groups')
           and (self._coord.run_watchers
                == tooz.coordination.CoordinationDriverCachedRunWatchers.run_watchers)):  # noqa
            self.assertIn(self.group_id, self._coord._joined_groups)
            self._coord.run_watchers()
            self.assertNotIn(self.group_id, self._coord._joined_groups)

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

        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

    def test_run_for_election_multiple_clients(self):
        self._coord.create_group(self.group_id).get()
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        client2.watch_elected_as_leader(self.group_id, self._set_event)
        client2.run_watchers()

        self.assertEqual(1, len(self.events))
        event = self.events[0]
        self.assertIsInstance(event, tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id, event.member_id)
        self.assertEqual(self.group_id, event.group_id)
        self.assertEqual(self._coord.get_leader(self.group_id).get(),
                         self.member_id)

        self.events = []

        self._coord.stop()
        client2.run_watchers()

        self.assertEqual(1, len(self.events))
        event = self.events[0]

        self.assertIsInstance(event, tooz.coordination.LeaderElected)
        self.assertEqual(member_id_test2,
                         event.member_id)
        self.assertEqual(self.group_id, event.group_id)
        self.assertEqual(client2.get_leader(self.group_id).get(),
                         member_id_test2)

        # Restart the coord because tearDown stops it
        self._coord.start()

    def test_get_leader(self):
        self._coord.create_group(self.group_id).get()

        leader = self._coord.get_leader(self.group_id).get()
        self.assertIsNone(leader)

        self._coord.join_group(self.group_id).get()

        leader = self._coord.get_leader(self.group_id).get()
        self.assertIsNone(leader)

        # Let's get elected
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        leader = self._coord.get_leader(self.group_id).get()
        self.assertEqual(leader, self.member_id)

    def test_run_for_election_multiple_clients_stand_down(self):
        self._coord.create_group(self.group_id).get()
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)
        self._coord.run_watchers()

        member_id_test2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id_test2)
        client2.start()
        client2.watch_elected_as_leader(self.group_id, self._set_event)
        client2.run_watchers()

        self.assertEqual(1, len(self.events))
        event = self.events[0]

        self.assertIsInstance(event, tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

        self.events = []

        self._coord.stand_down_group_leader(self.group_id)
        client2.run_watchers()

        self.assertEqual(1, len(self.events))
        event = self.events[0]

        self.assertIsInstance(event, tooz.coordination.LeaderElected)
        self.assertEqual(member_id_test2, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

        self.events = []

        client2.stand_down_group_leader(self.group_id)
        self._coord.run_watchers()

        self.assertEqual(1, len(self.events))
        event = self.events[0]

        self.assertIsInstance(event, tooz.coordination.LeaderElected)
        self.assertEqual(self.member_id, event.member_id)
        self.assertEqual(self.group_id, event.group_id)

    def test_unwatch_elected_as_leader(self):
        # Create a group and add a elected_as_leader callback
        self._coord.create_group(self.group_id).get()
        self._coord.watch_elected_as_leader(self.group_id, self._set_event)

        # Ensure exactly one leader election hook exists
        self.assertEqual(1,
                         len(self._coord._hooks_elected_leader[self.group_id]))

        # Unwatch, and ensure no leader election hooks exist
        self._coord.unwatch_elected_as_leader(self.group_id, self._set_event)
        self.assertEqual(0, len(self._coord._hooks_elected_leader))

    def test_unwatch_elected_as_leader_callback_not_found(self):
        self._coord.create_group(self.group_id).get()
        self.assertRaises(tooz.coordination.WatchCallbackNotFound,
                          self._coord.unwatch_elected_as_leader,
                          self.group_id, lambda x: None)

    def test_unwatch_join_group_callback_not_found(self):
        self._coord.create_group(self.group_id).get()
        self.assertRaises(tooz.coordination.WatchCallbackNotFound,
                          self._coord.unwatch_join_group,
                          self.group_id, lambda x: None)

    def test_unwatch_leave_group(self):
        # Create a group and add a leave_group callback
        self._coord.create_group(self.group_id).get()
        self.assertEqual(0, len(self._coord._hooks_leave_group))
        self._coord.watch_leave_group(self.group_id, self._set_event)

        # Ensure exactly one leave group hook exists
        self.assertEqual(1, len(self._coord._hooks_leave_group[self.group_id]))

        # Unwatch, and ensure no leave group hooks exist
        self._coord.unwatch_leave_group(self.group_id, self._set_event)
        self.assertEqual(0, len(self._coord._hooks_leave_group))

    def test_unwatch_leave_group_callback_not_found(self):
        self._coord.create_group(self.group_id).get()
        self.assertRaises(tooz.coordination.WatchCallbackNotFound,
                          self._coord.unwatch_leave_group,
                          self.group_id, lambda x: None)

    def test_get_lock(self):
        lock = self._coord.get_lock(tests.get_random_uuid())
        self.assertTrue(lock.acquire())
        self.assertTrue(lock.release())
        with lock:
            pass

    def test_heartbeat_lock_not_acquired(self):
        lock = self._coord.get_lock(tests.get_random_uuid())
        # Not all locks need heartbeat
        if hasattr(lock, "heartbeat"):
            self.assertFalse(lock.heartbeat())

    def test_get_shared_lock(self):
        lock = self._coord.get_lock(tests.get_random_uuid())
        self.assertTrue(lock.acquire(shared=True))
        self.assertTrue(lock.release())
        with lock(shared=True):
            pass

    def test_get_shared_lock_locking_same_lock_twice(self):
        lock = self._coord.get_lock(tests.get_random_uuid())
        self.assertTrue(lock.acquire(shared=True))
        self.assertTrue(lock.acquire(shared=True))
        self.assertTrue(lock.release())
        self.assertTrue(lock.release())
        self.assertFalse(lock.release())
        with lock(shared=True):
            pass

    def test_get_shared_lock_locking_two_lock(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        coord = tooz.coordination.get_coordinator(
            self.url, tests.get_random_uuid())
        coord.start()
        lock2 = coord.get_lock(name)

        self.assertTrue(lock1.acquire(shared=True))
        self.assertTrue(lock2.acquire(shared=True))
        self.assertTrue(lock1.release())
        self.assertTrue(lock2.release())

    def test_get_lock_locking_shared_and_exclusive(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        coord = tooz.coordination.get_coordinator(
            self.url, tests.get_random_uuid())
        coord.start()
        lock2 = coord.get_lock(name)

        self.assertTrue(lock1.acquire(shared=True))
        self.assertFalse(lock2.acquire(blocking=False))
        self.assertTrue(lock1.release())
        self.assertFalse(lock2.release())

    def test_get_lock_locking_exclusive_and_shared(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        coord = tooz.coordination.get_coordinator(
            self.url, tests.get_random_uuid())
        coord.start()
        lock2 = coord.get_lock(name)

        self.assertTrue(lock1.acquire())
        self.assertFalse(lock2.acquire(shared=True, blocking=False))
        self.assertTrue(lock1.release())
        self.assertFalse(lock2.release())

    def test_get_lock_concurrency_locking_same_lock(self):
        lock = self._coord.get_lock(tests.get_random_uuid())

        graceful_ending = threading.Event()

        def thread():
            self.assertTrue(lock.acquire())
            self.assertTrue(lock.release())
            graceful_ending.set()

        t = threading.Thread(target=thread)
        t.daemon = True
        with lock:
            t.start()
            # Ensure the thread try to get the lock
            time.sleep(.1)
        t.join()
        graceful_ending.wait(.2)
        self.assertTrue(graceful_ending.is_set())

    def _do_test_get_lock_concurrency_locking_two_lock(self, executor,
                                                       use_same_coord):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        with lock1:
            with executor(max_workers=1) as e:
                coord = self._coord if use_same_coord else None
                f = e.submit(try_to_lock_job, name, coord, self.url,
                             tests.get_random_uuid())
                self.assertFalse(f.result())

    def _do_test_get_lock_serial_locking_two_lock(self, executor,
                                                  use_same_coord):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock1.acquire()
        lock1.release()
        with executor(max_workers=1) as e:
            coord = self._coord if use_same_coord else None
            f = e.submit(try_to_lock_job, name, coord, self.url,
                         tests.get_random_uuid())
            self.assertTrue(f.result())

    def test_get_lock_concurrency_locking_two_lock_process(self):
        self._do_test_get_lock_concurrency_locking_two_lock(
            futures.ProcessPoolExecutor, False)

    def test_get_lock_serial_locking_two_lock_process(self):
        self._do_test_get_lock_serial_locking_two_lock(
            futures.ProcessPoolExecutor, False)

    def test_get_lock_concurrency_locking_two_lock_thread1(self):
        self._do_test_get_lock_concurrency_locking_two_lock(
            futures.ThreadPoolExecutor, False)

    def test_get_lock_concurrency_locking_two_lock_thread2(self):
        self._do_test_get_lock_concurrency_locking_two_lock(
            futures.ThreadPoolExecutor, True)

    def test_get_lock_concurrency_locking2(self):
        # NOTE(sileht): some database based lock can have only
        # one lock per connection, this test ensures acquiring a
        # second lock doesn't release the first one.
        lock1 = self._coord.get_lock(tests.get_random_uuid())
        lock2 = self._coord.get_lock(tests.get_random_uuid())

        graceful_ending = threading.Event()
        thread_locked = threading.Event()

        def thread():
            with lock2:
                try:
                    self.assertFalse(lock1.acquire(blocking=False))
                except tooz.NotImplemented:
                    pass
                thread_locked.set()
            graceful_ending.set()

        t = threading.Thread(target=thread)
        t.daemon = True

        with lock1:
            t.start()
            thread_locked.wait()
            self.assertTrue(thread_locked.is_set())
        t.join()
        graceful_ending.wait()
        self.assertTrue(graceful_ending.is_set())

    def test_get_lock_twice_locked_twice(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock2 = self._coord.get_lock(name)
        with lock1:
            self.assertFalse(lock2.acquire(blocking=False))

    def test_get_lock_context_fails(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock2 = self._coord.get_lock(name)
        with mock.patch.object(lock2, 'acquire', return_value=False):
            with lock1:
                self.assertRaises(
                    tooz.coordination.LockAcquireFailed,
                    lock2.__enter__)

    def test_get_lock_context_check_value(self):
        name = tests.get_random_uuid()
        lock = self._coord.get_lock(name)
        with lock as returned_lock:
            self.assertEqual(lock, returned_lock)

    def test_lock_context_manager_acquire_no_argument(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock2 = self._coord.get_lock(name)
        with lock1():
            self.assertFalse(lock2.acquire(blocking=False))

    def test_lock_context_manager_acquire_argument_return_value(self):
        name = tests.get_random_uuid()
        blocking_value = 10.12
        lock = self._coord.get_lock(name)
        with lock(blocking_value) as returned_lock:
            self.assertEqual(lock, returned_lock)

    def test_lock_context_manager_acquire_argument_release_within(self):
        name = tests.get_random_uuid()
        blocking_value = 10.12
        lock = self._coord.get_lock(name)
        with lock(blocking_value) as returned_lock:
            self.assertTrue(returned_lock.release())

    def test_lock_context_manager_acquire_argument(self):
        name = tests.get_random_uuid()
        blocking_value = 10.12
        lock = self._coord.get_lock(name)
        with mock.patch.object(lock, 'acquire', wraps=True, autospec=True) as \
                mock_acquire:
            with lock(blocking_value):
                mock_acquire.assert_called_once_with(blocking_value)

    def test_lock_context_manager_acquire_argument_timeout(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock2 = self._coord.get_lock(name)
        with lock1:
            try:
                with lock2(False):
                    self.fail('Lock acquire should have failed')
            except tooz.coordination.LockAcquireFailed:
                pass

    def test_get_lock_locked_twice(self):
        name = tests.get_random_uuid()
        lock = self._coord.get_lock(name)
        with lock:
            self.assertFalse(lock.acquire(blocking=False))

    def test_get_multiple_locks_with_same_coord(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        lock2 = self._coord.get_lock(name)
        self.assertTrue(lock1.acquire())
        self.assertFalse(lock2.acquire(blocking=False))
        self.assertFalse(self._coord.get_lock(name).acquire(blocking=False))
        self.assertTrue(lock1.release())

    def test_ensure_acquire_release_return(self):
        name = tests.get_random_uuid()
        lock1 = self._coord.get_lock(name)
        self.assertTrue(lock1.acquire())
        self.assertTrue(lock1.release())
        self.assertFalse(lock1.release())

    def test_get_lock_multiple_coords(self):
        member_id2 = tests.get_random_uuid()
        client2 = tooz.coordination.get_coordinator(self.url,
                                                    member_id2)
        client2.start()

        lock_name = tests.get_random_uuid()
        lock = self._coord.get_lock(lock_name)
        self.assertTrue(lock.acquire())

        lock2 = client2.get_lock(lock_name)
        self.assertFalse(lock2.acquire(blocking=False))
        self.assertTrue(lock.release())
        self.assertTrue(lock2.acquire(blocking=True))
        self.assertTrue(lock2.release())

    def test_get_started_status(self):
        self.assertTrue(self._coord.is_started)
        self._coord.stop()
        self.assertFalse(self._coord.is_started)
        self._coord.start()

    def do_test_name_property(self):
        name = tests.get_random_uuid()
        lock = self._coord.get_lock(name)
        self.assertEqual(name, lock.name)

    def test_acquire_twice_no_deadlock_releasing(self):
        name = tests.get_random_uuid()
        lock = self._coord.get_lock(name)
        self.assertTrue(lock.acquire(blocking=False))
        self.assertFalse(lock.acquire(blocking=False))
        self.assertTrue(lock.release())


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
