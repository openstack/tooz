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

from concurrent import futures
import threading
from unittest import mock

import sysv_ipc
from testtools import testcase

from tooz import coordination
from tooz.drivers import ipc
from tooz import tests


class TestIPCDriver(testcase.TestCase):
    def setUp(self):
        super().setUp()
        self.group_list_key = tests.get_random_uuid().decode("ascii")
        self.lock_name = tests.get_random_uuid()

    def _coordinator(self):
        coordinator = coordination.get_coordinator(
            "ipc://", tests.get_random_uuid()
        )
        assert isinstance(coordinator, ipc.IPCDriver)
        coordinator._GROUP_LIST_KEY = self.group_list_key
        coordinator._INTERNAL_LOCK_NAME = self.lock_name
        self.addCleanup(coordinator._executor.stop)
        self.addCleanup(self._stop, coordinator)
        return coordinator

    @staticmethod
    def _stop(coordinator):
        if coordinator.is_started:
            coordinator.stop()

    def test_stop_preserves_group_list_for_other_coordinators(self):
        first = self._coordinator()
        survivor = self._coordinator()
        newcomer = self._coordinator()
        first.start()
        survivor.start()
        first.create_group(b"group").get()

        first.stop()
        newcomer.start()

        self.assertEqual([b"group"], survivor.get_groups().get())
        self.assertEqual([b"group"], newcomer.get_groups().get())
        self.assertEqual(survivor._group_list.id, newcomer._group_list.id)

    def test_last_stop_removes_group_list(self):
        first = self._coordinator()
        last = self._coordinator()
        first.start()
        last.start()
        key = last._group_list.key

        first.stop()
        self.assertEqual(1, last._group_list.number_attached)
        last.stop()

        self.assertRaises(
            sysv_ipc.ExistentialError, sysv_ipc.SharedMemory, key
        )
        last.start()
        self.assertEqual([], last.get_groups().get())

    def test_start_during_stop(self):
        first = self._coordinator()
        newcomer = self._coordinator()
        first.start()
        first.create_group(b"group").get()
        looked_up = threading.Event()
        attach = threading.Event()
        stopping = threading.Event()
        memory = sysv_ipc.SharedMemory
        acquire = first._lock.acquire

        def delayed_attach(*args, **kwargs):
            # Split the constructor's lookup and attachment to force the
            # window in which another coordinator could remove the segment.
            segment = memory(*args, **kwargs)
            segment_id = segment.id
            segment.detach()
            looked_up.set()
            if not attach.wait(5):
                raise TimeoutError("Shared memory attachment was not resumed")
            return sysv_ipc.attach(segment_id)

        def acquire_to_stop(*args, **kwargs):
            stopping.set()
            return acquire(*args, **kwargs)

        def stop():
            try:
                first.stop()
            finally:
                # Also signal when testing an implementation that stops
                # without acquiring the lifecycle lock.
                stopping.set()

        with (
            mock.patch.object(
                sysv_ipc, "SharedMemory", side_effect=delayed_attach
            ),
            mock.patch.object(
                first._lock, "acquire", side_effect=acquire_to_stop
            ),
            futures.ThreadPoolExecutor(max_workers=2) as executor,
        ):
            starting = executor.submit(newcomer.start)
            try:
                self.assertTrue(looked_up.wait(5))
                stopped = executor.submit(stop)
                self.assertTrue(stopping.wait(5))
                attach.set()
                starting.result(timeout=5)
                stopped.result(timeout=5)
            finally:
                attach.set()

        self.assertEqual([b"group"], newcomer.get_groups().get())

    def test_start_failure_releases_lock(self):
        coordinator = self._coordinator()
        failure = sysv_ipc.PermissionsError("access denied")
        with mock.patch.object(sysv_ipc, "SharedMemory", side_effect=failure):
            error = self.assertRaises(
                sysv_ipc.PermissionsError, coordinator.start
            )

        self.assertIs(failure, error)
        self.assertFalse(coordinator.is_started)
        key = ipc.ftok(self.lock_name, ipc.IPCLock._LOCK_PROJECT)
        self.assertRaises(sysv_ipc.ExistentialError, sysv_ipc.Semaphore, key)
