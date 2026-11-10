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
from unittest import mock

from oslo_utils import timeutils
from testtools import testcase

from tooz import coordination


class TestHeart(testcase.TestCase):
    def setUp(self):
        super().setUp()
        self.driver = mock.Mock()
        self.driver.heartbeat.return_value = 60
        self.heart = coordination.Heart(self.driver)
        self.addCleanup(self._stop)

    def _stop(self):
        self.driver.heartbeat.side_effect = None
        self.heart.stop()
        if self.heart.is_alive():
            self.heart.wait(3)
        self.assertFalse(self.heart.is_alive())

    def test_stop_during_failed_attempt_does_not_retry(self):
        entered = threading.Event()
        resume = threading.Event()

        def heartbeat():
            entered.set()
            resume.wait(3)
            raise coordination.ToozConnectionError("offline")

        self.driver.heartbeat.side_effect = heartbeat
        self.heart.start()
        try:
            self.assertTrue(entered.wait(2))
            self.heart.stop()
        finally:
            resume.set()
        self.assertFalse(self.heart.wait(0.5))
        self.assertEqual(1, self.driver.heartbeat.call_count)
        self.assertEqual(0, self.heart.beats)

    def test_stop_during_successful_attempt_does_not_delay(self):
        entered = threading.Event()
        resume = threading.Event()

        def heartbeat():
            entered.set()
            resume.wait()
            return 30

        self.driver.heartbeat.side_effect = heartbeat
        self.heart.start()
        try:
            self.assertTrue(entered.wait(2))
            self.heart.stop()
        finally:
            resume.set()
        self.assertFalse(self.heart.wait(1))
        self.assertEqual(1, self.driver.heartbeat.call_count)
        self.assertEqual(0, self.heart.beats)

    def test_stop_interrupts_retry_delay(self):
        waiting = threading.Event()
        original_wait = self.heart._dead.wait

        def wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        self.driver.heartbeat.side_effect = coordination.ToozConnectionError(
            "offline"
        )
        with mock.patch.object(self.heart._dead, "wait", side_effect=wait):
            self.heart.start()
            self.assertTrue(waiting.wait(2))
            self.heart.stop()
            self.assertFalse(self.heart.wait(0.5))
        self.assertEqual(1, self.driver.heartbeat.call_count)

    def test_failure_is_retried_until_recovery(self):
        recovered = threading.Event()
        original_wait = self.heart._dead.wait

        def wait(timeout):
            if self.heart.beats:
                recovered.set()
            return original_wait(timeout)

        def heartbeat():
            if self.driver.heartbeat.call_count == 1:
                raise coordination.ToozConnectionError("offline")
            return 60

        self.driver.heartbeat.side_effect = heartbeat
        with mock.patch.object(self.heart._dead, "wait", side_effect=wait):
            self.heart.start()
            self.assertTrue(recovered.wait(3))
            self.heart.stop()
            self.assertFalse(self.heart.wait(1))
        self.assertEqual(1, self.heart.beats)
        self.assertEqual(2, self.driver.heartbeat.call_count)

    def _run_timed_heartbeats(self, interval):
        now = 0.0
        delays = []

        def heartbeat():
            nonlocal now
            now += 0.25
            if self.driver.heartbeat.call_count <= 2:
                raise coordination.ToozConnectionError("offline")
            return interval

        def wait(timeout):
            nonlocal now
            delays.append(timeout)
            now += max(0, timeout)
            if self.driver.heartbeat.call_count == 4:
                self.heart.stop()

        self.driver.heartbeat.side_effect = heartbeat
        with (
            mock.patch.object(timeutils, "now", lambda: now),
            mock.patch.object(self.heart._dead, "wait", side_effect=wait),
            mock.patch.object(coordination.LOG, "warning") as warning,
        ):
            self.heart._beat_forever_until_stopped()
        self.assertEqual(2, self.heart.beats)
        return delays, warning.call_args_list

    def test_retry_time_reduces_next_delay(self):
        delays, warnings = self._run_timed_heartbeats(interval=4)
        # Three 0.25s attempts plus two 1s retry delays take 2.75s.
        # The next successful heartbeat starts a fresh timing interval.
        self.assertEqual(
            [1, 1, 0.625, 1.875],
            delays,
        )
        self.assertEqual([], warnings)

    def test_retry_time_triggers_slow_heartbeat_warning(self):
        delays, warnings = self._run_timed_heartbeats(interval=2)
        self.assertEqual(
            [1, 1, -0.375, 0.875],
            delays,
        )
        self.assertEqual(1, len(warnings))
        self.assertIn("Heartbeating took too long", warnings[0].args[0])
        self.assertEqual((2.75, 0.75), warnings[0].args[1:])

    def test_stopped_heart_can_restart(self):
        for _ in range(2):
            beat = threading.Event()

            original_wait = self.heart._dead.wait

            def wait(timeout):
                beat.set()
                return original_wait(timeout)

            with mock.patch.object(self.heart._dead, "wait", side_effect=wait):
                self.heart.start()
                self.assertTrue(beat.wait(2))
                self.heart.stop()
                self.assertFalse(self.heart.wait(1))
            self.assertEqual(1, self.heart.beats)
