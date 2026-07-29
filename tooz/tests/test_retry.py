# Copyright (c) 2026 Red Hat, Inc.
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

import datetime

import tenacity
from testtools import testcase

from tooz import _retry


class TestRetry(testcase.TestCase):
    def _stop_of(self, **kwargs):
        @_retry.retry(**kwargs)
        def f():
            pass

        return f.retry.stop  # type: ignore[attr-defined]

    def test_no_stop_max_delay(self):
        self.assertEqual(tenacity.stop_never, self._stop_of())

    def test_stop_max_delay_none(self):
        self.assertEqual(
            tenacity.stop_never, self._stop_of(stop_max_delay=None)
        )

    def test_stop_max_delay_true_is_not_a_delay(self):
        # True/False are sentinels, not a 1-second delay (bool is a
        # subclass of int).
        self.assertEqual(
            tenacity.stop_never, self._stop_of(stop_max_delay=True)
        )

    def test_stop_max_delay_false_is_not_a_delay(self):
        self.assertEqual(
            tenacity.stop_never, self._stop_of(stop_max_delay=False)
        )

    def test_stop_max_delay_numeric(self):
        stop = self._stop_of(stop_max_delay=5)
        self.assertIsInstance(stop, tenacity.stop_after_delay)
        self.assertEqual(5, stop.max_delay)

    def test_stop_max_delay_timedelta(self):
        delta = datetime.timedelta(seconds=5)
        stop = self._stop_of(stop_max_delay=delta)
        self.assertIsInstance(stop, tenacity.stop_after_delay)
        self.assertEqual(delta.total_seconds(), stop.max_delay)
