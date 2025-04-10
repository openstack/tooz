# Copyright (c) 2015 OpenStack Foundation
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
from unittest import mock

from testtools import testcase

from tooz import coordination
from tooz import tests


class TestSherlockDriver(testcase.TestCase):

    def _create_coordinator(self, url="kubernetes://?namespace=fake_name"):
        return coordination.get_coordinator(
            url, tests.get_random_uuid())

    def test_connect_k8s_driver(self):
        c = self._create_coordinator()
        self.assertIsNone(c.start())

    @mock.patch("sherlock.KubernetesLock")
    def test_parsing_timeout_settings(self, k8s_mock):
        c = self._create_coordinator()

        name = tests.get_random_uuid()
        blocking_value = False
        timeout = 10.1
        lock = c.get_lock(name)
        with mock.patch.object(
            lock, 'acquire', wraps=True, autospec=True,
            return_value=mock.Mock()
        ) as mock_acquire:
            with lock(blocking_value, timeout):
                mock_acquire.assert_called_once_with(blocking_value, timeout)
        k8s_mock.assert_called_once_with(
            lock_name=mock.ANY, k8s_namespace='fake_name')

    @mock.patch("sherlock.KubernetesLock")
    def test_parsing_blocking_settings(self, k8s_mock):
        c = self._create_coordinator()

        name = tests.get_random_uuid()
        blocking_value = True
        lock = c.get_lock(name)
        with mock.patch.object(
            lock, 'acquire', wraps=True, autospec=True,
            return_value=mock.Mock()
        ) as mock_acquire:
            with lock(blocking_value):
                mock_acquire.assert_called_once_with(blocking_value)
        k8s_mock.assert_called_once_with(
            lock_name=mock.ANY, k8s_namespace='fake_name')

    @mock.patch("sherlock.KubernetesLock")
    @mock.patch("sherlock.configure")
    def test_parsing_expire_settings(self, conf_mock, k8s_mock):
        c = self._create_coordinator()

        name = tests.get_random_uuid()
        blocking_value = 20
        expire_value = 10
        lock = c.get_lock(name)
        lock.acquire(blocking=blocking_value, expire=expire_value)
        k8s_mock.assert_called_once_with(
            lock_name=mock.ANY, k8s_namespace='fake_name')
        conf_mock.assert_called_once_with(
            expire=expire_value,
            timeout=blocking_value)
