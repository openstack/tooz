# -*- coding: utf-8 -*-
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

import mock
import testtools

try:
    from etcd3 import exceptions as etcd3_exc
    ETCD3_AVAILABLE = True
except ImportError:
    ETCD3_AVAILABLE = False

from tooz import coordination


@testtools.skipUnless(ETCD3_AVAILABLE, 'etcd3 is not available')
class TestEtcd3(testtools.TestCase):
    FAKE_URL = "etcd3://mocked-not-really-localhost:2379"
    FAKE_MEMBER_ID = "mocked-not-really-member"

    def setUp(self):
        super(TestEtcd3, self).setUp()
        self._coord = coordination.get_coordinator(self.FAKE_URL,
                                                   self.FAKE_MEMBER_ID)

    def test_error_translation(self):
        lock = self._coord.get_lock('mocked-not-really-random')

        exc = etcd3_exc.ConnectionFailedError()
        with mock.patch.object(lock._lock, 'acquire', side_effect=exc):
            self.assertRaises(coordination.ToozConnectionError, lock.acquire)

        exc = etcd3_exc.ConnectionTimeoutError()
        with mock.patch.object(lock._lock, 'acquire', side_effect=exc):
            self.assertRaises(coordination.OperationTimedOut, lock.acquire)

        exc = etcd3_exc.InternalServerError()
        with mock.patch.object(lock._lock, 'acquire', side_effect=exc):
            self.assertRaises(coordination.ToozError, lock.acquire)
