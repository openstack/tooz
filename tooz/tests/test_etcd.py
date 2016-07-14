# -*- coding: utf-8 -*-
#
# Copyright 2016 Red Hat, Inc.
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
from testtools import testcase

import tooz.coordination


class TestEtcd(testcase.TestCase):
    FAKE_URL = "etcd://mocked-not-really-localhost:2379"
    FAKE_MEMBER_ID = "mocked-not-really-member"

    def setUp(self):
        super(TestEtcd, self).setUp()
        self._coord = tooz.coordination.get_coordinator(self.FAKE_URL,
                                                        self.FAKE_MEMBER_ID)

    def test_multiple_locks_etcd_wait_index(self):
        lock = self._coord.get_lock('mocked-not-really-random')

        return_values = [
            {'errorCode': {}, 'node': {}, 'index': 10},
            {'errorCode': None, 'node': {}, 'index': 10}
        ]
        with mock.patch.object(lock.client, 'put', side_effect=return_values):
            with mock.patch.object(lock.client, 'get') as mocked_get:
                self.assertTrue(lock.acquire())
                mocked_get.assert_called_once()
                call = str(mocked_get.call_args)
                self.assertIn("waitIndex=11", call)
