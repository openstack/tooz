# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

import uuid

import fixtures
from testtools import testcase

from tooz import coordination


class TestFileClosing(testcase.TestCase):
    @staticmethod
    def _get_random_uuid():
        return str(uuid.uuid4()).encode('ascii')

    def setUp(self):
        super(TestFileClosing, self).setUp()
        self.tempd = self.useFixture(fixtures.TempDir())
        self.url = 'file:///%s' % self.tempd.path
        self.member_id = self._get_random_uuid()

    def test_lock_closing(self):
        driver = coordination.get_coordinator(self.url, self.member_id)
        lk = driver.get_lock(b'test').lock
        self.assertFalse(lk.lockfile.closed)
        lk.close()
        self.assertTrue(lk.lockfile.closed)
