# -*- coding: utf-8 -*-

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

import os
import uuid

from oslo_utils import encodeutils
import testtools
from testtools import testcase

from tooz import coordination


@testtools.skipUnless(os.getenv("TOOZ_TEST_MYSQL_URL"),
                      "TOOZ_TEST_MYSQL_URL env variable is not set")
class TestMYSQLDriver(testcase.TestCase):

    MYSQL_URL = os.getenv("TOOZ_TEST_MYSQL_URL")

    def _create_coordinator(self, url):

        def _safe_stop(coord):
            try:
                coord.stop()
            except coordination.ToozError as e:
                message = encodeutils.exception_to_unicode(e)
                if (message != 'Can not stop a driver which has not'
                               ' been started'):
                    raise

        coord = coordination.get_coordinator(url,
                                             str(uuid.uuid4()).encode('ascii'))
        self.addCleanup(_safe_stop, coord)
        return coord

    def test_connect_failure_invalid_hostname_provided(self):
        url = self.MYSQL_URL.replace('localhost', 'invalidhost')
        c = self._create_coordinator(url)
        self.assertRaises(coordination.ToozConnectionError, c.start)

    def test_connect_failure_invalid_port_provided(self):
        url = self.MYSQL_URL.replace('localhost:13', 'localhost:54')
        c = self._create_coordinator(url)
        self.assertRaises(coordination.ToozConnectionError, c.start)

    def test_connect_failure_invalid_hostname_and_port_provided(self):
        url = self.MYSQL_URL.replace('localhost:13', 'invalidhost:54')
        c = self._create_coordinator(url)
        self.assertRaises(coordination.ToozConnectionError, c.start)

    def test_connect_failure_invalid_db_name_provided(self):
        url = self.MYSQL_URL.replace('test', 'invalid')
        c = self._create_coordinator(url)
        self.assertRaises(coordination.ToozConnectionError, c.start)

    def test_connect_success(self):
        c = self._create_coordinator(self.MYSQL_URL)
        c.start()
