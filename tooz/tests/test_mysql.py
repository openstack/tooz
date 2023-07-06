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
from unittest import mock

from oslo_utils import encodeutils
from testtools import testcase

import tooz
from tooz import coordination
from tooz import tests


class TestMySQLDriver(testcase.TestCase):

    def _create_coordinator(self, url):

        def _safe_stop(coord):
            try:
                coord.stop()
            except tooz.ToozError as e:
                message = encodeutils.exception_to_unicode(e)
                if (message != 'Can not stop a driver which has not'
                               ' been started'):
                    raise

        coord = coordination.get_coordinator(url,
                                             tests.get_random_uuid())
        self.addCleanup(_safe_stop, coord)
        return coord

    def test_connect_failure_invalid_hostname_provided(self):
        c = self._create_coordinator("mysql://invalidhost/test")
        self.assertRaises(coordination.ToozConnectionError, c.start)

    def test_connect_failure_invalid_port_provided(self):
        c = self._create_coordinator("mysql://localhost:54/test")
        self.assertRaises(coordination.ToozConnectionError, c.start)

    def test_connect_failure_invalid_hostname_and_port_provided(self):
        c = self._create_coordinator("mysql://invalidhost:54/test")
        self.assertRaises(coordination.ToozConnectionError, c.start)

    @mock.patch("pymysql.Connect")
    def test_parsing_tls_settings(self, sql_mock):
        c = self._create_coordinator(
            "mysql://invalidhost:54/test"
            "?ssl_ca=/ca/not/there"
            "&ssl_capath=/capath/not/there"
            "&ssl_check_hostname=False"
            "&ssl_verify_mode=yes"
            "&ssl_cert=/cert/not/there"
            "&ssl_key=/key/not/there"
            "&ssl_cipher=spam,ham"
        )
        c.start()
        sql_mock.assert_called_once_with(
            host="invalidhost",
            port=54,
            user=None,
            passwd=None,
            database="test",
            defer_connect=False,
            ssl=dict(
                ca="/ca/not/there",
                capath="/capath/not/there",
                check_hostname=False,
                verify_mode="yes",
                cert="/cert/not/there",
                key="/key/not/there",
                cipher="spam,ham"
            )

        )

    @mock.patch("pymysql.Connect")
    def test_parsing_timeout_settings(self, sql_mock):
        c = self._create_coordinator("mysql://localhost:3306/test")
        c.start()

        name = tests.get_random_uuid()
        blocking_value = False
        timeout = 10.1
        lock = c.get_lock(name)
        with mock.patch.object(lock, 'acquire', wraps=True, autospec=True) as \
                mock_acquire:
            with lock(blocking_value, timeout):
                mock_acquire.assert_called_once_with(blocking_value, timeout)

    @mock.patch("pymysql.Connect")
    def test_parsing_blocking_settings(self, sql_mock):
        c = self._create_coordinator("mysql://localhost:3306/test")
        c.start()

        name = tests.get_random_uuid()
        blocking_value = True
        lock = c.get_lock(name)
        with mock.patch.object(lock, 'acquire', wraps=True, autospec=True) as \
                mock_acquire:
            with lock(blocking_value):
                mock_acquire.assert_called_once_with(blocking_value)
