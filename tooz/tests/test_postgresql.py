# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

try:
    # Added in python 3.3+
    from unittest import mock
except ImportError:
    import mock

from oslo_utils import encodeutils
import testtools
from testtools import testcase

import tooz
from tooz import coordination
from tooz import tests

# Handle the case gracefully where the driver is not installed.
try:
    import psycopg2
    PGSQL_AVAILABLE = True
except ImportError:
    PGSQL_AVAILABLE = False


@testtools.skipUnless(PGSQL_AVAILABLE, 'psycopg2 is not available')
class TestPostgreSQLFailures(testcase.TestCase):

    # Not actually used (but required none the less), since we mock out
    # the connect() method...
    FAKE_URL = "postgresql://localhost:1"

    def _create_coordinator(self):

        def _safe_stop(coord):
            try:
                coord.stop()
            except tooz.ToozError as e:
                # TODO(harlowja): make this better, so that we don't have to
                # do string checking...
                message = encodeutils.exception_to_unicode(e)
                if (message != 'Can not stop a driver which has not'
                               ' been started'):
                    raise

        coord = coordination.get_coordinator(self.FAKE_URL,
                                             tests.get_random_uuid())
        self.addCleanup(_safe_stop, coord)
        return coord

    @mock.patch("tooz.drivers.pgsql.psycopg2.connect")
    def test_connect_failure(self, psycopg2_connector):
        psycopg2_connector.side_effect = psycopg2.Error("Broken")
        c = self._create_coordinator()
        self.assertRaises(coordination.ToozConnectionError, c.start)

    @mock.patch("tooz.drivers.pgsql.psycopg2.connect")
    def test_connect_failure_operational(self, psycopg2_connector):
        psycopg2_connector.side_effect = psycopg2.OperationalError("Broken")
        c = self._create_coordinator()
        self.assertRaises(coordination.ToozConnectionError, c.start)

    @mock.patch("tooz.drivers.pgsql.psycopg2.connect")
    def test_failure_acquire_lock(self, psycopg2_connector):
        execute_mock = mock.MagicMock()
        execute_mock.execute.side_effect = psycopg2.OperationalError("Broken")

        cursor_mock = mock.MagicMock()
        cursor_mock.__enter__ = mock.MagicMock(return_value=execute_mock)
        cursor_mock.__exit__ = mock.MagicMock(return_value=False)

        conn_mock = mock.MagicMock()
        conn_mock.cursor.return_value = cursor_mock
        psycopg2_connector.return_value = conn_mock

        c = self._create_coordinator()
        c.start()
        test_lock = c.get_lock(b'test-lock')
        self.assertRaises(tooz.ToozError, test_lock.acquire)

    @mock.patch("tooz.drivers.pgsql.psycopg2.connect")
    def test_failure_release_lock(self, psycopg2_connector):
        execute_mock = mock.MagicMock()
        execute_mock.execute.side_effect = [
            True,
            psycopg2.OperationalError("Broken"),
        ]

        cursor_mock = mock.MagicMock()
        cursor_mock.__enter__ = mock.MagicMock(return_value=execute_mock)
        cursor_mock.__exit__ = mock.MagicMock(return_value=False)

        conn_mock = mock.MagicMock()
        conn_mock.cursor.return_value = cursor_mock
        psycopg2_connector.return_value = conn_mock

        c = self._create_coordinator()
        c.start()
        test_lock = c.get_lock(b'test-lock')
        self.assertTrue(test_lock.acquire())
        self.assertRaises(tooz.ToozError, test_lock.release)
