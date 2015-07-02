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

import socket
import uuid

try:
    from unittest import mock
except ImportError:
    import mock

from testtools import testcase

from tooz import coordination


class TestMemcacheDriverFailures(testcase.TestCase):
    FAKE_URL = "memcached://mocked-not-really-localhost"

    @mock.patch('pymemcache.client.PooledClient')
    def test_client_failure_start(self, mock_client_cls):
        mock_client_cls.side_effect = socket.timeout('timed-out')
        member_id = str(uuid.uuid4()).encode('ascii')
        coord = coordination.get_coordinator(self.FAKE_URL, member_id)
        self.assertRaises(coordination.ToozConnectionError, coord.start)

    @mock.patch('pymemcache.client.PooledClient')
    def test_client_failure_join(self, mock_client_cls):
        mock_client = mock.MagicMock()
        mock_client_cls.return_value = mock_client
        member_id = str(uuid.uuid4()).encode('ascii')
        coord = coordination.get_coordinator(self.FAKE_URL, member_id)
        coord.start()
        mock_client.gets.side_effect = socket.timeout('timed-out')
        fut = coord.join_group(str(uuid.uuid4()).encode('ascii'))
        self.assertRaises(coordination.ToozConnectionError, fut.get)

    @mock.patch('pymemcache.client.PooledClient')
    def test_client_failure_leave(self, mock_client_cls):
        mock_client = mock.MagicMock()
        mock_client_cls.return_value = mock_client
        member_id = str(uuid.uuid4()).encode('ascii')
        coord = coordination.get_coordinator(self.FAKE_URL, member_id)
        coord.start()
        mock_client.gets.side_effect = socket.timeout('timed-out')
        fut = coord.leave_group(str(uuid.uuid4()).encode('ascii'))
        self.assertRaises(coordination.ToozConnectionError, fut.get)

    @mock.patch('pymemcache.client.PooledClient')
    def test_client_failure_heartbeat(self, mock_client_cls):
        mock_client = mock.MagicMock()
        mock_client_cls.return_value = mock_client
        member_id = str(uuid.uuid4()).encode('ascii')
        coord = coordination.get_coordinator(self.FAKE_URL, member_id)
        coord.start()
        mock_client.set.side_effect = socket.timeout('timed-out')
        self.assertRaises(coordination.ToozConnectionError, coord.heartbeat)

    @mock.patch('tooz.coordination._RunWatchersMixin.run_watchers',
                autospec=True)
    @mock.patch('pymemcache.client.PooledClient')
    def test_client_run_watchers_mixin(self, mock_client_cls,
                                       mock_run_watchers):
        mock_client = mock.MagicMock()
        mock_client_cls.return_value = mock_client
        member_id = str(uuid.uuid4()).encode('ascii')
        coord = coordination.get_coordinator(self.FAKE_URL, member_id)
        coord.start()
        coord.run_watchers()
        self.assertTrue(mock_run_watchers.called)
