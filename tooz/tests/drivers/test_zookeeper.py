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

from kazoo import exceptions
from kazoo.handlers import threading as threading_handler
from testtools import testcase

from tooz import coordination
from tooz import tests
from tooz import utils


class TestKazooDriver(testcase.TestCase):
    FAKE_URL = "kazoo://mocked-not-really-localhost"

    @mock.patch('kazoo.client.KazooClient')
    def test_get_members_with_capabilities_sends_reads_at_once(
        self, mock_client_cls
    ):
        mock_client = mock_client_cls.return_value
        mock_client.handler.timeout_exception = (
            threading_handler.KazooTimeoutError
        )
        coord = coordination.get_coordinator(
            self.FAKE_URL, tests.get_random_uuid()
        )
        coord.start()
        children = mock_client.get_children_async.return_value
        children.get.return_value = ['alive', 'gone']
        events = []

        def get_async(path):
            events.append(('send', path))

            def get():
                events.append(('wait', path))
                if path.endswith('/gone'):
                    raise exceptions.NoNodeError()
                return utils.dumps({'path': path}), None

            return mock.Mock(get=mock.Mock(side_effect=get))

        mock_client.get_async.side_effect = get_async
        # As kazoo's get() does.
        mock_client.get.side_effect = lambda path: get_async(path).get()
        members = coord.get_members_with_capabilities(b'group').get()
        self.assertEqual({b'alive': {'path': '/tooz/group/alive'}}, members)
        # Every read was sent before the first answer was awaited.
        self.assertEqual(
            [
                ('send', '/tooz/group/alive'),
                ('send', '/tooz/group/gone'),
                ('wait', '/tooz/group/alive'),
                ('wait', '/tooz/group/gone'),
            ],
            events,
        )
