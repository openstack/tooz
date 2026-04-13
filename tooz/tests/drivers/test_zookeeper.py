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

import fixtures
from kazoo import exceptions
from kazoo.handlers import threading as threading_handler
from kazoo import retry as kazoo_retry
from oslo_utils import netutils
from testtools import testcase

from tooz import coordination
from tooz.drivers import zookeeper
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


class TestZookeeperRetryParam(testcase.TestCase):
    def setUp(self):
        super().setUp()
        self.useFixture(
            fixtures.MockPatch('tooz.drivers.zookeeper.client.KazooClient')
        )
        self.driver = zookeeper.KazooDriver(
            tests.get_random_uuid(),
            netutils.urlsplit('zookeeper://localhost:2181'),
            {},
        )

    def test_validate_retry_param_kazoo_retry(self):
        retry = kazoo_retry.KazooRetry(max_tries=3)
        self.assertIs(retry, self.driver._validate_retry_param(retry))

    def test_validate_retry_param_dict(self):
        result = self.driver._validate_retry_param(
            {'max_tries': 5, 'delay': 0.5}
        )
        self.assertIsInstance(result, kazoo_retry.KazooRetry)
        self.assertEqual(5, result.max_tries)
        self.assertEqual(0.5, result.delay)

    def test_validate_retry_param_json_string(self):
        result = self.driver._validate_retry_param(
            '{"max_tries": 7, "backoff": 3}'
        )
        self.assertIsInstance(result, kazoo_retry.KazooRetry)
        self.assertEqual(7, result.max_tries)
        self.assertEqual(3, result.backoff)

    def test_validate_retry_param_json_not_dict(self):
        for value in ['[1, 2, 3]', '"text"', '10']:
            with self.subTest(f'invalid value: {value}'):
                self.assertRaises(
                    ValueError, self.driver._validate_retry_param, value
                )

    def test_validate_retry_param_invalid_json_string(self):
        self.assertRaises(
            ValueError, self.driver._validate_retry_param, 'not-json'
        )

    def test_validate_retry_param_unsupported_type(self):
        for value in [10, 1.5, ['max_tries'], object()]:
            with self.subTest(f'invalid value: {value}'):
                self.assertRaises(
                    TypeError, self.driver._validate_retry_param, value
                )

    def test_validate_retry_param_unknown_key(self):
        self.assertRaises(
            TypeError,
            self.driver._validate_retry_param,
            {'unknown_option': 1},
        )

    def test_make_client_validates_retry_params(self):
        mock_validate = self.useFixture(
            fixtures.MockPatchObject(self.driver, '_validate_retry_param')
        ).mock
        self.driver._make_client(
            netutils.urlsplit('zookeeper://localhost:2181'),
            {
                'connection_retry': '{"max_tries": 3}',
                'command_retry': {'max_tries': 4},
            },
        )
        mock_validate.assert_has_calls(
            [
                mock.call('{"max_tries": 3}'),
                mock.call({'max_tries': 4}),
            ]
        )
        self.assertEqual(2, mock_validate.call_count)
