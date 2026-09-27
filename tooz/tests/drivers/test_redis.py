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

from typing import Any
from unittest import mock

from redis import exceptions
from testtools import testcase

import tooz
from tooz import coordination
from tooz import tests

# Client methods that group operations use, with or without a transaction.
_CLIENT_METHODS = (
    'transaction',
    'exists',
    'hget',
    'hgetall',
    'hkeys',
    'mget',
    'smembers',
)


class TestRedis(testcase.TestCase):
    FAKE_URL = "redis://mocked-not-really-localhost"

    def _start_coordinator(self, mock_client_cls: Any) -> Any:
        client = mock_client_cls.return_value
        client.info.return_value = {"redis_version": "8.0.0"}
        coordinator = coordination.get_coordinator(
            self.FAKE_URL, tests.get_random_uuid()
        )
        coordinator.start(start_heart=False)
        return coordinator, client

    @staticmethod
    def _fail_everything(client: Any, failure: Exception) -> None:
        for name in _CLIENT_METHODS:
            getattr(client, name).side_effect = failure
        # Every registered script is this mock.
        client.register_script.return_value.side_effect = failure

    @mock.patch("tooz.drivers.redis.redis.Redis")
    def test_asynchronous_errors_are_translated(self, mock_client_cls):
        coordinator, client = self._start_coordinator(mock_client_cls)
        self.addCleanup(coordinator.stop)
        group = b'group'
        for failure, expected in (
            (
                exceptions.ConnectionError('refused'),
                coordination.ToozConnectionError,
            ),
            (
                exceptions.TimeoutError('timed out'),
                coordination.ToozConnectionError,
            ),
            (exceptions.ResponseError('bad reply'), tooz.ToozError),
        ):
            self._fail_everything(client, failure)
            for result in (
                coordinator.create_group(group),
                coordinator.join_group(group),
                coordinator.update_capabilities(group, {'a': 1}),
                coordinator.leave_group(group),
                coordinator.get_members(group),
                coordinator.get_members_with_capabilities(group),
                coordinator.get_member_capabilities(group, b'member'),
                coordinator.get_groups(),
                coordinator.delete_group(group),
            ):
                error = self.assertRaises(expected, result.get)
                self.assertIs(failure, error.__cause__)
                if expected is tooz.ToozError:
                    self.assertNotIsInstance(
                        error, coordination.ToozConnectionError
                    )

    @mock.patch("tooz.drivers.redis.redis.Redis")
    def test_stop_closes_the_client(self, mock_client_cls):
        coordinator, client = self._start_coordinator(mock_client_cls)

        coordinator.stop()

        self.assertFalse(coordinator.is_started)
        client.delete.assert_called_once()
        client.close.assert_called_once_with()

    def _assert_stop_releases_resources(self, client, coordinator, failure):
        coordinator._joined_groups.add(b'group')
        self._fail_everything(client, failure)
        client.delete.side_effect = failure

        with self.assertLogs("tooz.drivers.redis", "WARNING"):
            coordinator.stop()

        self.assertFalse(coordinator.is_started)
        self.assertIsNone(coordinator._executor.executor)
        self.assertIsNone(coordinator._client)
        # The stop is not retried, and the pooled connections are released.
        client.delete.assert_called_once()
        client.close.assert_called_once_with()

    @mock.patch("tooz.drivers.redis.redis.Redis")
    def test_stop_releases_resources_when_redis_refuses(self, mock_client_cls):
        coordinator, client = self._start_coordinator(mock_client_cls)
        self._assert_stop_releases_resources(
            client, coordinator, exceptions.ConnectionError('refused')
        )

    @mock.patch("tooz.drivers.redis.redis.Redis")
    def test_stop_releases_resources_when_redis_times_out(
        self, mock_client_cls
    ):
        coordinator, client = self._start_coordinator(mock_client_cls)
        self._assert_stop_releases_resources(
            client, coordinator, exceptions.TimeoutError('timed out')
        )

    def _sentinel_driver(self, manager):
        master = manager.master_for.return_value
        master.info.return_value = {"redis_version": "7.0.15"}
        coordinator = coordination.get_coordinator(
            "redis://localhost:26379?sentinel=service", b"member"
        )
        assert isinstance(
            coordinator, coordination.CoordinationDriverWithExecutor
        )
        self.addCleanup(coordinator._executor.stop)
        return coordinator, master

    @mock.patch("tooz.drivers.redis.sentinel.Sentinel")
    def test_stop_closes_all_sentinel_clients(self, sentinel_cls):
        manager = sentinel_cls.return_value
        manager.sentinels = [mock.Mock(), mock.Mock()]
        coordinator, master = self._sentinel_driver(manager)
        coordinator.start()

        coordinator.stop()

        master.close.assert_called_once_with()
        for client in manager.sentinels:
            client.close.assert_called_once_with()

    @mock.patch("tooz.drivers.redis.sentinel.Sentinel")
    def test_failed_start_closes_sentinel_clients(self, sentinel_cls):
        manager = sentinel_cls.return_value
        manager.sentinels = [mock.Mock(), mock.Mock()]
        coordinator, master = self._sentinel_driver(manager)
        # An unsupported server version is not retried.
        master.info.return_value = {"redis_version": "1.0.0"}

        self.assertRaises(tooz.NotImplemented, coordinator.start)

        master.close.assert_called_once_with()
        for client in manager.sentinels:
            client.close.assert_called_once_with()

    @mock.patch("tooz.drivers.redis.sentinel.Sentinel")
    def test_close_error_does_not_skip_other_sentinel_clients(
        self, sentinel_cls
    ):
        manager = sentinel_cls.return_value
        manager.sentinels = [mock.Mock(), mock.Mock()]
        coordinator, master = self._sentinel_driver(manager)
        coordinator.start()
        master.close.side_effect = exceptions.ConnectionError("close failed")

        with self.assertLogs("tooz.drivers.redis", "WARNING"):
            coordinator.stop()

        self.assertFalse(coordinator.is_started)
        for client in manager.sentinels:
            client.close.assert_called_once_with()

    @mock.patch("tooz.drivers.redis.sentinel.Sentinel")
    def test_restart_closes_each_sentinel_generation_once(self, sentinel_cls):
        managers = [mock.Mock(), mock.Mock()]
        sentinel_cls.side_effect = managers
        for manager in managers:
            manager.sentinels = [mock.Mock()]
            manager.master_for.return_value.info.return_value = {
                "redis_version": "7.0.15"
            }
        coordinator, _ = self._sentinel_driver(managers[0])

        for _ in managers:
            coordinator.start()
            coordinator.stop()

        for manager in managers:
            manager.master_for.return_value.close.assert_called_once_with()
            manager.sentinels[0].close.assert_called_once_with()
