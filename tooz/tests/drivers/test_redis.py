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

from redis import client as redis_client
from redis.commands import core as redis_commands
from redis import exceptions
from testtools import testcase

import tooz
from tooz import coordination
from tooz.drivers import redis as redis_driver
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


class TestRedisGroupReads(tests.TestWithCoordinator):
    """Group reads against a real Redis server."""

    def setUp(self):
        super().setUp()
        if not isinstance(self._coord, redis_driver.RedisDriver):
            self.skipTest("Redis driver only")
        self._coord.create_group(self.group_id).get()

    @property
    def _driver(self) -> redis_driver.RedisDriver:
        assert isinstance(self._coord, redis_driver.RedisDriver)
        return self._coord

    @property
    def _client(self) -> Any:
        client = self._driver._client
        assert client is not None
        return client

    def _member(self, capabilities=None, heart=False):
        member_id = tests.get_random_uuid()
        coord = coordination.get_coordinator(self.url, member_id)
        coord.start(start_heart=heart)
        self.addCleanup(coord.stop)
        coord.join_group(self.group_id, capabilities).get()
        return coord, member_id

    def _expire(self, member_id):
        self._client.delete(self._driver._encode_beat_id(member_id))

    def _in_group(self, member_id):
        return self._client.hexists(
            self._driver._encode_group_id(self.group_id), member_id
        )

    def test_group_reads_do_not_watch(self):
        self._member({'n': 1})
        with mock.patch.object(
            redis_client.Pipeline,
            'watch',
            side_effect=AssertionError('group reads must not WATCH'),
        ):
            members = self._coord.get_members(self.group_id).get()
            caps = self._coord.get_members_with_capabilities(
                self.group_id
            ).get()
            own = self._coord.get_member_capabilities(
                self.group_id, next(iter(members - {self.member_id}))
            ).get()
        self.assertEqual(1, len(members))
        self.assertEqual(members, set(caps))
        self.assertEqual({'n': 1}, own)

    def test_concurrent_write_does_not_restart_read(self):
        for n in range(3):
            self._member({'n': n})
        joiner = coordination.get_coordinator(
            self.url, tests.get_random_uuid()
        )
        joiner.start()
        self.addCleanup(joiner.stop)
        decoded: list[bytes] = []
        loads = self._driver._loads

        def decode(blob):
            if not decoded:
                # Another member joins while this read is in progress.
                joiner.join_group(self.group_id, {'n': 'late'}).get()
            decoded.append(blob)
            return loads(blob)

        with mock.patch.object(self._driver, '_loads', decode):
            result = self._coord.get_members_with_capabilities(
                self.group_id
            ).get()

        # Each of the three members was decoded exactly once: the join did
        # not make the read start again.
        self.assertEqual(3, len(decoded))
        self.assertNotIn({'n': 'late'}, list(result.values()))

    def test_expired_member_is_removed(self):
        _, gone = self._member({'n': 1})
        self._expire(gone)

        result = self._coord.get_members_with_capabilities(self.group_id)

        self.assertNotIn(gone, result.get())
        self.assertFalse(self._in_group(gone))

    def test_member_renewed_after_read_is_kept(self):
        late, member_id = self._member({'n': 1})
        self._expire(member_id)
        real_mget = redis_commands.BasicKeyCommands.mget
        renewed: list[bool] = []

        def mget(client, keys, *args):
            values = real_mget(client, keys, *args)
            if not renewed:
                # The member heartbeats after its liveness was read.
                late.heartbeat()
                renewed.append(True)
            return values

        with mock.patch.object(redis_commands.BasicKeyCommands, 'mget', mget):
            first = self._coord.get_members(self.group_id).get()

        self.assertNotIn(member_id, first)
        self.assertTrue(self._in_group(member_id))
        self.assertIn(member_id, self._coord.get_members(self.group_id).get())

    def test_self_is_not_removed(self):
        self._coord.join_group(self.group_id).get()
        self._expire(self.member_id)

        members = self._coord.get_members(self.group_id).get()

        self.assertIn(self.member_id, members)
        self.assertTrue(self._in_group(self.member_id))

    def test_expired_members_removed_in_batches(self):
        gone = [self._member({'n': n})[1] for n in range(5)]
        for member_id in gone:
            self._expire(member_id)
        prune = mock.Mock(wraps=self._driver._scripts['prune_members'])

        with mock.patch.object(self._driver, 'PRUNE_BATCH_SIZE', 2):
            with mock.patch.dict(self._driver._scripts, prune_members=prune):
                members = self._coord.get_members(self.group_id).get()

        self.assertEqual(set(), members)
        batches = [c.kwargs['args'] for c in prune.call_args_list]
        self.assertEqual([2, 2, 1], [len(b) for b in batches])
        self.assertEqual(sorted(gone), sorted(m for b in batches for m in b))
        self.assertFalse(any(self._in_group(m) for m in gone))

    def test_failed_removal_is_translated(self):
        _, gone = self._member({'n': 1})
        self._expire(gone)
        for failure, expected in (
            (
                exceptions.ConnectionError('refused'),
                coordination.ToozConnectionError,
            ),
            (
                exceptions.TimeoutError('timed out'),
                coordination.ToozConnectionError,
            ),
            (exceptions.ResponseError('bad script'), tooz.ToozError),
            (exceptions.ReadOnlyError('read only replica'), tooz.ToozError),
        ):
            for method in ('get_members', 'get_members_with_capabilities'):
                with self.subTest(failure=type(failure), method=method):
                    prune = mock.Mock(side_effect=failure)
                    with mock.patch.dict(
                        self._driver._scripts, prune_members=prune
                    ):
                        result = getattr(self._coord, method)(self.group_id)
                        error = self.assertRaises(expected, result.get)

                    self.assertIs(failure, error.__cause__)
                    prune.assert_called()
                    self.assertTrue(self._in_group(gone))
