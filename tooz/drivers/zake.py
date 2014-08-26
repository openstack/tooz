# Copyright (c) 2013-2014 Mirantis Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import

from zake import fake_client
from zake import fake_storage

from tooz.drivers import zookeeper


class ZakeDriver(zookeeper.BaseZooKeeperDriver):
    """The driver using the Zake client which mimic a fake Kazoo client
    without the need of real ZooKeeper servers.
    """

    # here we need to pass *threading handler* as an argument
    fake_storage = fake_storage.FakeStorage(
        fake_client.k_threading.SequentialThreadingHandler())

    def __init__(self, member_id, parsed_url, options):
        super(ZakeDriver, self).__init__(member_id, parsed_url, options)
        self._coord = fake_client.FakeClient(storage=self.fake_storage)

    @staticmethod
    def watch_join_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def unwatch_join_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def watch_leave_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def unwatch_leave_group(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise NotImplementedError

    @staticmethod
    def run_watchers():
        raise NotImplementedError
