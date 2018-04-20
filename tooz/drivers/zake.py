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

from tooz import coordination
from tooz.drivers import zookeeper


class ZakeDriver(zookeeper.KazooDriver):
    """This driver uses the `zake`_ client to mimic real `zookeeper`_ servers.

    It **should** be mainly used (and **is** really only intended to be used in
    this manner) for testing and integration (where real `zookeeper`_ servers
    are typically not available).

    .. _zake: https://pypi.org/project/zake
    .. _zookeeper: http://zookeeper.apache.org/
    """

    CHARACTERISTICS = (
        coordination.Characteristics.NON_TIMEOUT_BASED,
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    # NOTE(harlowja): this creates a shared backend 'storage' layer that
    # would typically exist inside a zookeeper server, but since zake has
    # no concept of a 'real' zookeeper server we create a fake one and share
    # it among active clients to simulate zookeeper's consistent storage in
    # a thread-safe manner.
    fake_storage = fake_storage.FakeStorage(
        fake_client.k_threading.SequentialThreadingHandler())

    @classmethod
    def _make_client(cls, parsed_url, options):
        if 'storage' in options:
            storage = options['storage']
        else:
            storage = cls.fake_storage
        return fake_client.FakeClient(storage=storage)
