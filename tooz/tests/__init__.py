# -*- coding: utf-8 -*-
#
#    Copyright (C) 2014 eNovance Inc. All Rights Reserved.
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

import functools
import os

import fixtures
from oslo_utils import uuidutils
import six
from testtools import testcase

import tooz


def get_random_uuid():
    return uuidutils.generate_uuid().encode('ascii')


def _skip_decorator(func):
    @functools.wraps(func)
    def skip_if_not_implemented(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except tooz.NotImplemented as e:
            raise testcase.TestSkipped(str(e))
    return skip_if_not_implemented


class SkipNotImplementedMeta(type):
    def __new__(cls, name, bases, local):
        for attr in local:
            value = local[attr]
            if callable(value) and (
                    attr.startswith('test_') or attr == 'setUp'):
                local[attr] = _skip_decorator(value)
        return type.__new__(cls, name, bases, local)


@six.add_metaclass(SkipNotImplementedMeta)
class TestWithCoordinator(testcase.TestCase):
    url = os.getenv("TOOZ_TEST_URL")

    def setUp(self):
        super(TestWithCoordinator, self).setUp()
        if self.url is None:
            raise RuntimeError("No URL set for this driver")
        if os.getenv("TOOZ_TEST_ETCD3"):
            self.url = self.url.replace("etcd://", "etcd3://")
        if os.getenv("TOOZ_TEST_ETCD3GW"):
            self.url = self.url.replace("etcd://", "etcd3+http://")
        self.useFixture(fixtures.NestedTempfile())
        self.group_id = get_random_uuid()
        self.member_id = get_random_uuid()
        self._coord = tooz.coordination.get_coordinator(self.url,
                                                        self.member_id)
        self._coord.start()

    def tearDown(self):
        self._coord.stop()
        super(TestWithCoordinator, self).tearDown()
