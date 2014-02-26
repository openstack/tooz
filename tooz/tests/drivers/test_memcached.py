# -*- coding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Author: Julien Danjou <julien@danjou.info>
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

from testtools import testcase


from tooz.drivers import memcached


class TestRetry(testcase.TestCase):
    def test_retry(self):
        self.i = 1

        @memcached.retry
        def x(add_that):
            if self.i == 1:
                self.i += add_that
                raise memcached.Retry
            return self.i

        self.assertEqual(x(42), 43)
