# Copyright 2016 Cloudbase Solutions Srl
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

import os

import fixtures
from testtools import testcase

import tooz
from tooz import coordination
from tooz import tests


class TestFileDriver(testcase.TestCase):
    _FAKE_MEMBER_ID = tests.get_random_uuid()

    def test_base_dir(self):
        file_path = '/fake/file/path'
        url = 'file://%s' % file_path

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        self.assertEqual(file_path, coord._dir)

    def test_leftover_file(self):
        fixture = self.useFixture(fixtures.TempDir())

        file_path = fixture.path
        url = 'file://%s' % file_path

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        coord.create_group(b"my_group").get()
        safe_group_id = coord._make_filesystem_safe(b"my_group")
        with open(os.path.join(file_path, 'groups',
                  safe_group_id, "junk.txt"), "wb"):
            pass
        os.unlink(os.path.join(file_path, 'groups',
                               safe_group_id, '.metadata'))
        self.assertRaises(tooz.ToozError,
                          coord.delete_group(b"my_group").get)
