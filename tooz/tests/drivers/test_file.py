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

import datetime
import os

import fixtures
from testtools import testcase

import tooz
from tooz import coordination
from tooz import tests
from tooz import utils


class TestFileDriver(testcase.TestCase):
    _FAKE_MEMBER_ID = tests.get_random_uuid()

    def test_base_dir(self):
        file_path = '/fake/file/path'
        url = f'file://{file_path}'

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        self.assertEqual(file_path, coord._dir)

    def test_leftover_file(self):
        fixture = self.useFixture(fixtures.TempDir())

        file_path = fixture.path
        url = f'file://{file_path}'

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        coord.create_group(b"my_group").get()
        safe_group_id = coord._make_filesystem_safe(b"my_group")
        with open(
            os.path.join(file_path, 'groups', safe_group_id, "junk.txt"), "wb"
        ):
            pass
        os.unlink(
            os.path.join(file_path, 'groups', safe_group_id, '.metadata')
        )
        self.assertRaises(tooz.ToozError, coord.delete_group(b"my_group").get)

    def test_get_members_bytes_member_id(self):
        """Member joined with bytes member_id is returned as bytes."""
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'
        member_id = b"test-member"

        coord = coordination.get_coordinator(url, member_id)
        coord.start()
        self.addCleanup(coord.stop)

        coord.create_group(b"my_group").get()
        coord.join_group(b"my_group").get()
        members = coord.get_members(b"my_group").get()

        self.assertEqual({member_id}, members)
        self.assertIsInstance(next(iter(members)), bytes)

    def test_get_members_str_member_id(self):
        """Member joined with str member_id is returned as str.

        There is no runtime enforcement that member_id must be bytes. When a
        str is passed, the file driver stores it with encoded=True and
        get_members returns the str form.
        """
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'
        member_id = "test-member"

        coord = coordination.get_coordinator(url, member_id)  # type: ignore[arg-type]
        coord.start()
        self.addCleanup(coord.stop)

        coord.create_group(b"my_group").get()
        coord.join_group(b"my_group").get()
        members = coord.get_members(b"my_group").get()

        self.assertEqual({member_id}, members)
        self.assertIsInstance(next(iter(members)), str)

    def test_get_groups_bytes_group_id(self):
        """Group created with bytes group_id is returned as bytes."""
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        group_id = b"my_group"
        coord.create_group(group_id).get()
        groups = coord.get_groups().get()

        self.assertIn(group_id, groups)
        self.assertIsInstance(groups[0], bytes)

    def test_get_groups_str_group_id(self):
        """Group created with str group_id is returned as str.

        There is no runtime enforcement that group_id must be bytes. When a
        str is passed, the file driver stores it with encoded=True and
        get_groups returns the str form.
        """
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        group_id = "my_group"
        coord.create_group(group_id).get()  # type: ignore[arg-type]
        groups = coord.get_groups().get()

        self.assertIn(group_id, groups)
        self.assertIsInstance(groups[0], str)

    def test_get_members_pre_136_bytes_member_id(self):
        """Group metadata written without 'encoded' field, bytes ID.

        Test behavior with a pre-1.36 payload if the user passed a bytes
        member_id.
        """
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'
        member_id = b"test-member"

        coord = coordination.get_coordinator(url, member_id)
        coord.start()
        self.addCleanup(coord.stop)

        coord.create_group(b"my_group").get()

        # Write a member file in the old pre-1.36 format: no 'encoded' key,
        # member_id stored directly as bytes.
        safe_group_id = coord._make_filesystem_safe(b"my_group")
        safe_member_id = coord._make_filesystem_safe(member_id)
        member_path = os.path.join(
            fixture.path, 'groups', safe_group_id, f"{safe_member_id}.raw"
        )
        old_format = {
            'member_id': member_id,
            'joined_on': datetime.datetime.now(),
        }
        with open(member_path, 'wb') as fh:
            fh.write(utils.dumps(old_format))

        members = coord.get_members(b"my_group").get()
        self.assertEqual({member_id}, members)
        self.assertIsInstance(next(iter(members)), bytes)

    def test_get_members_pre_136_str_member_id(self):
        """Group metadata written without 'encoded' field, str ID.

        Test behavior with a pre-1.36 payload if the user passed a str
        member_id.
        """
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'
        member_id = "test-member"

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        coord.create_group(b"my_group").get()

        # Write a member file in the old pre-1.36 format: no 'encoded' key,
        # member_id stored directly as str.
        safe_group_id = coord._make_filesystem_safe(b"my_group")
        safe_member_id = coord._make_filesystem_safe(member_id)
        member_path = os.path.join(
            fixture.path, 'groups', safe_group_id, f"{safe_member_id}.raw"
        )
        old_format = {
            'member_id': member_id,
            'joined_on': datetime.datetime.now(),
        }
        with open(member_path, 'wb') as fh:
            fh.write(utils.dumps(old_format))

        members = coord.get_members(b"my_group").get()
        self.assertEqual({member_id}, members)
        self.assertIsInstance(next(iter(members)), str)

    def test_get_groups_pre_136_bytes_group_id(self):
        """Group metadata written without 'encoded' field, bytes ID.

        Test behavior with a pre-1.36 payload if the user passed a bytes
        group_id.
        """
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'
        group_id = b"my_group"

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        # Write a group metadata file in the old pre-1.36 format: no
        # 'encoded' key, group_id stored directly as bytes.
        safe_group_id = coord._make_filesystem_safe(group_id)
        group_dir = os.path.join(fixture.path, 'groups', safe_group_id)
        os.makedirs(group_dir)
        old_format = {'group_id': group_id}
        with open(os.path.join(group_dir, '.metadata'), 'wb') as fh:
            fh.write(utils.dumps(old_format))

        groups = coord.get_groups().get()
        self.assertIn(group_id, groups)
        self.assertIsInstance(groups[0], bytes)

    def test_get_groups_pre_136_str_group_id(self):
        """Group metadata written without 'encoded' field, str ID.

        Test behavior with a pre-1.36 payload if the user passed a str
        group_id.
        """
        fixture = self.useFixture(fixtures.TempDir())
        url = f'file://{fixture.path}'
        group_id = "my_group"

        coord = coordination.get_coordinator(url, self._FAKE_MEMBER_ID)
        coord.start()
        self.addCleanup(coord.stop)

        # Write a group metadata file in the old pre-1.36 format: no
        # 'encoded' key, group_id stored directly as str.
        safe_group_id = coord._make_filesystem_safe(group_id)
        group_dir = os.path.join(fixture.path, 'groups', safe_group_id)
        os.makedirs(group_dir)
        old_format = {'group_id': group_id}
        with open(os.path.join(group_dir, '.metadata'), 'wb') as fh:
            fh.write(utils.dumps(old_format))

        groups = coord.get_groups().get()
        self.assertIn(group_id, groups)
        self.assertIsInstance(groups[0], str)
