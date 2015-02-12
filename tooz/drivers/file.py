# -*- coding: utf-8 -*-
#
# Copyright © 2015 eNovance
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
import errno
import os

import tooz
from tooz import coordination
from tooz.drivers import _retry
from tooz import locking


class FileLock(locking.Lock):
    """A file based lock."""

    def __init__(self, path):
        super(FileLock, self).__init__(path)
        self.acquired = False

    def acquire(self, blocking=True):
        self.lockfile = open(self.name, 'a')

        @_retry.retry(stop_max_delay=blocking)
        def _lock():
            # NOTE(jd) If the same process try to grab the lock, the call to
            # self.lock() will succeed, so we track internally if the process
            # already has the lock.
            if self.acquired is True:
                if blocking:
                    raise _retry.Retry
                return False
            try:
                self.lock()
            except IOError as e:
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    if blocking:
                        raise _retry.Retry
                    return False
            else:
                self.acquired = True
                return True

        return _lock()

    def release(self):
        self.unlock()
        self.lockfile.close()
        self.acquired = False

    def lock(self):
        raise NotImplementedError

    def unlock(self):
        raise NotImplementedError


class WindowsFileLock(FileLock):
    def lock(self):
        msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_NBLCK, 1)

    def unlock(self):
        msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_UNLCK, 1)


class PosixFileLock(FileLock):
    def lock(self):
        fcntl.lockf(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock(self):
        fcntl.lockf(self.lockfile, fcntl.LOCK_UN)


if os.name == 'nt':
    import msvcrt
    LockClass = WindowsFileLock
else:
    import fcntl
    LockClass = PosixFileLock


class FileDriver(coordination.CoordinationDriver):
    """A file based driver."""

    def __init__(self, member_id, parsed_url, options):
        """Initialize the file driver."""
        super(FileDriver, self).__init__()
        self._lockdir = parsed_url.path

    def get_lock(self, name):
        path = os.path.abspath(os.path.join(self._lockdir, name.decode()))
        return locking.SharedWeakLockHelper(self._lockdir, LockClass, path)

    @staticmethod
    def watch_join_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_join_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def watch_leave_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_leave_group(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented
