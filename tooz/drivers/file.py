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

import logging
import os
import threading

import fasteners
from oslo_utils import timeutils

import tooz
from tooz import coordination
from tooz import locking

LOG = logging.getLogger(__name__)


class FileLock(locking.Lock):
    """A file based lock."""

    def __init__(self, path):
        super(FileLock, self).__init__(path)
        self.acquired = False
        self._lock = fasteners.InterProcessLock(path)
        self._cond = threading.Condition()

    def acquire(self, blocking=True):
        timeout = None
        if not isinstance(blocking, bool):
            timeout = float(blocking)
            blocking = True
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()
        while True:
            with self._cond:
                if self.acquired and blocking:
                    if watch.expired():
                        return False
                    # If in the same process wait until we can attempt to
                    # acquire it (aka, another thread should release it before
                    # we can try to get it).
                    self._cond.wait(watch.leftover(return_none=True))
                elif self.acquired and not blocking:
                    return False
                else:
                    # All the prior waits may have left less time to wait...
                    timeout = watch.leftover(return_none=True)
                    self.acquired = self._lock.acquire(blocking=blocking,
                                                       timeout=timeout)
                    return self.acquired

    def release(self):
        with self._cond:
            if self.acquired:
                self._lock.release()
                self.acquired = False
                self._cond.notify_all()

    def __del__(self):
        if self.acquired:
            LOG.warn("unreleased lock %s garbage collected" % self.name)


class FileDriver(coordination.CoordinationDriver):
    """A file based driver.

    This driver uses files and directories (and associated file locks) to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.
    """

    def __init__(self, member_id, parsed_url, options):
        """Initialize the file driver."""
        super(FileDriver, self).__init__()
        self._lockdir = parsed_url.path

    def get_lock(self, name):
        path = os.path.abspath(os.path.join(self._lockdir, name.decode()))
        return locking.SharedWeakLockHelper(self._lockdir, FileLock, path)

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
