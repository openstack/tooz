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

import hashlib
import time

import six
import sysv_ipc

import tooz
from tooz import coordination
from tooz import locking

if sysv_ipc.KEY_MIN <= 0:
    _KEY_RANGE = abs(sysv_ipc.KEY_MIN) + sysv_ipc.KEY_MAX
else:
    _KEY_RANGE = sysv_ipc.KEY_MAX - sysv_ipc.KEY_MIN


class IPCLock(locking.Lock):
    """A sysv IPC based lock.

    Please ensure you have read over (and understand) the limitations of sysv
    IPC locks, and especially have tried and used $ ipcs -l (note the maximum
    number of semaphores system wide field that command outputs). To ensure
    that you do not reach that limit it is recommended to use destroy() at
    the correct program exit/entry points.
    """
    _LOCK_PROJECT = b'__TOOZ_LOCK_'

    def __init__(self, name):
        super(IPCLock, self).__init__(name)
        self.key = self.ftok(name, self._LOCK_PROJECT)
        self._lock = None

    @staticmethod
    def ftok(name, project):
        # Similar to ftok & http://semanchuk.com/philip/sysv_ipc/#ftok_weakness
        # but hopefully without as many weaknesses...
        h = hashlib.md5()
        if not isinstance(project, six.binary_type):
            project = project.encode('ascii')
        h.update(project)
        if not isinstance(name, six.binary_type):
            name = name.encode('ascii')
        h.update(name)
        return (int(h.hexdigest(), 16) % _KEY_RANGE) + sysv_ipc.KEY_MIN

    def acquire(self, blocking=True):
        if (blocking is not True
           and sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED is False):
            raise tooz.NotImplemented(
                "This system does not support semaphore timeout")
        # Convert blocking argument to a valid timeout value
        if blocking is True:
            timeout = None
            start_time = None
        elif blocking is False:
            timeout = 0
            start_time = None
        else:
            timeout = blocking
            start_time = time.time()
        while True:
            try:
                self._lock = sysv_ipc.Semaphore(self.key,
                                                flags=sysv_ipc.IPC_CREX,
                                                initial_value=1)
                self._lock.undo = True
            except sysv_ipc.ExistentialError:
                # We failed to create it because it already exists, then try to
                # grab the existing one.
                try:
                    self._lock = sysv_ipc.Semaphore(self.key)
                    self._lock.undo = True
                except sysv_ipc.ExistentialError:
                    # Semaphore has been deleted in the mean time, retry from
                    # the beginning!
                    continue
            if start_time is not None:
                elapsed = max(0.0, time.time() - start_time)
                if elapsed >= timeout:
                    # Ran out of time...
                    return False
                adjusted_timeout = timeout - elapsed
            else:
                adjusted_timeout = timeout
            try:
                self._lock.acquire(timeout=adjusted_timeout)
            except sysv_ipc.BusyError:
                self._lock = None
                return False
            except sysv_ipc.ExistentialError:
                # Likely the lock has been deleted in the meantime, retry
                continue
            else:
                return True

    def release(self):
        if self._lock is not None:
            self._lock.remove()
            self._lock = None
            return True
        return False


class IPCDriver(coordination.CoordinationDriver):

    def __init__(self, member_id, parsed_url, options):
        """Initialize the IPC driver."""
        super(IPCDriver, self).__init__()

    @staticmethod
    def get_lock(name):
        return IPCLock(name)

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
