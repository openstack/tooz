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

    def __init__(self, name, timeout):
        super(IPCLock, self).__init__(name)
        self.key = self.ftok(name, self._LOCK_PROJECT)
        try:
            self._lock = sysv_ipc.Semaphore(self.key,
                                            flags=sysv_ipc.IPC_CREX,
                                            initial_value=1)
        except sysv_ipc.ExistentialError:
            self._lock = sysv_ipc.Semaphore(self.key)
        self._lock.undo = True
        self.timeout = timeout

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
        timeout = self.timeout if blocking else 0
        try:
            self._lock.acquire(timeout=timeout)
        except (sysv_ipc.BusyError, sysv_ipc.ExistentialError):
            return False
        else:
            return True

    def release(self):
        try:
            self._lock.release()
        except sysv_ipc.ExistentialError:
            return False
        else:
            return True

    def destroy(self):
        """This will destroy the lock.

        NOTE(harlowja): this will destroy the lock, and if it is being shared
        across processes this can have unintended consquences, so it *must*
        only be used when it is *safe* to remove it (ie at a known program
        exit point, where it can be ensured that no other process will be
        using it, or that if other processes are using it they can tolerate
        it being destroyed).

        Read your man pages for `semctl(IPC_RMID)` before using this to
        understand its side-effects on other programs that *may* be
        concurrently using the same lock while it is being destroyed...
        """
        try:
            self._lock.remove()
        except sysv_ipc.ExistentialError:
            pass


class IPCDriver(coordination.CoordinationDriver):

    def __init__(self, member_id, parsed_url, options):
        """Initialize the IPC driver.

        :param lock_timeout: how many seconds to wait when trying to acquire
                             a lock in blocking mode. None means forever, 0
                             means don't wait, any other value means wait
                             this amount of seconds.
        """
        super(IPCDriver, self).__init__()
        self.lock_timeout = int(options.get('lock_timeout', ['30'])[-1])

    def get_lock(self, name):
        return IPCLock(name, self.lock_timeout)

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
