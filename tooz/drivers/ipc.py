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
import posix_ipc

from tooz import coordination
from tooz import locking
from tooz.openstack.common import lockutils


class IPCLock(locking.Lock):
    _LOCK_PREFIX = b'_tooz_'

    def __init__(self, name, timeout):
        self.lock = lockutils.external_lock(
            (self._LOCK_PREFIX + name).decode('ascii'))
        self.timeout = timeout

    def acquire(self, blocking=True):
        timeout = self.timeout if blocking else 0
        try:
            return bool(self.lock.acquire(timeout=timeout))
        # TODO(jd) This should be encapsulated in lockutils!
        except posix_ipc.BusyError:
            return False

    def release(self):
        return self.lock.release()


class IPCDriver(coordination.CoordinationDriver):

    def __init__(self, member_id, parsed_url, options):
        """Initialize the IPC driver.

        :param lock_timeout: how many seconds to wait when trying to acquire
                             a lock in blocking mode. None means forever, 0
                             means don't wait, any other value means wait
                             this amount of seconds. super(IPCDriver,
                             self).__init__() self.lock_timeout =
                             lock_timeout
        """
        super(IPCDriver, self).__init__()
        self.lock_timeout = int(options.get('lock_timeout', ['30'])[-1])

    def get_lock(self, name):
        return IPCLock(name, self.lock_timeout)

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
