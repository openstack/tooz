# -*- coding: utf-8 -*-
#
# Copyright © 2014 eNovance
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
import struct
import time

from concurrent import futures
import msgpack
import six
import sysv_ipc

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils

if sysv_ipc.KEY_MIN <= 0:
    _KEY_RANGE = abs(sysv_ipc.KEY_MIN) + sysv_ipc.KEY_MAX
else:
    _KEY_RANGE = sysv_ipc.KEY_MAX - sysv_ipc.KEY_MIN


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
        self.key = ftok(name, self._LOCK_PROJECT)
        self._lock = None

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
    """A `IPC`_ based driver.

    This driver uses `IPC`_ concepts to provide the coordination driver
    semantics and required API(s). It **is** missing some functionality but
    in the future these not implemented API(s) will be filled in.

    .. _IPC: http://en.wikipedia.org/wiki/Inter-process_communication
    """

    _SEGMENT_SIZE = 1024
    _GROUP_LIST_KEY = "GROUP_LIST"
    _GROUP_PROJECT = "_TOOZ_INTERNAL"
    _INTERNAL_LOCK_NAME = "TOOZ_INTERNAL_LOCK"

    def __init__(self, member_id, parsed_url, options):
        """Initialize the IPC driver."""
        super(IPCDriver, self).__init__()

    def _start(self):
        self._group_list = sysv_ipc.SharedMemory(
            ftok(self._GROUP_LIST_KEY, self._GROUP_PROJECT),
            sysv_ipc.IPC_CREAT,
            size=self._SEGMENT_SIZE)
        self._lock = self.get_lock(self._INTERNAL_LOCK_NAME)
        self._executor = futures.ThreadPoolExecutor(max_workers=1)

    def _stop(self):
        self._executor.shutdown(wait=True)
        try:
            self._group_list.detach()
            self._group_list.remove()
        except sysv_ipc.ExistentialError:
            pass

    def _read_group_list(self):
        data = self._group_list.read(byte_count=2)
        length = struct.unpack("H", data)[0]
        if length == 0:
            return set()
        data = self._group_list.read(byte_count=length, offset=2)
        return set(msgpack.loads(data))

    def _write_group_list(self, group_list):
        data = msgpack.dumps(list(group_list))
        if len(data) >= self._SEGMENT_SIZE - 2:
            raise coordination.ToozError("Group list is too big")
        self._group_list.write(struct.pack('H', len(data)))
        self._group_list.write(data, offset=2)

    def create_group(self, group_id):
        def _create_group():
            with self._lock:
                group_list = self._read_group_list()
                if group_id in group_list:
                    raise coordination.GroupAlreadyExist(group_id)
                group_list.add(group_id)
                self._write_group_list(group_list)

        return IPCFutureResult(self._executor.submit(_create_group))

    def delete_group(self, group_id):
        def _delete_group():
            with self._lock:
                group_list = self._read_group_list()
                if group_id not in group_list:
                    raise coordination.GroupNotCreated(group_id)
                group_list.remove(group_id)
                self._write_group_list(group_list)

        return IPCFutureResult(self._executor.submit(_delete_group))

    def _get_groups_handler(self):
        with self._lock:
            return self._read_group_list()

    def get_groups(self):
        return IPCFutureResult(self._executor.submit(
            self._get_groups_handler))

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


class IPCFutureResult(coordination.CoordAsyncResult):
    """IPC asynchronous result that references a future."""
    def __init__(self, fut):
        self._fut = fut

    def get(self, timeout=10):
        try:
            return self._fut.result(timeout=timeout)
        except futures.TimeoutError as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          utils.exception_message(e),
                                          cause=e)

    def done(self):
        return self._fut.done()
