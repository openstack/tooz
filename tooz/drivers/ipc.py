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

from __future__ import annotations

import hashlib
import msgpack
import struct
import sysv_ipc
import time

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils

if sysv_ipc.KEY_MIN <= 0:
    _KEY_RANGE = abs(sysv_ipc.KEY_MIN) + sysv_ipc.KEY_MAX
else:
    _KEY_RANGE = sysv_ipc.KEY_MAX - sysv_ipc.KEY_MIN


def ftok(name: str | bytes, project: str | bytes) -> int:
    # Similar to ftok & http://semanchuk.com/philip/sysv_ipc/#ftok_weakness
    # but hopefully without as many weaknesses...
    h = hashlib.md5(usedforsecurity=False)
    if not isinstance(project, bytes):
        project = project.encode('ascii')
    h.update(project)
    if not isinstance(name, bytes):
        name = name.encode('ascii')
    h.update(name)
    return (int(h.hexdigest(), 16) % int(_KEY_RANGE)) + int(sysv_ipc.KEY_MIN)


class IPCLock(locking.Lock):
    """A sysv IPC based lock.

    Please ensure you have read over (and understand) the limitations of sysv
    IPC locks, and especially have tried and used $ ipcs -l (note the maximum
    number of semaphores system wide field that command outputs). To ensure
    that you do not reach that limit it is recommended to use destroy() at
    the correct program exit/entry points.
    """

    _LOCK_PROJECT = b'__TOOZ_LOCK_'

    def __init__(self, name: bytes) -> None:
        super().__init__(name)
        self.key = ftok(name, self._LOCK_PROJECT)
        self._lock: sysv_ipc.Semaphore | None = None

    def break_(self) -> bool:
        try:
            lock = sysv_ipc.Semaphore(key=self.key)
            lock.remove()
        except sysv_ipc.ExistentialError:
            return False
        else:
            return True

    def acquire(
        self,
        blocking: bool = True,
        shared: bool = False,
        timeout: float | None = None,
    ) -> bool:
        if shared:
            raise tooz.NotImplemented("not implemented")

        if (
            blocking is not True
            and sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED is False
        ):
            raise tooz.NotImplemented(
                "This system does not support semaphore timeouts"
            )
        if blocking is False and timeout is not None:
            raise tooz.NotImplemented(
                "Timeout without blocking is not implemented"
            )
        blocking, timeout = utils.convert_blocking(blocking, timeout)
        start_time = None
        if not blocking:
            timeout = 0
        elif blocking and timeout is not None:
            start_time = time.time()
        while True:
            tmplock = None
            try:
                tmplock = sysv_ipc.Semaphore(
                    self.key, flags=sysv_ipc.IPC_CREX, initial_value=1
                )
                tmplock.undo = True
            except sysv_ipc.ExistentialError:
                # We failed to create it because it already exists, then try to
                # grab the existing one.
                try:
                    tmplock = sysv_ipc.Semaphore(self.key)
                    tmplock.undo = True
                except sysv_ipc.ExistentialError:
                    # Semaphore has been deleted in the mean time, retry from
                    # the beginning!
                    continue

            if start_time is not None:
                assert timeout is not None  # only set when timeout is not None
                elapsed = max(0.0, time.time() - start_time)
                if elapsed >= timeout:
                    # Ran out of time...
                    return False
                adjusted_timeout: float | None = timeout - elapsed
            else:
                adjusted_timeout = timeout
            try:
                tmplock.acquire(timeout=adjusted_timeout)
            except sysv_ipc.BusyError:
                tmplock = None
                return False
            except sysv_ipc.ExistentialError:
                # Likely the lock has been deleted in the meantime, retry
                continue
            else:
                self._lock = tmplock
                return True

    def release(self) -> bool:
        if self._lock is not None:
            try:
                self._lock.remove()
                self._lock = None
            except sysv_ipc.ExistentialError:
                return False
            return True
        return False


class IPCDriver(coordination.CoordinationDriverWithExecutor):
    """A `IPC`_ based driver.

    This driver uses `IPC`_ concepts to provide the coordination driver
    semantics and required API(s). It **is** missing some functionality but
    in the future these not implemented API(s) will be filled in.

    The IPC driver connection URI should look like::

      ipc://

    General recommendations/usage considerations:

    - It is **not** distributed (or recommended to be used in those
      situations, so the developer using this should really take that into
      account when applying this driver in there app).

    .. _IPC: http://en.wikipedia.org/wiki/Inter-process_communication
    """

    CHARACTERISTICS = (
        coordination.Characteristics.NON_TIMEOUT_BASED,
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    _SEGMENT_SIZE = 1024
    _GROUP_LIST_KEY = "GROUP_LIST"
    _GROUP_PROJECT = "_TOOZ_INTERNAL"
    _INTERNAL_LOCK_NAME = b"TOOZ_INTERNAL_LOCK"

    def _start(self) -> None:
        super()._start()
        self._group_list = sysv_ipc.SharedMemory(
            ftok(self._GROUP_LIST_KEY, self._GROUP_PROJECT),
            sysv_ipc.IPC_CREAT,
            size=self._SEGMENT_SIZE,
        )
        self._lock = self.get_lock(self._INTERNAL_LOCK_NAME)

    def _stop(self) -> None:
        super()._stop()
        try:
            self._group_list.detach()
            self._group_list.remove()
        except sysv_ipc.ExistentialError:
            pass

    def _read_group_list(self) -> set[bytes]:
        data = self._group_list.read(byte_count=2)
        length = struct.unpack("H", data)[0]
        if length == 0:
            return set()
        data = self._group_list.read(byte_count=length, offset=2)
        return set(msgpack.loads(data))

    def _write_group_list(self, group_list: set[bytes]) -> None:
        data = msgpack.dumps(list(group_list))
        if len(data) >= self._SEGMENT_SIZE - 2:
            raise tooz.ToozError("Group list is too big")
        self._group_list.write(struct.pack('H', len(data)))
        self._group_list.write(data, offset=2)

    def create_group(
        self, group_id: bytes
    ) -> coordination.CoordAsyncResult[None]:
        def _create_group() -> None:
            with self._lock:
                group_list = self._read_group_list()
                if group_id in group_list:
                    raise coordination.GroupAlreadyExist(group_id)
                group_list.add(group_id)
                self._write_group_list(group_list)

        return coordination.CoordinatorResult(
            self._executor.submit(_create_group)
        )

    def delete_group(
        self, group_id: bytes
    ) -> coordination.CoordAsyncResult[None]:
        def _delete_group() -> None:
            with self._lock:
                group_list = self._read_group_list()
                if group_id not in group_list:
                    raise coordination.GroupNotCreated(group_id)
                group_list.remove(group_id)
                self._write_group_list(group_list)

        return coordination.CoordinatorResult(
            self._executor.submit(_delete_group)
        )

    def watch_join_group(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.MemberJoinedGroup],
    ) -> None:
        # Check the group exist
        self.get_members(group_id).get()
        super().watch_join_group(group_id, callback)

    def watch_leave_group(
        self,
        group_id: bytes,
        callback: coordination.EventCallback[coordination.MemberLeftGroup],
    ) -> None:
        # Check the group exist
        self.get_members(group_id).get()
        super().watch_leave_group(group_id, callback)

    def _get_groups_handler(self) -> list[bytes]:
        with self._lock:
            return list(self._read_group_list())

    def get_groups(self) -> coordination.CoordAsyncResult[list[bytes]]:
        return coordination.CoordinatorResult(
            self._executor.submit(self._get_groups_handler)
        )

    def get_lock(self, name: bytes) -> IPCLock:
        return IPCLock(name)
