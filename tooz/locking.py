#
#    Copyright (C) 2014 eNovance Inc. All Rights Reserved.
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

from __future__ import annotations

import abc
import types
from typing import Any, Generic, Self, TypeVar

import tooz
from tooz import coordination

LockT = TypeVar('LockT', bound='Lock')


class _LockProxy(Generic[LockT]):
    def __init__(self, lock: LockT, *args: Any, **kwargs: Any):
        self.lock = lock
        self.args = args
        self.kwargs = kwargs

    def __enter__(self) -> LockT:
        return self.lock.__enter__(*self.args, **self.kwargs)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.lock.__exit__(exc_type, exc_val, exc_tb)


class Lock(metaclass=abc.ABCMeta):
    def __init__(self, name: bytes) -> None:
        if not name:
            raise ValueError("Locks must be provided a name")
        self._name = name

    @property
    def name(self) -> bytes:
        return self._name

    def __call__(self, *args: Any, **kwargs: Any) -> _LockProxy[Self]:
        return _LockProxy(self, *args, **kwargs)

    def __enter__(
        self,
        blocking: bool = True,
        shared: bool = False,
        timeout: int | None = None,
    ) -> Self:
        acquired = self.acquire(blocking, shared, timeout)
        if not acquired:
            name = (
                self.name.decode()
                if isinstance(self.name, bytes)
                else self.name
            )
            msg = f'Acquiring lock {name} failed'
            raise coordination.LockAcquireFailed(msg)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.release()

    def is_still_owner(self) -> bool:
        """Checks if the lock is still owned by the acquiree.

        :returns: returns true if still acquired (false if not) and
                  false if the lock was never acquired in the first place
                  or raises ``NotImplemented`` if not implemented.
        """
        raise tooz.NotImplemented("not implemented")

    @abc.abstractmethod
    def release(self) -> bool:
        """Attempts to release the lock, returns true if released.

        The behavior of releasing a lock which was not acquired in the first
        place is undefined (it can range from harmless to releasing some other
        users lock)..

        :returns: returns true if released (false if not)
        :rtype: bool
        """

    def break_(self) -> bool:
        """Forcefully release the lock.

        This is mostly used for testing purposes, to simulate an out of
        band operation that breaks the lock. Backends may allow waiters to
        acquire immediately if a lock is broken, or they should raise an
        exception. Releasing should be successful for objects that believe
        they hold the lock but do not have the lock anymore. However,
        they should be careful not to re-break the lock by releasing it,
        since they may not be the holder anymore.

        :returns: returns true if forcefully broken (false if not)
                  or raises ``NotImplemented`` if not implemented.
        """
        raise tooz.NotImplemented("not implemented")

    @abc.abstractmethod
    def acquire(
        self,
        blocking: bool = True,
        shared: bool = False,
        timeout: int | None = None,
    ) -> bool:
        """Attempts to acquire the lock.

        :param blocking: If True, blocks until the lock is acquired. If False,
                         returns right away. Otherwise, the value is used as a
                         timeout value and the call returns maximum after this
                         number of seconds.
        :param shared: If False, the lock is exclusive. If True, the lock can
                       be shareable or raises ``NotImplemented`` if not
                       implemented.
        :param timeout: Timeout to acquire a lock.
        :returns: returns true if acquired (false if not)
        :rtype: bool
        """
