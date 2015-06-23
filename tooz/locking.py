# -*- coding: utf-8 -*-
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
import abc

import six
import threading
import weakref


@six.add_metaclass(abc.ABCMeta)
class Lock(object):
    def __init__(self, name):
        if not name:
            raise ValueError("Locks must be provided a name")
        self.name = name

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    @abc.abstractmethod
    def release(self):
        """Attempts to release the lock, returns true if released.

        The behavior of releasing a lock which was not acquired in the first
        place is undefined (it can range from harmless to releasing some other
        users lock)..

        :returns: returns true if released (false if not)
        :rtype: bool
        """

    @abc.abstractmethod
    def acquire(self, blocking=True):
        """Attempts to acquire the lock.

        :param blocking: If True, blocks until the lock is acquired. If False,
                         returns right away. Otherwise, the value is used as a
                         timeout value and the call returns maximum after this
                         number of seonds.
        :returns: returns true if acquired (false if not)
        :rtype: bool

        """


class SharedWeakLockHelper(Lock):
    """Helper for lock that need to rely on a state in memory and
    be the same object across each coordinator.get_lock(...)
    """

    LOCKS_LOCK = threading.Lock()
    ACQUIRED_LOCKS = dict()
    RELEASED_LOCKS = weakref.WeakValueDictionary()

    def __init__(self, namespace, lockclass, name, *args, **kwargs):
        super(SharedWeakLockHelper, self).__init__(name)
        self._lock_key = "%s:%s" % (namespace, name)
        self._newlock = lambda: lockclass(
            self.name, *args, **kwargs)

    @property
    def lock(self):
        """Access the underlying lock object.

        For internal usage only.
        """
        with self.LOCKS_LOCK:
            try:
                l = self.ACQUIRED_LOCKS[self._lock_key]
            except KeyError:
                l = self.RELEASED_LOCKS.setdefault(
                    self._lock_key, self._newlock())
            return l

    def acquire(self, blocking=True):
        l = self.lock
        if l.acquire(blocking):
            with self.LOCKS_LOCK:
                self.RELEASED_LOCKS.pop(self._lock_key, None)
                self.ACQUIRED_LOCKS[self._lock_key] = l
            return True
        return False

    def release(self):
        with self.LOCKS_LOCK:
            try:
                l = self.ACQUIRED_LOCKS.pop(self._lock_key)
            except KeyError:
                return False
            else:
                if l.release():
                    self.RELEASED_LOCKS[self._lock_key] = l
                    return True
                else:
                    # Put it back...
                    self.ACQUIRED_LOCKS[self._lock_key] = l
                    return False
