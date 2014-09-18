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

        :returns: returns true if acquired (false if not)
        :rtype: bool
        """

    def destroy(self):
        """Removes the lock + any resources associated with the lock."""
