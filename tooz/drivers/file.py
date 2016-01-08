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

import contextlib
import datetime
import errno
import hashlib
import logging
import os
import shutil
import threading
import weakref

from concurrent import futures

import fasteners
from oslo_utils import encodeutils
from oslo_utils import timeutils
import six
import voluptuous

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils

LOG = logging.getLogger(__name__)


class _Barrier(object):
    def __init__(self):
        self.cond = threading.Condition()
        self.owner = None


@contextlib.contextmanager
def _translate_failures():
    try:
        yield
    except (EnvironmentError, voluptuous.Invalid) as e:
        coordination.raise_with_cause(coordination.ToozError,
                                      encodeutils.exception_to_unicode(e),
                                      cause=e)


_SCHEMAS = {
    'group': voluptuous.Schema({
        voluptuous.Required('group_id'): voluptuous.Any(six.text_type,
                                                        six.binary_type),
    }),
    'member': voluptuous.Schema({
        voluptuous.Required('member_id'): voluptuous.Any(six.text_type,
                                                         six.binary_type),
        voluptuous.Required('joined_on'): datetime.datetime,
    }, extra=voluptuous.ALLOW_EXTRA),
}


def _load_and_validate(blob, schema_key):
    data = utils.loads(blob)
    schema = _SCHEMAS[schema_key]
    schema(data)
    return data


def _lock_me(lock):

    def wrapper(func):

        @six.wraps(func)
        def decorator(*args, **kwargs):
            with lock:
                return func(*args, **kwargs)

        return decorator

    return wrapper


class FileLock(locking.Lock):
    """A file based lock."""

    def __init__(self, path, barrier, member_id):
        super(FileLock, self).__init__(path)
        self.acquired = False
        self._lock = fasteners.InterProcessLock(path)
        self._barrier = barrier
        self._member_id = member_id

    def is_still_owner(self):
        return self.acquired

    def acquire(self, blocking=True):
        blocking, timeout = utils.convert_blocking(blocking)
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()

        # Make the shared barrier ours first.
        with self._barrier.cond:
            while self._barrier.owner is not None:
                if not blocking or watch.expired():
                    return False
                self._barrier.cond.wait(watch.leftover(return_none=True))
            self._barrier.owner = (threading.current_thread().ident,
                                   os.getpid(), self._member_id)

        # Ok at this point we are now working in a thread safe manner,
        # and now we can try to get the actual lock...
        gotten = False
        try:
            gotten = self._lock.acquire(
                blocking=blocking,
                # Since the barrier waiting may have
                # taken a long time, we have to use
                # the leftover (and not the original).
                timeout=watch.leftover(return_none=True))
        finally:
            # NOTE(harlowja): do this in a finally block to **ensure** that
            # we release the barrier if something bad happens...
            if not gotten:
                # Release the barrier to let someone else have a go at it...
                with self._barrier.cond:
                    self._barrier.owner = None
                    self._barrier.cond.notify_all()

        self.acquired = gotten
        return gotten

    def release(self):
        if not self.acquired:
            return False
        with self._barrier.cond:
            self.acquired = False
            self._barrier.owner = None
            self._barrier.cond.notify_all()
        return True

    def __del__(self):
        if self.acquired:
            LOG.warn("Unreleased lock %s garbage collected", self.name)


class FileDriver(coordination._RunWatchersMixin,
                 coordination.CoordinationDriver):
    """A file based driver.

    This driver uses files and directories (and associated file locks) to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.

    General recommendations/usage considerations:

    - It does **not** automatically delete members from
      groups of processes that have died, manual cleanup will be needed
      for those types of failures.

    - It is **not** distributed (or recommended to be used in those
      situations, so the developer using this should really take that into
      account when applying this driver in there app).
    """

    CHARACTERISTICS = (
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    HASH_ROUTINE = 'sha1'
    """This routine is used to hash a member (or group) id into a filesystem
       safe name that can be used for member lookup and group joining."""

    _barriers = weakref.WeakValueDictionary()
    """
    Barriers shared among all file driver locks, this is required
    since interprocess locking is not thread aware, so we must add the
    thread awareness on-top of it instead.
    """

    def __init__(self, member_id, parsed_url, options):
        """Initialize the file driver."""
        super(FileDriver, self).__init__()
        self._member_id = member_id
        self._dir = parsed_url.path
        self._executor = utils.ProxyExecutor.build("File", options)
        self._group_dir = os.path.join(self._dir, 'groups')
        self._driver_lock_path = os.path.join(self._dir, '.driver_lock')
        self._driver_lock = self._get_raw_lock(self._driver_lock_path,
                                               self._member_id)
        self._reserved_dirs = [self._dir, self._group_dir]
        self._reserved_paths = list(self._reserved_dirs)
        self._reserved_paths.append(self._driver_lock_path)
        self._joined_groups = set()
        self._safe_member_id = self._make_filesystem_safe(member_id)

    @classmethod
    def _get_raw_lock(cls, path, member_id):
        lock_barrier = cls._barriers.setdefault(path, _Barrier())
        return FileLock(path, lock_barrier, member_id)

    def get_lock(self, name):
        path = utils.safe_abs_path(self._dir, name.decode())
        if path in self._reserved_paths:
            raise ValueError("Unable to create a lock using"
                             " reserved path '%s' for lock"
                             " with name '%s'" % (path, name))
        return self._get_raw_lock(path, self._member_id)

    @classmethod
    def _make_filesystem_safe(cls, item):
        return hashlib.new(cls.HASH_ROUTINE, item).hexdigest()

    def _start(self):
        for a_dir in self._reserved_dirs:
            utils.ensure_tree(a_dir)
        self._executor.start()

    def _stop(self):
        while self._joined_groups:
            self.leave_group(self._joined_groups.pop())
        self._executor.stop()

    def create_group(self, group_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)
        group_meta_path = os.path.join(group_dir, '.metadata')

        @_lock_me(self._driver_lock)
        def _do_create_group():
            if os.path.isdir(group_dir):
                raise coordination.GroupAlreadyExist(group_id)
            else:
                details = {
                    'group_id': group_id,
                }
                details_blob = utils.dumps(details)
                utils.ensure_tree(group_dir)
                with open(group_meta_path, "wb") as fh:
                    fh.write(details_blob)

        fut = self._executor.submit(_do_create_group)
        return FileFutureResult(fut)

    def join_group(self, group_id, capabilities=b""):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)
        me_path = os.path.join(group_dir, "%s.raw" % self._safe_member_id)

        @_lock_me(self._driver_lock)
        def _do_join_group():
            if not os.path.isdir(group_dir):
                raise coordination.GroupNotCreated(group_id)
            if os.path.isfile(me_path):
                raise coordination.MemberAlreadyExist(group_id,
                                                      self._member_id)
            details = {
                'capabilities': capabilities,
                'joined_on': datetime.datetime.now(),
                'member_id': self._member_id,
            }
            details_blob = utils.dumps(details)
            with open(me_path, "wb") as fh:
                fh.write(details_blob)
            self._joined_groups.add(group_id)

        fut = self._executor.submit(_do_join_group)
        return FileFutureResult(fut)

    def leave_group(self, group_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)
        me_path = os.path.join(group_dir, "%s.raw" % self._safe_member_id)

        @_lock_me(self._driver_lock)
        def _do_leave_group():
            if not os.path.isdir(group_dir):
                raise coordination.GroupNotCreated(group_id)
            try:
                os.unlink(me_path)
            except EnvironmentError as e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    raise coordination.MemberNotJoined(group_id,
                                                       self._member_id)
            else:
                self._joined_groups.discard(group_id)

        fut = self._executor.submit(_do_leave_group)
        return FileFutureResult(fut)

    def get_members(self, group_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)

        def _read_member_id(path):
            with open(path, 'rb') as fh:
                details = _load_and_validate(fh.read(), 'member')
                return details['member_id']

        @_lock_me(self._driver_lock)
        def _do_get_members():
            if not os.path.isdir(group_dir):
                raise coordination.GroupNotCreated(group_id)
            members = []
            try:
                entries = os.listdir(group_dir)
            except EnvironmentError as e:
                # Did someone manage to remove it before we got here...
                if e.errno != errno.ENOENT:
                    raise
            else:
                for entry in entries:
                    if not entry.endswith('.raw'):
                        continue
                    entry_path = os.path.join(group_dir, entry)
                    try:
                        member_id = _read_member_id(entry_path)
                    except EnvironmentError as e:
                        if e.errno != errno.ENOENT:
                            raise
                    else:
                        members.append(member_id)
            return members

        fut = self._executor.submit(_do_get_members)
        return FileFutureResult(fut)

    def get_member_capabilities(self, group_id, member_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)
        safe_member_id = self._make_filesystem_safe(member_id)
        member_path = os.path.join(group_dir, "%s.raw" % safe_member_id)

        @_lock_me(self._driver_lock)
        def _do_get_member_capabilities():
            try:
                with open(member_path, "rb") as fh:
                    contents = fh.read()
            except EnvironmentError as e:
                if e.errno == errno.ENOENT:
                    if not os.path.isdir(group_dir):
                        raise coordination.GroupNotCreated(group_id)
                    else:
                        raise coordination.MemberNotJoined(group_id,
                                                           member_id)
                else:
                    raise
            else:
                details = _load_and_validate(contents, 'member')
                return details.get("capabilities")

        fut = self._executor.submit(_do_get_member_capabilities)
        return FileFutureResult(fut)

    def delete_group(self, group_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)

        @_lock_me(self._driver_lock)
        def _do_delete_group():
            try:
                entries = os.listdir(group_dir)
            except EnvironmentError as e:
                if e.errno == errno.ENOENT:
                    raise coordination.GroupNotCreated(group_id)
                else:
                    raise
            else:
                if len(entries) > 1:
                    raise coordination.GroupNotEmpty(group_id)
                else:
                    try:
                        shutil.rmtree(group_dir)
                    except EnvironmentError as e:
                        if e.errno != errno.ENOENT:
                            raise

        fut = self._executor.submit(_do_delete_group)
        return FileFutureResult(fut)

    def get_groups(self):

        def _read_group_id(path):
            with open(path, 'rb') as fh:
                details = _load_and_validate(fh.read(), 'group')
                return details['group_id']

        @_lock_me(self._driver_lock)
        def _do_get_groups():
            groups = []
            for entry in os.listdir(self._group_dir):
                path = os.path.join(self._group_dir, entry, '.metadata')
                try:
                    groups.append(_read_group_id(path))
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        raise
            return groups

        fut = self._executor.submit(_do_get_groups)
        return FileFutureResult(fut)

    def _init_watch_group(self, group_id):
        group_members_fut = self.get_members(group_id)
        group_members = group_members_fut.get(timeout=None)
        self._group_members[group_id].update(group_members)

    def watch_join_group(self, group_id, callback):
        self._init_watch_group(group_id)
        return super(FileDriver, self).watch_join_group(group_id, callback)

    def unwatch_join_group(self, group_id, callback):
        return super(FileDriver, self).unwatch_join_group(group_id, callback)

    def watch_leave_group(self, group_id, callback):
        self._init_watch_group(group_id)
        return super(FileDriver, self).watch_leave_group(group_id, callback)

    def unwatch_leave_group(self, group_id, callback):
        return super(FileDriver, self).unwatch_leave_group(group_id, callback)

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented


class FileFutureResult(coordination.CoordAsyncResult):
    """File asynchronous result that references a future."""

    def __init__(self, fut):
        self._fut = fut

    def get(self, timeout=10):
        try:
            # Late translate the common failures since the file driver
            # may throw things that we can not catch in the callbacks where
            # it is used.
            with _translate_failures():
                return self._fut.result(timeout=timeout)
        except futures.TimeoutError as e:
            coordination.raise_with_cause(coordination.OperationTimedOut,
                                          encodeutils.exception_to_unicode(e),
                                          cause=e)

    def done(self):
        return self._fut.done()
