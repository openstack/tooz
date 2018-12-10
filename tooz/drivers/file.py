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
import functools
import hashlib
import logging
import os
import re
import shutil
import sys
import tempfile
import threading
import weakref

import fasteners
from oslo_utils import encodeutils
from oslo_utils import fileutils
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
        self.shared = False
        self.ref = 0


@contextlib.contextmanager
def _translate_failures():
    try:
        yield
    except (EnvironmentError, voluptuous.Invalid) as e:
        utils.raise_with_cause(tooz.ToozError,
                               encodeutils.exception_to_unicode(e),
                               cause=e)


def _convert_from_old_format(data):
    # NOTE(sileht): previous version of the driver was storing str as-is
    # making impossible to read from python3 something written with python2
    # version of the lib.
    # Now everything is stored with explicit type bytes or unicode. This
    # convert the old format to the new one to maintain compat of already
    # deployed file.
    # example of potential old python2 payload:
    # {b"member_id": b"member"}
    # {b"member_id": u"member"}
    # example of potential old python3 payload:
    # {u"member_id": b"member"}
    # {u"member_id": u"member"}
    if six.PY3 and b"member_id" in data or b"group_id" in data:
        data = dict((k.decode("utf8"), v) for k, v in data.items())
        # About member_id and group_id valuse if the file have been written
        # with python2 and in the old format, we can't known with python3
        # if we need to decode the value or not. Python3 see bytes blob
        # We keep it as-is and pray, this have a good change to break if
        # the application was using str in python2 and unicode in python3
        # The member file is often overridden so it's should be fine
        # But the group file can be very old, so we
        # now have to update it each time create_group is called
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
        self.ref = 0

    def is_still_owner(self):
        return self.acquired

    def acquire(self, blocking=True, shared=False):
        blocking, timeout = utils.convert_blocking(blocking)
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()

        # Make the shared barrier ours first.
        with self._barrier.cond:
            while self._barrier.owner is not None:
                if (shared and self._barrier.shared):
                    break
                if not blocking or watch.expired():
                    return False
                self._barrier.cond.wait(watch.leftover(return_none=True))
            self._barrier.owner = (threading.current_thread().ident,
                                   os.getpid(), self._member_id)
            self._barrier.shared = shared
            self._barrier.ref += 1
            self.ref += 1

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
                    self._barrier.ref = 0
                    self._barrier.shared = False
                    self._barrier.cond.notify_all()

        self.acquired = gotten
        return gotten

    def release(self):
        if not self.acquired:
            return False
        with self._barrier.cond:
            self._barrier.ref -= 1
            self.ref -= 1
            if not self.ref:
                self.acquired = False
            if not self._barrier.ref:
                self._barrier.owner = None
                self._lock.release()
                self._barrier.cond.notify_all()
        return True

    def __del__(self):
        if self.acquired:
            LOG.warning("Unreleased lock %s garbage collected", self.name)


class FileDriver(coordination.CoordinationDriverCachedRunWatchers,
                 coordination.CoordinationDriverWithExecutor):
    """A file based driver.

    This driver uses files and directories (and associated file locks) to
    provide the coordination driver semantics and required API(s). It **is**
    missing some functionality but in the future these not implemented API(s)
    will be filled in.

    The File driver connection URI should look like::

      file://DIRECTORY[?timeout=TIMEOUT]

    DIRECTORY is the location that should be used to store lock files.
    TIMEOUT defaults to 10.

    General recommendations/usage considerations:

    - It does **not** automatically delete members from
      groups of processes that have died, manual cleanup will be needed
      for those types of failures.

    - It is **not** distributed (or recommended to be used in those
      situations, so the developer using this should really take that into
      account when applying this driver in there app).
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
        super(FileDriver, self).__init__(member_id, parsed_url, options)
        self._dir = self._normalize_path(parsed_url.path)
        self._group_dir = os.path.join(self._dir, 'groups')
        self._tmpdir = os.path.join(self._dir, 'tmp')
        self._driver_lock_path = os.path.join(self._dir, '.driver_lock')
        self._driver_lock = self._get_raw_lock(self._driver_lock_path,
                                               self._member_id)
        self._reserved_dirs = [self._dir, self._group_dir, self._tmpdir]
        self._reserved_paths = list(self._reserved_dirs)
        self._reserved_paths.append(self._driver_lock_path)
        self._safe_member_id = self._make_filesystem_safe(member_id)
        self._timeout = int(self._options.get('timeout', 10))

    @staticmethod
    def _normalize_path(path):
        if sys.platform == 'win32':
            # Replace slashes with backslashes and make sure we don't
            # have any at the beginning of paths that include drive letters.
            #
            # Expected url format:
            # file:////share_address/share_name
            # file:///C:/path
            return re.sub(r'\\(?=\w:\\)', '',
                          os.path.normpath(path))
        return path

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
        item = utils.to_binary(item, encoding="utf8")
        return hashlib.new(cls.HASH_ROUTINE, item).hexdigest()

    def _start(self):
        super(FileDriver, self)._start()
        for a_dir in self._reserved_dirs:
            try:
                fileutils.ensure_tree(a_dir)
            except OSError as e:
                raise coordination.ToozConnectionError(e)

    def _update_group_metadata(self, path, group_id):
        details = {
            u'group_id': utils.to_binary(group_id, encoding="utf8")
        }
        details[u'encoded'] = details[u"group_id"] != group_id
        details_blob = utils.dumps(details)
        fd, name = tempfile.mkstemp("tooz", dir=self._tmpdir)
        with os.fdopen(fd, "wb") as fh:
            fh.write(details_blob)
        os.rename(name, path)

    def create_group(self, group_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)
        group_meta_path = os.path.join(group_dir, '.metadata')

        def _do_create_group():
            if os.path.exists(os.path.join(group_dir, ".metadata")):
                # NOTE(sileht): We update the group metadata even
                # they are already good, so ensure dict key are convert
                # to unicode in case of the file have been written with
                # tooz < 1.36
                self._update_group_metadata(group_meta_path, group_id)
                raise coordination.GroupAlreadyExist(group_id)
            else:
                fileutils.ensure_tree(group_dir)
                self._update_group_metadata(group_meta_path, group_id)
        fut = self._executor.submit(_do_create_group)
        return FileFutureResult(fut)

    def join_group(self, group_id, capabilities=b""):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)
        me_path = os.path.join(group_dir, "%s.raw" % self._safe_member_id)

        @_lock_me(self._driver_lock)
        def _do_join_group():
            if not os.path.exists(os.path.join(group_dir, ".metadata")):
                raise coordination.GroupNotCreated(group_id)
            if os.path.isfile(me_path):
                raise coordination.MemberAlreadyExist(group_id,
                                                      self._member_id)
            details = {
                u'capabilities': capabilities,
                u'joined_on': datetime.datetime.now(),
                u'member_id': utils.to_binary(self._member_id,
                                              encoding="utf-8")
            }
            details[u'encoded'] = details[u"member_id"] != self._member_id
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
            if not os.path.exists(os.path.join(group_dir, ".metadata")):
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

    _SCHEMAS = {
        'group': voluptuous.Schema({
            voluptuous.Required('group_id'): voluptuous.Any(six.text_type,
                                                            six.binary_type),
            # NOTE(sileht): tooz <1.36 was creating file without this
            voluptuous.Optional('encoded'): bool,
        }),
        'member': voluptuous.Schema({
            voluptuous.Required('member_id'): voluptuous.Any(six.text_type,
                                                             six.binary_type),
            voluptuous.Required('joined_on'): datetime.datetime,
            # NOTE(sileht): tooz <1.36 was creating file without this
            voluptuous.Optional('encoded'): bool,
        }, extra=voluptuous.ALLOW_EXTRA),
    }

    def _load_and_validate(self, blob, schema_key):
        data = utils.loads(blob)
        data = _convert_from_old_format(data)
        schema = self._SCHEMAS[schema_key]
        return schema(data)

    def _read_member_id(self, path):
        with open(path, 'rb') as fh:
            details = self._load_and_validate(fh.read(), 'member')
            if details.get("encoded"):
                return details[u'member_id'].decode("utf-8")
            return details[u'member_id']

    def get_members(self, group_id):
        safe_group_id = self._make_filesystem_safe(group_id)
        group_dir = os.path.join(self._group_dir, safe_group_id)

        @_lock_me(self._driver_lock)
        def _do_get_members():
            if not os.path.isdir(group_dir):
                raise coordination.GroupNotCreated(group_id)
            members = set()
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
                        m_time = datetime.datetime.fromtimestamp(
                            os.stat(entry_path).st_mtime)
                        current_time = datetime.datetime.now()
                        delta_time = timeutils.delta_seconds(m_time,
                                                             current_time)
                        if delta_time >= 0 and delta_time <= self._timeout:
                            member_id = self._read_member_id(entry_path)
                        else:
                            continue
                    except EnvironmentError as e:
                        if e.errno != errno.ENOENT:
                            raise
                    else:
                        members.add(member_id)
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
                details = self._load_and_validate(contents, 'member')
                return details.get(u"capabilities")

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
                elif len(entries) == 1 and entries != ['.metadata']:
                    raise tooz.ToozError(
                        "Unexpected path '%s' found in"
                        " group directory '%s' (expected to only find"
                        " a '.metadata' path)" % (entries[0], group_dir))
                else:
                    try:
                        shutil.rmtree(group_dir)
                    except EnvironmentError as e:
                        if e.errno != errno.ENOENT:
                            raise

        fut = self._executor.submit(_do_delete_group)
        return FileFutureResult(fut)

    def _read_group_id(self, path):
        with open(path, 'rb') as fh:
            details = self._load_and_validate(fh.read(), 'group')
            if details.get("encoded"):
                return details[u'group_id'].decode("utf-8")
            return details[u'group_id']

    def get_groups(self):

        def _do_get_groups():
            groups = []
            for entry in os.listdir(self._group_dir):
                path = os.path.join(self._group_dir, entry, '.metadata')
                try:
                    groups.append(self._read_group_id(path))
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        raise
            return groups

        fut = self._executor.submit(_do_get_groups)
        return FileFutureResult(fut)

    def heartbeat(self):
        for group_id in self._joined_groups:
            safe_group_id = self._make_filesystem_safe(group_id)
            group_dir = os.path.join(self._group_dir, safe_group_id)
            member_path = os.path.join(group_dir, "%s.raw" %
                                       self._safe_member_id)

            @_lock_me(self._driver_lock)
            def _do_heartbeat():
                try:
                    os.utime(member_path, None)
                except EnvironmentError as err:
                    if err.errno != errno.ENOENT:
                        raise
            _do_heartbeat()
        return self._timeout

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented


FileFutureResult = functools.partial(coordination.CoordinatorResult,
                                     failure_translator=_translate_failures)
