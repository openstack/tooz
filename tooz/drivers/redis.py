# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from __future__ import absolute_import

import contextlib
from distutils import version
import logging

try:
    from time import monotonic as _now  # noqa
except ImportError:
    from time import time as _now  # noqa

from concurrent import futures
from oslo_utils import strutils
import redis
from redis import exceptions
from redis import lock as redis_locks
from redis import sentinel
import six
from six.moves import map as compat_map
from six.moves import zip as compat_zip

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils

LOG = logging.getLogger(__name__)


def _version_checker(version_required):
    """Checks server version is supported *before* running decorated method."""

    if isinstance(version_required, six.string_types):
        desired_version = version.LooseVersion(version_required)
    elif isinstance(version_required, version.LooseVersion):
        desired_version = version_required
    else:
        raise TypeError("Version decorator expects a string/version type")

    def wrapper(meth):

        @six.wraps(meth)
        def decorator(self, *args, **kwargs):
            useable, redis_version = self._check_fetch_redis_version(
                desired_version)
            if not useable:
                raise tooz.NotImplemented("Redis version greater than or"
                                          " equal to '%s' is required"
                                          " to use this feature; '%s' is"
                                          " being used which is not new"
                                          " enough" % (desired_version,
                                                       redis_version))
            return meth(self, *args, **kwargs)

        return decorator

    return wrapper


@contextlib.contextmanager
def _translate_failures():
    """Translates common redis exceptions into tooz exceptions."""
    try:
        yield
    except (exceptions.ConnectionError, exceptions.TimeoutError) as e:
        raise coordination.ToozConnectionError(utils.exception_message(e))
    except exceptions.RedisError as e:
        raise coordination.ToozError(utils.exception_message(e))


class RedisLock(locking.Lock):
    def __init__(self, coord, client, name, timeout):
        self._name = "%s_%s_lock" % (coord.namespace, six.text_type(name))
        # Avoid using lua locks to keep compatible with more versions
        # of redis (and not just the ones that have lua support, also avoids
        # ones that don't appear to have fully working lua support...)
        #
        # Issue opened: https://github.com/andymccurdy/redis-py/issues/550
        #
        # When that gets fixed (and detects the servers capabilities better
        # we can likely turn this back on to being smart).
        self._lock = redis_locks.Lock(client, self._name, timeout=timeout)
        self._coord = coord
        self._acquired = False

    @property
    def name(self):
        return self._name

    def acquire(self, blocking=True):
        if blocking is True or blocking is False:
            blocking_timeout = None
        else:
            blocking_timeout = float(blocking)
            blocking = True
        with _translate_failures():
            self._acquired = self._lock.acquire(
                blocking=blocking, blocking_timeout=blocking_timeout)
            if self._acquired:
                self._coord._acquired_locks.add(self)
            return self._acquired

    def release(self):
        if not self._acquired:
            return False
        with _translate_failures():
            try:
                self._lock.release()
            except exceptions.LockError:
                return False
            self._coord._acquired_locks.discard(self)
            self._acquired = False
            return True

    def heartbeat(self):
        if self._acquired:
            with _translate_failures():
                self._lock.extend(self._lock.timeout)


class RedisDriver(coordination.CoordinationDriver):
    """Redis provides a few nice benefits that act as a poormans zookeeper.

    - Durability (when setup with AOF mode).
    - Consistent, note that this is still restricted to only
      one redis server, without the recently released redis (alpha)
      clustering > 1 server will not be consistent when partitions
      or failures occur (even redis clustering docs state it is
      not a fully AP or CP solution, which means even with it there
      will still be *potential* inconsistencies).
    - Master/slave failover (when setup with redis sentinel), giving
      some notion of HA (values *can* be lost when a failover transition
      occurs).

    To use a sentinel the connection URI must point to the Sentinel server.
    At connection time the sentinel will be asked for the current IP and port
    of the master and then connect there. The connection URI for sentinel
    should be written as follows::

      redis://<sentinel host>:<sentinel port>?sentinel=<master name>

    Additional sentinel hosts are listed with mutiple ``sentinel_fallback``
    parameters as follows:

      redis://<sentinel host>:<sentinel port>?sentinel=<master name>&
        sentinel_fallback=<other sentinel host>:<sentinel port>&
        sentinel_fallback=<other sentinel host>:<sentinel port>&
        sentinel_fallback=<other sentinel host>:<sentinel port>

    Further resources/links:

    - http://redis.io/
    - http://redis.io/topics/sentinel
    - http://redis.io/topics/cluster-spec

    Note that this client will itself retry on transaction failure (when they
    keys being watched have changed underneath the current transaction).
    Currently the number of attempts that are tried is infinite (this might
    be addressed in https://github.com/andymccurdy/redis-py/issues/566 when
    that gets worked on). See http://redis.io/topics/transactions for more
    information on this topic.
    """

    # The min redis version that this driver requires to operate with...
    #
    # NOTE(harlowja): 2.2.0 was selected since its the current version that
    # exists on ubuntu precise, that version will work to a degree (except
    # locks will not work with that version), in the future we can raise this
    # as we move off of precise (and/or need newer features that the older
    # versions of redis just don't have).
    _MIN_VERSION = version.LooseVersion("2.2.0")

    # Redis deletes dictionaries that have no keys in them, which means the
    # key will disappear which means we can't tell the difference between
    # a group not existing and a group being empty without this key being
    # saved...
    _GROUP_EXISTS = b'__created__'
    _NAMESPACE_SEP = b':'

    # This is for python3.x; which will behave differently when returned
    # binary types or unicode types (redis uses binary internally it appears),
    # so to just stick with a common way of doing this, make all the things
    # binary (with this default encoding if one is not given and a unicode
    # string is provided).
    _DEFAULT_ENCODING = 'utf8'

    # These are used when extracting options from to make a client.
    #
    # See: http://redis-py.readthedocs.org/en/latest/ for how to use these
    # options to configure the underlying redis client...
    _CLIENT_ARGS = frozenset([
        'db',
        'encoding',
        'retry_on_timeout',
        'socket_keepalive',
        'socket_timeout',
        'ssl',
        'ssl_certfile',
        'ssl_keyfile',
        'sentinel',
        'sentinel_fallback',
    ])
    _CLIENT_LIST_ARGS = frozenset([
        'sentinel_fallback',
    ])
    _CLIENT_BOOL_ARGS = frozenset([
        'retry_on_timeout',
        'ssl',
    ])
    _CLIENT_INT_ARGS = frozenset([
         'db',
         'socket_keepalive',
         'socket_timeout',
    ])
    _CLIENT_DEFAULT_SOCKET_TO = 30

    def __init__(self, member_id, parsed_url, options):
        super(RedisDriver, self).__init__()
        self._parsed_url = parsed_url
        self._options = options
        encoding = options.get('encoding', [self._DEFAULT_ENCODING])
        self._encoding = encoding[-1]
        timeout = options.get('timeout', [self._CLIENT_DEFAULT_SOCKET_TO])
        self.timeout = int(timeout[-1])
        self.membership_timeout = float(options.get(
            'membership_timeout', timeout)[-1])
        lock_timeout = options.get('lock_timeout', [self.timeout])
        self.lock_timeout = int(lock_timeout[-1])
        namespace = options.get('namespace', ['_tooz'])[-1]
        self._namespace = self._to_binary(namespace)
        self._group_prefix = self._namespace + b"_group"
        self._leader_prefix = self._namespace + b"_leader"
        self._beat_prefix = self._namespace + b"_beats"
        self._groups = self._namespace + b"_groups"
        self._client = None
        self._member_id = self._to_binary(member_id)
        self._acquired_locks = set()
        self._joined_groups = set()
        self._executor = None
        self._started = False
        self._server_info = {}

    def _check_fetch_redis_version(self, geq_version, not_existent=True):
        if isinstance(geq_version, six.string_types):
            desired_version = version.LooseVersion(geq_version)
        elif isinstance(geq_version, version.LooseVersion):
            desired_version = geq_version
        else:
            raise TypeError("Version check expects a string/version type")
        try:
            redis_version = version.LooseVersion(
                    self._server_info['redis_version'])
        except KeyError:
            return (not_existent, None)
        else:
            if redis_version < desired_version:
                return (False, redis_version)
            else:
                return (True, redis_version)

    def _to_binary(self, text):
        if not isinstance(text, six.binary_type):
            text = text.encode(self._encoding)
        return text

    @property
    def namespace(self):
        return self._namespace

    @property
    def running(self):
        return self._started

    # 2.6.0 is required since internally PEXPIRE is used and that was only
    # added in 2.6.0; so avoid using this on versions less than that...
    @_version_checker("2.6.0")
    def get_lock(self, name):
        return RedisLock(self, self._client, name, self.lock_timeout)

    @staticmethod
    def _dumps(data):
        return utils.dumps(data)

    @staticmethod
    def _loads(blob):
        return utils.loads(blob)

    @classmethod
    def _make_client(cls, parsed_url, options, default_socket_timeout):
        kwargs = {}
        if parsed_url.hostname:
            kwargs['host'] = parsed_url.hostname
            if parsed_url.port:
                kwargs['port'] = parsed_url.port
        else:
            if not parsed_url.path:
                raise ValueError("Expected socket path in parsed urls path")
            kwargs['unix_socket_path'] = parsed_url.path
        if parsed_url.password:
            kwargs['password'] = parsed_url.password
        for a in cls._CLIENT_ARGS:
            if a not in options:
                continue
            # The reason the last index is used is that when multiple options
            # of the same name are given via a url the values will be
            # accumulated in a list (and not just be a single value)...
            #
            # For ex: the following is a valid url which will have 2 values
            # for the 'timeout' argument:
            #
            # redis://localhost:6379?timeout=5&timeout=2
            if a in cls._CLIENT_BOOL_ARGS:
                v = strutils.bool_from_string(options[a][-1])
            elif a in cls._CLIENT_LIST_ARGS:
                v = options[a]
            elif a in cls._CLIENT_INT_ARGS:
                v = int(options[a][-1])
            else:
                v = options[a][-1]
            kwargs[a] = v
        if 'socket_timeout' not in kwargs:
            kwargs['socket_timeout'] = default_socket_timeout

        # Ask the sentinel for the current master if there is a
        # sentinel arg.
        if 'sentinel' in kwargs:
            sentinel_hosts = [
                tuple(fallback.split(':'))
                for fallback in kwargs.get('sentinel_fallback', [])
            ]
            sentinel_hosts.insert(0, (kwargs['host'], kwargs['port']))
            sentinel_server = sentinel.Sentinel(
                sentinel_hosts,
                socket_timeout=kwargs['socket_timeout'])
            sentinel_name = kwargs['sentinel']
            del kwargs['sentinel']
            if 'sentinel_fallback' in kwargs:
                del kwargs['sentinel_fallback']
            master_client = sentinel_server.master_for(sentinel_name, **kwargs)
            # The master_client is a redis.StrictRedis using a
            # Sentinel managed connection pool.
            return master_client
        return redis.StrictRedis(**kwargs)

    def _start(self):
        self._executor = futures.ThreadPoolExecutor(max_workers=1)
        try:
            self._client = self._make_client(self._parsed_url, self._options,
                                             self.timeout)
        except exceptions.RedisError as e:
            raise coordination.ToozConnectionError(utils.exception_message(e))
        else:
            # Ensure that the server is alive and not dead, this does not
            # ensure the server will always be alive, but does insure that it
            # at least is alive once...
            with _translate_failures():
                self._server_info = self._client.info()
            # Validate we have a good enough redis version we are connected
            # to so that the basic set of features we support will actually
            # work (instead of blowing up).
            new_enough, redis_version = self._check_fetch_redis_version(
                self._MIN_VERSION)
            if not new_enough:
                raise tooz.NotImplemented("Redis version greater than or"
                                          " equal to '%s' is required"
                                          " to use this driver; '%s' is"
                                          " being used which is not new"
                                          " enough" % (self._MIN_VERSION,
                                                       redis_version))
            self.heartbeat()
            self._started = True

    def _encode_beat_id(self, member_id):
        return self._NAMESPACE_SEP.join([self._beat_prefix,
                                         self._to_binary(member_id)])

    def _encode_member_id(self, member_id):
        member_id = self._to_binary(member_id)
        if member_id == self._GROUP_EXISTS:
            raise ValueError("Not allowed to use private keys as a member id")
        return member_id

    def _decode_member_id(self, member_id):
        return self._to_binary(member_id)

    def _encode_group_id(self, group_id, apply_namespace=True):
        group_id = self._to_binary(group_id)
        if not apply_namespace:
            return group_id
        return self._NAMESPACE_SEP.join([self._group_prefix, group_id])

    def _decode_group_id(self, group_id):
        return self._to_binary(group_id)

    def heartbeat(self):
        with _translate_failures():
            beat_id = self._encode_beat_id(self._member_id)
            # Use milliseconds if we can (which are more accurate than
            # just seconds); but this PSETEX support was added in 2.6.0 or
            # newer so we can only use it then...
            supports_psetex, _version = self._check_fetch_redis_version(
                '2.6.0', not_existent=False)
            if not supports_psetex:
                expiry_secs = max(0, int(self.membership_timeout))
                self._client.setex(beat_id, time=expiry_secs,
                                   value=b"Not dead!")
            else:
                expiry_ms = max(0, int(self.membership_timeout * 1000.0))
                self._client.psetex(beat_id, time_ms=expiry_ms,
                                    value=b"Not dead!")
        for lock in self._acquired_locks:
            try:
                lock.heartbeat()
            except coordination.ToozError:
                LOG.warning("Unable to heartbeat lock '%s'", lock,
                            exc_info=True)

    def _stop(self):
        while self._acquired_locks:
            lock = self._acquired_locks.pop()
            try:
                lock.release()
            except coordination.ToozError:
                LOG.warning("Unable to release lock '%s'", lock, exc_info=True)
        while self._joined_groups:
            group_id = self._joined_groups.pop()
            try:
                self.leave_group(group_id).get()
            except (coordination.MemberNotJoined,
                    coordination.GroupNotCreated):
                pass
            except coordination.ToozError:
                LOG.warning("Unable to leave group '%s'", group_id,
                            exc_info=True)
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        if self._client is not None:
            # Make sure we no longer exist...
            beat_id = self._encode_beat_id(self._member_id)
            try:
                # NOTE(harlowja): this will delete nothing if the key doesn't
                # exist in the first place, which is fine/expected/desired...
                with _translate_failures():
                    self._client.delete(beat_id)
            except coordination.ToozError:
                LOG.warning("Unable to delete heartbeat key '%s'", beat_id,
                            exc_info=True)
            self._client = None
        self._server_info = {}
        self._started = False

    def _submit(self, cb, *args, **kwargs):
        if not self._started:
            raise coordination.ToozError("Redis driver has not been started")
        try:
            return self._executor.submit(cb, *args, **kwargs)
        except RuntimeError:
            raise coordination.ToozError("Redis driver asynchronous executor"
                                         " has been shutdown")

    def create_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        def _create_group(p):
            if p.exists(encoded_group):
                raise coordination.GroupAlreadyExist(group_id)
            p.sadd(self._groups,
                   self._encode_group_id(group_id, apply_namespace=False))
            # Add our special key to avoid redis from deleting the dictionary
            # when it becomes empty (which is not what we currently want)...
            p.hset(encoded_group, self._GROUP_EXISTS, '1')

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _create_group, encoded_group,
                                              self._groups,
                                              value_from_callable=True))

    def update_capabilities(self, group_id, capabilities):
        encoded_group = self._encode_group_id(group_id)
        encoded_member_id = self._encode_member_id(self._member_id)

        def _update_capabilities(p):
            if not p.exists(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            if not p.hexists(encoded_group, encoded_member_id):
                raise coordination.MemberNotJoined(group_id, self._member_id)
            else:
                p.hset(encoded_group, encoded_member_id,
                       self._dumps(capabilities))

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _update_capabilities,
                                              encoded_group,
                                              value_from_callable=True))

    def leave_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)
        encoded_member_id = self._encode_member_id(self._member_id)

        def _leave_group(p):
            if not p.exists(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            c = p.hdel(encoded_group, encoded_member_id)
            if c == 0:
                raise coordination.MemberNotJoined(group_id, self._member_id)
            else:
                self._joined_groups.discard(group_id)

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _leave_group, encoded_group,
                                              value_from_callable=True))

    def get_members(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        def _get_members(p):
            if not p.exists(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            potential_members = []
            for m in p.hkeys(encoded_group):
                m = self._decode_member_id(m)
                if m != self._GROUP_EXISTS:
                    potential_members.append(m)
            if not potential_members:
                return []
            # Ok now we need to see which members have passed away...
            gone_members = set()
            member_values = p.mget(compat_map(self._encode_beat_id,
                                              potential_members))
            for (potential_member, value) in compat_zip(potential_members,
                                                        member_values):
                # Always preserve self (just incase we haven't heartbeated
                # while this call/s was being made...), this does *not* prevent
                # another client from removing this though...
                if potential_member == self._member_id:
                    continue
                if not value:
                    gone_members.add(potential_member)
            # Trash all the members that no longer are with us... RIP...
            if gone_members:
                many_at_once, _version = self._check_fetch_redis_version(
                    "2.4.0", not_existent=False)
                if not many_at_once:
                    for m in gone_members:
                        p.hdel(encoded_group, self._encode_member_id(m))
                else:
                    encoded_gone_members = [self._encode_member_id(m)
                                            for m in gone_members]
                    p.hdel(encoded_group, *encoded_gone_members)
                return [m for m in potential_members if m not in gone_members]
            else:
                return potential_members

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _get_members, encoded_group,
                                              value_from_callable=True))

    def get_member_capabilities(self, group_id, member_id):
        encoded_group = self._encode_group_id(group_id)
        encoded_member_id = self._encode_member_id(member_id)

        def _get_member_capabilities(p):
            if not p.exists(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            capabilities = p.hget(encoded_group, encoded_member_id)
            if capabilities is None:
                raise coordination.MemberNotJoined(group_id, member_id)
            return self._loads(capabilities)

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _get_member_capabilities,
                                              encoded_group,
                                              value_from_callable=True))

    def join_group(self, group_id, capabilities=b""):
        encoded_group = self._encode_group_id(group_id)
        encoded_member_id = self._encode_member_id(self._member_id)

        def _join_group(p):
            if not p.exists(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            c = p.hset(encoded_group, encoded_member_id,
                       self._dumps(capabilities))
            if c == 0:
                # Field already exists...
                raise coordination.MemberAlreadyExist(group_id,
                                                      self._member_id)
            else:
                self._joined_groups.add(group_id)

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _join_group,
                                              encoded_group,
                                              value_from_callable=True))

    def delete_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)

        def _delete_group(p):
            # An empty group still have the special key _GROUP_EXISTS set, so
            # its len is 1
            if p.hlen(encoded_group) > 1:
                raise coordination.GroupNotEmpty(group_id)
            if not p.delete(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            p.srem(self._groups,
                   self._encode_group_id(group_id,
                                         apply_namespace=False))

        return RedisFutureResult(self._submit(self._client.transaction,
                                              _delete_group,
                                              encoded_group,
                                              value_from_callable=True))

    def _destroy_group(self, group_id):
        """Should only be used in tests..."""
        self._client.delete(self._encode_group_id(group_id))

    def get_groups(self):

        def _get_groups():
            results = []
            for g in self._client.smembers(self._groups):
                results.append(self._decode_group_id(g))
            return results

        return RedisFutureResult(self._submit(_get_groups))

    def _init_watch_group(self, group_id):
        members = self.get_members(group_id)
        self._group_members[group_id].update(members.get(timeout=None))

    def watch_join_group(self, group_id, callback):
        self._init_watch_group(group_id)
        return super(RedisDriver, self).watch_join_group(group_id, callback)

    def unwatch_join_group(self, group_id, callback):
        return super(RedisDriver, self).unwatch_join_group(group_id, callback)

    def watch_leave_group(self, group_id, callback):
        self._init_watch_group(group_id)
        return super(RedisDriver, self).watch_leave_group(group_id, callback)

    def unwatch_leave_group(self, group_id, callback):
        return super(RedisDriver, self).unwatch_leave_group(group_id, callback)

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    def run_watchers(self, timeout=None):
        if timeout is not None:
            started_at = _now()
        result = []
        for group_id in self.get_groups().get(timeout=timeout):
            leftover_timeout = None
            if timeout is not None:
                elapsed = max(0.0, _now() - started_at)
                leftover_timeout = timeout - elapsed
            try:
                group_members = self.get_members(group_id).get(
                    timeout=leftover_timeout)
            except coordination.GroupNotCreated:
                group_members = set()
            else:
                group_members = set(group_members)
            # I was booted out...
            #
            # TODO(harlowja): perhaps we should have a way to notify
            # watchers that this has happened (the below mechanism will
            # also do this, but it might be better to have a separate
            # way when 'self' membership is lost)?
            if (group_id in self._joined_groups and
                    self._member_id not in group_members):
                self._joined_groups.discard(group_id)
            old_group_members = self._group_members.get(group_id, set())
            for member_id in (group_members - old_group_members):
                result.extend(
                    self._hooks_join_group[group_id].run(
                        coordination.MemberJoinedGroup(group_id,
                                                       member_id)))
            for member_id in (old_group_members - group_members):
                result.extend(
                    self._hooks_leave_group[group_id].run(
                        coordination.MemberLeftGroup(group_id,
                                                     member_id)))
            self._group_members[group_id] = group_members
        return result


class RedisFutureResult(coordination.CoordAsyncResult):
    """Redis asynchronous result that references a future."""

    def __init__(self, fut):
        self._fut = fut

    def get(self, timeout=10):
        try:
            # Late translate the common failures since the redis client
            # may throw things that we can not catch in the callbacks where
            # it is used (especially one that uses the transaction
            # method).
            with _translate_failures():
                return self._fut.result(timeout=timeout)
        except futures.TimeoutError as e:
            raise coordination.OperationTimedOut(utils.exception_message(e))

    def done(self):
        return self._fut.done()
