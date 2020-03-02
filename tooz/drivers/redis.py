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
import functools
import logging
import string
import threading

from oslo_utils import encodeutils
from oslo_utils import strutils
import redis
from redis import exceptions
from redis import sentinel
import six
from six.moves import map as compat_map
from six.moves import zip as compat_zip

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils

LOG = logging.getLogger(__name__)


@contextlib.contextmanager
def _translate_failures():
    """Translates common redis exceptions into tooz exceptions."""
    try:
        yield
    except (exceptions.ConnectionError, exceptions.TimeoutError) as e:
        utils.raise_with_cause(coordination.ToozConnectionError,
                               encodeutils.exception_to_unicode(e),
                               cause=e)
    except exceptions.RedisError as e:
        utils.raise_with_cause(tooz.ToozError,
                               encodeutils.exception_to_unicode(e),
                               cause=e)


class RedisLock(locking.Lock):
    def __init__(self, coord, client, name, timeout):
        name = "%s_%s_lock" % (coord.namespace, six.text_type(name))
        super(RedisLock, self).__init__(name)
        # NOTE(jd) Make sure we don't release and heartbeat at the same time by
        # using a exclusive access lock (LP#1557593)
        self._exclusive_access = threading.Lock()
        self._lock = client.lock(name,
                                 timeout=timeout,
                                 thread_local=False)
        self._coord = coord
        self._client = client

    def is_still_owner(self):
        with _translate_failures():
            lock_tok = self._lock.local.token
            if not lock_tok:
                return False
            owner_tok = self._client.get(self.name)
            return owner_tok == lock_tok

    def break_(self):
        with _translate_failures():
            return bool(self._client.delete(self.name))

    def acquire(self, blocking=True, shared=False):
        if shared:
            raise tooz.NotImplemented
        blocking, timeout = utils.convert_blocking(blocking)
        with _translate_failures():
            acquired = self._lock.acquire(
                blocking=blocking, blocking_timeout=timeout)
            if acquired:
                with self._exclusive_access:
                    self._coord._acquired_locks.add(self)
            return acquired

    def release(self):
        with self._exclusive_access:
            with _translate_failures():
                try:
                    self._lock.release()
                except exceptions.LockError as e:
                    LOG.error("Unable to release lock '%r': %s", self, e)
                    return False
                finally:
                    self._coord._acquired_locks.discard(self)
                return True

    def heartbeat(self):
        with self._exclusive_access:
            if self.acquired:
                with _translate_failures():
                    self._lock.reacquire()
                    return True
        return False

    @property
    def acquired(self):
        return self in self._coord._acquired_locks


class RedisDriver(coordination.CoordinationDriverCachedRunWatchers,
                  coordination.CoordinationDriverWithExecutor):
    """Redis provides a few nice benefits that act as a poormans zookeeper.

    It **is** fully functional and implements all of the coordination
    driver API(s). It stores data into `redis`_ using the provided `redis`_
    API(s) using `msgpack`_ encoded values as needed.

    - Durability (when setup with `AOF`_ mode).
    - Consistent, note that this is still restricted to only
      one redis server, without the recently released redis (alpha)
      clustering > 1 server will not be consistent when partitions
      or failures occur (even redis clustering docs state it is
      not a fully AP or CP solution, which means even with it there
      will still be *potential* inconsistencies).
    - Master/slave failover (when setup with redis `sentinel`_), giving
      some notion of HA (values *can* be lost when a failover transition
      occurs).

    The Redis driver connection URI should look like::

      redis://[:PASSWORD@]HOST:PORT[?OPTION=VALUE[&OPTION2=VALUE2[&...]]]

    For a list of options recognized by this driver, see the documentation
    for the member CLIENT_ARGS, and to determine the expected types of those
    options see CLIENT_BOOL_ARGS, CLIENT_INT_ARGS, and CLIENT_LIST_ARGS.

    To use a `sentinel`_ the connection URI must point to the sentinel server.
    At connection time the sentinel will be asked for the current IP and port
    of the master and then connect there. The connection URI for sentinel
    should be written as follows::

      redis://<sentinel host>:<sentinel port>?sentinel=<master name>

    Additional sentinel hosts are listed with multiple ``sentinel_fallback``
    parameters as follows::

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

    General recommendations/usage considerations:

    - When used for locks, run in AOF mode and think carefully about how
      your redis deployment handles losing a server (the clustering support
      is supposed to aid in losing servers, but it is also of unknown
      reliablity and is relatively new, so use at your own risk).

    .. _redis: http://redis.io/
    .. _msgpack: http://msgpack.org/
    .. _sentinel: http://redis.io/topics/sentinel
    .. _AOF: http://redis.io/topics/persistence
    """

    CHARACTERISTICS = (
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
        coordination.Characteristics.DISTRIBUTED_ACROSS_HOSTS,
        coordination.Characteristics.CAUSAL,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    MIN_VERSION = version.LooseVersion("2.6.0")
    """
    The min redis version that this driver requires to operate with...
    """

    GROUP_EXISTS = b'__created__'
    """
    Redis deletes dictionaries that have no keys in them, which means the
    key will disappear which means we can't tell the difference between
    a group not existing and a group being empty without this key being
    saved...
    """

    #: Value used (with group exists key) to keep a group from disappearing.
    GROUP_EXISTS_VALUE = b'1'

    #: Default namespace for keys when none is provided.
    DEFAULT_NAMESPACE = b'_tooz'

    NAMESPACE_SEP = b':'
    """
    Separator that is used to combine a key with the namespace (to get
    the **actual** key that will be used).
    """

    DEFAULT_ENCODING = 'utf8'
    """
    This is for python3.x; which will behave differently when returned
    binary types or unicode types (redis uses binary internally it appears),
    so to just stick with a common way of doing this, make all the things
    binary (with this default encoding if one is not given and a unicode
    string is provided).
    """

    CLIENT_ARGS = frozenset([
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
    """
    Keys that we allow to proxy from the coordinator configuration into the
    redis client (used to configure the redis client internals so that
    it works as you expect/want it to).

    See: http://redis-py.readthedocs.org/en/latest/#redis.Redis

    See: https://github.com/andymccurdy/redis-py/blob/2.10.3/redis/client.py
    """

    #: Client arguments that are expected/allowed to be lists.
    CLIENT_LIST_ARGS = frozenset([
        'sentinel_fallback',
    ])

    #: Client arguments that are expected to be boolean convertible.
    CLIENT_BOOL_ARGS = frozenset([
        'retry_on_timeout',
        'ssl',
    ])

    #: Client arguments that are expected to be int convertible.
    CLIENT_INT_ARGS = frozenset([
        'db',
        'socket_keepalive',
        'socket_timeout',
    ])

    #: Default socket timeout to use when none is provided.
    CLIENT_DEFAULT_SOCKET_TO = 30

    #: String used to keep a key/member alive (until it next expires).
    STILL_ALIVE = b"Not dead!"

    SCRIPTS = {
        'create_group': """
-- Extract *all* the variables (so we can easily know what they are)...
local namespaced_group_key = KEYS[1]
local all_groups_key = KEYS[2]
local no_namespaced_group_key = ARGV[1]
if redis.call("exists", namespaced_group_key) == 1 then
    return 0
end
redis.call("sadd", all_groups_key, no_namespaced_group_key)
redis.call("hset", namespaced_group_key,
           "${group_existence_key}", "${group_existence_value}")
return 1
""",
        'delete_group': """
-- Extract *all* the variables (so we can easily know what they are)...
local namespaced_group_key = KEYS[1]
local all_groups_key = KEYS[2]
local no_namespaced_group_key = ARGV[1]
if redis.call("exists", namespaced_group_key) == 0 then
    return -1
end
if redis.call("sismember", all_groups_key, no_namespaced_group_key) == 0 then
    return -2
end
if redis.call("hlen", namespaced_group_key) > 1 then
    return -3
end
-- First remove from the set (then delete the group); if the set removal
-- fails, at least the group will still exist (and can be fixed manually)...
if redis.call("srem", all_groups_key, no_namespaced_group_key) == 0 then
    return -4
end
redis.call("del", namespaced_group_key)
return 1
""",
        'update_capabilities': """
-- Extract *all* the variables (so we can easily know what they are)...
local group_key = KEYS[1]
local member_id = ARGV[1]
local caps = ARGV[2]
if redis.call("exists", group_key) == 0 then
    return -1
end
if redis.call("hexists", group_key, member_id) == 0 then
    return -2
end
redis.call("hset", group_key, member_id, caps)
return 1
""",
    }
    """`Lua`_ **template** scripts that will be used by various methods (they
    are turned into real scripts and loaded on call into the :func:`.start`
    method).

    .. _Lua: http://www.lua.org
    """

    EXCLUDE_OPTIONS = CLIENT_LIST_ARGS

    def __init__(self, member_id, parsed_url, options):
        super(RedisDriver, self).__init__(member_id, parsed_url, options)
        self._parsed_url = parsed_url
        self._encoding = self._options.get('encoding', self.DEFAULT_ENCODING)
        timeout = self._options.get('timeout', self.CLIENT_DEFAULT_SOCKET_TO)
        self.timeout = int(timeout)
        self.membership_timeout = float(self._options.get(
            'membership_timeout', timeout))
        lock_timeout = self._options.get('lock_timeout', self.timeout)
        self.lock_timeout = int(lock_timeout)
        namespace = self._options.get('namespace', self.DEFAULT_NAMESPACE)
        self._namespace = utils.to_binary(namespace, encoding=self._encoding)
        self._group_prefix = self._namespace + b"_group"
        self._beat_prefix = self._namespace + b"_beats"
        self._groups = self._namespace + b"_groups"
        self._client = None
        self._acquired_locks = set()
        self._started = False
        self._server_info = {}
        self._scripts = {}

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

    @property
    def namespace(self):
        return self._namespace

    @property
    def running(self):
        return self._started

    def get_lock(self, name):
        return RedisLock(self, self._client, name, self.lock_timeout)

    _dumps = staticmethod(utils.dumps)
    _loads = staticmethod(utils.loads)

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
        for a in cls.CLIENT_ARGS:
            if a not in options:
                continue
            if a in cls.CLIENT_BOOL_ARGS:
                v = strutils.bool_from_string(options[a])
            elif a in cls.CLIENT_LIST_ARGS:
                v = options[a]
            elif a in cls.CLIENT_INT_ARGS:
                v = int(options[a])
            else:
                v = options[a]
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
        super(RedisDriver, self)._start()
        try:
            self._client = self._make_client(self._parsed_url, self._options,
                                             self.timeout)
        except exceptions.RedisError as e:
            utils.raise_with_cause(coordination.ToozConnectionError,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)
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
                self.MIN_VERSION)
            if not new_enough:
                raise tooz.NotImplemented("Redis version greater than or"
                                          " equal to '%s' is required"
                                          " to use this driver; '%s' is"
                                          " being used which is not new"
                                          " enough" % (self.MIN_VERSION,
                                                       redis_version))
            tpl_params = {
                'group_existence_value': self.GROUP_EXISTS_VALUE,
                'group_existence_key': self.GROUP_EXISTS,
            }
            # For py3.x ensure these are unicode since the string template
            # replacement will expect unicode (and we don't want b'' as a
            # prefix which will happen in py3.x if this is not done).
            for (k, v) in six.iteritems(tpl_params.copy()):
                if isinstance(v, six.binary_type):
                    v = v.decode('ascii')
                tpl_params[k] = v
            prepared_scripts = {}
            for name, raw_script_tpl in six.iteritems(self.SCRIPTS):
                script_tpl = string.Template(raw_script_tpl)
                script = script_tpl.substitute(**tpl_params)
                prepared_scripts[name] = self._client.register_script(script)
            self._scripts = prepared_scripts
            self.heartbeat()
            self._started = True

    def _encode_beat_id(self, member_id):
        member_id = utils.to_binary(member_id, encoding=self._encoding)
        return self.NAMESPACE_SEP.join([self._beat_prefix, member_id])

    def _encode_member_id(self, member_id):
        member_id = utils.to_binary(member_id, encoding=self._encoding)
        if member_id == self.GROUP_EXISTS:
            raise ValueError("Not allowed to use private keys as a member id")
        return member_id

    def _decode_member_id(self, member_id):
        return utils.to_binary(member_id, encoding=self._encoding)

    def _encode_group_leader(self, group_id):
        group_id = utils.to_binary(group_id, encoding=self._encoding)
        return b"leader_of_" + group_id

    def _encode_group_id(self, group_id, apply_namespace=True):
        group_id = utils.to_binary(group_id, encoding=self._encoding)
        if not apply_namespace:
            return group_id
        return self.NAMESPACE_SEP.join([self._group_prefix, group_id])

    def _decode_group_id(self, group_id):
        return utils.to_binary(group_id, encoding=self._encoding)

    def heartbeat(self):
        with _translate_failures():
            beat_id = self._encode_beat_id(self._member_id)
            expiry_ms = max(0, int(self.membership_timeout * 1000.0))
            self._client.psetex(beat_id, time_ms=expiry_ms,
                                value=self.STILL_ALIVE)
        for lock in self._acquired_locks.copy():
            try:
                lock.heartbeat()
            except tooz.ToozError:
                LOG.warning("Unable to heartbeat lock '%s'", lock,
                            exc_info=True)
        return min(self.lock_timeout, self.membership_timeout)

    def _stop(self):
        while self._acquired_locks:
            lock = self._acquired_locks.pop()
            try:
                lock.release()
            except tooz.ToozError:
                LOG.warning("Unable to release lock '%s'", lock, exc_info=True)
        super(RedisDriver, self)._stop()
        if self._client is not None:
            # Make sure we no longer exist...
            beat_id = self._encode_beat_id(self._member_id)
            try:
                # NOTE(harlowja): this will delete nothing if the key doesn't
                # exist in the first place, which is fine/expected/desired...
                with _translate_failures():
                    self._client.delete(beat_id)
            except tooz.ToozError:
                LOG.warning("Unable to delete heartbeat key '%s'", beat_id,
                            exc_info=True)
            self._client = None
        self._server_info = {}
        self._scripts.clear()
        self._started = False

    def _submit(self, cb, *args, **kwargs):
        if not self._started:
            raise tooz.ToozError("Redis driver has not been started")
        return self._executor.submit(cb, *args, **kwargs)

    def _get_script(self, script_key):
        try:
            return self._scripts[script_key]
        except KeyError:
            raise tooz.ToozError("Redis driver has not been started")

    def create_group(self, group_id):
        script = self._get_script('create_group')

        def _create_group(script):
            encoded_group = self._encode_group_id(group_id)
            keys = [
                encoded_group,
                self._groups,
            ]
            args = [
                self._encode_group_id(group_id, apply_namespace=False),
            ]
            result = script(keys=keys, args=args)
            result = strutils.bool_from_string(result)
            if not result:
                raise coordination.GroupAlreadyExist(group_id)

        return RedisFutureResult(self._submit(_create_group, script))

    def update_capabilities(self, group_id, capabilities):
        script = self._get_script('update_capabilities')

        def _update_capabilities(script):
            keys = [
                self._encode_group_id(group_id),
            ]
            args = [
                self._encode_member_id(self._member_id),
                self._dumps(capabilities),
            ]
            result = int(script(keys=keys, args=args))
            if result == -1:
                raise coordination.GroupNotCreated(group_id)
            if result == -2:
                raise coordination.MemberNotJoined(group_id, self._member_id)

        return RedisFutureResult(self._submit(_update_capabilities, script))

    def leave_group(self, group_id):
        encoded_group = self._encode_group_id(group_id)
        encoded_member_id = self._encode_member_id(self._member_id)

        def _leave_group(p):
            if not p.exists(encoded_group):
                raise coordination.GroupNotCreated(group_id)
            p.multi()
            p.hdel(encoded_group, encoded_member_id)
            c = p.execute()[0]
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
            potential_members = set()
            for m in p.hkeys(encoded_group):
                m = self._decode_member_id(m)
                if m != self.GROUP_EXISTS:
                    potential_members.add(m)
            if not potential_members:
                return set()
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
                p.multi()
                encoded_gone_members = list(self._encode_member_id(m)
                                            for m in gone_members)
                p.hdel(encoded_group, *encoded_gone_members)
                p.execute()
                return set(m for m in potential_members
                           if m not in gone_members)
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
            p.multi()
            p.hset(encoded_group, encoded_member_id,
                   self._dumps(capabilities))
            c = p.execute()[0]
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
        script = self._get_script('delete_group')

        def _delete_group(script):
            keys = [
                self._encode_group_id(group_id),
                self._groups,
            ]
            args = [
                self._encode_group_id(group_id, apply_namespace=False),
            ]
            result = int(script(keys=keys, args=args))
            if result in (-1, -2):
                raise coordination.GroupNotCreated(group_id)
            if result == -3:
                raise coordination.GroupNotEmpty(group_id)
            if result == -4:
                raise tooz.ToozError("Unable to remove '%s' key"
                                     " from set located at '%s'"
                                     % (args[0], keys[-1]))
            if result != 1:
                raise tooz.ToozError("Internal error, unable"
                                     " to complete group '%s' removal"
                                     % (group_id))

        return RedisFutureResult(self._submit(_delete_group, script))

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

    def _get_leader_lock(self, group_id):
        name = self._encode_group_leader(group_id)
        return self.get_lock(name)

    def run_elect_coordinator(self):
        for group_id, hooks in six.iteritems(self._hooks_elected_leader):
            leader_lock = self._get_leader_lock(group_id)
            if leader_lock.acquire(blocking=False):
                # We got the lock
                hooks.run(coordination.LeaderElected(group_id,
                                                     self._member_id))

    def run_watchers(self, timeout=None):
        result = super(RedisDriver, self).run_watchers(timeout=timeout)
        self.run_elect_coordinator()
        return result


RedisFutureResult = functools.partial(coordination.CoordinatorResult,
                                      failure_translator=_translate_failures)
