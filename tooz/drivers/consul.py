# -*- coding: utf-8 -*-
#
# Copyright © 2015 Yahoo! Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import contextlib
import functools

import consul
from oslo_utils import encodeutils
import requests

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils


@contextlib.contextmanager
def _failure_translator():

    """Translates common consul exceptions into tooz exceptions."""
    try:
        yield
    except (consul.Timeout, requests.exceptions.RequestException) as e:
        utils.raise_with_cause(coordination.ToozConnectionError,
                               encodeutils.exception_to_unicode(e),
                               cause=e)
    except (consul.ConsulException, ValueError) as e:
        # ValueError = Typically json decoding failed for some reason.
        utils.raise_with_cause(tooz.ToozError,
                               encodeutils.exception_to_unicode(e),
                               cause=e)


def _translate_failures(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with _failure_translator():
            return func(*args, **kwargs)

    return wrapper


class ConsulLock(locking.Lock):
    def __init__(self, name, node, address, session_id, client, token=None):
        super(ConsulLock, self).__init__(name)
        self._name = name
        self._node = node
        self._address = address
        self._acl_token = token
        self._session_id = session_id
        self._client = client
        self.acquired = False

    def acquire(self, blocking=True, shared=False):
        if shared:
            raise tooz.NotImplemented

        @_retry.retry(stop_max_delay=blocking)
        @_translate_failures
        def _acquire():
            # Check if we are the owner and if we are simulate
            # blocking (because consul will not block a second
            # acquisition attempt by the same owner).
            _index, value = self._client.kv.get(key=self._name,
                                                token=self._acl_token)
            if value and value.get('Session') == self._session_id:
                if blocking is False:
                    return False
                else:
                    raise _retry.TryAgain
            else:
                # The value can be anything.
                gotten = self._client.kv.put(key=self._name,
                                             value="I got it!",
                                             acquire=self._session_id,
                                             token=self._acl_token)
                if gotten:
                    self.acquired = True
                    return True
                if blocking is False:
                    return False
                else:
                    raise _retry.TryAgain

        return _acquire()

    def release(self):
        if not self.acquired:
            return False
        # Get the lock to verify the session ID's are same
        _index, contents = self._client.kv.get(key=self._name,
                                               token=self._acl_token)
        if not contents:
            return False
        owner = contents.get('Session')
        if owner == self._session_id:
            removed = self._client.kv.put(key=self._name,
                                          value=self._session_id,
                                          release=self._session_id,
                                          token=self._acl_token)
            if removed:
                self.acquired = False
                return True
        return False


class ConsulDriver(coordination.CoordinationDriverCachedRunWatchers,
                   coordination.CoordinationDriverWithExecutor):
    """This driver uses `python-consul`_ client against `consul`_ servers.

    The ConsulDriver implements the coordination driver API(s) so that Consul
    can be used as an option for Distributed Locking and Group Membership. The
    data is stored in Consul's key-value store.

    The Consul driver connection URI should look like::

      consul://HOST[:PORT][?OPTION1=VALUE1[&OPTION2=VALUE2[&...]]]

    If not specified, PORT defaults to 8500.
    Available options are:

    ==================  =======
    Name                Default
    ==================  =======
    ttl                 15
    namespace           tooz
    acl_token           None
    ==================  =======

    For details on the available options, refer to
    http://python-consul2.readthedocs.org/en/latest/.

    The following Key/Value paths are utilized in Consul to implement the
    coordination APIs:
    +-------------------------------------------+--------------+--------------+
    | Key                                       | Value        | Description  |
    +===========================================+==============+==============+
    | <namespace>/groups/<group_id>             | None         | Group of     |
    |                                           |              | members.     |
    +-------------------------------------------+--------------+--------------+
    | <namespace>/groups/<group_id>/<member_id> | Member       | Member in a  |
    |                                           | capabilities | group.       |
    |                                           | encoded as   |              |
    |                                           | msgpack      |              |
    +-------------------------------------------+--------------+--------------+
    | <namespace>/group_locks/<group_id>        | Consul       | Lock for     |
    |                                           | session ID   | group        |
    |                                           |              | membership   |
    +-------------------------------------------+--------------+--------------+
    | <namespace>/locks/<name>                  | Consul       | Each key is  |
    |                                           | session ID   | a distributed|
    |                                           |              | lock.        |
    +-------------------------------------------+--------------+--------------+

    NOTE: A group in tooz is NOT the same as a Consul service, so tooz groups
    are implemented using Consul Key/Values and not with native services.
    Groups in tooz do not have host:port details or health checks that report
    to consul to verify that the service is still alive and listening on that
    host:port. If you need to use native Consul services, configure the Consul
    agent directly (not via tooz).

    .. _python-consul: http://python-consul2.readthedocs.org/
    .. _consul: https://consul.io/
    """

    #: Default namespace when none is provided
    TOOZ_NAMESPACE = "tooz"

    #: Default TTL
    DEFAULT_TTL = 15

    #: Default consul port if not provided.
    DEFAULT_PORT = 8500

    #: Consul ACL Token if not provided
    ACL_TOKEN = None

    CHARACTERISTICS = (
        # client liveness is based on more than just timeouts
        coordination.Characteristics.NON_TIMEOUT_BASED,
        # multiple service instances (group members) could register
        # with different ports per thread/proc
        # but the agent is always on locahost: so not DISTRIBUTED_ACROSS_HOSTS
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
        # https://www.consul.io/docs/internals/consensus#consistency-modes
        # The consul consistency mode determines the history characteristics.
        # Writes *always* go through a single leader process into raft log.
        # Reads *may* use all servers depending on the request's mode:
        #   - 'consistent' = LINEARIZABLE
        #   - 'default' = SEQUENTIAL
        #   - 'stale' = CAUSAL
        coordination.Characteristics.SEQUENTIAL,  # 'default' consistency mode
        # https://www.consul.io/docs/internals/consensus
        # raft log of servers is snapshotted + compacted
        coordination.Characteristics.SERIALIZABLE,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    def __init__(self, member_id, parsed_url, options):
        super(ConsulDriver, self).__init__(member_id, parsed_url, options)
        options = utils.collapse(options)
        self._host = parsed_url.hostname
        self._port = parsed_url.port or self.DEFAULT_PORT
        self._session_id = None
        self._session_name = encodeutils.safe_decode(member_id)
        self._ttl = int(options.get('ttl', self.DEFAULT_TTL))
        namespace = options.get('namespace', self.TOOZ_NAMESPACE)
        self._namespace = encodeutils.safe_decode(namespace)
        self._acl_token = options.get('acl_token', self.ACL_TOKEN)
        # the empty group name adds a trailing / needed for lookups
        self._groups_prefix = self._paths_join(self._namespace, "groups", "")
        self._client = None

    def _start(self):
        """Create a client, register a node and create a session."""
        # Create a consul client
        if self._client is None:
            self._client = consul.Consul(host=self._host, port=self._port,
                                         token=self._acl_token)

        local_agent = self._client.agent.self()
        self._node = local_agent['Member']['Name']
        self._address = local_agent['Member']['Addr']
        # implicitly uses the agent's datacenter (set in consul agent config)

        # Register a Node
        self._client.catalog.register(node=self._node,
                                      address=self._address,
                                      token=self._acl_token)

        # Create a session
        self._session_id = self._client.session.create(
                                        name=self._session_name,
                                        node=self._node,
                                        ttl=self._ttl,
                                        token=self._acl_token)

    def _stop(self):
        if self._client is not None:
            if self._session_id is not None:
                self._client.session.destroy(self._session_id,
                                             token=self._acl_token)
                self._session_id = None
            self._client = None

    def heartbeat(self):
        # THIS IS A REQUIREMENT FOR CONSUL TO WORK PROPERLY.
        # Consul maintains a "session" token that is used to as the basis
        # for all operations in the service. The session must be refreshed
        # periodically or else it assumes that the agent maintaining the
        # session has died or is unreachable. When a session expires all locks
        # are released and any services that were registered with that session
        # are marked as no longer active.
        self._client.session.renew(self._session_id, token=self._acl_token)
        # renew the session every half-TTL or 1 second, whatever is larger
        sleep_sec = max(self._ttl / 2, 1)
        return sleep_sec

    def _get_lock(self, real_name):
        return ConsulLock(real_name, self._node, self._address,
                          session_id=self._session_id,
                          client=self._client, token=self._acl_token)

    def get_lock(self, name):
        real_name = self._path_lock(name)
        return self._get_lock(real_name)

    def _get_group_lock(self, group_id):
        real_name = self._path_group_lock(group_id)
        return self._get_lock(real_name)

    @staticmethod
    def _paths_join(*args):
        pieces = []
        for arg in args:
            pieces.append(encodeutils.safe_decode(arg))
        return "/".join(pieces)

    def _path_lock(self, name):
        return self._paths_join(self._namespace, "locks", name)

    def _path_group_lock(self, name):
        return self._paths_join(self._namespace, "group_locks", name)

    def _path_group(self, group_id):
        # add an extra '/' at the end so that searches with this path
        # will go down and find their children
        return self._paths_join(self._namespace, "groups", group_id) + "/"

    def _path_member(self, group_id, member_id):
        return self._paths_join(
            self._namespace, "groups", group_id, member_id
        )

    def _group_path_to_id(self, base_path, group_path):
        """Translates a path into a group name.

        The group name is the last part of the path. So, we simply split on
        the path separator '/' and return the last element.

        Example:
           group_id = self._path_to_group_id("tooz/groups/helloworld")
           print(group_id) # "helloworld"
        """
        if group_path.startswith(base_path):
            group_id = group_path[len(base_path):]
        else:
            group_id = group_path
        # if a group has members (sub-keys) it will contain a trailing /
        # we need to strip this to get just the name
        # if a group has no members there is no trailing / (for some reason)
        group_id = group_id.strip("/")
        return utils.to_binary(group_id)

    def _get_group_members(self, group_path):
        index, data = self._client.kv.get(group_path, recurse=True)
        group = None
        members = []
        for kv in (data or []):
            if kv["Key"] == group_path:
                group = kv
            else:
                members.append(kv)
        return (group, members)

    def get_groups(self):

        @_translate_failures
        def _get_groups():
            groups = []
            index, data = self._client.kv.get(self._groups_prefix, keys=True,
                                              separator="/")
            for key in (data or []):
                if key != self._groups_prefix:
                    group_id = self._group_path_to_id(self._groups_prefix, key)
                    groups.append(group_id)
            return groups

        return ConsulFutureResult(self._executor.submit(_get_groups))

    def create_group(self, group_id):

        @_translate_failures
        def _create_group():
            group_path = self._path_group(group_id)
            # create with Check-And-Set index 0 will only succeed if the key
            # doesn't exit
            result = self._client.kv.put(group_path, "", cas=0)
            if not result:
                raise coordination.GroupAlreadyExist(group_id)
            return result

        return ConsulFutureResult(self._executor.submit(_create_group))

    def _destroy_group(self, group_id):
        """Should only be used in tests..."""
        with self._get_group_lock(group_id) as lock:
            group_path = self._path_group(group_id)
            self._client.kv.delete(group_path, recurse=True)
            self._client.kv.delete(lock._name)

    def delete_group(self, group_id):

        @_translate_failures
        def _delete_group():
            # create a lock for the group so that other operations on this
            # group do not conflict while the group is being deleted
            with self._get_group_lock(group_id) as lock:
                group_path = self._path_group(group_id)
                group, members = self._get_group_members(group_path)
                if not group:
                    raise coordination.GroupNotCreated(group_id)

                if members:
                    raise coordination.GroupNotEmpty(group_id)

                # delete the group recursively
                result = self._client.kv.delete(group_path, recurse=True)

                # delete the lock for the group
                self._client.kv.delete(lock._name)
                return result

        return ConsulFutureResult(self._executor.submit(_delete_group))

    def join_group(self, group_id, capabilities=b""):

        @_translate_failures
        def _join_group():
            # lock the group so that it doesn't get deleted while we join
            with self._get_group_lock(group_id):
                group_path = self._path_group(group_id)
                member_path = self._path_member(group_id, self._member_id)
                group, members = self._get_group_members(group_path)
                if not group:
                    raise coordination.GroupNotCreated(group_id)

                for m in members:
                    if m["Key"] == member_path:
                        raise coordination.MemberAlreadyExist(group_id,
                                                              self._member_id)

                # create with Check-And-Set index 0 will only succeed if the
                # key doesn't exit
                self._client.kv.put(member_path, utils.dumps(capabilities),
                                    cas=0)
                self._joined_groups.add(group_id)

        return ConsulFutureResult(self._executor.submit(_join_group))

    def leave_group(self, group_id):

        @_translate_failures
        def _leave_group():
            # NOTE: We do NOT have to lock the group here because deletes in
            #       Consul are atomic and succeed even if the key doesn't exist
            #       This means that there is no race condition between checking
            #       if the member exists and then deleting it.
            group_path = self._path_group(group_id)
            member_path = self._path_member(group_id, self._member_id)
            group, members = self._get_group_members(group_path)
            member = None
            for m in members:
                if m["Key"] == member_path:
                    member = m
                    break
            else:
                raise coordination.MemberNotJoined(group_id, self._member_id)

            # delete the member key with Check-And-Set semantics based on index
            # we read above
            self._client.kv.delete(member_path, cas=member["ModifyIndex"])
            self._joined_groups.discard(group_id)

        return ConsulFutureResult(self._executor.submit(_leave_group))

    def get_members(self, group_id):

        @_translate_failures
        def _get_members():
            group_path = self._path_group(group_id)
            group, members = self._get_group_members(group_path)
            if not group:
                raise coordination.GroupNotCreated(group_id)

            result = set()
            for m in members:
                member_id = self._group_path_to_id(group_path, m["Key"])
                result.add(member_id)
            return result

        return ConsulFutureResult(self._executor.submit(_get_members))

    def get_member_capabilities(self, group_id, member_id):

        @_translate_failures
        def _get_member_capabilities():
            member_path = self._path_member(group_id, member_id)
            index, data = self._client.kv.get(member_path)
            if not data:
                raise coordination.MemberNotJoined(group_id, member_id)
            return utils.loads(data["Value"])

        return ConsulFutureResult(
            self._executor.submit(_get_member_capabilities))

    def update_capabilities(self, group_id, capabilities):

        @_translate_failures
        def _update_capabilities():
            member_path = self._path_member(group_id, self._member_id)
            index, data = self._client.kv.get(member_path)
            if not data:
                raise coordination.MemberNotJoined(group_id, self._member_id)
            # no need to Check-And-Set here, latest write wins
            self._client.kv.put(member_path, utils.dumps(capabilities))

        return ConsulFutureResult(self._executor.submit(_update_capabilities))

    @staticmethod
    def watch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented

    @staticmethod
    def unwatch_elected_as_leader(group_id, callback):
        raise tooz.NotImplemented


ConsulFutureResult = functools.partial(coordination.CoordinatorResult,
                                       failure_translator=_failure_translator)
