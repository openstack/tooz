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

from __future__ import absolute_import

import consul
from oslo_utils import encodeutils

import tooz
from tooz import _retry
from tooz import coordination
from tooz import locking
from tooz import utils


class ConsulLock(locking.Lock):
    def __init__(self, name, node, address, session_id, client):
        super(ConsulLock, self).__init__(name)
        self._name = name
        self._node = node
        self._address = address
        self._session_id = session_id
        self._client = client
        self.acquired = False

    def acquire(self, blocking=True, shared=False):
        if shared:
            raise tooz.NotImplemented

        @_retry.retry(stop_max_delay=blocking)
        def _acquire():
            # Check if we are the owner and if we are simulate
            # blocking (because consul will not block a second
            # acquisition attempt by the same owner).
            _index, value = self._client.kv.get(key=self._name)
            if value and value.get('Session') == self._session_id:
                if blocking is False:
                    return False
                else:
                    raise _retry.TryAgain
            else:
                # The value can be anything.
                gotten = self._client.kv.put(key=self._name,
                                             value=u"I got it!",
                                             acquire=self._session_id)
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
        _index, contents = self._client.kv.get(key=self._name)
        if not contents:
            return False
        owner = contents.get('Session')
        if owner == self._session_id:
            removed = self._client.kv.put(key=self._name,
                                          value=self._session_id,
                                          release=self._session_id)
            if removed:
                self.acquired = False
                return True
        return False


class ConsulDriver(coordination.CoordinationDriver):
    """This driver uses `python-consul`_ client against `consul`_ servers.

    The ConsulDriver implements a minimal set of coordination driver API(s)
    needed to make Consul being used as an option for Distributed Locking. The
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
    ==================  =======

    For details on the available options, refer to
    http://python-consul.readthedocs.org/en/latest/.

    .. _python-consul: http://python-consul.readthedocs.org/
    .. _consul: https://consul.io/
    """

    #: Default namespace when none is provided
    TOOZ_NAMESPACE = u"tooz"

    #: Default TTL
    DEFAULT_TTL = 15

    #: Default consul port if not provided.
    DEFAULT_PORT = 8500

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
        self._client = None

    def _start(self):
        """Create a client, register a node and create a session."""
        # Create a consul client
        if self._client is None:
            self._client = consul.Consul(host=self._host, port=self._port)

        local_agent = self._client.agent.self()
        self._node = local_agent['Member']['Name']
        self._address = local_agent['Member']['Addr']

        # Register a Node
        self._client.catalog.register(node=self._node,
                                      address=self._address)

        # Create a session
        self._session_id = self._client.session.create(
            name=self._session_name, node=self._node, ttl=self._ttl)

    def _stop(self):
        if self._client is not None:
            if self._session_id is not None:
                self._client.session.destroy(self._session_id)
                self._session_id = None
            self._client = None

    def get_lock(self, name):
        real_name = self._paths_join(self._namespace, u"locks", name)
        return ConsulLock(real_name, self._node, self._address,
                          session_id=self._session_id,
                          client=self._client)

    @staticmethod
    def _paths_join(*args):
        pieces = []
        for arg in args:
            pieces.append(encodeutils.safe_decode(arg))
        return u"/".join(pieces)

    def watch_join_group(self, group_id, callback):
        raise tooz.NotImplemented

    def unwatch_join_group(self, group_id, callback):
        raise tooz.NotImplemented

    def watch_leave_group(self, group_id, callback):
        raise tooz.NotImplemented

    def unwatch_leave_group(self, group_id, callback):
        raise tooz.NotImplemented
