# -*- coding: utf-8 -*-
#
#    Copyright (C) 2013-2014 eNovance Inc. All Rights Reserved.
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
import collections

import six
from stevedore import driver

from tooz.openstack.common import network_utils

TOOZ_BACKENDS_NAMESPACE = "tooz.backends"


class Hooks(list):
    def run(self, *args, **kwargs):
        return list(map(lambda cb: cb(*args, **kwargs), self))


class Event(object):
    """Base class for events."""


class MemberJoinedGroup(Event):
    """A member joined a group event."""

    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id


class MemberLeftGroup(Event):
    """A member left a group event."""

    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id


class LeaderElected(Event):
    """A leader as been elected."""

    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id


@six.add_metaclass(abc.ABCMeta)
class CoordinationDriver(object):

    def __init__(self):
        self._hooks_join_group = collections.defaultdict(Hooks)
        self._hooks_leave_group = collections.defaultdict(Hooks)
        self._hooks_elected_leader = collections.defaultdict(Hooks)
        # A cache for group members
        self._group_members = collections.defaultdict(set)

    def _has_hooks_for_group(self, group_id):
        return (len(self._hooks_join_group[group_id])
                + len(self._hooks_leave_group[group_id]))

    @staticmethod
    def run_watchers():
        """Run the watchers callback."""
        raise NotImplementedError

    @abc.abstractmethod
    def watch_join_group(self, group_id, callback):
        """Call a function when group_id sees a new member joined.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member joins this group

        """
        self._hooks_join_group[group_id].append(callback)

    @abc.abstractmethod
    def unwatch_join_group(self, group_id, callback):
        """Stop executing a function when a group_id sees a new member joined.

        :param group_id: The group id to unwatch
        :param callback: The function that was executed when a member joined
                         this group
        """
        self._hooks_join_group[group_id].remove(callback)
        if (not self._has_hooks_for_group(group_id)
           and group_id in self._group_members):
            del self._group_members[group_id]

    @abc.abstractmethod
    def watch_leave_group(self, group_id, callback):
        """Call a function when group_id sees a new member leaving.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_leave_group[group_id].append(callback)

    @abc.abstractmethod
    def unwatch_leave_group(self, group_id, callback):
        """Stop executing a function when a group_id sees a new member leaving.

        :param group_id: The group id to unwatch
        :param callback: The function that was executed when a member left
                         this group
        """
        self._hooks_leave_group[group_id].remove(callback)
        if (not self._has_hooks_for_group(group_id)
           and group_id in self._group_members):
            del self._group_members[group_id]

    @abc.abstractmethod
    def watch_elected_as_leader(self, group_id, callback):
        """Call a function when member gets elected as leader.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_elected_leader[group_id].append(callback)

    @abc.abstractmethod
    def unwatch_elected_as_leader(self, group_id, callback):
        """Call a function when member gets elected as leader.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_elected_leader[group_id].remove(callback)
        if not self._hooks.elected_leader[group_id]:
            del self._hooks.elected_leader[group_id]

    @staticmethod
    def stand_down_group_leader(group_id):
        """Stand down as the group leader if we are.

        :param group_id: The group where we don't want to be a leader anymore
        """
        raise NotImplementedError

    def start(self):
        """Start the service engine.

        If needed, the establishment of a connection to the servers
        is initiated.
        """

    @staticmethod
    def stop():
        """Stop the service engine.

        If needed, the connection to servers is closed and the client will
        disappear from all joined groups.
        """

    @staticmethod
    def create_group(group_id):
        """Request the creation of a group asynchronously.

        :param group_id: the id of the group to create
        :type group_id: str
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def get_groups():
        """Return the list composed by all groups ids asynchronously.

        :returns: the list of all created group ids
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def join_group(group_id, capabilities=b""):
        """Join a group and establish group membership asynchronously.

        :param group_id: the id of the group to join
        :type group_id: str
        :param capabilities: the capabilities of the joined member
        :type capabilities: str
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def leave_group(group_id):
        """Leave a group asynchronously.

        :param group_id: the id of the group to leave
        :type group_id: str
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def get_members(group_id):
        """Return the list of all members ids of the specified group
        asynchronously.

        :returns: list of all created group ids
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def get_member_capabilities(group_id, member_id):
        """Return the capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :type group_id: str
        :param member_id: the id of the member
        :type member_id: str
        :returns: capabilities of a member
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def update_capabilities(group_id, capabilities):
        """Update capabilities of the caller in the specified group
        asynchronously.

        :param group_id: the id of the group of the current member
        :type group_id: str
        :param capabilities: the capabilities of the updated member
        :type capabilities: str
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def get_leader(group_id):
        """Return the leader for a group.

        :param group_id: the id of the group:
        :returns: the leader
        :rtype: CoordAsyncResult
        """
        raise NotImplementedError

    @staticmethod
    def get_lock(name):
        """Return a distributed lock.

        :param name: The lock name that is used to identify it across all
                     nodes.

        """
        raise NotImplementedError

    @staticmethod
    def heartbeat():
        """Method to run once in a while to be sure that the member is not dead
        and is still an active member of a group.


        """
        pass


@six.add_metaclass(abc.ABCMeta)
class CoordAsyncResult(object):
    """Representation of an asynchronous task, every call API
    returns an CoordAsyncResult object on which the result or
    the status of the task can be requested.
    """

    @abc.abstractmethod
    def get(self, timeout=10):
        """Retrieve the result of the corresponding asynchronous call.
        :param timeout: block until the timeout expire.
        :type timeout: float
        """

    @abc.abstractmethod
    def done(self):
        """Returns True if the task is done, False otherwise."""


def get_coordinator(backend_url, member_id):
    """Initialize and load the backend.

    :param backend_url: the backend URL to use
    :type backend: str
    :param member_id: the id of the member
    :type member_id: str
    """
    parsed_url = network_utils.urlsplit(backend_url)
    parsed_qs = six.moves.urllib.parse.parse_qs(parsed_url.query)
    return driver.DriverManager(
        namespace=TOOZ_BACKENDS_NAMESPACE,
        name=parsed_url.scheme,
        invoke_on_load=True,
        invoke_args=(member_id, parsed_url, parsed_qs)).driver


class ToozError(Exception):
    """Exception raised when an internal error occurs, for instance in
    case of server internal error.
    """


class ToozConnectionError(ToozError):
    """Exception raised when the client cannot manage to connect to the
    server.
    """


class GroupNotCreated(ToozError):
    """Exception raised when the caller request a group which does
    not exist.
    """
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupNotCreated, self).__init__(
            "Group %s does not exist" % group_id)


class GroupAlreadyExist(ToozError):
    """Exception raised when the caller try to create a group which already
    exist.
    """
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupAlreadyExist, self).__init__(
            "Group %s already exists" % group_id)


class MemberAlreadyExist(ToozError):
    """Exception raised when the caller try to join a group but a member
    with the same identifier belongs to that group.
    """
    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id
        super(MemberAlreadyExist, self).__init__(
            "Member %s has already joined %s" %
            (member_id, group_id))


class MemberNotJoined(ToozError):
    """Exception raised when the caller try to access a member which does not
    belongs to the specified group.
    """
    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id
        super(MemberNotJoined, self).__init__("Member %s has not joined %s" %
                                              (member_id, group_id))
