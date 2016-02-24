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
import enum

from oslo_utils import excutils
from oslo_utils import netutils
from oslo_utils import timeutils
import six
from stevedore import driver

import tooz

TOOZ_BACKENDS_NAMESPACE = "tooz.backends"


class Characteristics(enum.Enum):
    """Attempts to describe the characteristic that a driver supports."""

    DISTRIBUTED_ACROSS_THREADS = 'DISTRIBUTED_ACROSS_THREADS'
    """Coordinator components when used by multiple **threads** work
       the same as if those components were only used by a single thread."""

    DISTRIBUTED_ACROSS_PROCESSES = 'DISTRIBUTED_ACROSS_PROCESSES'
    """Coordinator components when used by multiple **processes** work
       the same as if those components were only used by a single thread."""

    DISTRIBUTED_ACROSS_HOSTS = 'DISTRIBUTED_ACROSS_HOSTS'
    """Coordinator components when used by multiple **hosts** work
       the same as if those components were only used by a single thread."""

    LINEARIZABLE = 'LINEARIZABLE'
    """The driver has the following properties:

    * Ensures each operation must take place before its
      completion time.
    * Any operation invoked subsequently must take place
      after the invocation and by extension, after the original operation
      itself.
    """

    SEQUENTIAL = 'SEQUENTIAL'
    """The driver has the following properties:

    * Operations can take effect before or after completion – but all
      operations retain the constraint that operations from any given process
      must take place in that processes order.
    """

    CAUSAL = 'CAUSAL'
    """The driver has the following properties:

    * Does **not** have to enforce the order of every
      operation from a process, perhaps, only causally related operations
      must occur in order.
    """

    SERIALIZABLE = 'SERIALIZABLE'
    """The driver has the following properties:

    * The history of **all** operations is equivalent to
      one that took place in some single atomic order but with unknown
      invocation and completion times - it places no bounds on
      time or order.
    """

    SAME_VIEW_UNDER_PARTITIONS = 'SAME_VIEW_UNDER_PARTITIONS'
    """When a client is connected to a server and that server is partitioned
    from a group of other servers it will (somehow) have the same view of
    data as a client connected to a server on the other side of the
    partition (typically this is accomplished by write availability being
    lost and therefore nothing can change).
    """

    SAME_VIEW_ACROSS_CLIENTS = 'SAME_VIEW_ACROSS_CLIENTS'
    """A client connected to one server will *always* have the same view
    every other client will have (no matter what server those other
    clients are connected to). Typically this is a sacrifice in
    write availability because before a write can be acknowledged it must
    be acknowledged by *all* servers in a cluster (so that all clients
    that are connected to those servers read the exact *same* thing).
    """


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

    requires_beating = False
    """
    Usage requirement that if true requires that the :py:meth:`~.heartbeat`
    be called periodically (at a given rate) to avoid locks, sessions and
    other from being automatically closed/discarded by the coordinators
    backing store.
    """

    CHARACTERISTICS = ()
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    def __init__(self):
        self._started = False
        self._hooks_join_group = collections.defaultdict(Hooks)
        self._hooks_leave_group = collections.defaultdict(Hooks)
        self._hooks_elected_leader = collections.defaultdict(Hooks)
        # A cache for group members
        self._group_members = collections.defaultdict(set)
        self.requires_beating = (
            CoordinationDriver.heartbeat != self.__class__.heartbeat
        )

    def _has_hooks_for_group(self, group_id):
        return (len(self._hooks_join_group[group_id]) +
                len(self._hooks_leave_group[group_id]))

    @staticmethod
    def run_watchers(timeout=None):
        """Run the watchers callback.

        This may also activate :py:meth:`.run_elect_coordinator` (depending
        on driver implementation).
        """
        raise tooz.NotImplemented

    @staticmethod
    def run_elect_coordinator():
        """Try to leader elect this coordinator & activate hooks on success."""
        raise tooz.NotImplemented

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
        try:
            self._hooks_join_group[group_id].remove(callback)
        except ValueError:
            raise WatchCallbackNotFound(group_id, callback)

        if (not self._has_hooks_for_group(group_id) and
           group_id in self._group_members):
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
        try:
            self._hooks_leave_group[group_id].remove(callback)
        except ValueError:
            raise WatchCallbackNotFound(group_id, callback)

        if (not self._has_hooks_for_group(group_id) and
           group_id in self._group_members):
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
        try:
            self._hooks_elected_leader[group_id].remove(callback)
        except ValueError:
            raise WatchCallbackNotFound(group_id, callback)

        if not self._hooks_elected_leader[group_id]:
            del self._hooks_elected_leader[group_id]

    @staticmethod
    def stand_down_group_leader(group_id):
        """Stand down as the group leader if we are.

        :param group_id: The group where we don't want to be a leader anymore
        """
        raise tooz.NotImplemented

    @property
    def is_started(self):
        return self._started

    def start(self):
        """Start the service engine.

        If needed, the establishment of a connection to the servers
        is initiated.
        """
        if self._started:
            raise ToozError(
                "Can not start a driver which has not been stopped")
        self._start()
        self._started = True

    def _start(self):
        pass

    def stop(self):
        """Stop the service engine.

        If needed, the connection to servers is closed and the client will
        disappear from all joined groups.
        """
        if not self._started:
            raise ToozError("Can not stop a driver which has not been started")
        self._stop()
        self._started = False

    def _stop(self):
        pass

    @staticmethod
    def create_group(group_id):
        """Request the creation of a group asynchronously.

        :param group_id: the id of the group to create
        :type group_id: str
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_groups():
        """Return the list composed by all groups ids asynchronously.

        :returns: the list of all created group ids
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def join_group(group_id, capabilities=b""):
        """Join a group and establish group membership asynchronously.

        :param group_id: the id of the group to join
        :type group_id: str
        :param capabilities: the capabilities of the joined member
        :type capabilities: object (typically str)
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def leave_group(group_id):
        """Leave a group asynchronously.

        :param group_id: the id of the group to leave
        :type group_id: str
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def delete_group(group_id):
        """Delete a group asynchronously.

        :param group_id: the id of the group to leave
        :type group_id: str
        :returns: Result
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_members(group_id):
        """Return the list of all members ids of the specified group.

        :returns: list of all created group ids
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

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
        raise tooz.NotImplemented

    @staticmethod
    def get_member_info(group_id, member_id):
        """Return the statistics and capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :type group_id: str
        :param member_id: the id of the member
        :type member_id: str
        :returns: capabilities and statistics of a member
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def update_capabilities(group_id, capabilities):
        """Update member capabilities in the specified group.

        :param group_id: the id of the group of the current member
        :type group_id: str
        :param capabilities: the capabilities of the updated member
        :type capabilities: object (typically str)
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_leader(group_id):
        """Return the leader for a group.

        :param group_id: the id of the group:
        :returns: the leader
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_lock(name):
        """Return a distributed lock.

        This is a exclusive lock, a second call to acquire() will block or
        return False.

        :param name: The lock name that is used to identify it across all
                     nodes.

        """
        raise tooz.NotImplemented

    @staticmethod
    def heartbeat():
        """Update member status to indicate it is still alive.

        Method to run once in a while to be sure that the member is not dead
        and is still an active member of a group.

        """
        pass


@six.add_metaclass(abc.ABCMeta)
class CoordAsyncResult(object):
    """Representation of an asynchronous task.

    Every call API returns an CoordAsyncResult object on which the result or
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


class _RunWatchersMixin(object):
    """Mixin to share the *mostly* common ``run_watchers`` implementation."""

    def run_watchers(self, timeout=None):
        with timeutils.StopWatch(duration=timeout) as w:
            known_groups = self.get_groups().get(
                timeout=w.leftover(return_none=True))
            result = []
            for group_id in known_groups:
                try:
                    group_members_fut = self.get_members(group_id)
                    group_members = group_members_fut.get(
                        timeout=w.leftover(return_none=True))
                except GroupNotCreated:
                    group_members = set()
                else:
                    group_members = set(group_members)
                if (group_id in self._joined_groups and
                        self._member_id not in group_members):
                    self._joined_groups.discard(group_id)
                old_group_members = self._group_members.get(group_id, set())
                for member_id in (group_members - old_group_members):
                    result.extend(
                        self._hooks_join_group[group_id].run(
                            MemberJoinedGroup(group_id, member_id)))
                for member_id in (old_group_members - group_members):
                    result.extend(
                        self._hooks_leave_group[group_id].run(
                            MemberLeftGroup(group_id, member_id)))
                self._group_members[group_id] = group_members
            return result


def get_coordinator(backend_url, member_id,
                    characteristics=frozenset(), **kwargs):
    """Initialize and load the backend.

    :param backend_url: the backend URL to use
    :type backend: str
    :param member_id: the id of the member
    :type member_id: str
    :param characteristics: set
    :type characteristics: set of :py:class:`.Characteristics` that will
                           be matched to the requested driver (this **will**
                           become a **required** parameter in a future tooz
                           version)
    :param kwargs: additional coordinator options (these take precedence over
                   options of the **same** name found in the ``backend_url``
                   arguments query string)
    """
    parsed_url = netutils.urlsplit(backend_url)
    parsed_qs = six.moves.urllib.parse.parse_qs(parsed_url.query)
    if kwargs:
        options = {}
        for (k, v) in six.iteritems(kwargs):
            options[k] = [v]
        for (k, v) in six.iteritems(parsed_qs):
            if k not in options:
                options[k] = v
    else:
        options = parsed_qs
    d = driver.DriverManager(
        namespace=TOOZ_BACKENDS_NAMESPACE,
        name=parsed_url.scheme,
        invoke_on_load=True,
        invoke_args=(member_id, parsed_url, options)).driver
    characteristics = set(characteristics)
    driver_characteristics = set(getattr(d, 'CHARACTERISTICS', set()))
    missing_characteristics = characteristics - driver_characteristics
    if missing_characteristics:
        raise ToozDriverChosenPoorly("Desired characteristics %s"
                                     " is not a strict subset of driver"
                                     " characteristics %s, %s"
                                     " characteristics were not found"
                                     % (characteristics,
                                        driver_characteristics,
                                        missing_characteristics))
    return d


class ToozError(Exception):
    """Exception raised when an internal error occurs.

    Raised for instance in case of server internal error.

    :ivar cause: the cause of the exception being raised, when not none this
                 will itself be an exception instance, this is useful for
                 creating a chain of exceptions for versions of python where
                 this is not yet implemented/supported natively.

    """

    def __init__(self, message, cause=None):
        super(ToozError, self).__init__(message)
        self.cause = cause


class ToozDriverChosenPoorly(ToozError):
    """Raised when a driver does not match desired characteristics."""


class ToozConnectionError(ToozError):
    """Exception raised when the client cannot connect to the server."""


class OperationTimedOut(ToozError):
    """Exception raised when an operation times out."""


class LockAcquireFailed(ToozError):
    """Exception raised when a lock acquire fails in a context manager."""


class GroupNotCreated(ToozError):
    """Exception raised when the caller request an nonexistent group."""
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupNotCreated, self).__init__(
            "Group %s does not exist" % group_id)


class GroupAlreadyExist(ToozError):
    """Exception raised trying to create an already existing group."""
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupAlreadyExist, self).__init__(
            "Group %s already exists" % group_id)


class MemberAlreadyExist(ToozError):
    """Exception raised trying to join a group already joined."""
    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id
        super(MemberAlreadyExist, self).__init__(
            "Member %s has already joined %s" %
            (member_id, group_id))


class MemberNotJoined(ToozError):
    """Exception raised trying to access a member not in a group."""
    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id
        super(MemberNotJoined, self).__init__("Member %s has not joined %s" %
                                              (member_id, group_id))


class GroupNotEmpty(ToozError):
    "Exception raised when the caller try to delete a group with members."
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupNotEmpty, self).__init__("Group %s is not empty" % group_id)


class WatchCallbackNotFound(ToozError):
    """Exception raised when unwatching a group.

    Raised when the caller tries to unwatch a group with a callback that
    does not exist.

    """
    def __init__(self, group_id, callback):
        self.group_id = group_id
        self.callback = callback
        super(WatchCallbackNotFound, self).__init__(
            'Callback %s is not registered on group %s' %
            (callback.__name__, group_id))


class SerializationError(ToozError):
    "Exception raised when serialization or deserialization breaks."


def raise_with_cause(exc_cls, message, *args, **kwargs):
    """Helper to raise + chain exceptions (when able) and associate a *cause*.

    **For internal usage only.**

    NOTE(harlowja): Since in py3.x exceptions can be chained (due to
    :pep:`3134`) we should try to raise the desired exception with the given
    *cause*.

    :param exc_cls: the :py:class:`~tooz.coordination.ToozError` class
                    to raise.
    :param message: the text/str message that will be passed to
                    the exceptions constructor as its first positional
                    argument.
    :param args: any additional positional arguments to pass to the
                 exceptions constructor.
    :param kwargs: any additional keyword arguments to pass to the
                   exceptions constructor.
    """
    if not issubclass(exc_cls, ToozError):
        raise ValueError("Subclass of tooz error is required")
    excutils.raise_with_cause(exc_cls, message, *args, **kwargs)
