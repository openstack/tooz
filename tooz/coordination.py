# -*- coding: utf-8 -*-
#
#    Copyright (C) 2016 Red Hat, Inc.
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
from concurrent import futures
import enum
import logging
import threading

from oslo_utils import encodeutils
from oslo_utils import netutils
from oslo_utils import timeutils
import six
from stevedore import driver
import tenacity

import tooz
from tooz import _retry
from tooz import partitioner
from tooz import utils

LOG = logging.getLogger(__name__)


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

    NON_TIMEOUT_BASED = 'NON_TIMEOUT_BASED'
    """The driver has the following property:

    * Its operations are not based on the timeout of other clients, but on some
      other more robust mechanisms.
    """

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

    def __repr__(self):
        return "<%s: group %s: +member %s>" % (self.__class__.__name__,
                                               self.group_id,
                                               self.member_id)


class MemberLeftGroup(Event):
    """A member left a group event."""

    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id

    def __repr__(self):
        return "<%s: group %s: -member %s>" % (self.__class__.__name__,
                                               self.group_id,
                                               self.member_id)


class LeaderElected(Event):
    """A leader as been elected."""

    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id


class Heart(object):
    """Coordination drivers main liveness pump (its heart)."""

    def __init__(self, driver, thread_cls=threading.Thread,
                 event_cls=threading.Event):
        self._thread_cls = thread_cls
        self._dead = event_cls()
        self._runner = None
        self._driver = driver
        self._beats = 0

    @property
    def beats(self):
        """How many times the heart has beaten."""
        return self._beats

    def is_alive(self):
        """Returns if the heart is beating."""
        return not (self._runner is None
                    or not self._runner.is_alive())

    def _beat_forever_until_stopped(self):
        """Inner beating loop."""
        retry = tenacity.Retrying(
            wait=tenacity.wait_fixed(1),
            before_sleep=tenacity.before_sleep_log(LOG, logging.warning),
        )
        while not self._dead.is_set():
            with timeutils.StopWatch() as w:
                wait_until_next_beat = retry(self._driver.heartbeat)
            ran_for = w.elapsed()
            has_to_sleep_for = wait_until_next_beat - ran_for
            if has_to_sleep_for < 0:
                LOG.warning(
                    "Heartbeating took too long to execute (it ran for"
                    " %0.2f seconds which is %0.2f seconds longer than"
                    " the next heartbeat idle time). This may cause"
                    " timeouts (in locks, leadership, ...) to"
                    " happen (which will not end well).", ran_for,
                    ran_for - wait_until_next_beat)
            self._beats += 1
            # NOTE(harlowja): use the event object for waiting and
            # not a sleep function since doing that will allow this code
            # to terminate early if stopped via the stop() method vs
            # having to wait until the sleep function returns.
            # NOTE(jd): Wait for only the half time of what we should.
            # This is a measure of safety, better be too soon than too late.
            self._dead.wait(has_to_sleep_for / 2.0)

    def start(self, thread_cls=None):
        """Starts the heart beating thread (noop if already started)."""
        if not self.is_alive():
            self._dead.clear()
            self._beats = 0
            if thread_cls is None:
                thread_cls = self._thread_cls
            self._runner = thread_cls(target=self._beat_forever_until_stopped)
            self._runner.daemon = True
            self._runner.start()

    def stop(self):
        """Requests the heart beating thread to stop beating."""
        self._dead.set()

    def wait(self, timeout=None):
        """Wait up to given timeout for the heart beating thread to stop."""
        self._runner.join(timeout)
        return self._runner.is_alive()


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

    def __init__(self, member_id, parsed_url, options):
        super(CoordinationDriver, self).__init__()
        self._member_id = member_id
        self._started = False
        self._hooks_join_group = collections.defaultdict(Hooks)
        self._hooks_leave_group = collections.defaultdict(Hooks)
        self._hooks_elected_leader = collections.defaultdict(Hooks)
        self.requires_beating = (
            CoordinationDriver.heartbeat != self.__class__.heartbeat
        )
        self.heart = Heart(self)

    def _has_hooks_for_group(self, group_id):
        return (group_id in self._hooks_join_group or
                group_id in self._hooks_leave_group)

    def join_partitioned_group(
            self, group_id,
            weight=1,
            partitions=partitioner.Partitioner.DEFAULT_PARTITION_NUMBER):
        """Join a group and get a partitioner.

        A partitioner allows to distribute a bunch of objects across several
        members using a consistent hash ring. Each object gets assigned (at
        least) one member responsible for it. It's then possible to check which
        object is owned by any member of the group.

        This method also creates if necessary, and joins the group with the
        selected weight.

        :param group_id: The group to create a partitioner for.
        :param weight: The weight to use in the hashring for this node.
        :param partitions: The number of partitions to create.
        :return: A :py:class:`~tooz.partitioner.Partitioner` object.

        """
        self.join_group_create(group_id, capabilities={'weight': weight})
        return partitioner.Partitioner(self, group_id, partitions=partitions)

    def leave_partitioned_group(self, partitioner):
        """Leave a partitioned group.

        This leaves the partitioned group and stop the partitioner.
        :param group_id: The group to create a partitioner for.
        """
        leave = self.leave_group(partitioner.group_id)
        partitioner.stop()
        return leave.get()

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

    def watch_join_group(self, group_id, callback):
        """Call a function when group_id sees a new member joined.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member joins this group

        """
        self._hooks_join_group[group_id].append(callback)

    def unwatch_join_group(self, group_id, callback):
        """Stop executing a function when a group_id sees a new member joined.

        :param group_id: The group id to unwatch
        :param callback: The function that was executed when a member joined
                         this group
        """
        try:
            # Check if group_id is in hooks to avoid creating a default empty
            # entry in hooks list.
            if group_id not in self._hooks_join_group:
                raise ValueError
            self._hooks_join_group[group_id].remove(callback)
        except ValueError:
            raise WatchCallbackNotFound(group_id, callback)

        if not self._hooks_join_group[group_id]:
            del self._hooks_join_group[group_id]

    def watch_leave_group(self, group_id, callback):
        """Call a function when group_id sees a new member leaving.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_leave_group[group_id].append(callback)

    def unwatch_leave_group(self, group_id, callback):
        """Stop executing a function when a group_id sees a new member leaving.

        :param group_id: The group id to unwatch
        :param callback: The function that was executed when a member left
                         this group
        """
        try:
            # Check if group_id is in hooks to avoid creating a default empty
            # entry in hooks list.
            if group_id not in self._hooks_leave_group:
                raise ValueError
            self._hooks_leave_group[group_id].remove(callback)
        except ValueError:
            raise WatchCallbackNotFound(group_id, callback)

        if not self._hooks_leave_group[group_id]:
            del self._hooks_leave_group[group_id]

    def watch_elected_as_leader(self, group_id, callback):
        """Call a function when member gets elected as leader.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_elected_leader[group_id].append(callback)

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

    def start(self, start_heart=False):
        """Start the service engine.

        If needed, the establishment of a connection to the servers
        is initiated.
        """
        if self._started:
            raise tooz.ToozError(
                "Can not start a driver which has not been stopped")
        self._start()
        if self.requires_beating and start_heart:
            self.heart.start()
        self._started = True
        # Tracks which group are joined
        self._joined_groups = set()

    def _start(self):
        pass

    def stop(self):
        """Stop the service engine.

        If needed, the connection to servers is closed and the client will
        disappear from all joined groups.
        """
        if not self._started:
            raise tooz.ToozError(
                "Can not stop a driver which has not been started")
        if self.heart.is_alive():
            self.heart.stop()
            self.heart.wait()
        # Some of the drivers modify joined_groups when being called to leave
        # so clone it so that we aren't modifying something while iterating.
        joined_groups = self._joined_groups.copy()
        leaving = [self.leave_group(group) for group in joined_groups]
        for fut in leaving:
            try:
                fut.get()
            except tooz.ToozError:
                # Whatever happens, ignore. Maybe we got booted out/never
                # existed in the first place, or something is down, but we just
                # want to call _stop after whatever happens to not leak any
                # connection.
                pass
        self._stop()
        self._started = False

    def _stop(self):
        pass

    @staticmethod
    def create_group(group_id):
        """Request the creation of a group asynchronously.

        :param group_id: the id of the group to create
        :type group_id: ascii bytes
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
        :type group_id: ascii bytes
        :param capabilities: the capabilities of the joined member
        :type capabilities: object
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @_retry.retry()
    def join_group_create(self, group_id, capabilities=b""):
        """Join a group and create it if necessary.

        If the group cannot be joined because it does not exist, it is created
        before being joined.

        This function will keep retrying until it can create the group and join
        it. Since nothing is transactional, it may have to retry several times
        if another member is creating/deleting the group at the same time.

        :param group_id: Identifier of the group to join and create
        :param capabilities: the capabilities of the joined member
        """
        req = self.join_group(group_id, capabilities)
        try:
            req.get()
        except GroupNotCreated:
            req = self.create_group(group_id)
            try:
                req.get()
            except GroupAlreadyExist:
                # The group might have been created in the meantime, ignore
                pass
            # Now retry to join the group
            raise _retry.TryAgain

    @staticmethod
    def leave_group(group_id):
        """Leave a group asynchronously.

        :param group_id: the id of the group to leave
        :type group_id: ascii bytes
        :returns: None
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def delete_group(group_id):
        """Delete a group asynchronously.

        :param group_id: the id of the group to leave
        :type group_id: ascii bytes
        :returns: Result
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_members(group_id):
        """Return the set of all members ids of the specified group.

        :returns: set of all created group ids
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_member_capabilities(group_id, member_id):
        """Return the capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :type group_id: ascii bytes
        :param member_id: the id of the member
        :type member_id: ascii bytes
        :returns: capabilities of a member
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def get_member_info(group_id, member_id):
        """Return the statistics and capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :type group_id: ascii bytes
        :param member_id: the id of the member
        :type member_id: ascii bytes
        :returns: capabilities and statistics of a member
        :rtype: CoordAsyncResult
        """
        raise tooz.NotImplemented

    @staticmethod
    def update_capabilities(group_id, capabilities):
        """Update member capabilities in the specified group.

        :param group_id: the id of the group of the current member
        :type group_id: ascii bytes
        :param capabilities: the capabilities of the updated member
        :type capabilities: object
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

        :return: The number of seconds to wait before sending a new heartbeat.
        """
        pass


@six.add_metaclass(abc.ABCMeta)
class CoordAsyncResult(object):
    """Representation of an asynchronous task.

    Every call API returns an CoordAsyncResult object on which the result or
    the status of the task can be requested.

    """

    @abc.abstractmethod
    def get(self, timeout=None):
        """Retrieve the result of the corresponding asynchronous call.

        :param timeout: block until the timeout expire.
        :type timeout: float
        """

    @abc.abstractmethod
    def done(self):
        """Returns True if the task is done, False otherwise."""


class CoordinatorResult(CoordAsyncResult):
    """Asynchronous result that references a future."""

    def __init__(self, fut, failure_translator=None):
        self._fut = fut
        self._failure_translator = failure_translator

    def get(self, timeout=None):
        try:
            if self._failure_translator:
                with self._failure_translator():
                    return self._fut.result(timeout=timeout)
            else:
                return self._fut.result(timeout=timeout)
        except futures.TimeoutError as e:
            utils.raise_with_cause(OperationTimedOut,
                                   encodeutils.exception_to_unicode(e),
                                   cause=e)

    def done(self):
        return self._fut.done()


class CoordinationDriverWithExecutor(CoordinationDriver):

    EXCLUDE_OPTIONS = None

    def __init__(self, member_id, parsed_url, options):
        self._options = utils.collapse(options, exclude=self.EXCLUDE_OPTIONS)
        self._executor = utils.ProxyExecutor.build(
            self.__class__.__name__, self._options)
        super(CoordinationDriverWithExecutor, self).__init__(
            member_id, parsed_url, options)

    def start(self, start_heart=False):
        self._executor.start()
        super(CoordinationDriverWithExecutor, self).start(start_heart)

    def stop(self):
        super(CoordinationDriverWithExecutor, self).stop()
        self._executor.stop()


class CoordinationDriverCachedRunWatchers(CoordinationDriver):
    """Coordination driver with a `run_watchers` implementation.

    This implementation of `run_watchers` is based on a cache of the group
    members between each run of `run_watchers` that is being updated between
    each run.

    """

    def __init__(self, member_id, parsed_url, options):
        super(CoordinationDriverCachedRunWatchers, self).__init__(
            member_id, parsed_url, options)
        # A cache for group members
        self._group_members = collections.defaultdict(set)
        self._joined_groups = set()

    def _init_watch_group(self, group_id):
        if group_id not in self._group_members:
            members = self.get_members(group_id)
            self._group_members[group_id] = members.get()

    def watch_join_group(self, group_id, callback):
        self._init_watch_group(group_id)
        super(CoordinationDriverCachedRunWatchers, self).watch_join_group(
            group_id, callback)

    def unwatch_join_group(self, group_id, callback):
        super(CoordinationDriverCachedRunWatchers, self).unwatch_join_group(
            group_id, callback)

        if (not self._has_hooks_for_group(group_id) and
           group_id in self._group_members):
            del self._group_members[group_id]

    def watch_leave_group(self, group_id, callback):
        self._init_watch_group(group_id)
        super(CoordinationDriverCachedRunWatchers, self).watch_leave_group(
            group_id, callback)

    def unwatch_leave_group(self, group_id, callback):
        super(CoordinationDriverCachedRunWatchers, self).unwatch_leave_group(
            group_id, callback)

        if (not self._has_hooks_for_group(group_id) and
           group_id in self._group_members):
            del self._group_members[group_id]

    def run_watchers(self, timeout=None):
        with timeutils.StopWatch(duration=timeout) as w:
            result = []
            group_with_hooks = set(self._hooks_join_group.keys()).union(
                set(self._hooks_leave_group.keys()))
            for group_id in group_with_hooks:
                try:
                    group_members = self.get_members(group_id).get(
                        timeout=w.leftover(return_none=True))
                except GroupNotCreated:
                    group_members = set()
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
    :type member_id: ascii bytes
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


# TODO(harlowja): We'll have to figure out a way to remove this 'alias' at
# some point in the future (when we have a better way to tell people it has
# moved without messing up their exception catching hierarchy).
ToozError = tooz.ToozError


class ToozDriverChosenPoorly(tooz.ToozError):
    """Raised when a driver does not match desired characteristics."""


class ToozConnectionError(tooz.ToozError):
    """Exception raised when the client cannot connect to the server."""


class OperationTimedOut(tooz.ToozError):
    """Exception raised when an operation times out."""


class LockAcquireFailed(tooz.ToozError):
    """Exception raised when a lock acquire fails in a context manager."""


class GroupNotCreated(tooz.ToozError):
    """Exception raised when the caller request an nonexistent group."""
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupNotCreated, self).__init__(
            "Group %s does not exist" % group_id)


class GroupAlreadyExist(tooz.ToozError):
    """Exception raised trying to create an already existing group."""
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupAlreadyExist, self).__init__(
            "Group %s already exists" % group_id)


class MemberAlreadyExist(tooz.ToozError):
    """Exception raised trying to join a group already joined."""
    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id
        super(MemberAlreadyExist, self).__init__(
            "Member %s has already joined %s" %
            (member_id, group_id))


class MemberNotJoined(tooz.ToozError):
    """Exception raised trying to access a member not in a group."""
    def __init__(self, group_id, member_id):
        self.group_id = group_id
        self.member_id = member_id
        super(MemberNotJoined, self).__init__("Member %s has not joined %s" %
                                              (member_id, group_id))


class GroupNotEmpty(tooz.ToozError):
    "Exception raised when the caller try to delete a group with members."
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupNotEmpty, self).__init__("Group %s is not empty" % group_id)


class WatchCallbackNotFound(tooz.ToozError):
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


# TODO(harlowja,jd): We'll have to figure out a way to remove this 'alias' at
# some point in the future (when we have a better way to tell people it has
# moved without messing up their exception catching hierarchy).
SerializationError = utils.SerializationError
