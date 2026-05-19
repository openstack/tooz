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

from __future__ import annotations

import abc
import collections
from collections.abc import Callable, Iterable
from concurrent import futures
import contextlib
import enum
import logging
import threading
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar
import urllib.parse

from oslo_utils import netutils
from oslo_utils import timeutils
from stevedore import driver
import tenacity

import tooz
from tooz import _retry
from tooz import partitioner
from tooz import utils

if TYPE_CHECKING:
    from tooz import locking

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

    * Operations can take effect before or after completion - but all
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


class Event:
    """Base class for events."""


class MemberJoinedGroup(Event):
    """A member joined a group event."""

    def __init__(self, group_id: bytes, member_id: bytes) -> None:
        self.group_id = group_id
        self.member_id = member_id

    def __repr__(self) -> str:
        group = (
            self.group_id.decode()
            if isinstance(self.group_id, bytes)
            else self.group_id
        )
        member = (
            self.member_id.decode()
            if isinstance(self.member_id, bytes)
            else self.member_id
        )
        return f"<{self.__class__.__name__}: group {group}: +member {member}>"


class MemberLeftGroup(Event):
    """A member left a group event."""

    def __init__(self, group_id: bytes, member_id: bytes) -> None:
        self.group_id = group_id
        self.member_id = member_id

    def __repr__(self) -> str:
        group = (
            self.group_id.decode()
            if isinstance(self.group_id, bytes)
            else self.group_id
        )
        member = (
            self.member_id.decode()
            if isinstance(self.member_id, bytes)
            else self.member_id
        )
        return f"<{self.__class__.__name__}: group {group}: -member {member}>"


class LeaderElected(Event):
    """A leader as been elected."""

    def __init__(self, group_id: bytes, member_id: bytes) -> None:
        self.group_id = group_id
        self.member_id = member_id


class Heart:
    """Coordination drivers main liveness pump (its heart)."""

    def __init__(
        self,
        driver: CoordinationDriver,
        thread_cls: type[threading.Thread] = threading.Thread,
        event_cls: type[threading.Event] = threading.Event,
    ) -> None:
        self._thread_cls = thread_cls
        self._dead = event_cls()
        self._runner: threading.Thread | None = None
        self._driver = driver
        self._beats = 0

    @property
    def beats(self) -> int:
        """How many times the heart has beaten."""
        return self._beats

    def is_alive(self) -> bool:
        """Returns if the heart is beating."""
        return not (self._runner is None or not self._runner.is_alive())

    def _beat_forever_until_stopped(self) -> None:
        """Inner beating loop."""
        retry = tenacity.Retrying(
            wait=tenacity.wait_fixed(1),
            before_sleep=tenacity.before_sleep_log(LOG, logging.WARNING),
        )
        while not self._dead.is_set():
            with timeutils.StopWatch() as w:
                wait_until_next_beat = retry(self._driver.heartbeat)
            assert wait_until_next_beat is not None  # narrow type
            ran_for = w.elapsed()
            has_to_sleep_for = wait_until_next_beat - ran_for
            if has_to_sleep_for < 0:
                LOG.warning(
                    "Heartbeating took too long to execute (it ran for"
                    " %0.2f seconds which is %0.2f seconds longer than"
                    " the next heartbeat idle time). This may cause"
                    " timeouts (in locks, leadership, ...) to"
                    " happen (which will not end well).",
                    ran_for,
                    ran_for - wait_until_next_beat,
                )
            self._beats += 1
            # NOTE(harlowja): use the event object for waiting and
            # not a sleep function since doing that will allow this code
            # to terminate early if stopped via the stop() method vs
            # having to wait until the sleep function returns.
            # NOTE(jd): Wait for only the half time of what we should.
            # This is a measure of safety, better be too soon than too late.
            self._dead.wait(has_to_sleep_for / 2.0)

    def start(self, thread_cls: type[threading.Thread] | None = None) -> None:
        """Starts the heart beating thread (noop if already started)."""
        if not self.is_alive():
            self._dead.clear()
            self._beats = 0
            if thread_cls is None:
                thread_cls = self._thread_cls
            self._runner = thread_cls(target=self._beat_forever_until_stopped)
            self._runner.daemon = True
            self._runner.start()

    def stop(self) -> None:
        """Requests the heart beating thread to stop beating."""
        self._dead.set()

    def wait(self, timeout: int | float | None = None) -> bool:
        """Wait up to given timeout for the heart beating thread to stop."""
        if self._runner is None:
            raise RuntimeError('runner must be initialized to wait')

        self._runner.join(timeout)
        return self._runner.is_alive()


T = TypeVar('T')
EventT = TypeVar('EventT', bound=Event, contravariant=True)


class EventCallback(Protocol, Generic[EventT]):
    __name__: str

    def __call__(self, event: EventT) -> None: ...


class Hooks(list[EventCallback[Any]]):
    def run(self, *args: Any, **kwargs: Any) -> list[Any]:
        return list(map(lambda cb: cb(*args, **kwargs), self))


Capabilities = dict[str, Any]


class CoordinationDriver:
    requires_beating = False
    """
    Usage requirement that if true requires that the :py:meth:`~.heartbeat`
    be called periodically (at a given rate) to avoid locks, sessions and
    other from being automatically closed/discarded by the coordinators
    backing store.
    """

    CHARACTERISTICS: tuple[Characteristics, ...] = ()
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    # TODO(stephenfin): Fix type of parsed_url
    # https://review.opendev.org/c/openstack/oslo.utils/+/980011
    def __init__(
        self, member_id: bytes, parsed_url: Any, options: dict[str, Any]
    ) -> None:
        super().__init__()
        self._member_id = member_id
        self._started = False
        self._hooks_join_group: collections.defaultdict[bytes, Hooks] = (
            collections.defaultdict(Hooks)
        )
        self._hooks_leave_group: collections.defaultdict[bytes, Hooks] = (
            collections.defaultdict(Hooks)
        )
        self._hooks_elected_leader: collections.defaultdict[bytes, Hooks] = (
            collections.defaultdict(Hooks)
        )
        self._joined_groups: set[bytes] = set()
        self.requires_beating = (
            CoordinationDriver.heartbeat != self.__class__.heartbeat
        )
        self.heart = Heart(self)

    def _has_hooks_for_group(self, group_id: bytes) -> bool:
        return (
            group_id in self._hooks_join_group
            or group_id in self._hooks_leave_group
        )

    def join_partitioned_group(
        self,
        group_id: bytes,
        weight: int = 1,
        partitions: int = partitioner.Partitioner.DEFAULT_PARTITION_NUMBER,
    ) -> partitioner.Partitioner:
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

    def leave_partitioned_group(
        self, partitioner: partitioner.Partitioner
    ) -> None:
        """Leave a partitioned group.

        This leaves the partitioned group and stop the partitioner.
        :param group_id: The group to create a partitioner for.
        """
        leave = self.leave_group(partitioner.group_id)
        partitioner.stop()
        leave.get()

    def run_watchers(self, timeout: float | None = None) -> list[Any]:
        """Run the watchers callback.

        This may also activate :py:meth:`.run_elect_coordinator` (depending
        on driver implementation).
        """
        raise tooz.NotImplemented("not implemented")

    def run_elect_coordinator(self) -> None:
        """Try to leader elect this coordinator & activate hooks on success."""
        raise tooz.NotImplemented("not implemented")

    def watch_join_group(
        self, group_id: bytes, callback: EventCallback[MemberJoinedGroup]
    ) -> None:
        """Call a function when group_id sees a new member joined.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member joins this group

        """
        self._hooks_join_group[group_id].append(callback)

    def unwatch_join_group(
        self, group_id: bytes, callback: EventCallback[MemberJoinedGroup]
    ) -> None:
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

    def watch_leave_group(
        self, group_id: bytes, callback: EventCallback[MemberLeftGroup]
    ) -> None:
        """Call a function when group_id sees a new member leaving.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_leave_group[group_id].append(callback)

    def unwatch_leave_group(
        self, group_id: bytes, callback: EventCallback[MemberLeftGroup]
    ) -> None:
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

    def watch_elected_as_leader(
        self, group_id: bytes, callback: EventCallback[LeaderElected]
    ) -> None:
        """Call a function when member gets elected as leader.

        The callback functions will be executed when `run_watchers` is
        called.

        :param group_id: The group id to watch
        :param callback: The function to execute when a member leaves this
                         group

        """
        self._hooks_elected_leader[group_id].append(callback)

    def unwatch_elected_as_leader(
        self, group_id: bytes, callback: EventCallback[LeaderElected]
    ) -> None:
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

    def stand_down_group_leader(self, group_id: bytes) -> bool:
        """Stand down as the group leader if we are.

        :param group_id: The group where we don't want to be a leader anymore
        """
        raise tooz.NotImplemented("not implemented")

    @property
    def is_started(self) -> bool:
        return self._started

    def start(self, start_heart: bool = False) -> None:
        """Start the service engine.

        If needed, the establishment of a connection to the servers
        is initiated.
        """
        if self._started:
            raise tooz.ToozError(
                "Can not start a driver which has not been stopped"
            )
        self._start()
        if self.requires_beating and start_heart:
            self.heart.start()
        self._started = True
        # Tracks which group are joined
        self._joined_groups = set()

    def _start(self) -> None:
        pass

    def stop(self) -> None:
        """Stop the service engine.

        If needed, the connection to servers is closed and the client will
        disappear from all joined groups.
        """
        if not self._started:
            raise tooz.ToozError(
                "Can not stop a driver which has not been started"
            )
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

    def _stop(self) -> None:
        pass

    def create_group(self, group_id: bytes) -> CoordAsyncResult[None]:
        """Request the creation of a group asynchronously.

        :param group_id: the id of the group to create
        :returns: A CoordAsyncResult of None.
        """
        raise tooz.NotImplemented("not implemented")

    def get_groups(self) -> CoordAsyncResult[list[bytes]]:
        """Return the list composed by all groups ids asynchronously.

        :returns: the list of all created group ids
        """
        raise tooz.NotImplemented("not implemented")

    def join_group(
        self, group_id: bytes, capabilities: Capabilities | None = None
    ) -> CoordAsyncResult[None]:
        """Join a group and establish group membership asynchronously.

        :param group_id: the id of the group to join
        :param capabilities: the capabilities of the joined member; a
            :class:`dict` mapping string keys to arbitrary values.
        :returns: None
        """
        raise tooz.NotImplemented("not implemented")

    @_retry.retry()
    def join_group_create(
        self, group_id: bytes, capabilities: Capabilities | None = None
    ) -> None:
        """Join a group and create it if necessary.

        See :meth:`join_group` for the description of the *capabilities*
        parameter.

        If the group cannot be joined because it does not exist, it is created
        before being joined.

        This function will keep retrying until it can create the group and join
        it. Since nothing is transactional, it may have to retry several times
        if another member is creating/deleting the group at the same time.

        :param group_id: Identifier of the group to join and create
        :param capabilities: the capabilities of the joined member; see
            :meth:`join_group`
        :returns: None
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

    def leave_group(self, group_id: bytes) -> CoordAsyncResult[None]:
        """Leave a group asynchronously.

        :param group_id: the id of the group to leave
        :returns: None
        """
        raise tooz.NotImplemented("not implemented")

    def delete_group(self, group_id: bytes) -> CoordAsyncResult[None]:
        """Delete a group asynchronously.

        :param group_id: the id of the group to leave
        :returns: None
        """
        raise tooz.NotImplemented("not implemented")

    def get_members(self, group_id: bytes) -> CoordAsyncResult[set[bytes]]:
        """Return the set of all member ids of a group asynchronously.

        :returns: set of all member ids in the specified group
        """
        raise tooz.NotImplemented("not implemented")

    def get_member_capabilities(
        self, group_id: bytes, member_id: bytes
    ) -> CoordAsyncResult[Capabilities | None]:
        """Return the capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :param member_id: the id of the member
        :returns: capabilities of a member
        """
        raise tooz.NotImplemented("not implemented")

    def get_member_info(
        self, group_id: bytes, member_id: bytes
    ) -> CoordAsyncResult[dict[str, Any]]:
        """Return the statistics and capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :param member_id: the id of the member
        :returns: capabilities and statistics of a member
        """
        raise tooz.NotImplemented("not implemented")

    def update_capabilities(
        self, group_id: bytes, capabilities: Capabilities
    ) -> CoordAsyncResult[None]:
        """Update member capabilities in the specified group.

        :param group_id: the id of the group of the current member
        :param capabilities: the updated capabilities of the member; see
            :meth:`join_group`
        :returns: None
        """
        raise tooz.NotImplemented("not implemented")

    def get_leader(self, group_id: bytes) -> CoordAsyncResult[bytes]:
        """Return the leader for a group.

        :param group_id: the id of the group:
        :returns: the leader
        """
        raise tooz.NotImplemented("not implemented")

    def get_lock(self, name: bytes) -> locking.Lock:
        """Return a distributed lock.

        This is a exclusive lock, a second call to acquire() will block or
        return False.

        :param name: The lock name that is used to identify it across all
                     nodes.
        """
        raise tooz.NotImplemented("not implemented")

    def heartbeat(self) -> float | None:
        """Update member status to indicate it is still alive.

        Method to run once in a while to be sure that the member is not dead
        and is still an active member of a group.

        :return: The number of seconds to wait before sending a new heartbeat.
        """
        pass


class CoordAsyncResult(Generic[T], metaclass=abc.ABCMeta):
    """Representation of an asynchronous task.

    Every call API returns an CoordAsyncResult object on which the result or
    the status of the task can be requested.

    """

    @abc.abstractmethod
    def get(self, timeout: float | None = None) -> T:
        """Retrieve the result of the corresponding asynchronous call.

        :param timeout: block until the timeout expire.
        """

    @abc.abstractmethod
    def done(self) -> bool:
        """Returns True if the task is done, False otherwise."""


class CoordinatorResult(CoordAsyncResult[T]):
    """Asynchronous result that references a future."""

    def __init__(
        self,
        fut: futures.Future[T],
        failure_translator: (
            Callable[[], contextlib.AbstractContextManager[Any]] | None
        ) = None,
    ) -> None:
        self._fut = fut
        self._failure_translator = failure_translator

    def get(self, timeout: float | None = None) -> T:
        try:
            if self._failure_translator:
                with self._failure_translator():
                    return self._fut.result(timeout=timeout)
            else:
                return self._fut.result(timeout=timeout)
        except futures.TimeoutError as e:
            utils.raise_with_cause(OperationTimedOut, str(e), cause=e)

    def done(self) -> bool:
        return self._fut.done()


class CoordinationDriverWithExecutor(CoordinationDriver):
    EXCLUDE_OPTIONS: frozenset[str] | None = None

    # TODO(stephenfin): Fix type of parsed_url
    # https://review.opendev.org/c/openstack/oslo.utils/+/980011
    def __init__(
        self, member_id: bytes, parsed_url: Any, options: dict[str, Any]
    ) -> None:
        self._options: dict[str, Any] = utils.collapse(
            options, exclude=self.EXCLUDE_OPTIONS
        )
        self._executor: utils.ProxyExecutor = utils.ProxyExecutor.build(
            self.__class__.__name__, self._options
        )
        super().__init__(member_id, parsed_url, options)

    def start(self, start_heart: bool = False) -> None:
        self._executor.start()
        super().start(start_heart)

    def stop(self) -> None:
        super().stop()
        self._executor.stop()


class CoordinationDriverCachedRunWatchers(CoordinationDriver):
    """Coordination driver with a `run_watchers` implementation.

    This implementation of `run_watchers` is based on a cache of the group
    members between each run of `run_watchers` that is being updated between
    each run.

    """

    # TODO(stephenfin): Fix type of parsed_url
    # https://review.opendev.org/c/openstack/oslo.utils/+/980011
    def __init__(
        self, member_id: bytes, parsed_url: Any, options: dict[str, Any]
    ) -> None:
        super().__init__(member_id, parsed_url, options)
        # A cache for group members
        self._group_members: collections.defaultdict[bytes, set[bytes]] = (
            collections.defaultdict(set)
        )
        self._joined_groups = set()

    def _init_watch_group(self, group_id: bytes) -> None:
        if group_id not in self._group_members:
            members = self.get_members(group_id)
            self._group_members[group_id] = members.get()

    def watch_join_group(
        self, group_id: bytes, callback: EventCallback[MemberJoinedGroup]
    ) -> None:
        self._init_watch_group(group_id)
        super().watch_join_group(group_id, callback)

    def unwatch_join_group(
        self, group_id: bytes, callback: EventCallback[MemberJoinedGroup]
    ) -> None:
        super().unwatch_join_group(group_id, callback)

        if (
            not self._has_hooks_for_group(group_id)
            and group_id in self._group_members
        ):
            del self._group_members[group_id]

    def watch_leave_group(
        self, group_id: bytes, callback: EventCallback[MemberLeftGroup]
    ) -> None:
        self._init_watch_group(group_id)
        super().watch_leave_group(group_id, callback)

    def unwatch_leave_group(
        self, group_id: bytes, callback: EventCallback[MemberLeftGroup]
    ) -> None:
        super().unwatch_leave_group(group_id, callback)

        if (
            not self._has_hooks_for_group(group_id)
            and group_id in self._group_members
        ):
            del self._group_members[group_id]

    def run_watchers(self, timeout: float | None = None) -> list[Any]:
        with timeutils.StopWatch(duration=timeout) as w:
            result = []
            group_with_hooks = set(self._hooks_join_group.keys()).union(
                set(self._hooks_leave_group.keys())
            )
            for group_id in group_with_hooks:
                try:
                    group_members = self.get_members(group_id).get(
                        timeout=w.leftover(return_none=True)
                    )
                except GroupNotCreated:
                    group_members = set()
                if (
                    group_id in self._joined_groups
                    and self._member_id not in group_members
                ):
                    self._joined_groups.discard(group_id)
                old_group_members = self._group_members.get(group_id, set())
                for member_id in group_members - old_group_members:
                    result.extend(
                        self._hooks_join_group[group_id].run(
                            MemberJoinedGroup(group_id, member_id)
                        )
                    )
                for member_id in old_group_members - group_members:
                    result.extend(
                        self._hooks_leave_group[group_id].run(
                            MemberLeftGroup(group_id, member_id)
                        )
                    )
                self._group_members[group_id] = group_members
            return result


def get_coordinator(
    backend_url: str,
    member_id: bytes,
    characteristics: Iterable[Characteristics] | None = None,
    **kwargs: Any,
) -> CoordinationDriver:
    """Initialize and load the backend.

    :param backend_url: the backend URL to use
    :param member_id: the id of the member
    :param characteristics: set
    :param kwargs: additional coordinator options (these take precedence over
                   options of the **same** name found in the ``backend_url``
                   arguments query string)
    """
    parsed_url = netutils.urlsplit(backend_url)
    parsed_qs = urllib.parse.parse_qs(parsed_url.query)
    if kwargs:
        options = {}
        for k, v in kwargs.items():
            options[k] = [v]
        for k, v in parsed_qs.items():
            if k not in options:
                options[k] = v
    else:
        options = parsed_qs

    m: driver.DriverManager[CoordinationDriver]
    m = driver.DriverManager(
        namespace=TOOZ_BACKENDS_NAMESPACE,
        name=parsed_url.scheme,
        invoke_on_load=True,
        invoke_args=(member_id, parsed_url, options),
    )
    d = m.driver
    assert isinstance(d, CoordinationDriver)  # narrow type
    driver_characteristics = set(getattr(d, 'CHARACTERISTICS', set()))
    characteristics_set: set[Characteristics] = (
        set(characteristics) if characteristics is not None else set()
    )
    missing_characteristics = characteristics_set - driver_characteristics
    if missing_characteristics:
        raise ToozDriverChosenPoorly(
            f"Desired characteristics {characteristics_set} is not a strict "
            f"subset of driver characteristics {driver_characteristics}, "
            f"{missing_characteristics} characteristics were not found"
        )
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

    # TODO(stephenfin): Make this only accept str
    def __init__(self, group_id: str | bytes) -> None:
        self.group_id = (
            group_id.decode() if isinstance(group_id, bytes) else group_id
        )
        super().__init__(f"Group {self.group_id} does not exist")


class GroupAlreadyExist(tooz.ToozError):
    """Exception raised trying to create an already existing group."""

    # TODO(stephenfin): Make this only accept str
    def __init__(self, group_id: str | bytes) -> None:
        self.group_id = (
            group_id.decode() if isinstance(group_id, bytes) else group_id
        )
        super().__init__(f"Group {self.group_id} already exists")


class MemberAlreadyExist(tooz.ToozError):
    """Exception raised trying to join a group already joined."""

    # TODO(stephenfin): Make this only accept str
    def __init__(self, group_id: str | bytes, member_id: str | bytes) -> None:
        self.group_id = (
            group_id.decode() if isinstance(group_id, bytes) else group_id
        )
        self.member_id = (
            member_id.decode() if isinstance(member_id, bytes) else member_id
        )
        super().__init__(
            f"Member {self.member_id} has already joined {self.group_id}"
        )


class MemberNotJoined(tooz.ToozError):
    """Exception raised trying to access a member not in a group."""

    # TODO(stephenfin): Make this only accept str
    def __init__(self, group_id: str | bytes, member_id: str | bytes) -> None:
        self.group_id = (
            group_id.decode() if isinstance(group_id, bytes) else group_id
        )
        self.member_id = (
            member_id.decode() if isinstance(member_id, bytes) else member_id
        )
        super().__init__(
            f"Member {self.member_id} has not joined {self.group_id}"
        )


class GroupNotEmpty(tooz.ToozError):
    "Exception raised when the caller try to delete a group with members."

    # TODO(stephenfin): Make this only accept str
    def __init__(self, group_id: str | bytes) -> None:
        self.group_id = (
            group_id.decode() if isinstance(group_id, bytes) else group_id
        )
        super().__init__(f"Group {self.group_id} is not empty")


class WatchCallbackNotFound(tooz.ToozError):
    """Exception raised when unwatching a group.

    Raised when the caller tries to unwatch a group with a callback that
    does not exist.

    """

    def __init__(self, group_id: bytes, callback: EventCallback[Any]) -> None:
        self.group_id = (
            group_id.decode() if isinstance(group_id, bytes) else group_id
        )
        self.callback = callback
        super().__init__(
            f"Callback {callback.__name__} is not registered on group "
            f"{self.group_id}"
        )


# TODO(harlowja,jd): We'll have to figure out a way to remove this 'alias' at
# some point in the future (when we have a better way to tell people it has
# moved without messing up their exception catching hierarchy).
SerializationError = utils.SerializationError
