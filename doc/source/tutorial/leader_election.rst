=================
 Leader Election
=================

Each group can elect its own leader. There can be only one leader at a time
in a group. Only members that are running for the election can be elected.
As soon as one of leader steps down or dies, a new member that was running
for the election will be elected.

.. literalinclude:: ../../../examples/leader_election.py
   :language: python

The method
:meth:`tooz.coordination.CoordinationDriver.watch_elected_as_leader` allows
to register for a function to be called back when the member is elected as a
leader. Using this function indicates that the run is therefore running for
the election. The member can stop running by unregistering all its callbacks
with :meth:`tooz.coordination.CoordinationDriver.unwatch_elected_as_leader`.
It can also temporarily try to step down as a leader with
:meth:`tooz.coordination.CoordinationDriver.stand_down_group_leader`. If
another member is in the run for election, it may be elected instead.

To retrieve the leader of a group, even when not being part of the group,
the method :meth:`tooz.coordination.CoordinationDriver.get_leader()` can be
used.
