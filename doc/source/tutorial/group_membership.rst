=====================
 Group Membership
=====================

Basic operations
===================

One of the feature provided by the coordinator is the ability to handle
group membership. Once a group is created, any coordinator can join the
group and become a member of it. Any coordinator can be notified when a
members joins or leaves the group.

.. literalinclude:: ../../../examples/group_membership.py
   :language: python

Note that all the operation are asynchronous. That means you cannot be sure
that your group has been created or joined before you call the
:meth:`tooz.coordination.CoordAsyncResult.get` method.

You can also leave a group using the
:meth:`tooz.coordination.CoordinationDriver.leave_group` method. The list of
all available groups is retrievable via the
:meth:`tooz.coordination.CoordinationDriver.get_groups` method.

Watching Group Changes
======================
It's possible to watch and get notified when the member list of a group
changes. That's useful to run callback functions whenever something happens
in that group.


.. literalinclude:: ../../../examples/group_membership_watch.py
   :language: python

Using :meth:`tooz.coordination.CoordinationDriver.watch_join_group` and
:meth:`tooz.coordination.CoordinationDriver.watch_leave_group` your
application can be notified each time a member join or leave a group. To
stop watching an event, the two methods
:meth:`tooz.coordination.CoordinationDriver.unwatch_join_group` and
:meth:`tooz.coordination.CoordinationDriver.unwatch_leave_group` allow to
unregister a particular callback.
