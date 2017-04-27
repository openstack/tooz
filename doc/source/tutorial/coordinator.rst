=======================
 Creating A Coordinator
=======================

The principal object provided by tooz is the *coordinator*. It allows you to
use various features, such as group membership, leader election or
distributed locking.

The features provided by tooz coordinator are implemented using different
drivers. When creating a coordinator, you need to specify which back-end
driver you want it to use. Different drivers may provide different set of
capabilities.

If a driver does not support a feature, it will raise a
:class:`~tooz.NotImplemented` exception.

This example program loads a basic coordinator using the ZooKeeper based
driver.

.. literalinclude:: ../../../examples/coordinator.py
   :language: python

The second argument passed to the coordinator must be a unique identifier
identifying the running program.

After the coordinator is created, it can be used to use the various features
provided.

In order to keep the connection to the coordination server active, the method
:meth:`~tooz.coordination.CoordinationDriver.heartbeat` method must be called
regularly. This will ensure that the coordinator is not considered dead by
other program participating in the coordination. Unless you want to call it
manually, you can use tooz builtin heartbeat manager by passing the
`start_heart` argument.

.. literalinclude:: ../../../examples/coordinator_heartbeat.py
   :language: python

heartbeat at different moment or intervals.

Note that certain drivers, such as `memcached` are heavily based on timeout,
so the interval used to run the heartbeat is important.
