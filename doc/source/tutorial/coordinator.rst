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

This example program loads a basic coordinataor using the ZooKeeper based
driver.

.. literalinclude:: ../../../examples/coordinator.py
   :language: python

The second argument passed to the coordinator must be a unique identifier
identifying the running program.

After the coordinator is created, it can be used to use the various features
provided.

In order to keep the connection to the coordination server active, you must
call regularly the :meth:`~tooz.coordination.CoordinationDriver.heartbeat`
method. This will ensure that the coordinator is not considered dead by
other program participating in the coordination.

.. literalinclude:: ../../../examples/coordinator_heartbeat.py
   :language: python

We use a pretty simple mechanism in this example to send a heartbeat every
once in a while, but depending on your application, you may want to send the
heartbeat at different moment or intervals.

Note that certain drivers, such as `memcached` are heavily based on timeout,
so the interval used to run the heartbeat is important.
