=======
Drivers
=======

Tooz is provided with several drivers implementing the provided coordination
API. While all drivers provides the same set of features with respect to the
API, some of them have different properties:

* `zookeeper`_ is the reference implementation and provides the most solid
  features as it's possible to build a cluster of ZooKeeper that is
  resilient towards network partitions for example.

* `memcached`_ is a basic implementation and provides little resiliency, though
  it's much simpler to setup. A lot of the features provided in tooz are based
  on timeout (heartbeats, locks, etc) so are less resilient than other
  backends.

* `redis`_ is a basic implementation and provides reasonable resiliency
  when used with redis-sentinel. A lot of the features provided in tooz are
  based on timeout (heartbeats, locks, etc) so are less resilient than other
  backends.

* `ipc` is based on Posix IPC and only implements a lock mechanism for now, and
  some basic group primitives (with huge limitations). The lock can only be
  distributed locally to a computer processes.

* `file` is based on file and only implements a lock based on POSIX or Window
  file locking for now. The lock can only be distributed locally to a computer
  processes.

* `zake`_ is a driver using a fake implementation of ZooKeeper and can be
  used to use Tooz in your unit tests suite for example.

* `postgresql`_ is a driver providing only distributed lock (for now)
  and based on the PostgreSQL database server.

* `mysql`_ is a driver providing only distributed lock (for now)
  and based on the MySQL database server.

.. _zookeeper: http://zookeeper.apache.org/
.. _memcached: http://memcached.org/
.. _zake: https://pypi.python.org/pypi/zake
.. _redis: http://redis.io
.. _postgresql: http://postgresql.org
.. _mysql: http://mysql.org
