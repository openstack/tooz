=======
Drivers
=======

Tooz is provided with several drivers implementing the provided coordination
API. While all drivers provides the same set of features with respect to the
API, some of them have different characteristics:

Zookeeper
---------

**Driver:** :py:class:`tooz.drivers.zookeeper.KazooDriver`

**Characteristics:**

:py:attr:`tooz.drivers.zookeeper.KazooDriver.CHARACTERISTICS`

**Entrypoint name:** ``zookeeper`` or ``kazoo``

**Summary:**

The zookeeper is the reference implementation and provides the most solid
features as it's possible to build a cluster of zookeeper servers that is
resilient towards network partitions for example.

**Test driver:** :py:class:`tooz.drivers.zake.ZakeDriver`

**Characteristics:**

:py:attr:`tooz.drivers.zake.ZakeDriver.CHARACTERISTICS`

**Test driver entrypoint name:** ``zake``

Considerations
~~~~~~~~~~~~~~

- Primitives are based on sessions (and typically require careful selection
  of session heartbeat periodicity and server side configuration of session
  expiry).

Memcached
---------

**Driver:** :py:class:`tooz.drivers.memcached.MemcachedDriver`

**Characteristics:**

:py:attr:`tooz.drivers.memcached.MemcachedDriver.CHARACTERISTICS`

**Entrypoint name:** ``memcached``

**Summary:**

The memcached driver is a basic implementation and provides little
resiliency, though it's much simpler to setup. A lot of the features provided
in tooz are based on timeout (heartbeats, locks, etc) so are less resilient
than other backends.

Considerations
~~~~~~~~~~~~~~

- Less resilient than other backends such as zookeeper and redis.
- Primitives are often based on TTL(s) that may expire before
  being renewed.
- Lacks certain primitives (compare and delete) so certain functionality
  is fragile and/or broken due to this.

Redis
-----

**Driver:** :py:class:`tooz.drivers.redis.RedisDriver`

**Characteristics:**

:py:attr:`tooz.drivers.redis.RedisDriver.CHARACTERISTICS`

**Entrypoint name:** ``redis``

**Summary:**

The redis driver is a basic implementation and provides reasonable resiliency
when used with `redis-sentinel`_. A lot of the features provided in tooz are
based on timeout (heartbeats, locks, etc) so are less resilient than other
backends.

Considerations
~~~~~~~~~~~~~~

- Less resilient than other backends such as zookeeper.
- Primitives are often based on TTL(s) that may expire before
  being renewed.

IPC
---

**Driver:** :py:class:`tooz.drivers.ipc.IPCDriver`

**Characteristics:** :py:attr:`tooz.drivers.ipc.IPCDriver.CHARACTERISTICS`

**Entrypoint name:** ``ipc``

**Summary:**

The IPC driver is based on Posix IPC API and implements a lock
mechanism and some basic group primitives (with **huge**
limitations).

Considerations
~~~~~~~~~~~~~~

- The lock can **only** be distributed locally to a computer
  processes.

File
----

**Driver:** :py:class:`tooz.drivers.file.FileDriver`

**Characteristics:** :py:attr:`tooz.drivers.file.FileDriver.CHARACTERISTICS`

**Entrypoint name:** ``file``

**Summary:**

The file driver is a **simple** driver based on files and directories. It
implements a lock based on POSIX or Window file level locking
mechanism and some basic group primitives (with **huge**
limitations).

Considerations
~~~~~~~~~~~~~~

- The lock can **only** be distributed locally to a computer processes.
- Certain concepts provided by it are **not** crash tolerant.

PostgreSQL
----------

**Driver:** :py:class:`tooz.drivers.pgsql.PostgresDriver`

**Characteristics:**

:py:attr:`tooz.drivers.pgsql.PostgresDriver.CHARACTERISTICS`

**Entrypoint name:** ``postgresql``

**Summary:**

The postgresql driver is a driver providing only a distributed lock (for now)
and is based on the `PostgreSQL database server`_ and its API(s) that provide
for `advisory locks`_ to be created and used by applications. When a lock is
acquired it will release either when explicitly released or automatically when
the database session ends (for example if the program using the lock crashes).

Considerations
~~~~~~~~~~~~~~

- Lock that may be acquired restricted by
  ``max_locks_per_transaction * (max_connections + max_prepared_transactions)``
  upper bound (PostgreSQL server configuration settings).

MySQL
-----

**Driver:**  :py:class:`tooz.drivers.mysql.MySQLDriver`

**Characteristics:** :py:attr:`tooz.drivers.mysql.MySQLDriver.CHARACTERISTICS`

**Entrypoint name:** ``mysql``

**Summary:**

The MySQL driver is a driver providing only distributed locks (for now)
and is based on the `MySQL database server`_ supported `get_lock`_
primitives. When a lock is acquired it will release either when explicitly
released or automatically when the database session ends (for example if
the program using the lock crashes).

Considerations
~~~~~~~~~~~~~~

- Does **not** work correctly on some MySQL versions.
- Does **not** work when MySQL replicates from one server to another (locks
  are local to the server that they were created from).

Etcd
----

**Driver:**  :py:class:`tooz.drivers.etcd.EtcdDriver`

**Characteristics:** :py:attr:`tooz.drivers.etcd.EtcdDriver.CHARACTERISTICS`

**Entrypoint name:** ``etcd``

**Summary:**

The etcd driver is a driver providing only distributed locks (for now)
and is based on the `etcd server`_ supported key/value storage and
associated primitives.

Consul
------

**Driver:**  :py:class:`tooz.drivers.consul.ConsulDriver`

**Characteristics:**

:py:attr:`tooz.drivers.consul.ConsulDriver.CHARACTERISTICS`

**Entrypoint name:** ``consul``

**Summary:**

The `consul`_ driver is a driver providing only distributed locks (for now)
and is based on the consul server key/value storage and/or
primitives. When a lock is acquired it will release either when explicitly
released or automatically when the consul session ends (for example if
the program using the lock crashes).

Characteristics
---------------

.. autoclass:: tooz.coordination.Characteristics

.. _etcd server: https://coreos.com/etcd/
.. _consul: https://www.consul.io/
.. _advisory locks: http://www.postgresql.org/docs/8.2/interactive/\
                    explicit-locking.html#ADVISORY-LOCKS
.. _get_lock: http://dev.mysql.com/doc/refman/5.5/en/\
              miscellaneous-functions.html#function_get-lock
.. _PostgreSQL database server: http://postgresql.org
.. _MySQL database server: http://mysql.org
.. _redis-sentinel: http://redis.io/topics/sentinel
