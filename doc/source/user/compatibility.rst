=============
Compatibility
=============

Grouping
========

APIs
----

* :py:meth:`~tooz.coordination.CoordinationDriver.watch_join_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.unwatch_join_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.watch_leave_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.unwatch_leave_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.create_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.get_groups`
* :py:meth:`~tooz.coordination.CoordinationDriver.join_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.leave_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.delete_group`
* :py:meth:`~tooz.coordination.CoordinationDriver.get_members`
* :py:meth:`~tooz.coordination.CoordinationDriver.get_member_capabilities`
* :py:meth:`~tooz.coordination.CoordinationDriver.update_capabilities`

Driver support
--------------

.. list-table::
   :header-rows: 1

   * - Driver
     - Supported
   * - :py:class:`~tooz.drivers.consul.ConsulDriver`
     - No
   * - :py:class:`~tooz.drivers.etcd.EtcdDriver`
     - No
   * - :py:class:`~tooz.drivers.file.FileDriver`
     - Yes
   * - :py:class:`~tooz.drivers.ipc.IPCDriver`
     - No
   * - :py:class:`~tooz.drivers.memcached.MemcachedDriver`
     - Yes
   * - :py:class:`~tooz.drivers.mysql.MySQLDriver`
     - No
   * - :py:class:`~tooz.drivers.pgsql.PostgresDriver`
     - No
   * - :py:class:`~tooz.drivers.redis.RedisDriver`
     - Yes
   * - :py:class:`~tooz.drivers.zake.ZakeDriver`
     - Yes
   * - :py:class:`~tooz.drivers.zookeeper.KazooDriver`
     - Yes

Leaders
=======

APIs
----

* :py:meth:`~tooz.coordination.CoordinationDriver.watch_elected_as_leader`
* :py:meth:`~tooz.coordination.CoordinationDriver.unwatch_elected_as_leader`
* :py:meth:`~tooz.coordination.CoordinationDriver.stand_down_group_leader`
* :py:meth:`~tooz.coordination.CoordinationDriver.get_leader`

Driver support
--------------

.. list-table::
   :header-rows: 1

   * - Driver
     - Supported
   * - :py:class:`~tooz.drivers.consul.ConsulDriver`
     - No
   * - :py:class:`~tooz.drivers.etcd.EtcdDriver`
     - No
   * - :py:class:`~tooz.drivers.file.FileDriver`
     - No
   * - :py:class:`~tooz.drivers.ipc.IPCDriver`
     - No
   * - :py:class:`~tooz.drivers.memcached.MemcachedDriver`
     - Yes
   * - :py:class:`~tooz.drivers.mysql.MySQLDriver`
     - No
   * - :py:class:`~tooz.drivers.pgsql.PostgresDriver`
     - No
   * - :py:class:`~tooz.drivers.redis.RedisDriver`
     - Yes
   * - :py:class:`~tooz.drivers.zake.ZakeDriver`
     - Yes
   * - :py:class:`~tooz.drivers.zookeeper.KazooDriver`
     - Yes

Locking
=======

APIs
----

* :py:meth:`~tooz.coordination.CoordinationDriver.get_lock`

Driver support
--------------

.. list-table::
   :header-rows: 1

   * - Driver
     - Supported
   * - :py:class:`~tooz.drivers.consul.ConsulDriver`
     - Yes
   * - :py:class:`~tooz.drivers.etcd.EtcdDriver`
     - Yes
   * - :py:class:`~tooz.drivers.file.FileDriver`
     - Yes
   * - :py:class:`~tooz.drivers.ipc.IPCDriver`
     - Yes
   * - :py:class:`~tooz.drivers.memcached.MemcachedDriver`
     - Yes
   * - :py:class:`~tooz.drivers.mysql.MySQLDriver`
     - Yes
   * - :py:class:`~tooz.drivers.pgsql.PostgresDriver`
     - Yes
   * - :py:class:`~tooz.drivers.redis.RedisDriver`
     - Yes
   * - :py:class:`~tooz.drivers.zake.ZakeDriver`
     - Yes
   * - :py:class:`~tooz.drivers.zookeeper.KazooDriver`
     - Yes
