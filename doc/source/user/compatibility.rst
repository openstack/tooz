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

===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
:py:class:`~tooz.drivers.consul.ConsulDriver`    :py:class:`~tooz.drivers.etcd.EtcdDriver`    :py:class:`~tooz.drivers.file.FileDriver`    :py:class:`~tooz.drivers.ipc.IPCDriver`    :py:class:`~tooz.drivers.memcached.MemcachedDriver`    :py:class:`~tooz.drivers.mysql.MySQLDriver`    :py:class:`~tooz.drivers.pgsql.PostgresDriver`    :py:class:`~tooz.drivers.redis.RedisDriver`    :py:class:`~tooz.drivers.zake.ZakeDriver`    :py:class:`~tooz.drivers.zookeeper.KazooDriver`
===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
No                                               No                                           Yes                                          No                                         Yes                                                    No                                             No                                                Yes                                            Yes                                          Yes
===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================

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

===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
:py:class:`~tooz.drivers.consul.ConsulDriver`    :py:class:`~tooz.drivers.etcd.EtcdDriver`    :py:class:`~tooz.drivers.file.FileDriver`    :py:class:`~tooz.drivers.ipc.IPCDriver`    :py:class:`~tooz.drivers.memcached.MemcachedDriver`    :py:class:`~tooz.drivers.mysql.MySQLDriver`    :py:class:`~tooz.drivers.pgsql.PostgresDriver`    :py:class:`~tooz.drivers.redis.RedisDriver`    :py:class:`~tooz.drivers.zake.ZakeDriver`    :py:class:`~tooz.drivers.zookeeper.KazooDriver`
===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
No                                               No                                           No                                           No                                         Yes                                                    No                                             No                                                Yes                                            Yes                                          Yes
===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================

Locking
=======

APIs
----

* :py:meth:`~tooz.coordination.CoordinationDriver.get_lock`

Driver support
--------------

===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
:py:class:`~tooz.drivers.consul.ConsulDriver`    :py:class:`~tooz.drivers.etcd.EtcdDriver`    :py:class:`~tooz.drivers.file.FileDriver`    :py:class:`~tooz.drivers.ipc.IPCDriver`    :py:class:`~tooz.drivers.memcached.MemcachedDriver`    :py:class:`~tooz.drivers.mysql.MySQLDriver`    :py:class:`~tooz.drivers.pgsql.PostgresDriver`    :py:class:`~tooz.drivers.redis.RedisDriver`    :py:class:`~tooz.drivers.zake.ZakeDriver`    :py:class:`~tooz.drivers.zookeeper.KazooDriver`
===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
Yes                                              Yes                                          Yes                                          Yes                                        Yes                                                    Yes                                            Yes                                               Yes                                            Yes                                          Yes
===============================================  ===========================================  ===========================================  =========================================  =====================================================  =============================================  ================================================  =============================================  ===========================================  =================================================
