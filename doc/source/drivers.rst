=======
Drivers
=======

Tooz is provided with several drivers implementing the provided coordination
API. While all drivers provides the same set of features with respect to the
API, some of them have different properties:

* `zookeeper`_ is the reference implementation and provides the most solid
  features as it's possible to build a cluster of ZooKeeper that is
  resilient towards network partitions for example.

* `memcached`_ is a basic implementation and provides less resiliency, though
  it's much simpler to setup. A lot of the features provided in tooz are
  based on timeout (heartbeats, locks, etc) so are less resilient than other
  backends.

* `zake`_ is a driver using a fake implementation of ZooKeeper and can be
  used to use Tooz in your unit tests suite for example.

.. _zookeeper: http://zookeeper.apache.org/
.. _memcached: http://memcached.org/
.. _zake: https://pypi.python.org/pypi/zake
