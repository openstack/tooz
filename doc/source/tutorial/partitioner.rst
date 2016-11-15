=============
 Partitioner
=============

Tooz provides a partitioner object based on its consistent hash ring
implementation. It can be used to map Python objects to one or several nodes.
The partitioner object automatically keeps track of nodes joining and leaving
the group, so the rebalancing is managed.

.. literalinclude:: ../../../examples/partitioner.py
   :language: python
