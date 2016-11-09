===========
 Hash ring
===========

Tooz provides a consistent hash ring implementation. It can be used to map
objects (represented via binary keys) to one or several nodes. When the node
list changes, the rebalancing of objects across the ring is kept minimal.

.. literalinclude:: ../../../examples/hashring.py
   :language: python
