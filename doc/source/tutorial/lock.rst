======
 Lock
======

Tooz provides distributed locks. A lock is identified by a name, and a lock can
only be acquired by one coordinator at a time.

.. literalinclude:: ../../../examples/lock.py
   :language: python

The method :meth:`tooz.coordination.CoordinationDriver.get_lock` allows
to create a lock identified by a name. Once you retrieve this lock, you can
use it as a context manager or use the :meth:`tooz.locking.Lock.acquire` and
:meth:`tooz.locking.Lock.release` methods to acquire and release the lock.
