from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

# Create a lock
lock = coordinator.get_lock("foobar")
with lock:
    print("Do something that is distributed")

coordinator.stop()
