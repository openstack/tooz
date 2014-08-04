import time

from tooz import coordination

coordinator = coordination.get_coordinator('memcached://localhost', b'host-1')
coordinator.start()

while True:
    coordinator.heartbeat()
    time.sleep(0.1)

coordinator.stop()
