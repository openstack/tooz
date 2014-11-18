import time

from tooz import coordination

ALIVE_TIME = 5

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

start = time.time()
while time.time() - start < ALIVE_TIME:
    coordinator.heartbeat()
    time.sleep(0.1)

coordinator.stop()
