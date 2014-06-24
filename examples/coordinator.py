from tooz import coordination

coordinator = coordination.get_coordinator('zookeeper', b'host-1')
coordinator.start()
coordinator.stop()
