from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()
coordinator.stop()
