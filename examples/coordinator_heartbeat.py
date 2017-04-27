from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start(start_heart=True)
coordinator.stop()
