from tooz import coordination

coordinator = coordination.get_coordinator('zookeeper://localhost', b'host-1')
coordinator.start()

# Create a group
request = coordinator.create_group(b"my group")
request.get()

# Join a group
request = coordinator.join_group(b"my group")
request.get()

coordinator.stop()
