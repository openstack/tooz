from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()
partitioner = coordinator.join_partitioned_group("group1")

# Returns {'host-1'}
member = partitioner.members_for_object(object())

coordinator.leave_partitioned_group(partitioner)
coordinator.stop()
