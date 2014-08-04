from tooz import coordination

coordinator = coordination.get_coordinator('zookeeper://localhost', b'host-1')
coordinator.start()

# Create a group
request = coordinator.create_group(b"my group")
request.get()

# Join a group
request = coordinator.join_group(b"my group")
request.get()


def when_i_am_elected_leader(event):
    # event is a LeaderElected event
    print(event.group_id, event.member_id)


# Propose to be a leader for the group
coordinator.watch_elected_as_leader(b"my_group",
                                    when_i_am_elected_leader)

while True:
    coordinator.heartbeat()
    coordinator.run_watchers()

coordinator.stop()
