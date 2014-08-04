from tooz import coordination

coordinator = coordination.get_coordinator('zookeeper://localhost', b'host-1')
coordinator.start()

# Create a group
request = coordinator.create_group(b"my group")
request.get()


def group_joined(event):
    # Event is an instance of tooz.coordination.MemberJoinedGroup
    print(event.group_id, event.member_id)


coordinator.watch_join_group(b"my group", group_joined)

coordinator.stop()
