import uuid

import six

from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

# Create a group
group = six.binary_type(six.text_type(uuid.uuid4()).encode('ascii'))
request = coordinator.create_group(group)
request.get()


def group_joined(event):
    # Event is an instance of tooz.coordination.MemberJoinedGroup
    print(event.group_id, event.member_id)


coordinator.watch_join_group(group, group_joined)
coordinator.stop()
