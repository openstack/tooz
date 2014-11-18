import time
import uuid

import six

from tooz import coordination

ALIVE_TIME = 1
coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

# Create a group
group = six.binary_type(six.text_type(uuid.uuid4()).encode('ascii'))
request = coordinator.create_group(group)
request.get()

# Join a group
request = coordinator.join_group(group)
request.get()


def when_i_am_elected_leader(event):
    # event is a LeaderElected event
    print(event.group_id, event.member_id)


# Propose to be a leader for the group
coordinator.watch_elected_as_leader(group, when_i_am_elected_leader)

start = time.time()
while time.time() - start < ALIVE_TIME:
    coordinator.heartbeat()
    coordinator.run_watchers()
    time.sleep(0.1)

coordinator.stop()
