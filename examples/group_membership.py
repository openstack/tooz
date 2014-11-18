import uuid

import six

from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

# Create a group
group = six.binary_type(six.text_type(uuid.uuid4()).encode('ascii'))
request = coordinator.create_group(group)
request.get()

# Join a group
request = coordinator.join_group(group)
request.get()

coordinator.stop()
