# Copyright (C) 2020 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import uuid

from tooz import coordination

ALIVE_TIME = 1
coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

# Create a group
group = bytes(str(uuid.uuid4()).encode('ascii'))
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
