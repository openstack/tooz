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

import uuid

from tooz import coordination

coordinator = coordination.get_coordinator('zake://', b'host-1')
coordinator.start()

# Create a group
group = bytes(str(uuid.uuid4()).encode('ascii'))
request = coordinator.create_group(group)
request.get()


def group_joined(event):
    # Event is an instance of tooz.coordination.MemberJoinedGroup
    print(event.group_id, event.member_id)


coordinator.watch_join_group(group, group_joined)
coordinator.stop()
