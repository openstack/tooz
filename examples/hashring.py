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

from tooz import hashring

hashring = hashring.HashRing({'node1', 'node2', 'node3'})

# Returns set(['node2'])
nodes_for_foo = hashring[b'foo']

# Returns set(['node2', 'node3'])
nodes_for_foo_with_replicas = hashring.get_nodes(b'foo',
                                                 replicas=2)

# Returns set(['node1', 'node3'])
nodes_for_foo_with_replicas = hashring.get_nodes(b'foo',
                                                 replicas=2,
                                                 ignore_nodes={'node2'})
