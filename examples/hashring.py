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
