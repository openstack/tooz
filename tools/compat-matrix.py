# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from tabulate import tabulate


def print_header(txt, delim="="):
    print(txt)
    print(delim * len(txt))


def print_methods(methods):
    driver_tpl = ":py:meth:`~tooz.coordination.CoordinationDriver.%s`"
    for api_name in methods:
        method_name = driver_tpl % api_name
        print("* %s" % method_name)
    if methods:
        print("")


driver_tpl = ":py:class:`~tooz.drivers.%s`"
driver_class_names = [
    "etcd.EtcdDriver",
    "file.FileDriver",
    "ipc.IPCDriver",
    "memcached.MemcachedDriver",
    "mysql.MySQLDriver",
    "pgsql.PostgresDriver",
    "redis.RedisDriver",
    "zake.ZakeDriver",
    "zookeeper.KazooDriver",
]
driver_headers = []
for n in driver_class_names:
    driver_headers.append(driver_tpl % (n))

print_header("Grouping")
print("")

print_header("APIs", delim="-")
print("")
grouping_methods = [
    'watch_join_group',
    'unwatch_join_group',
    'watch_leave_group',
    'unwatch_leave_group',
    'create_group',
    'get_groups',
    'join_group',
    'leave_group',
    'delete_group',
    'get_members',
    'get_member_capabilities',
    'update_capabilities',
]
print_methods(grouping_methods)

print_header("Driver support", delim="-")
print("")
grouping_table = [
    [
        "No",  # Etcd
        "Yes",  # File
        "No",  # IPC
        "Yes",  # Memcached
        "No",  # MySQL
        "No",  # PostgreSQL
        "Yes",  # Redis
        "Yes",  # Zake
        "Yes",  # Zookeeper
    ],
]
print(tabulate(grouping_table, driver_headers, tablefmt="rst"))
print("")

print_header("Leaders")
print("")

print_header("APIs", delim="-")
print("")
leader_methods = [
    'watch_elected_as_leader',
    'unwatch_elected_as_leader',
    'stand_down_group_leader',
    'get_leader',
]
print_methods(leader_methods)

print_header("Driver support", delim="-")
print("")
leader_table = [
    [
        "No",  # Etcd
        "No",  # File
        "No",  # IPC
        "Yes",  # Memcached
        "No",  # MySQL
        "No",  # PostgreSQL
        "Yes",  # Redis
        "Yes",  # Zake
        "Yes",  # Zookeeper
    ],
]
print(tabulate(leader_table, driver_headers, tablefmt="rst"))
print("")

print_header("Locking")
print("")

print_header("APIs", delim="-")
print("")
lock_methods = [
    'get_lock',
]
print_methods(lock_methods)

print_header("Driver support", delim="-")
print("")
lock_table = [
    [
        "Yes",  # Etcd
        "Yes",  # File
        "Yes",  # IPC
        "Yes",  # Memcached
        "Yes",  # MySQL
        "Yes",  # PostgreSQL
        "Yes",  # Redis
        "Yes",  # Zake
        "Yes",  # Zookeeper
    ],
]
print(tabulate(lock_table, driver_headers, tablefmt="rst"))
print("")
