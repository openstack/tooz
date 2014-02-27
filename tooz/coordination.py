# -*- coding: utf-8 -*-
#
#    Copyright (C) 2013 eNovance Inc. All Rights Reserved.
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

import abc
import six

from stevedore import driver

TOOZ_BACKENDS_NAMESPACE = "tooz.backends"


@six.add_metaclass(abc.ABCMeta)
class CoordinationDriver(object):

    @abc.abstractmethod
    def start(self, timeout):
        """Start the service engine.

        If needed, the establishment of a connection to the servers
        is initiated.

        :param timeout: Time in seconds to wait for connection to succeed.
        :type timeout: int
        """

    @abc.abstractmethod
    def stop(self):
        """Stop the service engine.

        If needed, the connection to servers is closed and the client will
        disappear from all joined groups.
        """

    @abc.abstractmethod
    def create_group(self, group_id):
        """Request the creation of a group asynchronously.

        :param group_id: the id of the group to create
        :type group_id: str
        :returns: None
        :rtype: CoordAsyncResult
        """

    @abc.abstractmethod
    def get_groups(self):
        """Return the list composed by all groups ids asynchronously.

        :returns: the list of all created group ids
        :rtype: CoordAsyncResult
        """

    @abc.abstractmethod
    def join_group(self, group_id, capabilities=b""):
        """Join a group and establish group membership asynchronously.

        :param group_id: the id of the group to join
        :type group_id: str
        :param capabilities: the capabilities of the joined member
        :type capabilities: str
        :returns: None
        :rtype: CoordAsyncResult
        """

    @abc.abstractmethod
    def leave_group(self, group_id):
        """Leave a group asynchronously.

        :param group_id: the id of the group to leave
        :type group_id: str
        :returns: None
        :rtype: CoordAsyncResult
        """

    @abc.abstractmethod
    def get_members(self, group_id):
        """Return the list of all members ids of the specified group
        asynchronously.

        :returns: list of all created group ids
        :rtype: CoordAsyncResult
        """

    @abc.abstractmethod
    def get_member_capabilities(self, group_id, member_id):
        """Return the capabilities of a member asynchronously.

        :param group_id: the id of the group of the member
        :type group_id: str
        :param member_id: the id of the member
        :type member_id: str
        :returns: capabilities of a member
        :rtype: CoordAsyncResult
        """

    @abc.abstractmethod
    def update_capabilities(self, group_id, capabilities):
        """Update capabilities of the caller in the specified group
        asynchronously.

        :param group_id: the id of the group of the current member
        :type group_id: str
        :param capabilities: the capabilities of the updated member
        :type capabilities: str
        :returns: None
        :rtype: CoordAsyncResult
        """


@six.add_metaclass(abc.ABCMeta)
class CoordAsyncResult(object):
    """Representation of an asynchronous task, every call API
    returns an CoordAsyncResult object on which the result or
    the status of the task can be requested.
    """

    @abc.abstractmethod
    def get(self, timeout=10):
        """Retrieve the result of the corresponding asynchronous call.
        :param timeout: block until the timeout expire.
        :type timeout: float
        """

    @abc.abstractmethod
    def done(self):
        """Returns True if the task is done, False otherwise."""


    #TODO(yassine)
    #Replace kwargs by something more simple.
def get_coordinator(backend, member_id, **kwargs):
    """Initialize and load the backend.

    :param backend: the current tooz provided backends are 'zookeeper'
    :type backend: str
    :param member_id: the id of the member
    :type member_id: str
    :param kwargs: additional backend specific options
    :type kwargs: dict
    """
    return driver.DriverManager(namespace=TOOZ_BACKENDS_NAMESPACE,
                                name=backend,
                                invoke_on_load=True,
                                invoke_args=(member_id,),
                                invoke_kwds=kwargs).driver


class ToozError(Exception):
    """Exception raised when an internal error occurs, for instance in
    case of server internal error.
    """


class ToozConnectionError(ToozError):
    """Exception raised when the client cannot manage to connect to the
    server.
    """


class GroupNotCreated(ToozError):
    """Exception raised when the caller request a group which does
    not exist.
    """


class GroupAlreadyExist(ToozError):
    """Exception raised when the caller try to create a group which already
    exist.
    """
    def __init__(self, group_id):
        self.group_id = group_id
        super(GroupAlreadyExist, self).__init__(
            "Group %s already exists" % group_id)


class MemberAlreadyExist(ToozError):
    """Exception raised when the caller try to join a group but a member
    with the same identifier belongs to that group.
    """


class MemberNotJoined(ToozError):
    """Exception raised when the caller try to access a member which does not
    belongs to the specified group.
    """
