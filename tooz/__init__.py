# -*- coding: utf-8 -*-
#
#    Copyright (C) 2014 eNovance Inc. All Rights Reserved.
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


class ToozError(Exception):
    """Exception raised when an internal error occurs.

    Raised for instance in case of server internal error.

    :ivar cause: the cause of the exception being raised, when not none this
                 will itself be an exception instance, this is useful for
                 creating a chain of exceptions for versions of python where
                 this is not yet implemented/supported natively.

    """

    def __init__(self, message, cause=None):
        super(ToozError, self).__init__(message)
        self.cause = cause


class NotImplemented(NotImplementedError, ToozError):
    pass
