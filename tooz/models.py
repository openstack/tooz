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


class Member(object):
    """The member of a group.

    A member is composed of a member identifier, a group identifier and
    capabilities. The capabilities correspond to a bytes string defined by
    the user of the API, for instance it could be resulting from JSON
    serialization, Google Protocol Buffers, MsgPack and so on.
    """

    def __init__(self, group_id, member_id, capabilities):
        self.group_id = group_id
        self.member_id = member_id
        self.capabilities = capabilities
