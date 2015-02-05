# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import six

import msgpack

from tooz import coordination


def exception_message(exc):
    """Return the string representation of exception."""
    try:
        return six.text_type(exc)
    except UnicodeError:
        return str(exc)


def to_binary(text, encoding='ascii'):
    """Return the binary representation of string (if not already binary)."""
    if not isinstance(text, six.binary_type):
        text = text.encode(encoding)
    return text


def dumps(data, excp_cls=coordination.ToozError):
    """Serializes provided data using msgpack into a byte string.

    TODO(harlowja): use oslo.serialization 'msgpackutils.py' when we can since
    that handles more native types better than the default does...
    """
    try:
        return msgpack.packb(data, use_bin_type=True)
    except (msgpack.PackException, ValueError) as e:
        raise excp_cls(exception_message(e))


def loads(blob, excp_cls=coordination.ToozError):
    """Deserializes provided data using msgpack (from a prior byte string).

    TODO(harlowja): use oslo.serialization 'msgpackutils.py' when we can since
    that handles more native types better than the default does...
    """
    try:
        return msgpack.unpackb(blob, encoding='utf-8')
    except (msgpack.UnpackException, ValueError) as e:
        raise excp_cls(exception_message(e))
