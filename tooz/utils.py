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

import base64
import datetime
import operator
import os

import futurist
import msgpack
from oslo_serialization import msgpackutils
from oslo_utils import encodeutils
from oslo_utils import excutils
import six

import tooz


class Base64LockEncoder(object):
    def __init__(self, keyspace_url, prefix=''):
        self.keyspace_url = keyspace_url
        if prefix:
            self.keyspace_url += prefix

    def check_and_encode(self, name):
        if not isinstance(name, (six.text_type, six.binary_type)):
            raise TypeError("Provided lock name is expected to be a string"
                            " or binary type and not %s" % type(name))
        try:
            return self.encode(name)
        except (UnicodeDecodeError, UnicodeEncodeError) as e:
            raise ValueError("Invalid lock name due to encoding/decoding "
                             " issue: %s"
                             % encodeutils.exception_to_unicode(e))

    def encode(self, name):
        if isinstance(name, six.text_type):
            name = name.encode("ascii")
        enc_name = base64.urlsafe_b64encode(name)
        return self.keyspace_url + "/" + enc_name.decode("ascii")


class ProxyExecutor(object):
    KIND_TO_FACTORY = {
        'threaded': (lambda:
                     futurist.ThreadPoolExecutor(max_workers=1)),
        'synchronous': lambda: futurist.SynchronousExecutor(),
    }

    # Provide a few common aliases...
    KIND_TO_FACTORY['thread'] = KIND_TO_FACTORY['threaded']
    KIND_TO_FACTORY['threading'] = KIND_TO_FACTORY['threaded']
    KIND_TO_FACTORY['sync'] = KIND_TO_FACTORY['synchronous']

    DEFAULT_KIND = 'threaded'

    def __init__(self, driver_name, default_executor_factory):
        self.default_executor_factory = default_executor_factory
        self.driver_name = driver_name
        self.started = False
        self.executor = None
        self.internally_owned = True

    @classmethod
    def build(cls, driver_name, options):
        default_executor_fact = cls.KIND_TO_FACTORY[cls.DEFAULT_KIND]
        if 'executor' in options:
            executor_kind = options['executor']
            try:
                default_executor_fact = cls.KIND_TO_FACTORY[executor_kind]
            except KeyError:
                executors_known = sorted(list(cls.KIND_TO_FACTORY))
                raise tooz.ToozError("Unknown executor"
                                     " '%s' provided, accepted values"
                                     " are %s" % (executor_kind,
                                                  executors_known))
        return cls(driver_name, default_executor_fact)

    def start(self):
        if self.started:
            return
        self.executor = self.default_executor_factory()
        self.started = True

    def stop(self):
        executor = self.executor
        self.executor = None
        if executor is not None:
            executor.shutdown()
        self.started = False

    def submit(self, cb, *args, **kwargs):
        if not self.started:
            raise tooz.ToozError("%s driver asynchronous executor"
                                 " has not been started"
                                 % self.driver_name)
        try:
            return self.executor.submit(cb, *args, **kwargs)
        except RuntimeError:
            raise tooz.ToozError("%s driver asynchronous executor has"
                                 " been shutdown" % self.driver_name)


def safe_abs_path(rooted_at, *pieces):
    # Avoids the following junk...
    #
    # >>> import os
    # >>> os.path.join("/b", "..")
    # '/b/..'
    # >>> os.path.abspath(os.path.join("/b", ".."))
    # '/'
    path = os.path.abspath(os.path.join(rooted_at, *pieces))
    if not path.startswith(rooted_at):
        raise ValueError("Unable to create path that is outside of"
                         " parent directory '%s' using segments %s"
                         % (rooted_at, list(pieces)))
    return path


def convert_blocking(blocking):
    """Converts a multi-type blocking variable into its derivatives."""
    timeout = None
    if not isinstance(blocking, bool):
        timeout = float(blocking)
        blocking = True
    return blocking, timeout


def collapse(config, exclude=None, item_selector=operator.itemgetter(-1)):
    """Collapses config with keys and **list/tuple** values.

    NOTE(harlowja): The last item/index from the list/tuple value is selected
    be default as the new value (values that are not lists/tuples are left
    alone). If the list/tuple value is empty (zero length), then no value
    is set.
    """
    if not isinstance(config, dict):
        raise TypeError("Unexpected config type, dict expected")
    if not config:
        return {}
    if exclude is None:
        exclude = set()
    collapsed = {}
    for (k, v) in six.iteritems(config):
        if isinstance(v, (tuple, list)):
            if k in exclude:
                collapsed[k] = v
            else:
                if len(v):
                    collapsed[k] = item_selector(v)
        else:
            collapsed[k] = v
    return collapsed


def to_binary(text, encoding='ascii'):
    """Return the binary representation of string (if not already binary)."""
    if not isinstance(text, six.binary_type):
        text = text.encode(encoding)
    return text


class SerializationError(tooz.ToozError):
    "Exception raised when serialization or deserialization breaks."


def dumps(data, excp_cls=SerializationError):
    """Serializes provided data using msgpack into a byte string."""
    try:
        return msgpackutils.dumps(data)
    except (msgpack.PackException, ValueError) as e:
        raise_with_cause(excp_cls,
                         encodeutils.exception_to_unicode(e),
                         cause=e)


def loads(blob, excp_cls=SerializationError):
    """Deserializes provided data using msgpack (from a prior byte string)."""
    try:
        return msgpackutils.loads(blob)
    except (msgpack.UnpackException, ValueError) as e:
        raise_with_cause(excp_cls,
                         encodeutils.exception_to_unicode(e),
                         cause=e)


def millis_to_datetime(milliseconds):
    """Converts number of milliseconds (from epoch) into a datetime object."""
    return datetime.datetime.fromtimestamp(float(milliseconds) / 1000)


def raise_with_cause(exc_cls, message, *args, **kwargs):
    """Helper to raise + chain exceptions (when able) and associate a *cause*.

    **For internal usage only.**

    NOTE(harlowja): Since in py3.x exceptions can be chained (due to
    :pep:`3134`) we should try to raise the desired exception with the given
    *cause*.

    :param exc_cls: the :py:class:`~tooz.ToozError` class to raise.
    :param message: the text/str message that will be passed to
                    the exceptions constructor as its first positional
                    argument.
    :param args: any additional positional arguments to pass to the
                 exceptions constructor.
    :param kwargs: any additional keyword arguments to pass to the
                   exceptions constructor.
    """
    if not issubclass(exc_cls, tooz.ToozError):
        raise ValueError("Subclass of tooz error is required")
    excutils.raise_with_cause(exc_cls, message, *args, **kwargs)
