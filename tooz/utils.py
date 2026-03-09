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

from __future__ import annotations

import base64
from collections.abc import Callable, Collection, Sequence
from concurrent import futures
import datetime
import operator
import os
from typing import TYPE_CHECKING, Any, NoReturn, ParamSpec, TypeVar

import futurist
import msgpack
from oslo_serialization import msgpackutils
from oslo_utils import excutils

import tooz

if TYPE_CHECKING:
    from typing_extensions import Self


P = ParamSpec('P')
R = TypeVar('R')


class Base64LockEncoder:
    def __init__(self, keyspace_url: str, prefix: str = '') -> None:
        self.keyspace_url = keyspace_url
        if prefix:
            self.keyspace_url += prefix

    def check_and_encode(self, name: str | bytes) -> str:
        if not isinstance(name, (str, bytes)):
            raise TypeError(
                "Provided lock name is expected to be a string"
                f" or binary type and not {type(name)}"
            )
        try:
            return self.encode(name)
        except (UnicodeDecodeError, UnicodeEncodeError) as e:
            raise ValueError(
                f"Invalid lock name due to encoding/decoding issue: {e}"
            )

    def encode(self, name: str | bytes) -> str:
        if isinstance(name, str):
            name = name.encode("ascii")
        enc_name = base64.urlsafe_b64encode(name)
        return self.keyspace_url + "/" + enc_name.decode("ascii")


class ProxyExecutor:
    KIND_TO_FACTORY = {
        'threaded': (lambda: futurist.ThreadPoolExecutor(max_workers=1)),
        'synchronous': lambda: futurist.SynchronousExecutor(),
    }

    # Provide a few common aliases...
    KIND_TO_FACTORY['thread'] = KIND_TO_FACTORY['threaded']
    KIND_TO_FACTORY['threading'] = KIND_TO_FACTORY['threaded']
    KIND_TO_FACTORY['sync'] = KIND_TO_FACTORY['synchronous']

    DEFAULT_KIND = 'threaded'

    def __init__(
        self,
        driver_name: str,
        default_executor_factory: Callable[[], futures.Executor],
    ) -> None:
        self.default_executor_factory = default_executor_factory
        self.driver_name = driver_name
        self.started = False
        self.executor: futures.Executor | None = None
        self.internally_owned = True

    @classmethod
    def build(cls, driver_name: str, options: dict[str, Any]) -> Self:
        default_executor_fact = cls.KIND_TO_FACTORY[cls.DEFAULT_KIND]
        if 'executor' in options:
            executor_kind = options['executor']
            try:
                default_executor_fact = cls.KIND_TO_FACTORY[executor_kind]
            except KeyError:
                executors_known = sorted(list(cls.KIND_TO_FACTORY))
                raise tooz.ToozError(
                    f"Unknown executor '{executor_kind}' provided, "
                    f"accepted values are {executors_known}"
                )
        return cls(driver_name, default_executor_fact)

    def start(self) -> None:
        if self.started:
            return
        self.executor = self.default_executor_factory()
        self.started = True

    def stop(self) -> None:
        executor = self.executor
        self.executor = None
        if executor is not None:
            executor.shutdown()
        self.started = False

    def submit(
        self, cb: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> futures.Future[R]:
        if not self.started:
            raise tooz.ToozError(
                f"{self.driver_name} driver asynchronous executor "
                f"has not been started"
            )

        assert self.executor is not None  # narrow type

        try:
            return self.executor.submit(cb, *args, **kwargs)
        except RuntimeError:
            raise tooz.ToozError(
                f"{self.driver_name} driver asynchronous executor has"
                f"been shutdown"
            )


def safe_abs_path(rooted_at: str, *pieces: str) -> str:
    # Avoids the following junk...
    #
    # >>> import os
    # >>> os.path.join("/b", "..")
    # '/b/..'
    # >>> os.path.abspath(os.path.join("/b", ".."))
    # '/'
    path = os.path.abspath(os.path.join(rooted_at, *pieces))
    if not path.startswith(rooted_at):
        raise ValueError(
            "Unable to create path that is outside of"
            f" parent directory '{rooted_at}' using segments {list(pieces)}"
        )
    return path


# TODO(stephenfin): Tighten the types here and start logging a deprecation
# warning for non-boolean blocking values.
def convert_blocking(
    blocking: bool | float | str, timeout: float | None = None
) -> tuple[bool, float | None]:
    """Converts a multi-type blocking variable into its derivatives."""
    if isinstance(blocking, bool):
        return blocking, timeout

    timeout = float(blocking)
    blocking = True
    return blocking, timeout


def collapse(
    config: dict[str, Any],
    exclude: Collection[str] | None = None,
    item_selector: Callable[[Sequence[Any]], Any] = operator.itemgetter(-1),
) -> dict[str, Any]:
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
        exclude = frozenset()
    collapsed = {}
    for k, v in config.items():
        if isinstance(v, (tuple, list)):
            if k in exclude:
                collapsed[k] = v
            else:
                if len(v):
                    collapsed[k] = item_selector(v)
        else:
            collapsed[k] = v
    return collapsed


def to_binary(text: str | bytes, encoding: str = 'ascii') -> bytes:
    """Return the binary representation of string (if not already binary)."""
    if not isinstance(text, bytes):
        text = text.encode(encoding)
    return text


class SerializationError(tooz.ToozError):
    "Exception raised when serialization or deserialization breaks."


def dumps(
    data: Any, excp_cls: type[tooz.ToozError] = SerializationError
) -> bytes:
    """Serializes provided data using msgpack into a byte string."""
    try:
        return msgpackutils.dumps(data)
    except (msgpack.PackException, ValueError) as e:
        raise_with_cause(excp_cls, str(e), cause=e)


def loads(
    blob: bytes, excp_cls: type[tooz.ToozError] = SerializationError
) -> Any:
    """Deserializes provided data using msgpack (from a prior byte string)."""
    try:
        return msgpackutils.loads(blob)
    except (msgpack.UnpackException, ValueError) as e:
        raise_with_cause(excp_cls, str(e), cause=e)


def millis_to_datetime(milliseconds: str | float) -> datetime.datetime:
    """Converts number of milliseconds (from epoch) into a datetime object."""
    return datetime.datetime.fromtimestamp(float(milliseconds) / 1000)


# https://review.opendev.org/c/openstack/oslo.utils/+/979690
def raise_with_cause(  # type: ignore[misc]
    exc_cls: type[tooz.ToozError],
    message: str,
    *args: Any,
    **kwargs: Any,
) -> NoReturn:
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
