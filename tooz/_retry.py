#
# Copyright © 2016 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from collections.abc import Callable
import datetime
from typing import Any, TypeVar

import tenacity
from tenacity import stop
from tenacity import wait


WrappedFn = TypeVar('WrappedFn', bound=Callable[..., Any])

_default_wait = wait.wait_exponential(max=1)


# TODO(stephenfin): Remove kwargs
def retry(
    stop_max_delay: int | float | bool | None = None, **kwargs: Any
) -> Callable[[WrappedFn], WrappedFn]:
    if isinstance(stop_max_delay, (int, float, datetime.timedelta)):
        return tenacity.retry(
            wait=_default_wait,
            retry=tenacity.retry_never,
            stop=stop.stop_after_delay(stop_max_delay),
        )

    return tenacity.retry(
        wait=_default_wait,
        retry=tenacity.retry_never,
    )


TryAgain = tenacity.TryAgain
