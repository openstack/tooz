# -*- coding: utf-8 -*-
#
# Copyright © 2014 eNovance
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
import retrying


class Retry(Exception):
    """Exception raised if we need to retry."""


def retry_if_retry_raised(exception):
    return isinstance(exception, Retry)


RETRYING_KWARGS = dict(
    retry_on_exception=retry_if_retry_raised,
    wait='exponential_sleep',
    wait_exponential_max=1,
)


def retry(**kwargs):
    delay = kwargs.get('stop_max_delay', None)
    kwargs['stop_max_delay'] = delay if delay not in (True, False) else None
    k = RETRYING_KWARGS.copy()
    k.update(kwargs)
    return retrying.retry(**k)


Retrying = retrying.Retrying
