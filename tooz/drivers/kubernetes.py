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

from kubernetes.client import exceptions as k8s_exc
import sherlock

import tooz
from tooz import coordination
from tooz import locking
from tooz import utils


class KubernetesLock(locking.Lock):
    def __init__(self, name, namespace, lock):
        super().__init__(name)
        self._name = name
        self._namespace = namespace
        self._lock = lock
        self._client = lock.client

    def is_still_owner(self):
        if not self._lock.locked():
            return False
        try:
            holder = self._client.read_namespaced_lease(
                self._name, self._namespace
            ).spec.holder_identity
            if holder == self._lock._owner:
                return True
        except k8s_exc.ApiException as e:
            if "Reason: Not Found" not in str(e):
                utils.raise_with_cause(
                    tooz.ToozError,
                    f"operation error: {str(e)}",
                    cause=e)
        return False

    def acquire(self, blocking=True, shared=False, expire=None):
        if shared:
            raise tooz.NotImplemented
        blocking, timeout = utils.convert_blocking(blocking)
        sherlock.configure(
            expire=expire,
            timeout=int(timeout) if timeout else timeout
        )
        return self._lock.acquire(blocking=blocking)

    def release(self):
        if self._lock.locked():
            try:
                self._lock.release()
            except sherlock.lock.LockException as le:
                msg = "Lock was not set by this process."
                if msg in str(le):
                    return True
                else:
                    raise
            return True
        else:
            return False

    @property
    def acquired(self):
        return (self._lock.locked() and self.is_still_owner())


class SherlockDriver(coordination.CoordinationDriverCachedRunWatchers):
    """This driver uses the `sherlock`_ client against `kubernetes`_ servers.

    The Kubernetes coordinator url should look like::

      kubernetes://[[?OPTION1=VALUE1[&OPTION2=VALUE2[&...]]]

    Currently the following options will be proxied to the contained client:

    ================  ===============================  ====================
    Name              Source                           Default
    ================  ===============================  ====================
    namespace         'namespace' options key          openstack
    ================  ===============================  ====================

    .. _kubernetes: https://kubernetes.io/
    .. _sherlock: https://sher-lock.readthedocs.io/en/latest/
    """
    #: Default namespace when none is provided.
    K8S_NAMESPACE = "openstack"

    CHARACTERISTICS = (
        coordination.Characteristics.NON_TIMEOUT_BASED,
        coordination.Characteristics.DISTRIBUTED_ACROSS_THREADS,
        coordination.Characteristics.DISTRIBUTED_ACROSS_PROCESSES,
        coordination.Characteristics.DISTRIBUTED_ACROSS_HOSTS,
    )
    """
    Tuple of :py:class:`~tooz.coordination.Characteristics` introspectable
    enum member(s) that can be used to interogate how this driver works.
    """

    def __init__(self, member_id, parsed_url, options):
        super().__init__(member_id, parsed_url, options)
        options = utils.collapse(options)
        self._namespace = options.get('namespace', self.K8S_NAMESPACE)

    def get_lock(self, name):
        lock = sherlock.KubernetesLock(
            lock_name=name, k8s_namespace=self._namespace)
        return KubernetesLock(name=name, namespace=self._namespace, lock=lock)
