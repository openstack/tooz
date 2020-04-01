# -*- coding: utf-8 -*-
#
# Copyright 2020 Red Hat, Inc.
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

import ddt
from testtools import testcase
from unittest import mock

import tooz.coordination
import tooz.drivers.etcd3gw as etcd3gw_driver
import tooz.tests


@ddt.ddt
class TestEtcd3Gw(testcase.TestCase):
    FAKE_MEMBER_ID = tooz.tests.get_random_uuid()

    @ddt.data({'coord_url': 'etcd3+http://',
               'protocol': 'http',
               'host': etcd3gw_driver.Etcd3Driver.DEFAULT_HOST,
               'port': etcd3gw_driver.Etcd3Driver.DEFAULT_PORT,
               'ca_cert': None,
               'cert_key': None,
               'cert_cert': None,
               'timeout': etcd3gw_driver.Etcd3Driver.DEFAULT_TIMEOUT},
              {'coord_url': ('etcd3+https://my_host:666?ca_cert=/my/ca_cert&'
                             'cert_key=/my/cert_key&cert_cert=/my/cert_cert&'
                             'timeout=42'),
               'protocol': 'https',
               'host': 'my_host',
               'port': 666,
               'ca_cert': '/my/ca_cert',
               'cert_key': '/my/cert_key',
               'cert_cert': '/my/cert_cert',
               'timeout': 42})
    @ddt.unpack
    @mock.patch('etcd3gw.client')
    def test_etcd3gw_client_init(self,
                                 mock_etcd3gw_client,
                                 coord_url,
                                 protocol,
                                 host,
                                 port,
                                 ca_cert,
                                 cert_key,
                                 cert_cert,
                                 timeout):
        tooz.coordination.get_coordinator(coord_url, self.FAKE_MEMBER_ID)
        mock_etcd3gw_client.assert_called_with(host=host,
                                               port=port,
                                               protocol=protocol,
                                               ca_cert=ca_cert,
                                               cert_key=cert_key,
                                               cert_cert=cert_cert,
                                               timeout=timeout)
