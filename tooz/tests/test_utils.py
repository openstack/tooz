# -*- coding: utf-8 -*-

# Copyright (c) 2015 OpenStack Foundation
# All Rights Reserved.
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

import os
import tempfile

import futurist
import six
from testtools import testcase

import tooz
from tooz import utils


class TestProxyExecutor(testcase.TestCase):
    def test_fetch_check_executor(self):
        try_options = [
            ({'executor': 'sync'}, futurist.SynchronousExecutor),
            ({'executor': 'thread'}, futurist.ThreadPoolExecutor),
        ]
        for options, expected_cls in try_options:
            executor = utils.ProxyExecutor.build("test", options)
            self.assertTrue(executor.internally_owned)

            executor.start()
            self.assertTrue(executor.started)
            self.assertIsInstance(executor.executor, expected_cls)

            executor.stop()
            self.assertFalse(executor.started)

    def test_fetch_default_executor(self):
        executor = utils.ProxyExecutor.build("test", {})
        executor.start()
        try:
            self.assertIsInstance(executor.executor,
                                  futurist.ThreadPoolExecutor)
        finally:
            executor.stop()

    def test_fetch_unknown_executor(self):
        options = {'executor': 'huh'}
        self.assertRaises(tooz.ToozError,
                          utils.ProxyExecutor.build, 'test',
                          options)

    def test_no_submit_stopped(self):
        executor = utils.ProxyExecutor.build("test", {})
        self.assertRaises(tooz.ToozError,
                          executor.submit, lambda: None)


class TestUtilsSafePath(testcase.TestCase):
    base = tempfile.gettempdir()

    def test_join(self):
        self.assertEqual(os.path.join(self.base, 'b'),
                         utils.safe_abs_path(self.base, "b"))
        self.assertEqual(os.path.join(self.base, 'b', 'c'),
                         utils.safe_abs_path(self.base, "b", 'c'))
        self.assertEqual(self.base,
                         utils.safe_abs_path(self.base, "b", 'c', '../..'))

    def test_unsafe_join(self):
        self.assertRaises(ValueError, utils.safe_abs_path,
                          self.base, "../b")
        self.assertRaises(ValueError, utils.safe_abs_path,
                          self.base, "b", 'c', '../../../')


class TestUtilsCollapse(testcase.TestCase):

    def test_bad_type(self):
        self.assertRaises(TypeError, utils.collapse, "")
        self.assertRaises(TypeError, utils.collapse, [])
        self.assertRaises(TypeError, utils.collapse, 2)

    def test_collapse_simple(self):
        ex = {
            'a': [1],
            'b': 2,
            'c': (1, 2, 3),
        }
        c_ex = utils.collapse(ex)
        self.assertEqual({'a': 1, 'c': 3, 'b': 2}, c_ex)

    def test_collapse_exclusions(self):
        ex = {
            'a': [1],
            'b': 2,
            'c': (1, 2, 3),
        }
        c_ex = utils.collapse(ex, exclude=['a'])
        self.assertEqual({'a': [1], 'c': 3, 'b': 2}, c_ex)

    def test_no_collapse(self):
        ex = {
            'a': [1],
            'b': [2],
            'c': (1, 2, 3),
        }
        c_ex = utils.collapse(ex, exclude=set(six.iterkeys(ex)))
        self.assertEqual(ex, c_ex)

    def test_custom_selector(self):
        ex = {
            'a': [1, 2, 3],
        }
        c_ex = utils.collapse(ex,
                              item_selector=lambda items: items[0])
        self.assertEqual({'a': 1}, c_ex)

    def test_empty_lists(self):
        ex = {
            'a': [],
            'b': (),
            'c': [1],
        }
        c_ex = utils.collapse(ex)
        self.assertNotIn('b', c_ex)
        self.assertNotIn('a', c_ex)
        self.assertIn('c', c_ex)
