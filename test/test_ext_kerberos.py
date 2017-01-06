#!/usr/bin/env python
# encoding: utf-8

"""Test Kerberos extension."""

from nose.tools import eq_, nottest, ok_, raises
from threading import Lock, Thread
from time import sleep, time
import sys


class MockHTTPKerberosAuth(object):

  def __init__(self, **kwargs):
    self._lock = Lock()
    self._calls = set()
    self._items = []

  def __call__(self, n):
    with self._lock:
      ok_(not self._items)
      self._items.append(n)
    sleep(0.25)
    with self._lock:
      thread = self._items.pop()
      eq_(thread, n)
      self._calls.add(thread)


class MockModule(object):
  def __init__(self):
    self.HTTPKerberosAuth = MockHTTPKerberosAuth


sys.modules['requests_kerberos'] = MockModule()

from hdfs.ext.kerberos import _HdfsHTTPKerberosAuth


class TestKerberosClient(object):

  def test_max_concurrency(self):
    auth = _HdfsHTTPKerberosAuth(1, mutual_auth='OPTIONAL')
    t1 = Thread(target=auth.__call__, args=(1, ))
    t1.start()
    t2 = Thread(target=auth.__call__, args=(2, ))
    t2.start()
    t1.join()
    t2.join()
    eq_(auth._calls, set([1, 2]))
