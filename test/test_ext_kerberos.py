#!/usr/bin/env python
# encoding: utf-8

"""Test Kerberos extension."""

from mock import Mock
from nose.tools import eq_, nottest, ok_, raises
from threading import Lock, Thread
from time import sleep, time
import sys

try:
  import requests_kerberos
except ImportError:
  sys.modules['requests_kerberos'] = Mock()

from hdfs.ext.kerberos import KerberosClient


class TestKerberosClient(object):

  def test_concurrency(self):
    lock = Lock()
    calls = set()
    items = []

    def add_item(method, url, **kwargs):
      with lock:
        ok_(not items)
        items.append(kwargs['thread'])
      sleep(0.25)
      with lock:
        thread = items.pop()
        eq_(thread, kwargs['thread'])
        calls.add(thread)
      return Mock()

    session = Mock()
    session.request = add_item
    client = KerberosClient('http://nn', max_concurrency=1, session=session)
    args = ('POST', 'http://foo')
    t1 = Thread(
      target=client._request,
      args=args,
      kwargs={'thread': 1, 'auth': True}
    )
    t1.start()
    t2 = Thread(
      target=client._request,
      args=args,
      kwargs={'thread': 2, 'auth': True}
    )
    t2.start()
    t1.join()
    t2.join()
    eq_(calls, set([1, 2]))

  def test_concurrency_no_auth(self):
    lock = Lock()
    calls = set()
    items = []

    def add_item(method, url, **kwargs):
      thread = kwargs['thread']
      with lock:
        if items:
          other = items.pop()
          ok_(other != thread)
        else:
          items.append(thread)
      sleep(0.25)
      with lock:
        ok_(not items)
        calls.add(thread)
      return Mock()

    session = Mock()
    session.request = add_item
    client = KerberosClient('http://nn', max_concurrency=1, session=session)
    args = ('POST', 'http://foo')
    t1 = Thread(
      target=client._request,
      args=args,
      kwargs={'thread': 1, 'auth': True}
    )
    t1.start()
    t2 = Thread(target=client._request, args=args, kwargs={'thread': 2})
    t2.start()
    t1.join()
    t2.join()
    eq_(calls, set([1, 2]))
