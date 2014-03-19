#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""


from hdfs.util import Config
from hdfs.client import *
from hdfs.__main__ import load_client
from ConfigParser import NoOptionError, NoSectionError
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from time import sleep


class _TestSession(object):

  """Base class to run tests using remote HDFS.

  Currently, only running tests via Kerberos authentication is supported.

  """

  delay = 1 # delay in seconds between requests

  @classmethod
  def setup_class(cls):
    config = Config()
    try:
      alias = config.parser.get('hdfs', 'test.alias')
    except (NoOptionError, NoSectionError):
      pass
    else:
      cls.client = load_client(alias)

  def setup(self):
    if not self.client:
      raise SkipTest

  def teardown(self):
    sleep(self.delay)


class TestClient(_TestSession):

  """Test Client interactions."""

  def test_list_status_absolute_root(self):
    ok_(self.client._list_status('/'))

  def test_list_status_test_root(self):
    ok_(self.client._list_status(''))

  def test_list_status_test_root(self):
    eq_(
      self.client._list_status('').content,
      self.client._list_status(self.client.root).content,
    )
