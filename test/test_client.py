#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""


from hdfs.util import Config
from hdfs.client import *
from ConfigParser import NoOptionError, NoSectionError
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from time import sleep


class _TestSession(object):

  """Base class to run tests using remote HDFS.

  Currently, only running tests via Kerberos authentication is supported.

  """

  url = None

  @classmethod
  def setup_class(cls):
    config = Config()
    try:
      alias = config.parser.get('hdfs', 'test.alias')
      cls.root = config.parser.get('hdfs', 'test.root')
    except (NoOptionError, NoSectionError):
      pass
    else:
      cls.url = config.parser.get('alias', alias)

  def setup(self):
    if not self.url:
      raise SkipTest

  def teardown(self):
    sleep(1)


class TestRawClient(_TestSession):

  """Test Client interactions."""

  @classmethod
  def setup_class(cls):
    super(TestRawClient, cls).setup_class()
    self.client = KerberosClient(url=cls.url, root=cls.root)

  def test_list_status_absolute_root(self):
    ok_(self.client.list_status('/'))

  def test_list_status_test_root(self):
    ok_(self.client.list_status(''))

  def test_list_status_test_root(self):
    eq_(self.client.list_status(''), self.client.list_status(self.root))

  def test_mkdirs(self):
    file_status = self.client.list_status('')['FileStatuses']['FileStatus']
    n_files = len(file_status)
    self.client.mkdirs('test_mkdirs')
    eq_
