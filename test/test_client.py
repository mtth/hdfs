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


  """

  url = None

  @classmethod
  def setup_class(cls):
    config = Config()
    try:
      alias = config.parser.get('hdfs', 'test.alias')
    except (NoOptionError, NoSectionError):
      pass
    else:
      cls.url = config.parser.get('alias', alias)

  def setup(self):
    if not self.url:
      raise SkipTest

  def teardown(self):
    sleep(1)


class TestClient(_TestSession):

  """Test Client interactions."""

  @classmethod
  def setup_class(cls):
    super(TestClient, cls).setup_class()
    cls.client = KerberosClient(cls.url)

  def test_list_status(self):
    eq_(self.client.list_status('/').status_code, 200)
