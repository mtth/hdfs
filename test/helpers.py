#!/usr/bin/env python
# encoding: utf-8

"""Test helpers."""

from ConfigParser import NoOptionError, NoSectionError
from hdfs import * # import all clients
from hdfs.util import HdfsError
from nose.plugins.skip import SkipTest
from nose.tools import eq_
from time import sleep


class _TestSession(object):

  """Base class to run tests using remote HDFS.

  These tests are run only if a `test_alias` section is defined in the 
  `~/.hdfsrc` configuration file, with the root pointing to a test 
  directory.

  .. warning::

    The test directory is entirely cleaned during tests. Don't put anything
    important in it!

  Also contains a few helper functions.

  """

  delay = 1 # delay in seconds between tests

  @classmethod
  def setup_class(cls):
    try:
      client = Client.from_alias('test')
      client._delete('', recursive=True)
    except (ImportError, NoOptionError, NoSectionError, HdfsError):
      cls.client = None
    else:
      cls.client = client

  def setup(self):
    if not self.client:
      raise SkipTest
    else:
      self.client._mkdirs('')

  def teardown(self):
    if self.client:
      self.client._delete('', recursive=True)
    sleep(self.delay)

  def _check_content(self, path, content):
    eq_(self.client._open(path).content, content)

  def _file_exists(self, path):
    try:
      status = self.client.status(path)
    except HdfsError:
      # head doesn't exist
      return False
    else:
      return True
