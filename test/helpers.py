#!/usr/bin/env python
# encoding: utf-8

"""Test helpers."""

from six.moves.configparser import NoOptionError, NoSectionError
from hdfs import * # import all clients
from hdfs.util import HdfsError
from nose.plugins.skip import SkipTest
from nose.tools import eq_
from time import sleep
import os


class _TestSession(object):

  """Base class to run tests using remote HDFS.

  These tests are run only if a `HDFSCLI_TEST_ALIAS` or `HDFSCLI_TEST_URL`
  environment variable is defined (the former taking precedence). For safety,
  a suffix is appended to any defined root.

  .. warning::

    The new root directory used is entirely cleaned during tests!

  Also contains a few helper functions.

  """

  delay = 0.5 # delay in seconds between tests
  root_suffix = '/.hdfscli/' # also used as default root if none specified

  @classmethod
  def setup_class(cls):
    alias = os.getenv('HDFSCLI_TEST_ALIAS')
    url = os.getenv('HDFSCLI_TEST_URL')
    if alias:
      cls.client = Client.from_alias(alias)
      cls.client.root = (cls.client.root or '').rstrip('/') + cls.root_suffix
    elif url:
      cls.client = InsecureClient(url, root=cls.root_suffix)
    else:
      cls.client = None

  @classmethod
  def teardown_class(cls):
    if cls.client:
      cls.client._delete('', recursive=True)

  def setup(self):
    if not self.client:
      raise SkipTest
    else:
      self.client._delete('', recursive=True)
      sleep(self.delay)

  # Helpers.

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
