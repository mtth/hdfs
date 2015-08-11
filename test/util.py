#!/usr/bin/env python
# encoding: utf-8

"""Test helpers."""

from hdfs import InsecureClient
from hdfs.config import Config
from hdfs.util import HdfsError
from nose.plugins.skip import SkipTest
from nose.tools import eq_
from six.moves.configparser import NoOptionError, NoSectionError
from time import sleep
import os
import posixpath as psp


def save_config(config, path=None):
  """Save configuration to file.

  :param config: :class:`~hdfs.config.Config` instance.

  """
  with open(path or config.path, 'w') as writer:
    config.write(writer)


class _IntegrationTest(object):

  """Base class to run tests using remote HDFS.

  These tests are run only if a `HDFSCLI_TEST_ALIAS` or `HDFSCLI_TEST_URL`
  environment variable is defined (the former taking precedence). For safety,
  a suffix is appended to any defined root.

  .. warning::

    The new root directory used is entirely cleaned during tests!

  Also contains a few helper functions.

  """

  client = None
  delay = 0.5 # Delay in seconds between tests.
  root_suffix = '.hdfscli' # Also used as default root if none specified.

  @classmethod
  def setup_class(cls):
    alias = os.getenv('HDFSCLI_TEST_ALIAS')
    url = os.getenv('HDFSCLI_TEST_URL')
    if alias:
      cls.client = Config().get_client(alias)
      if cls.client.root:
        cls.client.root = psp.join(cls.client.root, cls.root_suffix)
      else:
        cls.client.root = cls.root_suffix
    elif url:
      cls.client = InsecureClient(url, root=cls.root_suffix)

  @classmethod
  def teardown_class(cls):
    if cls.client:
      cls.client.delete('', recursive=True)

  def setup(self):
    if not self.client:
      raise SkipTest
    else:
      self.client.delete('', recursive=True)
      sleep(self.delay)

  # Helpers.

  def _read(self, hdfs_path):
    with self.client.read(hdfs_path) as reader:
      return reader.read()

  def _exists(self, hdfs_path):
    return bool(self.client.status(hdfs_path, strict=False))
