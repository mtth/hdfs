#!/usr/bin/env python
# encoding: utf-8

"""Test helpers."""

from hdfs import InsecureClient
from hdfs.config import Config
from hdfs.util import HdfsError
from requests.exceptions import ConnectionError
from six.moves.configparser import NoOptionError, NoSectionError
from time import sleep
import os
import posixpath as psp
import pytest


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

  def setup_method(self):
    if not self.client:
      pytest.skip()
    else:
      try:
        self.client.delete('', recursive=True)
        # Wrapped inside a `ConnectionError` block because this causes failures
        # when trying to reuse some streamed connections when they aren't fully
        # read (even though it is closed explicitly, it acts differently than
        # when all its content has been read), but only on HttpFS. A test which
        # needs this for example is `test_ext_avro.py:TestMain.test_schema`.
        # This seems related to this issue:
        # https://github.com/kennethreitz/requests/issues/1915 (even on more
        # recent versions of `requests` though).
        #
        # Here is a simple test case that will pass on WebHDFS but fail on
        # HttpFS:
        #
        # .. code:: python
        #
        #   client = Config().get_client('test-webhdfs')
        #   client.write('foo', 'hello')
        #   with client.read('foo') as reader:
        #     pass # Will succeed if this is replaced by `reader.read()`.
        #   client.delete('foo')
        #
      except ConnectionError:
        self.client.delete('', recursive=True) # Retry.
      finally:
        sleep(self.delay)

  # Helpers.

  def _read(self, hdfs_path, encoding=None):
    with self.client.read(hdfs_path, encoding=encoding) as reader:
      return reader.read()

  def _write(self, hdfs_path, data, encoding=None):
    with self.client.write(hdfs_path, encoding=encoding) as writer:
      return writer.write(data)

  def _exists(self, hdfs_path):
    return bool(self.client.status(hdfs_path, strict=False))
