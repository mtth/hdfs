#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import *
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest


class TestConfig(object):

  def test_get_alias(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('[foo_alias]\nurl=1\nauth=k\nroot=2\n')
      config = Config(tpath)
      eq_(
        config.get_alias('foo'),
        {'url': '1', 'auth': 'k', 'root': '2'}
      )

  def test_get_alias_defaults(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('[foo_alias]\nurl=1\n')
      config = Config(tpath)
      eq_(
        config.get_alias('foo'),
        {'url': '1', 'auth': 'insecure', 'root': None}
      )

  @raises(HdfsError)
  def test_missing_alias(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('[foo_alias]\nurl=1\n')
      Config(tpath).get_alias('bar')

  @raises(HdfsError)
  def test_invalid_alias(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('[foo_alias]\nauth=2\n')
      Config(tpath).get_alias('foo')
