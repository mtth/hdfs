#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import *
from nose.tools import eq_, raises


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
        {'url': '1'},
      )

  @raises(HdfsError)
  def test_missing_alias(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('[foo_alias]\nurl=1\n')
      Config(tpath).get_alias('bar')


class TestHuman(object):

  def test_hsize(self):
    eq_(hsize(0), '   0 B')
    eq_(hsize(1023), '1023 B')
    eq_(hsize(1024), '   1kB')

  def test_htime(self):
    eq_(htime(0), ' 0.0s')
    eq_(htime(50), '50.0s')
    eq_(htime(60), ' 1.0m')
    eq_(htime(90), ' 1.5m')
    eq_(htime(3600), ' 1.0h')
    eq_(htime(3600 * 24 * 7 * 4 * 12 * 24), '24.0Y')
