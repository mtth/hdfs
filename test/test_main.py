#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from hdfs.__main__ import *
from hdfs.util import temppath
from logging.handlers import TimedRotatingFileHandler
from nose.tools import eq_, nottest, ok_, raises
from util import _IntegrationTest
import filecmp
import logging as lg
import sys


class TestParseArg(object):

  @raises(HdfsError)
  def test_parse_invalid(self):
    parse_arg({'foo': 'a'}, 'foo', int)

  def test_parse_int(self):
    eq_(parse_arg({'foo': '1'}, 'foo', int), 1)
    eq_(parse_arg({'foo': '1'}, 'foo', int, ','), 1)

  def test_parse_float(self):
    eq_(parse_arg({'foo': '123.4'}, 'foo', float), 123.4)

  def test_parse_int_list(self):
    eq_(parse_arg({'foo': '1,'}, 'foo', int, ','), [1])
    eq_(parse_arg({'foo': '1,2'}, 'foo', int, ','), [1,2])


class TestMain(_IntegrationTest):

  dpath = osp.join(osp.dirname(__file__), 'dat')

  def teardown(self):
    lg.getLogger().handlers = [] # Clean up handlers.

  def _dircmp(self, dpath):
    dircmp = filecmp.dircmp(self.dpath, dpath)
    ok_(not dircmp.left_only)
    ok_(not dircmp.right_only)
    ok_(not dircmp.diff_files)

  def test_download(self):
    self.client.upload('foo', self.dpath)
    with temppath() as tpath:
      main(
        ['download', 'foo', tpath, '--silent', '--threads', '1'],
        self.client
      )
      self._dircmp(tpath)

  @raises(SystemExit)
  def test_download_overwrite(self):
    self.client.upload('foo', self.dpath)
    with temppath() as tpath:
      with open(tpath, 'w'):
        pass
      main(
        ['download', 'foo', tpath, '--silent', '--threads', '1'],
        self.client
      )
      self._dircmp(tpath)

  def test_download_force(self):
    self.client.write('foo', 'hey')
    with temppath() as tpath:
      with open(tpath, 'w'):
        pass
      main(
        ['download', 'foo', tpath, '--silent', '--force', '--threads', '1'],
        self.client
      )
      with open(tpath) as reader:
        eq_(reader.read(), 'hey')

  def test_upload(self):
    main(
      ['upload', self.dpath, 'bar', '--silent', '--threads', '1'],
      self.client
    )
    with temppath() as tpath:
      self.client.download('bar', tpath)
      self._dircmp(tpath)

  @raises(SystemExit)
  def test_upload_overwrite(self):
    self.client.write('bar', 'hey')
    main(
      ['upload', self.dpath, 'bar', '--silent', '--threads', '1'],
      self.client
    )

  def test_upload_force(self):
    self.client.write('bar', 'hey')
    main(
      ['upload', self.dpath, 'bar', '--silent', '--threads', '1', '--force'],
      self.client
    )
    with temppath() as tpath:
      self.client.download('bar', tpath)
      self._dircmp(tpath)

  def test_upload_append(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hey')
      main(['upload', tpath, 'bar', '--silent', '--threads', '1'], self.client)
      main(
        ['upload', tpath, 'bar', '--silent', '--threads', '1', '--append'],
        self.client
      )
    with self.client.read('bar') as reader:
      eq_(reader.read(), b'heyhey')

  @raises(SystemExit)
  def test_upload_append_folder(self):
    with temppath() as tpath:
      main(['upload', self.dpath, '--silent', '--append'], self.client)
