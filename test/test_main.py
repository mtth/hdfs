#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from hdfs.__main__ import _Progress, configure_client, main, parse_arg
from hdfs.config import Config, NullHandler
from hdfs.util import HdfsError, temppath
from logging.handlers import TimedRotatingFileHandler
from nose.tools import eq_, nottest, ok_, raises
from util import _IntegrationTest, save_config
import filecmp
import logging as lg
import os
import os.path as osp
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


class TestConfigureClient(object):

  def test_with_alias(self):
    url = 'http://host:port'
    with temppath() as tpath:
      config = Config(path=tpath)
      section = 'dev.alias'
      config.add_section(section)
      config.set(section, 'url', url)
      args = {'--alias': 'dev', '--log': False, '--verbose': 0}
      client = configure_client('test', args, config=config)
      eq_(client.url, url)

class TestProgress(object):

  def test_single_file(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        progress = _Progress(100, 1, writer=writer)
        progress('foo', 60)
        eq_(progress._data['foo'], 60)
        eq_(progress._pending_files, 0)
        eq_(progress._downloading_files, 1)
        progress('foo', 40)
        progress('foo', -1)
        eq_(progress._downloading_files, 0)
        eq_(progress._complete_files, 1)

  def test_from_local_path(self):
    with temppath() as dpath:
      os.mkdir(dpath)
      fpath1 = osp.join(dpath, 'foo')
      with open(fpath1, 'w') as writer:
        writer.write('hey')
      os.mkdir(osp.join(dpath, 'bar'))
      fpath2 = osp.join(dpath, 'bar', 'baz')
      with open(fpath2, 'w') as writer:
        writer.write('hello')
      with temppath() as tpath:
        with open(tpath, 'w') as writer:
          progress = _Progress.from_local_path(dpath, writer=writer)
          eq_(progress._total_bytes, 8)
          eq_(progress._pending_files, 2)


class TestMain(_IntegrationTest):

  dpath = osp.join(osp.dirname(__file__), 'dat')

  def setup(self):
    self._root_logger = lg.getLogger()
    self._handlers = self._root_logger.handlers
    super(TestMain, self).setup()

  def teardown(self):
    self._root_logger.handlers = self._handlers

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

  def test_download_stream(self):
    self.client.write('foo', 'hello')
    with temppath() as tpath:
      stdout = sys.stdout
      try:
        with open(tpath, 'wb') as writer:
          sys.stdout = writer
          main(
            ['download', 'foo', '-', '--silent', '--threads', '1'],
            self.client
          )
      finally:
        sys.stdout = stdout
      with open(tpath) as reader:
        eq_(reader.read(), 'hello')

  @raises(SystemExit)
  def test_download_stream_multiple_files(self):
    self.client.upload('foo', self.dpath)
    main(
      ['download', 'foo', '-', '--silent', '--threads', '1'],
      self.client
    )

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
