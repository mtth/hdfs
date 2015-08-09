#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from hdfs.__main__ import *
from hdfs.util import Config, temppath
from logging.handlers import TimedRotatingFileHandler
from nose.tools import eq_, ok_, raises
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


class TestCliConfig(object):

  def test_create_client_with_alias(self):
    with temppath() as tpath:
      config = Config(path=tpath)
      section = 'dev.alias'
      config.add_section(section)
      config.set(section, 'url', 'http://host:port')
      config.save()
      cli_config = CliConfig('cmd', path=tpath, verbosity=-1)
      cli_config.create_client(alias='dev')

  @raises(HdfsError)
  def test_create_client_with_missing_alias(self):
    with temppath() as tpath:
      cli_config = CliConfig('cmd', path=tpath, verbosity=-1)
      cli_config.create_client(alias='dev')

  @raises(HdfsError)
  def test_create_client_with_no_alias_without_default(self):
    with temppath() as tpath:
      cli_config = CliConfig('cmd', path=tpath, verbosity=-1)
      cli_config.create_client()

  def test_create_client_with_default_alias(self):
    with temppath() as tpath:
      config = Config(path=tpath)
      config.add_section(config.global_section)
      config.set(config.global_section, 'default.alias', 'dev')
      section = 'dev.alias'
      config.add_section(section)
      config.set(section, 'url', 'http://host:port')
      config.save()
      cli_config = CliConfig('cmd', path=tpath, verbosity=-1)
      cli_config.create_client()

  def test_get_file_handler(self):
    try:
      with temppath() as tpath:
        cli_config = CliConfig('cmd', path=tpath)
        handler = cli_config.get_file_handler()
        ok_(isinstance(handler, TimedRotatingFileHandler))
    finally:
      lg.getLogger().handlers = [] # Clean up handlers.

  def test_disable_file_logging(self):
    try:
      with temppath() as tpath:
        config = Config(path=tpath)
        config.add_section('cmd.command')
        config.set('cmd.command', 'log.disable', 'true')
        config.save()
        cli_config = CliConfig('cmd', path=tpath)
        ok_(not cli_config.get_file_handler())
    finally:
      lg.getLogger().handlers = [] # Clean up handlers.


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
      main(['download', 'foo', tpath, '--silent'], self.client)
      self._dircmp(tpath)

  def test_upload(self):
    main(['upload', self.dpath, 'bar', '--silent'], self.client)
    with temppath() as tpath:
      self.client.download('bar', tpath)
      self._dircmp(tpath)
