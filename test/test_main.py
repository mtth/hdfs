#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from hdfs.__main__ import *
from hdfs.util import Config, temppath
from logging.handlers import TimedRotatingFileHandler
from nose.tools import eq_, ok_, raises
from string import Template
from util import _IntegrationTest
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
      config.add_section('cmd')
      config.set('cmd', 'default.alias', 'dev')
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
        config.add_section('cmd')
        config.set('cmd', 'log.disable', 'true')
        config.save()
        cli_config = CliConfig('cmd', path=tpath)
        ok_(not cli_config.get_file_handler())
    finally:
      lg.getLogger().handlers = [] # Clean up handlers.

  def _write_client_module(self, path, class_name):
    template = osp.join(osp.dirname(__file__), 'dat', 'client_template.py')
    with open(template) as reader:
      contents = Template(reader.read()).substitute({
        'class_name': class_name,
      })
    with open(path, 'w') as writer:
      writer.write(contents)

  def test_load_client_from_path(self):
    with temppath() as module_path:
      self._write_client_module(module_path, 'PathClient')
      with temppath() as config_path:
        config = Config(path=config_path)
        config.add_section('cmd')
        config.set('cmd', 'autoload.paths', module_path)
        section = 'dev.alias'
        config.add_section(section)
        config.set(section, 'client', 'PathClient')
        config.set(section, 'url', 'http://host:port')
        config.save()
        cli_config = CliConfig('cmd', path=config_path, verbosity=-1)
        client = cli_config.create_client('dev')
        eq_(client.one, 1)

  def test_load_client_from_module(self):
    with temppath() as module_dpath:
      os.mkdir(module_dpath)
      sys.path.append(module_dpath)
      module_fpath = osp.join(module_dpath, 'module_client.py')
      self._write_client_module(module_fpath, 'ModuleClient')
      try:
        with temppath() as config_path:
          config = Config(path=config_path)
          config.add_section('cmd')
          config.set('cmd', 'autoload.modules', 'module_client')
          section = 'dev.alias'
          config.add_section(section)
          config.set(section, 'client', 'ModuleClient')
          config.set(section, 'url', 'http://host:port')
          config.save()
          cli_config = CliConfig('cmd', path=config_path, verbosity=-1)
          client = cli_config.create_client('dev')
          eq_(client.one, 1)
      finally:
        sys.path.remove(module_dpath)


class TestMain(_IntegrationTest):

  def teardown(self):
    lg.getLogger().handlers = [] # Clean up handlers.

  def test_download(self):
    pass # TODO

  def test_upload(self):
    pass # TODO
