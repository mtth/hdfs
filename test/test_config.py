#!/usr/bin/env python
# encoding: utf-8

"""Test configuration module."""

from hdfs.client import Client
from hdfs.config import Config
from hdfs.util import HdfsError, temppath
from logging.handlers import TimedRotatingFileHandler
from nose.tools import eq_, ok_, nottest, raises
from string import Template
from util import save_config
import logging as lg
import os
import os.path as osp
import sys


class TestConfig(object):

  @nottest # TODO: Find cross-platform way to reset the environment variable.
  def test_config_path(self):
    path = os.getenv('HDFSCLI_CONFIG')
    try:
      with temppath() as tpath:
        os.environ['HDFSCLI_CONFIG'] = tpath
        with open(tpath, 'w') as writer:
          writer.write('[foo]\nbar=hello')
        eq_(Config().get('foo', 'bar'), 'hello')
    finally:
      if path:
        os['HDFSCLI_CONFIG'] = path
      else:
        del os['HDFSCLI_CONFIG']

  def _write_client_module(self, path, class_name):
    template = osp.join(osp.dirname(__file__), 'dat', 'client_template.py')
    with open(template) as reader:
      contents = Template(reader.read()).substitute({
        'class_name': class_name,
      })
    with open(path, 'w') as writer:
      writer.write(contents)

  def test_autoload_client_from_path(self):
    with temppath() as module_path:
      self._write_client_module(module_path, 'PathClient')
      with temppath() as config_path:
        config = Config(config_path)
        config.add_section(config.global_section)
        config.set(config.global_section, 'autoload.paths', module_path)
        config._autoload()
        client = Client.from_options({'url': ''}, 'PathClient')
        eq_(client.one, 1)

  @raises(SystemExit)
  def test_autoload_missing_path(self):
    with temppath() as module_path:
      with temppath() as config_path:
        config = Config(config_path)
        config.add_section(config.global_section)
        config.set(config.global_section, 'autoload.paths', module_path)
        config._autoload()

  def test_autoload_client_from_module(self):
    with temppath() as module_dpath:
      os.mkdir(module_dpath)
      sys.path.append(module_dpath)
      module_fpath = osp.join(module_dpath, 'mclient.py')
      self._write_client_module(module_fpath, 'ModuleClient')
      try:
        with temppath() as config_path:
          config = Config(config_path)
          config.add_section(config.global_section)
          config.set(config.global_section, 'autoload.modules', 'mclient')
          config._autoload()
          client = Client.from_options({'url': ''}, 'ModuleClient')
          eq_(client.one, 1)
      finally:
        sys.path.remove(module_dpath)

  def test_create_client_with_alias(self):
    with temppath() as tpath:
      config = Config(path=tpath)
      section = 'dev.alias'
      config.add_section(section)
      config.set(section, 'url', 'http://host:port')
      save_config(config)
      Config(path=tpath).get_client('dev')

  def test_create_client_with_alias_and_timeout(self):
    with temppath() as tpath:
      config = Config(path=tpath)
      section = 'dev.alias'
      config.add_section(section)
      config.set(section, 'url', 'http://host:port')
      config.set(section, 'timeout', '1')
      save_config(config)
      eq_(Config(path=tpath).get_client('dev')._timeout, 1)
      config.set(section, 'timeout', '1,2')
      save_config(config)
      eq_(Config(path=tpath).get_client('dev')._timeout, (1,2))

  @raises(HdfsError)
  def test_create_client_with_missing_alias(self):
    with temppath() as tpath:
      Config(tpath).get_client('dev')

  @raises(HdfsError)
  def test_create_client_with_no_alias_without_default(self):
    with temppath() as tpath:
      Config(tpath).get_client()

  def test_create_client_with_default_alias(self):
    with temppath() as tpath:
      config = Config(tpath)
      config.add_section(config.global_section)
      config.set(config.global_section, 'default.alias', 'dev')
      section = 'dev.alias'
      config.add_section(section)
      config.set(section, 'url', 'http://host:port')
      save_config(config)
      Config(tpath).get_client()

  def test_get_file_handler(self):
    with temppath() as tpath:
      config = Config(tpath)
      handler = config.get_log_handler('cmd')
      ok_(isinstance(handler, TimedRotatingFileHandler))

  def test_disable_file_logging(self):
    with temppath() as tpath:
      config = Config(tpath)
      config.add_section('cmd.command')
      config.set('cmd.command', 'log.disable', 'true')
      save_config(config)
      config = Config(tpath)
      handler = config.get_log_handler('cmd')
      ok_(not isinstance(handler, TimedRotatingFileHandler))
