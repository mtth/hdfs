#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
  RawConfigParser)
from contextlib import contextmanager
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from os import close, remove
from shutil import rmtree
from tempfile import gettempdir, mkstemp
import logging as lg
import os.path as osp
import sys
import warnings as wr


_logger = lg.getLogger(__name__)


class HdfsError(Exception):

  """Base error class.

  :param message: Error message.
  :param args: optional Message formatting arguments.

  """

  def __init__(self, message, *args):
    super(HdfsError, self).__init__(message % args if args else message)


class InstanceLogger(lg.LoggerAdapter):

  """Logger adapter that preprends the instance's `repr` to messages.

  :param instance: Instance to prepend messages with.
  :param logger: Logger instance where messages will be logged.
  :param extra: Dictionary of contextual information, passed to the formatter.

  """

  def __init__(self, instance, logger, extra=None):
    # not using super since `LoggerAdapter` is an old-style class in python 2.6
    lg.LoggerAdapter.__init__(self, logger, extra)
    self.instance = instance

  def process(self, msg, kwargs):
    """Transform message.

    :param msg: Original message.
    :param kwargs: Dictionary of kwargs eventually passed to the formatter.

    """
    return '%r :: %s' % (self.instance, msg), kwargs


class Config(object):

  """Configuration class.

  :param path: path to configuration file. If no file exists at that location,
    the configuration parser will be empty.

  """

  def __init__(self, path=osp.expanduser('~/.hdfsrc')):
    self._logger = InstanceLogger(self, _logger)
    self.parser = RawConfigParser()
    self.path = path
    if osp.exists(path):
      try:
        self.parser.read(self.path)
      except ParsingError:
        raise HdfsError('Invalid configuration file %r.', path)

  def __repr__(self):
    return '<Config(path=%r)>' % (self.path, )

  def save(self):
    """Save configuration parser back to file."""
    with open(self.path, 'w') as writer:
      self.parser.write(writer)
    self._logger.info('Saved.')

  def get_alias(self, alias=None):
    """Retrieve alias information from configuration file.

    :param alias: Alias name. If not specified, will use the `default.alias` in
      the `hdfs` section if provided, else will raise :class:`HdfsError`.

    Raises :class:`HdfsError` if no matching / an invalid alias was found.

    """
    try:
      alias = alias or self.parser.get('hdfs', 'default.alias')
    except (NoOptionError, NoSectionError):
      raise HdfsError('No alias specified and no default alias found.')
    section = '%s_alias' % (alias, )
    try:
      options = dict(self.parser.items(section))
    except NoSectionError:
      raise HdfsError('Alias not found: %r.', alias)
    else:
      return options

  def get_file_handler(self):
    """Create and configure logging file handler.

    The default path can be configured via the `default.log` option in the
    `hdfs` section.

    """
    try:
      handler_path = self.parser.get('hdfs', 'log')
    except (NoOptionError, NoSectionError):
      handler_path = osp.join(gettempdir(), 'hdfs.log')
    try:
      handler = TimedRotatingFileHandler(
        handler_path,
        when='midnight', # daily backups
        backupCount=1,
        encoding='utf-8',
      )
    except IOError:
      wr.warn('Unable to write to log file at %s.' % (handler_path, ))
    else:
      handler_format = (
        '%(asctime)s | %(levelname)4.4s | %(name)s > %(message)s'
      )
      handler.setFormatter(lg.Formatter(handler_format))
      return handler


@contextmanager
def temppath():
  """Create a temporary path.

  Usage::

    with temppath() as path:
      pass # do stuff

  Any file or directory corresponding to the path will be automatically deleted
  afterwards.

  """
  (desc, path) = mkstemp()
  close(desc)
  remove(path)
  try:
    _logger.debug('Created temporary path at %s.', path)
    yield path
  finally:
    if osp.exists(path):
      if osp.isdir(path):
        rmtree(path)
        _logger.debug('Deleted temporary directory at %s.', path)
      else:
        remove(path)
        _logger.debug('Deleted temporary file at %s.', path)
    else:
      _logger.debug('No temporary file or directory to delete at %s.', path)

def hsize(size):
  """Transform size from bytes to human readable format (kB, MB, ...).

  :param size: Size in bytes.

  """
  for suffix in [' ', 'k', 'M', 'G', 'T']:
    if size < 1024.0:
      return '%4.0f%sB' % (size, suffix)
    size /= 1024.0

def htime(time):
  """Transform time from seconds to human readable format (min, hour, ...).

  :param time: Time in seconds.

  """
  for (multiplier, suffix) in [
    (60.0, 's'),
    (60, 'm'),
    (24, 'h'),
    (7, 'd'),
    (4, 'w'),
    (12, 'M'),
    (float('inf'), 'Y'),
  ]:
    if time < multiplier:
      return '%4.1f%s' % (time, suffix)
    time /= multiplier

def catch(*error_classes):
  """Returns a decorator that catches errors and prints messages to stderr.

  :param \*error_classes: Error classes.

  Also exits with status 1 if any errors are caught.

  """
  def decorator(func):
    """Decorator."""
    @wraps(func)
    def wrapper(*args, **kwargs):
      """Wrapper. Finally."""
      try:
        return func(*args, **kwargs)
      except error_classes as err:
        _logger.error(err)
        sys.stderr.write('%s\n' % (err, ))
        sys.exit(1)
      except Exception as err: # catch all
        _logger.exception('Unexpected exception.')
        raise RuntimeError('View log for details.')
    return wrapper
  return decorator
