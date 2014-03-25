#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
  RawConfigParser)
from contextlib import contextmanager
from functools import wraps
from os import close, remove
from os.path import exists
from tempfile import mkstemp
import sys


class HdfsError(Exception):

  """Base error class.

  :param message: error message
  :param args: optional message formatting arguments

  """

  def __init__(self, message, *args):
    super(HdfsError, self).__init__(message % args or ())


class Config(object):

  """Configuration class.

  :param path: path to configuration file. If no file exists at that location,
    the configuration parser will be empty.

  """

  def __init__(self, path):
    self.parser = RawConfigParser()
    self.path = path
    if exists(path):
      try:
        self.parser.read(self.path)
      except ParsingError:
        raise HdfsError('Invalid configuration file %r.', path)

  def save(self):
    """Save configuration parser back to file."""
    with open(self.path, 'w') as writer:
      self.parser.write(writer)

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


@contextmanager
def temppath():
  """Create a temporary filepath.

  Usage::

    with temppath() as path:
      # do stuff

  Any file corresponding to the path will be automatically deleted afterwards.

  """
  (desc, path) = mkstemp()
  close(desc)
  remove(path)
  try:
    yield path
  finally:
    if exists(path):
      remove(path)

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

  :param *error_classes: Error classes.

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
        sys.stderr.write('%s\n' % (err, ))
        sys.exit(1)
    return wrapper
  return decorator
