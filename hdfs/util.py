#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
  RawConfigParser)
from contextlib import contextmanager
from functools import wraps
from itertools import chain
from os import close, remove
from os.path import exists, expanduser
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

  def __init__(self, path=expanduser('~/.hdfsrc')):
    self.parser = RawConfigParser()
    self.path = path
    if exists(path):
      try:
        self.parser.read(self.path)
      except ParsingError:
        raise AzkabanError('Invalid configuration file %r.', path)

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
    try:
      return {
        'url': options['url'],
        'auth': options.get('auth', 'insecure'),
        'root': options.get('root', None),
      }
    except KeyError:
      raise HdfsError('No URL found for alias %r.', alias)


class HdfsInfo(object):

  """Wrapper around FileStatus JSON objects.

  Cf. http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatus

  :param status: FileStatus dictionary.
  :param folder: Head of path.

  """

  summary = None

  def __init__(self, status, folder):
    path_suffix = status['pathSuffix']
    folder = folder.rstrip('/')
    if folder and path_suffix:
      self.path = '%s/%s' % (folder, path_suffix)
    else:
      self.path = path_suffix or folder
    self.status = status

  def __str__(self):
    return '%s%s' % (self.path, '/' if self.is_dir else '')

  @property
  def is_dir(self):
    """Is the underlying node a directory?"""
    return self.status['type'] == 'DIRECTORY'

  @property
  def size(self):
    """Size in bytes."""
    return self.summary['length'] if self.summary else self.status['length']

  def add_summary(self, summary):
    """Add summary info for directories.

    :param summary: Content summary dictionary.

    """
    self.summary = summary


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
  for suffix in ['B', 'kB', 'MB', 'GB', 'TB']:
    if size < 1024.0:
      return '%3.0f%s' % (size, suffix)
    size /= 1024.0

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
