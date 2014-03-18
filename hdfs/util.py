#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
  RawConfigParser)
from os.path import exists, expanduser


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

  def get_option(self, command, name, default=None):
    """Get default option value for a command.

    :param command: Command the option should be looked up for.
    :param name: Name of the option.
    :param default: Default value to be returned if not found in the
      configuration file. If not provided, will raise
      :class:`~hdfs.util.HdfsError`.

    """
    try:
      return self.parser.get(command, 'default.%s' % (name, ))
    except (NoOptionError, NoSectionError):
      if default:
        return default
      else:
        raise HdfsError(
          'No default %(name)s found in %(path)r for %(command)s.\n'
          'You can specify one by adding a `default.%(name)s` option in the '
          '`%(command)s` section.'
          % {'command': command, 'name': name, 'path': self.path}
        )
