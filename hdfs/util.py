#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from contextlib import contextmanager
from functools import wraps
from os import close, remove
from shutil import rmtree
from six.moves.configparser import ParsingError, RawConfigParser
from six.moves.queue import Queue
from tempfile import mkstemp
from threading import Thread
import logging as lg
import os
import os.path as osp
import sys


_logger = lg.getLogger(__name__)


class HdfsError(Exception):

  """Base error class.

  :param message: Error message.
  :param args: optional Message formatting arguments.

  """

  def __init__(self, message, *args):
    super(HdfsError, self).__init__(message % args if args else message)


class Config(RawConfigParser):

  """Configuration class.

  :param path: path to configuration file. If no file exists at that location,
    the configuration parser will be empty. If not specified, the value of the
    `HDFSCLI_RCPATH` environment variable is used if it exists, otherwise it
    defaults to `~/.hdfsclirc`.

  """

  default_path = osp.expanduser('~/.hdfsclirc')

  def __init__(self, path=None):
    RawConfigParser.__init__(self)
    self.path = path or os.getenv('HDFSCLI_RCPATH', self.default_path)
    if osp.exists(self.path):
      try:
        self.read(self.path)
      except ParsingError:
        raise HdfsError('Invalid configuration file %r.', self.path)
      _logger.info('Instantiated configuration from %r.', self.path)
    else:
      _logger.info('Instantiated empty configuration.')

  def __repr__(self):
    return '<Config(path=%r)>' % (self.path, )

  def save(self):
    """Save configuration parser back to file."""
    with open(self.path, 'w') as writer:
      self.write(writer)
    _logger.info('Saved.')

  @staticmethod
  def parse_boolean(value):
    """Parse configuration value into boolean.

    :param value: String element to be parsed.

    Behavior is similar to the `RawConfigParser.getboolean` function for
    strings.

    """
    if not value:
      return False
    value = str(value).lower()
    if value in set(['1', 'yes', 'true', 'on']):
      return True
    elif value in set(['0', 'no', 'false', 'off']):
      return False
    else:
      raise ValueError('Invalid boolean string: %r' % (value, ))


class AsyncWriter(object):

  """Asynchronous publisher-consumer.

  :param consumer: Function which takes a single generator as argument.

  This class can be used to transform functions which expect a generator into
  file-like writer objects. This can make it possible to combine different APIs
  together more easily. For example, to send streaming requests:

  .. code:: python

    import requests as rq

    with AsyncWriter(lambda data: rq.post(URL, data=data)) as writer:
      writer.write('Hello, world!')

  """

  def __init__(self, consumer):
    self._consumer = consumer
    self._queue = None
    self._reader = None
    self._err = None
    _logger.debug('Instantiated %r.', self)

  def __repr__(self):
    return '<%s(consumer=%r)>' % (self.__class__.__name__, self._consumer)

  def __enter__(self):
    if self._queue:
      raise ValueError('Cannot nest contexts.')
    self._queue = Queue()
    self._err = None

    def consumer(data):
      """Wrapped consumer that lets us get a child's exception."""
      try:
        self._consumer(data)
      except Exception as err: # pylint: disable=broad-except
        self._err = err

    def reader(queue):
      """Generator read by the consumer."""
      while True:
        chunk = queue.get()
        if chunk is None:
          break
        yield chunk

    self._reader = Thread(target=consumer, args=(reader(self._queue), ))
    self._reader.start()
    _logger.debug('Started child thread.')
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    _logger.debug('Signaling child.')
    self._queue.put(None)
    self._reader.join()
    if self._err:
      raise self._err # pylint: disable=raising-bad-type
    else:
      _logger.debug('Child terminated without errors.')
    self._queue = None

  def flush(self):
    """Pass-through implementation."""
    pass

  def write(self, chunk):
    """Stream data to the underlying consumer.

    :param chunk: Bytes to write. These will be buffered in memory until the
      consumer reads them.

    """
    self._queue.put(chunk)


@contextmanager
def temppath(dpath=None):
  """Create a temporary path.

  :param dpath: Explicit directory name where to create the temporary path. A
    system dependent default will be used otherwise (cf. `tempfile.mkstemp`).

  Usage::

    with temppath() as path:
      pass # do stuff

  Any file or directory corresponding to the path will be automatically deleted
  afterwards.

  """
  (desc, path) = mkstemp(dir=dpath)
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
        sys.exit(1)
      except Exception as err: # pylint: disable=broad-except
        _logger.exception('Unexpected exception.')
        sys.stderr.write('View log for more details.\n')
        sys.exit(1)
    return wrapper
  return decorator
