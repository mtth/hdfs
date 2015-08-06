#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from contextlib import contextmanager
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from os import close, remove
from shutil import rmtree
from six.moves.configparser import (NoOptionError, NoSectionError,
  ParsingError, RawConfigParser)
from six.moves.queue import Queue
from tempfile import gettempdir, mkstemp
from threading import Thread
import logging as lg
import os
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
    the configuration parser will be empty. If not specified, the value of the
    `HDFSCLI_RCPATH` environment variable is used if it exists, otherwise it
    defaults to `~/.hdfsrc`.

  """

  default_path = osp.expanduser('~/.hdfsrc')

  def __init__(self, path=None):
    self.path = path or os.getenv('HDFSCLI_RCPATH', self.default_path)
    self._logger = InstanceLogger(self, _logger)
    self.parser = RawConfigParser()
    if osp.exists(self.path):
      try:
        self.parser.read(self.path)
      except ParsingError:
        raise HdfsError('Invalid configuration file %r.', self.path)

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
    for suffix in ('.alias', '_alias'):
      section = '%s%s' % (alias, suffix)
      try:
        options = dict(self.parser.items(section))
      except NoSectionError:
        pass # Backwards compatibility.
      else:
        return options
    raise HdfsError('Alias not found: %r.', alias)

  def get_file_handler(self, name):
    """Create and configure logging file handler.

    :param name: Section name used to find the path to the log file. If no
      `log` option exists in this section, the path will default to
      `<name>.log`.

    The default path can be configured via the `default.log` option in the
    `hdfs` section.

    """
    try:
      handler_path = self.parser.get(name, 'log')
    except (NoOptionError, NoSectionError):
      handler_path = osp.join(gettempdir(), '%s.log' % (name, ))
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

  @staticmethod
  def parse_boolean(s):
    """Parse configuration value into boolean.

    :param s: Element to be parsed.

    Behavior is similar to the `RawConfigParser.getboolean` function for
    strings.

    """
    if not s:
      return False
    s = str(s).lower()
    if s in set(['1', 'yes', 'true', 'on']):
      return True
    elif s in set(['0', 'no', 'false', 'off']):
      return False
    else:
      raise ValueError('Invalid boolean string: %r' % (s, ))


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
    self._err = None

  def __enter__(self):
    if self._queue:
      raise ValueError('Cannot nest contexts.')
    self._queue = Queue()

    def consumer(data):
      """Wrapped consumer that lets us get a child's exception."""
      try:
        self._consumer(data)
      except Exception as err:
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
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self._queue.put(None)
    self._reader.join()
    if self._err:
      raise self._err # Child error.
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


class SeekableReader(object):

  """Buffered reader.

  :param reader: Non-seekable reader.
  :param nbytes: Backlog to keep.

  Uses a circular byte array.

  """

  def __init__(self, reader, nbytes):
    self._reader = reader
    self._size = nbytes + 1
    self._buffer = bytearray(self._size)
    self._start = 0
    self._end = 0

  def read(self, nbytes):
    count = (self._end - self._start) % self._size
    if count:
      missing = nbytes - count
      if missing <= 0:
        chunk = self._buffer[self._start:]
        self._start = 1
        return chunk
      else:
        pass
    chunk = self._reader.read(nbytes)
    # TODO: buffer this chunk.
    return chunk

  def seek(self, offset, whence=0):
    # TODO: this.
    pass


@contextmanager
def temppath(dir=None):
  """Create a temporary path.

  :param dir: Explicit directory name where to create the temporary path. A
    system dependent default will be used otherwise (cf. `tempfile.mkstemp`).

  Usage::

    with temppath() as path:
      pass # do stuff

  Any file or directory corresponding to the path will be automatically deleted
  afterwards.

  """
  (desc, path) = mkstemp(dir=dir)
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
        sys.stderr.write(
          'Unexpected error: %s\n'
          'View log for more details.\n' % (err, )
        )
        sys.exit(1)
    return wrapper
  return decorator
