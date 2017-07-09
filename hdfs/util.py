#!/usr/bin/env python
# encoding: utf-8

"""Common utilities."""

from contextlib import contextmanager
from shutil import rmtree
from six.moves.queue import Queue
from tempfile import mkstemp
from threading import Thread
import logging as lg
import os
import os.path as osp


_logger = lg.getLogger(__name__)


class HdfsError(Exception):

  """Base error class.

  :param message: Error message.
  :param args: optional Message formatting arguments.
  :param kwargs: optional extra values to store

  """

  def __init__(self, message, *args, **kwargs):
    super(HdfsError, self).__init__(message % args if args else message)
    self.response = kwargs.get('response', None)

class AsyncWriter(object):

  """Asynchronous publisher-consumer.

  :param consumer: Function which takes a single generator as argument.

  This class can be used to transform functions which expect a generator into
  file-like writer objects. This can make it possible to combine different APIs
  together more easily. For example, to send streaming requests:

  .. code-block:: python

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
        _logger.debug('Starting consumer.')
        self._consumer(data)
      except Exception as err: # pylint: disable=broad-except
        _logger.debug('Exception in child.')
        self._err = err
      finally:
        _logger.debug('Finished consumer.')

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
    if exc_value:
      _logger.debug('Exception in parent.')
    if self._reader and self._reader.is_alive():
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
  os.close(desc)
  os.remove(path)
  try:
    _logger.debug('Created temporary path at %s.', path)
    yield path
  finally:
    if osp.exists(path):
      if osp.isdir(path):
        rmtree(path)
        _logger.debug('Deleted temporary directory at %s.', path)
      else:
        os.remove(path)
        _logger.debug('Deleted temporary file at %s.', path)
    else:
      _logger.debug('No temporary file or directory to delete at %s.', path)
