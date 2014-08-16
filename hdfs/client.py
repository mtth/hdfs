#!/usr/bin/env python
# encoding: utf-8

"""HDFS clients."""

from .util import Config, HdfsError, InstanceLogger, temppath
from getpass import getuser
from itertools import repeat
from multiprocessing.pool import ThreadPool
from random import sample
from shutil import move
import logging as lg
import os
import os.path as osp
import posixpath
import re
import requests as rq
import time


_logger = lg.getLogger(__name__)


def _on_error(response):
  """Callback when an API response has a non 2XX status code.

  :param response: Response.

  """
  if response.status_code == 401:
    raise HdfsError('Authentication failure. Check your credentials.')
  try:
    # Cf. http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#Error+Responses
    message = response.json()['RemoteException']['message']
  except ValueError:
    # No clear one thing to display, display entire message content
    message = response.content
  raise HdfsError(message)


class _Request(object):

  """Class to define API requests.

  :param verb: HTTP verb (`'GET'`, `'PUT'`, etc.).
  :param kwargs: Keyword arguments passed to the request handler.

  """

  webhdfs_prefix = '/webhdfs/v1'
  doc_url = 'http://hadoop.apache.org/docs/r1.0.4/webhdfs.html'

  def __init__(self, verb, **kwargs):
    self.kwargs = kwargs
    try:
      self.handler = getattr(rq, verb.lower())
    except AttributeError:
      raise HdfsError('Invalid HTTP verb %r.', verb)

  def __call__(self):
    pass # make pylint happy

  def to_method(self, operation):
    """Returns method associated with request to attach to client.

    :param operation: operation name.

    This is called inside the metaclass to switch :class:`_Request` objects
    with the method they represent.

    """
    def api_handler(client, path, data=None, **params):
      """Wrapper function."""
      url = '%s%s%s' % (
        client.url,
        self.webhdfs_prefix,
        client.resolve(path),
      )
      params['op'] = operation
      for key, value in client._params.items():
        params.setdefault(key, value)
      response = self.handler(
        url=url,
        auth=client._auth,
        data=data,
        params=params,
        **self.kwargs
      )
      if not response: # non 2XX status code
        client._logger.warning(
          '[%s] %s:\n%s', response.status_code,
          response.request.path_url, response.text
        )
        return _on_error(response)
      else:
        client._logger.debug(
          '[%s] %s', response.status_code, response.request.path_url
        )
        return response
    api_handler.__name__ = '%s_handler' % (operation.lower(), )
    api_handler.__doc__ = 'Cf. %s#%s' % (self.doc_url, operation)
    return api_handler


class _ClientType(type):

  """Metaclass that enables short and dry request definitions.

  This metaclass transforms any :class:`_Request` instances into their
  corresponding API handlers. Note that the operation used is determined
  directly from the name of the attribute (trimming numbers and underscores and
  uppercasing it).

  """

  pattern = re.compile(r'_|\d')

  def __new__(mcs, name, bases, attrs):
    for key, value in attrs.items():
      if isinstance(value, _Request):
        attrs[key] = value.to_method(mcs.pattern.sub('', key).upper())
    client = super(_ClientType, mcs).__new__(mcs, name, bases, attrs)
    client.__registry__[client.__name__] = client
    return client


class Client(object):

  """Base HDFS web client.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param auth: Authentication mechanism (forwarded to the request handler).
  :param params: Extra parameters forwarded with every request. Useful for
    custom authentication. Parameters specified in the request handler will
    override these defaults.
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters.

  In general, this client should only be used when its subclasses (e.g.
  :class:`InsecureClient`, :class:`~TokenClient`, :class:`~KerberosClient`) do
  not provide enough flexibility.

  """

  __metaclass__ = _ClientType
  __registry__ = {}

  def __init__(self, url, root=None, auth=None, params=None, proxy=None):
    self._logger = InstanceLogger(self, _logger)
    self._class_name = self.__class__.__name__ # cache this
    self.url = url
    self.root = root
    self._auth = auth
    self._params = params or {}
    if proxy:
      self._params['doas'] = proxy

  def __repr__(self):
    return '<%s(url=%s, root=%s)>' % (self._class_name, self.url, self.root)

  # Raw API endpoints

  _append = _Request('PUT') # doesn't allow for streaming
  _append_1 = _Request('POST', allow_redirects=False) # cf. `read`
  _create = _Request('PUT') # doesn't allow for streaming
  _create_1 = _Request('PUT', allow_redirects=False) # cf. `write`
  _delete = _Request('DELETE')
  _get_content_summary = _Request('GET')
  _get_file_checksum = _Request('GET')
  _get_file_status = _Request('GET')
  _get_home_directory = _Request('GET')
  _list_status = _Request('GET')
  _mkdirs = _Request('PUT')
  _open = _Request('GET', stream=True)
  _rename = _Request('PUT')
  _set_owner = _Request('PUT')
  _set_permission = _Request('PUT')
  _set_replication = _Request('PUT')
  _set_times = _Request('PUT')

  # Exposed endpoints

  def resolve(self, hdfs_path):
    """Return absolute, normalized path, with special markers expanded.

    :param hdfs_path: Remote path.

    Currently supported markers:

    * `'#LATEST'`: this marker gets expanded to the most recently updated file
      or folder. They can be combined using the `'{N}'` suffix. For example,
      `'foo/#LATEST{2}'` is equivalent to `'foo/#LATEST/#LATEST'`.

    """
    path = hdfs_path
    if not (osp.isabs(path) or self.root and osp.isabs(self.root)):
      raise HdfsError('Path %r is relative but no absolute root found.', path)
    path = osp.normpath(osp.join(self.root, path))

    def expand_latest(match):
      """Substitute #LATEST marker."""
      prefix = match.string[:match.start()]
      suffix = ''
      n = match.group(1) # n as in {N} syntax
      for _ in repeat(None, int(n) if n else 1):
        statuses = self._list_status(osp.join(prefix, suffix)).json()
        candidates = sorted([
          (-status['modificationTime'], status['pathSuffix'])
          for status in statuses['FileStatuses']['FileStatus']
        ])
        if not candidates:
          raise HdfsError('Cannot expand #LATEST. %r is empty.', prefix)
        elif len(candidates) == 1 and candidates[0][1] == '':
          raise HdfsError('Cannot expand #LATEST. %r is a file.', prefix)
        suffix = osp.join(suffix, candidates[0][1])
      return os.sep + suffix

    path = re.sub(r'/?#LATEST(?:{(\d+)})?(?=/|$)', expand_latest, path)
    # #LATEST expansion (could cache the pattern, but not worth it)

    self._logger.debug('Resolved path %s to %s.', hdfs_path, path)
    return path

  def content(self, hdfs_path):
    """Get content summary for a file or folder on HDFS.

    :param hdfs_path: Remote path.

    This method returns a JSON ContentSummary_ object if `hdfs_path` exists and
    raises :class:`HdfsError` otherwise.

    .. _ContentSummary: CS_
    .. _CS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#ContentSummary

    """
    self._logger.info('Fetching content summary for %s.', hdfs_path)
    return self._get_content_summary(hdfs_path).json()['ContentSummary']

  def status(self, hdfs_path):
    """Get status for a file or folder on HDFS.

    :param hdfs_path: Remote path.

    This method returns a JSON FileStatus_ object if `hdfs_path` exists and
    raises :class:`HdfsError` otherwise.

    .. _FileStatus: FS_
    .. _FS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatus

    """
    self._logger.info('Fetching status for %s.', hdfs_path)
    return self._get_file_status(hdfs_path).json()['FileStatus']

  def parts(self, hdfs_path, parts=None):
    """Returns a dictionary of part-files corresponding to a path.

    :param hdfs_path: Remote path. If it points to a non partitioned file,
      a dictionary with only element that path is returned. This makes it
      easier to handle these two cases.
    :param parts: List of part-files numbers or total number of part-files to
      select. If a number, that many partitions will be chosen at random. By
      default all part-files are returned. If `parts` is a list and one of the
      parts is not found or too many samples are demanded, an
      :class:`~hdfs.util.HdfsError` is raised.

    """
    self._logger.debug('Fetching parts for %s.', hdfs_path)
    status = self.status(hdfs_path)
    if status['type'] == 'FILE':
      if parts and parts != 1 and parts != [0]:
        raise HdfsError('%r is not partitioned.', hdfs_path)
      return {hdfs_path: status}
    else:
      pattern = re.compile(r'^part-(?:(?:m|r)-|)(\d+)[^/]*$')
      matches = (
        (path, pattern.match(status['pathSuffix']), status)
        for path, status in self.walk(hdfs_path, depth=1)
      )
      part_files = dict(
        (int(match.group(1)), (path, status))
        for path, match, status in matches
        if match
      )
      self._logger.debug(
        'Found %s parts for %s: %s.', len(part_files), hdfs_path,
        ', '.join(t[0] for t in sorted(part_files.values()))
      )
      if parts:
        if isinstance(parts, int):
          self._logger.debug('Choosing %s parts randomly.', parts)
          if parts > len(part_files):
            raise HdfsError('Not enough part-files in %r.', hdfs_path)
          parts = sample(part_files, parts)
        try:
          paths = dict(part_files[p] for p in parts)
        except KeyError as err:
          raise HdfsError('No part-file %r in %r.', err.args[0], hdfs_path)
        self._logger.info(
          'Returning %s of %s parts for %s: %s.', len(paths), len(part_files),
          hdfs_path, ', '.join(sorted(paths))
        )
      else:
        paths = dict(part_files.values())
        self._logger.info(
          'Returning all %s parts for %s: %s.', len(paths), hdfs_path,
          ', '.join(sorted(paths))
        )
      return paths

  def write(self, hdfs_path, data, overwrite=False, permission=None,
    blocksize=None, replication=None, buffersize=None):
    """Create a file on HDFS.

    :param hdfs_path: Path where to create file. The necessary directories will
      be created appropriately.
    :param data: Contents of file to write. Can be a string, a generator or a
      file object. The last two options will allow streaming upload (i.e.
      without having to load the entire contents into memory).
    :param overwrite: Overwrite any existing file or directory.
    :param permissions: Octal permissions to set on the newly created file.
      Leading zeros may be omitted.
    :param blocksize: Block size of the file.
    :param replication: Number of replications of the file.
    :param buffersize: Size of upload buffer.

    """
    self._logger.info('Writing to %s.', hdfs_path)
    res_1 = self._create_1(
      hdfs_path,
      overwrite=overwrite,
      permission=permission,
      blocksize=blocksize,
      replication=replication,
      buffersize=buffersize,
    )
    res_2 = rq.put(res_1.headers['location'], data=data)
    if not res_2:
      _on_error(res_2)

  def append(self, hdfs_path, data, buffersize=None):
    """Append to an existing file on HDFS.

    :param hdfs_path: Path to file on HDFS. This file must exist.
    :param data: Contents to be appended. See :meth:`write` for more details.
    :param buffersize: Size of upload buffer.

    """
    self._logger.info('Appending to %s.', hdfs_path)
    res_1 = self._append_1(
      hdfs_path,
      buffersize=buffersize,
    )
    res_2 = rq.post(res_1.headers['location'], data=data)
    if not res_2:
      _on_error(res_2)

  def upload(self, hdfs_path, local_path, overwrite=False, **kwargs):
    """Upload a file or directory to HDFS.

    :param hdfs_path: Target HDFS path. Note that unlike the :meth:`download`
      method, this cannot point to an existing directory.
    :param hdfs_path: Local path to file.
    :param overwrite: Overwrite any existing file or directory.
    :param kwargs: Keyword arguments forwarded to :meth:`write`.

    """
    self._logger.info('Uploading %s to %s.', local_path, hdfs_path)
    if not osp.exists(local_path):
      raise HdfsError('No file found at %r.', local_path)
    elif osp.isdir(local_path):
      raise HdfsError('%r is a directory, cannot upload.', local_path)
    with open(local_path, 'rb') as reader:
      self.write(hdfs_path, reader, overwrite=overwrite, **kwargs)

  def read(self, hdfs_path, offset=0, length=None, buffer_size=None,
    chunk_size=1024, buffer_char=None):
    """Read file. Returns a generator.

    :param hdfs_path: HDFS path.
    :param offset: Starting byte position.
    :param length: Number of bytes to be processed. `None` will read the entire
      file.
    :param buffer_size: Size of the buffer in bytes used for transferring the
      data. Defaults the the value set in the HDFS configuration.
    :param chunk_size: Interval in bytes at which the generator will yield.
    :param buffer_char: Character by which to buffer the file instead of
      yielding every `chunk_size` bytes. Note that this can cause the entire
      file to be yielded at once if the character is not appropriate.

    If only reading part of a file, don't forget to close the connection by
    terminating the generator by using its `close` method.

    """
    self._logger.info('Reading file %s.', hdfs_path)
    res = self._open(
      hdfs_path,
      offset=offset,
      length=length,
      buffersize=buffer_size
    )
    def reader():
      """Generator that also terminates the connection when closed."""
      try:
        if not buffer_char:
          for chunk in res.iter_content(chunk_size):
            yield chunk
        else:
          buf = ''
          for chunk in res.iter_content(chunk_size):
            buf += chunk
            splits = buf.split(buffer_char)
            for part in splits[:-1]:
              yield part
            buf = splits[-1]
          yield buf
      except GeneratorExit:
        pass
      finally:
        res.close()
    return reader()

  def download(self, hdfs_path, local_path, overwrite=False, n_threads=-1,
    **kwargs):
    """Download a (potentially distributed) file from HDFS and save it locally.
    This method returns the local path corresponding to the downloaded file or
    directory.

    :param hdfs_path: Path on HDFS of the file to download.
    :param local_path: Local path.
    :param overwrite: Overwrite any existing file or directory.
    :param n_threads: Number of threads to use for parallel downloading of
      part-files. A value of `None` or `1` indicates that parallelization won't
      be used; `-1` uses as many threads as there are part-files.
    :param \*\*kwargs: Keyword arguments forwarded to :meth:`read`.

    """
    hdfs_path = hdfs_path.rstrip(posixpath.sep)
    if osp.isdir(local_path):
      dst = osp.join(local_path, posixpath.basename(hdfs_path))
    else:
      local_dir = osp.dirname(local_path) or '.'
      if osp.isdir(local_dir):
        dst = local_path
      else:
        # fail early
        raise HdfsError('Parent directory %s does not exist', local_dir)

    def _download(paths):
      """Download and atomic swap."""
      _hdfs_path, _local_path = paths
      if osp.isfile(_local_path) and not overwrite:
        raise HdfsError('A local file already exists at %s.', _local_path)
      else:
        with temppath() as tpath:
          os.mkdir(tpath)
          _temp_path = osp.join(tpath, posixpath.basename(_hdfs_path))
          # ensure temp filename is the same as the source name to ensure
          # consistency with the `mv` behavior below (hence creating the
          # seemingly superfluous temp directory)
          self._logger.debug('Downloading %s to %s.', _hdfs_path, _temp_path)
          with open(_temp_path, 'wb') as writer:
            for chunk in self.read(_hdfs_path, **kwargs):
              writer.write(chunk)
          self._logger.debug(
            'Download of %s to %s complete. Moving it to %s.',
            _hdfs_path, _temp_path, _local_path
          )
          move(_temp_path, _local_path) # consistent with `mv` behavior
        self._logger.info('Downloaded %s to %s.', _hdfs_path, _local_path)

    def _delayed_download(indexed_paths):
      """Download and atomic swap, with delay to avoid replay errors."""
      file_n, paths = indexed_paths
      # Sleep some milliseconds so that authentication time stamps are not same
      time.sleep(0.01 * file_n)
      _download(paths)

    status = self.status(hdfs_path)
    if status['type'] == 'FILE':
      self._logger.debug(
        '%s is a non-partitioned file.', hdfs_path
      )
      if osp.isdir(local_path):
        local_path = osp.join(local_path, posixpath.basename(hdfs_path))
      _download((hdfs_path, local_path))
    else:
      self._logger.debug(
        '%s is a directory.', hdfs_path
      )
      parts = sorted(self.parts(hdfs_path))
      if osp.exists(local_path) and not osp.isdir(local_path):
        # remote path is a distributed file but we are writing to a single file
        raise HdfsError('Local path %r is not a directory.', local_path)
        # fail now instead of after the download
      else:
        # remote path is a distributed file and we are writing to a directory
        with temppath() as tpath:
          _temp_dir_path = osp.join(tpath, posixpath.basename(hdfs_path))
          os.makedirs(_temp_dir_path)
          # similarly to above, we add an extra directory to ensure that the
          # final name is consistent with the source (guaranteeing expected
          # behavior of the `move` function below)
          part_paths = [(_hdfs_path, _temp_dir_path) for _hdfs_path in parts]
          if n_threads == -1:
            n_threads = len(part_paths)
          else:
            n_threads = min(len(part_paths), n_threads or 0)
            # min(None, ...) returns None
          if n_threads > 1:
            self._logger.debug(
              'Starting parallel download using %s threads.', n_threads
            )
            p = ThreadPool(n_threads)
            p.map(_delayed_download, enumerate(part_paths))
          else:
            self._logger.debug('Starting synchronous download.')
            map(_download, part_paths)
            # maps, comprehensions, nothin' on you
          move(_temp_dir_path, local_path) # consistent with `mv` behavior
          self._logger.debug(
            'Moved %s to %s.', _temp_dir_path, local_path
          )
    return dst

  def delete(self, hdfs_path, recursive=False):
    """Remove a file or directory from HDFS.

    :param hdfs_path: HDFS path.
    :param recursive: Recursively delete files and directories. By default,
      this method will raise :class:`~hdfs.util.HdfsError` if trying to delete
      a non-empty directory.

    """
    self._logger.info('Deleting %s%s.', hdfs_path, ' [R]' if recursive else '')
    res = self._delete(hdfs_path, recursive=recursive)
    if not res.json()['boolean']:
      raise HdfsError('Remote path %r not found.', hdfs_path)

  def rename(self, hdfs_src_path, hdfs_dst_path):
    """Move a file or folder.

    :param hdfs_src_path: Source path.
    :param hdfs_dst_path: Destination path. If the path already exists and is
      a directory, the source will be moved into it. If the path exists and is
      a file, this method will raise :class:`~hdfs.util.HdfsError`.

    """
    self._logger.info('Renaming %s to %s.', hdfs_src_path, hdfs_dst_path)
    hdfs_dst_path = self.resolve(hdfs_dst_path)
    res = self._rename(hdfs_src_path, destination=hdfs_dst_path)
    if not res.json()['boolean']:
      raise HdfsError('Remote path %r not found.', hdfs_src_path)

  def walk(self, hdfs_path, depth=0):
    """Depth-first walk of remote folder statuses.

    :param hdfs_path: Starting path.
    :param depth: Maximum depth to explore. Specify `-1` for no limit.

    This method returns a generator yielding tuples `(path, status)`
    where `path` is the absolute path to the current file or directory, and
    `status` is a JSON FileStatus_ object.

    """
    hdfs_path = self.resolve(hdfs_path)
    def _walk(dir_path, dir_status, depth):
      """Recursion helper."""
      yield dir_path, dir_status
      if depth != 0:
        statuses = self._list_status(dir_path).json()['FileStatuses']
        for status in statuses['FileStatus']:
          path = osp.join(dir_path, status['pathSuffix'])
          if status['type'] == 'FILE':
            yield path, status
          else: # directory
            for a in _walk(path, status, depth - 1):
              yield a
    status = self.status(hdfs_path)
    if status['type'] == 'FILE':
      yield hdfs_path, status
    else:
      for a in _walk(hdfs_path, status, depth):
        yield a

  # Class loaders

  @classmethod
  def _from_options(cls, class_name, options):
    """Load client from options.

    :param class_name: Client class name. Pass `None` for the base
      :class:`Client` class.
    :param options: Options dictionary.

    """
    class_name = class_name or 'Client'
    try:
      return cls.__registry__[class_name](**options)
    except KeyError:
      raise HdfsError('Unknown client class: %r', class_name)
    except TypeError:
      raise HdfsError('Invalid options: %r', options)

  @classmethod
  def from_alias(cls, alias=None, path=None):
    """Load client associated with configuration alias.

    :param alias: Alias name.
    :param path: Path to configuration file. Defaults to `.hdfsrc` in the
      current user's home directory.

    """
    path = path or osp.expanduser('~/.hdfsrc')
    options = Config(path).get_alias(alias)
    return cls._from_options(options.pop('client', None), options)


# Custom client classes
# ---------------------

class InsecureClient(Client):

  """HDFS web client to use when security is off.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param user: User default. Defaults to the current user's (as determined by
    `whoami`).
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters. Default to
    `user`'s home directory on HDFS.

  """

  def __init__(self, url, user=None, proxy=None, root=None):
    user = user or getuser()
    super(InsecureClient, self).__init__(
      url,
      params={'user.name': user},
      proxy=proxy,
      root=root or '/user/%s/' % (user, ),
    )


class TokenClient(Client):

  """HDFS web client using Hadoop token delegation security.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param token: Hadoop delegation token.
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters.

  """

  def __init__(self, url, token, proxy=None, root=None):
    super(TokenClient, self).__init__(
      url=url,
      params={'delegation': token},
      proxy=proxy,
      root=root,
    )
