#!/usr/bin/env python
# encoding: utf-8

"""HDFS clients."""

from .util import Config, HdfsError, InstanceLogger, temppath
from getpass import getuser
from itertools import repeat
from multiprocessing.pool import ThreadPool
from random import sample
from shutil import move, rmtree
from urllib import quote
from warnings import warn
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

  def __init__(self, method, **kwargs):
    self.method = method
    self.kwargs = kwargs

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
      return client._request(
        method=self.method,
        url=url,
        auth=client.auth, # TODO: See why this can't be moved to `_request`.
        data=data,
        params=params,
        **self.kwargs
      )

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
    example for custom authentication. Parameters specified in the request
    handler will override these defaults.
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters.
  :param timeout: Forwarded to the request handler. How long to wait for the
    server to send data before giving up, as a float, or a `(connect_timeout,
    read_timeout)` tuple. If the timeout is reached, an appropriate exception
    will be raised. See the requests_ documentation for details.
  :param verify: Forwarded to the request handler. If `True`, the SSL cert will
    be verified. See the requests_ documentation for details.
  :param cert: Forwarded to the request handler. Path to client certificate
    file. See the requests_ documentation for details.

  In general, this client should only be used directly when its subclasses
  (e.g. :class:`InsecureClient`, :class:`TokenClient`, and others provided by
  extensions) do not provide enough flexibility.

  .. _requests: http://docs.python-requests.org/en/latest/api/#requests.request

  """

  __metaclass__ = _ClientType
  __registry__ = {}

  def __init__(
    self, url, auth=None, params=None, proxy=None, root=None, timeout=None,
    verify=True, cert=None,
  ):
    self._logger = InstanceLogger(self, _logger)
    self._class_name = self.__class__.__name__ # cache this
    self.url = url.rstrip('/')
    self.auth = auth
    self.params = params or {}
    if proxy:
      self.params['doas'] = proxy
    self.root = root
    if self.root and not posixpath.isabs(self.root):
      raise HdfsError('Non-absolute root: %r', self.root)
    self.timeout = int(timeout) if timeout else None
    self.verify = Config.parse_boolean(verify)
    self.cert = cert

  def __repr__(self):
    return '<%s(url=%s, root=%s)>' % (self._class_name, self.url, self.root)

  # Generic request handler

  def _request(self, method, url, **kwargs):
    """Send request to WebHDFS API.

    :param method: HTTP verb.
    :param url: Url to send the request to.
    :param \*\*kwargs: Extra keyword arguments forwarded to the request
      handler. If any `params` are defined, these will take precendence over
      the instance's defaults.

    """
    params = kwargs.setdefault('params', {})
    for key, value in self.params.items():
      params.setdefault(key, value)
    response = rq.request(
      method=method,
      url=url,
      timeout=self.timeout,
      verify=self.verify,
      cert=self.cert,
      headers={'content-type': 'application/octet-stream'}, # For HttpFS.
      **kwargs
    )
    if not response: # non 2XX status code
      self._logger.warning(
        '[%s] %s:\n%s', response.status_code,
        response.request.path_url, response.text
      )
      return _on_error(response)
    else:
      self._logger.debug(
        '[%s] %s', response.status_code, response.request.path_url
      )
      return response

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
    if not posixpath.isabs(path):
      if not self.root:
        raise HdfsError('Path %r is relative but no root found.', path)
      path = posixpath.join(self.root, path)
    path = posixpath.normpath(path)

    def expand_latest(match):
      """Substitute #LATEST marker."""
      prefix = match.string[:match.start()]
      suffix = ''
      n = match.group(1) # n as in {N} syntax
      for _ in repeat(None, int(n) if n else 1):
        statuses = self._list_status(posixpath.join(prefix, suffix)).json()
        candidates = sorted([
          (-status['modificationTime'], status['pathSuffix'])
          for status in statuses['FileStatuses']['FileStatus']
        ])
        if not candidates:
          raise HdfsError('Cannot expand #LATEST. %r is empty.', prefix)
        elif len(candidates) == 1 and candidates[0][1] == '':
          raise HdfsError('Cannot expand #LATEST. %r is a file.', prefix)
        suffix = posixpath.join(suffix, candidates[0][1])
      return '/' + suffix

    path = re.sub(r'/?#LATEST(?:{(\d+)})?(?=/|$)', expand_latest, path)
    # #LATEST expansion (could cache the pattern, but not worth it)

    self._logger.debug('Resolved path %r to %r.', hdfs_path, path)
    return quote(path, '/=')

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
    blocksize=None, replication=None, buffersize=None, append=False):
    """Create a file on HDFS.

    :param hdfs_path: Path where to create file. The necessary directories will
      be created appropriately.
    :param data: Contents of file to write. Can be a string, a generator or a
      file object. The last two options will allow streaming upload (i.e.
      without having to load the entire contents into memory).
    :param overwrite: Overwrite any existing file or directory.
    :param permission: Octal permissions to set on the newly created file.
      Leading zeros may be omitted.
    :param blocksize: Block size of the file.
    :param replication: Number of replications of the file.
    :param buffersize: Size of upload buffer.
    :param append: Append to a file rather than create a new one.

    """
    if append:
      if overwrite:
        raise ValueError('Cannot both overwrite and append.')
      if permission or blocksize or replication:
        raise ValueError('Cannot change file properties while appending.')
      self._logger.info('Appending to %s.', hdfs_path)
      res = self._append_1(hdfs_path, buffersize=buffersize)
    else:
      self._logger.info('Writing to %s.', hdfs_path)
      res = self._create_1(
        hdfs_path,
        overwrite=overwrite,
        permission=permission,
        blocksize=blocksize,
        replication=replication,
        buffersize=buffersize,
      )
    self._request(
      method='POST' if append else 'PUT',
      url=res.headers['location'],
      data=data,
    )

  def append(self, hdfs_path, data, buffersize=None):
    """Append to an existing file on HDFS.

    :param hdfs_path: Path to file on HDFS. This file must exist.
    :param data: Contents to be appended. See :meth:`write` for more details.
    :param buffersize: Size of upload buffer.

    """
    warn(DeprecationWarning(
      '`Client.append` is going away in 2.0. Please use `Client.write` with '
      '`append=True` instead.'
    ))
    self._logger.info('Appending to %s.', hdfs_path)
    res = self._append_1(
      hdfs_path,
      buffersize=buffersize,
    )
    self._request(
      method='POST',
      url=res.headers['location'],
      data=data,
    )

  def upload(self, hdfs_path, local_path, overwrite=False, n_threads=-1,
    temp_dir=None, **kwargs):
    """Upload a file or directory to HDFS.

    :param hdfs_path: Target HDFS path. If it already exists and is a
      directory, files will be uploaded inside.
    :param local_path: Local path to file or folder. If a folder, all the files
      inside of it will be uploaded (note that this implies that folders empty
      of files will not be created remotely).
    :param overwrite: Overwrite any existing file or directory.
    :param n_threads: Number of threads to use for parallel downloading of
      part-files. A value of `None` or `1` indicates that parallelization won't
      be used; `-1` uses as many threads as there are part-files.
    :param temp_dir: Directory under which the files will first be uploaded
      when `overwrite=True` and the final remote path already exists. Once the
      upload successfully completes, it will be swapped in.
    :param \*\*kwargs: Keyword arguments forwarded to :meth:`write`.

    On success, this method returns the remote upload path.

    """
    self._logger.info('Uploading %s to %s.', local_path, hdfs_path)

    def _upload(_indexed_path_tuple):
      """Upload a single file."""
      _index, (_local_path, _temp_path) = _indexed_path_tuple
      time.sleep(_index) # Avoid replay errors.
      self._logger.debug('Uploading %r to %r.', _local_path, _temp_path)
      with open(_local_path, 'rb') as _reader:
        self.write(_temp_path, _reader, overwrite=False, **kwargs)

    # First, we gather information about remote paths.
    hdfs_path = self.resolve(hdfs_path)
    temp_path = None
    try:
      statuses = [status for _, status in self.list(hdfs_path)]
    except HdfsError as err:
      if 'not a directory' in str(err):
        # Remote path is a normal file.
        if not overwrite:
          raise HdfsError('Remote path %r already exists.', hdfs_path)
      else:
        # Remote path doesn't exist.
        temp_path = hdfs_path
    else:
      # Remote path is a directory.
      suffixes = set(status['pathSuffix'] for status in statuses)
      local_name = osp.basename(local_path)
      hdfs_path = posixpath.join(hdfs_path, local_name)
      if local_name in suffixes:
        if not overwrite:
          raise HdfsError('Remote path %r already exists.', hdfs_path)
      else:
        temp_path = hdfs_path
    if not temp_path:
      # The remote path already exists, we need to generate a temporary one.
      remote_dpath, remote_name = posixpath.split(hdfs_path)
      temp_dir =  temp_dir or remote_dpath
      temp_path = posixpath.join(
        temp_dir,
        '%s.temp-%s' % (remote_name, int(time.time()))
      )
      self._logger.debug(
        'Upload destination %s already exists. Using temporary path %s.',
        hdfs_path, temp_path
      )
    # Then we figure out which files we need to upload, and where.
    if osp.isdir(local_path):
      local_fpaths = [
        osp.join(dpath, fpath)
        for dpath, _, fpaths in os.walk(local_path)
        for fpath in fpaths
      ]
      if not local_fpaths:
        raise HdfsError('No files to upload found inside %r.', local_path)
      offset = len(local_path.rstrip(os.sep)) + len(os.sep)
      fpath_tuples = [
        (fpath, posixpath.join(temp_path, fpath[offset:].replace(os.sep, '/')))
        for fpath in local_fpaths
      ]
    elif osp.exists(local_path):
      fpath_tuples = [(local_path, temp_path)]
    else:
      raise HdfsError('Local path %r does not exist.', local_path)
    # Finally, we upload all files (optionally, in parallel).
    if n_threads == -1:
      n_threads = len(fpath_tuples)
    elif not n_threads:
      n_threads = 1
    else:
      n_threads = min(n_threads, len(fpath_tuples))
    self._logger.debug(
      'Uploading %s files using %s thread(s).', len(fpath_tuples), n_threads
    )
    try:
      if n_threads == 1:
        map(_upload, enumerate(fpath_tuples))
      else:
        ThreadPool(n_threads).map(_upload, enumerate(fpath_tuples))
    except Exception as err:
      try:
        self.delete(temp_path, recursive=True)
      except Exception:
        self._logger.exception('Unable to cleanup temporary folder.')
      finally:
        raise err
    else:
      if temp_path != hdfs_path:
        self._logger.debug(
          'Upload of %s complete. Moving from %s to %s.',
          local_path, temp_path, hdfs_path
        )
        self.delete(hdfs_path, recursive=True)
        self.rename(temp_path, hdfs_path)
      else:
        self._logger.debug(
          'Upload of %s to %s complete.', local_path, hdfs_path
        )
    return hdfs_path

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
    temp_dir=None, **kwargs):
    """Download a file or folder from HDFS and save it locally.

    :param hdfs_path: Path on HDFS of the file or folder to download. If a
      folder, all the files under it will be downloaded.
    :param local_path: Local path. If it already exists and is a directory,
      the files will be downloaded inside of it.
    :param overwrite: Overwrite any existing file or directory.
    :param n_threads: Number of threads to use for parallel downloading of
      part-files. A value of `None` or `1` indicates that parallelization won't
      be used; `-1` uses as many threads as there are part-files.
    :param temp_dir: Directory under which the files will first be downloaded
      when `overwrite=True` and the final destination path already exists. Once
      the download successfully completes, it will be swapped in.
    :param \*\*kwargs: Keyword arguments forwarded to :meth:`read`.

    On success, this method returns the local download path.

    """
    self._logger.info('Downloading %r to %r.', hdfs_path, local_path)

    def _download(_indexed_path_tuple):
      """Download a single file."""
      _index, (_remote_path, _temp_path) = _indexed_path_tuple
      time.sleep(_index)
      _dpath = osp.dirname(_temp_path)
      self._logger.debug('Downloading %r to %r.', _remote_path, _temp_path)
      if not osp.exists(_dpath):
        os.makedirs(_dpath)
      with open(_temp_path, 'wb') as _writer:
        for chunk in self.read(_remote_path, **kwargs):
          _writer.write(chunk)

    # First, we figure out where we will download the files to.
    hdfs_path = self.resolve(hdfs_path)
    local_path = osp.realpath(local_path)
    if osp.isdir(local_path):
      local_path = osp.join(local_path, posixpath.basename(hdfs_path))
    if osp.exists(local_path):
      if not overwrite:
        raise HdfsError('Path %r already exists.', local_path)
      local_dpath, local_name = osp.split(local_path)
      temp_dir = temp_dir or local_dpath
      temp_path = osp.join(
        temp_dir,
        '%s.temp-%s' % (local_name, int(time.time()))
      )
      self._logger.debug(
        'Download destination %s already exists. Using temporary path %s.',
        local_path, temp_path
      )
    else:
      if not osp.isdir(osp.dirname(local_path)):
        raise HdfsError('Parent directory of %r does not exist.', local_path)
      temp_path = local_path
    # Then we figure out which files we need to download and where.
    remote_fpaths = [
      fpath
      for (fpath, status) in self.walk(hdfs_path, depth=-1)
      if status['type'] == 'FILE'
    ]
    if not remote_fpaths:
      raise HdfsError('No files to download found inside %r.', hdfs_path)
    offset = len(hdfs_path) + 1 # Prefix length.
    fpath_tuples = [
      (
        fpath,
        osp.join(temp_path, fpath[offset:].replace('/', os.sep)).rstrip(os.sep)
      )
      for fpath in remote_fpaths
    ]
    # Finally, we download all of them.
    if n_threads == -1:
      n_threads = len(fpath_tuples)
    elif not n_threads:
      n_threads = 1
    else:
      n_threads = min(n_threads, len(fpath_tuples))
    self._logger.debug(
      'Downloading %s files using %s thread(s).', len(fpath_tuples), n_threads
    )
    try:
      if n_threads == 1:
        map(_download, enumerate(fpath_tuples))
      else:
        ThreadPool(n_threads).map(_download, enumerate(fpath_tuples))
    except Exception as err:
      try:
        if osp.isdir(temp_path):
          rmtree(temp_path)
        else:
          os.remove(temp_path)
      except Exception:
        self._logger.exception('Unable to cleanup temporary folder.')
      finally:
        raise err
    else:
      if temp_path != local_path:
        self._logger.debug(
          'Download of %s complete. Moving from %s to %s.',
          hdfs_path, temp_path, local_path
        )
        if osp.isdir(local_path):
          rmtree(local_path)
        else:
          os.remove(local_path)
        move(temp_path, local_path)
      else:
        self._logger.debug(
          'Download of %s to %s complete.', hdfs_path, local_path
        )
    return local_path

  def delete(self, hdfs_path, recursive=False):
    """Remove a file or directory from HDFS.

    :param hdfs_path: HDFS path.
    :param recursive: Recursively delete files and directories. By default,
      this method will raise an :class:`HdfsError` if trying to delete a
      non-empty directory.

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
      a file, this method will raise an :class:`HdfsError`.

    """
    self._logger.info('Renaming %s to %s.', hdfs_src_path, hdfs_dst_path)
    hdfs_dst_path = self.resolve(hdfs_dst_path)
    res = self._rename(hdfs_src_path, destination=hdfs_dst_path)
    if not res.json()['boolean']:
      raise HdfsError(
        'Unable to rename %r to %r.',
        self.resolve(hdfs_src_path), hdfs_dst_path
      )

  def set_owner(self, hdfs_path, owner=None, group=None):
    """Change the owner of file.

    :param hdfs_path: HDFS path.
    :param owner: Optional, new owner for file.
    :param group: Optional, new group for file.

    At least one of `owner` and `group` must be specified.

    """
    if not owner and not group:
      raise ValueError('Must set at least one of owner or group.')
    messages = []
    if owner:
      messages.append('owner to %s' % (owner, ))
    if group:
      messages.append('group to %s' % (group, ))
    self._logger.info('Changing %s of %s.', ', and'.join(messages), hdfs_path)
    self._set_owner(hdfs_path, owner=owner, group=group)

  def set_permissions(self, hdfs_path, permissions):
    """Change the permissions of file.

    :param hdfs_path: HDFS path.
    :param permissions: New octal permissions string of file.

    """
    self._logger.info(
      'Changing permissions of %s to %s.', hdfs_path, permissions
    )
    self._set_permission(hdfs_path, permission=permissions)

  def list(self, hdfs_path):
    """Return status of files contained in a remote folder.

    :param hdfs_path: Remote path to a directory. If `hdfs_path` doesn't exist
      or points to a normal file, an :class:`HdfsError` will be raised.

    This method returns a list of tuples `(path, status)` where `path` is the
    absolute path to a file or directory, and `status` is its corresponding
    JSON FileStatus_ object.

    """
    self._logger.info('Listing %s.', hdfs_path)
    hdfs_path = self.resolve(hdfs_path)
    statuses = self._list_status(hdfs_path).json()['FileStatuses']['FileStatus']
    if len(statuses) == 1 and (
      not statuses[0]['pathSuffix'] or self.status(hdfs_path)['type'] == 'FILE'
      # HttpFS behaves incorrectly here, we sometimes need an extra call to
      # make sure we always identify if we are dealing with a file.
    ):
      raise HdfsError('%r is not a directory.', hdfs_path)
    return [
      (osp.join(hdfs_path, status['pathSuffix']), status)
      for status in statuses
    ]

  def walk(self, hdfs_path, depth=0):
    """Depth-first walk of remote folder statuses.

    :param hdfs_path: Starting path.
    :param depth: Maximum depth to explore. Specify `-1` for no limit.

    This method returns a generator yielding tuples `(path, status)`
    where `path` is the absolute path to the current file or directory, and
    `status` is a JSON FileStatus_ object.

    """
    self._logger.info('Walking %s.', hdfs_path)

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

    hdfs_path = self.resolve(hdfs_path)
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
  :param \*\*kwargs: Keyword arguments passed to the base class' constructor.

  """

  def __init__(self, url, user=None, **kwargs):
    user = user or getuser()
    kwargs.setdefault('params', {})['user.name'] = user
    super(InsecureClient, self).__init__(url, **kwargs)


class TokenClient(Client):

  """HDFS web client using Hadoop token delegation security.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param token: Hadoop delegation token.
  :param \*\*kwargs: Keyword arguments passed to the base class' constructor.

  """

  def __init__(self, url, token, **kwargs):
    kwargs.setdefault('params', {})['delegation'] = token
    super(TokenClient, self).__init__(url, **kwargs)
