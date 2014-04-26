#!/usr/bin/env python
# encoding: utf-8

"""HDFS clients."""

from .util import HdfsError
from getpass import getuser
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import os.path as osp
import re
import requests as rq


def _hdfs_abspath(path, root):
  """Return normalized absolute path.

  :param path: HDFS path.
  :param root: Root, used to allow relative paths.

  """
  if not (osp.isabs(path) or root and osp.isabs(root)):
    raise HdfsError('Path %r is relative but no absolute root found.', path)
  return osp.normpath(osp.join(root, path))

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

  Note that by default the `allow_redirects` keyword argument is set to `True`
  and passed to the request handler. This is for convenience as all but 2 of
  the API endpoints require it.

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
        _hdfs_abspath(path, client.root),
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
        return _on_error(response)
      else:
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
    self.url = url
    self.root = root
    self._auth = auth
    self._params = params or {}
    if proxy:
      self._params['doas'] = proxy

  @classmethod
  def load(cls, class_name, options):
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

  def content(self, hdfs_path):
    """Get content summary for a file or folder on HDFS.

    :param hdfs_path: Remote path.

    This method returns a JSON ContentSummary_ object if `hdfs_path` exists and
    raises :class:`HdfsError` otherwise.

    .. _ContentSummary: CS_
    .. _CS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#ContentSummary

    """
    return self._get_content_summary(hdfs_path).json()['ContentSummary']

  def status(self, hdfs_path):
    """Get status for a file or folder on HDFS.

    :param hdfs_path: Remote path.

    This method returns a JSON FileStatus_ object if `hdfs_path` exists and
    raises :class:`HdfsError` otherwise.

    .. _FileStatus: FS_
    .. _FS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatus

    """
    return self._get_file_status(hdfs_path).json()['FileStatus']

  def write(self, hdfs_path, data, overwrite=False, permission=None,
    blocksize=None, replication=None, callback=None):
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
    :param callback: Function to be called while the upload is in progress.
      This function is called if `data` is a generator or file object right
      before each yield.

    """
    res_1 = self._create_1(
      hdfs_path,
      overwrite=overwrite,
      permission=permission,
      blocksize=blocksize,
      replication=replication,
    )
    if callback and not isinstance(data, basestring):
      def _data(data=data):
        """Wrapped data generator. Note the local variable caching."""
        for e in data:
          callback()
          yield e
      data = _data()
    res_2 = rq.put(res_1.headers['location'], data=data)
    if not res_2:
      _on_error(res_2)

  def upload(self, hdfs_path, local_path, **kwargs):
    """Upload a file or directory to HDFS.

    :param hdfs_path: Target HDFS path.
    :param hdfs_path: Local path to file.
    :param kwargs: Keyword arguments forwarded to :meth:`write`.

    """
    if not osp.exists(local_path):
      raise HdfsError('No file found at %r.', local_path)
    if osp.isdir(local_path):
      raise HdfsError('%r is a directory, cannot upload.', local_path)
    with open(local_path) as reader:
      self.write(hdfs_path, reader, **kwargs)

  def read(self, hdfs_path, writer, offset=0, length=None, buffer_size=None,
    callback=None, chunk_size=1024):
    """Read file.

    :param hdfs_path: HDFS path.
    :param writer: File object.
    :param offset: Starting byte position.
    :param length: Number of bytes to be processed. `None` will read the entire
      file.
    :param buffer_size: Size of the buffer in bytes used for transferring the
      data. Defaults the the value set in the HDFS configuration.
    :param callback: Function to be called while the download is in progress.
      This function will be passed the current byte position as single
      argument.
    :param chunk_size: Interval in bytes at which the callback function will
      be called.

    """
    res = self._open(
      hdfs_path,
      offset=offset,
      length=length,
      buffersize=buffer_size
    )
    position = 0
    for chunk in res.iter_content(chunk_size):
      if callback:
        callback(position)
        position += len(chunk)
      writer.write(chunk)

  def download(self, hdfs_path, local_path, overwrite=False, **kwargs):
    """Download a file from HDFS.

    :param hdfs_path: Path on HDFS of the file to download.
    :param local_path: Local path. This must not be a directory.
    :param kwargs: Keyword arguments forwarded to :meth:`read`.

    """
    if osp.isdir(local_path):
      raise HdfsError('%r is a directory. Cannot download.', local_path)
    if not osp.exists(local_path) or overwrite:
      with open(local_path, 'w') as writer:
        self.read(hdfs_path, writer, **kwargs)
    else:
      raise HdfsError('%r already exists. Aborting download.', local_path)

  def delete(self, hdfs_path, recursive=False):
    """Remove a file or directory from HDFS.

    :param hdfs_path: HDFS path.
    :param recursive: Recursively delete files and directories. By default,
      this method will raise :class:`~hdfs.util.HdfsError` if trying to delete
      a non-empty directory.

    """
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
    hdfs_dst_path = _hdfs_abspath(hdfs_dst_path, self.root)
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

    .. _FileStatus: FS_
    .. _FS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatus

    """
    hdfs_path = _hdfs_abspath(hdfs_path, self.root)
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


class KerberosClient(Client):

  """HDFS web client using Kerberos authentication.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters.

  """

  def __init__(self, url, proxy=None, root=None):
    super(KerberosClient, self).__init__(
      url,
      auth=HTTPKerberosAuth(OPTIONAL),
      proxy=proxy,
      root=root,
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
