#!/usr/bin/env python
# encoding: utf-8

"""HDFS clients."""

from .util import Config, HdfsError
from getpass import getuser
from os.path import exists, isdir, join
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import re
import requests as rq


class _Request(object):

  """Class to define API requests.

  :param verb: HTTP verb (`'GET'`, `'PUT'`, etc.).
  :param kwargs: Keyword arguments passed to the request handler.

  Note that by default the `allow_redirects` keyword argument is set to `True`
  and passed to the request handler. This is for convenience as all but 2 of
  the API endpoints require it.

  """

  api_prefix = '/webhdfs/v1'
  doc_url = 'http://hadoop.apache.org/docs/r1.0.4/webhdfs.html'

  def __init__(self, verb, **kwargs):
    kwargs.setdefault('allow_redirects', True) # convenience
    self.kwargs = kwargs
    try:
      self.handler = getattr(rq, verb.lower())
    except AttributeError:
      raise HdfsError('Invalid HTTP verb %r.', verb)

  def __call__(self):
    # make pylint happy
    pass

  def to_method(self, operation):
    """Returns method associated with request to attach to client.

    :param operation: operation name.

    This is called inside the metaclass to switch :class:`_Request` objects
    with the method they represent.

    """
    def api_handler(client, path, data=None, **params):
      """Wrapper function."""
      url = '%s%s%s' % (client.url, self.api_prefix, client._abs(path))
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
        return client._on_error(response)
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
    return super(_ClientType, mcs).__new__(mcs, name, bases, attrs)


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

  _root = None

  def __init__(self, url, auth=None, params=None, proxy=None, root=None):
    self.url = url
    self._auth = auth
    self._params = params or {}
    if proxy:
      self._params['doas'] = proxy
    if root and root != '/':
      self._root = root.rstrip('/')

  def _abs(self, path):
    """Return absolute path.

    :param path: HDFS path.

    """
    path = re.compile(r'^(\./|\.$)').sub('', path)
    if not path.startswith('/'):
      if not self._root:
        raise HdfsError('Invalid relative path %r.', path)
      else:
        path = '%s/%s' % (self._root, path)
    return path

  def _rel(self, path):
    """Return relative path.

    :param path: HDFS path.

    If the client's `root` is found in the path, it will be replaced by a
    `'.'`.

    """
    path = self._abs(path)
    return re.compile('^%s' % (self._root, )).sub('.', path)

  def _on_error(self, response):
    """Callback when an API response has a non 2XX status code.

    :param response: Response.

    """
    try:
      # Cf. http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#Error+Responses
      message = response.json()['RemoteException']['message']
    except ValueError:
      # No clear one thing to display?
      message = response.content
    raise HdfsError(message)

  # Raw API endpoints

  _append = _Request('PUT') # doesn't allow for streaming
  _append_1 = _Request('POST', allow_redirects=False)
  _create = _Request('PUT') # doesn't allow for streaming
  _create_1 = _Request('PUT', allow_redirects=False)
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

  def write(
    self, hdfs_path, data, overwrite=False, permission=None, blocksize=None,
    replication=None,
  ):
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

    """
    res_1 = self._create_1(
      hdfs_path,
      overwrite=overwrite,
      permission=permission,
      blocksize=blocksize,
      replication=replication,
    )
    res_2 = rq.put(res_1.headers['location'], data=data)
    if not res_2:
      self._on_error(res_2)

  def upload(self, hdfs_path, local_path, **kwargs):
    """Upload a file or directory to HDFS.

    :param hdfs_path: Target HDFS path.
    :param hdfs_path: Local path to file.
    :param kwargs: Keyword arguments forwarded to :meth:`write`.

    """
    if not exists(local_path):
      raise HdfsError('No file found at %r.', local_path)
    if isdir(local_path):
      raise HdfsError('Cannot upload directory %r.', local_path)
    with open(local_path) as reader:
      self.write(hdfs_path, reader, **kwargs)

  def read(self, hdfs_path, writer, offset=0, length=None, buffer_size=1024):
    """Read file.

    :param hdfs_path: HDFS path.
    :param writer: File object.
    :param offset: Starting byte position.
    :param length: Number of bytes to be processed. `None` will read the entire
      file.
    :param buffer_size: Batch size in bytes.

    """
    res = self._open(hdfs_path, offset=offset, length=length)
    for chunk in res.iter_content(buffer_size):
      writer.write(chunk)

  def download(self, hdfs_path, local_path, overwrite=False, **kwargs):
    """Download a file from HDFS.

    :param hdfs_path: Path on HDFS of the file to download.
    :param local_path: Local path.
    :param kwargs: Keyword arguments forwarded to :meth:`read`.

    """
    if isdir(local_path):
      local_path = join(local_path, hdfs_path.rsplit('/', 1)[-1])
    if not exists(local_path) or overwrite:
      with open(local_path, 'w') as writer:
        self.read(hdfs_path, writer, **kwargs)
    else:
      raise HdfsError('Path %r already exists.', local_path)

  def delete(self, hdfs_path, recursive=False):
    """Remove a file or directory from HDFS.

    :param hdfs_path: HDFS path.
    :param recursive: Recursively delete files and directories. By default,
      this method will raise :class:`~hdfs.util.HdfsError` if trying to delete
      a non-empty directory.

    """
    res = self._delete(hdfs_path, recursive=recursive)
    if not res.json()['boolean']:
      raise HdfsError('Path %r not found.', hdfs_path)

  def rename(self, hdfs_src_path, hdfs_dst_path):
    """Move a file or folder.

    :param hdfs_src_path: Source path.
    :param hdfs_dst_path: Destination path. If the path already exists and is
      a directory, the source will be moved into it. If the path exists and is
      a file, this method will raise :class:`~hdfs.util.HdfsError`.

    """
    if not hdfs_dst_path.startswith('/'):
      hdfs_dst_path = '%s/%s' % (self._root, hdfs_dst_path)
    res = self._rename(hdfs_src_path, destination=hdfs_dst_path)
    if not res.json()['boolean']:
      raise HdfsError('Path %r not found.', hdfs_src_path)

  def walk(self, hdfs_path, depth=0, usage=False):
    """Depth-first walk of remote folder hierarchy.

    :param hdfs_path: Starting path.
    :param depth: Maximum depth to explore directories.
    :param usage: Include summary content usage for directories.

    This method returns a generator containing FileStatus_ (and optionally,
    ContentSummary_) JSON objects.

    .. _FileStatus: FS_
    .. _ContentSummary: CS_
    .. _FS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatus
    .. _CS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#ContentSummary

    """
    status = self._get_file_status(hdfs_path).json()['FileStatus']
    def _walk(dir_path, status, depth):
      """Recursion helper."""
      if usage:
        summary = self._get_content_summary(dir_path).json()['ContentSummary']
      else:
        summary = None
      yield dir_path, status, summary
      if depth > 0:
        statuses = self._list_status(dir_path).json()['FileStatuses']
        for status in statuses['FileStatus']:
          path = '/'.join([dir_path, status['pathSuffix']])
          if status['type'] == 'FILE':
            yield path, status, None
          else: # directory
            if depth > 0:
              for a in _walk(path, status, depth - 1):
                yield a
    if status['type'] == 'FILE':
      yield hdfs_path, status, None
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
  :param root: Root path. Used to allow relative path parameters. Defaults to
    the current user's (as determined by `whoami`) home directory on HDFS.

  """

  def __init__(self, url, proxy=None, root=None):
    super(KerberosClient, self).__init__(
      url,
      auth=HTTPKerberosAuth(OPTIONAL),
      proxy=proxy,
      root=root or '/user/%s/' % (getuser(), ),
    )

  def _on_error(self, response):
    """Callback when an API response has a non-200 status code.

    :param response: response.

    """
    if response.status_code == 401:
      raise HdfsError(
        'Authentication failure. Check your kerberos credentials.'
      )
    return super(KerberosClient, self)._on_error(response)


class TokenClient(Client):

  """HDFS web client using Hadoop token delegation security.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param token: Hadoop delegation token.
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters. Defaults to
    the current user's (as determined by `whoami`) home directory on HDFS.

  """

  def __init__(self, url, token, proxy=None, root=None):
    super(TokenClient, self).__init__(
      url=url,
      params={'delegation': token},
      proxy=proxy,
      root=root or '/user/%s/' % (getuser(), ),
    )


def get_client_from_alias(alias):
  """Load client corresponding to an alias.

  :param alias: Alias name.

  """
  options = Config().get_alias(alias)
  auth = options.pop('auth')
  try:
    if auth == 'insecure':
      return InsecureClient(**options)
    elif auth == 'kerberos':
      return KerberosClient(**options)
    elif auth == 'token':
      return TokenClient(**options)
    else:
      raise HdfsError('Invalid auth parameter %r for alias %r.', auth, alias)
  except ValueError:
    raise HdfsError('Invalid alias %r.', alias)
