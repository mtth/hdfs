#!/usr/bin/env python
# encoding: utf-8

"""HDFS clients."""

from .util import HdfsError
from getpass import getuser
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import re
import requests as rq


API_PREFIX = '/webhdfs/v1'
DOC_URL = 'http://hadoop.apache.org/docs/r1.0.4/webhdfs.html'


class _Request(object):

  """Class to define requests.

  :param verb: HTTP verb
  :param kwargs: Keyword arguments passed to the request handler.

  """

  def __init__(self, verb, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    self.kwargs = kwargs
    try:
      self.handler = getattr(rq, verb.lower())
    except AttributeError:
      raise HdfsError('Invalid HTTP verb %r.', verb)

  def to_method(self, operation):
    """Returns method associated with request to attach to client.

    :param operation: operation name.

    """
    def api_handler(client, path, data=None, **params):
      """Wrapper function."""
      if not path.startswith('/'):
        if not client.root:
          raise HdfsError('Invalid relative path %r.', path)
        else:
          path = '%s/%s' % (client.root.rstrip('/'), path)
      url = '%s%s%s' % (client.url, API_PREFIX, path)
      params['op'] = operation
      for key, value in client.params.items():
        params.setdefault(key, value)
      response = self.handler(
        url=url,
        auth=client.auth,
        data=data,
        params=params,
        **self.kwargs
      )
      if not response: # non 2XX status code
        return client.on_error(response)
      else:
        return response
    api_handler.__name__ = '%s_handler' % (operation.lower(), )
    api_handler.__doc__ = 'Cf. %s#%s' % (DOC_URL, operation)
    return api_handler


class _ClientType(type):

  """Metaclass that enables short and dry request definitions.

  Note that the names of the methods are changed from underscore case to upper
  case to determine the end operation, removing any numbers in the process.

  """

  pattern = re.compile('_|\d')

  def __new__(mcs, name, bases, attrs):
    for key, value in attrs.items():
      if isinstance(value, _Request):
        attrs[key] = value.to_method(mcs.pattern.sub('', key).upper())
    return super(_ClientType, mcs).__new__(mcs, name, bases, attrs)


class Client(object):

  """HDFS web client.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param auth: Authentication mechanism (forwarded to the request handler).
  :param params: Extra parameters forwarded with every request. Useful for
    custom authentication. Parameters specified in the request handler will
    override these defaults.
  :param proxy: User to proxy as.
  :param root: Root path. Used to allow relative path parameters.

  In general, this client should not be instantiated directly but by using one
  of its subclasses. E.g. :class:`~hdfs.client.InsecureClient`,
  :class:`~hdfs.client.TokenClient` or :class:`~hdfs.client.KerberosClient`.

  """

  __metaclass__ = _ClientType

  def __init__(self, url, auth=None, params=None, proxy=None, root=None):
    self.url = url
    self.auth = auth
    self.params = params or {}
    if proxy:
      self.params['doas'] = proxy
    self.root = root

  @classmethod
  def from_config(cls, options):
    """Load client from configuration options.

    :param options: Dictionary of options.

    """
    try:
      return cls(**options)
    except ValueError as err:
      raise HdfsError('Invalid alias.')

  def on_error(self, response):
    """Callback when an API response has a non 2XX status code.

    :param response: Response.

    """
    # Cf. http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#Error+Responses
    try:
      message = response.json()['RemoteException']['message']
    except ValueError:
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

  def upload(self, hdfs_path, data, overwrite=False, permission=None):
    """Create a file on HDFS.

    :param hdfs_path: Path where to create file. The necessary directories will
      be created appropriately.
    :param data: Contents of file to write. Can either be a string or a file
      object (will allow for streaming uploads).
    :param overwrite: Overwrite any existing file.
    :param permissions: Octal permissions to set on newly created file.

    """
    res_1 = self._create_1(
      hdfs_path,
      overwrite=overwrite,
      permission=permission
    )
    res_2 = rq.put(res_1.headers['location'], data=data)
    if not res_2:
      self.on_error(res_2)

  def download(self, hdfs_path, local_path, max_connections=5):
    """Download a file from HDFS.

    :param hdfs_path: Path on HDFS of file to download.
    :param local_path: Local path.

    """
    pass

  def stream(self, hdfs_path):
    """Stream file from HDFS to standard out.

    :param hdfs_path: TODO

    """
    pass

  def delete(self, hdfs_path):
    """TODO: delete docstring.

    :param hdfs_path: TODO

    """
    pass

  def rename(self, hdfs_src_path, hdfs_dst_path, overwrite=False):
    """Rename a file or folder.

    :param hdfs_src_path: Source path.
    :param hdfs_dst_path: Destination path.
    :param overwrite: Overwrite an existing file or folder.

    Allows relative paths for both source and destination.

    """
    pass

  def list(self, hdfs_path):
    """List files in a directory.

    :param path: HDFS path.

    """
    return self._list_status(path).json()['FileStatuses']['FileStatus']

  def size(self, path):
    """Size of directory.

    :param path: HDFS path.

    """
    pass


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
      root = root or '/user/%s/' % (user, ),
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
      root = root or '/user/%s/' % (getuser(), ),
    )

  def on_error(self, response):
    """Callback when an API response has a non-200 status code.

    :param response: response.

    """
    if response.status_code == 401:
      raise HdfsError(
        'Authentication failure. Check your kerberos credentials.'
      )
    return super(KerberosClient, self).on_error(response)


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
      root=root,
    )
