#!/usr/bin/env python
# encoding: utf-8

"""HDFS client."""

from .util import HdfsError, request


class BaseClient(object):

  """HDFS web client.

  :param host: hostname or IP address of HDFS namenode
  :param port: WebHDFS port on namenode
  :param auth: authentication mechanism
  :param user: user name, used to enable relative paths

  """

  def __init__(self, host, port, auth=None, user=None):
    self.host = host
    self.port = port
    self.auth = auth
    self.user = user

  def _url(self, path):
    """Endpoint URL for a path.

    :param path: HDFS path. If the client was instantiated with a `user`
      parameter, relative paths will be expanded to start from this user's
      directory.

    """
    base_url = 'http://%s:%s/webhdfs/v1' % (self.host, self.port)
    if not path.startswith('/'):
      if not self.user:
        raise HdfsError(
          'Relative paths only allowed when a `user` parameter is specified.'
        )
      path = '/user/%s/%s' % (self.user, path)
    return '%s%s' % (base_url, path)

  @request('GET')
  def list_status(self, path):
    """List statuses of all files in directory.

    :param path: HDFS path. If the client was instantiated with a `user`
      parameter, relative paths will be expanded to start from this user's
      directory.

    """
    return self._url(path), {}

  @request('GET')
  def get_file_status(self, path):
    """List statuses of a single file.

    :param path: HDFS path. If the client was instantiated with a `user`
      parameter, relative paths will be expanded to start from this user's
      directory.

    """
    return self._url(path), {}

  @request('GET')
  def open(self, path, offset=None, length=None, buffersize=None):
    """Open file.

    :param path: HDFS path. If the client was instantiated with a `user`
      parameter, relative paths will be expanded to start from this user's
      directory.
    :param offset: The starting byte position.
    :param length: The number of bytes to be processed.
    :param buffersize: The size of the buffer used in transferring data.

    """
    params = {'offset': offset, 'length': length, 'buffersize': buffersize}
    return self._url(path), params

class Client(BaseClient):

  """Derived methods."""

  def ls(self, path):
    """List files and directories.

    :param path: remote path.

    """
    pass
