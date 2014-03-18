#!/usr/bin/env python
# encoding: utf-8

"""HDFS clients.

Currently the base client and a Kerberos authenticated client are available.

"""

from .util import HdfsError
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests as rq


class _Request(object):

  """Class to define requests.

  :param verb: HTTP verb

  """

  api_root = '/webhdfs/v1'
  doc_root = 'http://hadoop.apache.org/docs/r1.0.4/webhdfs.html'

  def __init__(self, verb):
    try:
      self.handler = getattr(rq, verb.lower())
    except AttributeError as err:
      raise HdfsError('Invalid HTTP verb %r.', verb)

  def to_method(self, operation):
    """Returns method associated with request to attach to client.

    :param operation: operation name.

    """
    def api_handler(client, path, **params):
      """Wrapper function."""
      if not path.startswith('/'):
        raise HdfsError('Invalid relative path %r.', path)
      url = '%s%s%s' % (client.url, self.api_root, path)
      params['op'] = operation
      return self.handler(
        url=url,
        auth=client.auth,
        params=params,
        allow_redirects=True,
      )
    api_handler.__name__ = '%s_handler' % (operation.lower(), )
    api_handler.__doc__ = 'Cf. %s#%s' % (self.doc_root, operation)
    return api_handler


class _ClientType(type):

  """Metaclass that enables short and dry request definitions.

  Note that the names of the methods are changed from underscore case to upper
  case to determine the end operation.

  """

  def __new__(cls, name, bases, attrs):
    for key, value in attrs.items():
      if isinstance(value, _Request):
        attrs[key] = value.to_method(key.replace('_', '').upper())
    return super(_ClientType, cls).__new__(cls, name, bases, attrs)


class Client(object):

  """Base HDFS web client.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param auth: Authentication mechanism (forwarded to the request handler).

  """

  __metaclass__ = _ClientType

  def __init__(self, url, auth=None):
    self.url = url
    self.auth = auth

  # general case

  delete = _Request('DELETE')
  get_content_summary = _Request('GET')
  get_file_checksum = _Request('GET')
  get_file_status = _Request('GET')
  get_home_directory = _Request('GET')
  list_status = _Request('GET')
  mkdirs = _Request('PUT')
  open = _Request('GET')
  rename = _Request('PUT')
  set_owner = _Request('PUT')
  set_permission = _Request('PUT')
  set_replication = _Request('PUT')
  set_times = _Request('PUT')

  # special cases (requiring 2 step process)

  def create(self, path, **params):
    """Cf. http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#CREATE"""
    pass

  def append(self, path, **params):
    """Cf. http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#APPEND"""
    pass


class KerberosClient(Client):

  """HDFS web client using Kerberos authentication.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode

  """

  def __init__(self, url):
    super(KerberosClient, self).__init__(url, auth=HTTPKerberosAuth(OPTIONAL))
