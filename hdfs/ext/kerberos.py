#!/usr/bin/env python
# encoding: utf-8

"""This extension provides support for clusters using Kerberos authentication.

Namely, it adds a new :class:`~hdfs.client.Client` subclass,
:class:`KerberosClient`, which handles authentication appropriately.

"""

from ..client import Client
from ..util import HdfsError
from requests_kerberos import HTTPKerberosAuth
import requests_kerberos # For mutual authentication globals.


class KerberosClient(Client):

  """HDFS web client using Kerberos authentication.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode
  :param mutual_auth: Whether to enforce mutual authentication or not (possible
    values: `'REQUIRED'`, `'OPTIONAL'`, `'DISABLED'`).
  :param \*\*kwargs: Keyword arguments passed to the base class' constructor.

  """

  def __init__(self, url, mutual_auth='OPTIONAL', **kwargs):
    try:
      _mutual_auth = getattr(requests_kerberos, mutual_auth)
    except AttributeError:
      raise HdfsError('Invalid mutual authentication type: %r', mutual_auth)
    kwargs['auth'] = HTTPKerberosAuth(_mutual_auth)
    super(KerberosClient, self).__init__(url, **kwargs)
