#!/usr/bin/env python
# encoding: utf-8

"""Kerberos extensions.

Adds :class:`KerberosClient` for HDFS clusters using Kerberos authentication.

"""

from ..client import Client
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


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
