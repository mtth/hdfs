#!/usr/bin/env python
# encoding: utf-8

"""Support for clusters using Kerberos authentication.

This extension adds a new :class:`hdfs.client.Client` subclass,
:class:`KerberosClient`, which handles authentication appropriately.

To activate, add the following inside `~/.hdfscli.cfg` (or wherever your
configuration file is located):

.. code-block:: cfg

  [global]
  autoload.modules = hdfs.ext.kerberos

We can also import the new client class directly (this will however not expose
it to the CLI):

.. code-block:: python

  from hdfs.ext.kerberos import KerberosClient

  client = KerberosClient('http://host:port')

"""

from ..client import Client
from ..util import HdfsError
from requests_kerberos import HTTPKerberosAuth
from six import string_types
from threading import Lock, Semaphore
from time import sleep, time
import requests as rq
import requests_kerberos # For mutual authentication globals.


class KerberosClient(Client):

  """HDFS web client using Kerberos authentication.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode.
  :param mutual_auth: Whether to enforce mutual authentication or not (possible
    values: `'REQUIRED'`, `'OPTIONAL'`, `'DISABLED'`).
  :param max_concurrency: Maximum number of allowed concurrent requests. This
    is required since requests exceeding the threshold allowed by the server
    will be unable to authenticate.
  :param \*\*kwargs: Keyword arguments passed to the base class' constructor.

  To avoid replay errors, a timeout of 1 ms is enforced between requests.

  """

  _delay = 0.001 # Seconds.

  def __init__(self, url, mutual_auth='OPTIONAL', max_concurrency=1, **kwargs):
    # Note the handling of options passed in as strings to support
    # instantiation via the configuration file.
    self._lock = Lock()
    self._sem = Semaphore(int(max_concurrency))
    self._timestamp = time() - self._delay
    session = kwargs.setdefault('session', rq.Session())
    if isinstance(mutual_auth, string_types):
      try:
        _mutual_auth = getattr(requests_kerberos, mutual_auth)
      except AttributeError:
        raise HdfsError('Invalid mutual authentication type: %r', mutual_auth)
    else:
      _mutual_auth = mutual_auth
    session.auth = HTTPKerberosAuth(_mutual_auth)
    super(KerberosClient, self).__init__(url, **kwargs)

  def _request(self, method, url, **kwargs):
    """Overriden method to avoid authentication errors.

    Authentication will otherwise fail if too many concurrent requests are
    being made.

    """
    if not 'auth' in kwargs:
      # Request doesn't need to be authenticated, bypass this.
      return super(KerberosClient, self)._request(method, url, **kwargs)
    with self._sem:
      with self._lock:
        delay = self._timestamp + self._delay - time()
        if delay > 0:
          sleep(delay) # Avoid replay errors.
        self._timestamp = time()
      return super(KerberosClient, self)._request(method, url, **kwargs)
