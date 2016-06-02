#!/usr/bin/env python
# encoding: utf-8

"""Support for clusters using Kerberos_ authentication.

This extension adds a new :class:`hdfs.client.Client` subclass,
:class:`KerberosClient`, which handles authentication appropriately with
Kerberized clusters:

.. code-block:: python

  from hdfs.ext.kerberos import KerberosClient
  client = KerberosClient('http://host:port')

To expose this class to the command line interface (so that it can be used by
aliases), we add the following line inside the `global` section of
`~/.hdfscli.cfg` (or wherever our configuration file is located):

.. code-block:: cfg

  autoload.modules = hdfs.ext.kerberos

Here is what our earlier configuration would look like if we updated it to
support a Kerberized production grid:

.. code-block:: cfg

  [global]
  default.alias = dev
  autoload.modules = hdfs.ext.kerberos

  [dev.alias]
  url = http://dev.namenode:port

  [prod.alias]
  url = http://prod.namenode:port
  client = KerberosClient

.. _Kerberos: http://web.mit.edu/kerberos/

"""

from ..client import Client
from ..util import HdfsError
from six import string_types
from threading import Lock, Semaphore
from time import sleep, time
import requests as rq
import requests_kerberos # For mutual authentication globals.


class _HdfsHTTPKerberosAuth(requests_kerberos.HTTPKerberosAuth):

  """Kerberos authenticator which throttles authentication requests.

  Without it, authentication will otherwise fail if too many concurrent
  requests are being made. To avoid replay errors, a timeout of 1 ms is also
  enforced between requests.

  """

  _delay = 0.001 # Seconds.

  def __init__(self, max_concurrency, mutual_auth):
    self._lock = Lock()
    self._sem = Semaphore(max_concurrency)
    self._timestamp = time() - self._delay
    super(_HdfsHTTPKerberosAuth, self).__init__(mutual_auth)

  def __call__(self, req):
    with self._sem:
      with self._lock:
        delay = self._timestamp + self._delay - time()
        if delay > 0:
          sleep(delay) # Avoid replay errors.
        self._timestamp = time()
      return super(_HdfsHTTPKerberosAuth, self).__call__(req)


class KerberosClient(Client):

  """HDFS web client using Kerberos authentication.

  :param url: Hostname or IP address of HDFS namenode, prefixed with protocol,
    followed by WebHDFS port on namenode.
  :param mutual_auth: Whether to enforce mutual authentication or not (possible
    values: `'REQUIRED'`, `'OPTIONAL'`, `'DISABLED'`).
  :param max_concurrency: Maximum number of allowed concurrent authentication
    requests. This is required since requests exceeding the threshold allowed
    by the server will be unable to authenticate.
  :param \*\*kwargs: Keyword arguments passed to the base class' constructor.

  To avoid replay errors, a timeout of 1 ms is enforced between requests.

  """

  def __init__(self, url, mutual_auth='OPTIONAL', max_concurrency=1, **kwargs):
    # Note the handling of options passed in as strings to support
    # instantiation via the configuration file.
    session = kwargs.setdefault('session', rq.Session())
    if isinstance(mutual_auth, string_types):
      try:
        _mutual_auth = getattr(requests_kerberos, mutual_auth)
      except AttributeError:
        raise HdfsError('Invalid mutual authentication type: %r', mutual_auth)
    else:
      _mutual_auth = mutual_auth
    session.auth = _HdfsHTTPKerberosAuth(int(max_concurrency), _mutual_auth)
    super(KerberosClient, self).__init__(url, **kwargs)
