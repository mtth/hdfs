#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__all__ = [
  'get_client_from_alias',
  'Client', 'InsecureClient', 'KerberosClient', 'TokenClient',
]
__version__ = '0.2.0'

from os.path import expanduser
try:
  from .client import Client, InsecureClient, KerberosClient, TokenClient
  from .util import Config
except ImportError:
  pass # in setup.py


def get_client_from_alias(alias, path=None):
  """Load client associated with configuration alias.

  :param alias: Alias name.
  :param path: Path to configuration file. Defaults to `.hdfsrc` in the current
    user's home directory.

  """
  path = path or expanduser('~/.hdfsrc')
  options = Config(path).get_alias(alias)
  return Client.load(options.pop('client', None), options)
