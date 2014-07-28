#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__all__ = [
  'Client', 'InsecureClient', 'KerberosClient', 'TokenClient',
  'AvroReader',
]
__version__ = '0.2.4'

import logging as lg
try:
  from .client import Client, InsecureClient, KerberosClient, TokenClient
  from .ext.avro import AvroReader
  from .util import Config
except ImportError:
  pass # in setup.py


class NullHandler(lg.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass


lg.getLogger(__name__).addHandler(NullHandler())
