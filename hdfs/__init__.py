#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__all__ = [
  'Client', 'InsecureClient', 'KerberosClient', 'TokenClient',
  'AvroReader',
]
__version__ = '0.2.1'

try:
  from .client import Client, InsecureClient, KerberosClient, TokenClient
  from .ext.avro import AvroReader
  from .util import Config
except ImportError:
  pass # in setup.py
