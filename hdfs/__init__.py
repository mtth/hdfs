#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__version__ = '0.1.0'

try:
  from .client import (InsecureClient, KerberosClient, TokenClient,
    get_client_from_alias)
except ImportError:
  pass # in setup.py
