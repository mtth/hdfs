#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__version__ = '0.0.2'

try:
  from .client import InsecureClient, KerberosClient, TokenClient
except ImportError:
  pass # in setup.py
