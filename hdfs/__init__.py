#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__version__ = '2.0.0'

import logging as lg
try:
  from .client import Client, InsecureClient, TokenClient
  from .config import Config, NullHandler
  from .util import HdfsError
except ImportError:
  pass # In setup.py.
