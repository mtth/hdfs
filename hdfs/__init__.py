#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__version__ = '2.0.0'

import logging as lg
try:
  from .client import Client, InsecureClient, TokenClient
  from .config import Config
  from .util import HdfsError
except ImportError:
  pass # In setup.py.


class _NullHandler(lg.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass


lg.getLogger(__name__).addHandler(_NullHandler())
