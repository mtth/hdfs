#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI."""

__version__ = '0.2.8'

import logging as lg
try:
  from .client import Client, InsecureClient, TokenClient
  from .ext import * # import all exported extensions, nothin' on you
except ImportError:
  pass # in setup.py


class _NullHandler(lg.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass


lg.getLogger(__name__).addHandler(_NullHandler())
