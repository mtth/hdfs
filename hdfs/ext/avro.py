#!/usr/bin/env python
# encoding: utf-8

"""Avro extension."""

from avro.datafile import DataFileReader
from avro.io import DatumReader


def foo(path):
  """Example."""
  with DataFileReader(path, DatumReader()) as reader:
    for record in reader:
      print record
