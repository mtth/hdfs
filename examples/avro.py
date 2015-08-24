#!/usr/bin/env python
# encoding: utf-8

"""Avro extension example."""

from hdfs import Config
from hdfs.ext.avro import AvroReader, AvroWriter


# Get the default alias' client.
client = Config().get_client()

# Some sample data.
records = [
  {'name': 'Ann', 'age': 23},
  {'name': 'Bob', 'age': 22},
]

# Write an Avro File to HDFS (since our records' schema is very simple, we let
# the writer infer it automatically, otherwise we would pass it as argument).
with AvroWriter(client, 'names.avro', overwrite=True) as writer:
  for record in records:
    writer.write(record)

# Read it back.
with AvroReader(client, 'names.avro') as reader:
  schema = reader.schema # The inferred schema.
  content = reader.content # The remote file's HDFS content object.
  assert list(reader) == records # The records match!
