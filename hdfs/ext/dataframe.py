#!/usr/bin/env python
# encoding: utf-8

"""Read and write Pandas dataframes directly from HDFS.

Sample usage:

.. code:: python

  from hdfs.ext.dataframe import read_dataframe, write_dataframe
  import pandas as pd

  # Create sample dataframe.
  df = pd.DataFrame.from_records([
    {'A': 10, 'B': 21},
    {'A': 11, 'B': 23},
  ])

  # Write dataframe to HDFS (serialized as Avro records).
  write_dataframe(client, 'data.avro', df)

  # Read the Avro file back from HDFS.
  read_dataframe(client, 'data.avro') # == df

This extension requires both the `avro` extension and `pandas` to be installed.
Currently only Avro serialization is supported.

"""

from .avro import AvroReader, AvroWriter
import pandas as pd


def read_dataframe(client, hdfs_path):
  """Read dataframe from HDFS Avro file.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path to an Avro file (potentially distributed).

  """
  with AvroReader(client, hdfs_path) as reader:
    # Hack-ish, but loading all elements in memory first to get length.
    return pd.DataFrame.from_records(list(reader))


def write_dataframe(client, hdfs_path, df, **kwargs):
  """Save dataframe to HDFS as Avro.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path where the dataframe will be stored.
  :param df: Dataframe to store.
  :param \*\*kwargs: Keyword arguments passed through to
    :class:`hdfs.ext.avro.AvroWriter`.

  """
  with AvroWriter(client, hdfs_path, **kwargs) as writer:
    for _, row in df.iterrows():
      writer.write(row.to_dict())
