#!/usr/bin/env python
# encoding: utf-8

"""Read and write Pandas_ dataframes directly from HDFS.

.. literalinclude:: /../examples/dataframe.py

This extension requires both the `avro` extension and `pandas` to be installed.
Currently only Avro serialization is supported.

.. _Pandas: http://pandas.pydata.org/

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
