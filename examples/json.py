#!/usr/bin/env python
# encoding: utf-8

"""Sample HdfsCLI script."""

from hdfs import Config
from json import dumps, load


# Get the default alias' client.
client = Config().get_client()

# Our new model.
weights = {
  '(intercept)': 48.,
  'first_feature': 2.,
  'second_feature': 12.,
  # ...
}

# The path on HDFS where we will store the file.
path = 'models/3.json'

# Serialize to JSON and upload to HDFS.
data = dumps(weights)
client.write(path, data=data, encoding='utf-8', overwrite=True)

# The file's HDFS status, we can use it to verify that all the data is there.
status = client.status(path)
assert status['length'] == len(data)

# Download the file back and check that the deserialized contents match.
with client.read(path, encoding='utf-8') as reader:
  assert load(reader) == weights
