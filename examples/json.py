#!/usr/bin/env python
# encoding: utf-8

"""Sample HdfsCLI script.

This example shows how to write files to HDFS, read them back, and perform a
few other simple filesystem operations.

"""

from hdfs import Config
from json import dump, load


# Get the default alias' client. (See the quickstart section in the
# documentation to learn more about this.)
client = Config().get_client()

# Some fake data that we are interested in uploading to HDFS.
model = {
  '(intercept)': 48.,
  'first_feature': 2.,
  'second_feature': 12.,
}

# First, we delete any existing `models/` folder on HDFS.
client.delete('models', recursive=True)

# We can now upload the data, first as CSV.
with client.write('models/1.csv', encoding='utf-8') as writer:
  for item in model.items():
    writer.write(u'%s,%s\n' % item)

# We can also serialize it to JSON and directly upload it.
with client.write('models/1.json', encoding='utf-8') as writer:
  dump(model, writer)

# We can check that the files exist and get their properties.
assert client.list('models') == ['1.csv', '1.json']
status = client.status('models/1.csv')
content = client.content('models/1.json')

# Later, we can download the files back. The `delimiter` option makes it
# convenient to read CSV files.
with client.read('models/1.csv', delimiter='\n', encoding='utf-8') as reader:
  items = (line.split(',') for line in reader if line)
  assert dict((name, float(value)) for name, value in items) == model

# Loading JSON directly from HDFS is even simpler.
with client.read('models/1.json', encoding='utf-8') as reader:
  assert load(reader) == model
