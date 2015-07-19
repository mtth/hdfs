#!/usr/bin/env bash

# Download latest Hadoop distribution.
#
# A temporary folder is created and `$HADOOP_HOME` will be printed to stdout on
# success.

set -o nounset
set -o errexit

hadoop='hadoop-2.7.1'

cd "$(mktemp -d 2>/dev/null || mktemp -d -t 'hadoop')"
curl -O "http://download.nextag.com/apache/hadoop/common/${hadoop}/${hadoop}.tar.gz"
tar -xzf "${hadoop}.tar.gz"
echo "$(pwd)/${hadoop}"
