#!/usr/bin/env bash

# Start or stop Hadoop HDFS daemons required for tests.
#
# `$HADOOP_HOME` should be set before this command is run.

set -o nounset
set -o errexit

if [[ $# -ne 1 ]]; then
  echo "usage: $0 (start|stop)" >&2
  exit 1
fi

HADOOP_CONF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../etc/hadoop"
"${HADOOP_HOME}/sbin/hadoop-daemon.sh" --config "$HADOOP_CONF_DIR" --script hdfs "$1" datanode
"${HADOOP_HOME}/sbin/hadoop-daemon.sh" --config "$HADOOP_CONF_DIR" --script hdfs "$1" namenode
