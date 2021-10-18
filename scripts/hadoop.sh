#!/usr/bin/env bash

# Hadoop utilities to setup a standalone HDFS cluster for integration tests.
#
# The following commands will download Hadoop locally and start a single node
# HDFS cluster:
#
# ```bash
# $ export HADOOP_HOME="$(./scripts/hadoop.sh download)"
# $ export HADOOP_CONF_DIR="$(./scripts/hadoop.sh config)"
# $ ./scripts/hadoop.sh start
# ```
#
# Later, to stop it:
#
# ```bash
# $ ./scripts/hadoop.sh stop
# ```
#

set -o nounset
set -o errexit

# Print  usage and exit.
#
# Refer to individual functions below for more information.
#
usage() {
  echo "usage: $0 (config|download|start|stop)" >&2
  exit 1
}

# Download Hadoop binary.
#
# TODO: Test against several versions? (But they are very big...)
#
hadoop-download() {
  # Verification as per https://web.archive.org/web/20211018165755/https://hadoop.apache.org/releases.html#to-verify-hadoop-releases-using-gpg
  local hadoop='hadoop-2.9.2'
  export TEMP_HADOOP_FILE="/tmp/${hadoop}.tar.gz"
  export TEMP_HASH_FILE="$TEMP_HADOOP_FILE.asc"
  cd "$(mktemp -d 2>/dev/null || mktemp -d -t 'hadoop')"
  if [ ! -f "$TEMP_HADOOP_FILE" ]; then
    curl "https://archive.apache.org/dist/hadoop/common/${hadoop}/${hadoop}.tar.gz" --output "$TEMP_HADOOP_FILE"
  fi
  curl "https://archive.apache.org/dist/hadoop/common/${hadoop}/${hadoop}.tar.gz.asc" --output "$TEMP_HASH_FILE"
  curl "https://downloads.apache.org/hadoop/common/KEYS" --output "/tmp/KEYS"
  cd /tmp
  gpg --import "/tmp/KEYS" 2> /dev/null
  if gpg --verify hadoop-2.9.2.tar.gz.asc 2>&1| grep -q "Primary key fingerprint"; then
    # "2>&1" means "to > (redirect) 2 (stderr) to &1 (input of stdout)"; otherwise the grep doesn't work.
    cd - > /dev/null # go back, but don't output
    tar -xzf "${TEMP_HADOOP_FILE}"
    echo "$(pwd)/${hadoop}"
  else
    echo "removing $TEMP_HADOOP_FILE and redownloading it"
    rm -f $TEMP_HADOOP_FILE
    hadoop-download
  fi
}

# Generate configuration and print corresponding path.
#
# The returned path is suitable to be used as environment variable
# `$HADOOP_CONF_DIR`. Note that this is necessary because proxy users are
# defined as property keys, so it's not possible to allow the current user
# otherwise.
#
hadoop-config() {
  local tpl_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../etc/hadoop"
  local conf_dir="$(mktemp -d 2>/dev/null || mktemp -d -t 'hadoop-conf')"
  for i in "$tpl_dir"/*; do
    sed -e "s/#USER#/$(whoami)/" "$i" >"${conf_dir}/$(basename "$i")"
  done
  echo "$conf_dir"
}

# Start HDFS cluster (single namenode and datanode) and HttpFS server.
#
# This requires `$HADOOP_HOME` and `$HADOOP_CONF_DIR` to be set.
#
hadoop-start() {
  "${HADOOP_HOME}/bin/hdfs" namenode -format -nonInteractive || :
  "${HADOOP_HOME}/sbin/hadoop-daemon.sh" --config "$HADOOP_CONF_DIR" --script hdfs start namenode
  "${HADOOP_HOME}/sbin/hadoop-daemon.sh" --config "$HADOOP_CONF_DIR" --script hdfs start datanode
  HTTPFS_CONFIG="$HADOOP_CONF_DIR" "${HADOOP_HOME}/sbin/httpfs.sh" start
}

# Stop HDFS cluster and HttpFS server.
#
# This requires `$HADOOP_HOME` to be set.
#
hadoop-stop() {
  "${HADOOP_HOME}/sbin/httpfs.sh" stop
  "${HADOOP_HOME}/sbin/hadoop-daemon.sh" --script hdfs stop datanode
  "${HADOOP_HOME}/sbin/hadoop-daemon.sh" --script hdfs stop namenode
}

if [[ $# -ne 1 ]]; then
  usage
fi

case "$1" in
  download) hadoop-download ;;
  config) hadoop-config ;;
  start) hadoop-start ;;
  stop) hadoop-stop ;;
  *) usage ;;
esac
