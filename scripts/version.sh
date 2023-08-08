#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
shopt -s nullglob

__dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fail() { # MSG
  echo "$1" >&2 && exit 1
}

version_pattern="__version__ = '([^']+)'"

main() {
  cd "$__dirname/.."
  local line="$(grep __version__ hdfs/__init__.py)"
  echo "$line"
  if ! [[ $line =~ $version_pattern ]]; then
    fail 'missing version'
  fi
  echo "${BASH_REMATCH[1]}"
}

main "$@"
