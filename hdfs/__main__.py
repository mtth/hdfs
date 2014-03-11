#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS | -u URL] ls [-r] RPATH
  hdfs [-a ALIAS | -u URL] get [-p PARTS] RPATH [[-r] LPATH]
  hdfs -h | --help | -v | --version

Commands:
  ls
  get

Arguments:
  RPATH                   Remote path (on HDFS).
  LPATH                   Local path.

Options:
  -h --help               Show this message and exit.
  -p PARTS --parts=PARTS  Which part files to include.
  -r --recursive          Operate on all files and directories recursively.
  -v --version            Show version and exit.

"""

from docopt import docopt
from getpass import getuser
from hdfs import __version__
from hdfs.client import Client
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = Client(
    host='eat1-magicnn01.grid.linkedin.com',
    port=50070,
    auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL),
    user=getuser(),
  )
  print getattr(client, args['COMMAND'])(args['PATH'])

if __name__ == '__main__':
  main()
