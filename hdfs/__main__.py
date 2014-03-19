#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS]
  hdfs [-a ALIAS] ls [-r] RPATH
  hdfs [-a ALIAS] get [-p PARTS] RPATH [[-r] LPATH]
  hdfs -h | --help | -v | --version

Commands:
  ls
  get

Arguments:
  RPATH                   Remote path (on HDFS).
  LPATH                   Local path.

Options:
  -a ALIAS --alias=ALIAS  Alias.
  -h --help               Show this message and exit.
  -p PARTS --parts=PARTS  Which part files to include.
  -r --recursive          Operate on all files and directories recursively.
  -v --version            Show version and exit.

"""

from docopt import docopt
from getpass import getuser
from hdfs import __version__
from hdfs.client import KerberosClient, TokenClient
from hdfs.util import Config
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


def load_client(alias):
  """Load client from alias.

  :param alias: Alias name.

  """
  options = Config().get_alias(alias)
  auth = options.pop('auth')
  if auth == 'insecure':
    return InsecureClient.from_config(options)
  elif auth == 'kerberos':
    return KerberosClient.from_config(options)
  elif auth == 'token':
    return TokenClient.from_config(options)
  else:
    raise HdfsError('Invalid auth %r for alias %r.', auth, alias)


def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = load_client(args['--alias'])


if __name__ == '__main__':
  main()
