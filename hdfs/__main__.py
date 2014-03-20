#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] info [-d DEPTH] RPATH
  hdfs [-a ALIAS] upload [-op PERM] RPATH [[-r] LPATH]
  hdfs [-a ALIAS] download [-ol LEN] RPATH [[-r] LPATH]
  hdfs -h | --help | -v | --version

Commands:
  info                    List files and directories along with their size.
  upload                  Upload a file. Reads from stdin by default.
  download                Download a file. Outputs to stdout by default.

Arguments:
  RPATH                   Remote path (on HDFS).
  LPATH                   Local path.

Options:
  -a ALIAS --alias=ALIAS  Alias.
  -d DEPTH --depth=DEPTH  Maximum depth.
  -h --help               Show this message and exit.
  -l LEN --len=LEN        Length.
  -o --overwrite          Allow overwriting.
  -p PERM --perm=PERM     Permissions.
  -r --recursive          Operate on all files and directories recursively.
  -v --version            Show version and exit.

"""

from docopt import docopt
from getpass import getuser
from hdfs import __version__
from hdfs.client import KerberosClient, InsecureClient, TokenClient
from hdfs.util import catch, Config, HdfsError, hsize
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import sys


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


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = load_client(args['--alias'])
  rpath = args['RPATH']
  if rpath == '.':
    rpath = ''
  if args['info']:
    try:
      depth = int(args['--depth'] or '0')
    except ValueError:
      raise HdfsError('Invalid depth argument.')
    for info in client.info(rpath, depth=depth):
      sys.stdout.write('%s\t%s\n' % (hsize(info.size), info))
  elif args['upload']:
    if args['LPATH']:
      client.upload(
        rpath,
        args['LPATH'],
        recursive=args['--recursive'],
        overwrite=args['--overwrite'],
        permission=args['--perm'],
      )
    else:
      client.write(
        rpath,
        (line for line in sys.stdin), # doesn't work with stdin directly, why?
        overwrite=args['--overwrite'],
        permission=args['--perm'],
      )
  elif args['download']:
    try:
      length = int(args['--len'] or '0') or None
    except ValueError:
      raise HdfsError('Invalid length argument.')
    if args['LPATH']:
      client.download(
        rpath,
        args['LPATH'],
        recursive=args['--recursive'],
        overwrite=args['--overwrite'],
      )
    else:
      client.read(
        rpath,
        sys.stdout,
        length=length,
      )


if __name__ == '__main__':
  main()
