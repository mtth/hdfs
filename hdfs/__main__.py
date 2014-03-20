#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] upload [-f | -l] RPATH [[-r] LPATH]
  hdfs [-a ALIAS] download [-f | -l] RPATH [[-r] LPATH]
  hdfs -h | --help | -v | --version

Commands:
  upload                      Upload a file. Reads from stdin by default.
  download                    Download a file. Outputs to stdout by default.

Arguments:
  RPATH                       Remote path (on HDFS).
  LPATH                       Local path.

Options:
  -a ALIAS --alias=ALIAS      Alias.
  -h --help                   Show this message and exit.
  -f --fresh                  Overwrite existing files.
  -l --lazy                   Skip up-to-date files and overwrite old ones.
  -r --recursive              Operate on all files and directories recursively.
  -v --version                Show version and exit.

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

def recursive_download(client, rpath, lpath, fresh, lazy):
  """Download folder hierarchy.

  :param client: TODO
  :param rpath: TODO
  :param lpath: TODO
  :param fresh: TODO
  :param lazy: TODO

  """
  pass

def recursive_upload(client, rpath, lpath, fresh, lazy):
  """Upload folder hierarchy.

  :param client: TODO
  :param rpath: TODO
  :param lpath: TODO
  :param fresh: TODO
  :param lazy: TODO

  """
  pass

@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = load_client(args['--alias'])
  rpath = args['RPATH']
  if rpath == '.':
    rpath = ''
  if args['info']:
    sizes = args['--sizes']
    for info in client.info(rpath, depth=depth, sizes=sizes):
      if sizes:
        sys.stdout.write('%s\t%s\n' % (hsize(info.size), info))
      else:
        sys.stdout.write('%s\n' % (info, ))
  else:
    lpath = args['LPATH']
    fresh = args['--fresh']
    recursive = args['--recursive']
    if args['upload']:
      if lpath:
        if recursive:
          recursive_upload(client, rpath, lpath, fresh, lazy)
        else:
          client.upload(rpath, lpath, overwrite=fresh)
      else:
        reader = (line for line in sys.stdin) # doesn't work with stdin, why?
        client.write(rpath, reader, overwrite=fresh)
    elif args['download']:
      if lpath:
        if recursive:
          recursive_download(client, rpath, lpath, fresh, lazy)
        else:
          client.download(rpath, lpath, overwrite=fresh)
      else:
        client.read(rpath, sys.stdout)


if __name__ == '__main__':
  main()
