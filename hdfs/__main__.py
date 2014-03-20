#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] upload [-f] RPATH [[-r] LPATH]
  hdfs [-a ALIAS] download [-f] RPATH [[-r] LPATH]
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
  -f --force                  Overwrite existing files.
  -r --recursive              Operate on all files and directories recursively.
  -v --version                Show version and exit.

"""

from docopt import docopt, DocoptExit
from hdfs import __version__
from hdfs.client import KerberosClient, InsecureClient, TokenClient
from hdfs.util import catch, Config, HdfsError
from os import walk
from os.path import isdir, join
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

def find_local_paths(lpath, base_rpath):
  """Find files for recursive upload.

  :param lpath: Local path.

  """
  paths = []
  if not isdir(lpath):
    paths.append((base_rpath, lpath))
  for dir_path, fnames, dnames in walk(lpath):
    dir_path = dir_path.lstrip('./')
    for fname in fnames:
      _lpath = join(dir_path, fname)
      _rpath = join(base_rpath, _lpath)
      paths.append((_rpath, _lpath))
  return paths

def find_remote_paths(rpath, base_lpath):
  """Find files for recursive download.

  :param rpath: HDFS path.

  Note that there is no endpoint for recursive remote filetree exploration so
  this is (much) slower than the local version.

  """
  paths = []
  # TODO
  return paths

@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = load_client(args['--alias'])
  rpath = args['RPATH']
  lpath = args['LPATH']
  force = args['--force']
  recursive = args['--recursive']
  if recursive and not lpath:
    raise DocoptExit() # inconsistency in docopt
  if args['upload']:
    if lpath:
      if recursive:
        paths = find_local_paths(lpath, rpath)
      else:
        paths = [(rpath, lpath)]
      for _rpath, _lpath in paths:
        sys.stdout.write('Uploading %s ... ' % (_lpath, ))
        try:
          client.upload(_rpath, _lpath, overwrite=force)
        except HdfsError as err:
          sys.stdout.write('ERROR: %s\n' % (err, ))
        else:
          sys.stdout.write('OK\n')
    else:
      sys.stdout.write('Uploading ... ')
      reader = (line for line in sys.stdin) # doesn't work with stdin, why?
      client.write(rpath, reader, overwrite=force)
      sys.stdout.write('OK\n')
  elif args['download']:
    if lpath:
      if recursive:
        paths = find_remote_paths(rpath, lpath)
      else:
        paths = [(rpath, lpath)]
      for _rpath, _lpath in paths:
        sys.stdout.write('Downloading %s ... ' % (_rpath, ))
        try:
          client.download(rpath, lpath, overwrite=force)
        except HdfsError as err:
          sys.stdout.write('ERROR: %s\n' % (err, ))
        else:
          sys.stdout.write('OK\n')
    else:
      sys.stdout.write('Downloading ... ')
      client.read(rpath, sys.stdout)
      sys.stdout.write('OK\n')


if __name__ == '__main__':
  main()
