#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] upload [-f] LPATH RPATH
  hdfs [-a ALIAS] download [-f] RPATH LPATH
  hdfs -h | --help | -v | --version

Commands:
  upload                      Upload a file. Specify - to read from stdin.
  download                    Download a file. Specify - to output to stdout.

Arguments:
  RPATH                       Remote path (on HDFS).
  LPATH                       Local path.

Options:
  -a ALIAS --alias=ALIAS      Alias.
  -h --help                   Show this message and exit.
  -f --force                  Overwrite existing files.
  -v --version                Show version and exit.

"""

from docopt import docopt
from hdfs import __version__
from hdfs.client import get_client_from_alias
from hdfs.util import catch, HdfsError
import sys


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = get_client_from_alias(args['--alias'])
  rpath = args['RPATH']
  lpath = args['LPATH']
  force = args['--force']
  if args['upload']:
    if lpath != '-':
      sys.stdout.write('Uploading %s to %s ... ' % (lpath, rpath))
      sys.stdout.flush()
      try:
        client.upload(rpath, lpath, overwrite=force)
      except HdfsError as err:
        sys.stdout.write('%s\n' % (err, ))
      else:
        sys.stdout.write('OK\n')
    else:
      sys.stdout.write('Uploading from stdin to %s ... ' % (rpath, ))
      sys.stdout.flush()
      reader = (line for line in sys.stdin) # doesn't work with stdin, why?
      client.write(rpath, reader, overwrite=force)
      sys.stdout.write('OK\n')
  elif args['download']:
    if lpath != '-':
      sys.stdout.write('Downloading %s to %s ... ' % (rpath, lpath))
      sys.stdout.flush()
      try:
        client.download(rpath, lpath, overwrite=force)
      except HdfsError as err:
        sys.stdout.write('%s\n' % (err, ))
      else:
        sys.stdout.write('OK\n')
    else:
      client.read(rpath, sys.stdout)


if __name__ == '__main__':
  main()
