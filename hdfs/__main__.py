#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] info [-jud DEPTH] RPATH
  hdfs [-a ALIAS] download [-f] RPATH LPATH
  hdfs [-a ALIAS] upload [-f] LPATH RPATH
  hdfs -h | --help | -v | --version

Commands:
  info                          Display statistics about files and directories.
  download                      Download a file. Specify - to output to stdout.
  upload                        Upload a file. Specify - to read from stdin.

Arguments:
  RPATH                         Remote path (on HDFS).
  LPATH                         Local path.

Options:
  -a ALIAS --alias=ALIAS        Alias.
  -d DEPTH --depth=DEPTH        Maximum depth to explore directories.
  -f --force                    Overwrite existing files.
  -h --help                     Show this message and exit.
  -j --json                     Output JSON instead of tab delimited data.
  -u --usage                    Include content summary for directories.
  -v --version                  Show version and exit.

HdfsCLI exits with return status 1 if an error occurred and 0 otherwise.

"""

from docopt import docopt
from hdfs import __version__
from hdfs.client import get_client_from_alias
from hdfs.util import catch, HdfsError, hsize, htime
from json import dumps
from time import time
import sys


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = get_client_from_alias(args['--alias'])
  rpath = args['RPATH']
  lpath = args['LPATH']
  if args['info']:
    try:
      depth = int(args['--depth'] or '0')
    except ValueError:
      raise HdfsError('Invalid --depth option: %r.', args['--depth'])
    gen = client.walk(rpath, depth, args['--usage'])
    if args['--json']:
      info = [
        {'path': path, 'status': status, 'summary': summary}
        for path, status, summary in gen
      ]
      sys.stdout.write('%s\n' % (dumps(info), ))
    else:
      for fpath, status, summary in gen:
        type_ = status['type']
        if type_ == 'DIRECTORY':
          size = hsize(summary['length']) if summary else '     -'
        else:
          size = hsize(status['length'])
        time_ = htime(time() - status['modificationTime'] / 1000)
        sys.stdout.write('%s\t%s\t%s\t%s\n' % (size, time_, type_[0], fpath))
  elif args['upload']:
    force = args['--force']
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
        client.download(rpath, lpath, overwrite=args['--force'])
      except HdfsError as err:
        sys.stdout.write('%s\n' % (err, ))
      else:
        sys.stdout.write('OK\n')
    else:
      client.read(rpath, sys.stdout)


if __name__ == '__main__':
  main()
