#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] [--info] [-j] [-d DEPTH] [PATH]
  hdfs [-a ALIAS] --read PATH
  hdfs [-a ALIAS] --write [-o] PATH
  hdfs [-a ALIAS] --download [-o] [-t THREADS] PATH LOCALPATH
  hdfs -h | --help | -v | --version

Commands:
  --info                        View information about files and directories.
                                This is the default command.
  --read                        Read a file from HDFS to standard out. If
                                `PATH` is a directory, this command will
                                attempt to read (in order) any part-files found
                                directly under it.
  --download                    Download a file from HDFS into `LOCALPATH`. If 
                                `PATH` is a directory, attempt to download all 
                                part-files found in it into `LOCALPATH`.
  --write                       Write from standard in to HDFS.

Arguments:
  PATH                          Remote HDFS path.
  LOCALPATH                     Local file or directory for downloading. 

Options:
  -a ALIAS --alias=ALIAS        Alias, defaults to alias pointed to by 
                                `default.alias` in ~/.hdfsrc.
  -d DEPTH --depth=DEPTH        Maximum depth to explore directories. Specify
                                `-1` for no limit [default: 0].
  -o --overwrite                'Smart' mode overwrite: Overwrite existing files
                                at target location whose timestamps are newer 
                                than source files or if file sizes do not match.
  -h --help                     Show this message and exit.
  -j --json                     Output JSON instead of tab delimited data.
  -t THREADS --threads=THREADS  Number of threads to use for downloading part-
                                files. `-1` allocates a thread per part-file
                                while `1` disables parallelization [default: 1].
  -v --version                  Show version and exit.

Examples:
  hdfs -a prod /user/foo
  hdfs --read logs/1987-03-23 >>logs
  hdfs --write -o data/weights.tsv <weights.tsv

HdfsCLI exits with return status 1 if an error occurred and 0 otherwise.

"""

from docopt import docopt
from hdfs import __version__
from hdfs.client import Client
from hdfs.util import catch, HdfsError, hsize, htime
from json import dumps
from time import time
import sys
import os


def infos(client, hdfs_path, depth, json):
  """Get informations about files and directories.

  :param client: :class:`~hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param depth: Maximum exploration depth.
  :param json: Return JSON output.

  """
  def _infos():
    """Helper generator."""
    for path, status in client.walk(hdfs_path, depth=depth):
      if status['type'] == 'DIRECTORY':
        yield path, status, client.content(path)
      else:
        yield path, status, None
  if json:
    info = [
      {'path': path, 'status': status, 'content': content}
      for path, status, content in _infos()
    ]
    sys.stdout.write('%s\n' % (dumps(info), ))
  else:
    for fpath, status, content in _infos():
      type_ = status['type']
      if type_ == 'DIRECTORY':
        size = hsize(content['length']) if content else '     -'
      else:
        size = hsize(status['length'])
      time_ = htime(time() - status['modificationTime'] / 1000)
      sys.stdout.write('%s\t%s\t%s\t%s\n' % (size, time_, type_[0], fpath))

def read(reader, size, message, _out=sys.stdout, _err=sys.stderr):
  """Download a file from HDFS.

  :param reader: Generator.
  :param message: Message printed for each file read.
  :param _out: Performance caching.
  :param _err: Performance caching.

  """
  if _out.isatty():
    for chunk in reader:
      _out.write(chunk)
  else:
    _err.write('%s\t ??%%\r' % (message, ))
    _err.flush()
    try:
      position = 0
      for chunk in reader:
        position += len(chunk)
        progress = 100 * position / size
        _err.write('%s\t%3i%%\r' % (message, progress))
        _err.flush()
        _out.write(chunk)
    except HdfsError as exc:
      _err.write('%s\t%s\n' % (message, exc))
    else:
      _err.write('%s\t     \n' % (message, ))

@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = Client.from_alias(args['--alias'])
  rpath = args['PATH'] or ''

  int_args = {}
  for p in ('--depth', '--threads'):
    try:
      int_args[p] = int(args[p])
    except ValueError:
      raise HdfsError('Invalid `%s` option: %r.', p, args[p])

  if args['--write']:
    reader = (line for line in sys.stdin) # doesn't work with stdin, why?
    client.write(rpath, reader, overwrite=args['--overwrite'])
  elif args['--read']:
    parts = client.parts(rpath)
    for index, path in enumerate(parts):
      size = client.status(path)['length']
      message = '%s\t%s\t[%s/%s]' % (hsize(size), path, index + 1, len(parts))
      read(client.read(path), size, message)
  elif args['--download']:
    status_dict = client.status(rpath)
    client.download(rpath, args['LOCALPATH'], overwrite=args['--overwrite'], 
      num_threads=int_args['--threads'])
  else:
    infos(client, rpath, int_args['--depth'], args['--json'])

if __name__ == '__main__':
  main()
