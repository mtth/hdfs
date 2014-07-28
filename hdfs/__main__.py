#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] [--info] [-j] [-d DEPTH] [RPATH]
  hdfs [-a ALIAS] --read RPATH
  hdfs [-a ALIAS] --write [-o] RPATH
  hdfs [-a ALIAS] --download [-o] [-t THREADS] RPATH LPATH
  hdfs -h | --help | -l | --log | -v | --version

Commands:
  --download                    Download a (potentially distributed) file from
                                HDFS into `LPATH`.
  --info                        View information about files and directories.
  --read                        Read a file from HDFS to standard out. Note
                                that this only works for normal files.
  --write                       Write from standard in to a path on HDFS.

Arguments:
  LPATH                         Path to local file or directory.
  RPATH                         Remote HDFS path.

Options:
  -a ALIAS --alias=ALIAS        Alias, defaults to alias pointed to by
                                `default.alias` in ~/.hdfsrc.
  -d DEPTH --depth=DEPTH        Maximum depth to explore directories. Specify
                                `-1` for no limit [default: 0].
  -l --log                      Show path to current log file and exit.
  -h --help                     Show this message and exit.
  -j --json                     Output JSON instead of tab delimited data.
  -o --overwrite                Allow overwriting any existing files.
  -t THREADS --threads=THREADS  Number of threads to use for downloading
                                distributed files. `-1` allocates a thread per
                                part-file while `1` disables parallelization
                                altogether [default: -1].
  -v --version                  Show version and exit.

Examples:
  hdfs -a prod /user/foo
  hdfs --read logs/1987-03-23 >>logs
  hdfs --write -o data/weights.tsv <weights.tsv
  hdfs --download features.avro dat/

HdfsCLI exits with return status 1 if an error occurred and 0 otherwise.

"""

from docopt import docopt
from hdfs import __version__
from hdfs.client import Client
from hdfs.util import Config, HdfsError, catch, hsize, htime
from json import dumps
from time import time
import logging as lg
import sys


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

  If the output is piped to something else than a TTY, progress percentages
  are displayed.

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
  # arguments parsing first for quicker feedback on invalid arguments
  args = docopt(__doc__, version=__version__)
  # set up logging
  logger = lg.getLogger('hdfs')
  logger.setLevel(lg.DEBUG)
  handler = Config().get_file_handler()
  if handler:
    logger.addHandler(handler)
  # set up client and fix arguments
  client = Client.from_alias(args['--alias'])
  rpath = args['RPATH'] or ''
  for option in ('--depth', '--threads'):
    try:
      args[option] = int(args[option])
    except ValueError:
      raise HdfsError('Invalid `%s` option: %r.', option, args[option])
  # run command
  if args['--log']:
    if handler:
      sys.stdout.write('%s\n' % (handler.baseFilename, ))
    else:
      raise HdfsError('No log file active.')
  elif args['--write']:
    reader = (line for line in sys.stdin) # doesn't work with stdin, why?
    client.write(rpath, reader, overwrite=args['--overwrite'])
  elif args['--read']:
    size = client.status(rpath)['length']
    read(client.read(rpath), size, '%s\t' % (rpath, ))
  elif args['--download']:
    client.download(
      rpath,
      args['LPATH'],
      overwrite=args['--overwrite'],
      n_threads=args['--threads']
    )
  else:
    infos(client, rpath, args['--depth'], args['--json'])

if __name__ == '__main__':
  main()
