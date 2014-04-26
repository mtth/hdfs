#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] [--info] [-jd DEPTH] [PATH]
  hdfs [-a ALIAS] --read [-p PARTS] PATH
  hdfs [-a ALIAS] --write [-o] PATH
  hdfs -h | --help | -v | --version

Commands:
  --info                        View information about files and directories.
                                This is the default command.
  --read                        Read a file from HDFS to standard out. If
                                `PATH` is a directory, this command will
                                attempt to read (in order) any part-files found
                                directly under it.
  --write                       Write from standard in to HDFS.

Arguments:
  PATH                          Remote HDFS path.

Options:
  -a ALIAS --alias=ALIAS        Alias.
  -d DEPTH --depth=DEPTH        Maximum depth to explore directories. Specify
                                `-1` for no limit.
  -o --overwrite                Overwrite any existing file.
  -h --help                     Show this message and exit.
  -j --json                     Output JSON instead of tab delimited data.
  -p PARTS --parts=PARTS        Comma separated list of part-file numbers. Only
                                those will be read if `PATH` is partitioned.
  -v --version                  Show version and exit.

Examples:
  hdfs -a prod /user/foo
  hdfs --read logs/1987-03-23 >>logs
  hdfs --write -f data/weights.tsv <weights.tsv

HdfsCLI exits with return status 1 if an error occurred and 0 otherwise.

"""

from docopt import docopt
from hdfs import __version__, get_client_from_alias
from hdfs.util import catch, HdfsError, hsize, htime
from json import dumps
from time import time
import re
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

def read(client, hdfs_path, suffix=''):
  """Download a file from HDFS.

  :param client: :class:`~hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param size: Size in bytes of file read. Used for progress indicator.

  """
  if sys.stdout.isatty():
    client.read(hdfs_path, sys.stdout)
  else:
    size = client.status(hdfs_path)['length']
    message = '%s\t%s%s' % (hsize(size), hdfs_path, suffix)
    sys.stderr.write('%s\t ??%%\r' % (message, ))
    sys.stderr.flush()
    try:
      def callback(position, out=sys.stderr):
        """Callback helper. Note the stderr local variable caching."""
        progress = 100 * position / size
        out.write('%s\t%3i%%\r' % (message, progress))
        out.flush()
      client.read(hdfs_path, sys.stdout, callback=callback)
    except HdfsError as err:
      sys.stderr.write('%s\t%s\n' % (message, err))
    else:
      sys.stderr.write('%s\t     \n' % (message, ))

@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = get_client_from_alias(args['--alias'])
  rpath = args['PATH'] or ''
  try:
    depth = int(args['--depth'] or '0')
  except ValueError:
    raise HdfsError('Invalid `--depth` option: %r.', args['--depth'])
  if args['--write']:
    reader = (line for line in sys.stdin) # doesn't work with stdin, why?
    client.write(rpath, reader, overwrite=args['--overwrite'])
  elif args['--read']:
    content = client.content(rpath)
    if not content['directoryCount']:
      read(client, rpath)
    else:
      pattern = re.compile(r'^part-(?:m|r)-(\d+)[^/]*$')
      matches = (
        (path, pattern.match(status['pathSuffix']))
        for path, status in client.walk(rpath, depth=1)
      )
      part_files = dict(
        (int(match.group(1)), path)
        for path, match in matches
        if match
      )
      if not part_files:
        raise HdfsError('No part-files found in directory %r.', rpath)
      if args['--parts']:
        try:
          paths = [part_files[int(p)] for p in args['--parts'].split(',')]
        except ValueError:
          raise HdfsError('Invalid `--parts` option: %r.', args['--parts'])
        except KeyError as err:
          raise HdfsError('Missing part-file: %r.', err.args[0])
      else:
        paths = sorted(part_files.values())
      for index, path in enumerate(paths):
        read(client, path, '\t[%s/%s]' % (index + 1, len(paths)))
  else:
    infos(client, rpath, depth, args['--json'])

if __name__ == '__main__':
  main()
