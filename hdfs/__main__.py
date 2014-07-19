#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] [--info] [-j] [-d DEPTH] [PATH]
  hdfs [-a ALIAS] --read PATH
  hdfs [-a ALIAS] --write [-o] PATH
  hdfs [-a ALIAS] --download [-t THREADS] [-o|-s] PATH LOCALPATH
  hdfs -h | --help | -v | --version

Commands:
  --info                        View information about files and directories.
                                This is the default command.
  --read                        Read a file from HDFS to standard out. If
                                `PATH` is a directory, this command will
                                attempt to read (in order) any part-files found
                                directly under it.
  --download                    Download a file from HDFS. If `PATH` is a 
                                directory, attempt to download all part-file
                                found directly under into directory specified
                                by `LOCALPATH`.  Otherwise, download the file.
  --write                       Write from standard in to HDFS.

Arguments:
  PATH                          Remote HDFS path.
  LOCALPATH                     Local file or directory for downloadeding. 

Options:
  -a ALIAS --alias=ALIAS        Alias.
  -d DEPTH --depth=DEPTH        Maximum depth to explore directories. Specify
                                `-1` for no limit [default: 0].
  -o --overwrite                Ovewrite files.
  -s --smartoverwrite           'Smart' mode overwrite: Overwrite any existing
                                file if HDFS timestamp is newer than local or 
                                if file sizes do not match.
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

  overwrite = args['--overwrite']
  if overwrite == False and args['--smartoverwrite']:
    overwrite = 'smart'

  try:
    depth = int(args['--depth'])
  except ValueError:
    raise HdfsError('Invalid `--depth` option: %r.', args['--depth'])
  if args['--write']:
    reader = (line for line in sys.stdin) # doesn't work with stdin, why?
    client.write(rpath, reader, overwrite=overwrite)
  elif args['--read']:
    parts = client.parts(rpath)
    for index, path in enumerate(parts):
      size = client.status(path)['length']
      message = '%s\t%s\t[%s/%s]' % (hsize(size), path, index + 1, len(parts))
      read(client.read(path), size, message)
  elif args['--download']:
    status_dict = client.status(rpath)
    if status_dict['type'] == 'DIRECTORY':
      client.download_parts(rpath, args['LOCALPATH'], overwrite=overwrite, 
        num_threads=int(args['--threads']))
    else:
      lpath = args['LOCALPATH'] 
      if os.path.isdir(lpath) or os.path.join(lpath,"") == lpath:
        # local path is a directory
        lpath = client._get_local_file_name(rpath, lpath)

      client.download(rpath, lpath, overwrite=overwrite)

  else:
    infos(client, rpath, depth, args['--json'])

if __name__ == '__main__':
  main()
