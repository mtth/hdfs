#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfs [-a ALIAS] info [-jud DEPTH] RPATH
  hdfs [-a ALIAS] download [-fd DEPTH] RPATH LPATH
  hdfs [-a ALIAS] upload [-f] LPATH RPATH
  hdfs -h | --help | -v | --version

Commands:
  info                          Display statistics about files and directories.
  download                      Download a file or directory. If downloading a
                                single file, you can specify `-` as LPATH to
                                pipe the output to stdout.
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
from hdfs import __version__, get_client_from_alias
from hdfs.util import catch, HdfsError, hsize, htime
from json import dumps
from os import makedirs
from os.path import dirname, isdir, join
from time import time
import errno
import sys


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  client = get_client_from_alias(args['--alias'])
  rpath = args['RPATH']
  lpath = args['LPATH']
  try:
    depth = int(args['--depth'] or '0')
  except ValueError:
    raise HdfsError('Invalid --depth option: %r.', args['--depth'])
  if args['info']:
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
    force = args['--force']
    if depth:
      if lpath == '-':
        raise HdfsError(
          'Piping to stdout only supported when downloading a single file.'
        )
      for _rpath, status, _ in client.walk(rpath, depth=depth):
        if status['type'] == 'FILE':
          _rel_rpath = _rpath.replace(r'^.*?%s/' % (rpath, ), '').split('/')
          _lpath = join(lpath, *_rel_rpath)
          download_file(client, _rpath, _lpath, force, status['length'])
    else:
      if lpath == '-':
        client.read(rpath, sys.stdout)
      else:
        download_file(client, rpath, lpath, force)


def download_file(client, rpath, lpath, force, size=None):
  """Download a file from HDFS.

  :param client: :class:`~hdfs.client.Client` instance.
  :param rpath: Remote path.
  :param lpath: Local path. Any missing directories in the path hierarchy will
    be created.
  :param force: Overwrite an existing file. Note that this does not allow the
    creation of new directories to overwrite existing files. Only the terminal
    node can.
  :param size: Optional size in bytes, will save a remote call.

  """
  try:
    makedirs(dirname(lpath))
  except OSError as err:
    if err.errno == errno.EEXIST and isdir(dirname(lpath)):
      pass
    else:
      sys.stderr.write('%s ERROR: invalid local path %s\n' % (rpath, lpath))
      return
  sys.stdout.write('%s  ??%%' % (rpath, ))
  sys.stdout.flush()
  try:
    size = size or list(client.walk(rpath))[0][1]['length']
    def callback(position):
      progress = 100 * position / size
      sys.stdout.write('\r%s %3i%%' % (rpath, progress))
      sys.stdout.flush()
    client.download(
      rpath,
      lpath,
      overwrite=force,
      callback=callback,
    )
  except HdfsError as err:
    sys.stdout.write('\n')
    sys.stderr.write('%s\n' % (err, ))
  else:
    sys.stdout.write('\r%s     \n' % (rpath, ))

if __name__ == '__main__':
  main()
