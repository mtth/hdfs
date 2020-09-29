import fnmatch
import re

from hdfs import Client

import posixpath


def glob(client, hdfs_path):
  """Return a list of paths matching a pathname pattern.

  The pattern may contain simple shell-style wildcards a la
  fnmatch. However, unlike fnmatch, filenames starting with a
  dot are special cases that are not matched by '*' and '?'
  patterns.

  :param client: Instance of :class:`Client`.
  :param hdfs_path: HDFS path. May contain special characters like '*', '?' and '['.

    Sample usages:

    .. code-block:: python

      glob(client, './foo/bar/*')
      glob(client, './foo/bar/file[0-9].txt')
      glob(client, './foo/bar/file?.txt')

  """
  return list(iglob(client, hdfs_path))


def iglob(client, hdfs_path):
  """Return an iterator which yields the paths matching a pathname pattern.

  The pattern may contain simple shell-style wildcards a la
  fnmatch. However, unlike fnmatch, filenames starting with a
  dot are special cases that are not matched by '*' and '?'
  patterns.

  :param client: Instance of :class:`Client`.
  :param hdfs_path: HDFS path. May contain special characters like '*', '?' and '['.

    Sample usages:

    .. code-block:: python

      iglob(client, './foo/bar/*')
      iglob(client, './foo/bar/file[0-9].txt')
      iglob(client, './foo/bar/file?.txt')

  """
  dirname, basename = posixpath.split(hdfs_path)
  if not has_magic(hdfs_path):
    if basename:
      if client.status(hdfs_path, strict=False):
        yield hdfs_path
    else:
      # Patterns ending with a slash should match only directories
      if client.status(dirname)['type'] == 'DIRECTORY':
        yield hdfs_path
    return
  if not dirname:
    for p in glob1(client, None, basename):
      yield p
    return
  # `os.path.split()` returns the argument itself as a dirname if it is a
  # drive or UNC path.  Prevent an infinite recursion if a drive or UNC path
  # contains magic characters (i.e. r'\\?\C:').
  if dirname != hdfs_path and has_magic(dirname):
    dirs = iglob(client, dirname)
  else:
    dirs = [dirname]
  if has_magic(basename):
    glob_in_dir = glob1
  else:
    glob_in_dir = glob0
  for dirname in dirs:
    for name in glob_in_dir(client, dirname, basename):
      yield posixpath.join(dirname, name)


def glob1(client, dirname, pattern):
  if not dirname:
    if isinstance(pattern, bytes):
      dirname = bytes(client.resolve('.'))
    else:
      dirname = client.resolve('.')
  names = client.list(dirname)
  if not _ishidden(pattern):
    names = [x for x in names if not _ishidden(x)]
  return fnmatch.filter(names, pattern)


def glob0(client, dirname, basename):
  if not basename:
    # `os.path.split()` returns an empty basename for paths ending with a
    # directory separator.  'q*x/' should match only directories.
    if client.status(dirname)['type'] == 'DIRECTORY':
      return [basename]
  else:
    if client.status(posixpath.join(dirname, basename), strict=False):
      return [basename]
  return []


magic_check = re.compile('([*?[])')
magic_check_bytes = re.compile(b'([*?[])')


def has_magic(s):
  if isinstance(s, bytes):
    match = magic_check_bytes.search(s)
  else:
    match = magic_check.search(s)
  return match is not None


def _ishidden(path):
  return path[0] in ('.', b'.'[0])

