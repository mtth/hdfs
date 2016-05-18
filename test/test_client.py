#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from collections import defaultdict
from hdfs.client import *
from hdfs.util import HdfsError, temppath
from util import _IntegrationTest
from nose.tools import eq_, nottest, ok_, raises
from requests.exceptions import ConnectTimeout, ReadTimeout
from shutil import rmtree
from six import b
from tempfile import mkdtemp
import os
import os.path as osp
import posixpath as psp


class TestLoad(object):

  """Test client loader."""

  def test_bare(self):
    client = Client.from_options({'url': 'foo'})
    ok_(isinstance(client, Client))

  def test_new_type(self):
    class NewClient(Client):
      def __init__(self, url, bar):
        super(NewClient, self).__init__(url)
        self.bar = bar
    client = Client.from_options({'url': 'bar', 'bar': 2}, 'NewClient')
    eq_(client.bar, 2)

  @raises(HdfsError)
  def test_missing_options(self):
    Client.from_options({}, 'KerberosClient')

  @raises(HdfsError)
  def test_invalid_options(self):
    Client.from_options({'foo': 123})

  @raises(HdfsError)
  def test_missing_type(self):
    Client.from_options({}, 'MissingClient')

  def test_timeout(self):
    eq_(Client('')._timeout, None)
    eq_(Client('', timeout=1)._timeout, 1)
    eq_(Client('', timeout=(1,2))._timeout, (1,2))
    eq_(Client.from_options({'url': ''})._timeout, None)


class TestOptions(_IntegrationTest):

  """Test client options."""

  def test_timeout(self):
    self.client._timeout = 1e-4 # Small enough for it to always timeout.
    try:
      self.client.status('.')
    except (ConnectTimeout, ReadTimeout):
      self.client._timeout = None
    else:
      raise HdfsError('No timeout.')


class TestApi(_IntegrationTest):

  """Test client raw API interactions."""

  def test_list_status_absolute_root(self):
    ok_(self.client._list_status('/'))

  def test_get_folder_status(self):
    self.client._mkdirs('foo')
    status = self.client._get_file_status('foo').json()['FileStatus']
    eq_(status['type'], 'DIRECTORY')

  def test_get_home_directory(self):
    path = self.client._get_home_directory('/').json()['Path']
    ok_('/user/' in path)

  def test_create_file(self):
    path = 'foo'
    self.client._create(path, data='hello')
    ok_(self._exists(path))

  def test_create_nested_file(self):
    path = 'foo/bar'
    self.client._create(path, data='hello')
    ok_(self._exists(path))

  def test_delete_file(self):
    path = 'bar'
    self.client._create(path, data='hello')
    ok_(self.client._delete(path).json()['boolean'])
    ok_(not self._exists(path))

  def test_delete_missing_file(self):
    path = 'bar2'
    ok_(not self.client._delete(path).json()['boolean'])

  def test_rename_file(self):
    paths = ['foo', '%s/bar' % (self.client.root.rstrip('/'), )]
    self.client._create(paths[0], data='hello')
    ok_(self.client._rename(paths[0], destination=paths[1]).json()['boolean'])
    ok_(not self._exists(paths[0]))
    eq_(self.client._open(paths[1].rsplit('/', 1)[1]).content, b'hello')
    self.client._delete(paths[1])

  def test_rename_file_to_existing(self):
    p = ['foo', '%s/bar' % (self.client.root.rstrip('/'), )]
    self.client._create(p[0], data='hello')
    self.client._create(p[1], data='hi')
    try:
      ok_(not self.client._rename(p[0], destination=p[1]).json()['boolean'])
    finally:
      self.client._delete(p[0])
      self.client._delete(p[1])

  def test_open_file(self):
    self.client._create('foo', data='hello')
    eq_(self.client._open('foo').content, b'hello')

  def test_get_file_checksum(self):
    self.client._create('foo', data='hello')
    data = self.client._get_file_checksum('foo').json()['FileChecksum']
    eq_(sorted(data), ['algorithm', 'bytes', 'length'])
    ok_(int(data['length']))

  @raises(HdfsError)
  def test_get_file_checksum_on_folder(self):
    self.client._get_file_checksum('')


class TestResolve(_IntegrationTest):

  def test_resolve_relative(self):
    eq_(Client('url', root='/').resolve('bar'), '/bar')
    eq_(Client('url', root='/foo').resolve('bar'), '/foo/bar')
    eq_(Client('url', root='/foo/').resolve('bar'), '/foo/bar')
    eq_(Client('url', root='/foo/').resolve('bar/'), '/foo/bar')
    eq_(Client('url', root='/foo/').resolve('/bar/'), '/bar')

  def test_resolve_relative_no_root(self):
    root = self.client.root
    try:
      self.client.root = None
      home = self.client._get_home_directory('/').json()['Path']
      eq_(self.client.resolve('bar'), psp.join(home, 'bar'))
      eq_(self.client.root, home)
    finally:
      self.client.root = root

  def test_resolve_relative_root(self):
    root = self.client.root
    try:
      self.client.root = 'bar'
      home = self.client._get_home_directory('/').json()['Path']
      eq_(self.client.resolve('foo'), psp.join(home, 'bar', 'foo'))
      eq_(self.client.root, psp.join(home, 'bar'))
    finally:
      self.client.root = root

  def test_resolve_absolute(self):
    eq_(Client('url').resolve('/bar'), '/bar')
    eq_(Client('url').resolve('/bar/foo/'), '/bar/foo')

  def test_create_file_with_percent(self):
    # `%` (`0x25`) is a special case because it seems to cause errors (even
    # though the action still goes through). Typical error message will be
    # `"Unknown exception in doAs"`.
    path = 'fo&o/a%a'
    try:
      self.client.write(path, data='hello')
    except HdfsError:
      pass
    with self.client.read(path) as reader:
      eq_(reader.read(), b'hello')


class TestWrite(_IntegrationTest):

  def test_create_from_string(self):
    self.client.write('up', u'hello, world!')
    eq_(self._read('up'), b'hello, world!')

  def test_create_from_string_with_encoding(self):
    self.client.write('up', u'hello, world!')
    eq_(self._read('up'), b'hello, world!')

  def test_create_from_generator(self):
    data = (e for e in [b'hello, ', b'world!'])
    self.client.write('up', data)
    eq_(self._read('up'), b'hello, world!')

  def test_create_from_generator_with_encoding(self):
    data = (e for e in [u'hello, ', u'world!'])
    self.client.write('up', data, encoding='utf-8')
    eq_(self._read('up'), b'hello, world!')

  def test_create_from_file_object(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      with open(tpath) as reader:
        self.client.write('up', reader)
    eq_(self._read('up'), b'hello, world!')

  def test_create_set_permission(self):
    self.client.write('up', 'hello, world!', permission='722')
    eq_(self._read('up'), b'hello, world!')
    eq_(self.client.status('up')['permission'], '722')

  @raises(HdfsError)
  def test_create_to_existing_file_without_overwrite(self):
    self.client.write('up', 'hello, world!')
    self.client.write('up', 'hello again, world!')

  def test_create_and_overwrite_file(self):
    self.client.write('up', 'hello, world!')
    self.client.write('up', 'hello again, world!', overwrite=True)
    eq_(self._read('up'), b'hello again, world!')

  def test_as_context_manager(self):
    with self.client.write('up') as writer:
      writer.write(b'hello, ')
      writer.write(b'world!')
    eq_(self._read('up'), b'hello, world!')

  def test_as_context_manager_with_encoding(self):
    with self.client.write('up', encoding='utf-8') as writer:
      writer.write(u'hello, ')
      writer.write(u'world!')
    eq_(self._read('up'), b'hello, world!')

  def test_dump_json(self):
    from json import dump, loads
    data = {'one': 1, 'two': 2}
    with self.client.write('up', encoding='utf-8') as writer:
      dump(data, writer)
    eq_(loads(self._read('up', encoding='utf-8')), data)

  @raises(HdfsError)
  def test_create_and_overwrite_directory(self):
    # can't overwrite a directory with a file
    self.client._mkdirs('up')
    self.client.write('up', b'hello, world!')

  @raises(HdfsError)
  def test_create_invalid_path(self):
    # conversely, can't overwrite a file with a directory
    self.client.write('up', 'hello, world!')
    self.client.write('up/up', 'hello again, world!')


class TestAppend(_IntegrationTest):

  @classmethod
  def setup_class(cls):
    super(TestAppend, cls).setup_class()
    if cls.client:
      try:
        cls.client.write('ap', '')
        # can't append to an empty file
        cls.client.write('ap', '', append=True)
        # try a simple append
      except HdfsError as err:
        if 'Append is not supported' in str(err):
          cls.client = None
          # skip these tests if HDFS isn't configured to support appends
        else:
          raise err

  def test_simple(self):
    self.client.write('ap', 'hello,')
    self.client.write('ap', ' world!', append=True)
    eq_(self._read('ap'), b'hello, world!')

  @raises(HdfsError)
  def test_missing_file(self):
    self.client.write('ap', 'hello!', append=True)

  @raises(ValueError)
  def test_overwrite_and_append(self):
    self.client.write('ap', 'hello!', overwrite=True, append=True)

  @raises(ValueError)
  def test_set_permission_and_append(self):
    self.client.write('ap', 'hello!', permission='777', append=True)


class TestUpload(_IntegrationTest):

  def test_upload_file(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      self.client.upload('up', tpath)
    eq_(self._read('up'), b'hello, world!')

  @raises(HdfsError)
  def test_upload_missing(self):
    with temppath() as tpath:
      self.client.upload('up', tpath)

  @raises(HdfsError)
  def test_upload_empty_directory(self):
    dpath = mkdtemp()
    try:
      self.client.upload('up', dpath)
    finally:
      os.rmdir(dpath)

  def test_upload_directory_to_existing_directory(self):
    dpath = mkdtemp()
    try:
      npath = osp.join(dpath, 'hi')
      os.mkdir(npath)
      with open(osp.join(npath, 'foo'), 'w') as writer:
        writer.write('hello!')
      os.mkdir(osp.join(npath, 'bar'))
      with open(osp.join(npath, 'bar', 'baz'), 'w') as writer:
        writer.write('world!')
      self.client._mkdirs('up')
      self.client.upload('up', npath)
      eq_(self._read('up/hi/foo'), b'hello!')
      eq_(self._read('up/hi/bar/baz'), b'world!')
    finally:
      rmtree(dpath)

  def test_upload_directory_to_missing(self):
    dpath = mkdtemp()
    try:
      with open(osp.join(dpath, 'foo'), 'w') as writer:
        writer.write('hello!')
      os.mkdir(osp.join(dpath, 'bar'))
      with open(osp.join(dpath, 'bar', 'baz'), 'w') as writer:
        writer.write('world!')
      self.client.upload('up', dpath)
      eq_(self._read('up/foo'), b'hello!')
      eq_(self._read('up/bar/baz'), b'world!')
    finally:
      rmtree(dpath)

  def test_upload_directory_overwrite_existing_file(self):
    dpath = mkdtemp()
    try:
      with open(osp.join(dpath, 'foo'), 'w') as writer:
        writer.write('hello!')
      os.mkdir(osp.join(dpath, 'bar'))
      with open(osp.join(dpath, 'bar', 'baz'), 'w') as writer:
        writer.write('world!')
      self.client.write('up', 'hi')
      self.client.upload('up', dpath, overwrite=True)
      eq_(self._read('up/foo'), b'hello!')
      eq_(self._read('up/bar/baz'), b'world!')
    finally:
      rmtree(dpath)

  def test_upload_overwrite(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello')
      self.client.upload('up', tpath)
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('there')
      self.client.upload('up', tpath, overwrite=True)
    eq_(self._read('up'), b'there')

  @raises(HdfsError)
  def test_upload_overwrite_error(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('here')
      self.client.upload('up', tpath)
      self.client.upload('up', tpath)

  def test_upload_cleanup(self):
    dpath = mkdtemp()
    _write = self.client.write

    def write(hdfs_path, *args, **kwargs):
      if 'bar' in hdfs_path:
        raise RuntimeError()
      return _write(hdfs_path, *args, **kwargs)

    try:
      self.client.write = write
      npath = osp.join(dpath, 'hi')
      os.mkdir(npath)
      with open(osp.join(npath, 'foo'), 'w') as writer:
        writer.write('hello!')
      os.mkdir(osp.join(npath, 'bar'))
      with open(osp.join(npath, 'bar', 'baz'), 'w') as writer:
        writer.write('world!')
      try:
        self.client.upload('foo', dpath)
      except RuntimeError:
        ok_(not self._exists('foo'))
      else:
        ok_(False) # This shouldn't happen.
    finally:
      rmtree(dpath)
      self.client.write = _write

  def test_upload_no_cleanup(self):
    dpath = mkdtemp()
    _write = self.client.write

    def write(hdfs_path, *args, **kwargs):
      if 'bar' in hdfs_path:
        raise RuntimeError()
      return _write(hdfs_path, *args, **kwargs)

    try:
      self.client.write = write
      npath = osp.join(dpath, 'hi')
      os.mkdir(npath)
      with open(osp.join(npath, 'foo'), 'w') as writer:
        writer.write('hello!')
      os.mkdir(osp.join(npath, 'bar'))
      with open(osp.join(npath, 'bar', 'baz'), 'w') as writer:
        writer.write('world!')
      try:
        self.client.upload('foo', dpath, cleanup=False)
      except RuntimeError:
        # The outer folder still exists.
        ok_(self._exists('foo'))
      else:
        ok_(False) # This shouldn't happen.
    finally:
      rmtree(dpath)
      self.client.write = _write

  def test_upload_with_progress(self):

    def callback(path, nbytes, history=defaultdict(list)):
      history[path].append(nbytes)
      return history

    dpath = mkdtemp()
    try:
      path1 = osp.join(dpath, 'foo')
      with open(path1, 'w') as writer:
        writer.write('hello!')
      os.mkdir(osp.join(dpath, 'bar'))
      path2 = osp.join(dpath, 'bar', 'baz')
      with open(path2, 'w') as writer:
        writer.write('the world!')
      self.client.upload(
        'up',
        dpath,
        chunk_size=4,
        n_threads=1, # Callback isn't thread-safe.
        progress=callback
      )
      eq_(self._read('up/foo'), b'hello!')
      eq_(self._read('up/bar/baz'), b'the world!')
      eq_(
        callback('', 0),
        {path1: [4, 6, -1], path2: [4, 8, 10, -1], '': [0]}
      )
    finally:
      rmtree(dpath)


class TestDelete(_IntegrationTest):

  def test_delete_file(self):
    self.client.write('foo', 'hello, world!')
    ok_(self.client.delete('foo'))
    ok_(not self._exists('foo'))

  def test_delete_empty_directory(self):
    self.client._mkdirs('foo')
    ok_(self.client.delete('foo'))
    ok_(not self._exists('foo'))

  def test_delete_missing_file(self):
    ok_(not self.client.delete('foo'))

  def test_delete_non_empty_directory(self):
    self.client.write('de/foo', 'hello, world!')
    ok_(self.client.delete('de', recursive=True))
    ok_(not self._exists('de'))

  @raises(HdfsError)
  def test_delete_non_empty_directory_without_recursive(self):
    self.client.write('de/foo', 'hello, world!')
    self.client.delete('de')


class TestRead(_IntegrationTest):

  @raises(ValueError)
  def test_progress_without_chunk_size(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo', progress=lambda path, nbytes: None) as reader:
      pass

  @raises(ValueError)
  def test_delimiter_without_encoding(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo', delimiter=',') as reader:
      pass

  @raises(ValueError)
  def test_delimiter_with_chunk_size(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo', delimiter=',', chunk_size=1) as reader:
      pass

  def test_read_file(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo') as reader:
      eq_(reader.read(), b'hello, world!')

  @raises(HdfsError)
  def test_read_directory(self):
    self.client._mkdirs('foo')
    with self.client.read('foo') as reader:
      pass

  @raises(HdfsError)
  def test_read_missing_file(self):
    with self.client.read('foo') as reader:
      pass

  def test_read_file_from_offset(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo', offset=7) as reader:
      eq_(reader.read(), b'world!')

  def test_read_file_from_offset_with_limit(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo', offset=7, length=5) as reader:
      eq_(reader.read(), b'world')

  def test_read_file_with_chunk_size(self):
    self.client.write('foo', 'hello, world!')
    with self.client.read('foo', chunk_size=5) as reader:
      eq_(list(reader), [b'hello', b', wor', b'ld!'])

  def test_with_progress(self):
    def cb(path, nbytes, chunk_lengths=[]):
      chunk_lengths.append(nbytes)
      return chunk_lengths
    self.client.write('foo', 'hello, world!')
    with temppath() as tpath:
      with open(tpath, 'wb') as writer:
        with self.client.read('foo', chunk_size=5, progress=cb) as reader:
          for chunk in reader:
            writer.write(chunk)
      with open(tpath, 'rb') as reader:
        eq_(reader.read(), b'hello, world!')
      eq_(cb('', 0), [5, 10, 13, -1, 0])

  def test_read_with_encoding(self):
    s = u'hello, world!'
    self.client.write('foo', s)
    with self.client.read('foo', encoding='utf-8') as reader:
      eq_(reader.read(), s)

  def test_read_with_chunk_size_and_encoding(self):
    s = u'hello, world!'
    self.client.write('foo', s)
    with self.client.read('foo', chunk_size=5, encoding='utf-8') as reader:
      eq_(list(reader), [u'hello', u', wor', u'ld!'])

  def test_read_json(self):
    from json import dumps, load
    data = {'one': 1, 'two': 2}
    self.client.write('foo', data=dumps(data), encoding='utf-8')
    with self.client.read('foo', encoding='utf-8') as reader:
      eq_(load(reader), data)

  def test_read_with_delimiter(self):
    self.client.write('foo', u'hi\nworld!\n')
    with self.client.read('foo', delimiter='\n', encoding='utf-8') as reader:
      eq_(list(reader), [u'hi', u'world!', u''])


class TestRename(_IntegrationTest):

  def test_rename_file(self):
    self.client.write('foo', 'hello, world!')
    self.client.rename('foo', 'bar')
    eq_(self._read('bar'), b'hello, world!')

  @raises(HdfsError)
  def test_rename_missing_file(self):
    self.client.rename('foo', 'bar')

  @raises(HdfsError)
  def test_rename_file_to_existing_file(self):
    self.client.write('foo', 'hello, world!')
    self.client.write('bar', 'hello again, world!')
    self.client.rename('foo', 'bar')

  def test_move_file_into_existing_directory(self):
    self.client.write('foo', 'hello, world!')
    self.client._mkdirs('bar')
    self.client.rename('foo', 'bar')
    eq_(self._read('bar/foo'), b'hello, world!')

  def test_rename_file_into_existing_directory(self):
    self.client.write('foo', 'hello, world!')
    self.client._mkdirs('bar')
    self.client.rename('foo', 'bar/baz')
    eq_(self._read('bar/baz'), b'hello, world!')

  def test_rename_file_with_special_characters(self):
    path = 'fo&oa ?a=1'
    self.client.write('foo', 'hello, world!')
    self.client.rename('foo', path)
    eq_(self._read(path), b'hello, world!')


class TestDownload(_IntegrationTest):

  @raises(HdfsError)
  def test_missing_dir(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      self.client.download('dl', osp.join(tpath, 'foo'))

  def test_normal_file(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      fpath = self.client.download('dl', tpath)
      with open(fpath) as reader:
        eq_(reader.read(), 'hello')

  def test_nonpartitioned_file(self):
    partname = 'part-r-00000'
    self.client.write('dl/' + partname, 'world')
    with temppath() as tpath:
      fname = self.client.download('dl/' + partname, tpath)
      with open(fname) as reader:
        eq_(reader.read(), 'world')

  def test_singly_partitioned_file(self):
    partname = 'part-r-00000'
    self.client.write('dl/' + partname, 'world')
    with temppath() as tpath:
      os.mkdir(tpath)
      fname = self.client.download('dl', tpath)
      with open(osp.join(fname, partname)) as reader:
        eq_(reader.read(), 'world')

  def _download_partitioned_file(self, n_threads):
    parts = {
      'part-r-00000': 'fee',
      'part-r-00001': 'faa',
      'part-r-00002': 'foo',
    }
    for name, content in parts.items():
      self.client.write('dl/%s' % (name, ), content)
    with temppath() as tpath:
      self.client.download('dl', tpath, n_threads=-1)
      local_parts = os.listdir(tpath)
      eq_(set(local_parts), set(parts)) # We have all the parts.
      for part in local_parts:
        with open(osp.join(tpath, part)) as reader:
          eq_(reader.read(), parts[part]) # Their content is correct.

  def test_partitioned_file_max_threads(self):
    self._download_partitioned_file(0)

  def test_partitioned_file_sync(self):
    self._download_partitioned_file(1)

  def test_partitioned_file_setting_n_threads(self):
    self._download_partitioned_file(2)

  def test_overwrite_file(self):
    with temppath() as tpath:
      self.client.write('dl', 'hello')
      self.client.download('dl', tpath)
      self.client.write('dl', 'there', overwrite=True)
      fname = self.client.download('dl', tpath, overwrite=True)
      with open(fname) as reader:
        eq_(reader.read(), 'there')

  @raises(HdfsError)
  def test_download_file_to_existing_file(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hi')
      self.client.download('dl', tpath)

  def test_download_file_to_existing_file_with_overwrite(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hi')
      self.client.download('dl', tpath, overwrite=True)
      with open(tpath) as reader:
        eq_(reader.read(), 'hello')

  def test_download_file_to_existing_folder(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      os.mkdir(tpath)
      self.client.download('dl', tpath)
      with open(osp.join(tpath, 'dl')) as reader:
        eq_(reader.read(), 'hello')

  @raises(HdfsError)
  def test_download_file_to_existing_folder_with_matching_file(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      os.mkdir(tpath)
      with open(osp.join(tpath, 'dl'), 'w') as writer:
        writer.write('hey')
      self.client.download('dl', tpath)

  def test_download_file_to_existing_folder_overwrite_matching_file(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      os.mkdir(tpath)
      with open(osp.join(tpath, 'dl'), 'w') as writer:
        writer.write('hey')
      self.client.download('dl', tpath, overwrite=True)
      with open(osp.join(tpath, 'dl')) as reader:
        eq_(reader.read(), 'hello')

  def test_download_folder_to_existing_folder(self):
    self.client.write('foo/dl', 'hello')
    self.client.write('foo/bar/dl', 'there')
    with temppath() as tpath:
      os.mkdir(tpath)
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'foo', 'dl')) as reader:
        eq_(reader.read(), 'hello')
      with open(osp.join(tpath, 'foo', 'bar', 'dl')) as reader:
        eq_(reader.read(), 'there')

  def test_download_folder_to_existing_folder_parallel(self):
    self.client.write('foo/dl', 'hello')
    self.client.write('foo/bar/dl', 'there')
    with temppath() as tpath:
      os.mkdir(tpath)
      self.client.download('foo', tpath, n_threads=0)
      with open(osp.join(tpath, 'foo', 'dl')) as reader:
        eq_(reader.read(), 'hello')
      with open(osp.join(tpath, 'foo', 'bar', 'dl')) as reader:
        eq_(reader.read(), 'there')

  def test_download_folder_to_missing_folder(self):
    self.client.write('foo/dl', 'hello')
    self.client.write('foo/bar/dl', 'there')
    with temppath() as tpath:
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'dl')) as reader:
        eq_(reader.read(), 'hello')
      with open(osp.join(tpath, 'bar', 'dl')) as reader:
        eq_(reader.read(), 'there')

  def test_download_cleanup(self):
    self.client.write('foo/dl', 'hello')
    self.client.write('foo/bar/dl', 'there')
    _read = self.client.read

    def read(hdfs_path, *args, **kwargs):
      if 'bar' in hdfs_path:
        raise RuntimeError()
      return _read(hdfs_path, *args, **kwargs)

    with temppath() as tpath:
      try:
        self.client.read = read
        self.client.download('foo', tpath)
      except RuntimeError:
        ok_(not osp.exists(tpath))
      else:
        ok_(False) # This shouldn't happen.
      finally:
        self.client.read = _read

  @raises(HdfsError)
  def test_download_empty_folder(self):
    self.client._mkdirs('foo')
    with temppath() as tpath:
      self.client.download('foo', tpath)

  def test_download_dir_whitespace(self):
    self.client.write('foo/foo bar.txt', 'hello')
    with temppath() as tpath:
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'foo bar.txt')) as reader:
        eq_(reader.read(), 'hello')

  def test_download_file_whitespace(self):
    self.client.write('foo/foo bar%.txt', 'hello')
    with temppath() as tpath:
      self.client.download('foo/foo bar%.txt', tpath)
      with open(tpath) as reader:
        eq_(reader.read(), 'hello')


class TestStatus(_IntegrationTest):

  def test_directory(self):
    self.client._mkdirs('foo')
    status = self.client.status('foo')
    eq_(status['type'], 'DIRECTORY')
    eq_(status['length'], 0)

  def test_file(self):
    self.client.write('foo', 'hello, world!')
    status = self.client.status('foo')
    eq_(status['type'], 'FILE')
    eq_(status['length'], 13)

  @raises(HdfsError)
  def test_missing(self):
    self.client.status('foo')

  def test_missing_non_strict(self):
    ok_(self.client.status('foo', strict=False) is None)


class TestSetOwner(_IntegrationTest):

  @classmethod
  def setup_class(cls):
    super(TestSetOwner, cls).setup_class()
    if cls.client:
      try:
        cls.client.write('foo', '')
        cls.client.set_owner('foo', 'bar')
      except HdfsError as err:
        if 'Non-super user cannot change owner' in str(err):
          cls.client = None
          # Skip these tests if HDFS isn't configured to support them.
        else:
          raise err

  def test_directory_owner(self):
    new_owner = 'newowner'
    self.client._mkdirs('foo')
    self.client.set_owner('foo', 'oldowner')
    self.client.set_owner('foo', new_owner)
    status = self.client.status('foo')
    eq_(status['owner'], new_owner)

  def test_file_owner(self):
    new_owner = 'newowner'
    self.client.write('foo', 'hello, world!')
    self.client.set_owner('foo', 'oldowner')
    self.client.set_owner('foo', new_owner)
    status = self.client.status('foo')
    eq_(status['owner'], new_owner)

  def test_directory_for_group(self):
    new_group = 'newgroup'
    self.client._mkdirs('foo')
    self.client.set_owner('foo', group='oldgroup')
    self.client.set_owner('foo', group=new_group)
    status = self.client.status('foo')
    eq_(status['group'], new_group)

  def test_file_for_group(self):
    new_group = 'newgroup'
    self.client.write('foo', 'hello, world!')
    self.client.set_owner('foo', group='oldgroup')
    self.client.set_owner('foo', group=new_group)
    status = self.client.status('foo')
    eq_(status['group'], new_group)

  @raises(HdfsError)
  def test_missing_for_group(self):
    self.client.set_owner('foo', group='blah')


class TestSetPermission(_IntegrationTest):

  def test_directory(self):
    new_permission = '755'
    self.client._mkdirs('foo', permission='444')
    self.client.set_permission('foo', new_permission)
    status = self.client.status('foo')
    eq_(status['permission'], new_permission)

  def test_file(self):
    new_permission = '755'
    self.client.write('foo', 'hello, world!', permission='444')
    self.client.set_permission('foo', new_permission)
    status = self.client.status('foo')
    eq_(status['permission'], new_permission)

  @raises(HdfsError)
  def test_missing(self):
    self.client.set_permission('foo', '755')


class TestContent(_IntegrationTest):

  def test_directory(self):
    self.client.write('foo', 'hello, world!')
    content = self.client.content('')
    eq_(content['directoryCount'], 1)
    eq_(content['fileCount'], 1)
    eq_(content['length'], 13)

  def test_file(self):
    self.client.write('foo', 'hello, world!')
    content = self.client.content('foo')
    eq_(content['directoryCount'], 0)
    eq_(content['fileCount'], 1)
    eq_(content['length'], 13)

  @raises(HdfsError)
  def test_missing(self):
    self.client.content('foo')

  def test_missing_non_strict(self):
    ok_(self.client.content('foo', strict=False) is None)


class TestList(_IntegrationTest):

  @raises(HdfsError)
  def test_file(self):
    self.client.write('foo', 'hello, world!')
    self.client.list('foo')

  @raises(HdfsError)
  def test_missing(self):
    self.client.list('foo')

  def test_empty_dir(self):
    self.client._mkdirs('foo')
    eq_(self.client.list('foo'), [])

  def test_dir(self):
    self.client.write('foo/bar', 'hello, world!')
    eq_(self.client.list('foo'), ['bar'])

  def test_dir_with_status(self):
    self.client.write('foo/bar', 'hello, world!')
    statuses = self.client.list('foo', status=True)
    eq_(len(statuses), 1)
    status = self.client.status('foo/bar')
    status['pathSuffix'] = 'bar'
    eq_(statuses[0], ('bar', status))


class TestWalk(_IntegrationTest):

  @raises(HdfsError)
  def test_missing(self):
    list(self.client.walk('foo'))

  def test_file(self):
    self.client.write('foo', 'hello, world!')
    ok_(not list(self.client.walk('foo')))

  def test_folder(self):
    self.client.write('hello', 'hello, world!')
    self.client.write('foo/hey', 'hey, world!')
    infos = list(self.client.walk(''))
    eq_(len(infos), 2)
    eq_(infos[0], (psp.join(self.client.root), ['foo'], ['hello']))
    eq_(infos[1], (psp.join(self.client.root, 'foo'), [], ['hey']))

  def test_folder_with_depth(self):
    self.client.write('foo/bar', 'hello, world!')
    infos = list(self.client.walk('', depth=1))
    eq_(len(infos), 1)
    eq_(infos[0], (self.client.root, ['foo'], []))

  def test_folder_with_status(self):
    self.client.write('foo', 'hello, world!')
    infos = list(self.client.walk('', status=True))
    status = self.client.status('foo')
    status['pathSuffix'] = 'foo'
    eq_(len(infos), 1)
    eq_(
      infos[0],
      (
        (self.client.root, self.client.status('')),
        [],
        [('foo', status)]
      )
    )


class TestLatestExpansion(_IntegrationTest):

  def test_resolve_simple(self):
    self.client.write('bar', 'hello, world!')
    self.client.write('foo', 'hello again, world!')
    eq_(self.client.resolve('#LATEST'), osp.join(self.client.root, 'foo'))

  def test_resolve_nested(self):
    self.client.write('baz/bar', 'hello, world!')
    self.client.write('bar/bar', 'hello there, world!')
    self.client.write('bar/foo', 'hello again, world!')
    latest = self.client.resolve('#LATEST/#LATEST')
    eq_(latest, osp.join(self.client.root, 'bar', 'foo'))

  def test_resolve_multiple(self):
    self.client.write('bar/bar', 'hello, world!')
    self.client.write('bar/foo', 'hello again, world!')
    latest = self.client.resolve('#LATEST/#LATEST')
    eq_(latest, osp.join(self.client.root, 'bar', 'foo'))

  def test_resolve_multiple_shortcut(self):
    self.client.write('bar/bar', 'hello, world!')
    self.client.write('bar/foo', 'hello again, world!')
    latest = self.client.resolve('#LATEST{2}')
    eq_(latest, osp.join(self.client.root, 'bar', 'foo'))

  @nottest # HttpFS is inconsistent here.
  @raises(HdfsError)
  def test_resolve_file(self):
    self.client.write('bar', 'hello, world!')
    self.client.resolve('bar/#LATEST')

  @raises(HdfsError)
  def test_resolve_empty_directory(self):
    self.client._mkdirs('bar')
    self.client.resolve('bar/#LATEST')


class TestParts(_IntegrationTest):

  @raises(HdfsError)
  def test_missing(self):
    self.client.parts('foo')

  @raises(HdfsError)
  def test_file(self):
    self.client.write('foo', 'hello')
    self.client.parts('foo')

  @raises(HdfsError)
  def test_empty_folder(self):
    self.client._mkdirs('foo')
    self.client.parts('foo')

  @raises(HdfsError)
  def test_folder_without_parts(self):
    self.client.write('foo/bar', 'hello')
    self.client.parts('foo')

  def test_folder_with_single_part(self):
    fname = 'part-m-00000.avro'
    self.client.write(psp.join('foo', fname), 'first')
    eq_(self.client.parts('foo'), [fname])

  def test_folder_with_multiple_parts(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    eq_(self.client.parts('foo'), fnames)

  def test_folder_with_multiple_parts_and_others(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', '.header'), 'metadata')
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    eq_(self.client.parts('foo'), fnames)

  def test_with_selection(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', '.header'), 'metadata')
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    parts = self.client.parts('foo', parts=1)
    eq_(len(parts), 1)
    ok_(parts[0] in fnames)

  def test_with_selection(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', '.header'), 'metadata')
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    eq_(self.client.parts('foo', parts=[1]), fnames[1:])

  def test_with_status(self):
    fname = 'part-m-00000.avro'
    fpath = psp.join('foo', fname)
    self.client.write(fpath, 'first')
    status = self.client.status(fpath)
    status['pathSuffix'] = fname
    eq_(self.client.parts('foo', status=True), [(fname, status)])


class TestMakeDirs(_IntegrationTest):

  def test_simple(self):
    self.client.makedirs('foo')
    eq_(self.client.status('foo')['type'], 'DIRECTORY')

  def test_nested(self):
    self.client.makedirs('foo/bar')
    eq_(self.client.status('foo/bar')['type'], 'DIRECTORY')

  def test_with_permission(self):
    self.client.makedirs('foo', permission='733')
    eq_(self.client.status('foo')['permission'], '733')

  @raises(HdfsError)
  def test_overwrite_file(self):
    self.client.write('foo', 'hello')
    self.client.makedirs('foo')

  def test_overwrite_directory_with_permission(self):
    self.client.makedirs('foo', permission='733')
    self.client.makedirs('foo/bar', permission='722')
    eq_(self.client.status('foo')['permission'], '733')
    eq_(self.client.status('foo/bar')['permission'], '722')


class TestSetTimes(_IntegrationTest):

  @raises(ValueError)
  def test_none(self):
    self.client.makedirs('foo')
    self.client.set_times('foo')

  @raises(HdfsError)
  def test_missing(self):
    self.client.set_times('foo', 1234)

  @nottest # HttpFS doesn't raise an error here.
  @raises(HdfsError)
  def test_negative(self):
    self.client.write('foo', 'hello')
    self.client.set_times('foo', access_time=-1234)

  def test_file(self):
    self.client.write('foo', 'hello')
    self.client.set_times('foo', access_time=1234)
    eq_(self.client.status('foo')['accessTime'], 1234)
    self.client.set_times('foo', modification_time=12345)
    eq_(self.client.status('foo')['modificationTime'], 12345)
    self.client.set_times('foo', access_time=1, modification_time=2)
    status = self.client.status('foo')
    eq_(status['accessTime'], 1)
    eq_(status['modificationTime'], 2)

  def test_folder(self):
    self.client.write('foo/bar', 'hello')
    self.client.set_times('foo', access_time=1234)
    eq_(self.client.status('foo')['accessTime'], 1234)
    self.client.set_times('foo', modification_time=12345)
    eq_(self.client.status('foo')['modificationTime'], 12345)
    self.client.set_times('foo', access_time=1, modification_time=2)
    status = self.client.status('foo')
    eq_(status['accessTime'], 1)
    eq_(status['modificationTime'], 2)


class TestChecksum(_IntegrationTest):

  @raises(HdfsError)
  def test_missing(self):
    self.client.checksum('foo')

  @raises(HdfsError)
  def test_folder(self):
    self.client.makedirs('foo')
    self.client.checksum('foo')

  def test_file(self):
    self.client.write('foo', 'hello')
    checksum = self.client.checksum('foo')
    eq_(set(['algorithm', 'bytes', 'length']), set(checksum))


class TestSetReplication(_IntegrationTest):

  @raises(HdfsError)
  def test_missing(self):
    self.client.set_replication('foo', 1)

  @raises(HdfsError)
  def test_folder(self):
    self.client.makedirs('foo')
    self.client.set_replication('foo', 1)

  @raises(HdfsError)
  def test_invalid_replication(self):
    self.client.write('foo', 'hello')
    self.client.set_replication('foo', 0)

  def test_file(self):
    self.client.write('foo', 'hello')
    replication = self.client.status('foo')['replication'] + 1
    self.client.set_replication('foo', replication)
    eq_(self.client.status('foo')['replication'], replication)


class TestTokenClient(object):

  def test_without_session(self):
    client = TokenClient('url', '123')
    eq_(client._session.params['delegation'], '123')

  def test_with_session(self):
    session = rq.Session()
    client = TokenClient('url', '123', session=session)
    eq_(session.params['delegation'], '123')
