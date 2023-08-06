#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from collections import defaultdict
from hdfs.client import *
from hdfs.util import HdfsError, temppath
from test.util import _IntegrationTest
from requests.exceptions import ConnectTimeout, ReadTimeout
from shutil import rmtree
from six import b
from tempfile import mkdtemp
import os
import os.path as osp
import posixpath as psp
import pytest


class TestLoad(object):

  """Test client loader."""

  def test_bare(self):
    client = Client.from_options({'url': 'foo'})
    assert isinstance(client, Client)

  def test_new_type(self):
    class NewClient(Client):
      def __init__(self, url, bar):
        super(NewClient, self).__init__(url)
        self.bar = bar
    client = Client.from_options({'url': 'bar', 'bar': 2}, 'NewClient')
    assert client.bar == 2

  def test_missing_options(self):
    with pytest.raises(HdfsError):
      Client.from_options({}, 'KerberosClient')

  def test_invalid_options(self):
    with pytest.raises(HdfsError):
      Client.from_options({'foo': 123})

  def test_missing_type(self):
    with pytest.raises(HdfsError):
      Client.from_options({}, 'MissingClient')

  def test_timeout(self):
    assert Client('')._timeout == None
    assert Client('', timeout=1)._timeout == 1
    assert Client('', timeout=(1,2))._timeout == (1,2)
    assert Client.from_options({'url': ''})._timeout == None


class TestOptions(_IntegrationTest):

  """Test client options."""

  @pytest.mark.skip(reason="TODO: Investigate why this fails in Python 3.7 and 3.9")
  def test_timeout(self):
    with pytest.raises(ConnectTimeout, ReadTimeout):
      self.client._timeout = 1e-6 # Small enough for it to always timeout.
      try:
        self.client.status('.')
      finally:
        self.client._timeout = None


class TestApi(_IntegrationTest):

  """Test client raw API interactions."""

  def test_list_status_absolute_root(self):
    assert self.client._list_status('/')

  def test_get_folder_status(self):
    self.client._mkdirs('foo')
    status = self.client._get_file_status('foo').json()['FileStatus']
    assert status['type'] == 'DIRECTORY'

  def test_get_home_directory(self):
    path = self.client._get_home_directory('/').json()['Path']
    assert '/user/' in path

  def test_delete_file(self):
    path = 'bar'
    self._write(path, b'hello')
    assert self.client._delete(path).json()['boolean']
    assert not self._exists(path)

  def test_delete_missing_file(self):
    path = 'bar2'
    assert not self.client._delete(path).json()['boolean']

  def test_rename_file(self):
    paths = ['foo', '{}/bar'.format(self.client.root.rstrip('/'))]
    self._write(paths[0], b'hello')
    assert self.client._rename(paths[0], destination=paths[1]).json()['boolean']
    assert not self._exists(paths[0])
    assert self.client._open(paths[1].rsplit('/', 1)[1]).content == b'hello'
    self.client._delete(paths[1])

  def test_rename_file_to_existing(self):
    p = ['foo', '{}/bar'.format(self.client.root.rstrip('/'))]
    self._write(p[0], b'hello')
    self._write(p[1], b'hi')
    try:
      assert not self.client._rename(p[0], destination=p[1]).json()['boolean']
    finally:
      self.client._delete(p[0])
      self.client._delete(p[1])

  def test_open_file(self):
    self._write('foo', b'hello')
    assert self.client._open('foo').content == b'hello'

  def test_get_file_checksum(self):
    self._write('foo', b'hello')
    data = self.client._get_file_checksum('foo').json()['FileChecksum']
    assert sorted(data) == ['algorithm', 'bytes', 'length']
    assert int(data['length'])

  def test_get_file_checksum_on_folder(self):
    with pytest.raises(HdfsError):
      self.client._get_file_checksum('')


class TestResolve(_IntegrationTest):

  def test_resolve_relative(self):
    assert Client('url', root='/').resolve('bar') == '/bar'
    assert Client('url', root='/foo').resolve('bar') == '/foo/bar'
    assert Client('url', root='/foo/').resolve('bar') == '/foo/bar'
    assert Client('url', root='/foo/').resolve('bar/') == '/foo/bar'
    assert Client('url', root='/foo/').resolve('/bar/') == '/bar'

  def test_resolve_relative_no_root(self):
    root = self.client.root
    try:
      self.client.root = None
      home = self.client._get_home_directory('/').json()['Path']
      assert self.client.resolve('bar') == psp.join(home, 'bar')
      assert self.client.root == home
    finally:
      self.client.root = root

  def test_resolve_relative_root(self):
    root = self.client.root
    try:
      self.client.root = 'bar'
      home = self.client._get_home_directory('/').json()['Path']
      assert self.client.resolve('foo') == psp.join(home, 'bar', 'foo')
      assert self.client.root == psp.join(home, 'bar')
    finally:
      self.client.root = root

  def test_resolve_absolute(self):
    assert Client('url').resolve('/bar') == '/bar'
    assert Client('url').resolve('/bar/foo/') == '/bar/foo'

  def test_create_file_with_percent(self):
    # `%` (`0x25`) is a special case because it seems to cause errors (even
    # though the action still goes through). Typical error message will be
    # `"Unknown exception in doAs"`.
    path = 'fo&o/a%a'
    try:
      self._write(path, b'hello')
    except HdfsError:
      pass
    assert self._read(path) == b'hello'


class TestWrite(_IntegrationTest):

  def test_create_from_string(self):
    self.client.write('up', b'hello, world!')
    assert self._read('up') == b'hello, world!'

  def test_create_from_string_with_encoding(self):
    self.client.write('up', u'hello, world!', encoding='utf-8')
    assert self._read('up') == b'hello, world!'

  def test_create_from_generator(self):
    data = (e for e in [b'hello, ', b'world!'])
    self.client.write('up', data)
    assert self._read('up') == b'hello, world!'

  def test_create_from_generator_with_encoding(self):
    data = (e for e in [u'hello, ', u'world!'])
    self.client.write('up', data, encoding='utf-8')
    assert self._read('up') == b'hello, world!'

  def test_create_from_file_object(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      with open(tpath) as reader:
        self.client.write('up', reader)
    assert self._read('up') == b'hello, world!'

  def test_create_set_permission(self):
    self.client.write('up', b'hello, world!', permission='722')
    assert self._read('up') == b'hello, world!'
    assert self.client.status('up')['permission'] == '722'

  def test_create_to_existing_file_without_overwrite(self):
    with pytest.raises(HdfsError):
      self.client.write('up', b'hello, world!')
      self.client.write('up', b'hello again, world!')

  def test_create_and_overwrite_file(self):
    self.client.write('up', b'hello, world!')
    self.client.write('up', b'hello again, world!', overwrite=True)
    assert self._read('up') == b'hello again, world!'

  def test_as_context_manager(self):
    with self.client.write('up') as writer:
      writer.write(b'hello, ')
      writer.write(b'world!')
    assert self._read('up') == b'hello, world!'

  def test_as_context_manager_with_encoding(self):
    with self.client.write('up', encoding='utf-8') as writer:
      writer.write(u'hello, ')
      writer.write(u'world!')
    assert self._read('up') == b'hello, world!'

  def test_dump_json(self):
    from json import dump, loads
    data = {'one': 1, 'two': 2}
    with self.client.write('up', encoding='utf-8') as writer:
      dump(data, writer)
    assert loads(self._read('up', encoding='utf-8')) == data

  def test_create_and_overwrite_directory(self):
    with pytest.raises(HdfsError):
      # can't overwrite a directory with a file
      self.client._mkdirs('up')
      self.client.write('up', b'hello, world!')

  def test_create_invalid_path(self):
    with pytest.raises(HdfsError):
      # conversely, can't overwrite a file with a directory
      self.client.write('up', b'hello, world!')
      self.client.write('up/up', b'hello again, world!')


class TestAppend(_IntegrationTest):

  @classmethod
  def setup_class(cls):
    super(TestAppend, cls).setup_class()
    if cls.client:
      try:
        cls.client.write('ap', b'') # We can't append to an empty file.
        cls.client.write('ap', b'', append=True) # Try a simple append.
      except HdfsError as err:
        if 'Append is not supported' in str(err):
          cls.client = None
          # Skip these tests if HDFS isn't configured to support appends.
        else:
          raise err

  def test_simple(self):
    self.client.write('ap', b'hello,')
    self.client.write('ap', b' world!', append=True)
    assert self._read('ap') == b'hello, world!'

  def test_missing_file(self):
    with pytest.raises(HdfsError):
      self.client.write('ap', b'hello!', append=True)

  def test_overwrite_and_append(self):
    with pytest.raises(ValueError):
      self.client.write('ap', b'hello!', overwrite=True, append=True)

  def test_set_permission_and_append(self):
    with pytest.raises(ValueError):
      self.client.write('ap', b'hello!', permission='777', append=True)


class TestUpload(_IntegrationTest):

  def test_upload_file(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      self.client.upload('up', tpath)
    assert self._read('up') == b'hello, world!'

  def test_upload_missing(self):
    with pytest.raises(HdfsError):
      with temppath() as tpath:
        self.client.upload('up', tpath)

  def test_upload_empty_directory(self):
    with pytest.raises(HdfsError):
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
      assert self._read('up/hi/foo') == b'hello!'
      assert self._read('up/hi/bar/baz') == b'world!'
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
      assert self._read('up/foo') == b'hello!'
      assert self._read('up/bar/baz') == b'world!'
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
      self._write('up', b'hi')
      self.client.upload('up', dpath, overwrite=True)
      assert self._read('up/foo') == b'hello!'
      assert self._read('up/bar/baz') == b'world!'
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
    assert self._read('up') == b'there'

  def test_upload_overwrite_error(self):
    with pytest.raises(HdfsError):
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
        assert not self._exists('foo')
      else:
        assert False # This shouldn't happen.
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
        assert self._exists('foo')
      else:
        assert False # This shouldn't happen.
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
      assert self._read('up/foo') == b'hello!'
      assert self._read('up/bar/baz') == b'the world!'
      assert (
        callback('', 0) ==
        {path1: [4, 6, -1], path2: [4, 8, 10, -1], '': [0]})
    finally:
      rmtree(dpath)


class TestDelete(_IntegrationTest):

  def test_delete_file(self):
    self._write('foo', b'hello, world!')
    assert self.client.delete('foo')
    assert not self._exists('foo')

  def test_delete_empty_directory(self):
    self.client._mkdirs('foo')
    assert self.client.delete('foo')
    assert not self._exists('foo')

  def test_delete_missing_file(self):
    assert not self.client.delete('foo')

  def test_delete_non_empty_directory(self):
    self._write('de/foo', b'hello, world!')
    assert self.client.delete('de', recursive=True)
    assert not self._exists('de')

  def test_delete_non_empty_directory_without_recursive(self):
    with pytest.raises(HdfsError):
      self._write('de/foo', b'hello, world!')
      self.client.delete('de')

  def test_trash_file(self):
    self._write('foo', b'hello, world!')
    assert self.client.delete('foo', skip_trash=False)
    assert self.client.status('foo', strict=False) == None

  def test_trash_missing_file(self):
    assert not self.client.delete('foo', skip_trash=False)

  def test_trash_directory_non_recursive(self):
    with pytest.raises(HdfsError):
      self._write('bar/foo', b'hello, world!')
      self.client.delete('bar', skip_trash=False)

  def test_trash_directory(self):
    self._write('bar/foo', b'hello, world!')
    assert self.client.delete('bar', recursive=True, skip_trash=False)
    assert self.client.status('bar', strict=False) == None


class TestRead(_IntegrationTest):

  def test_progress_without_chunk_size(self):
    with pytest.raises(ValueError):
      self._write('foo', b'hello, world!')
      with self.client.read('foo', progress=lambda path, nbytes: None) as reader:
        pass

  def test_delimiter_without_encoding(self):
    with pytest.raises(ValueError):
      self._write('foo', b'hello, world!')
      with self.client.read('foo', delimiter=',') as reader:
        pass

  def test_delimiter_with_chunk_size(self):
    with pytest.raises(ValueError):
      self._write('foo', b'hello, world!')
      with self.client.read('foo', delimiter=',', chunk_size=1) as reader:
        pass

  def test_read_file(self):
    self._write('foo', b'hello, world!')
    with self.client.read('foo') as reader:
      assert reader.read() == b'hello, world!'

  def test_read_directory(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      with self.client.read('foo') as reader:
        pass

  def test_read_missing_file(self):
    with pytest.raises(HdfsError):
      with self.client.read('foo') as reader:
        pass

  def test_read_file_from_offset(self):
    self._write('foo', b'hello, world!')
    with self.client.read('foo', offset=7) as reader:
      assert reader.read() == b'world!'

  def test_read_file_from_offset_with_limit(self):
    self._write('foo', b'hello, world!')
    with self.client.read('foo', offset=7, length=5) as reader:
      assert reader.read() == b'world'

  def test_read_file_with_chunk_size(self):
    self._write('foo', b'hello, world!')
    with self.client.read('foo', chunk_size=5) as reader:
      assert list(reader) == [b'hello', b', wor', b'ld!']

  def test_with_progress(self):
    def cb(path, nbytes, chunk_lengths=[]):
      chunk_lengths.append(nbytes)
      return chunk_lengths
    self._write('foo', b'hello, world!')
    with temppath() as tpath:
      with open(tpath, 'wb') as writer:
        with self.client.read('foo', chunk_size=5, progress=cb) as reader:
          for chunk in reader:
            writer.write(chunk)
      with open(tpath, 'rb') as reader:
        assert reader.read() == b'hello, world!'
      assert cb('', 0) == [5, 10, 13, -1, 0]

  def test_read_with_encoding(self):
    s = u'hello, world!'
    self._write('foo', s, encoding='utf-8')
    with self.client.read('foo', encoding='utf-8') as reader:
      assert reader.read() == s

  def test_read_with_chunk_size_and_encoding(self):
    s = u'hello, world!'
    self._write('foo', s, encoding='utf-8')
    with self.client.read('foo', chunk_size=5, encoding='utf-8') as reader:
      assert list(reader) == [u'hello', u', wor', u'ld!']

  def test_read_json(self):
    from json import dumps, load
    data = {'one': 1, 'two': 2}
    self._write('foo', data=dumps(data), encoding='utf-8')
    with self.client.read('foo', encoding='utf-8') as reader:
      assert load(reader) == data

  def test_read_with_delimiter(self):
    self._write('foo', u'hi\nworld!\n', encoding='utf-8')
    with self.client.read('foo', delimiter='\n', encoding='utf-8') as reader:
      assert list(reader) == [u'hi', u'world!', u'']


class TestRename(_IntegrationTest):

  def test_rename_file(self):
    self._write('foo', b'hello, world!')
    self.client.rename('foo', 'bar')
    assert self._read('bar') == b'hello, world!'

  def test_rename_missing_file(self):
    with pytest.raises(HdfsError):
      self.client.rename('foo', 'bar')

  def test_rename_file_to_existing_file(self):
    with pytest.raises(HdfsError):
      self._write('foo', b'hello, world!')
      self._write('bar', b'hello again, world!')
      self.client.rename('foo', 'bar')

  def test_move_file_into_existing_directory(self):
    self._write('foo', b'hello, world!')
    self.client._mkdirs('bar')
    self.client.rename('foo', 'bar')
    assert self._read('bar/foo') == b'hello, world!'

  def test_rename_file_into_existing_directory(self):
    self._write('foo', b'hello, world!')
    self.client._mkdirs('bar')
    self.client.rename('foo', 'bar/baz')
    assert self._read('bar/baz') == b'hello, world!'

  def test_rename_file_with_special_characters(self):
    path = 'fo&oa ?a=1'
    self._write('foo', b'hello, world!')
    self.client.rename('foo', path)
    assert self._read(path) == b'hello, world!'


class TestDownload(_IntegrationTest):

  def test_missing_dir(self):
    with pytest.raises(HdfsError):
      self._write('dl', b'hello')
      with temppath() as tpath:
        self.client.download('dl', osp.join(tpath, 'foo'))

  def test_normal_file(self):
    self._write('dl', b'hello')
    with temppath() as tpath:
      fpath = self.client.download('dl', tpath)
      with open(fpath) as reader:
        assert reader.read() == 'hello'

  def test_nonpartitioned_file(self):
    partname = 'part-r-00000'
    self._write('dl/' + partname, b'world')
    with temppath() as tpath:
      fname = self.client.download('dl/' + partname, tpath)
      with open(fname) as reader:
        assert reader.read() == 'world'

  def test_singly_partitioned_file(self):
    partname = 'part-r-00000'
    self._write('dl/' + partname, b'world')
    with temppath() as tpath:
      os.mkdir(tpath)
      fname = self.client.download('dl', tpath)
      with open(osp.join(fname, partname)) as reader:
        assert reader.read() == 'world'

  def _download_partitioned_file(self, n_threads):
    parts = {
      'part-r-00000': b'fee',
      'part-r-00001': b'faa',
      'part-r-00002': b'foo',
    }
    for name, content in parts.items():
      self._write('dl/{}'.format(name), content)
    with temppath() as tpath:
      self.client.download('dl', tpath, n_threads=-1)
      local_parts = os.listdir(tpath)
      assert set(local_parts) == set(parts) # We have all the parts.
      for part in local_parts:
        with open(osp.join(tpath, part), mode='rb') as reader:
          assert reader.read() == parts[part] # Their content is correct.

  def test_partitioned_file_max_threads(self):
    self._download_partitioned_file(0)

  def test_partitioned_file_sync(self):
    self._download_partitioned_file(1)

  def test_partitioned_file_setting_n_threads(self):
    self._download_partitioned_file(2)

  def test_overwrite_file(self):
    with temppath() as tpath:
      self._write('dl', b'hello')
      self.client.download('dl', tpath)
      self.client.write('dl', b'there', overwrite=True)
      fname = self.client.download('dl', tpath, overwrite=True)
      with open(fname) as reader:
        assert reader.read() == 'there'

  def test_download_file_to_existing_file(self):
    with pytest.raises(HdfsError):
      self._write('dl', b'hello')
      with temppath() as tpath:
        with open(tpath, 'w') as writer:
          writer.write('hi')
        self.client.download('dl', tpath)

  def test_download_file_to_existing_file_with_overwrite(self):
    self._write('dl', b'hello')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hi')
      self.client.download('dl', tpath, overwrite=True)
      with open(tpath) as reader:
        assert reader.read() == 'hello'

  def test_download_file_to_existing_folder(self):
    self._write('dl', b'hello')
    with temppath() as tpath:
      os.mkdir(tpath)
      self.client.download('dl', tpath)
      with open(osp.join(tpath, 'dl')) as reader:
        assert reader.read() == 'hello'

  def test_download_file_to_existing_folder_with_matching_file(self):
    with pytest.raises(HdfsError):
      self._write('dl', b'hello')
      with temppath() as tpath:
        os.mkdir(tpath)
        with open(osp.join(tpath, 'dl'), 'w') as writer:
          writer.write('hey')
        self.client.download('dl', tpath)

  def test_download_file_to_existing_folder_overwrite_matching_file(self):
    self._write('dl', b'hello')
    with temppath() as tpath:
      os.mkdir(tpath)
      with open(osp.join(tpath, 'dl'), 'w') as writer:
        writer.write('hey')
      self.client.download('dl', tpath, overwrite=True)
      with open(osp.join(tpath, 'dl')) as reader:
        assert reader.read() == 'hello'

  def test_download_folder_to_existing_folder(self):
    self._write('foo/dl', b'hello')
    self._write('foo/bar/dl', b'there')
    with temppath() as tpath:
      os.mkdir(tpath)
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'foo', 'dl')) as reader:
        assert reader.read() == 'hello'
      with open(osp.join(tpath, 'foo', 'bar', 'dl')) as reader:
        assert reader.read() == 'there'

  def test_download_folder_to_existing_folder_parallel(self):
    self._write('foo/dl', b'hello')
    self._write('foo/bar/dl', b'there')
    with temppath() as tpath:
      os.mkdir(tpath)
      self.client.download('foo', tpath, n_threads=0)
      with open(osp.join(tpath, 'foo', 'dl')) as reader:
        assert reader.read() == 'hello'
      with open(osp.join(tpath, 'foo', 'bar', 'dl')) as reader:
        assert reader.read() == 'there'

  def test_download_folder_to_missing_folder(self):
    self._write('foo/dl', b'hello')
    self._write('foo/bar/dl', b'there')
    with temppath() as tpath:
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'dl')) as reader:
        assert reader.read() == 'hello'
      with open(osp.join(tpath, 'bar', 'dl')) as reader:
        assert reader.read() == 'there'

  def test_download_cleanup(self):
    self._write('foo/dl', b'hello')
    self._write('foo/bar/dl', b'there')
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
        assert not osp.exists(tpath)
      else:
        assert False # This shouldn't happen.
      finally:
        self.client.read = _read

  def test_download_empty_folder(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      with temppath() as tpath:
        self.client.download('foo', tpath)

  def test_download_dir_whitespace(self):
    self._write('foo/foo bar.txt', b'hello')
    with temppath() as tpath:
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'foo bar.txt')) as reader:
        assert reader.read() == 'hello'

  def test_download_file_whitespace(self):
    self._write('foo/foo bar%.txt', b'hello')
    with temppath() as tpath:
      self.client.download('foo/foo bar%.txt', tpath)
      with open(tpath) as reader:
        assert reader.read() == 'hello'


class TestStatus(_IntegrationTest):

  def test_directory(self):
    self.client._mkdirs('foo')
    status = self.client.status('foo')
    assert status['type'] == 'DIRECTORY'
    assert status['length'] == 0

  def test_file(self):
    self._write('foo', b'hello, world!')
    status = self.client.status('foo')
    assert status['type'] == 'FILE'
    assert status['length'] == 13

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.status('foo')

  def test_missing_non_strict(self):
    assert self.client.status('foo', strict=False) is None


class TestSetOwner(_IntegrationTest):

  @classmethod
  def setup_class(cls):
    super(TestSetOwner, cls).setup_class()
    if cls.client:
      try:
        cls.client.write('foo', b'')
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
    assert status['owner'] == new_owner

  def test_file_owner(self):
    new_owner = 'newowner'
    self._write('foo', b'hello, world!')
    self.client.set_owner('foo', 'oldowner')
    self.client.set_owner('foo', new_owner)
    status = self.client.status('foo')
    assert status['owner'] == new_owner

  def test_directory_for_group(self):
    new_group = 'newgroup'
    self.client._mkdirs('foo')
    self.client.set_owner('foo', group='oldgroup')
    self.client.set_owner('foo', group=new_group)
    status = self.client.status('foo')
    assert status['group'] == new_group

  def test_file_for_group(self):
    new_group = 'newgroup'
    self._write('foo', b'hello, world!')
    self.client.set_owner('foo', group='oldgroup')
    self.client.set_owner('foo', group=new_group)
    status = self.client.status('foo')
    assert status['group'] == new_group

  def test_missing_for_group(self):
    with pytest.raises(HdfsError):
      self.client.set_owner('foo', group='blah')


class TestSetPermission(_IntegrationTest):

  def test_directory(self):
    new_permission = '755'
    self.client._mkdirs('foo', permission='444')
    self.client.set_permission('foo', new_permission)
    status = self.client.status('foo')
    assert status['permission'] == new_permission

  def test_file(self):
    new_permission = '755'
    self.client.write('foo', b'hello, world!', permission='444')
    self.client.set_permission('foo', new_permission)
    status = self.client.status('foo')
    assert status['permission'] == new_permission

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.set_permission('foo', '755')


class TestContent(_IntegrationTest):

  def test_directory(self):
    self._write('foo', b'hello, world!')
    content = self.client.content('')
    assert content['directoryCount'] == 1
    assert content['fileCount'] == 1
    assert content['length'] == 13

  def test_file(self):
    self._write('foo', b'hello, world!')
    content = self.client.content('foo')
    assert content['directoryCount'] == 0
    assert content['fileCount'] == 1
    assert content['length'] == 13

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.content('foo')

  def test_missing_non_strict(self):
    assert self.client.content('foo', strict=False) is None


class TestAcl(_IntegrationTest):

  def test_directory(self):
    self._write('foo', b'hello, world!')
    content = self.client.acl_status('')
    assert len(content) > 1
    assert 'entries' in content
    assert 'group' in content
    assert 'owner' in content

  def test_set_acl(self):
    self.client.write('foo', 'hello, world!')
    self.client.set_acl('foo', 'user::rwx,user:foouser:rwx,group::r--,other::---')
    content = self.client.acl_status('foo')
    assert any('user:foouser:rwx' in s for s in content['entries'])
    assert len(content) > 1
    assert content['entries'] is not None

  def test_modify_acl(self):
    self.client.write('foo', 'hello, world!')
    self.client.set_acl('foo', 'user::rwx,user:foouser:rwx,group::r--,other::---')
    self.client.set_acl('foo', 'user:foouser:rw-', clear=False)
    content = self.client.acl_status('foo')
    assert any('user:foouser:rw-' in s for s in content['entries'])

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.acl_status('foo')

  def test_missing_non_strict(self):
    assert self.client.acl_status('foo', strict=False) is None

  def test_remove_acl_entries(self):
    self.client.write('foo', 'hello, world!')
    self.client.set_acl('foo', 'user:baruser:rwx,user:foouser:rw-', clear=False)
    self.client.remove_acl_entries('foo', 'user:foouser:')
    content = self.client.acl_status('foo')
    assert not any('user:foouser:rw-' in s for s in content['entries'])
    assert any('user:baruser:rwx' in s for s in content['entries'])

  def test_remove_default_acl(self):
    self.client.write('foo', 'hello, world!')
    self.client.set_acl('foo', 'user:foouser:rwx', clear=False)
    self.client.remove_default_acl('foo')
    content = self.client.acl_status('foo')
    assert not any('user::rwx' in s for s in content['entries'])

  def test_remove_acl(self):
    self.client.write('foo', 'hello, world!')
    self.client.remove_acl('foo')
    content = self.client.acl_status('foo')
    assert content.get('entries') == []


class TestList(_IntegrationTest):

  def test_file(self):
    with pytest.raises(HdfsError):
      self.client.write('foo', 'hello, world!')
      self.client.list('foo')

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.list('foo')

  def test_empty_dir(self):
    self.client._mkdirs('foo')
    assert self.client.list('foo') == []

  def test_dir(self):
    self.client.write('foo/bar', 'hello, world!')
    assert self.client.list('foo') == ['bar']

  def test_dir_with_status(self):
    self.client.write('foo/bar', 'hello, world!')
    statuses = self.client.list('foo', status=True)
    assert len(statuses) == 1
    status = self.client.status('foo/bar')
    status['pathSuffix'] = 'bar'
    assert statuses[0] == ('bar', status)


class TestWalk(_IntegrationTest):

  def test_missing(self):
    with pytest.raises(HdfsError):
      list(self.client.walk('foo'))

  def test_file(self):
    self.client.write('foo', 'hello, world!')
    assert not list(self.client.walk('foo'))

  def test_folder(self):
    self.client.write('hello', 'hello, world!')
    self.client.write('foo/hey', 'hey, world!')
    infos = list(self.client.walk(''))
    assert len(infos) == 2
    assert infos[0] == (psp.join(self.client.root), ['foo'], ['hello'])
    assert infos[1] == (psp.join(self.client.root, 'foo'), [], ['hey'])

  def test_folder_with_depth(self):
    self.client.write('foo/bar', 'hello, world!')
    infos = list(self.client.walk('', depth=1))
    assert len(infos) == 1
    assert infos[0] == (self.client.root, ['foo'], [])

  def test_folder_with_status(self):
    self.client.write('foo', 'hello, world!')
    infos = list(self.client.walk('', status=True))
    status = self.client.status('foo')
    status['pathSuffix'] = 'foo'
    assert len(infos) == 1
    assert (
      infos[0] ==
      (
        (self.client.root, self.client.status('')),
        [],
        [('foo', status)]
      ))

  def test_skip_missing_folder(self):
    self.client.write('file', 'one')
    self.client.write('folder/hey', 'two')
    for info in self.client.walk('', ignore_missing=True):
      assert info == (psp.join(self.client.root), ['folder'], ['file'])
      self.client.delete('folder', recursive=True)

  def test_status_and_allow_dir_changes(self):
    with pytest.raises(ValueError):
      list(self.client.walk('.', status=True, allow_dir_changes=True))

  def test_allow_dir_changes_subset(self):
    self.client.write('foo/file1', 'one')
    self.client.write('bar/file2', 'two')
    infos = self.client.walk('.', allow_dir_changes=True)
    info = next(infos)
    info[1][:] = ['bar']
    info = next(infos)
    assert info == (psp.join(self.client.root, 'bar'), [], ['file2'])

  def test_allow_dir_changes_insert(self):
    self.client.write('foo/file1', 'one')
    infos = self.client.walk('.', allow_dir_changes=True)
    info = next(infos)
    self.client.write('bar/file2', 'two')
    info[1][:] = ['bar'] # Insert new directory.
    info = next(infos)
    assert info == (psp.join(self.client.root, 'bar'), [], ['file2'])


class TestLatestExpansion(_IntegrationTest):

  def test_resolve_simple(self):
    self.client.write('bar', 'hello, world!')
    self.client.write('foo', 'hello again, world!')
    assert self.client.resolve('#LATEST') == osp.join(self.client.root, 'foo')

  def test_resolve_nested(self):
    self.client.write('baz/bar', 'hello, world!')
    self.client.write('bar/bar', 'hello there, world!')
    self.client.write('bar/foo', 'hello again, world!')
    latest = self.client.resolve('#LATEST/#LATEST')
    assert latest == osp.join(self.client.root, 'bar', 'foo')

  def test_resolve_multiple(self):
    self.client.write('bar/bar', 'hello, world!')
    self.client.write('bar/foo', 'hello again, world!')
    latest = self.client.resolve('#LATEST/#LATEST')
    assert latest == osp.join(self.client.root, 'bar', 'foo')

  def test_resolve_multiple_shortcut(self):
    self.client.write('bar/bar', 'hello, world!')
    self.client.write('bar/foo', 'hello again, world!')
    latest = self.client.resolve('#LATEST{2}')
    assert latest == osp.join(self.client.root, 'bar', 'foo')

  @pytest.mark.skip(reason="HttpFS is inconsistent here.")
  def test_resolve_file(self):
    with pytest.raises(HdfsError):
      self.client.write('bar', 'hello, world!')
      self.client.resolve('bar/#LATEST')

  def test_resolve_empty_directory(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('bar')
      self.client.resolve('bar/#LATEST')


class TestParts(_IntegrationTest):

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.parts('foo')

  def test_file(self):
    with pytest.raises(HdfsError):
      self.client.write('foo', 'hello')
      self.client.parts('foo')

  def test_empty_folder(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      self.client.parts('foo')

  def test_folder_without_parts(self):
    with pytest.raises(HdfsError):
      self.client.write('foo/bar', 'hello')
      self.client.parts('foo')

  def test_folder_with_single_part(self):
    fname = 'part-m-00000.avro'
    self.client.write(psp.join('foo', fname), 'first')
    assert self.client.parts('foo') == [fname]

  def test_folder_with_multiple_parts(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    assert self.client.parts('foo') == fnames

  def test_folder_with_multiple_parts_and_others(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', '.header'), 'metadata')
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    assert self.client.parts('foo') == fnames

  def test_with_selection(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', '.header'), 'metadata')
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    parts = self.client.parts('foo', parts=1)
    assert len(parts) == 1
    assert parts[0] in fnames

  def test_with_selection(self):
    fnames = ['part-m-00000.avro', 'part-m-00001.avro']
    self.client.write(psp.join('foo', '.header'), 'metadata')
    self.client.write(psp.join('foo', fnames[0]), 'first')
    self.client.write(psp.join('foo', fnames[1]), 'second')
    assert self.client.parts('foo', parts=[1]) == fnames[1:]

  def test_with_status(self):
    fname = 'part-m-00000.avro'
    fpath = psp.join('foo', fname)
    self.client.write(fpath, 'first')
    status = self.client.status(fpath)
    status['pathSuffix'] = fname
    assert self.client.parts('foo', status=True) == [(fname, status)]


class TestMakeDirs(_IntegrationTest):

  def test_simple(self):
    self.client.makedirs('foo')
    assert self.client.status('foo')['type'] == 'DIRECTORY'

  def test_nested(self):
    self.client.makedirs('foo/bar')
    assert self.client.status('foo/bar')['type'] == 'DIRECTORY'

  def test_with_permission(self):
    self.client.makedirs('foo', permission='733')
    assert self.client.status('foo')['permission'] == '733'

  def test_overwrite_file(self):
    with pytest.raises(HdfsError):
      self.client.write('foo', 'hello')
      self.client.makedirs('foo')

  def test_overwrite_directory_with_permission(self):
    self.client.makedirs('foo', permission='733')
    self.client.makedirs('foo/bar', permission='722')
    assert self.client.status('foo')['permission'] == '733'
    assert self.client.status('foo/bar')['permission'] == '722'


class TestSetTimes(_IntegrationTest):

  def test_none(self):
    with pytest.raises(ValueError):
      self.client.makedirs('foo')
      self.client.set_times('foo')

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.set_times('foo', 1234)

  @pytest.mark.skip() # HttpFS doesn't raise an error here.
  def test_negative(self):
    with pytest.raises(HdfsError):
      self.client.write('foo', 'hello')
      self.client.set_times('foo', access_time=-1234)

  def test_file(self):
    self.client.write('foo', 'hello')
    self.client.set_times('foo', access_time=1234)
    assert self.client.status('foo')['accessTime'] == 1234
    self.client.set_times('foo', modification_time=12345)
    assert self.client.status('foo')['modificationTime'] == 12345
    self.client.set_times('foo', access_time=1, modification_time=2)
    status = self.client.status('foo')
    assert status['accessTime'] == 1
    assert status['modificationTime'] == 2

  def test_folder(self):
    self.client.write('foo/bar', 'hello')
    self.client.set_times('foo', access_time=1234)
    assert self.client.status('foo')['accessTime'] == 1234
    self.client.set_times('foo', modification_time=12345)
    assert self.client.status('foo')['modificationTime'] == 12345
    self.client.set_times('foo', access_time=1, modification_time=2)
    status = self.client.status('foo')
    assert status['accessTime'] == 1
    assert status['modificationTime'] == 2


class TestChecksum(_IntegrationTest):

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.checksum('foo')

  def test_folder(self):
    with pytest.raises(HdfsError):
      self.client.makedirs('foo')
      self.client.checksum('foo')

  def test_file(self):
    self.client.write('foo', 'hello')
    checksum = self.client.checksum('foo')
    assert {'algorithm', 'bytes', 'length'} == set(checksum)


class TestSetReplication(_IntegrationTest):

  def test_missing(self):
    with pytest.raises(HdfsError):
      self.client.set_replication('foo', 1)

  def test_folder(self):
    with pytest.raises(HdfsError):
      self.client.makedirs('foo')
      self.client.set_replication('foo', 1)

  def test_invalid_replication(self):
    with pytest.raises(HdfsError):
      self.client.write('foo', 'hello')
      self.client.set_replication('foo', 0)

  def test_file(self):
    self.client.write('foo', 'hello')
    replication = self.client.status('foo')['replication'] + 1
    self.client.set_replication('foo', replication)
    assert self.client.status('foo')['replication'] == replication


class TestTokenClient(object):

  def test_without_session(self):
    client = TokenClient('url', '123')
    assert client._session.params['delegation'] == '123'

  def test_with_session(self):
    session = rq.Session()
    client = TokenClient('url', '123', session=session)
    assert session.params['delegation'] == '123'


class TestSnapshot(_IntegrationTest):

  @classmethod
  def setup_class(cls):
    super(TestSnapshot, cls).setup_class()
    if cls.client:
      cls.client._mkdirs('foo')
      try:
        cls.client.allow_snapshot('foo')
      except HdfsError as err:
        if 'java.lang.IllegalArgumentException: No enum constant' in str(err):
          cls.client = None
          # Skip these tests if we get this error message from HDFS (currently
          # happens using HTTPFS) which causes all snapshot operations to fail.
        else:
          raise err

  def test_allow_snapshot(self):
    self.client._mkdirs('foo')
    self.client.allow_snapshot('foo')

  def test_allow_snapshot_double(self):
    self.client._mkdirs('foo')
    self.client.allow_snapshot('foo')
    self.client.allow_snapshot('foo')

  def test_disallow_snapshot(self):
    self.client._mkdirs('foo')
    self.client.allow_snapshot('foo')
    self.client.disallow_snapshot('foo')

  def test_disallow_no_allow(self):
    self.client._mkdirs('foo')
    self.client.disallow_snapshot('foo')

  def test_allow_snapshot_not_exists(self):
    with pytest.raises(HdfsError):
      self.client.allow_snapshot('foo')

  def test_disallow_snapshot_not_exists(self):
    with pytest.raises(HdfsError):
      self.client.disallow_snapshot('foo')

  def test_allow_snapshot_file(self):
    with pytest.raises(HdfsError):
      self._write('foo', b'hello')
      self.client.allow_snapshot('foo')

  def test_disallow_snapshot_file(self):
    with pytest.raises(HdfsError):
      self._write('foo', b'hello')
      self.client.disallow_snapshot('foo')

  def test_create_delete_snapshot(self):
    # One cannot test creation and deletion separately, as one cannot
    # clean HDFS for test isolation if a created snapshot remains
    # undeleted.
    self.client._mkdirs('foo')
    self.client.allow_snapshot('foo')
    self.client.create_snapshot('foo', 'mysnap')
    self.client.delete_snapshot('foo', 'mysnap')

  def test_create_snapshot_name(self):
    self.client._mkdirs('foo')
    self.client.allow_snapshot('foo')
    try:
      snapshot_path = self.client.create_snapshot('foo', 'mysnap')
      assert re.search(r'/foo/\.snapshot/mysnap$',snapshot_path)
    finally:
      # Cleanup, as it breaks other tests otherwise: the dir cannot be
      # removed with an active snapshots.
      self.client.delete_snapshot('foo', 'mysnap')

  def test_delete_snapshot_other(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      self.client.allow_snapshot('foo')
      self.client.create_snapshot('foo', 'mysnap')
      try:
        self.client.delete_snapshot('foo', 'othersnap')
      finally:
        # Cleanup, as it breaks other tests otherwise: the dir cannot be
        # removed with an active snapshots.
        self.client.delete_snapshot('foo', 'mysnap')

  def test_disallow_snapshot_exists(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo_disallow')
      self.client.allow_snapshot('foo_disallow')
      self.client.create_snapshot('foo_disallow', 'mysnap')
      try:
        self.client.disallow_snapshot('foo_disallow')
      finally:
        # Cleanup, as it breaks other tests otherwise: the dir cannot be
        # removed with an active snapshots.
        self.client.delete_snapshot('foo_disallow', 'mysnap')

  def test_create_snapshot_noallow(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      self.client.create_snapshot('foo', 'mysnap')

  def test_delete_snapshot_noallow(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      self.client.delete_snapshot('foo', 'mysnap')

  def test_create_snapshot_noexist(self):
    with pytest.raises(HdfsError):
     self.client.create_snapshot('foo', 'mysnap')

  def test_rename_snapshot(self):
    self.client._mkdirs('foo')
    self.client.allow_snapshot('foo')
    self.client.create_snapshot('foo', 'myspan')
    try:
      self.client.rename_snapshot('foo', 'myspan', 'yourspan')
    finally:
      self.client.delete_snapshot('foo', 'yourspan')

  def test_rename_snapshot_not_exists(self):
    with pytest.raises(HdfsError):
      self.client.rename_snapshot('foo', 'myspan', 'yourspan')

  def test_rename_snapshot_not_overwrite(self):
    with pytest.raises(HdfsError):
      self.client._mkdirs('foo')
      self.client.allow_snapshot('foo')
      self.client.create_snapshot('foo', 'myspan')
      self.client.create_snapshot('foo', 'yourspan')
      try:
        self.client.rename_snapshot('foo', 'myspan', 'yourspan')
      finally:
        self.client.delete_snapshot('foo', 'myspan')
        self.client.delete_snapshot('foo', 'yourspan')
