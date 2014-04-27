#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import Config, temppath
from hdfs.client import *
from ConfigParser import NoOptionError, NoSectionError
from getpass import getuser
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from os import mkdir, rmdir
from os.path import join
from shutil import rmtree
from tempfile import mkdtemp
from time import sleep


def status(response):
  """Helper for requests that return boolean JSON responses."""
  return response.json()['boolean']


class TestLoad(object):

  """Test client loader."""

  def test_bare(self):
    client = Client._from_options(None, {'url': 'foo'})
    ok_(isinstance(client, Client))

  def test_kerberos(self):
    client = Client._from_options('KerberosClient', {'url': 'foo'})
    ok_(isinstance(client, KerberosClient))

  def test_new_type(self):
    class NewClient(Client):
      def __init__(self, url, bar):
        super(NewClient, self).__init__(url)
        self.bar = bar
    client = Client._from_options('NewClient', {'url': 123, 'bar': 2})
    eq_(client.bar, 2)

  @raises(HdfsError)
  def test_missing_options(self):
    client = Client._from_options('KerberosClient', {})

  @raises(HdfsError)
  def test_invalid_options(self):
    client = Client._from_options(None, {'foo': 123})

  @raises(HdfsError)
  def test_missing_type(self):
    client = Client._from_options('foo', {})


class _TestSession(object):

  """Base class to run tests using remote HDFS.

  These tests are run only if a `test.alias` is defined in the `~/.hdfsrc`
  configuration file.

  .. warning::

    The test directory is entirely cleaned during tests. Don't put anything
    important in it!

  Also contains a few helper functions.

  """

  delay = 1 # delay in seconds between tests

  @classmethod
  def setup_class(cls):
    try:
      client = Client.from_alias('test')
      client._delete('', recursive=True)
    except (NoOptionError, NoSectionError, HdfsError):
      cls.client = None
    else:
      cls.client = client

  def setup(self):
    if not self.client:
      raise SkipTest
    else:
      self.client._mkdirs('')

  def teardown(self):
    if self.client:
      self.client._delete('', recursive=True)
    sleep(self.delay)

  def _check_content(self, path, content):
    eq_(self.client._open(path).content, content)

  def _file_exists(self, path):
    try:
      status = self.client.status(path)
    except HdfsError:
      # head doesn't exist
      return False
    else:
      return True


class TestApi(_TestSession):

  """Test client raw API interactions."""

  def test_list_status_absolute_root(self):
    ok_(self.client._list_status('/'))

  def test_list_status_test_root(self):
    eq_(
      self.client._list_status('').content,
      self.client._list_status(self.client.root).content,
    )

  def test_get_file_status(self):
    status = self.client._get_file_status('').json()['FileStatus']
    eq_(status['type'], 'DIRECTORY')

  def test_get_home_directory(self):
    path = self.client._get_home_directory('').json()['Path']
    ok_('/user/' in path)

  def test_create_file(self):
    path = 'foo'
    self.client._create(path, data='hello')
    ok_(self._file_exists(path))

  def test_delete_file(self):
    path = 'bar'
    self.client._create(path, data='hello')
    ok_(status(self.client._delete(path)))
    ok_(not self._file_exists(path))

  def test_delete_missing_file(self):
    path = 'bar2'
    ok_(not status(self.client._delete(path)))

  def test_rename_file(self):
    paths = ['foo', '%s/bar' % (self.client.root.rstrip('/'), )]
    self.client._create(paths[0], data='hello')
    ok_(status(self.client._rename(paths[0], destination=paths[1])))
    ok_(not self._file_exists(paths[0]))
    eq_(self.client._open(paths[1].rsplit('/', 1)[1]).content, 'hello')
    self.client._delete(paths[1])

  def test_rename_file_to_existing(self):
    paths = ['foo', '%s/bar' % (self.client.root.rstrip('/'), )]
    self.client._create(paths[0], data='hello')
    self.client._create(paths[1], data='hi')
    try:
      ok_(not status(self.client._rename(paths[0], destination=paths[1])))
    finally:
      self.client._delete(paths[0])
      self.client._delete(paths[1])

  def test_open_file(self):
    self.client._create('foo', data='hello')
    eq_(self.client._open('foo').content, 'hello')

  def test_get_file_checksum(self):
    self.client._create('foo', data='hello')
    data = self.client._get_file_checksum('foo').json()['FileChecksum']
    eq_(sorted(data), ['algorithm', 'bytes', 'length'])
    ok_(int(data['length']))

  @raises(HdfsError)
  def test_get_file_checksum_on_folder(self):
    self.client._get_file_checksum('')


class TestWrite(_TestSession):

  def test_create_from_string(self):
    self.client.write('up', 'hello, world!')
    self._check_content('up', 'hello, world!')

  def test_create_from_generator(self):
    data = (e for e in ['hello, ', 'world!'])
    self.client.write('up', data)
    self._check_content('up', 'hello, world!')

  def test_create_from_file_object(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      with open(tpath) as reader:
        self.client.write('up', reader)
    self._check_content('up', 'hello, world!')

  def test_create_set_permissions(self):
    pass # TODO

  @raises(HdfsError)
  def test_create_to_existing_file_without_overwrite(self):
    self.client.write('up', 'hello, world!')
    self.client.write('up', 'hello again, world!')

  def test_create_and_overwrite_file(self):
    self.client.write('up', 'hello, world!')
    self.client.write('up', 'hello again, world!', overwrite=True)
    self._check_content('up', 'hello again, world!')

  @raises(HdfsError)
  def test_create_and_overwrite_directory(self):
    # can't overwrite a directory with a file
    self.client._mkdirs('up')
    self.client.write('up', 'hello, world!')

  @raises(HdfsError)
  def test_create_invalid_path(self):
    # conversely, can't overwrite a file with a directory
    self.client.write('up', 'hello, world!')
    self.client.write('up/up', 'hello again, world!')


class TestUpload(_TestSession):

  def test_upload_file(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      self.client.upload('up', tpath)
    self._check_content('up', 'hello, world!')

  @raises(HdfsError)
  def test_upload_directory(self):
    tdpath = mkdtemp()
    try:
      self.client.upload('up', tdpath)
    finally:
      rmdir(tdpath)


class TestDelete(_TestSession):

  def test_delete_file(self):
    self.client.write('foo', 'hello, world!')
    self.client.delete('foo')

  def test_delete_empty_directory(self):
    self.client._mkdirs('foo')
    self.client.delete('foo')

  @raises(HdfsError)
  def test_delete_missing_file(self):
    self.client.delete('foo')

  def test_delete_non_empty_directory(self):
    self.client.write('de/foo', 'hello, world!')
    self.client.delete('de', recursive=True)

  @raises(HdfsError)
  def test_delete_non_empty_directory_without_recursive(self):
    self.client.write('de/foo', 'hello, world!')
    self.client.delete('de')


class TestRead(_TestSession):

  def _read(self, writer, *args, **kwargs):
    for chunk in self.client.read(*args, **kwargs):
      writer.write(chunk)

  def test_read_file(self):
    self.client.write('foo', 'hello, world!')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        self._read(writer, 'foo')
      with open(tpath) as reader:
        eq_(reader.read(), 'hello, world!')

  @raises(HdfsError)
  def test_read_directory(self):
    self.client._mkdirs('foo')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        self._read(writer, 'foo')

  @raises(HdfsError)
  def test_read_missing_file(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        self._read(writer, 'foo')

  def test_read_file_from_offset(self):
    self.client.write('foo', 'hello, world!')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        self._read(writer, 'foo', offset=7)
      with open(tpath) as reader:
        eq_(reader.read(), 'world!')

  def test_read_file_from_offset_with_limit(self):
    self.client.write('foo', 'hello, world!')
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        self._read(writer, 'foo', offset=7, length=5)
      with open(tpath) as reader:
        eq_(reader.read(), 'world')


class TestRename(_TestSession):

  def test_rename_file(self):
    self.client.write('foo', 'hello, world!')
    self.client.rename('foo', 'bar')
    self._check_content('bar', 'hello, world!')

  @raises(HdfsError)
  def test_rename_missing_file(self):
    self.client.rename('foo', 'bar')

  @raises(HdfsError)
  def test_rename_file_to_existing_file(self):
    self.client.write('foo', 'hello, world!')
    self.client.write('bar', 'hello again, world!')
    self.client.rename('foo', 'bar')

  def test_rename_file_into_existing_directory(self):
    self.client.write('foo', 'hello, world!')
    self.client._mkdirs('bar')
    self.client.rename('foo', 'bar')
    self._check_content('bar/foo', 'hello, world!')


class TestStatus(_TestSession):

  def test_directory(self):
    status = self.client.status('')
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


class TestContent(_TestSession):

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


class TestWalk(_TestSession):

  def test_file(self):
    self.client.write('foo', 'hello, world!')
    infos = list(self.client.walk('foo'))
    status = self.client.status('foo')
    eq_(len(infos), 1)
    eq_(infos[0], (osp.join(self.client.root, 'foo'), status))

  def test_file_with_depth(self):
    self.client.write('foo', 'hello, world!')
    infos = list(self.client.walk('foo', depth=48))
    status = self.client.status('foo')
    eq_(len(infos), 1)
    eq_(infos[0], (osp.join(self.client.root, 'foo'), status))

  def test_dir_without_depth(self):
    self.client.write('bar/foo', 'hello, world!')
    infos = list(self.client.walk('bar', depth=0))
    status = self.client.status('bar')
    eq_(len(infos), 1)
    eq_(infos[0], (osp.join(self.client.root, 'bar'), status))

  def test_dir_with_depth(self):
    self.client.write('bar/foo', 'hello, world!')
    self.client.write('bar/baz', 'hello again, world!')
    self.client.write('bar/bax/foo', 'hello yet again, world!')
    infos = list(self.client.walk('bar', depth=1))
    eq_(len(infos), 4)
