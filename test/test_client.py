#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import Config, temppath
from hdfs.client import *
from hdfs.__main__ import load_client
from ConfigParser import NoOptionError, NoSectionError
from getpass import getuser
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from time import sleep


def status(response):
  """Helper for requests that return boolean JSON responses."""
  return response.json()['boolean']


class TestLoad(object):

  """Test client loaders."""

  def test_load_insecure_client(self):
    options = {'url': 'foo', 'root': '/'}
    client = InsecureClient.from_config(options)
    eq_(client.url, options['url'])
    eq_(client.root, options['root'])
    eq_(client.params, {'user.name': getuser()})

  def test_load_kerberos_client(self):
    options = {'url': 'foo', 'proxy': 'bar', 'root': '/'}
    client = KerberosClient.from_config(options)
    eq_(client.url, options['url'])
    eq_(client.root, options['root'])

  def test_load_token_client(self):
    options = {'url': 'foo', 'token': 'abc', 'root': '/'}
    client = TokenClient.from_config(options)
    eq_(client.url, options['url'])
    eq_(client.root, options['root'])
    eq_(client.params, {'delegation': options['token']})


class _TestSession(object):

  """Base class to run tests using remote HDFS.

  These tests are run only if a `test.alias` is defined in the `~/.hdfsrc`
  configuration file.

  .. warning::

    The test directory is entirely cleaned during tests. Don't put anything
    important in it!

  """

  delay = 1 # delay in seconds between tests

  @classmethod
  def setup_class(cls):
    config = Config()
    try:
      client = load_client(config.parser.get('hdfs', 'test.alias'))
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
    if cls.client:
      cls.client._delete('', recursive=True)
    sleep(self.delay)


class TestApi(_TestSession):

  """Test client raw API interactions."""

  # helper(s)

  def _file_exists(self, path):
    statuses = self.client._list_status('').json()['FileStatuses']
    return path in set(e['pathSuffix'] for e in statuses['FileStatus'])

  # actual tests

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
    ok_(status(self.client._create(path, data='hello')))
    data = self.client._list_status('').json()['FileStatuses']['FileStatus']
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


class TestCreate(_TestSession):

  """Test client create file."""

  def _check_contents(self, path, contents):
    eq_(self.client._open(path).contents, contents)

  def test_create_from_string(self):
    self.client.create('up', 'hello, world!')
    self._check_contents('up', 'hello, world!')

  def test_create_from_generator(self):
    data = (e for e in ['hello, ', 'world!'])
    self.client.create('up', data)
    self._check_contents('up', 'hello, world!')

  def test_create_from_file_object(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      with open(tpath) as reader:
        self.client.create('up', reader)
    self._check_contents('up', 'hello, world!')

  def test_create_set_permissions(self):
    pass # TODO

  @raises(HdfsError)
  def test_create_to_existing_fie_without_overwrite(self):
    self.client.create('up', 'hello, world!')
    self.client.create('up', 'hello again, world!')

  def test_create_and_overwrite_file(self):
    self.client.create('up', 'hello, world!')
    self.client.create('up', 'hello again, world!', overwrite=True)
    self._check_contents('up', 'hello again, world!')

  def test_create_and_overwrite_directory(self):
    self.client._mkdirs('up')
    self.client.create('up', 'hello, world!')
    self._check_contents('up', 'hello, world!')

  @raises(HdfsError)
  def test_create_invalid_path(self):
    # one of the directories in the new path is an already existing file
    self.client.create('up', 'hello, world!')
    self.client.create('up/up', 'hello again, world!')


class TestUpload(_TestSession):

  """Test client upload files."""

  def _check_contents(self, path, contents):
    eq_(self.client._open(path).contents, contents)
