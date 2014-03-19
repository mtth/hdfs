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


class TestLocalClient(object):

  """Test local client things. E.g. loaders."""

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

  Currently, only running tests via Kerberos authentication is supported.

  """

  delay = 1 # delay in seconds between requests

  @classmethod
  def setup_class(cls):
    config = Config()
    try:
      client = load_client(config.parser.get('hdfs', 'test.alias'))
      client._mkdirs('')
    except (NoOptionError, NoSectionError, HdfsError):
      cls.client = None
    else:
      cls.client = client

  @classmethod
  def teardown_class(cls):
    if cls.client:
      cls.client._delete('')

  def setup(self):
    if not self.client:
      raise SkipTest

  def teardown(self):
    sleep(self.delay)


class TestClient(_TestSession):

  """Test Client general interactions."""

  def _file_exists(self, path):
    statuses = self.client._list_status('').json()['FileStatuses']
    return path in set(e['pathSuffix'] for e in statuses['FileStatus'])

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

  def test_create_delete_file(self):
    path = 'foo'
    ok_(self.client._create(path, data='hello'))
    data = self.client._list_status('').json()['FileStatuses']['FileStatus']
    ok_(self._file_exists(path))
    ok_(status(self.client._delete(path)))
    ok_(not self._file_exists(path))

  def test_create_streaming(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello\nworld!')
      with open(tpath) as reader:
        self.client._create('foo', data=(line for line in reader))
    eq_(self.client._open('foo').content, 'hello\nworld!')
    self.client._delete('foo')

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

  @raises(HdfsError)
  def test_get_file_checksum_on_folder(self):
    self.client._get_file_checksum('')


class TestClientFileActions(_TestSession):

  """Test Client file interactions, avoiding deleting a file every time."""

  path = 'test_file'

  @classmethod
  def setup_class(cls):
    super(TestClientFileActions, cls).setup_class()
    cls.client._create(cls.path, data='hello')

  @classmethod
  def teardown_class(cls):
    cls.client._delete(cls.path)
    super(TestClientFileActions, cls).teardown_class()

  def test_open_file(self):
    content = self.client._open(self.path).content
    eq_(content, 'hello')

  def test_get_file_checksum(self):
    data = self.client._get_file_checksum(self.path).json()['FileChecksum']
    eq_(sorted(data), ['algorithm', 'bytes', 'length'])
    ok_(int(data['length']))

  def test_get_file_checksum(self):
    data = self.client._get_file_checksum(self.path).json()['FileChecksum']
    eq_(sorted(data), ['algorithm', 'bytes', 'length'])
    ok_(int(data['length']))

  def test_get_file_checksum(self):
    data = self.client._get_file_checksum(self.path).json()['FileChecksum']
    eq_(sorted(data), ['algorithm', 'bytes', 'length'])
    ok_(int(data['length']))
