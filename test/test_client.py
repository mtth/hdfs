#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.client import *
from hdfs.util import HdfsError, temppath
from helpers import _TestSession
from nose.tools import eq_, nottest, ok_, raises
from requests.exceptions import ConnectTimeout, ReadTimeout
from shutil import rmtree
from tempfile import mkdtemp
import os
import os.path as osp
import time


def status(response):
  """Helper for requests that return boolean JSON responses."""
  return response.json()['boolean']


class TestLoad(object):

  """Test client loader."""

  def test_bare(self):
    client = Client._from_options(None, {'url': 'foo'})
    ok_(isinstance(client, Client))

  def test_new_type(self):
    class NewClient(Client):
      def __init__(self, url, bar):
        super(NewClient, self).__init__(url)
        self.bar = bar
    client = Client._from_options('NewClient', {'url': 'bar', 'bar': 2})
    eq_(client.bar, 2)

  @raises(HdfsError)
  def test_missing_options(self):
    Client._from_options('KerberosClient', {})

  @raises(HdfsError)
  def test_invalid_options(self):
    Client._from_options(None, {'foo': 123})

  @raises(HdfsError)
  def test_missing_type(self):
    Client._from_options('MissingClient', {})

  def test_verify(self):
    ok_(not Client._from_options(None, {'url': '', 'verify': 'false'}).verify)
    ok_(Client._from_options(None, {'url': '', 'verify': 'yes'}).verify)

  def test_timeout(self):
    eq_(Client('').timeout, None)
    eq_(Client('', timeout=1).timeout, 1)
    eq_(Client('', timeout=(1,2)).timeout, (1,2))
    eq_(Client._from_options(None, {'url': ''}).timeout, None)
    eq_(Client._from_options(None, {'url': '', 'timeout': '1'}).timeout, 1)
    eq_(Client._from_options(None, {'url': '', 'timeout': '1,2'}).timeout, (1,2))

  def test_cert(self):
    eq_(Client('').cert, None)
    eq_(Client('', cert='foo').cert, 'foo')
    eq_(Client('', cert='foo,bar').cert, ('foo', 'bar'))
    eq_(Client('', cert=('foo', 'bar')).cert, ('foo', 'bar'))


class TestOptions(_TestSession):

  """Test client options."""

  def test_timeout(self):
    self.client.timeout = 1e-4 # Small enough for it to always timeout.
    try:
      self.client.status('.')
    except (ConnectTimeout, ReadTimeout):
      self.client.timeout = None
    else:
      raise HdfsError('No timeout.')


class TestApi(_TestSession):

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
    ok_(self._file_exists(path))

  def test_create_nested_file(self):
    path = 'foo/bar'
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


class TestResolve(_TestSession):

  def test_resolve_relative(self):
    eq_(Client('url', root='/').resolve('bar'), '/bar')
    eq_(Client('url', root='/foo').resolve('bar'), '/foo/bar')
    eq_(Client('url', root='/foo/').resolve('bar'), '/foo/bar')
    eq_(Client('url', root='/foo/').resolve('bar/'), '/foo/bar')
    eq_(Client('url', root='/foo/').resolve('/bar/'), '/bar')

  @raises(HdfsError)
  def test_resolve_relative_no_root(self):
    Client('url').resolve('bar')

  @raises(HdfsError)
  def test_resolve_relative_root(self):
    Client('', root='bar').resolve('foo')

  def test_resolve_absolute(self):
    eq_(Client('url').resolve('/bar'), '/bar')
    eq_(Client('url').resolve('/bar/foo/'), '/bar/foo')

  def test_resolve_filename(self):
    path = 'fo&o/a?%a'
    encoded = self.client.resolve(path)
    eq_(encoded.split('/')[-2:], ['fo%26o', 'a%3F%25a'])

  def test_resolve_filename_with_safe_characters(self):
    path = 'foo=1'
    encoded = self.client.resolve(path)
    eq_(encoded.split('/')[-1], 'foo=1')

  def test_create_file_with_reserved_characters(self):
    path = 'fo&o/a?a'
    self.client.write(path, data='hello')
    eq_(''.join(self.client.read(path)), 'hello')

  def test_create_file_with_percent(self):
    # `%` (`0x25`) is a special case because it seems to cause errors (even
    # though the action still goes through). Typical error message will be
    # `"Unknown exception in doAs"`.
    path = 'fo&o/a%a'
    try:
      self.client.write(path, data='hello')
    except HdfsError:
      pass
    eq_(''.join(self.client.read(path)), 'hello')


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


class TestAppend(_TestSession):

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
    self._check_content('ap', 'hello, world!')

  @raises(HdfsError)
  def test_missing_file(self):
    self.client.write('ap', 'hello!', append=True)

  @raises(ValueError)
  def test_overwrite_and_append(self):
    self.client.write('ap', 'hello!', overwrite=True, append=True)

  @raises(ValueError)
  def test_set_permission_and_append(self):
    self.client.write('ap', 'hello!', permission='777', append=True)


class TestUpload(_TestSession):

  def test_upload_file(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hello, world!')
      self.client.upload('up', tpath)
    self._check_content('up', 'hello, world!')

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
      self._check_content('up/hi/foo', 'hello!')
      self._check_content('up/hi/bar/baz', 'world!')
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
      self._check_content('up/foo', 'hello!')
      self._check_content('up/bar/baz', 'world!')
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
      self._check_content('up/foo', 'hello!')
      self._check_content('up/bar/baz', 'world!')
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
    self._check_content('up', 'there')

  @raises(HdfsError)
  def test_upload_overwrite_error(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('here')
      self.client.upload('up', tpath)
      self.client.upload('up', tpath)


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

  def _read(self, writer, *args, **kwargs):
    for chunk in self.client.read(*args, **kwargs):
      writer.write(chunk)


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


class TestDownload(_TestSession):

  def setup(self):
    super(TestDownload, self).setup()
    self.parts = {
      'part-r-00000': 'fee',
      'part-r-00001': 'faa',
      'part-r-00002': 'foo',
    }

  @raises(HdfsError)
  def test_missing_dir(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      self.client.download('dl', osp.join(tpath, 'foo'))

  def test_normal_file(self):
    self.client.write('dl', 'hello')
    with temppath() as tpath:
      fname = self.client.download('dl', tpath)
      with open(fname) as reader:
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

  def test_partitioned_file(self):
    for name, content in self.parts.items():
      self.client.write('dl/%s' % (name, ), content)
    with temppath() as tpath:
      self.client.download('dl', tpath, n_threads=-1)
      self.check_contents(tpath)

  def test_partitioned_file_setting_n_threads(self):
    for name, content in self.parts.items():
      self.client.write('dl/%s' % (name, ), content)
    with temppath() as tpath:
      self.client.download('dl', tpath, n_threads=2)
      self.check_contents(tpath)

  def test_partitioned_file_sync(self):
    for name, content in self.parts.items():
      self.client.write('dl/%s' % (name, ), content)
    with temppath() as tpath:
      self.client.download('dl', tpath, n_threads=0)
      self.check_contents(tpath)

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

  def test_download_folder_to_missing_folder(self):
    self.client.write('foo/dl', 'hello')
    self.client.write('foo/bar/dl', 'there')
    with temppath() as tpath:
      self.client.download('foo', tpath)
      with open(osp.join(tpath, 'dl')) as reader:
        eq_(reader.read(), 'hello')
      with open(osp.join(tpath, 'bar', 'dl')) as reader:
        eq_(reader.read(), 'there')

  @raises(HdfsError)
  def test_download_empty_folder(self):
    self.client._mkdirs('foo')
    with temppath() as tpath:
      self.client.download('foo', tpath)

  # helpers

  def check_contents(self, local_path):
    local_parts = os.listdir(local_path)
    eq_(set(local_parts), set(self.parts))
    # we have all the parts
    for part in local_parts:
      with open(osp.join(local_path, part)) as reader:
        eq_(reader.read(), self.parts[part])
        # their content is correct


class TestStatus(_TestSession):

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


class TestSetOwner(_TestSession):

  @classmethod
  def setup_class(cls):
    super(TestSetOwner, cls).setup_class()
    if cls.client:
      try:
        cls.client.write('foo', '')
        cls.client.set_owner('foo', 'bar')
      except HdfsError as err:
        if 'Non-super user cannot change owner' in err.message:
          cls.client = None
          # skip these tests if HDFS isn't configured to support them.
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


class TestSetPermissions(_TestSession):

  def test_directory(self):
    new_permission = '755'
    self.client._mkdirs('foo', permission='444')
    self.client.set_permissions('foo', new_permission)
    status = self.client.status('foo')
    eq_(status['permission'], new_permission)

  def test_file(self):
    new_permission = '755'
    self.client.write('foo', 'hello, world!', permission='444')
    self.client.set_permissions('foo', new_permission)
    status = self.client.status('foo')
    eq_(status['permission'], new_permission)

  @raises(HdfsError)
  def test_missing(self):
    self.client.set_permissions('foo', '755')


class TestMakeDirectory(_TestSession):

  def test_make_directory(self):
    path = 'bar'
    self.client.make_directory(path)
    ok_(self._file_exists(path))

  def test_make_directory_with_permissions(self):
    path = 'foo'
    permission = '555'
    self.client.make_directory(path, permission=permission)
    ok_(self._file_exists(path))
    eq_(self.client.status(path)['permission'], permission)


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


class TestList(_TestSession):

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
    statuses = self.client.list('foo')
    eq_(len(statuses), 1)
    status = self.client.status('foo/bar')
    status['pathSuffix'] = 'bar'
    eq_(statuses[0], (osp.join(self.client.root, 'foo', 'bar'), status))


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


class TestLatestExpansion(_TestSession):

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
