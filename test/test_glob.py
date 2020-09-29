import posixpath

from nose.tools import eq_

from hdfs.glob import glob
from util import _IntegrationTest


class TestGlob(_IntegrationTest):

  def setup(self):
    super(TestGlob, self).setup()
    self.__build_dirs()

  def __build_dirs(self):
    """
    Structure:

    dir_1
        dir_1_1
            file_1_3_1.txt
        dir_1_2
            file_1_3_1.txt
        dir_1_3
            file_1_3_1.txt
            file_1_3_2.txt
            file_1_3_3.txt
        file_1_1.txt
    dir_2
        dir_2_1
            file_2_3_1.txt
        dir_2_2
            file_2_3_1.txt
        dir_2_3
            file_2_3_1.txt
            file_2_3_2.txt
            file_2_3_3.txt
        file_2_1.txt
    """
    self._write(posixpath.join('dir_1', 'dir_1_1', 'file_1_3_1.txt'), b'file_1_3_1')
    self._write(posixpath.join('dir_1', 'dir_1_2', 'file_1_3_1.txt'), b'file_1_3_1')
    self._write(posixpath.join('dir_1', 'dir_1_3', 'file_1_3_1.txt'), b'file_1_3_1')
    self._write(posixpath.join('dir_1', 'dir_1_3', 'file_1_3_2.txt'), b'file_1_3_2')
    self._write(posixpath.join('dir_1', 'dir_1_3', 'file_1_3_3.txt'), b'file_1_3_3')
    self._write(posixpath.join('dir_1', 'file_1_1.txt'), b'file_1_1')
    self._write(posixpath.join('dir_2', 'dir_2_1', 'file_2_3_1.txt'), b'file_2_3_1')
    self._write(posixpath.join('dir_2', 'dir_2_2', 'file_2_2_1.txt'), b'file_2_2_1')
    self._write(posixpath.join('dir_2', 'dir_2_3', 'file_2_3_1.txt'), b'file_2_3_1')
    self._write(posixpath.join('dir_2', 'dir_2_3', 'file_2_3_2.txt'), b'file_2_3_2')
    self._write(posixpath.join('dir_2', 'dir_2_3', 'file_2_3_3.txt'), b'file_2_3_3')
    self._write(posixpath.join('dir_2', 'file_2_1.txt'), b'file_2_1')

  def test(self):
    values = [
      ('./dir_1/dir_1_3/*', [
        './dir_1/dir_1_3/file_1_3_1.txt',
        './dir_1/dir_1_3/file_1_3_2.txt',
        './dir_1/dir_1_3/file_1_3_3.txt',
      ]),
      ('./dir_2/dir_2_3/file_2_3_?.txt', [
        './dir_2/dir_2_3/file_2_3_1.txt',
        './dir_2/dir_2_3/file_2_3_2.txt',
        './dir_2/dir_2_3/file_2_3_3.txt',
      ]),
      ('*/*.txt', [
        'dir_1/file_1_1.txt',
        'dir_2/file_2_1.txt',
      ]),
      ('./dir_[1-2]/file_[1-2]_1.txt', [
        './dir_1/file_1_1.txt',
        './dir_2/file_2_1.txt',
      ]),
      ('./dir_*/dir_*/file_[1-2]_3_2.txt', [
        './dir_1/dir_1_3/file_1_3_2.txt',
        './dir_2/dir_2_3/file_2_3_2.txt',
      ]),
      ('./dir_[3-4]/file_[1-2]_1.txt', []),
      ('./dir_*/dir_*/file_[3-4]_3_2.txt', []),
    ]
    for pattern, expected in values:
      actual = glob(self.client, pattern)
      eq_(expected, actual, 'Unexpected result for pattern ' + pattern)



