#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import *
import pytest


class TestAsyncWriter(object):

  def test_basic(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with AsyncWriter(consumer) as writer:
      writer.write('one')
      writer.write('two')
    assert result == [['one','two']]

  def test_multiple_writer_uses(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    writer = AsyncWriter(consumer)
    with writer:
      writer.write('one')
      writer.write('two')
    with writer:
      writer.write('three')
      writer.write('four')
    assert result == [['one','two'],['three','four']]

  def test_multiple_consumer_uses(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with AsyncWriter(consumer) as writer:
      writer.write('one')
      writer.write('two')
    with AsyncWriter(consumer) as writer:
      writer.write('three')
      writer.write('four')
    assert result == [['one','two'],['three','four']]

  def test_nested(self):
    with pytest.raises(ValueError):
      result = []
      def consumer(gen):
        result.append(list(gen))
      with AsyncWriter(consumer) as _writer:
        _writer.write('one')
        with _writer as writer:
          writer.write('two')

  def test_child_error(self):
    with pytest.raises(HdfsError):
      def consumer(gen):
        for value in gen:
          if value == 'two':
            raise HdfsError('Yo')
      with AsyncWriter(consumer) as writer:
        writer.write('one')
        writer.write('two')

  def test_parent_error(self):
    with pytest.raises(HdfsError):
      def consumer(gen):
        for value in gen:
          pass
      def invalid(w):
        w.write('one')
        raise HdfsError('Ya')
      with AsyncWriter(consumer) as writer:
        invalid(writer)


class TestTemppath(object):

  def test_new(self):
    with temppath() as tpath:
      assert not osp.exists(tpath)

  def test_cleanup(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hi')
    assert not osp.exists(tpath)

  def test_dpath(self):
    with temppath() as dpath:
      os.mkdir(dpath)
      with temppath(dpath) as tpath:
        assert osp.dirname(tpath) == dpath
