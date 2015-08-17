#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import *
from nose.tools import eq_, ok_, raises


class TestAsyncWriter(object):

  def test_basic(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with AsyncWriter(consumer) as writer:
      writer.write(1)
      writer.write(2)
    eq_(result, [[1,2]])

  def test_multiple_writer_uses(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    writer = AsyncWriter(consumer)
    with writer:
      writer.write(1)
      writer.write(2)
    with writer:
      writer.write(3)
      writer.write(4)
    eq_(result, [[1,2],[3,4]])

  def test_multiple_consumer_uses(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with AsyncWriter(consumer) as writer:
      writer.write(1)
      writer.write(2)
    with AsyncWriter(consumer) as writer:
      writer.write(3)
      writer.write(4)
    eq_(result, [[1,2],[3,4]])

  @raises(ValueError)
  def test_nested(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with AsyncWriter(consumer) as _writer:
      _writer.write(1)
      with _writer as writer:
        writer.write(2)

  @raises(HdfsError)
  def test_child_error(self):
    def consumer(gen):
      for value in gen:
        if value == 2:
          raise HdfsError('Yo')
    with AsyncWriter(consumer) as writer:
      writer.write(1)
      writer.write(2)

  @raises(HdfsError)
  def test_parent_error(self):
    def consumer(gen):
      for value in gen:
        pass
    def invalid(w):
      w.write(1)
      raise HdfsError('Ya')
    with AsyncWriter(consumer) as writer:
      invalid(writer)


class TestTemppath(object):

  def test_new(self):
    with temppath() as tpath:
      ok_(not osp.exists(tpath))

  def test_cleanup(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('hi')
    ok_(not osp.exists(tpath))

  def test_dpath(self):
    with temppath() as dpath:
      os.mkdir(dpath)
      with temppath(dpath) as tpath:
        eq_(osp.dirname(tpath), dpath)
