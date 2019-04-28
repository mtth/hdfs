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
      writer.write('one')
      writer.write('two')
    eq_(result, [['one','two']])

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
    eq_(result, [['one','two'],['three','four']])

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
    eq_(result, [['one','two'],['three','four']])

  @raises(ValueError)
  def test_nested(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with AsyncWriter(consumer) as _writer:
      _writer.write('one')
      with _writer as writer:
        writer.write('two')

  @raises(HdfsError)
  def test_child_error(self):
    def consumer(gen):
      for value in gen:
        if value == 'two':
          raise HdfsError('Yo')
    with AsyncWriter(consumer) as writer:
      writer.write('one')
      writer.write('two')

  @raises(HdfsError)
  def test_parent_error(self):
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
