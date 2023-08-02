#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs client interactions with HDFS."""

from hdfs.util import *
from nose.tools import eq_, ok_, raises
import time
import threading

class TestAsyncWriter(object):

  AsyncWriterFactory = AsyncWriter

  def test_basic(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with self.AsyncWriterFactory(consumer) as writer:
      writer.write('one')
      writer.write('two')
    eq_(result, [['one','two']])

  def test_multiple_writer_uses(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    writer = self.AsyncWriterFactory(consumer)
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
    with self.AsyncWriterFactory(consumer) as writer:
      writer.write('one')
      writer.write('two')
    with self.AsyncWriterFactory(consumer) as writer:
      writer.write('three')
      writer.write('four')
    eq_(result, [['one','two'],['three','four']])

  @raises(ValueError)
  def test_nested(self):
    result = []
    def consumer(gen):
      result.append(list(gen))
    with self.AsyncWriterFactory(consumer) as _writer:
      _writer.write('one')
      with _writer as writer:
        writer.write('two')

  @raises(HdfsError)
  def test_child_error(self):
    def consumer(gen):
      for value in gen:
        if value == 'two':
          raise HdfsError('Yo')
    with self.AsyncWriterFactory(consumer) as writer:
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
    with self.AsyncWriterFactory(consumer) as writer:
      invalid(writer)

import logging

class WaitingConsumer():
  
  do_consume = False
  _logger = logging.getLogger(__name__)
  def __init__(self):
    self.values = []

  def consume(self, gen):
    for value in gen:
      while not self.do_consume:
        time.sleep(0.15)
      WaitingConsumer._logger.debug(f"adding {value}")
      self.values.append(value)

      self.do_consume = False

class TestBoundedAsyncWriter(TestAsyncWriter):

  AsyncWriterFactory = BoundedAsyncWriter

  def test_boundness(self):

    def do_some_writing(writer):
      with writer as active_writer:
        active_writer.write('one')
        active_writer.write('two')
        active_writer.write('three')
        active_writer.write('four')

    consumer = WaitingConsumer()
    writer = self.AsyncWriterFactory(consumer.consume,4)

    a_thread = threading.Thread(target=do_some_writing, daemon=True, args=[writer])
    a_thread.start() # current buffer contains one and two

    consumer.do_consume=True # after consuming buffer contains two and three
    time.sleep(0.4)
    assert writer.is_full #== True

    consumer.do_consume=True # after consuming buffer contains only three
    time.sleep(0.4)
    assert writer.is_full #== True

    consumer.do_consume=True # after consuming buffer is empty but four will be inserted very quickly
    time.sleep(0.4)
    assert not writer.is_full #== False
    
    consumer.do_consume=True # after consuming buffer is empty

    time.sleep(0.4)
    assert not writer.is_full #== False
   
    assert consumer.values == ['one', 'two', 'three', 'four']


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
