#!/usr/bin/env python
# encoding: utf-8

"""Test that the examples run correctly."""

from hdfs import Config
from imp import load_source
from nose.plugins.skip import SkipTest
from six import add_metaclass
from util import _IntegrationTest
import os
import os.path as osp


class _ExamplesType(type):

  """Metaclass generating a test for each example."""

  dpath = osp.join(osp.dirname(__file__), os.pardir, 'examples')

  def __new__(mcs, cls, bases, attrs):

    def make_test(fname):
      fpath = osp.join(mcs.dpath, fname)
      module = osp.splitext(fname)[0]

      def test(self):
        try:
          load_source(module, fpath)
        except ImportError:
          # Unmet dependency.
          raise SkipTest

      test.__name__ = 'test_%s' % (module, )
      test.__doc__ = 'Test for example %s.' % (fpath, )
      return test

    for fname in os.listdir(mcs.dpath):
      if osp.splitext(fname)[1] == '.py':
        test = make_test(fname)
        attrs[test.__name__] = test
    return super(_ExamplesType, mcs).__new__(mcs, cls, bases, attrs)


@add_metaclass(_ExamplesType)
class TestExamples(_IntegrationTest):

  """Empty since tests are injected by the metaclass."""

  _get_client = None

  @classmethod
  def setup_class(cls):
    super(TestExamples, cls).setup_class()
    cls._get_client = Config.get_client
    Config.get_client = staticmethod(lambda: cls.client)

  @classmethod
  def teardown_class(cls):
    Config.get_client = cls._get_client
    super(TestExamples, cls).teardown_class()
