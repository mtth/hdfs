#!/usr/bin/env python
# encoding: utf-8

"""A template for generating new clients.

This is used to test autoloading from `CliConfig` (see `test/test_main.py`).

"""

from hdfs import Client


class $class_name(Client):

  one = 1

  def __init__(self, url):
    super($class_name, self).__init__(url)
