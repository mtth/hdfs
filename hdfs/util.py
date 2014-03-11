#!/usr/bin/env python
# encoding: utf-8

"""Utilities."""

from functools import wraps
import requests as rq


class HdfsError(Exception):

  """Base error class.

  :param message: error message
  :param args: optional message formatting arguments

  """

  def __init__(self, message, *args):
    super(HdfsError, self).__init__(message % args or ())


def request(meth, json=True):
  """Decorator to transform methods into requests.

  :param meth: HTTP verb.

  The name of the method is used to generate the operation.

  """
  handler = getattr(rq, meth.lower())
  def decorator(func):
    """Actual decorator."""
    @wraps(func)
    def wrapper(client, *args, **kwargs):
      """Wrapper function."""
      url, params = func(client, *args, **kwargs)
      params.setdefault('op', func.__name__.replace('_', '').upper())
      res = handler(
        url=url,
        params=params,
        auth=client.auth,
      )
      if json:
        return res.json()
      else:
        return res.content
    return wrapper
  return decorator
