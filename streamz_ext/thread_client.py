from __future__ import absolute_import, division, print_function

from concurrent.futures import ThreadPoolExecutor, Future
from functools import wraps
from operator import getitem

from tornado import gen
from tornado.ioloop import IOLoop

from dask.compatibility import apply

from . import core, sources
from .core import Stream, identity


def result_maybe(future_maybe):
    # duck typing ?
    try:
        return future_maybe.result()
    except AttributeError:
        return future_maybe


def delayed_execution(func):
    @wraps(func)
    def inner(*args, **kwargs):
        args = tuple([result_maybe(v) for v in args])
        kwargs = {k: result_maybe(v) for k, v in kwargs.items()}
        return func(*args, **kwargs)

    return inner


def wrap_executor(executor):
    executor._submit = executor.submit

    @wraps(executor.submit)
    def inner(fn, *args, **kwargs):
        wfn = delayed_execution(fn)
        return executor._submit(wfn, *args, **kwargs)

    executor.submit = inner
    return executor


ex = wrap_executor(ThreadPoolExecutor())

ex.loop = IOLoop.current()


def default_client():
    return ex
