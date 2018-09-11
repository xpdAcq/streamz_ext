from dask.distributed import default_client as dask_default_client
from .thread_client import default_client as thread_default_client
from streamz_ext import Stream


from concurrent.futures import ThreadPoolExecutor, Future
from functools import wraps
from operator import getitem

from tornado import gen
from tornado.ioloop import IOLoop

from dask.compatibility import apply

from . import core, sources
from .core import Stream, identity

DEFAULT_BACKENDS = {'dask': dask_default_client,
                    'thread': thread_default_client}


class ParallelStream(Stream):
    """ A Parallel stream using Dask

    This object is fully compliant with the ``streamz.core.Stream`` object but
    uses a Dask client for execution.  Operations like ``map`` and
    ``accumulate`` submit functions to run on the Dask instance using
    ``dask.distributed.Client.submit`` and pass around Dask futures.
    Time-based operations like ``timed_window``, buffer, and so on operate as
    normal.

    Typically one transfers between normal Stream and DaskStream objects using
    the ``Stream.scatter()`` and ``DaskStream.gather()`` methods.

    Examples
    --------
    >>> from dask.distributed import Client
    >>> client = Client()

    >>> from streamz import Stream
    >>> source = Stream()
    >>> source.scatter().map(func).accumulate(binop).gather().sink(...)

    See Also
    --------
    dask.distributed.Client
    """

    def __init__(self, upstream=None, upstreams=None, stream_name=None,
                 loop=None, asynchronous=None, ensure_io_loop=False,
                 backend='dask'):
        if upstreams is not None:
            self.upstreams = list(upstreams)
        else:
            self.upstreams = [upstream]
        upstream_backends = set(
            [getattr(u, 'backend') for u in self.upstreams])
        upstream_backends.remove(None)
        if len(upstream_backends) > 1:
            raise RuntimeError("Mixing backends is not supported")
        elif upstream_backends:
            self.backend = upstream_backends.pop()
        else:
            self.backend = DEFAULT_BACKENDS.get(backend, backend)
        if not loop:
            loop = self.backend().loop
        super().__init__(upstream=upstream, upstreams=upstreams,
                         stream_name=stream_name,
                         loop=loop, asynchronous=asynchronous,
                         ensure_io_loop=ensure_io_loop)
