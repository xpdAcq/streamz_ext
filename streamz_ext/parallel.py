from streamz_ext.core import get_io_loop
from streamz_ext.clients import DEFAULT_BACKENDS
from operator import getitem

from tornado import gen

from dask.compatibility import apply

from . import core, sources
from .core import Stream, identity


class ParallelStream(Stream):
    """ A Parallel stream using multiple backends

    This object is fully compliant with the ``streamz.core.Stream`` object but
    uses a client for execution.  Operations like ``map`` and
    ``accumulate`` submit functions to run on the client instance
    and pass around futures.
    Time-based operations like ``timed_window``, buffer, and so on operate as
    normal.

    Typically one transfers between normal Stream and ParallelStream
    objects using the ``Stream.scatter()`` and ``ParallelStream.gather()`` methods.

    Examples
    --------
    >>> from dask.distributed import Client
    >>> client = Client()

    >>> from streamz_ext import Stream
    >>> source = Stream()
    >>> source.scatter().map(func).accumulate(binop).gather().sink(...)

    # This runs on thread backends
    >>> from streamz_ext import Stream
    >>> source = Stream()
    >>> source.scatter(backend='thread').map(func).accumulate(binop).gather().sink(...)

    See Also
    --------
    dask.distributed.Client
    """

    @classmethod
    def _get_name(cls):
        return cls.__class__.___name__

    def __init__(self, *args, backend="dask", **kwargs):
        super().__init__(*args, **kwargs)
        upstream_backends = set(
            [getattr(u, "default_client", None) for u in self.upstreams]
        )
        if None in upstream_backends:
            upstream_backends.remove(None)
        if len(upstream_backends) > 1:
            raise RuntimeError("Mixing backends is not supported")
        elif upstream_backends:
            self.default_client = upstream_backends.pop()
        else:
            self.default_client = DEFAULT_BACKENDS.get(backend, backend)
        if "loop" not in kwargs and getattr(
            self.default_client(), "loop", None
        ):
            loop = self.default_client().loop
            self._set_loop(loop)
            if kwargs.get("ensure_io_loop", False) and not self.loop:
                self._set_asynchronous(False)
            if self.loop is None and self.asynchronous is not None:
                self._set_loop(get_io_loop(self.asynchronous))


@core.Stream.register_api()
@ParallelStream.register_api()
class scatter(ParallelStream):
    @gen.coroutine
    def update(self, x, who=None):
        client = self.default_client()
        future = yield client.scatter(x, asynchronous=True)
        f = yield self._emit(future)
        raise gen.Return(f)


@ParallelStream.register_api()
class gather(ParallelStream):
    """ Wait on and gather results from ParallelStream to local Stream

    This waits on every result in the stream and then gathers that result back
    to the local stream.  Warning, this can restrict parallelism.  It is common
    to combine a ``gather()`` node with a ``buffer()`` to allow unfinished
    futures to pile up.

    Examples
    --------
    >>> local_stream = dask_stream.buffer(20).gather()

    See Also
    --------
    buffer
    scatter
    """

    @gen.coroutine
    def update(self, x, who=None):
        client = self.default_client()
        result = yield client.gather(x, asynchronous=True)
        result2 = yield self._emit(result)
        raise gen.Return(result2)


@ParallelStream.register_api()
class map(ParallelStream):
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        ParallelStream.__init__(self, upstream)

    def update(self, x, who=None):
        client = self.default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self._emit(result)


@ParallelStream.register_api()
class accumulate(ParallelStream):
    def __init__(
        self,
        upstream,
        func,
        start=core.no_default,
        returns_state=False,
        **kwargs
    ):
        self.func = func
        self.state = start
        self.returns_state = returns_state
        self.kwargs = kwargs
        ParallelStream.__init__(self, upstream)

    def update(self, x, who=None):
        if self.state is core.no_default:
            self.state = x
            return self._emit(self.state)
        else:
            client = self.default_client()
            result = client.submit(self.func, self.state, x, **self.kwargs)
            if self.returns_state:
                state = client.submit(getitem, result, 0)
                result = client.submit(getitem, result, 1)
            else:
                state = result
            self.state = state
            return self._emit(result)


@ParallelStream.register_api()
class starmap(ParallelStream):
    def __init__(self, upstream, func, **kwargs):
        self.func = func
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs

        ParallelStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        client = self.default_client()
        result = client.submit(apply, self.func, x, self.kwargs)
        return self._emit(result)


@ParallelStream.register_api()
class buffer(ParallelStream, core.buffer):
    pass


@ParallelStream.register_api()
class combine_latest(ParallelStream, core.combine_latest):
    pass


@ParallelStream.register_api()
class delay(ParallelStream, core.delay):
    pass


@ParallelStream.register_api()
class latest(ParallelStream, core.latest):
    pass


@ParallelStream.register_api()
class partition(ParallelStream, core.partition):
    pass


@ParallelStream.register_api()
class rate_limit(ParallelStream, core.rate_limit):
    pass


@ParallelStream.register_api()
class sliding_window(ParallelStream, core.sliding_window):
    pass


@ParallelStream.register_api()
class timed_window(ParallelStream, core.timed_window):
    pass


@ParallelStream.register_api()
class union(ParallelStream, core.union):
    pass


@ParallelStream.register_api()
class zip(ParallelStream, core.zip):
    pass


@ParallelStream.register_api()
class zip_latest(ParallelStream, core.zip_latest):
    pass


@ParallelStream.register_api(staticmethod)
class filenames(ParallelStream, sources.filenames):
    pass


@ParallelStream.register_api(staticmethod)
class from_textfile(ParallelStream, sources.from_textfile):
    pass


@ParallelStream.register_api()
class unique(ParallelStream, core.unique):
    pass


@ParallelStream.register_api()
class filter(ParallelStream, core.filter):
    pass


@ParallelStream.register_api()
class pluck(ParallelStream, core.pluck):
    pass
