from __future__ import absolute_import, division, print_function

from concurrent.futures import ThreadPoolExecutor, Future
from functools import wraps
from operator import getitem

from streamz.core import _truthy
from tornado import gen
from tornado.ioloop import IOLoop

from dask.compatibility import apply

from . import core, sources
from .core import Stream, identity

NULL_COMPUTE = '~~NULL_COMPUTE~~'


def result_maybe(future_maybe):
    try:
        return future_maybe.result()
    except AttributeError:
        return future_maybe


def return_null(func):
    @wraps(func)
    def inner(x, *args, **kwargs):
        tv = func(x, *args, **kwargs)
        if tv:
            return x
        else:
            return NULL_COMPUTE

    return inner


def filter_null_wrapper(func):
    @wraps(func)
    def inner(*args, **kwargs):
        if (any(a == NULL_COMPUTE for a in args)
                or any(v == NULL_COMPUTE for v in kwargs.values())):
            return NULL_COMPUTE
        else:
            return func(*args, **kwargs)

    return inner


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


class ThreadStream(Stream):
    """ A Parallel stream using Dask

    This object is fully compliant with the ``streamz.core.Stream`` object but
    uses a Thread Pool for execution.  Operations like ``map`` and
    ``accumulate`` submit functions to run on the Thread Pool using
    ``concurrent.futures.ThreadPoolExecutor.submit`` and pass around futures.
    Time-based operations like ``timed_window``, buffer, and so on operate as
    normal.

    Typically one transfers between normal Stream and ThreadStream objects using
    the ``Stream.scatter()`` and ``ThreadStream.gather()`` methods.

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

    _graphviz_fillcolor = "green"

    def __init__(self, *args, **kwargs):
        super(ThreadStream, self).__init__(*args, **kwargs)


@core.Stream.register_api()
@ThreadStream.register_api()
class thread_scatter(ThreadStream):
    @gen.coroutine
    def update(self, x, who=None):
        client = default_client()
        future = client.submit(identity, x)
        f = yield self._emit(future)
        raise gen.Return(f)


@ThreadStream.register_api()
class gather(core.Stream):
    """ Wait on and gather results from ThreadStream to local Stream

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @gen.coroutine
    def update(self, x, who=None):
        # If we have a sequence of futures await each one
        if isinstance(x, tuple):
            final_result = []
            for sub_x in x:
                yx = yield sub_x
                final_result.append(yx)
            result = tuple(final_result)
        else:
            result = yield x
        if result != NULL_COMPUTE:
            result2 = yield self._emit(result)
            raise gen.Return(result2)


@ThreadStream.register_api()
class map(ThreadStream):
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = filter_null_wrapper(func)
        self.kwargs = kwargs
        self.args = args

        ThreadStream.__init__(self, upstream)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self._emit(result)


@ThreadStream.register_api()
class accumulate(ThreadStream):
    def __init__(
            self,
            upstream,
            func,
            start=core.no_default,
            returns_state=False,
            **kwargs
    ):
        self.func = filter_null_wrapper(func)
        self.state = start
        self.returns_state = returns_state
        self.kwargs = kwargs
        ThreadStream.__init__(self, upstream)

    def update(self, x, who=None):
        if self.state is core.no_default:
            self.state = x
            return self._emit(self.state)
        else:
            client = default_client()
            result = client.submit(self.func, self.state, x, **self.kwargs)
            if self.returns_state:
                state = client.submit(getitem, result, 0)
                result = client.submit(getitem, result, 1)
            else:
                state = result
            self.state = state
            return self._emit(result)


@ThreadStream.register_api()
class starmap(ThreadStream):
    def __init__(self, upstream, func, **kwargs):
        self.func = filter_null_wrapper(func)
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs

        ThreadStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(apply, self.func, x, self.kwargs)
        return self._emit(result)


@ThreadStream.register_api()
class filter(ThreadStream):
    def __init__(self, upstream, predicate, *args, **kwargs):
        if predicate is None:
            predicate = _truthy
        self.predicate = return_null(predicate)
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(self.predicate, x, *self.args, **self.kwargs)
        return self._emit(result)


@ThreadStream.register_api()
class buffer(ThreadStream, core.buffer):
    pass


@ThreadStream.register_api()
class combine_latest(ThreadStream, core.combine_latest):
    pass


@ThreadStream.register_api()
class delay(ThreadStream, core.delay):
    pass


@ThreadStream.register_api()
class latest(ThreadStream, core.latest):
    pass


@ThreadStream.register_api()
class partition(ThreadStream, core.partition):
    pass


@ThreadStream.register_api()
class rate_limit(ThreadStream, core.rate_limit):
    pass


@ThreadStream.register_api()
class sliding_window(ThreadStream, core.sliding_window):
    pass


@ThreadStream.register_api()
class timed_window(ThreadStream, core.timed_window):
    pass


@ThreadStream.register_api()
class union(ThreadStream, core.union):
    pass


@ThreadStream.register_api()
class zip(ThreadStream, core.zip):
    pass


@ThreadStream.register_api()
class zip_latest(ThreadStream, core.zip_latest):
    pass


@ThreadStream.register_api(staticmethod)
class filenames(ThreadStream, sources.filenames):
    pass


@ThreadStream.register_api(staticmethod)
class from_textfile(ThreadStream, sources.from_textfile):
    pass


@ThreadStream.register_api()
class pluck(ThreadStream, core.pluck):
    pass
