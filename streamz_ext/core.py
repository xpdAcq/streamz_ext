from collections import Hashable
from collections.abc import Sequence
import threading

from streamz.core import *
from streamz.core import (
    combine_latest as _combine_latest,
    zip as _zip,
    zip_latest as _zip_latest,
)
from streamz.core import _global_sinks, _truthy

no_default = '--no-default--'

_html_update_streams = set()

thread_state = threading.local()

logger = logging.getLogger(__name__)

_io_loops = []

def sync(loop, func, *args, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """
    # This was taken from distrbuted/utils.py
    timeout = kwargs.pop('callback_timeout', None)

    def make_coro():
        coro = gen.maybe_future(func(*args, **kwargs))
        if timeout is None:
            return coro
        else:
            return gen.with_timeout(timedelta(seconds=timeout), coro)

    e = threading.Event()
    main_tid = get_thread_identity()
    result = [None]
    error = [False]

    @gen.coroutine
    def f():
        try:
            if main_tid == get_thread_identity():
                raise RuntimeError("sync() called from thread of running loop")
            yield gen.moment
            thread_state.asynchronous = True
            result[0] = yield make_coro()
        except Exception as exc:
            logger.exception(exc)
            error[0] = sys.exc_info()
        finally:
            thread_state.asynchronous = False
            e.set()

    loop.add_callback(f)
    while not e.is_set():
        e.wait(1000000)
    if error[0]:
        six.reraise(*error[0])
    else:
        return result[0]


def get_io_loop(asynchronous=None):
    if asynchronous:
        return IOLoop.current()

    if not _io_loops:
        loop = IOLoop()
        thread = threading.Thread(target=loop.start)
        thread.daemon = True
        thread.start()
        _io_loops.append(loop)

    return _io_loops[-1]


def stream_init(self, upstream=None, upstreams=None, stream_name=None,
                loop=None, asynchronous=None, ensure_io_loop=False):
    self.downstreams = OrderedWeakrefSet()
    if upstreams is not None:
        self.upstreams = list(upstreams)
    else:
        self.upstreams = [upstream]

    self._set_asynchronous(asynchronous)
    self._set_loop(loop)
    if ensure_io_loop and not self.loop:
        self._set_asynchronous(False)
    if self.loop is None and self.asynchronous is not None:
        self._set_loop(get_io_loop(self.asynchronous))

    for upstream in self.upstreams:
        if upstream:
            upstream.downstreams.add(self)

    self.name = stream_name


Stream.__init__ = stream_init


def _set_loop(self, loop):
    self.loop = None
    if loop is not None:
        self._inform_loop(loop)
    else:
        for upstream in self.upstreams:
            if upstream and upstream.loop:
                self.loop = upstream.loop
                break


Stream._set_loop = _set_loop


def _inform_loop(self, loop):
    """
    Percolate information about an event loop to the rest of the stream
    """
    if self.loop is not None:
        if self.loop is not loop:
            raise ValueError("Two different event loops active")
    else:
        self.loop = loop
        for upstream in self.upstreams:
            if upstream:
                upstream._inform_loop(loop)
        for downstream in self.downstreams:
            if downstream:
                downstream._inform_loop(loop)


Stream._inform_loop = Stream._inform_loop


def _set_asynchronous(self, asynchronous):
    self.asynchronous = None
    if asynchronous is not None:
        self._inform_asynchronous(asynchronous)
    else:
        for upstream in self.upstreams:
            if upstream and upstream.asynchronous:
                self.asynchronous = upstream.asynchronous
                break


Stream._set_asynchronous = _set_asynchronous


def _inform_asynchronous(self, asynchronous):
    """
    Percolate information about an event loop to the rest of the stream
    """
    if self.asynchronous is not None:
        if self.asynchronous is not asynchronous:
            raise ValueError(
                "Stream has both asynchronous and synchronous elements")
    else:
        self.asynchronous = asynchronous
        for upstream in self.upstreams:
            if upstream:
                upstream._inform_asynchronous(asynchronous)
        for downstream in self.downstreams:
            if downstream:
                downstream._inform_asynchronous(asynchronous)


Stream._inform_asynchronous = _inform_asynchronous


def scatter(self, **kwargs):
    from .parallel import scatter
    return scatter(self, **kwargs)


Stream.scatter = scatter


def emit(self, x, asynchronous=False):
    """ Push data into the stream at this point

    This is typically done only at source Streams but can theortically be
    done at any point
    """
    ts_async = getattr(thread_state, 'asynchronous', False)
    if self.loop is None or asynchronous or self.asynchronous or ts_async:
        if not ts_async:
            thread_state.asynchronous = True
        try:
            result = self._emit(x)
            if self.loop:
                return gen.convert_yielded(result)
        finally:
            thread_state.asynchronous = ts_async
    else:
        @gen.coroutine
        def _():
            thread_state.asynchronous = True
            try:
                result = yield self._emit(x)
            finally:
                del thread_state.asynchronous

            raise gen.Return(result)

        sync(self.loop, _)


Stream.emit = emit


@Stream.register_api()
class starsink(Stream):
    """ Apply a function on every element

    Examples
    --------
    >>> source = Stream()
    >>> L = list()
    >>> source.sink(L.append)
    >>> source.sink(print)
    >>> source.sink(print)
    >>> source.emit(123)
    123
    123
    >>> L
    [123]

    See Also
    --------
    map
    Stream.sink_to_list
    """

    _graphviz_shape = "trapezium"

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # take the stream specific kwargs out
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)
        _global_sinks.add(self)

    def update(self, x, who=None):
        y = x + self.args
        result = self.func(*y, **self.kwargs)
        if gen.isawaitable(result):
            return result
        else:
            return []


@Stream.register_api()
class filter(Stream):
    """ Only pass through elements that satisfy the predicate

    Parameters
    ----------
    predicate : function
        The predicate. Should return True or False, where
        True means that the predicate is satisfied.

    Examples
    --------
    >>> source = Stream()
    >>> source.filter(lambda x: x % 2 == 0).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    2
    4
    """

    def __init__(self, upstream, predicate, *args, **kwargs):
        if predicate is None:
            predicate = _truthy
        self.predicate = predicate
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None):
        if self.predicate(x, *self.args, **self.kwargs):
            return self._emit(x)


@Stream.register_api()
class unique(Stream):
    """ Avoid sending through repeated elements

    This deduplicates a stream so that only new elements pass through.
    You can control how much of a history is stored with the ``history=``
    parameter.  For example setting ``history=1`` avoids sending through
    elements when one is repeated right after the other.

    Examples
    --------
    >>> source = Stream()
    >>> source.unique(history=1).sink(print)
    >>> for x in [1, 1, 2, 2, 2, 1, 3]:
    ...     source.emit(x)
    1
    2
    1
    3
    """

    def __init__(self, upstream, history=None, key=identity, **kwargs):
        self.seen = dict()
        self.key = key
        if history:
            from zict import LRU

            self.seen = LRU(history, self.seen)
            self.non_hash_seen = deque(maxlen=history)

        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None):
        y = self.key(x)
        # If y is a dict then we can't use LRU cache use FILO deque instead
        if not isinstance(y, Hashable):
            if y not in self.non_hash_seen:
                self.non_hash_seen.append(y)
                return self._emit(x)
        else:
            if y not in self.seen:
                self.seen[y] = 1
                return self._emit(x)


def move_to_first(node, f=True):
    """Promote current node to first in the execution order

    Parameters
    ----------
    node : Streamz instance
        Node to be promoted
    f : bool or Sequence of Streamz
        The upstream node(s) to promote this node for. If True, promote all
        upstream nodes. Defaults to True

    Notes
    -----
    This is often used for saving data, since saving data before the rest of
    the data is processed makes sure that all the data that can be saved
    (before an exception is hit) is saved.
    """
    if f is True:
        f = node.upstreams
    if not isinstance(f, Sequence):
        f = (f,)
    for upstream in f:
        for n in upstream.downstreams.data:
            if n() is node:
                break
        upstream.downstreams.data._od.move_to_end(n, last=False)
        del n
    return node


@Stream.register_api()
class combine_latest(_combine_latest):
    """ Combine multiple streams together to a stream of tuples

    This will emit a new tuple of all of the most recent elements seen from
    any stream.

    Parameters
    ----------
    emit_on : stream or list of streams or None
        only emit upon update of the streams listed.
        If None, emit on update from any stream
    first : stream or iterable of streams or None
        reorder the listed upstream nodes' emit order to emit to this node
        first. If None, do not reorder emits, defaults to None.

    See Also
    --------
    zip
    """

    def __init__(self, *upstreams, **kwargs):
        first = kwargs.pop("first", None)

        _combine_latest.__init__(self, *upstreams, **kwargs)
        if first:
            move_to_first(self, first)


@Stream.register_api()
class zip(_zip):
    """ Combine streams together into a stream of tuples

    We emit a new tuple once all streams have produce a new tuple.

    Parameters
    ----------
    first : stream or iterable of streams or None
        reorder the listed upstream nodes' emit order to emit to this node
        first. If None, do not reorder emits, defaults to None.

    See also
    --------
    combine_latest
    zip_latest
    """

    def __init__(self, *upstreams, **kwargs):
        first = kwargs.pop("first", None)

        _zip.__init__(self, *upstreams, **kwargs)
        if first:
            move_to_first(self, first)


@Stream.register_api()
class zip_latest(_zip_latest):
    """Combine multiple streams together to a stream of tuples

        The stream which this is called from is lossless. All elements from
        the lossless stream are emitted reguardless of when they came in.
        This will emit a new tuple consisting of an element from the lossless
        stream paired with the latest elements from the other streams.
        Elements are only emitted when an element on the lossless stream are
        received, similar to ``combine_latest`` with the ``emit_on`` flag.

    Parameters
    ----------
    first : stream or iterable of streams or None
        reorder the listed upstream nodes' emit order to emit to this node
        first. If None, do not reorder emits, defaults to None.

        See Also
        --------
        Stream.combine_latest
        Stream.zip
        """

    def __init__(self, *upstreams, **kwargs):
        first = kwargs.pop("first", None)

        _zip_latest.__init__(self, *upstreams, **kwargs)
        if first:
            move_to_first(self, first)


def destroy_pipeline(source_node: Stream):
    """Destroy all the nodes attached to the source

    Parameters
    ----------
    source_node : Stream
        The source node for the pipeline
    """
    for ds in list(source_node.downstreams):
        destroy_pipeline(ds)
    if source_node.upstreams:
        try:
            source_node.destroy()
        # some source nodes are tuples and some are bad wekrefs
        except (AttributeError, KeyError) as e:
            pass
