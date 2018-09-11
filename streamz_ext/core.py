from collections import Hashable
from collections.abc import Sequence

from streamz.core import *
from streamz.core import (
    combine_latest as _combine_latest,
    zip as _zip,
    zip_latest as _zip_latest,
)
from streamz.core import _global_sinks, _truthy


def scatter(self, **kwargs):
    from .parallel import scatter
    return scatter(self, **kwargs)


Stream.scatter = scatter


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
