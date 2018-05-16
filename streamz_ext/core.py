from collections import MutableMapping

from streamz.core import *
from streamz.core import _global_sinks, _truthy


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
    _graphviz_shape = 'trapezium'

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
        stream_name = kwargs.pop('stream_name', None)
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
            self.dict_seen = deque(maxlen=history)

        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None):
        y = self.key(x)
        # If y is a dict then we can't use LRU cache use FILO deque instead
        if isinstance(y, MutableMapping):
            if y not in self.dict_seen:
                self.dict_seen.append(y)
                return self._emit(x)
        else:
            if y not in self.seen:
                self.seen[y] = 1
                return self._emit(x)
