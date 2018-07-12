from streamz_ext.graph import *

try:
    from streamz.tests.test_graph import *
except ImportError:
    pass


def test_streamz_graph():
    a = StreamzGraph()
    a.add_node(Stream, stream_name='a')
    z = Stream(stream_name='b')
    a.add_node(z)
    a.add_edge('a', (Stream.union, None, dict(stream_name='c')))
    a.add_edge('b', 'c')
    assert len(a) == 3
    l = a.nodes['c']['node'].sink_to_list()

    for i in range(5):
        a.nodes['a']['node'].emit(i)

    assert len(l) == 5


def test_compose_graph():
    def makea():
        a = StreamzGraph()
        a.add_node(Stream, stream_name='a')
        z = Stream(stream_name='b')
        a.add_node(z)
        a.add_edge('a', (Stream.union, None, dict(stream_name='c')))
        a.add_edge('b', 'c')
        return a

    l = []

    def makeb():
        a = StreamzGraph()
        a.add_node(Stream, stream_name='c')
        a.add_edge('c', (Stream.sink, (lambda x: l.append(x),), {}))
        return a

    z = makea()
    zz = makeb()
    zzz = nx.compose(zz, z)
    for i in range(5):
        zzz.nodes['a']['node'].emit(i)
        zzz.nodes['b']['node'].emit(i)
    assert len(l) == 10


def test_create_streamz_graph():
    a = Stream(stream_name='source')
    b = a.map(lambda x: x + 1)
    L = b.sink_to_list()

    z = create_streamz_graph(a)
    assert len(z) == 3
    ll = []
    for i in range(5):
        a.emit(i)
        ll.append(i + 1)
        z.nodes['source']['node'].emit(i)
        ll.append(i + 1)
    assert L == ll
