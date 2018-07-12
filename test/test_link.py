from streamz_ext.link import *
from streamz_ext import Stream, create_streamz_graph
try:
    from streamz.tests.test_link import *
except ImportError:
    pass


def test_link():
    def make_a():
        out_a = Stream(stream_name='in_a').map(lambda x: x + 1,
                                               stream_name='out_a')
        return create_streamz_graph(out_a)

    def make_b():
        out_b = Stream(stream_name='out_a').map(lambda x: x * 2,
                                                stream_name='out_b')
        return create_streamz_graph(out_b)
    a = make_a()
    b = make_b()
    L = b.node['out_b']['node'].sink_to_list()
    for i in range(10):
        a.node['in_a']['node'].emit(i)
    assert len(L) == 0
    z = link(a, b)
    for i in range(10):
        a.node['in_a']['node'].emit(i)
    assert L == [(i + 1) * 2 for i in range(10)]


def test_double_link():
    def make_a():
        out_a = Stream(stream_name='in_a').map(lambda x: x + 1,
                                               stream_name='out_a')
        return create_streamz_graph(out_a)

    def make_b():
        out_b = Stream(stream_name='out_a').map(lambda x: x * 2,
                                                stream_name='out_b')
        return create_streamz_graph(out_b)

    def make_c():
        a = Stream(stream_name='out_a')
        b = Stream(stream_name='out_b')
        a_zip = a.zip(b, stream_name='z')
        out_c = a_zip.map(sum, stream_name='out_c')
        return create_streamz_graph(out_c)

    a = make_a()
    b = make_b()
    L = b.node['out_b']['node'].sink_to_list()
    ab = link(a, b)
    c = make_c()
    L2 = c.node['out_c']['node'].sink_to_list()
    abc = link(ab, c)
    assert len(c.node['z']['node'].upstreams) == 2

    for i in range(10):
        abc.node['in_a']['node'].emit(i)
    assert L == [(i + 1) * 2 for i in range(10)]
    assert L2 == [((i + 1) * 2) + i + 1 for i in range(10)]
