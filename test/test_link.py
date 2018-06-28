from streamz_ext.link import *
from streamz_ext import Stream
try:
    from streamz.tests.test_link import *
except ImportError:
    pass


def test_link():
    def make_a():
        source = Stream()
        out_a = source.map(lambda x: x + 1)
        return {'in_a': source, 'out_a': out_a}

    def make_b():
        out_a = Stream()
        out_b = out_a.map(lambda x: x * 2)
        return {'out_a': out_a, 'out_b': out_b}
    a = make_a()
    b = make_b()
    L = b['out_b'].sink_to_list()
    for i in range(10):
        a['in_a'].emit(i)
    assert len(L) == 0
    link(a, b)
    for i in range(10):
        a['in_a'].emit(i)
    assert L == [(i + 1) * 2 for i in range(10)]


# def test_link_bypass():
#     def make_a():
#         source = Stream()
#         out_a = source.map(lambda x: x + 1)
#         return {'in_a': source, 'out_a': out_a}
#
#     def make_b():
#         out_a = Stream()
#         out_b = out_a.map(lambda x: x * 2)
#         return {'out_a': out_a, 'out_b': out_b}
#     a = make_a()
#     b = make_b()
#     L = b['out_b'].sink_to_list()
#     for i in range(10):
#         a['in_a'].emit(i)
#     assert len(L) == 0
#     link(a, b, bypass=True)
#     for i in range(10):
#         a['in_a'].emit(i)
#     assert L == [(i + 1) * 2 for i in range(10)]


def test_double_link():
    def make_a():
        source = Stream()
        out_a = source.map(lambda x: x + 1)
        return {'in_a': source, 'out_a': out_a}

    def make_b():
        out_a = Stream()
        out_b = out_a.map(lambda x: x * 2)
        return {'out_a': out_a, 'out_b': out_b}

    def make_c():
        out_a = Stream()
        out_b = Stream()
        out_c = out_a.zip(out_b).map(sum)
        return {'out_a': out_a, 'out_b': out_b, 'out_c': out_c}

    a = make_a()
    b = make_b()
    L = b['out_b'].sink_to_list()
    ab = link(a, b)
    c = make_c()
    L2 = c['out_c'].sink_to_list()
    abc = link(ab, c)

    for i in range(10):
        a['in_a'].emit(i)
    assert L == [(i + 1) * 2 for i in range(10)]
    assert L2 == [((i + 1) * 2) + i + 1 for i in range(10)]


# def test_double_link_bypass():
#     def make_a():
#         source = Stream()
#         out_a = source.map(lambda x: x + 1)
#         return {'in_a': source, 'out_a': out_a}
#
#     def make_b():
#         out_a = Stream()
#         out_b = out_a.map(lambda x: x * 2)
#         return {'out_a': out_a, 'out_b': out_b}
#
#     def make_c():
#         out_a = Stream()
#         out_b = Stream()
#         out_c = out_a.zip(out_b).map(sum)
#         return {'out_a': out_a, 'out_b': out_b, 'out_c': out_c}
#
#     a = make_a()
#     b = make_b()
#     L = b['out_b'].sink_to_list()
#     ab = link(a, b, bypass=True)
#     c = make_c()
#     L2 = c['out_c'].sink_to_list()
#     abc = link(ab, c, bypass=True)
#
#     for i in range(10):
#         a['in_a'].emit(i)
#     assert L == [(i + 1) * 2 for i in range(10)]
#     assert L2 == [((i + 1) * 2) + i + 1 for i in range(10)]
