from streamz.tests.test_core import *
from streamz_ext import Stream


def test_star_sink():
    L = []

    def add(x, y):
        L.append(x + y)

    source = Stream()
    source.starsink(add)

    source.emit((1, 10))

    assert L[0] == 11


def test_unique_dict():
    source = Stream()
    L = source.unique(history=1).sink_to_list()

    source.emit({'a': 1})
    source.emit({'a': 1})
    source.emit({'a': 1})

    assert L == [{'a': 1}]


def test_execution_order():
    L = []
    for i in range(5):
        s = Stream()
        b = s.pluck(1)
        a = s.pluck(0)
        l = a.combine_latest(b, emit_on=a).sink_to_list()
        z = [(1, 'red'), (2, 'blue'), (3, 'green')]
        for zz in z:
            s.emit(zz)
        L.append((l,))
    for ll in L:
        assert ll == L[0]

    L2 = []
    for i in range(5):
        s = Stream()
        a = s.pluck(0)
        b = s.pluck(1)
        l = a.combine_latest(b, emit_on=a).sink_to_list()
        z = [(1, 'red'), (2, 'blue'), (3, 'green')]
        for zz in z:
            s.emit(zz)
        L2.append((l,))
    for ll, ll2 in zip(L, L2):
        assert ll2 == L2[0]
        assert ll != ll2


def test_starmap():
    def add(x=0, y=0):
        return x + y

    source = Stream()
    L = source.starmap(add).sink_to_list()

    source.emit((1, 10))

    assert L[0] == 11


def test_filter_args_kwargs():
    def f(x, y, z=False):
        print(y)
        print(z)
        return y and z

    source = Stream()
    L = source.filter(f, True, z=True).sink_to_list()
    source.emit(1)
    assert L[0] is 1
