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
