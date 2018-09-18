import matplotlib.pyplot as plt

from streamz_ext.graph import *

try:
    from streamz.tests.test_graph import *
except ImportError:
    pass


def test_run_vis_smoke():
    source = Stream()

    def sleep_inc(x):
        if x == 9:
            raise RuntimeError()
        plt.pause(.1)
        return x + 1

    def print_sleep(x):
        plt.pause(.1)
        print(x)

    b = source.map(sleep_inc)
    b.sink(print_sleep)
    b.sink(print_sleep)
    gv = run_vis(
        source,
        source_node=True,
        edge_style={"color": "k"},
        node_label_style={"font_size": 10},
        edge_label_style=lambda x: {"label": x["label"], "font_size": 15},
        node_style=node_style,
        force_draw=True,
    )
    plt.pause(.1)
    for i in range(10):
        try:
            source.emit(i)
            plt.pause(.1)
        except RuntimeError:
            pass
    plt.pause(.1)
