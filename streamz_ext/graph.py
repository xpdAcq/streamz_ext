from functools import wraps
from weakref import ref

from streamz.graph import *
from streamz.graph import _clean_text

import networkx as nx


def create_graph_nodes(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
    """
    edge_kwargs = {}
    if node is None:
        return
    t = hash(node)
    graph.add_node(t,
                   label=_clean_text(str(node)),
                   shape=node._graphviz_shape,
                   orientation=str(node._graphviz_orientation),
                   style=node._graphviz_style,
                   fillcolor=node._graphviz_fillcolor,
                   node=ref(node))
    if prior_node:
        tt = hash(prior_node)
        # If we emit on something other than all the upstreams vis it
        if (isinstance(node, combine_latest)
                and node.emit_on != node.upstreams
                and prior_node in node.emit_on):
            edge_kwargs['style'] = 'dashed'
        if graph.has_edge(t, tt):
            return
        if pc == 'downstream':
            graph.add_edge(tt, t)
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip([list(node.downstreams), list(node.upstreams)],
                         ['downstream', 'upstreams']):
        for node2 in nodes:
            if node2 is not None:
                create_graph_nodes(node2, graph, node, pc=pc)


def readable_graph(node, source_node=False):
    """Create human readable version of this object's task graph.

    Parameters
    ----------
    node: Stream instance
        A node in the task graph
    """
    import networkx as nx
    g = nx.DiGraph()
    if source_node:
        create_edge_label_graph(node, g)
    else:
        create_graph(node, g)
    mapping = {k: '{}'.format(g.node[k]['label']) for k in g}
    idx_mapping = {}
    for k, v in mapping.items():
        if v in idx_mapping.keys():
            idx_mapping[v] += 1
            mapping[k] += '-{}'.format(idx_mapping[v])
        else:
            idx_mapping[v] = 0

    gg = {k: v for k, v in mapping.items()}
    rg = nx.relabel_nodes(g, gg, copy=True)
    return rg, gg


def decorate_nodes(graph, decorator, *args, **kwargs):
    for n, attrs in graph.nodes.items():
        attrs['node']().update = decorator(attrs['node']().update, *args,
                                           **kwargs)


def run_vis(node, **kwargs):
    g, gg = readable_graph(node, **kwargs)
    gz = to_graphviz(g)
    gz.view()

    def live_vis_descorator(func):
        node = hash(func.__self__)
        node_name = gg[node]

        # @wraps
        def wrapps(*args, **kwargs):
            g.nodes[node_name]['fillcolor'] = 'yellow'
            gz = to_graphviz(g)
            gz.view()
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                g.nodes[node_name]['fillcolor'] = 'red'
                gz = to_graphviz(g)
                gz.view()
                raise e
            else:
                g.nodes[node_name]['fillcolor'] = 'green'
                gz = to_graphviz(g)
                gz.view()
                return ret

        return wrapps

    node_g = nx.DiGraph()
    create_graph_nodes(node, node_g)
    decorate_nodes(node_g, live_vis_descorator)


if __name__ == '__main__':
    from streamz_ext import Stream
    import time

    source = Stream()


    def sleep_inc(x):
        time.sleep(3)
        return x + 1


    b = source.map(sleep_inc)
    c = b.sink(print)
    run_vis(source)
    for i in range(10):
        source.emit(i)
        time.sleep(1)
