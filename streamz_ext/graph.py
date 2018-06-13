from streamz.graph import *
from streamz.graph import _clean_text


def create_graph_with_node(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
    """
    if node is None:
        return
    t = hash(node)
    graph.add_node(t,
                   label=_clean_text(str(node)),
                   shape=node._graphviz_shape,
                   orientation=str(node._graphviz_orientation),
                   style=node._graphviz_style,
                   fillcolor=node._graphviz_fillcolor,
                   n=node)
    if prior_node:
        tt = hash(prior_node)
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
                create_graph(node2, graph, node, pc=pc)


def decorate_nodes(graph, decorator, *args, **kwargs):
    for n, attrs in graph.items():
        attrs['node'].update = decorator(attrs['node'].update, *args, **kwargs)


def run_vis(node, **kwargs):
    # TODO: this needs to include the actual nodes in the graph
    g = readable_graph(node, **kwargs)
    gz = to_graphviz(g)
    gz.view()

    def live_vis_descorator(func):
        # TODO: need link between the graph node names and the nodes
        # themselves so we can actually know node_name
        node = func.__self__

        def wrapps(data):
            g[node_name]['fillcolor'] = 'yellow'
            gz.render()
            try:
                ret = func(data)
            except Exception as e:
                g[node_name]['fillcolor'] = 'red'
                gz.render()
                raise e
            else:
                g[node_name]['fillcolor'] = 'green'
                gz.render()
                return ret
    decorate_nodes(g, live_vis_descorator)
