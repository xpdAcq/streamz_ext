from weakref import ref

import matplotlib.pyplot as plt
import networkx as nx
from grave import plot_network
from streamz import combine_latest
from streamz.graph import *
from streamz.graph import _clean_text
from streamz_ext import Stream


def create_graph_nodes(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain
    with weakrefs to nodes in the graph nodes

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
    """
    edge_kwargs = {}
    if node is None:
        return
    t = hash(node)
    graph.add_node(
        t,
        label=_clean_text(str(node)),
        shape=node._graphviz_shape,
        orientation=str(node._graphviz_orientation),
        style=node._graphviz_style,
        fillcolor=node._graphviz_fillcolor,
        node=ref(node),
    )
    if prior_node:
        tt = hash(prior_node)
        # If we emit on something other than all the upstreams vis it
        if (
            isinstance(node, combine_latest)
            and node.emit_on != node.upstreams
            and prior_node in node.emit_on
        ):
            edge_kwargs["style"] = "dashed"
        if graph.has_edge(t, tt):
            return
        if pc == "downstream":
            graph.add_edge(tt, t)
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip(
        [list(node.downstreams), list(node.upstreams)],
        ["downstream", "upstreams"],
    ):
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
    mapping = {k: "{}".format(g.node[k]["label"]) for k in g}
    idx_mapping = {}
    for k, v in mapping.items():
        if v in idx_mapping.keys():
            idx_mapping[v] += 1
            mapping[k] += "-{}".format(idx_mapping[v])
        else:
            idx_mapping[v] = 0

    gg = {k: v for k, v in mapping.items()}
    rg = nx.relabel_nodes(g, gg, copy=True)
    return rg, gg


class LiveGraphPlot(object):
    """Live plotting of the streamz graph status"""

    def __init__(
        self,
        graph,
        layout="spectral",
        node_style=None,
        edge_style=None,
        node_label_style=None,
        edge_label_style=None,
        ax=None,
        force_draw=False,
    ):
        """

        Parameters
        ----------
        graph : nx.Graph
            The graph to be plotted
        layout : string or callable, optional, default: "spectral"
            Specifies the type of layout to use for plotting.
            It must be one of "spring", "circular", "random", "kamada_kawai",
            "shell", "spectral", or a callable.
        node_style : dict or callable, optional
            The style parameters for nodes, if callable must return a dict
        edge_style : dict or callable, optional
            The style parameters for edges, if callable must return a dict
        node_label_style : dict or callable, optional
            The style parameters for node labels, if callable must return a dict
        edge_label_style : dict or callable, optional
            The style parameters for edge labels, if callable must return a dict
        ax : matplotlib axis object, optional
            The axis to plot on. If not provided produce fig and ax internally.
        force_draw : bool, optional
            If True force drawing every time graph is updated, else only draw
            when idle. Defaults to False
        """
        self.force_draw = force_draw
        if edge_label_style is None:
            edge_label_style = {}
        if node_label_style is None:
            node_label_style = {}
        if edge_style is None:
            edge_style = {}
        if node_style is None:
            node_style = {}
        self.node_style = node_style
        self.edge_style = edge_style
        self.node_label_style = node_label_style
        self.edge_label_style = edge_label_style
        self.layout = layout
        self.graph = graph
        if not ax:
            fig, ax = plt.subplots()
        self.ax = ax
        self.art = plot_network(
            self.graph,
            node_style=self.node_style,
            edge_style=self.edge_style,
            node_label_style=self.node_label_style,
            edge_label_style=self.edge_label_style,
            layout=self.layout,
            ax=self.ax,
        )
        self.update()

    def update(self):
        """Update the graph plot"""
        # TODO: reuse the current node positions (if no new nodes added)
        self.art._reprocess()
        if self.force_draw:
            plt.draw()
        else:
            self.ax.figure.canvas.draw_idle()


def decorate_nodes(graph, update_decorator=None, emit_decorator=None):
    """Decorate node methods for nodes in a graph

    Parameters
    ----------
    graph : nx.Graph instance
        The graph who's nodes are to be updated
    update_decorator : callable, optional
        The function to wrap the update method. If None no decorator is applied.
    emit_decorator : callable, optional
        The function to wrap the _emit method. If None no decorator is applied.

    Returns
    -------

    """
    for n, attrs in graph.nodes.items():
        nn = attrs["node"]()
        if nn.__class__ != Stream:
            if update_decorator:
                nn.update = update_decorator(attrs["node"]().update)
            if emit_decorator:
                nn._emit = emit_decorator(attrs["node"]()._emit)


status_color_map = {"running": "yellow", "waiting": "green", "error": "red"}


def node_style(node_attrs):
    d = {
        "size": 2000,
        "color": status_color_map.get(node_attrs.get("status", "NA"), "k"),
    }
    return d


def run_vis(node, source_node=False, **kwargs):
    """Start the visualization of a pipeline

    Parameters
    ----------
    node : Stream instance
        A node in the pipeline
    source_node : bool
        If True the input node is the source node and numbers the
        graph edges accordingly, defaults to False
    kwargs : Any
        kwargs passed to LiveGraphPlot

    Returns
    -------

    """
    g, gg = readable_graph(node, source_node=source_node)
    fig, ax = plt.subplots()
    gv = LiveGraphPlot(g, ax=ax, **kwargs)

    def update_decorator(func):
        node = hash(func.__self__)
        node_name = gg[node]

        # @wraps
        def wrapps(*args, **kwargs):
            g.nodes[node_name]["status"] = "running"
            gv.update()
            plt.pause(.1)
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                g.nodes[node_name]["status"] = "error"
                gv.update()
                raise e
            else:
                g.nodes[node_name]["status"] = "waiting"
                gv.update()
                return ret

        return wrapps

    def emit_decorator(func):
        node = hash(func.__self__)
        node_name = gg[node]

        def wrapps(*args, **kwargs):
            g.nodes[node_name]["status"] = "waiting"
            gv.update()
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                g.nodes[node_name]["status"] = "error"
                gv.update()
                raise e
            else:
                return ret

        return wrapps

    node_g = nx.DiGraph()
    create_graph_nodes(node, node_g)
    decorate_nodes(node_g, update_decorator, emit_decorator)
    return gv
