from streamz_ext.core import combine_latest, zip_latest
from streamz.graph import *
from streamz.graph import _clean_text, _get_display_cls


def create_graph(node, graph, prior_node=None, pc=None):
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
                   fillcolor=node._graphviz_fillcolor)
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
            graph.add_edge(tt, t, **edge_kwargs)
        else:
            graph.add_edge(t, tt, **edge_kwargs)

    for nodes, pc in zip([list(node.downstreams), list(node.upstreams)],
                         ['downstream', 'upstreams']):
        for node2 in nodes:
            if node2 is not None:
                create_graph(node2, graph, node, pc=pc)


def create_edge_label_graph(node, graph, prior_node=None, pc=None, i=None):
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
                   fillcolor=node._graphviz_fillcolor)
    if prior_node:
        tt = hash(prior_node)
        # If we emit on something other than all the upstreams vis it
        if ((isinstance(node, combine_latest)
                and node.emit_on != node.upstreams
                and prior_node in node.emit_on)
            or (isinstance(node, zip_latest)
                and node.lossless == prior_node)):
            edge_kwargs['style'] = 'dashed'
        if graph.has_edge(t, tt):
            return
        if i is None:
            i = ''
        if pc == 'downstream':
            graph.add_edge(tt, t, label=str(i), **edge_kwargs)
        else:
            graph.add_edge(t, tt, **edge_kwargs)

    for nodes, pc in zip([list(node.downstreams), list(node.upstreams)],
                         ['downstream', 'upstreams']):
        for i, node2 in enumerate(nodes):
            if node2 is not None:
                if len(nodes) > 1:
                    create_edge_label_graph(node2, graph, node, pc=pc, i=i)
                else:
                    create_edge_label_graph(node2, graph, node, pc=pc)


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
    return rg


def to_graphviz(graph, **graph_attr):
    import graphviz
    gvz = graphviz.Digraph(graph_attr=graph_attr)
    for node, attrs in graph.node.items():
        gvz.node(node, **attrs)
    for edge, attrs in graph.edges().items():
        gvz.edge(edge[0], edge[1], **attrs)
    return gvz


def visualize(node, filename='mystream.png', source_node=False, **kwargs):
    """
    Render a task graph using dot.

    If `filename` is not None, write a file to disk with that name in the
    format specified by `format`.  `filename` should not include an extension.

    Parameters
    ----------
    node : Stream instance
        The stream to display.
    filename : str or None, optional
        The name (without an extension) of the file to write to disk.  If
        `filename` is None, no file will be written, and we communicate with
        dot using only pipes.  Default is 'mydask'.
    format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
        Format in which to write output file.  Default is 'png'.

    Returns
    -------
    result : None or IPython.display.Image or IPython.display.SVG  (See below.)

    Notes
    -----
    If IPython is installed, we return an IPython.display object in the
    requested format.  If IPython is not installed, we just return None.

    We always return None if format is 'pdf' or 'dot', because IPython can't
    display these formats natively. Passing these formats with filename=None
    will not produce any useful output.

    See Also
    --------
    streams.graph.readable_graph
    """
    rg = readable_graph(node, source_node=source_node)
    g = to_graphviz(rg, **kwargs)

    fmts = ['.png', '.pdf', '.dot', '.svg', '.jpeg', '.jpg']
    if filename is None:
        format = 'png'

    elif any(filename.lower().endswith(fmt) for fmt in fmts):
        filename, format = os.path.splitext(filename)
        format = format[1:].lower()

    else:
        format = 'png'

    data = g.pipe(format=format)
    if not data:
        raise RuntimeError("Graphviz failed to properly produce an image. "
                           "This probably means your installation of graphviz "
                           "is missing png support. See: "
                           "https://github.com/ContinuumIO/anaconda-issues/"
                           "issues/485 for more information.")

    display_cls = _get_display_cls(format)

    if not filename:
        return display_cls(data=data)

    full_filename = '.'.join([filename, format])
    with open(full_filename, 'wb') as f:
        f.write(data)

    return display_cls(filename=full_filename)
