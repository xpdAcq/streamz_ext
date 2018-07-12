import networkx as nx

from .graph import StreamzGraph


def link(*args):
    """Link a series of sub graphs in order

    Parameters
    ----------
    args : iterable of StreamzGraph

    Returns
    -------
    StreamzGraph:
        The the final pipeline StreamzGraph

    """
    total_graph = StreamzGraph()
    for graph in args:
        total_graph = nx.compose(graph, total_graph)
    return total_graph
