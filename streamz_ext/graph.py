import networkx as nx

from streamz.graph import *
from streamz_ext import Stream
from typing import Type


class StreamzGraph(nx.DiGraph):

    def _node_name(self, node):
        n = getattr(node, 'name', hash(node))
        if n is None:
            n = hash(node)
        return n

    def add_node(self, node: Type[Stream] or Stream, *args, **kwargs):
        # None is used as a place holder for the upstream/upstreams if any
        if isinstance(node, type) or callable(node):
            node = node(None, *args, **kwargs)
        n = self._node_name(node)
        super().add_node(n, node=node)
        return n

    def add_edge(self, u: str or tuple, v: str or tuple, **attrs):
        new_uv = []
        for uv in [u, v]:
            if isinstance(uv, tuple):
                a, b, c = uv
                if not b:
                    b = ()
                if not c:
                    c = {}
                new_uv.append(self.add_node(a, *b, **c))
            else:
                new_uv.append(uv)
        u, v = new_uv
        if (u, v) in self.edges or (v, u) in self.edges:
            return
        # Need to remove old references when overriding

        un = self.nodes[u]['node']
        vn = self.nodes[v]['node']
        if un not in vn.upstreams:
            if self._node_name(un) in [self._node_name(n) for n in vn.upstreams]:
                # get the index of the node to be replaced
                l = [self._node_name(n) for n in vn.upstreams]
                idx = l.index(self._node_name(un))
                # perform the connection
                un.connect(vn)
                # pop the old to make way for the new
                new = vn.upstreams.pop(-1)
                vn.upstreams.insert(idx, new)
                vn.upstreams[idx + 1].disconnect(vn)
            else:
                un.connect(vn)
        super().add_edge(u, v)

    def add_edges_from(self, ebunch, **attrs):
        for u, v, *d in ebunch:
            self.add_edge(u, v)

    def add_nodes_from(self, nodes, **attr):
        for n in nodes:
            if len(n) == 2:
                n = n[1]['node']
            self.add_node(n)

    def fresh_copy(self):
        return StreamzGraph()


def _create_streamz_graph(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: Stream instance
    graph: StreamzGraph instance
    """
    if node is None:
        return
    t = graph.add_node(node)
    if prior_node:
        tt = getattr(prior_node, 'name', hash(prior_node))
        if tt is None:
            tt = hash(prior_node)
        if graph.has_edge(t, tt) or graph.has_edge(tt, t):
            return
        if pc == 'downstream':
            graph.add_edge(tt, t)
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip([list(node.downstreams), list(node.upstreams)],
                         ['downstream', 'upstreams']):
        for node2 in nodes:
            if node2 is not None:
                _create_streamz_graph(node2, graph, node, pc=pc)


def create_streamz_graph(node):
    g = StreamzGraph()
    _create_streamz_graph(node, g)
    return g
