

# TODO: should the inputs and outputs be defined as kwargs for the pipeline
# generating function?
# This would make the requirements more explicit and have a linker error
# maybe we use sig f(*, kwarg1, kwarg2, **kwargs)?
def micro_link(input_graph, output_graph):
    """Link to dicts of sub graphs together by their keys

    Parameters
    ----------
    input_graph : dict of Streams
    output_graph : dict of Streams
    """
    for name in output_graph:
        if name in input_graph:
            input_graph[name].connect(output_graph[name])
    # TODO: idiomatic way to do this? (essentially left update)
    input_graph.update(
        {k: v for k, v in output_graph.items() if k not in input_graph})


# TODO: support bypass? Maybe that should be a separate pipeline optimization
# step
def link(*args):
    """Link a series of sub graphs in order

    Parameters
    ----------
    args : iterable of dicts of Streams

    Returns
    -------
    dict of Streams:
        The the final pipeline dict

    """
    total_graph = {}
    for graph in args:
        micro_link(total_graph, graph)
    return total_graph
