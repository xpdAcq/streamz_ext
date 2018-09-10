from .core import *
from .graph import *
from .sources import *
try:
    from .dask import DaskStream, scatter
except ImportError:
    pass

__version__ = '0.2.2'
