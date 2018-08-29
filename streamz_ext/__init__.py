__version__ = "0.2.1"

from .core import *
from .graph import *
from .sources import *
from .thread import *

try:
    from .dask import DaskStream, scatter
except ImportError:
    pass
