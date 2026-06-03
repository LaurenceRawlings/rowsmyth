"""
Rowsmyth: declarative relational test data as Spark DataFrames.

Generate seed datasets row-by-row with real foreign-key integrity.
"""

from rowsmyth.context import Generation, RowCtx, generate
from rowsmyth.factory import Factory
from rowsmyth.pool import Pool
from rowsmyth.table import Model, variant

try:
    from rowsmyth._version import __version__
except ImportError:
    __version__ = "0.0.0"

__all__ = [
    "Factory",
    "Generation",
    "Model",
    "Pool",
    "RowCtx",
    "__version__",
    "generate",
    "variant",
]
