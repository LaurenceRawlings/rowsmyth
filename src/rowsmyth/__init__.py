"""
Rowsmyth: declarative relational test data as Spark DataFrames.

Generate seed datasets row-by-row with real foreign-key integrity.
"""

from rowsmyth.dataset import Dataset, RowCtx
from rowsmyth.factory import Factory
from rowsmyth.model import (
    Model,
    WrongDeclarativeBaseError,
    declarative_base,
    variant,
)
from rowsmyth.pool import Pool

try:
    from rowsmyth._version import __version__
except ImportError:
    __version__ = "0.0.0"

__all__ = [
    "Dataset",
    "Factory",
    "Model",
    "Pool",
    "RowCtx",
    "WrongDeclarativeBaseError",
    "__version__",
    "declarative_base",
    "variant",
]
