"""
Rowsmyth: declarative relational test data as Spark DataFrames.

Generate seed datasets row-by-row with real foreign-key integrity.
"""

from rowsmyth.dataset import Dataset, RowCtx
from rowsmyth.errors import (
    CompoundPrimaryKeyError,
    DataframeNotFoundError,
    DatasetContextError,
    DatasetLookupError,
    DeclarativeBaseError,
    EmptyPoolError,
    FactoryError,
    InvalidDeclarativeBaseError,
    InvalidModelDefinitionError,
    MissingRequiredColumnError,
    PoolError,
    PoolSampleError,
    ReservedColumnError,
    RowsmythError,
    SchemaError,
    UnknownColumnError,
    UnknownVariantError,
    UnresolvedPoolError,
    VariantError,
    WrongDeclarativeBaseError,
)
from rowsmyth.factory import Factory
from rowsmyth.model import (
    Model,
    declarative_base,
    variant,
)
from rowsmyth.pool import Pool

try:
    from rowsmyth._version import __version__
except ImportError:
    __version__ = "0.0.0"

__all__ = [
    "CompoundPrimaryKeyError",
    "DataframeNotFoundError",
    "Dataset",
    "DatasetContextError",
    "DatasetLookupError",
    "DeclarativeBaseError",
    "EmptyPoolError",
    "Factory",
    "FactoryError",
    "InvalidDeclarativeBaseError",
    "InvalidModelDefinitionError",
    "MissingRequiredColumnError",
    "Model",
    "Pool",
    "PoolError",
    "PoolSampleError",
    "ReservedColumnError",
    "RowCtx",
    "RowsmythError",
    "SchemaError",
    "UnknownColumnError",
    "UnknownVariantError",
    "UnresolvedPoolError",
    "VariantError",
    "WrongDeclarativeBaseError",
    "__version__",
    "declarative_base",
    "variant",
]
