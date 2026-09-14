"""
Rowsmyth: declarative relational test data as Spark DataFrames.

Generate seed datasets with real foreign-key integrity, then materialise
ordinary DataFrames and temp views.
"""

from rowsmyth.dataset import Dataset, RowCtx
from rowsmyth.errors import (
    ColumnTypeError,
    CompoundPrimaryKeyError,
    DatasetConfigurationError,
    DatasetContextError,
    DatasetError,
    DatasetLookupError,
    DeclarativeBaseError,
    DuplicatePrimaryKeyError,
    EmptyPoolError,
    FactoryError,
    ForeignKeyViolationError,
    IntegrityError,
    InvalidDeclarativeBaseError,
    InvalidModelDefinitionError,
    LazyValueError,
    MissingRequiredColumnError,
    PoolError,
    PoolSampleError,
    ReservedColumnError,
    RowsmythError,
    RowsmythWarning,
    SchemaError,
    TableNotFoundError,
    UnknownColumnError,
    UnknownVariantError,
    VariantError,
    ViewCollisionError,
    WrongDeclarativeBaseError,
)
from rowsmyth.factory import Factory
from rowsmyth.lazy import Lazy, lazy
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
    "ColumnTypeError",
    "CompoundPrimaryKeyError",
    "Dataset",
    "DatasetConfigurationError",
    "DatasetContextError",
    "DatasetError",
    "DatasetLookupError",
    "DeclarativeBaseError",
    "DuplicatePrimaryKeyError",
    "EmptyPoolError",
    "Factory",
    "FactoryError",
    "ForeignKeyViolationError",
    "IntegrityError",
    "InvalidDeclarativeBaseError",
    "InvalidModelDefinitionError",
    "Lazy",
    "LazyValueError",
    "MissingRequiredColumnError",
    "Model",
    "Pool",
    "PoolError",
    "PoolSampleError",
    "ReservedColumnError",
    "RowCtx",
    "RowsmythError",
    "RowsmythWarning",
    "SchemaError",
    "TableNotFoundError",
    "UnknownColumnError",
    "UnknownVariantError",
    "VariantError",
    "ViewCollisionError",
    "WrongDeclarativeBaseError",
    "__version__",
    "declarative_base",
    "lazy",
    "variant",
]
