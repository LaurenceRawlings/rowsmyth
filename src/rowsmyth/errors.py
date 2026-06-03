"""Rowsmyth exception hierarchy."""

from __future__ import annotations


class RowsmythError(Exception):
    """Base class for all rowsmyth domain errors."""


class DatasetContextError(RowsmythError):
    """Raised when a factory operation needs an active dataset context."""


class DeclarativeBaseError(RowsmythError):
    """Raised for invalid declarative base usage."""


class InvalidDeclarativeBaseError(DeclarativeBaseError):
    """Raised when a model does not extend a rowsmyth declarative base."""


class WrongDeclarativeBaseError(DeclarativeBaseError):
    """Raised when a model is used with a dataset for another declarative base."""


class SchemaError(RowsmythError):
    """Raised for model schema or column validation failures."""


class InvalidModelDefinitionError(SchemaError):
    """Raised when model schema metadata is internally inconsistent."""


class ReservedColumnError(SchemaError):
    """Raised when a model declares a reserved rowsmyth column."""


class UnknownColumnError(SchemaError):
    """Raised when a caller provides columns absent from the model schema."""


class MissingRequiredColumnError(SchemaError):
    """Raised when a non-nullable column has no generated value."""


class PoolError(RowsmythError):
    """Raised for Spark pool lookup failures."""


class EmptyPoolError(PoolError):
    """Raised when a pool source contains no values."""


class PoolSampleError(PoolError):
    """Raised when a pool sample request cannot be satisfied."""


class UnresolvedPoolError(PoolError):
    """Raised when a deferred pool choice cannot be resolved."""


class FactoryError(RowsmythError):
    """Raised for invalid factory configuration or use."""


class CompoundPrimaryKeyError(FactoryError):
    """Raised when a scalar FK factory targets a compound primary key."""


class VariantError(RowsmythError):
    """Raised for model variant lookup or execution failures."""


class UnknownVariantError(VariantError):
    """Raised when a named variant is not declared on a model."""


class DatasetLookupError(RowsmythError):
    """Raised when looking up committed dataset outputs fails."""


class DataframeNotFoundError(DatasetLookupError):
    """Raised when a requested DataFrame has not been created in a dataset."""
