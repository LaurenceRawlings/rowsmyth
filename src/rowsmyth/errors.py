"""Rowsmyth exception hierarchy."""

from __future__ import annotations


class RowsmythError(Exception):
    """Base class for all rowsmyth domain errors."""


class RowsmythWarning(UserWarning):
    """Base class for all rowsmyth warnings."""


class DatasetError(RowsmythError):
    """Raised for dataset session configuration, lookup or state failures."""


class DatasetContextError(DatasetError):
    """Raised when a factory operation needs an active dataset context."""


class DatasetConfigurationError(DatasetError):
    """Raised when a dataset session is configured with unsupported options."""


class DatasetLookupError(DatasetError):
    """Raised when looking up committed dataset outputs fails."""


class TableNotFoundError(DatasetLookupError):
    """Raised when a requested table has not been created in a dataset."""


class ViewCollisionError(DatasetError):
    """Raised when two declarative bases register the same temp view name."""


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


class ColumnTypeError(SchemaError):
    """Raised when a generated value does not match its declared Spark type."""


class IntegrityError(RowsmythError):
    """Raised when generated rows break a declared relational constraint."""


class DuplicatePrimaryKeyError(IntegrityError):
    """Raised when two rows in one table share a primary key."""


class ForeignKeyViolationError(IntegrityError):
    """Raised when a declared foreign key points at a missing parent row."""


class PoolError(RowsmythError):
    """Raised for pool lookup failures."""


class EmptyPoolError(PoolError):
    """Raised when a pool source contains no values."""


class PoolSampleError(PoolError):
    """Raised when a pool sample request cannot be satisfied."""


class FactoryError(RowsmythError):
    """Raised for invalid factory configuration or use."""


class CompoundPrimaryKeyError(FactoryError):
    """Raised when a scalar FK factory targets a compound primary key."""


class LazyValueError(FactoryError):
    """Raised when :func:`rowsmyth.lazy` is given a non-callable value."""


class VariantError(RowsmythError):
    """Raised for model variant lookup or execution failures."""


class UnknownVariantError(VariantError):
    """Raised when a named variant is not declared on a model."""
