"""Tests for the public rowsmyth error hierarchy."""

from __future__ import annotations

from rowsmyth import (
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


def test_domain_errors_share_rowsmyth_root() -> None:
    errors = [
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
        SchemaError,
        UnknownColumnError,
        UnknownVariantError,
        UnresolvedPoolError,
        VariantError,
        WrongDeclarativeBaseError,
    ]
    assert all(issubclass(error, RowsmythError) for error in errors)


def test_group_errors_have_expected_specific_subclasses() -> None:
    assert issubclass(WrongDeclarativeBaseError, DeclarativeBaseError)
    assert issubclass(InvalidDeclarativeBaseError, DeclarativeBaseError)
    assert issubclass(InvalidModelDefinitionError, SchemaError)
    assert issubclass(UnknownColumnError, SchemaError)
    assert issubclass(ReservedColumnError, SchemaError)
    assert issubclass(MissingRequiredColumnError, SchemaError)
    assert issubclass(EmptyPoolError, PoolError)
    assert issubclass(PoolSampleError, PoolError)
    assert issubclass(UnresolvedPoolError, PoolError)
    assert issubclass(CompoundPrimaryKeyError, FactoryError)
    assert issubclass(UnknownVariantError, VariantError)
    assert issubclass(DataframeNotFoundError, DatasetLookupError)
