"""Foreign-key resolution and row validation."""

from __future__ import annotations

import datetime
import decimal
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from rowsmyth.errors import (
    ColumnTypeError,
    CompoundPrimaryKeyError,
    MissingRequiredColumnError,
    UnknownColumnError,
)
from rowsmyth.lazy import Lazy

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructField

    from rowsmyth.dataset import RowCtx
    from rowsmyth.factory import Factory
    from rowsmyth.model import Model


def new_parent(factory: Factory, ctx: RowCtx) -> Model:
    """Create one parent row and append it to the accumulator."""
    from rowsmyth.model import validate_dataset_base

    validate_dataset_base(factory._model, ctx._dataset)
    attrs = factory._row(ctx._dataset, ctx._acc, index=ctx.index, injected={})
    ctx._acc.setdefault(factory._model.__table_name__, []).append(attrs)
    return factory._model(**attrs)


def resolve_fk(child_factory: Factory, ctx: RowCtx, slot: str) -> Any:
    """Resolve a Factory used as a column value to a primary key."""
    from rowsmyth.model import validate_dataset_base

    table = child_factory._model
    validate_dataset_base(table, ctx._dataset)
    pk = table.__primary_key__
    if len(pk) != 1:
        msg = (
            f"{table.__table_name__}: Factory() as column value requires a "
            "single-column primary key; use ctx.parent() for compound keys"
        )
        raise CompoundPrimaryKeyError(msg)
    inst = ctx._parents.get(slot)
    if inst is None:
        inst = new_parent(child_factory, ctx)
        ctx._parents[slot] = inst
    return inst.attrs[pk[0]]


def resolve_row_values(attrs: dict[str, Any], ctx: RowCtx) -> None:
    """Resolve Factory and lazy values in place."""
    from rowsmyth.factory import Factory

    for col, value in list(attrs.items()):
        if isinstance(value, Factory):
            attrs[col] = resolve_fk(value, ctx, slot=col)
        elif isinstance(value, Lazy):
            attrs[col] = value.fn(ctx)


def validate_row(table: type[Model], attrs: dict[str, Any]) -> None:
    """Validate one generated row against the model schema."""
    name = table.__table_name__
    unknown = sorted(set(attrs) - table._field_names())
    if unknown:
        msg = f"{name}: unknown columns: {unknown}"
        raise UnknownColumnError(msg)

    missing = []
    nulls = []
    for field in table.__definition__.fields:
        value = attrs.get(field.name)
        if value is None:
            if not field.nullable:
                (missing if field.name not in attrs else nulls).append(field.name)
            continue
        if not isinstance(value, _accepted_types(field.dataType)):
            raise ColumnTypeError(_type_error(name, field, value))

    invalid = missing + nulls
    if invalid:
        msg = f"{name}: NOT NULL columns without a value: {invalid}"
        raise MissingRequiredColumnError(msg)


def apply_variant(
    table: type[Model],
    obj: Model,
    variant_name: str,
    ctx: RowCtx,
) -> dict[str, Any]:
    """Run a named variant method and return its partial override."""
    method = table._variants[variant_name]
    return dict(method(obj, ctx))


def _type_error(name: str, field: StructField, value: Any) -> str:
    accepted = ", ".join(
        sorted(cls.__name__ for cls in _accepted_types(field.dataType))
    )
    msg = (
        f"{name}.{field.name}: {field.dataType.simpleString()} column expects "
        f"{accepted}, got {type(value).__name__}"
    )
    if callable(value):
        msg = f"{msg}; wrap callables in rowsmyth.lazy() to defer them per row"
    return msg


def _accepted_types(data_type: DataType) -> tuple[type, ...]:
    return _type_map().get(type(data_type), (object,))


@lru_cache(maxsize=1)
def _type_map() -> dict[type, tuple[type, ...]]:
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        DateType,
        DayTimeIntervalType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        MapType,
        Row,
        ShortType,
        StringType,
        StructType,
        TimestampNTZType,
        TimestampType,
    )

    integers = (int,)
    reals = (float, int)
    return {
        ArrayType: (list, tuple),
        BinaryType: (bytearray, bytes),
        BooleanType: (bool,),
        ByteType: integers,
        DateType: (datetime.date,),
        DayTimeIntervalType: (datetime.timedelta,),
        DecimalType: (decimal.Decimal,),
        DoubleType: reals,
        FloatType: reals,
        IntegerType: integers,
        LongType: integers,
        MapType: (dict,),
        ShortType: integers,
        StringType: (str,),
        StructType: (Row, dict, list, tuple),
        TimestampNTZType: (datetime.datetime,),
        TimestampType: (datetime.datetime,),
    }
