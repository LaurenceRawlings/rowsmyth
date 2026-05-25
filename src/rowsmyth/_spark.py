from __future__ import annotations

from typing import Any

from sqlalchemy import types as sa


def _map_type(attr_name: str, col_type: Any, spark_types: Any) -> Any:  # noqa: C901
    if isinstance(col_type, sa.SmallInteger):
        return spark_types.ShortType()
    if isinstance(col_type, sa.BigInteger):
        return spark_types.LongType()
    if isinstance(col_type, sa.Integer):
        return spark_types.IntegerType()
    if isinstance(col_type, sa.Double):
        return spark_types.DoubleType()
    if isinstance(col_type, sa.Float):
        return spark_types.FloatType()
    if isinstance(col_type, sa.Numeric):
        return spark_types.DecimalType(col_type.precision or 10, col_type.scale or 0)
    if isinstance(col_type, (sa.String, sa.Text, sa.Unicode, sa.UnicodeText)):
        return spark_types.StringType()
    if isinstance(col_type, sa.Boolean):
        return spark_types.BooleanType()
    if isinstance(col_type, sa.DateTime):
        return spark_types.TimestampType()
    if isinstance(col_type, sa.Date):
        return spark_types.DateType()
    if isinstance(col_type, sa.LargeBinary):
        return spark_types.BinaryType()
    if isinstance(col_type, sa.Uuid):
        return spark_types.StringType()
    col_type_name = type(col_type).__name__
    msg = f"Unsupported SQLAlchemy type for column '{attr_name}': {col_type_name}"
    raise TypeError(msg)


def to_spark_schema(cls: type) -> Any:
    try:
        from pyspark.sql import types as spark_types
    except ImportError:
        msg = (
            "pyspark is required to use __spark_schema__. "
            "Install it with: uv add pyspark"
        )
        raise ImportError(msg) from None

    fields = []
    for prop in cls.__mapper__.column_attrs:  # ty: ignore[unresolved-attribute]
        col = prop.columns[0]
        spark_type = _map_type(prop.key, col.type, spark_types)
        metadata = dict(col.info) if col.info else {}
        if col.comment is not None:
            metadata["comment"] = col.comment
        fields.append(
            spark_types.StructField(
                prop.key, spark_type, nullable=col.nullable, metadata=metadata
            )
        )
    return spark_types.StructType(fields)
