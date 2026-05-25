import sys
from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from rowsmyth import declarative_base


def test_integer_column_maps_to_integer_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_int"
        id: Mapped[int] = mapped_column(primary_key=True)

    from pyspark.sql.types import IntegerType, StructType

    schema = T.__spark_schema__
    assert isinstance(schema, StructType)
    assert schema["id"].dataType == IntegerType()


def test_small_integer_maps_to_short_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_smallint"
        id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)

    from pyspark.sql.types import ShortType

    assert T.__spark_schema__["id"].dataType == ShortType()


def test_big_integer_maps_to_long_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_bigint"
        id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    from pyspark.sql.types import LongType

    assert T.__spark_schema__["id"].dataType == LongType()


def test_float_column_maps_to_float_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_float"
        id: Mapped[int] = mapped_column(primary_key=True)
        val: Mapped[float] = mapped_column(Float)

    from pyspark.sql.types import FloatType

    assert T.__spark_schema__["val"].dataType == FloatType()


def test_string_column_maps_to_string_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_str"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String)

    from pyspark.sql.types import StringType

    assert T.__spark_schema__["name"].dataType == StringType()


def test_boolean_column_maps_to_boolean_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_bool"
        id: Mapped[int] = mapped_column(primary_key=True)
        active: Mapped[bool] = mapped_column(Boolean)

    from pyspark.sql.types import BooleanType

    assert T.__spark_schema__["active"].dataType == BooleanType()


def test_date_column_maps_to_date_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_date"
        id: Mapped[int] = mapped_column(primary_key=True)
        created: Mapped[date] = mapped_column(Date)

    from pyspark.sql.types import DateType

    assert T.__spark_schema__["created"].dataType == DateType()


def test_datetime_column_maps_to_timestamp_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_dt"
        id: Mapped[int] = mapped_column(primary_key=True)
        created: Mapped[datetime] = mapped_column(DateTime)

    from pyspark.sql.types import TimestampType

    assert T.__spark_schema__["created"].dataType == TimestampType()


def test_large_binary_maps_to_binary_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_bin"
        id: Mapped[int] = mapped_column(primary_key=True)
        data: Mapped[bytes] = mapped_column(LargeBinary)

    from pyspark.sql.types import BinaryType

    assert T.__spark_schema__["data"].dataType == BinaryType()


def test_numeric_maps_to_decimal_type():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_num"
        id: Mapped[int] = mapped_column(primary_key=True)
        amount: Mapped[Decimal] = mapped_column(Numeric(precision=10, scale=2))

    from pyspark.sql.types import DecimalType

    assert T.__spark_schema__["amount"].dataType == DecimalType(10, 2)


def test_non_optional_column_is_not_nullable():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_nn"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String)

    assert T.__spark_schema__["name"].nullable is False


def test_optional_column_is_nullable():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_null"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str | None] = mapped_column(String)

    assert T.__spark_schema__["name"].nullable is True


def test_column_comment_in_metadata():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_comment"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String, comment="The user's name")

    assert T.__spark_schema__["name"].metadata["comment"] == "The user's name"


def test_column_info_in_metadata():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_info"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(
            String, info={"pii": True, "owner": "data-team"}
        )

    meta = T.__spark_schema__["name"].metadata
    assert meta["pii"] is True
    assert meta["owner"] == "data-team"


def test_comment_and_info_merged_in_metadata():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_merged"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String, comment="a name", info={"pii": True})

    meta = T.__spark_schema__["name"].metadata
    assert meta["comment"] == "a name"
    assert meta["pii"] is True


def test_no_comment_key_when_comment_is_none():
    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_nocomment"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String)

    assert "comment" not in T.__spark_schema__["name"].metadata


def test_unsupported_type_raises_type_error():
    from sqlalchemy import JSON

    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_bad"
        id: Mapped[int] = mapped_column(primary_key=True)
        data: Mapped[str] = mapped_column(JSON)

    with pytest.raises(
        TypeError, match="Unsupported SQLAlchemy type for column 'data'"
    ):
        _ = T.__spark_schema__


def test_double_column_maps_to_double_type():
    from sqlalchemy import Double

    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_double"
        id: Mapped[int] = mapped_column(primary_key=True)
        val: Mapped[float] = mapped_column(Double)

    from pyspark.sql.types import DoubleType

    assert T.__spark_schema__["val"].dataType == DoubleType()


def test_uuid_column_maps_to_string_type():
    import uuid

    from sqlalchemy import Uuid

    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_uuid"
        id: Mapped[uuid.UUID] = mapped_column(
            Uuid, primary_key=True, default=uuid.uuid4
        )

    from pyspark.sql.types import StringType

    assert T.__spark_schema__["id"].dataType == StringType()


def test_pyspark_not_installed_raises_import_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", None)

    Base = declarative_base()

    class T(Base):
        __tablename__ = "ts_nopyspark"
        id: Mapped[int] = mapped_column(primary_key=True)

    with pytest.raises(ImportError, match="pyspark is required"):
        _ = T.__spark_schema__
