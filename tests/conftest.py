"""Shared integration fixtures for the public rowsmyth API."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from rowsmyth import declarative_base, variant

Base = declarative_base()


class Role(Base):
    __table_name__ = "roles"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "id": ctx.sequence(),
            "name": ctx.random.choice(["admin", "user", "guest"]),
        }


class User(Base):
    __table_name__ = "users"
    __catalog__ = "main"
    __schema__ = "app"
    __comment__ = "Application users"
    __primary_key__ = ("id",)
    __table_tags__: ClassVar[dict[str, str]] = {"layer": "silver"}
    __expectations__: ClassVar[dict[str, str]] = {
        "id_not_null": "id IS NOT NULL",
        "email_not_null": "email IS NOT NULL",
    }
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("role_id", LongType(), False),
        StructField(
            "full_name",
            StringType(),
            False,
            metadata={"comment": "User display name"},
        ),
        StructField(
            "email",
            StringType(),
            False,
            metadata={
                "comment": "User email, PII",
                "tags": {"pii": "true", "classification": "restricted"},
            },
        ),
        StructField("status", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        first = ctx.faker.first_name()
        last = ctx.faker.last_name()
        return {
            "id": ctx.sequence(),
            "role_id": Role.factory(),
            "full_name": f"{first} {last}",
            "email": ctx.faker.unique.ascii_email(),
            "status": ctx.random.choices(["active", "inactive"], weights=[9, 1])[0],
        }

    @variant
    def churned(self, ctx) -> dict:
        return {"status": "inactive"}


class Post(Base):
    __table_name__ = "posts"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("author_id", LongType(), False),
        StructField("title", StringType(), False),
        StructField("published", BooleanType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "id": ctx.sequence(),
            "author_id": User.factory(),
            "title": ctx.faker.sentence(nb_words=4),
            "published": False,
        }


class Order(Base):
    __table_name__ = "orders"
    __primary_key__ = ("order_id", "region")
    __definition__ = StructType([
        StructField("order_id", LongType(), False),
        StructField("region", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "order_id": ctx.sequence(),
            "region": ctx.random.choice(["eu", "us"]),
        }


class OrderLine(Base):
    __table_name__ = "order_lines"
    __primary_key__ = ("line_id",)
    __definition__ = StructType([
        StructField("line_id", LongType(), False),
        StructField("order_id", LongType(), False),
        StructField("order_region", StringType(), False),
        StructField("qty", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        order = ctx.parent(Order, role="placed_order")
        return {
            "line_id": ctx.sequence(),
            "order_id": order.key["order_id"],
            "order_region": order.key["region"],
            "qty": ctx.random.randint(1, 5),
        }


class BadCompoundFk(Base):
    __table_name__ = "bad_compound_fk"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("parent_ref", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "id": ctx.sequence(),
            "parent_ref": Order.factory(),
        }


class PoolConsumer(Base):
    __table_name__ = "pool_consumer"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("role_id", LongType(), False),
        StructField("fallback_role_id", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "id": ctx.sequence(),
            "role_id": ctx.pool("roles", "id").choice(),
            "fallback_role_id": 0,
        }


class NullableDemo(Base):
    __table_name__ = "nullable_demo"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("optional", StringType(), True),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence()}


class MissingRequired(Base):
    __table_name__ = "missing_required"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("required_col", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence()}


class AbstractState(Base):
    __definition__ = StructType([StructField("id", LongType(), False)])
    __primary_key__ = ("id",)

    @variant
    def archived(self, ctx) -> dict:
        return {"status": "archived"}


class StatefulItem(AbstractState):
    __table_name__ = "stateful_items"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("status", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence(), "status": "active"}


class SequenceLeft(Base):
    __table_name__ = "sequence_left"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("shared_id", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence(), "shared_id": ctx.sequence("shared")}


class SequenceRight(Base):
    __table_name__ = "sequence_right"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("shared_id", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence(), "shared_id": ctx.sequence("shared")}


@pytest.fixture
def app_base():
    return Base


@pytest.fixture
def models():
    return {
        "bad_compound_fk": BadCompoundFk,
        "missing_required": MissingRequired,
        "nullable_demo": NullableDemo,
        "order": Order,
        "order_line": OrderLine,
        "pool_consumer": PoolConsumer,
        "post": Post,
        "role": Role,
        "sequence_left": SequenceLeft,
        "sequence_right": SequenceRight,
        "stateful_item": StatefulItem,
        "user": User,
    }


def _ensure_java_on_path() -> None:
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        return
    java_bin = str(Path(java_home) / "bin")
    path = os.environ.get("PATH", "")
    if java_bin.casefold() not in path.casefold():
        os.environ["PATH"] = f"{java_bin}{os.pathsep}{path}"


def _configure_pyspark_env() -> None:
    python = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    _ensure_java_on_path()
    _configure_pyspark_env()
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("rowsmyth-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.timeout", "600")
        .config("spark.network.timeout", "600s")
        .getOrCreate()
    )
    yield session
    session.stop()
