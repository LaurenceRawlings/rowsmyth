"""Shared pytest fixtures."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import ClassVar

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from rowsmyth import Model, variant


class Role(Model):
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


class User(Model):
    __table_name__ = "users"
    __catalog__ = "main"
    __schema__ = "app"
    __comment__ = "Application users"
    __primary_key__ = ("id",)
    __table_tags__: ClassVar[dict[str, str]] = {"layer": "silver"}
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("role_id", LongType(), False),
        StructField(
            "full_name",
            StringType(),
            False,
            metadata={
                "comment": "User display name",
            },
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


class Post(Model):
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


class OrderLine(Model):
    """Child with compound FK to Order."""

    __table_name__ = "order_lines"
    __primary_key__ = ("line_id",)
    __definition__ = StructType([
        StructField("line_id", LongType(), False),
        StructField("order_id", LongType(), False),
        StructField("order_region", StringType(), False),
        StructField("qty", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        order = ctx.parent(Order)
        return {
            "line_id": ctx.sequence(),
            "order_id": order.key["order_id"],
            "order_region": order.key["region"],
            "qty": ctx.random.randint(1, 5),
        }


class Order(Model):
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


class BadCompoundFk(Model):
    """Model that incorrectly uses Factory() with compound PK parent."""

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


class AbstractBase(Model):
    """Intermediate base without __table_name__ - not registered."""

    __definition__ = StructType([StructField("id", LongType(), False)])
    __primary_key__ = ("id",)

    def generator(self, ctx) -> dict:
        return {"id": 1}


class ConcreteChild(AbstractBase):
    __table_name__ = "concrete_child"
    __definition__ = StructType([StructField("id", LongType(), False)])
    __primary_key__ = ("id",)

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence()}


class NullableDemo(Model):
    __table_name__ = "nullable_demo"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("optional", StringType(), True),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence()}


class MissingRequired(Model):
    __table_name__ = "missing_required"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("required_col", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {"id": ctx.sequence()}


class PoolConsumer(Model):
    __table_name__ = "pool_consumer"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("role_id", LongType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "id": ctx.sequence(),
            "role_id": ctx.pool("roles", "id").choice(),
        }


@pytest.fixture
def role_model() -> type[Role]:
    return Role


@pytest.fixture
def user_model() -> type[User]:
    return User


@pytest.fixture
def post_model() -> type[Post]:
    return Post


@pytest.fixture
def order_model() -> type[Order]:
    return Order


@pytest.fixture
def order_line_model() -> type[OrderLine]:
    return OrderLine


@pytest.fixture
def bad_compound_fk_model() -> type[BadCompoundFk]:
    return BadCompoundFk


@pytest.fixture
def concrete_child_model() -> type[ConcreteChild]:
    return ConcreteChild


@pytest.fixture
def nullable_demo_model() -> type[NullableDemo]:
    return NullableDemo


@pytest.fixture
def missing_required_model() -> type[MissingRequired]:
    return MissingRequired


@pytest.fixture
def pool_consumer_model() -> type[PoolConsumer]:
    return PoolConsumer


def _ensure_java_on_path() -> None:
    """Prepend JAVA_HOME/bin when JAVA_HOME is set (common on Windows)."""
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        return
    java_bin = str(Path(java_home) / "bin")
    path = os.environ.get("PATH", "")
    if java_bin.casefold() not in path.casefold():
        os.environ["PATH"] = f"{java_bin}{os.pathsep}{path}"


def _configure_pyspark_env() -> None:
    """Stable local Spark settings (especially on Windows)."""
    python = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Single local Spark session for the test run."""
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
