"""Tests for Model registry and metadata."""

from __future__ import annotations

from typing import ClassVar

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from rowsmyth import Model, declarative_base


def test_registry_contains_tables(app_base) -> None:
    assert "roles" in app_base.registry
    assert "users" in app_base.registry
    assert "roles" not in Model.registry
    assert "users" not in Model.registry


def test_abstract_base_not_registered(app_base, concrete_child_model) -> None:
    assert "abstract_base" not in app_base.registry
    assert "concrete_child" in app_base.registry
    assert concrete_child_model.__table_name__ == "concrete_child"


def test_declarative_base_returns_scoped_registry(user_model) -> None:
    OtherBase = declarative_base()

    class OtherUser(OtherBase):
        __table_name__ = "users"
        __primary_key__ = ("id",)
        __definition__ = user_model.__definition__

        def generator(self, ctx) -> dict:
            return {}

    assert OtherBase.registry == {"users": OtherUser}
    assert user_model is not OtherUser
    assert user_model.__rowsmyth_base__.registry["users"] is user_model


def test_direct_model_subclass_with_table_name_raises(user_model) -> None:
    with pytest.raises(TypeError, match="declarative_base"):

        class Direct(Model):
            __table_name__ = "direct_test_only"
            __primary_key__ = ("id",)
            __definition__ = user_model.__definition__


def test_concrete_child_registers_to_abstract_parent_base(user_model) -> None:
    Base = declarative_base()

    class Abstract(Base):
        __definition__ = user_model.__definition__
        __primary_key__ = ("id",)

    class Concrete(Abstract):
        __table_name__ = "concrete_test_only"
        __definition__ = user_model.__definition__
        __primary_key__ = ("id",)

        def generator(self, ctx) -> dict:
            return {}

    assert "abstract" not in Base.registry
    assert Base.registry["concrete_test_only"] is Concrete


def test_fqn(user_model, role_model) -> None:
    assert user_model.fqn() == "main.app.users"
    assert role_model.fqn() == "roles"


def test_column_metadata_helpers(user_model) -> None:
    assert user_model.column_tags() == {
        "email": {"pii": "true", "classification": "restricted"}
    }
    assert user_model.column_comments() == {
        "full_name": "User display name",
        "email": "User email, PII",
    }


def test_uc_tag_sql(user_model) -> None:
    assert user_model.uc_tag_sql() == [
        "ALTER TABLE `main`.`app`.`users` SET TAGS ('layer' = 'silver')",
        (
            "ALTER TABLE `main`.`app`.`users` ALTER COLUMN `email` "
            "SET TAGS ('pii' = 'true', 'classification' = 'restricted')"
        ),
    ]


def test_uc_tag_sql_empty_without_tags(role_model) -> None:
    assert role_model.uc_tag_sql() == []


def test_uc_tag_sql_escapes_quotes(app_base, user_model) -> None:
    class Tagged(app_base):
        __table_name__ = "tagged_test_only"
        __primary_key__ = ("id",)
        __table_tags__: ClassVar[dict[str, str]] = {"owner": "data's team"}
        __definition__ = user_model.__definition__

        def generator(self, ctx) -> dict:
            return {}

    try:
        assert Tagged.uc_tag_sql()[0] == (
            "ALTER TABLE `tagged_test_only` SET TAGS ('owner' = 'data''s team')"
        )
    finally:
        app_base.registry.pop("tagged_test_only", None)


def test_factory_returns_builder(user_model) -> None:
    factory = user_model.factory()
    assert factory.table is user_model


def test_reserved_internal_columns_raise() -> None:
    Base = declarative_base()
    with pytest.raises(ValueError, match="reserved rowsmyth columns"):

        class Reserved(Base):
            __table_name__ = "reserved_test_only"
            __primary_key__ = ("id",)
            __definition__ = StructType([
                StructField("id", LongType(), False),
                StructField("__rowsmyth_pool", LongType(), True),
            ])


def test_model_constructor_unknown_column_raises(role_model) -> None:
    with pytest.raises(ValueError, match="unknown columns"):
        role_model(id=1, name="admin", unknown=True)


def test_generator_not_implemented(app_base, user_model) -> None:
    class Bare(app_base):
        __table_name__ = "bare_test_only"
        __primary_key__ = ("id",)
        __definition__ = user_model.__definition__

    try:
        with pytest.raises(NotImplementedError, match="generator"):
            Bare().generator(None)  # type: ignore[arg-type]
    finally:
        app_base.registry.pop("bare_test_only", None)
