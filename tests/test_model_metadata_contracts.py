"""Public model, registry and Unity Catalog metadata contracts."""

from __future__ import annotations

from typing import ClassVar

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from rowsmyth import (
    CompoundPrimaryKeyError,
    InvalidDeclarativeBaseError,
    InvalidModelDefinitionError,
    Model,
    ReservedColumnError,
    UnknownColumnError,
    declarative_base,
)


def test_declarative_base_scopes_registry(app_base, models) -> None:
    role_model = models["role"]
    user_model = models["user"]
    other_base = declarative_base()

    class OtherUser(other_base):
        __table_name__ = "users"
        __primary_key__ = ("id",)
        __definition__ = user_model.__definition__

        def generator(self, ctx) -> dict:
            return {}

    assert app_base.registry["roles"] is role_model
    assert app_base.registry["users"] is user_model
    assert other_base.registry == {"users": OtherUser}
    assert "roles" not in Model.registry


def test_abstract_classes_do_not_register_but_concrete_children_do(
    app_base,
    models,
) -> None:
    assert "abstract_state" not in app_base.registry
    assert app_base.registry["stateful_items"] is models["stateful_item"]


def test_direct_model_subclass_with_table_name_raises(models) -> None:
    with pytest.raises(InvalidDeclarativeBaseError, match="declarative_base"):

        class Direct(Model):
            __table_name__ = "direct_test_only"
            __primary_key__ = ("id",)
            __definition__ = models["user"].__definition__


def test_model_constructor_exposes_attrs_key_pk_and_attribute_access(models) -> None:
    role_model = models["role"]
    role = role_model(id=7, name="admin")

    assert role.attrs == {"id": 7, "name": "admin"}
    assert role.key == {"id": 7}
    assert role.pk == 7
    assert role.id == 7
    assert role.name == "admin"
    with pytest.raises(AttributeError, match="missing"):
        _ = role.missing
    with pytest.raises(UnknownColumnError, match="unknown columns"):
        role_model(id=1, name="admin", unknown=True)


def test_compound_primary_key_has_key_but_no_scalar_pk(models) -> None:
    order_model = models["order"]
    order = order_model(order_id=1, region="eu")

    assert order.key == {"order_id": 1, "region": "eu"}
    with pytest.raises(CompoundPrimaryKeyError, match="single-column primary key"):
        _ = order.pk


def test_fqn_uses_available_catalog_parts(models) -> None:
    base = declarative_base()

    class SchemaOnly(base):
        __table_name__ = "schema_only"
        __schema__ = "app"
        __primary_key__ = ("id",)
        __definition__ = models["role"].__definition__

        def generator(self, ctx) -> dict:
            return {}

    class CatalogOnly(base):
        __table_name__ = "catalog_only"
        __catalog__ = "main"
        __primary_key__ = ("id",)
        __definition__ = models["role"].__definition__

        def generator(self, ctx) -> dict:
            return {}

    assert models["user"].fqn() == "main.app.users"
    assert models["role"].fqn() == "roles"
    assert SchemaOnly.fqn() == "app.schema_only"
    assert CatalogOnly.fqn() == "main.catalog_only"


def test_unity_catalog_metadata_helpers_escape_values(models) -> None:
    user_model = models["user"]
    assert user_model.__expectations__ == {
        "id_not_null": "id IS NOT NULL",
        "email_not_null": "email IS NOT NULL",
    }
    assert user_model.column_tags() == {
        "email": {"pii": "true", "classification": "restricted"}
    }
    assert user_model.column_comments() == {
        "full_name": "User display name",
        "email": "User email, PII",
    }
    assert user_model.uc_tag_sql() == [
        "COMMENT ON TABLE `main`.`app`.`users` IS 'Application users'",
        "ALTER TABLE `main`.`app`.`users` SET TAGS ('layer' = 'silver')",
        (
            "ALTER TABLE `main`.`app`.`users` ALTER COLUMN `full_name` "
            "COMMENT 'User display name'"
        ),
        (
            "ALTER TABLE `main`.`app`.`users` ALTER COLUMN `email` "
            "COMMENT 'User email, PII'"
        ),
        (
            "ALTER TABLE `main`.`app`.`users` ALTER COLUMN `email` "
            "SET TAGS ('pii' = 'true', 'classification' = 'restricted')"
        ),
    ]


def test_unity_catalog_sql_escapes_quotes(app_base, models) -> None:
    class Tagged(app_base):
        __table_name__ = "tagged_test_only"
        __comment__ = "Data's table"
        __primary_key__ = ("id",)
        __table_tags__: ClassVar[dict[str, str]] = {"owner": "data's team"}
        __definition__ = models["role"].__definition__

        def generator(self, ctx) -> dict:
            return {}

    try:
        assert Tagged.uc_tag_sql()[:2] == [
            "COMMENT ON TABLE `tagged_test_only` IS 'Data''s table'",
            "ALTER TABLE `tagged_test_only` SET TAGS ('owner' = 'data''s team')",
        ]
    finally:
        app_base.registry.pop("tagged_test_only", None)


def test_model_definition_validation_errors(models) -> None:
    base = declarative_base()
    with pytest.raises(ReservedColumnError, match="reserved rowsmyth columns"):

        class Reserved(base):
            __table_name__ = "reserved_test_only"
            __primary_key__ = ("id",)
            __definition__ = StructType([
                StructField("id", LongType(), False),
                StructField("__rowsmyth_pool", LongType(), True),
            ])

    with pytest.raises(InvalidModelDefinitionError, match="missing primary key"):

        class BadPrimaryKey(base):
            __table_name__ = "bad_primary_key_test_only"
            __primary_key__ = ("missing_id",)
            __definition__ = models["role"].__definition__

    with pytest.raises(InvalidModelDefinitionError, match="primary key"):

        class EmptyPrimaryKey(base):
            __table_name__ = "empty_primary_key_test_only"
            __primary_key__ = ()
            __definition__ = models["role"].__definition__


def test_generator_must_be_implemented(app_base, models) -> None:
    class Bare(app_base):
        __table_name__ = "bare_test_only"
        __primary_key__ = ("id",)
        __definition__ = models["role"].__definition__

    try:
        with pytest.raises(NotImplementedError, match="generator"):
            Bare().generator(None)  # type: ignore[arg-type]
    finally:
        app_base.registry.pop("bare_test_only", None)
