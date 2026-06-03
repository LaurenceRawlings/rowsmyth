"""Unit tests with mocked Spark (no JVM required)."""

from __future__ import annotations

import random
from typing import Any
from unittest.mock import MagicMock

import pytest

from rowsmyth import variant
from rowsmyth.dataset import Dataset, RowCtx, require_active
from rowsmyth.pool import PoolChoice
from rowsmyth.resolution import (
    apply_variant,
    new_parent,
    resolve_fk,
    resolve_row_values,
    validate_once,
)


class _Row:
    def __init__(self, value: Any) -> None:
        self._value = value

    def __getitem__(self, index: int) -> Any:
        return self._value


def _mock_spark(
    *,
    pool_values: list[Any] | None = None,
) -> MagicMock:
    spark = MagicMock()
    df = MagicMock()
    spark.createDataFrame.return_value = df
    if pool_values is not None:
        distinct = MagicMock()
        distinct.collect.return_value = [_Row(v) for v in pool_values]
        select = MagicMock()
        select.distinct.return_value = distinct
        table_df = MagicMock()
        table_df.select.return_value = select
        spark.table.return_value = table_df
    return spark


def test_require_active_raises() -> None:
    with pytest.raises(RuntimeError, match="inside dataset"):
        require_active()


def test_dataset_exposes_spark(app_base) -> None:
    spark = _mock_spark()
    dataset = Dataset(spark, MagicMock(), random.Random(0), 1, app_base)
    assert dataset.spark is spark
    assert dataset.base is app_base


def test_pool_empty_mocked(app_base) -> None:
    spark = _mock_spark(pool_values=[])
    gen = Dataset(spark, MagicMock(), random.Random(0), None, app_base)
    with pytest.raises(ValueError, match="no values"):
        gen.pool("roles", "id").sample(1)


def test_pool_with_values(app_base) -> None:
    spark = _mock_spark(pool_values=[1, 2, 3])
    gen = Dataset(spark, MagicMock(), random.Random(0), None, app_base)
    pool = gen.pool("roles", "id")
    assert pool.sample(1)[0] in (1, 2, 3)
    choice = pool.choice()
    assert isinstance(choice, PoolChoice)
    assert choice.view == "roles"
    assert choice.column == "id"


def test_create_mocked(app_base, role_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark, seed=0) as gen:
        result = role_model.factory().count(2).create()
    assert len(result) == 2
    assert "roles" in gen.dataframes
    spark.createDataFrame.assert_called_once()
    gen.dataframes["roles"].createOrReplaceTempView.assert_called_with("roles")


def test_resolve_fk_injected(app_base, role_model, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, acc)
        inst = role_model(id=99, name="admin")
        ctx._parents["role_id"] = inst
        value = resolve_fk(role_model.factory(), ctx, slot="role_id")
        assert value == 99


def test_resolve_fk_creates_parent(app_base, role_model, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, acc)
        value = resolve_fk(role_model.factory(), ctx, slot="role_id")
        assert isinstance(value, int)
        assert "roles" in acc


def test_resolve_row_callable(app_base, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, acc)
        attrs = {"status": "active", "email": lambda c: f"{c.row['status']}@x.com"}
        ctx.row = attrs
        resolve_row_values(attrs, ctx)
        assert attrs["email"] == "active@x.com"


def test_new_parent(app_base, role_model, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, acc)
        inst = new_parent(role_model.factory(), ctx)
        assert inst.table is role_model
        assert "roles" in acc


def test_validate_once_skips_second(app_base, nullable_demo_model) -> None:
    spark = _mock_spark()
    gen = Dataset(spark, MagicMock(), random.Random(0), None, app_base)
    validate_once(gen, nullable_demo_model, {"id": 1})
    validate_once(gen, nullable_demo_model, {})


def test_validate_once_raises(app_base, missing_required_model) -> None:
    spark = _mock_spark()
    gen = Dataset(spark, MagicMock(), random.Random(0), None, app_base)
    with pytest.raises(ValueError, match="required_col"):
        validate_once(gen, missing_required_model, {"id": 1})


def test_apply_variant(app_base, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, {})
        result = apply_variant(user_model, user_model(), "churned", ctx)
        assert result == {"status": "inactive"}


def test_rowctx_parent(app_base, role_model, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, acc)
        parent = ctx.parent(role_model, role="custom")
        assert parent.table is role_model


def test_variant_non_function() -> None:
    class CallableObj:
        def __call__(self) -> dict[str, Any]:
            return {}

    assert variant(CallableObj()) is not None


def test_factory_fluent_methods(role_model, user_model) -> None:
    f = role_model.factory()
    assert f.count(2) is f
    assert f.where(name="x") is f
    assert f.has(role_model.factory()) is f
    g = user_model.factory().variant("churned")
    assert g._variant == "churned"


def test_rowctx_properties(app_base, user_model) -> None:
    spark = _mock_spark(pool_values=[10, 20])
    with app_base.dataset(spark, seed=5) as gen:
        ctx = RowCtx(gen, user_model, 0, {}, {})
        assert ctx.faker is gen.faker
        assert ctx.random is gen.random
        assert ctx.seed == 5
        assert ctx.spark is spark
        assert ctx.sequence() >= 1
        assert isinstance(ctx.pool("roles", "id").choice(), PoolChoice)


def test_model_key_and_pk(role_model) -> None:
    role = role_model(id=7, name="admin")
    assert role.key == {"id": 7}
    assert role.pk == 7
    assert role.id == 7
    assert role.name == "admin"
    with pytest.raises(AttributeError, match="missing"):
        _ = role.missing


def test_resolve_factory_in_attrs(app_base, role_model, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, user_model, 0, {}, acc)
        attrs = {"role_id": role_model.factory()}
        ctx.row = attrs
        resolve_row_values(attrs, ctx)
        assert isinstance(attrs["role_id"], int)


def test_has_children_mocked(app_base, post_model, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark, seed=1):
        user_model.factory().count(1).has(
            post_model.factory().count(2),
            via="author_id",
        ).create()


def test_variant_create_mocked(app_base, user_model) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark, seed=2):
        user_model.factory().count(1).variant("churned").create()


def test_compound_fk_resolve_raises(
    app_base,
    bad_compound_fk_model,
    order_model,
) -> None:
    spark = _mock_spark()
    with app_base.dataset(spark):
        acc: dict[str, list[dict[str, Any]]] = {}
        gen = require_active()
        ctx = RowCtx(gen, bad_compound_fk_model, 0, {}, acc)
        with pytest.raises(TypeError, match="compound keys"):
            resolve_fk(order_model.factory(), ctx, slot="parent_ref")
