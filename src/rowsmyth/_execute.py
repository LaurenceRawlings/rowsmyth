from __future__ import annotations

import functools
import graphlib
import random
from typing import TYPE_CHECKING, Any

from factory.alchemy import SQLAlchemyModelFactory
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import MANYTOONE, Session

if TYPE_CHECKING:
    from ._builder import FactoryBuilder


def _seed_random(seed: int) -> None:
    from faker import Faker

    random.seed(seed)
    Faker.seed(seed)


def check_cycles(builder: FactoryBuilder) -> None:
    graph: dict[type, set[type]] = {}

    def _collect(b: FactoryBuilder) -> None:
        if b.model in graph:
            return
        graph[b.model] = set()
        for child_builder, _ in b._children:
            graph[b.model].add(child_builder.model)
            _collect(child_builder)

    _collect(builder)
    ts = graphlib.TopologicalSorter(graph)
    ts.prepare()


def _build_fk_dependency_graph(builders: list[FactoryBuilder]) -> dict[type, set[type]]:
    model_set = {b.model for b in builders}
    return {
        b.model: {
            rel.mapper.class_
            for rel in b.model.__mapper__.relationships  # ty: ignore[unresolved-attribute]
            if rel.direction is MANYTOONE
            and rel.mapper.class_ in model_set
            and rel.mapper.class_ is not b.model
        }
        for b in builders
    }


@functools.cache
def _get_default_builders(model: type) -> dict[str, FactoryBuilder]:
    from ._builder import FactoryBuilder

    return {
        getattr(key, "key", key): val
        for key, val in model.generators().items()  # ty: ignore[unresolved-attribute]
        if isinstance(val, FactoryBuilder)
    }


def collect_models(builder: FactoryBuilder) -> set[type]:
    models: set[type] = set()

    def _collect(b: FactoryBuilder) -> None:
        models.add(b.model)
        for child_builder, _ in b._children:
            _collect(child_builder)
        for default_builder in _get_default_builders(b.model).values():
            _collect(default_builder)

    _collect(builder)
    return models


def collect_all_tables(models: set[type]) -> set[Table]:
    seen: set[Table] = {m.__table__ for m in models}  # ty: ignore[unresolved-attribute]
    queue: list[Table] = list(seen)
    while queue:
        table = queue.pop()
        for fk in table.foreign_key_constraints:
            for col in fk.elements:
                ref_table = col.column.table
                if ref_table not in seen:
                    seen.add(ref_table)
                    queue.append(ref_table)
    return seen


def _resolve_variant(builder: FactoryBuilder) -> dict[str, Any]:
    variant_name = builder._pick_variant()
    if not variant_name:
        return {}
    raw = builder.model.__variants__[variant_name](builder.model)  # ty: ignore[unresolved-attribute]
    return _resolve_attr_names(raw)


def _resolve_attr_names(d: dict[Any, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, val in d.items():
        attr_name = getattr(key, "key", key)
        result[attr_name] = val
    return result


def build_session(models: set[type]) -> Session:
    tables = collect_all_tables(models)
    metadata = next(iter(models)).__table__.metadata  # ty: ignore[unresolved-attribute]
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine, tables=list(tables))
    return Session(engine)


def make_factory(model: type, session: Session) -> type[SQLAlchemyModelFactory]:
    from ._builder import FactoryBuilder

    raw_generators = {
        k: v
        for k, v in model.generators().items()  # ty: ignore[unresolved-attribute]
        if not isinstance(v, FactoryBuilder)
    }
    generators = _resolve_attr_names(raw_generators)
    meta = type(
        "Meta",
        (),
        {
            "model": model,
            "sqlalchemy_session": session,
            "sqlalchemy_session_persistence": "commit",
        },
    )
    attrs: dict[str, Any] = {"Meta": meta, **generators}
    return type(f"{model.__name__}Factory", (SQLAlchemyModelFactory,), attrs)


@functools.cache
def find_relationship(
    child_model: type, parent_model: type, via: str | None = None
) -> str:
    if via is not None:
        return via
    matches = [
        rel.key
        for rel in child_model.__mapper__.relationships  # ty: ignore[unresolved-attribute]
        if rel.mapper.class_ is parent_model
    ]
    if not matches:
        msg = (
            f"No relationship from {child_model.__name__} to {parent_model.__name__}. "
            f"Define one via SQLAlchemy relationship() or pass via= to .has()"
        )
        raise ValueError(msg)
    if len(matches) > 1:
        msg = (
            f"Ambiguous - found {matches} on {child_model.__name__} pointing to "
            f"{parent_model.__name__}. Pass via='rel_name' to .has()"
        )
        raise ValueError(msg)
    return matches[0]


def _create_instance(
    builder: FactoryBuilder,
    extra_overrides: dict[str, Any],
    factories: dict[type, type[SQLAlchemyModelFactory]],
) -> Any:
    inst_overrides = _resolve_variant(builder)
    inst_overrides.update(extra_overrides)

    default_builders = _get_default_builders(builder.model)
    for rel in builder.model.__mapper__.relationships:  # ty: ignore[unresolved-attribute]
        if rel.direction is not MANYTOONE:
            continue
        if rel.key not in inst_overrides and rel.key in default_builders:
            inst_overrides[rel.key] = _create_instance(
                default_builders[rel.key], {}, factories
            )

    instance = factories[builder.model](**inst_overrides)

    for child_builder, via in builder._children:
        rel_key = find_relationship(child_builder.model, builder.model, via)
        for _ in range(child_builder._resolve_count()):
            _create_instance(child_builder, {rel_key: instance}, factories)

    return instance


def execute_builder(
    builder: FactoryBuilder,
    overrides: dict[Any, Any],
    seed: int | None = None,
) -> list[Any]:
    if seed is not None:
        _seed_random(seed)

    check_cycles(builder)
    models = collect_models(builder)
    session = build_session(models)
    factories = {m: make_factory(m, session) for m in models}
    root_overrides = _resolve_attr_names(overrides)

    return [
        _create_instance(builder, root_overrides, factories)
        for _ in range(builder._resolve_count())
    ]
