from __future__ import annotations

import graphlib
import random
from typing import Any

from sqlalchemy.orm import MANYTOONE

from ._execute import (
    _build_fk_dependency_graph,
    _resolve_variant,
    _seed_random,
    build_session,
    make_factory,
)


class Dataset:
    """Generates one instance per registered model, respecting FK order.

    Obtain via ``Base.dataset(*builders)``. Use when you need a minimal
    coherent dataset across all tables rather than a deep hierarchy rooted
    at one model.

    Example:
        >>> # result = Base.dataset(User.factory(5), Order.factory(10)).create()
        >>> # result["users"]   # -> 5 User instances
        >>> # result["orders"]  # -> 10 Order instances
    """  # doctest: +SKIP

    def __init__(
        self,
        base: type,
        overrides: dict[type, Any] | None = None,
        instances: list[Any] | None = None,
    ) -> None:
        self._base = base
        self._overrides: dict[type, Any] = overrides or {}
        self._instances: list[Any] = instances or []
        self._seed: int | None = None

    def random_seed(self, value: int) -> Dataset:
        """Fix the random seed for reproducible output.

        Args:
            value: Integer seed passed to both ``random`` and ``Faker``.

        Returns:
            ``self`` for chaining.
        """
        self._seed = value
        return self

    def create(self) -> dict[str, list[Any]]:
        """Generate and persist all instances to an in-memory SQLite database.

        Raw instances passed to ``dataset()`` are persisted first (Phase 1)
        and added to the FK pool. Factories run second (Phase 2) and may
        wire FKs to any pooled row, including seeded ones.

        Returns:
            Dict keyed by table name mapping to lists of persisted instances.
            Seeded rows appear first within each table's list.

        Raises:
            ValueError: If a parent model has 0 instances but a child model
                has an FK pointing to it.
        """
        if self._seed is not None:
            _seed_random(self._seed)

        seeded_models = {type(inst) for inst in self._instances}
        ordered = self._topo_sort(self._collect_builders(seeded_models))
        all_models = {b.model for b in ordered} | seeded_models
        session = build_session(all_models)
        factories = {m: make_factory(m, session) for m in {b.model for b in ordered}}

        pool: dict[type, list[Any]] = {}
        result: dict[str, list[Any]] = {}

        self._seed_phase(session, pool, result)

        for builder in ordered:
            instances: list[Any] = []
            for _ in range(builder._resolve_count()):
                inst_overrides = _resolve_variant(builder)
                inst_overrides.update(self._build_fk_overrides(builder, pool))
                instances.append(factories[builder.model](**inst_overrides))

            pool[builder.model] = instances
            tablename = builder.model.__tablename__
            if tablename in result:
                result[tablename].extend(instances)
            else:
                result[tablename] = instances

        return result

    def _collect_builders(self, seeded_models: set[type]) -> list[Any]:
        from ._builder import FactoryBuilder

        builders = []
        for mapper in self._base.registry.mappers:  # ty: ignore[unresolved-attribute]
            model = mapper.class_
            if model in self._overrides:
                builders.append(self._overrides[model])
            elif model not in seeded_models:
                builders.append(FactoryBuilder(model, 1))
        return builders

    def _seed_phase(
        self,
        session: Any,
        pool: dict[type, list[Any]],
        result: dict[str, list[Any]],
    ) -> None:
        for inst in self._instances:
            session.add(inst)
        if self._instances:
            session.commit()
        for inst in self._instances:
            model_cls = type(inst)
            pool.setdefault(model_cls, []).append(inst)
            result.setdefault(model_cls.__tablename__, []).append(inst)

    def _build_fk_overrides(
        self, builder: Any, pool: dict[type, list[Any]]
    ) -> dict[str, Any]:
        fk_overrides: dict[str, Any] = {}
        for rel in builder.model.__mapper__.relationships:  # ty: ignore[unresolved-attribute]
            if rel.direction is not MANYTOONE:
                continue
            related = rel.mapper.class_
            if related not in pool:
                continue
            if not pool[related]:
                msg = (
                    f"Cannot wire {builder.model.__name__}.{rel.key}: "
                    f"{related.__name__} has 0 instances in the dataset. "
                    f"Use {related.__name__}.factory(0) only for models "
                    f"with no FK dependents."
                )
                raise ValueError(msg)
            fk_overrides[rel.key] = random.choice(pool[related])
        return fk_overrides

    def _topo_sort(self, builders: list[Any]) -> list[Any]:
        model_to_builder = {b.model: b for b in builders}
        graph = _build_fk_dependency_graph(builders)
        ts = graphlib.TopologicalSorter(graph)
        ordered_models = list(ts.static_order())
        return [model_to_builder[m] for m in ordered_models if m in model_to_builder]
