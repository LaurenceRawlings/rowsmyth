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

    def __init__(self, base: type, overrides: dict[type, Any] | None = None) -> None:
        self._base = base
        self._overrides: dict[type, Any] = overrides or {}
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
        """Generate and persist one instance per registered model.

        Instances are created in FK dependency order (parents before children).
        FK columns are wired automatically by sampling randomly from the parent pool.

        Returns:
            Dict keyed by table name mapping to lists of persisted instances.

        Raises:
            ValueError: If a parent model has 0 instances but a child model
                has an FK pointing to it.
        """
        from ._builder import FactoryBuilder

        if self._seed is not None:
            _seed_random(self._seed)

        builders = []
        for mapper in self._base.registry.mappers:  # ty: ignore[unresolved-attribute]
            model = mapper.class_
            builders.append(self._overrides.get(model, FactoryBuilder(model, 1)))

        ordered = self._topo_sort(builders)
        models = {b.model for b in ordered}
        session = build_session(models)
        factories = {m: make_factory(m, session) for m in models}

        pool: dict[type, list[Any]] = {}
        result: dict[str, list[Any]] = {}

        for builder in ordered:
            instances: list[Any] = []
            for _ in range(builder._resolve_count()):
                fk_overrides: dict[str, Any] = {}
                for rel in builder.model.__mapper__.relationships:
                    if rel.direction is not MANYTOONE:
                        continue
                    related = rel.mapper.class_
                    if related not in pool:
                        continue
                    if not pool[related]:
                        model_name = builder.model.__name__
                        related_name = related.__name__
                        msg = (
                            f"Cannot wire {model_name}.{rel.key}: "
                            f"{related_name} has 0 instances in the dataset. "
                            f"Use {related_name}.factory(0) only for models "
                            f"with no FK dependents."
                        )
                        raise ValueError(msg)
                    fk_overrides[rel.key] = random.choice(pool[related])

                inst_overrides = _resolve_variant(builder)
                inst_overrides.update(fk_overrides)

                instance = factories[builder.model](**inst_overrides)
                instances.append(instance)

            pool[builder.model] = instances
            result[builder.model.__tablename__] = instances

        return result

    def _topo_sort(self, builders: list[Any]) -> list[Any]:
        model_to_builder = {b.model: b for b in builders}
        graph = _build_fk_dependency_graph(builders)
        ts = graphlib.TopologicalSorter(graph)
        ordered_models = list(ts.static_order())
        return [model_to_builder[m] for m in ordered_models if m in model_to_builder]
