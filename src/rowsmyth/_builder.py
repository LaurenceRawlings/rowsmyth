from __future__ import annotations

import random
from typing import Any


class FactoryBuilder:
    """Fluent builder for generating and persisting model instances.

    Obtain via ``Model.factory(count)``. Chain configuration methods,
    then call ``.create()`` to generate and persist to an in-memory database.

    Example:
        >>> # users = User.factory(10).has(Order.factory(1, 3)).create()
    """  # doctest: +SKIP

    def __init__(
        self, model: type, count: int = 1, max_count: int | None = None
    ) -> None:
        self.model = model
        self.min_count = count
        self.max_count = max_count
        self._children: list[tuple[FactoryBuilder, str | None]] = []
        self._mix: dict[str, float] = {}
        self._overrides: dict[str, Any] = {}
        self._seed: int | None = None

    def has(self, *builders: FactoryBuilder, via: str | None = None) -> FactoryBuilder:
        """Attach child builders whose rows are created for each parent instance.

        Args:
            *builders: ``FactoryBuilder`` instances for child models.
            via: Relationship attribute name on the child model to use when
                wiring the FK. Only required when the child has multiple
                relationships pointing to the same parent model.

        Returns:
            ``self`` for chaining.

        Example:
            >>> # User.factory(5).has(Order.factory(1, 3))
        """  # doctest: +SKIP
        for builder in builders:
            self._children.append((builder, via))
        return self

    def mix(self, **proportions: float) -> FactoryBuilder:
        """Set variant proportions for this builder.

        Args:
            **proportions: Variant name to proportion in ``(0, 1]``. Total
                must not exceed 1.0. The remainder (``1 - sum``) is the
                proportion of un-varied (default) instances.

        Returns:
            ``self`` for chaining.

        Raises:
            ValueError: If proportions sum to more than 1.0, or a named
                variant is not defined on the model.

        Example:
            >>> # User.factory(100).mix(admin=0.05, premium=0.20)
        """  # doctest: +SKIP
        for name, proportion in proportions.items():
            if proportion <= 0.0:
                msg = f"Proportion for '{name}' must be > 0, got {proportion}"
                raise ValueError(msg)
        total = sum(proportions.values())
        if total > 1.0:
            msg = f"Proportions sum to {total:.2f}, must be ≤ 1.0"
            raise ValueError(msg)
        for name in proportions:
            if name not in self.model.__variants__:  # ty: ignore[unresolved-attribute]
                msg = (
                    f"Variant '{name}' not defined on {self.model.__name__}. "
                    f"Defined: {list(self.model.__variants__)}"  # ty: ignore[unresolved-attribute]
                )
                raise ValueError(msg)
        self._mix = proportions
        return self

    def _resolve_count(self) -> int:
        if self.max_count is None:
            return self.min_count
        return random.randint(self.min_count, self.max_count)

    def _pick_variant(self) -> str | None:
        if not self._mix:
            return None
        r = random.random()
        cumulative = 0.0
        for name, proportion in self._mix.items():
            cumulative += proportion
            if r < cumulative:
                return name
        # Remainder proportion (1 - sum) maps to no variant. When proportions
        # sum to exactly 1.0, floating-point imprecision may leave a tiny gap
        # here for a negligible fraction of calls - that's acceptable.
        return None

    def random_seed(self, value: int) -> FactoryBuilder:
        """Fix the random seed for reproducible output.

        Args:
            value: Integer seed passed to both ``random`` and ``Faker``.

        Returns:
            ``self`` for chaining.

        Example:
            >>> # User.factory(10).random_seed(42).create()
        """  # doctest: +SKIP
        self._seed = value
        return self

    def where(self, overrides: dict[Any, Any]) -> FactoryBuilder:
        """Apply fixed column overrides to every generated instance.

        Args:
            overrides: Dict mapping column attributes (or string names) to
                fixed values. These take precedence over generators and variants.

        Returns:
            ``self`` for chaining.

        Example:
            >>> # User.factory(5).where({User.tier: "premium"})
        """  # doctest: +SKIP
        self._overrides.update(overrides)
        return self

    def create(self) -> list[Any]:
        """Generate and persist all instances to an in-memory SQLite database.

        Returns:
            List of root model instances. Child instances (added via ``.has()``)
            are persisted and accessible via SQLAlchemy relationships but are
            not included in the returned list.

        Example:
            >>> # users = User.factory(10).has(Order.factory(2)).create()
            >>> # len(users)  # -> 10
        """  # doctest: +SKIP
        from ._execute import execute_builder

        return execute_builder(self, self._overrides, self._seed)
