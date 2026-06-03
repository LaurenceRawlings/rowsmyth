"""Foreign-key resolution and row validation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rowsmyth.context import Generation, RowCtx
    from rowsmyth.factory import Factory
    from rowsmyth.table import Model


def new_parent(factory: Factory, ctx: RowCtx) -> Model:
    """Create one parent row and append it to the accumulator."""
    attrs = factory.row(ctx._gen, ctx._acc, index=ctx.index, injected={})
    ctx._acc.setdefault(factory.table.__table_name__, []).append(attrs)
    return factory.table(**attrs)


def resolve_fk(child_factory: Factory, ctx: RowCtx, slot: str) -> Any:
    """Resolve a Factory used as a column value to a primary key."""
    table = child_factory.table
    pk = table.__primary_key__
    if len(pk) != 1:
        msg = (
            f"{table.__table_name__}: Factory() as column value requires a "
            "single-column primary key; use ctx.parent() for compound keys"
        )
        raise TypeError(msg)
    inst = ctx._parents.get(slot)
    if inst is None:
        inst = new_parent(child_factory, ctx)
        ctx._parents[slot] = inst
    return inst.attrs[pk[0]]


def resolve_row_values(attrs: dict[str, Any], ctx: RowCtx) -> None:
    """Resolve Factory and callable values in place."""
    from rowsmyth.factory import Factory

    for col, value in list(attrs.items()):
        if isinstance(value, Factory):
            attrs[col] = resolve_fk(value, ctx, slot=col)
        elif callable(value):
            attrs[col] = value(ctx)


def validate_once(gen: Generation, table: type[Model], attrs: dict[str, Any]) -> None:
    """Fail fast on the first row if NOT NULL columns are missing."""
    name = table.__table_name__
    if name in gen._validated:
        return
    missing = [
        f.name
        for f in table.__definition__.fields
        if not f.nullable and f.name not in attrs
    ]
    if missing:
        msg = f"{name}: NOT NULL columns without a value: {missing}"
        raise ValueError(msg)
    gen._validated.add(name)


def apply_variant(
    table: type[Model],
    obj: Model,
    variant_name: str,
    ctx: RowCtx,
) -> dict[str, Any]:
    """Run a named variant method and return its partial override."""
    method = table._variants[variant_name]
    return dict(method(obj, ctx))
