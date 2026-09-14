"""Explicit deferred column values."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rowsmyth.errors import LazyValueError

if TYPE_CHECKING:
    from collections.abc import Callable

    from rowsmyth.dataset import RowCtx


class Lazy:
    """
    A column value computed per row from the row context.

    Wrap a callable with :func:`lazy` instead of passing it directly: rowsmyth
    never calls a bare callable, so a class, builtin or partial passed as a
    column value is treated as data rather than silently invoked.
    """

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[RowCtx], Any]) -> None:
        if not callable(fn):
            msg = f"lazy() requires a callable taking the row context, got {fn!r}"
            raise LazyValueError(msg)
        self.fn = fn


def lazy(fn: Callable[[RowCtx], Any]) -> Lazy:
    """
    Mark a callable as a deferred column value.

    The callable receives the :class:`~rowsmyth.dataset.RowCtx` for the row
    being generated and its return value becomes the column value.
    """
    return Lazy(fn)
