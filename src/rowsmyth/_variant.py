from collections.abc import Callable


def variant[F: Callable[..., object]](fn: F) -> F:
    """Mark a classmethod as a named variant for use with ``.mix()``.

    A variant is a classmethod that returns a dict of column overrides
    applied to a weighted subset of generated instances.

    Args:
        fn: The classmethod to mark as a variant.

    Returns:
        The same callable, with ``_is_variant = True`` set.

    Example:
        >>> from rowsmyth import variant
        >>> @variant
        ... def admin(cls):
        ...     return {}
        >>> admin._is_variant
        True
    """
    fn._is_variant = True  # ty: ignore[unresolved-attribute]
    return fn
