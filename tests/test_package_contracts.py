"""Package import and public namespace contracts."""

from __future__ import annotations

import builtins
import sys

import rowsmyth


def test_public_namespace_contains_supported_user_symbols() -> None:
    expected = {
        "Dataset",
        "Factory",
        "Model",
        "Pool",
        "RowCtx",
        "RowsmythError",
        "__version__",
        "declarative_base",
        "variant",
    }

    assert expected.issubset(set(rowsmyth.__all__))
    assert all(hasattr(rowsmyth, name) for name in rowsmyth.__all__)


def test_version_fallback_when_generated_module_is_unavailable(monkeypatch) -> None:
    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "rowsmyth._version":
            msg = "no version"
            raise ImportError(msg)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)
    sys.modules.pop("rowsmyth", None)
    sys.modules.pop("rowsmyth._version", None)
    try:
        import rowsmyth as reloaded

        assert reloaded.__version__ == "0.0.0"
    finally:
        sys.modules.pop("rowsmyth", None)
        sys.modules["rowsmyth"] = rowsmyth
