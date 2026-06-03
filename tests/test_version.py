"""Tests for package version import."""

from __future__ import annotations

import builtins
import sys


def test_version_fallback(monkeypatch) -> None:
    """Cover ImportError branch when _version is unavailable."""
    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "rowsmyth._version":
            msg = "no version"
            raise ImportError(msg)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)
    sys.modules.pop("rowsmyth", None)
    sys.modules.pop("rowsmyth._version", None)
    import rowsmyth

    assert rowsmyth.__version__ == "0.0.0"
