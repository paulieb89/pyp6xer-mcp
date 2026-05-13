"""Tests for _get_cache: key lookup and missing-key error."""
import pytest
from unittest.mock import MagicMock


def _make_ctx(cache: dict) -> MagicMock:
    ctx = MagicMock()
    ctx.lifespan_context = {"cache": cache}
    return ctx


def test_get_cache_raises_on_missing_key():
    from server import _get_cache

    ctx = _make_ctx({})
    with pytest.raises(ValueError, match="No file loaded"):
        _get_cache(ctx, "nonexistent")


def test_get_cache_returns_entry_for_known_key():
    from server import _get_cache

    entry = {"xer": object(), "source": "test.xer"}
    ctx = _make_ctx({"mykey": entry})
    assert _get_cache(ctx, "mykey") is entry
