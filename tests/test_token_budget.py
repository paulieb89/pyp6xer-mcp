"""Token-budget smoke tests for analysis tool responses.

Loads helal.xer (gitignored) and measures the tiktoken count of each major
tool's JSON output.  Any response over TOKEN_BUDGET fails — that's the signal
to add pagination or truncation to that tool.

Skip the whole module gracefully when helal.xer is absent (CI / clean clones).
"""
import json
import os
import pytest

XER_PATH = os.path.join(os.path.dirname(__file__), "..", "helal.xer")
TOKEN_BUDGET = 8_192

pytestmark = pytest.mark.skipif(
    not os.path.exists(XER_PATH),
    reason="helal.xer not present (gitignored sample file)",
)


@pytest.fixture(scope="module")
def mock_ctx():
    """Context-shaped object that serves the real helal.xer from the cache."""
    from unittest.mock import MagicMock
    from server import _load_xer_content

    _, xer, _text = _load_xer_content(XER_PATH, None)
    ctx = MagicMock()
    ctx.lifespan_context = {"cache": {"default": {"xer": xer}}}
    return ctx


@pytest.fixture(scope="module")
def enc():
    import tiktoken
    return tiktoken.get_encoding("cl100k_base")


def _tokens(enc, text: str) -> int:
    return len(enc.encode(text))


# ---------------------------------------------------------------------------
# Per-tool tests
# ---------------------------------------------------------------------------

def test_critical_path_token_budget(mock_ctx, enc):
    from server import pyp6xer_critical_path
    output = pyp6xer_critical_path(ctx=mock_ctx)
    count = _tokens(enc, output)
    data = json.loads(output)
    print(f"\ncritical_path: {data['critical_count']} activities → {count:,} tokens")
    assert count <= TOKEN_BUDGET, (
        f"critical_path response is {count:,} tokens (budget {TOKEN_BUDGET}). "
        "Consider capping activities or trimming fields."
    )


def test_float_analysis_token_budget(mock_ctx, enc):
    from server import pyp6xer_float_analysis
    output = pyp6xer_float_analysis(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nfloat_analysis: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_schedule_quality_token_budget(mock_ctx, enc):
    from server import pyp6xer_schedule_quality
    output = pyp6xer_schedule_quality(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nschedule_quality: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_schedule_health_check_token_budget(mock_ctx, enc):
    from server import pyp6xer_schedule_health_check
    output = pyp6xer_schedule_health_check(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nschedule_health_check: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_relationship_analysis_token_budget(mock_ctx, enc):
    from server import pyp6xer_relationship_analysis
    output = pyp6xer_relationship_analysis(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nrelationship_analysis: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_wbs_analysis_token_budget(mock_ctx, enc):
    from server import pyp6xer_wbs_analysis
    output = pyp6xer_wbs_analysis(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nwbs_analysis: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_work_package_summary_token_budget(mock_ctx, enc):
    from server import pyp6xer_work_package_summary
    output = pyp6xer_work_package_summary(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nwork_package_summary: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_progress_summary_token_budget(mock_ctx, enc):
    from server import pyp6xer_progress_summary
    output = pyp6xer_progress_summary(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nprogress_summary: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_earned_value_token_budget(mock_ctx, enc):
    from server import pyp6xer_earned_value
    output = pyp6xer_earned_value(ctx=mock_ctx)
    count = _tokens(enc, output)
    print(f"\nearned_value: {count:,} tokens")
    assert count <= TOKEN_BUDGET


def test_list_activities_token_budget(mock_ctx, enc):
    from server import pyp6xer_list_activities
    output = pyp6xer_list_activities(ctx=mock_ctx)
    count = _tokens(enc, output)
    data = json.loads(output)
    print(f"\nlist_activities (default limit=50): {data['count']} of {data['total']} activities → {count:,} tokens")
    assert count <= TOKEN_BUDGET, (
        f"list_activities unfiltered is {count:,} tokens. "
        "This tool needs a hard default page_size cap."
    )
