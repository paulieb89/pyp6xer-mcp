"""Tests for _apply_activity_update: raw-table mutation, change log, and validation."""
import pytest
from unittest.mock import MagicMock


def _make_entry(task_uid="100", task_code="A1000"):
    """Minimal cache entry: one TASK row with numeric and date fields."""
    mock_task = MagicMock()
    mock_task.uid = task_uid
    mock_task.task_code = task_code

    mock_xer = MagicMock()
    mock_xer.tasks = {task_uid: mock_task}

    return {
        "xer": mock_xer,
        "raw_tables": {
            "TASK": {
                "cols": ["task_id", "task_code", "phys_complete_pct", "remain_drtn_hr_cnt", "act_start_date"],
                "rows": [
                    {
                        "task_id": task_uid,
                        "task_code": task_code,
                        "phys_complete_pct": "0",
                        "remain_drtn_hr_cnt": "80",
                        "act_start_date": "",
                    }
                ],
            }
        },
        "table_order": ["TASK"],
        "header": "ERMHDR\t3.1",
        "source": "/tmp/test.xer",
    }


def _raw_row(entry):
    return entry["raw_tables"]["TASK"]["rows"][0]


# ---------------------------------------------------------------------------
# Test 5a: raw table row is mutated in place
# ---------------------------------------------------------------------------

def test_apply_update_modifies_raw_row():
    from server import _apply_activity_update

    entry = _make_entry()
    _apply_activity_update(entry, "A1000", None, {"phys_complete_pct": 75})
    assert _raw_row(entry)["phys_complete_pct"] == "75"


# ---------------------------------------------------------------------------
# Test 5b: change log has from/to for every updated field
# ---------------------------------------------------------------------------

def test_apply_update_returns_change_log():
    from server import _apply_activity_update

    entry = _make_entry()
    applied = _apply_activity_update(entry, "A1000", None, {"phys_complete_pct": 50})
    assert applied["phys_complete_pct"]["from"] == "0"
    assert applied["phys_complete_pct"]["to"] == "50"


# ---------------------------------------------------------------------------
# Test 5c: date fields are padded to XER datetime format "YYYY-MM-DD HH:MM"
# ---------------------------------------------------------------------------

def test_apply_update_date_field_pads_time():
    from server import _apply_activity_update

    entry = _make_entry()
    _apply_activity_update(entry, "A1000", None, {"act_start_date": "2025-06-01"})
    assert _raw_row(entry)["act_start_date"] == "2025-06-01 00:00"


# ---------------------------------------------------------------------------
# Test 6a: unknown field raises ValueError before any mutation
# ---------------------------------------------------------------------------

def test_apply_update_raises_on_unknown_field():
    from server import _apply_activity_update

    entry = _make_entry()
    with pytest.raises(ValueError, match="not updatable"):
        _apply_activity_update(entry, "A1000", None, {"bad_field": "x"})


# ---------------------------------------------------------------------------
# Test 6b: missing task raises ValueError
# ---------------------------------------------------------------------------

def test_apply_update_raises_when_task_not_found():
    from server import _apply_activity_update

    entry = _make_entry()
    with pytest.raises(ValueError, match="not found"):
        _apply_activity_update(entry, "MISSING", None, {"phys_complete_pct": 50})
