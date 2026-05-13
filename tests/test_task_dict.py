"""Tests for _task_to_dict: field projection and full summary field coverage."""
from datetime import datetime
from unittest.mock import MagicMock


def _make_mock_task():
    task = MagicMock()
    task.uid = "100"
    task.task_code = "A1000"
    task.name = "Excavation"
    task.status.value = "TK_Active"
    task.type.value = "TT_Task"
    task.wbs.full_code = "PROJ.WP1"
    task.wbs.name = "Work Package 1"
    task.start = datetime(2025, 1, 1)
    task.finish = datetime(2025, 3, 31)
    task.target_start_date = datetime(2025, 1, 1)
    task.target_end_date = datetime(2025, 3, 31)
    task.original_duration = 60
    task.remaining_duration = 30
    task.total_float = 5
    task.free_float = 2
    task.is_critical = False
    task.is_longest_path = False
    task.percent_complete = 0.5
    task.budgeted_cost = 10_000.0
    task.actual_cost = 5_000.0
    task.remaining_cost = 5_000.0
    return task


def test_field_projection_returns_only_requested_fields():
    from server import _task_to_dict

    result = _task_to_dict(_make_mock_task(), fields=["task_code", "name"])
    assert set(result.keys()) == {"task_code", "name"}
    assert result["task_code"] == "A1000"
    assert result["name"] == "Excavation"


def test_no_projection_returns_all_summary_fields():
    from server import _task_to_dict, ACTIVITY_SUMMARY_FIELDS

    result = _task_to_dict(_make_mock_task())
    for field in ACTIVITY_SUMMARY_FIELDS:
        assert field in result, f"Missing field: {field}"
