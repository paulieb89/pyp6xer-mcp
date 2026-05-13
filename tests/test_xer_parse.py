"""Tests for XER parse/serialize and the float-lag monkey-patch."""
import pytest


# ---------------------------------------------------------------------------
# Test 1: int_or_zero float string patch
# Regression: fractional lag values ("0.8") must not raise ValueError.
# The patch is applied at server import time.
# ---------------------------------------------------------------------------

def test_int_or_zero_handles_float_strings():
    import server  # noqa: F401 — triggers the _taskpred patch
    import xerparser.schemas.taskpred as taskpred

    fn = taskpred.int_or_zero
    assert fn("0.8") == 0
    assert fn("2.5") == 2
    assert fn("3") == 3
    assert fn("") == 0
    assert fn(None) == 0


# ---------------------------------------------------------------------------
# Tests 2 & 3: parse/serialize round-trip and column order preservation
# ---------------------------------------------------------------------------

MINIMAL_XER = (
    "ERMHDR\t3.1\tProject\n"
    "%T\tPROJECT\n"
    "%F\tproj_id\tproj_short_name\n"
    "%R\t1\tPROJ\n"
    "%E\n"
    "%T\tTASK\n"
    "%F\ttask_id\ttask_code\ttask_name\n"
    "%R\t100\tA1000\tExcavation\n"
    "%R\t101\tA1010\tConcreting\n"
    "%E\n"
)


def test_parse_serialize_roundtrip():
    from server import _parse_raw_tables, _serialize_xer

    header, table_order, raw_tables = _parse_raw_tables(MINIMAL_XER)
    serialized = _serialize_xer(header, table_order, raw_tables)
    header2, order2, tables2 = _parse_raw_tables(serialized)

    assert header == header2
    assert table_order == order2
    assert raw_tables == tables2


def test_column_order_preserved_through_roundtrip():
    from server import _parse_raw_tables, _serialize_xer

    header, table_order, raw_tables = _parse_raw_tables(MINIMAL_XER)
    # Columns must be in declared order before serialization
    assert raw_tables["TASK"]["cols"] == ["task_id", "task_code", "task_name"]

    serialized = _serialize_xer(header, table_order, raw_tables)
    _, _, tables2 = _parse_raw_tables(serialized)
    assert tables2["TASK"]["cols"] == ["task_id", "task_code", "task_name"]
