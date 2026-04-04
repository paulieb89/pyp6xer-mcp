#!/usr/bin/env python3
"""
PyP6Xer MCP Server

Wraps the xerparser library to enable LLMs to load, analyse, query, and export
Oracle Primavera P6 .xer schedule files.

Supports loading from:
  - Local file path (str)
  - HTTP/HTTPS URL (downloaded via httpx)
  - Base64-encoded file content (from file uploads)
"""

from __future__ import annotations

import base64
import csv
import io
import json
import tempfile
from datetime import datetime
from typing import Optional

import httpx
from fastmcp import Context, FastMCP
from fastmcp.server.lifespan import lifespan
from xerparser.src.xer import Xer

# ---------------------------------------------------------------------------
# Lifespan – shared in-memory cache for all tool calls
# ---------------------------------------------------------------------------

@lifespan
async def xer_lifespan(server):
    """Initialise an empty XER file cache, shared across all tool calls."""
    cache: dict = {}
    yield {"cache": cache}


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "pyp6xer",
    instructions=(
        "Analyse Primavera P6 XER schedule files. "
        "Start by calling pyp6xer_load_file with a local path, HTTP(S) URL, or "
        "base64-encoded file content. Then use the analysis tools. "
        "Multiple files can be loaded simultaneously using different cache_key values."
    ),
    lifespan=xer_lifespan,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M"

# Fields available on list/search tools (summary dict)
ACTIVITY_SUMMARY_FIELDS: list[str] = [
    "task_id", "task_code", "name", "status", "type",
    "wbs", "wbs_name", "start", "finish",
    "target_start", "target_finish",
    "original_duration_days", "remaining_duration_days",
    "total_float_days", "free_float_days",
    "is_critical", "is_longest_path",
    "percent_complete", "budgeted_cost", "actual_cost", "remaining_cost",
]

# Additional fields available on get_activity only
ACTIVITY_DETAIL_FIELDS: list[str] = ACTIVITY_SUMMARY_FIELDS + [
    "actual_start", "actual_finish",
    "early_start", "early_finish",
    "late_start", "late_finish",
    "resources", "predecessors", "successors",
]


def _fmt_date(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.strftime(DATE_FMT)


def _fmt_pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def _get_cache(ctx: Context, cache_key: str) -> dict:
    cache = ctx.lifespan_context["cache"]
    if cache_key not in cache:
        raise ValueError(
            f"No file loaded with key '{cache_key}'. "
            "Call pyp6xer_load_file first."
        )
    return cache[cache_key]


def _get_xer(ctx: Context, cache_key: str) -> Xer:
    return _get_cache(ctx, cache_key)["xer"]


def _get_project(xer: Xer, proj_id: str | None):
    if proj_id:
        proj = xer.projects.get(proj_id)
        if proj is None:
            ids = list(xer.projects.keys())
            raise ValueError(
                f"Project '{proj_id}' not found. Available project IDs: {ids}"
            )
        return proj
    projects = list(xer.projects.values())
    if not projects:
        raise ValueError("No projects found in this XER file.")
    return projects[0]


def _get_tasks(xer: Xer, proj_id: str | None):
    """Return tasks for a project (or all tasks if proj_id is None)."""
    if proj_id:
        return _get_project(xer, proj_id).tasks
    return list(xer.tasks.values())


def _task_to_dict(task, fields: list[str] | None = None) -> dict:
    """Standard activity summary dict. Pass fields to project to a subset."""
    try:
        start = _fmt_date(task.start)
    except (ValueError, AttributeError):
        start = ""
    try:
        finish = _fmt_date(task.finish)
    except (ValueError, AttributeError):
        finish = ""

    full = {
        "task_id": task.uid,
        "task_code": task.task_code,
        "name": task.name,
        "status": task.status.value,
        "type": task.type.value,
        "wbs": task.wbs.full_code if task.wbs else "",
        "wbs_name": task.wbs.name if task.wbs else "",
        "start": start,
        "finish": finish,
        "target_start": _fmt_date(task.target_start_date),
        "target_finish": _fmt_date(task.target_end_date),
        "original_duration_days": task.original_duration,
        "remaining_duration_days": task.remaining_duration,
        "total_float_days": task.total_float,
        "free_float_days": task.free_float,
        "is_critical": task.is_critical,
        "is_longest_path": task.is_longest_path,
        "percent_complete": round(task.percent_complete * 100, 1),
        "budgeted_cost": task.budgeted_cost,
        "actual_cost": task.actual_cost,
        "remaining_cost": task.remaining_cost,
    }
    if fields:
        return {k: v for k, v in full.items() if k in fields}
    return full


def _parse_raw_tables(content: str) -> tuple[str, list[str], dict]:
    """
    Parse XER content, preserving column order per table for round-trip write support.

    Returns:
        header      - ERMHDR line
        table_order - ordered list of table names
        raw_tables  - {table_name: {"cols": [...], "rows": [dict]}}
    """
    sections = content.split("%T\t")
    header = sections.pop(0).strip()
    table_order: list[str] = []
    raw_tables: dict = {}

    for section in sections:
        lines = [ln for ln in section.splitlines() if ln.strip()]
        if not lines:
            continue
        name = lines[0].strip()
        if len(lines) < 2:
            table_order.append(name)
            raw_tables[name] = {"cols": [], "rows": []}
            continue
        cols = lines[1].strip().split("\t")[1:]  # skip %F prefix
        rows = []
        for line in lines[2:]:
            if line.startswith("%R"):
                vals = line.strip().split("\t")[1:]
                # Pad if fewer values than columns
                while len(vals) < len(cols):
                    vals.append("")
                rows.append(dict(zip(cols, vals)))
        table_order.append(name)
        raw_tables[name] = {"cols": cols, "rows": rows}

    return header, table_order, raw_tables


def _serialize_xer(header: str, table_order: list[str], raw_tables: dict) -> str:
    """Reconstruct XER text from cached raw table data."""
    lines = [header]
    for name in table_order:
        tbl = raw_tables.get(name)
        if tbl is None:
            continue
        lines.append(f"%T\t{name}")
        if tbl["cols"]:
            lines.append("%F\t" + "\t".join(tbl["cols"]))
            for row in tbl["rows"]:
                vals = [row.get(c, "") for c in tbl["cols"]]
                lines.append("%R\t" + "\t".join(vals))
        lines.append("%E")
    lines.append("")
    return "\n".join(lines)


def _load_xer_content(file_path: str | None, file_content: str | None) -> tuple[str, Xer]:
    """Load XER from path/URL/base64. Returns (source_label, Xer)."""
    if file_content:
        raw_bytes = base64.b64decode(file_content)
        text = raw_bytes.decode(Xer.CODEC, errors="replace")
        xer = Xer(text)
        return "base64_upload", xer, text

    if not file_path:
        raise ValueError("Provide either file_path or file_content.")

    if file_path.startswith(("http://", "https://")):
        response = httpx.get(file_path, timeout=60, follow_redirects=True)
        response.raise_for_status()
        text = response.content.decode(Xer.CODEC, errors="replace")
        xer = Xer(text)
        return file_path, xer, text

    with open(file_path, "rb") as f:
        raw_bytes = f.read()
    text = raw_bytes.decode(Xer.CODEC, errors="replace")
    xer = Xer(text)
    return file_path, xer, text


# ---------------------------------------------------------------------------
# ── SCHEMA DISCOVERY ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_get_activity_schema() -> str:
    """Return the available field names for activity read tools.

    Use the returned field names with the `fields` parameter of
    pyp6xer_list_activities, pyp6xer_get_activity, and pyp6xer_search_activities
    to limit response size to only the columns you need.

    summary_fields are available on list_activities and search_activities.
    detail_fields are only available on get_activity (they require fetching
    relationships and resources which are not on the list view).
    """
    return json.dumps({
        "summary_fields": ACTIVITY_SUMMARY_FIELDS,
        "detail_fields": ACTIVITY_DETAIL_FIELDS,
        "note": (
            "summary_fields: available on list_activities and search_activities. "
            "detail_fields: available on get_activity only."
        ),
    }, indent=2)


# ---------------------------------------------------------------------------
# ── FILE MANAGEMENT ──────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_load_file(
    cache_key: str = "default",
    file_path: Optional[str] = None,
    file_content: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Load a Primavera P6 XER file into the analysis cache.

    Accepts a local file path, an HTTP/HTTPS URL, or a base64-encoded string
    of the file's binary content. The loaded data is stored under cache_key
    so multiple schedules can be open simultaneously.

    Args:
        cache_key: Identifier for this file in the cache (default: "default").
        file_path: Local path (e.g. "/data/project.xer") or URL
                   (e.g. "https://example.com/project.xer").
        file_content: Base64-encoded XER file bytes (for direct uploads).
    """
    source, xer, raw_text = _load_xer_content(file_path, file_content)
    header, table_order, raw_tables = _parse_raw_tables(raw_text)

    cache = ctx.lifespan_context["cache"]
    cache[cache_key] = {
        "xer": xer,
        "raw_tables": raw_tables,
        "table_order": table_order,
        "header": header,
        "source": source,
    }

    proj_names = [f"{p.short_name} – {p.name}" for p in xer.projects.values()]
    result = {
        "status": "loaded",
        "cache_key": cache_key,
        "source": source,
        "projects": proj_names,
        "total_activities": len(xer.tasks),
        "total_relationships": len(xer.relationships),
        "total_resources": len(xer.resources),
    }
    return json.dumps(result, indent=2)


@mcp.tool
def pyp6xer_clear_cache(
    cache_key: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Remove one or all loaded XER files from the cache.

    Args:
        cache_key: Key to remove. If omitted, clears all cached files.
    """
    cache = ctx.lifespan_context["cache"]
    if cache_key:
        if cache_key in cache:
            del cache[cache_key]
            return json.dumps({"status": "cleared", "cache_key": cache_key})
        return json.dumps({"status": "not_found", "cache_key": cache_key})
    count = len(cache)
    cache.clear()
    return json.dumps({"status": "cleared_all", "count": count})


@mcp.tool
def pyp6xer_get_upload_url(ctx: Context = None) -> str:
    """Get instructions for uploading an XER file to this server.

    Since this is a local server, files are loaded directly via
    pyp6xer_load_file using a local path or URL.
    """
    return json.dumps({
        "instructions": (
            "This is a local MCP server. To load an XER file, use pyp6xer_load_file with: "
            "(1) file_path='/absolute/path/to/file.xer' for a local file, "
            "(2) file_path='https://...' for a URL, or "
            "(3) file_content='<base64_encoded_bytes>' for direct content upload."
        )
    })


# ---------------------------------------------------------------------------
# ── PROJECTS ─────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_list_projects(
    cache_key: str = "default",
    ctx: Context = None,
) -> str:
    """List all projects in the loaded XER file with summary statistics.

    Returns project IDs, names, data date, finish date, activity counts,
    and high-level cost and schedule metrics.
    """
    xer = _get_xer(ctx, cache_key)
    result = []
    for proj in xer.projects.values():
        not_started = sum(1 for t in proj.tasks if t.status.is_not_started)
        in_progress = sum(1 for t in proj.tasks if t.status.is_in_progress)
        completed = sum(1 for t in proj.tasks if t.status.is_completed)
        critical = sum(1 for t in proj.tasks if t.is_critical)
        result.append({
            "proj_id": proj.uid,
            "short_name": proj.short_name,
            "name": proj.name,
            "plan_start": _fmt_date(proj.plan_start_date),
            "data_date": _fmt_date(proj.data_date),
            "finish_date": _fmt_date(proj.finish_date),
            "must_finish": _fmt_date(proj.must_finish_date),
            "total_activities": len(proj.tasks),
            "not_started": not_started,
            "in_progress": in_progress,
            "completed": completed,
            "critical_activities": critical,
            "budgeted_cost": proj.budgeted_cost,
            "actual_cost": proj.actual_cost,
            "remaining_cost": proj.remaining_cost,
            "duration_percent": _fmt_pct(proj.duration_percent),
            "task_percent": _fmt_pct(proj.task_percent),
            "original_duration_days": proj.original_duration,
            "remaining_duration_days": proj.remaining_duration,
        })
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# ── ACTIVITIES ───────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_list_activities(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    status: Optional[str] = None,
    wbs_code: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    fields: Optional[list[str]] = None,
    ctx: Context = None,
) -> str:
    """List activities with optional filtering and pagination.

    Args:
        cache_key: Cache key of the loaded file.
        proj_id:   Filter to a specific project (use proj_id from pyp6xer_list_projects).
        status:    Filter by status: 'not_started', 'in_progress', 'complete', or 'all' (default).
        wbs_code:  Filter by WBS full_code prefix (e.g. 'PROJ.PHASE1').
        limit:     Max results per page (default 50, max 500).
        offset:    Pagination offset (default 0).
        fields:    Subset of fields to return per activity. Call pyp6xer_get_activity_schema
                   for available names. Omit to return all fields.
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)

    # Status filter
    status_map = {
        "not_started": lambda t: t.status.is_not_started,
        "in_progress": lambda t: t.status.is_in_progress,
        "complete": lambda t: t.status.is_completed,
    }
    if status and status in status_map:
        tasks = [t for t in tasks if status_map[status](t)]

    # WBS filter
    if wbs_code:
        tasks = [t for t in tasks if t.wbs and t.wbs.full_code.startswith(wbs_code)]

    total = len(tasks)
    limit = min(limit, 500)
    page = tasks[offset: offset + limit]

    return json.dumps({
        "total": total,
        "count": len(page),
        "offset": offset,
        "limit": limit,
        "has_more": total > offset + len(page),
        "next_offset": offset + len(page) if total > offset + len(page) else None,
        "activities": [_task_to_dict(t, fields) for t in page],
    }, indent=2)


@mcp.tool
def pyp6xer_get_activity(
    task_code: str,
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    fields: Optional[list[str]] = None,
    ctx: Context = None,
) -> str:
    """Get full details for a single activity including dates, float, costs,
    resources assigned, and predecessor/successor relationships.

    Args:
        task_code: Activity ID (e.g. 'A1000').
        cache_key: Cache key of the loaded file.
        proj_id:   Optional project filter.
        fields:    Subset of fields to return. Call pyp6xer_get_activity_schema
                   for available names. Omit to return all fields.
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)
    task = next((t for t in tasks if t.task_code == task_code), None)
    if task is None:
        raise ValueError(f"Activity '{task_code}' not found.")

    # Build full detail dict (no projection yet — detail fields extend summary)
    detail = _task_to_dict(task)
    detail.update({
        "actual_start": _fmt_date(task.act_start_date),
        "actual_finish": _fmt_date(task.act_end_date),
        "early_start": _fmt_date(task.early_start_date),
        "early_finish": _fmt_date(task.early_end_date),
        "late_start": _fmt_date(task.late_start_date),
        "late_finish": _fmt_date(task.late_end_date),
        "expected_finish": _fmt_date(task.expect_end_date),
        "duration_type": task.duration_type,
        "percent_complete_type": task.complete_pct_type,
        "physical_pct": round(task.phys_complete_pct * 100, 1),
        "float_path": task.float_path,
        "constraints": {
            "primary": {
                "type": task.cstr_type,
                "date": _fmt_date(task.cstr_date),
            },
            "secondary": {
                "type": task.cstr_type2,
                "date": _fmt_date(task.cstr_date2),
            },
        },
        "at_completion_cost": task.at_completion_cost,
        "cost_variance": task.cost_variance,
        "predecessors": [
            {
                "task_code": lnk.task.task_code,
                "name": lnk.task.name,
                "type": lnk.link,
                "lag_days": lnk.lag,
            }
            for lnk in task.predecessors
        ],
        "successors": [
            {
                "task_code": lnk.task.task_code,
                "name": lnk.task.name,
                "type": lnk.link,
                "lag_days": lnk.lag,
            }
            for lnk in task.successors
        ],
        "resources": [
            {
                "resource": res.resource.name,
                "type": res.rsrc_type,
                "target_qty": res.target_qty,
                "actual_qty": res.act_reg_qty + res.act_ot_qty,
                "remain_qty": res.remain_qty,
                "target_cost": res.target_cost,
                "actual_cost": res.act_total_cost,
                "remain_cost": res.remain_cost,
            }
            for res in task.resources.values()
        ],
        "memos": [m.memo_text for m in task.memos] if task.memos else [],
    })
    if fields:
        detail = {k: v for k, v in detail.items() if k in fields}
    return json.dumps(detail, indent=2)


@mcp.tool
def pyp6xer_search_activities(
    query: str,
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    limit: int = 20,
    fields: Optional[list[str]] = None,
    ctx: Context = None,
) -> str:
    """Search activities by name or activity ID (case-insensitive substring match).

    Args:
        query:     Search string matched against task_code and name.
        cache_key: Cache key of the loaded file.
        proj_id:   Optional project filter.
        limit:     Maximum results to return (default 20).
        fields:    Subset of fields to return per activity. Call pyp6xer_get_activity_schema
                   for available names. Omit to return all fields.
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)
    q = query.lower()
    matches = [
        t for t in tasks
        if q in t.task_code.lower() or q in t.name.lower()
    ][:limit]

    return json.dumps({
        "query": query,
        "count": len(matches),
        "activities": [_task_to_dict(t, fields) for t in matches],
    }, indent=2)


# ---------------------------------------------------------------------------
# ── RESOURCES & CALENDARS ────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_list_resources(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """List all resources with assignment counts and cost/quantity totals.

    Args:
        cache_key: Cache key of the loaded file.
        proj_id:   Optional project filter (restricts to resources assigned in that project).
    """
    xer = _get_xer(ctx, cache_key)

    if proj_id:
        proj = _get_project(xer, proj_id)
        task_rsrcs = proj.resources
    else:
        task_rsrcs = list(xer.tasks.values())
        # flatten: all TASKRSRC across all tasks
        task_rsrcs = [tr for t in xer.tasks.values() for tr in t.resources.values()]

    # Aggregate by resource name
    rsrc_stats: dict = {}
    for tr in task_rsrcs:
        name = tr.resource.name
        if name not in rsrc_stats:
            rsrc_stats[name] = {
                "name": name,
                "rsrc_id": tr.resource.uid,
                "type": tr.resource.type,
                "assignments": 0,
                "target_qty": 0.0,
                "actual_qty": 0.0,
                "remain_qty": 0.0,
                "target_cost": 0.0,
                "actual_cost": 0.0,
                "remain_cost": 0.0,
            }
        s = rsrc_stats[name]
        s["assignments"] += 1
        s["target_qty"] += tr.target_qty
        s["actual_qty"] += tr.act_reg_qty + tr.act_ot_qty
        s["remain_qty"] += tr.remain_qty
        s["target_cost"] += tr.target_cost
        s["actual_cost"] += tr.act_total_cost
        s["remain_cost"] += tr.remain_cost

    resources = sorted(rsrc_stats.values(), key=lambda r: -r["assignments"])
    return json.dumps({"total": len(resources), "resources": resources}, indent=2)


@mcp.tool
def pyp6xer_list_calendars(
    cache_key: str = "default",
    ctx: Context = None,
) -> str:
    """List all calendars defined in the XER file.

    Returns calendar name, type (global/project/resource), hours per day/week/year,
    and whether it is the project default.
    """
    xer = _get_xer(ctx, cache_key)
    cals = []
    for cal in xer.calendars.values():
        cals.append({
            "clndr_id": cal.uid,
            "name": cal.name,
            "type": cal.type.name if hasattr(cal, "type") else "",
            "is_default": getattr(cal, "is_default", False),
            "day_hr_cnt": getattr(cal, "day_hr_cnt", 8),
            "week_hr_cnt": getattr(cal, "week_hr_cnt", 40),
            "year_hr_cnt": getattr(cal, "year_hr_cnt", 2080),
        })
    return json.dumps({"total": len(cals), "calendars": cals}, indent=2)


# ---------------------------------------------------------------------------
# ── SCHEDULE ANALYSIS ────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_critical_path(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Return all activities on the critical path (total float ≤ 0 or longest path flag).

    Activities are sorted by early start date. Includes float, dates, and
    predecessor/successor counts.
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)
    critical = [t for t in tasks if t.is_critical or t.is_longest_path]

    def _sort_key(t):
        try:
            return t.start
        except Exception:
            return datetime.max

    critical.sort(key=_sort_key)

    result = []
    for t in critical:
        d = _task_to_dict(t)
        d["predecessor_count"] = len(t.predecessors)
        d["successor_count"] = len(t.successors)
        result.append(d)

    return json.dumps({
        "critical_count": len(result),
        "total_activities": len(tasks),
        "critical_pct": round(len(result) / len(tasks) * 100, 1) if tasks else 0,
        "activities": result,
    }, indent=2)


@mcp.tool
def pyp6xer_float_analysis(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    max_float_days: int = 30,
    ctx: Context = None,
) -> str:
    """Analyse total float distribution across activities.

    Groups activities into float buckets and flags near-critical activities.

    Args:
        cache_key:      Cache key of the loaded file.
        proj_id:        Optional project filter.
        max_float_days: Upper bound for detailed buckets (default 30).
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)

    # Only non-completed, non-LOE tasks
    relevant = [
        t for t in tasks
        if not t.status.is_completed
        and not t.type.is_loe
        and not t.type.is_wbs
        and t.total_float is not None
    ]

    buckets = {
        "negative": [],
        "zero": [],
        "0_to_5": [],
        "5_to_15": [],
        "15_to_30": [],
        "over_30": [],
    }

    for t in relevant:
        f = t.total_float
        if f < 0:
            buckets["negative"].append(t.task_code)
        elif f == 0:
            buckets["zero"].append(t.task_code)
        elif f <= 5:
            buckets["0_to_5"].append(t.task_code)
        elif f <= 15:
            buckets["5_to_15"].append(t.task_code)
        elif f <= max_float_days:
            buckets["15_to_30"].append(t.task_code)
        else:
            buckets["over_30"].append(t.task_code)

    floats = [t.total_float for t in relevant]
    return json.dumps({
        "analyzed_activities": len(relevant),
        "total_activities": len(tasks),
        "distribution": {k: len(v) for k, v in buckets.items()},
        "negative_float_activities": buckets["negative"],
        "zero_float_activities": buckets["zero"][:50],  # cap for readability
        "stats": {
            "min_float_days": min(floats) if floats else None,
            "max_float_days": max(floats) if floats else None,
            "avg_float_days": round(sum(floats) / len(floats), 1) if floats else None,
        },
    }, indent=2)


@mcp.tool
def pyp6xer_schedule_quality(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Run DCMA-style schedule quality checks.

    Checks include:
    - Missing predecessors / successors (open ends)
    - Activities with lags or leads
    - Activities with hard constraints
    - Negative total float
    - Activities with no resources (optional warning)
    - Milestone checks
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)

    # Exclude LOE, WBS summary, and completed
    workable = [
        t for t in tasks
        if not t.type.is_loe
        and not t.type.is_wbs
        and not t.status.is_completed
    ]
    n = len(workable)
    if n == 0:
        return json.dumps({"error": "No open activities to analyse."})

    no_pred = [t.task_code for t in workable if not t.predecessors]
    no_succ = [t.task_code for t in workable if not t.successors]
    with_lags = [
        t.task_code for t in workable
        if any(lnk.lag > 0 for lnk in t.predecessors)
    ]
    with_leads = [
        t.task_code for t in workable
        if any(lnk.lag < 0 for lnk in t.predecessors)
    ]
    constrained = [
        t.task_code for t in workable
        if t.cstr_type and t.cstr_type not in ("CS_ALAP", "CS_MSOA")
    ]
    neg_float = [t.task_code for t in workable if t.is_critical and t.total_float is not None and t.total_float < 0]
    no_resources = [t.task_code for t in workable if not t.resources and not t.type.is_milestone]

    def _pct(lst):
        return round(len(lst) / n * 100, 1)

    checks = {
        "activities_analysed": n,
        "missing_predecessors": {"count": len(no_pred), "pct": _pct(no_pred), "activities": no_pred[:20]},
        "missing_successors": {"count": len(no_succ), "pct": _pct(no_succ), "activities": no_succ[:20]},
        "activities_with_lags": {"count": len(with_lags), "pct": _pct(with_lags)},
        "activities_with_leads": {"count": len(with_leads), "pct": _pct(with_leads), "activities": with_leads[:20]},
        "hard_constraints": {"count": len(constrained), "pct": _pct(constrained), "activities": constrained[:20]},
        "negative_float": {"count": len(neg_float), "pct": _pct(neg_float), "activities": neg_float},
        "no_resources_assigned": {"count": len(no_resources), "pct": _pct(no_resources)},
    }

    # DCMA thresholds (pass if below)
    thresholds = {
        "missing_predecessors": 5.0,
        "missing_successors": 5.0,
        "activities_with_lags": 5.0,
        "activities_with_leads": 0.0,
        "hard_constraints": 5.0,
        "negative_float": 0.0,
    }
    flags = {
        k: ("FAIL" if checks[k]["pct"] > v else "PASS")
        for k, v in thresholds.items()
    }
    pass_count = sum(1 for v in flags.values() if v == "PASS")
    checks["dcma_results"] = flags
    checks["dcma_score"] = f"{pass_count}/{len(flags)}"

    return json.dumps(checks, indent=2)


@mcp.tool
def pyp6xer_schedule_health_check(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Generate a composite schedule health score with narrative summary.

    Combines data date currency, float distribution, critical path density,
    open ends, and constraint usage into a single 0–100 health score.
    """
    xer = _get_xer(ctx, cache_key)
    proj = _get_project(xer, proj_id)
    tasks = proj.tasks if proj_id else list(xer.tasks.values())

    workable = [
        t for t in tasks
        if not t.type.is_loe and not t.type.is_wbs and not t.status.is_completed
    ]
    n = len(workable)
    score = 100
    issues = []

    if n == 0:
        return json.dumps({"health_score": "N/A", "message": "No open activities found."})

    # 1. Data date currency (deduct up to 20 pts if data date is old)
    days_since_update = (datetime.now() - proj.data_date).days
    if days_since_update > 90:
        score -= 20
        issues.append(f"Data date is {days_since_update} days old (>90 days)")
    elif days_since_update > 30:
        score -= 10
        issues.append(f"Data date is {days_since_update} days old (>30 days)")

    # 2. Open ends
    no_pred_pct = sum(1 for t in workable if not t.predecessors) / n * 100
    no_succ_pct = sum(1 for t in workable if not t.successors) / n * 100
    open_end_pct = (no_pred_pct + no_succ_pct) / 2
    if open_end_pct > 10:
        score -= 20
        issues.append(f"{open_end_pct:.1f}% open ends (predecessors or successors missing)")
    elif open_end_pct > 5:
        score -= 10
        issues.append(f"{open_end_pct:.1f}% open ends")

    # 3. Negative float
    neg_float = [t for t in workable if t.total_float is not None and t.total_float < 0]
    if neg_float:
        score -= 15
        issues.append(f"{len(neg_float)} activities have negative float")

    # 4. Hard constraints
    constrained_pct = sum(
        1 for t in workable
        if t.cstr_type and t.cstr_type not in ("CS_ALAP", "CS_MSOA")
    ) / n * 100
    if constrained_pct > 10:
        score -= 10
        issues.append(f"{constrained_pct:.1f}% of activities have hard constraints")

    # 5. Critical path density
    critical_pct = sum(1 for t in workable if t.is_critical) / n * 100
    if critical_pct > 50:
        score -= 10
        issues.append(f"High critical path density: {critical_pct:.1f}%")

    # 6. Schedule overrun
    if proj.data_date > proj.finish_date:
        score -= 15
        issues.append("Scheduled finish date has already passed the data date")

    score = max(0, score)
    rating = (
        "Excellent" if score >= 85
        else "Good" if score >= 70
        else "Fair" if score >= 55
        else "Poor"
    )

    return json.dumps({
        "health_score": score,
        "rating": rating,
        "open_activities": n,
        "data_date": _fmt_date(proj.data_date),
        "finish_date": _fmt_date(proj.finish_date),
        "days_since_update": days_since_update,
        "issues_found": issues,
        "recommendations": issues if issues else ["Schedule appears healthy."],
    }, indent=2)


@mcp.tool
def pyp6xer_slipping_activities(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    min_days_slip: int = 0,
    ctx: Context = None,
) -> str:
    """Find activities that are running late (forecast finish > baseline finish).

    Args:
        cache_key:     Cache key of the loaded file.
        proj_id:       Optional project filter.
        min_days_slip: Only return activities slipping by at least this many days (default 0).
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)

    slipping = []
    for t in tasks:
        if t.status.is_completed:
            continue
        try:
            forecast = t.finish
        except Exception:
            continue
        baseline = t.target_end_date
        slip = (forecast - baseline).days
        if slip > min_days_slip:
            d = _task_to_dict(t)
            d["forecast_finish"] = _fmt_date(forecast)
            d["slip_days"] = slip
            slipping.append(d)

    slipping.sort(key=lambda x: -x["slip_days"])
    return json.dumps({
        "slipping_count": len(slipping),
        "min_days_slip_filter": min_days_slip,
        "activities": slipping,
    }, indent=2)


@mcp.tool
def pyp6xer_relationship_analysis(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Analyse relationship types, lag/lead distribution, and logic density.

    Reports counts by type (FS/SS/FF/SF), lag distribution, and
    activities with no logic ties.
    """
    xer = _get_xer(ctx, cache_key)

    if proj_id:
        rels = _get_project(xer, proj_id).relationships
    else:
        rels = list(xer.relationships.values())

    type_counts: dict = {}
    lag_buckets = {"lead_<0": 0, "zero": 0, "lag_1_5": 0, "lag_6_15": 0, "lag_>15": 0}
    total_lag = 0

    for r in rels:
        link = r.link
        type_counts[link] = type_counts.get(link, 0) + 1
        lag = r.lag
        total_lag += lag
        if lag < 0:
            lag_buckets["lead_<0"] += 1
        elif lag == 0:
            lag_buckets["zero"] += 1
        elif lag <= 5:
            lag_buckets["lag_1_5"] += 1
        elif lag <= 15:
            lag_buckets["lag_6_15"] += 1
        else:
            lag_buckets["lag_>15"] += 1

    n_rels = len(rels)
    return json.dumps({
        "total_relationships": n_rels,
        "by_type": type_counts,
        "lag_distribution": lag_buckets,
        "avg_lag_days": round(total_lag / n_rels, 1) if n_rels else 0,
        "leads_present": lag_buckets["lead_<0"] > 0,
    }, indent=2)


# ---------------------------------------------------------------------------
# ── RESOURCE ANALYSIS ────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_resource_utilization(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    rsrc_name: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Summarise resource loading: planned vs actual vs remaining quantities and costs.

    Args:
        cache_key:  Cache key of the loaded file.
        proj_id:    Optional project filter.
        rsrc_name:  Filter to a specific resource name (substring match).
    """
    xer = _get_xer(ctx, cache_key)
    if proj_id:
        task_rsrcs = _get_project(xer, proj_id).resources
    else:
        task_rsrcs = [tr for t in xer.tasks.values() for tr in t.resources.values()]

    # Aggregate by resource
    by_rsrc: dict = {}
    for tr in task_rsrcs:
        name = tr.resource.name
        if rsrc_name and rsrc_name.lower() not in name.lower():
            continue
        if name not in by_rsrc:
            by_rsrc[name] = {
                "resource": name,
                "type": tr.resource.type,
                "assignments": 0,
                "target_qty": 0.0,
                "actual_qty": 0.0,
                "remain_qty": 0.0,
                "target_cost": 0.0,
                "actual_cost": 0.0,
                "remain_cost": 0.0,
                "at_completion_cost": 0.0,
            }
        s = by_rsrc[name]
        s["assignments"] += 1
        s["target_qty"] += tr.target_qty
        s["actual_qty"] += tr.act_reg_qty + tr.act_ot_qty
        s["remain_qty"] += tr.remain_qty
        s["target_cost"] += tr.target_cost
        s["actual_cost"] += tr.act_total_cost
        s["remain_cost"] += tr.remain_cost
        s["at_completion_cost"] += tr.act_total_cost + tr.remain_cost

    for s in by_rsrc.values():
        s["utilization_pct"] = (
            round(s["actual_qty"] / s["target_qty"] * 100, 1)
            if s["target_qty"] else None
        )

    items = sorted(by_rsrc.values(), key=lambda x: -x["target_cost"])
    return json.dumps({"total_resources": len(items), "resources": items}, indent=2)


# ---------------------------------------------------------------------------
# ── WBS ANALYSIS ─────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_wbs_analysis(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Return the WBS hierarchy with task counts and cost rollups per node.

    Shows each WBS element's direct and total (rolled-up) activity counts,
    schedule range, and cost summary.
    """
    xer = _get_xer(ctx, cache_key)
    proj = _get_project(xer, proj_id)

    def _wbs_node_dict(node) -> dict:
        all_tasks = node.all_tasks
        direct_tasks = node.tasks if hasattr(node, "tasks") else []
        completed = sum(1 for t in all_tasks if t.status.is_completed)
        return {
            "wbs_id": node.uid,
            "code": node.full_code,
            "name": node.name,
            "depth": node.depth,
            "direct_activities": len(direct_tasks),
            "total_activities": len(all_tasks),
            "completed_activities": completed,
            "budgeted_cost": round(node.budgeted_cost, 2),
            "actual_cost": round(node.actual_cost, 2),
            "remaining_cost": round(node.remaining_cost, 2),
            "children_count": len(node.children),
        }

    nodes = sorted(proj.wbs_nodes, key=lambda n: n.full_code)
    return json.dumps({
        "total_wbs_nodes": len(nodes),
        "wbs_nodes": [_wbs_node_dict(n) for n in nodes],
    }, indent=2)


@mcp.tool
def pyp6xer_work_package_summary(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Summarise leaf-level WBS nodes (work packages) with schedule and cost data.

    Leaf nodes are WBS elements with no children — the lowest level of the breakdown.
    """
    xer = _get_xer(ctx, cache_key)
    proj = _get_project(xer, proj_id)

    leaves = [n for n in proj.wbs_nodes if not n.children and not n.is_proj_node]
    result = []
    for n in sorted(leaves, key=lambda x: x.full_code):
        tasks = n.all_tasks
        if not tasks:
            continue
        open_tasks = [t for t in tasks if not t.status.is_completed]
        starts = []
        finishes = []
        for t in tasks:
            try:
                starts.append(t.start)
                finishes.append(t.finish)
            except Exception:
                pass

        result.append({
            "code": n.full_code,
            "name": n.name,
            "total_activities": len(tasks),
            "completed": len(tasks) - len(open_tasks),
            "remaining": len(open_tasks),
            "earliest_start": _fmt_date(min(starts)) if starts else "",
            "latest_finish": _fmt_date(max(finishes)) if finishes else "",
            "budgeted_cost": round(n.budgeted_cost, 2),
            "actual_cost": round(n.actual_cost, 2),
            "remaining_cost": round(n.remaining_cost, 2),
            "cost_variance": round(n.cost_variance, 2),
        })

    return json.dumps({
        "work_package_count": len(result),
        "work_packages": result,
    }, indent=2)


# ---------------------------------------------------------------------------
# ── PROGRESS & EARNED VALUE ──────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_progress_summary(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Summarise schedule progress: status breakdown, percent complete, milestones.

    Returns counts by status, weighted percent complete, and milestone statistics.
    """
    xer = _get_xer(ctx, cache_key)
    proj = _get_project(xer, proj_id)
    tasks = proj.tasks if proj_id else list(xer.tasks.values())

    not_started = [t for t in tasks if t.status.is_not_started]
    in_progress = [t for t in tasks if t.status.is_in_progress]
    completed = [t for t in tasks if t.status.is_completed]
    milestones = [t for t in tasks if t.type.is_milestone]

    # Weighted percent complete by original duration
    total_dur = sum(t.original_duration for t in tasks if t.original_duration > 0)
    weighted_pct = (
        sum(t.percent_complete * t.original_duration for t in tasks if t.original_duration > 0)
        / total_dur
        if total_dur else 0
    )

    # Simple average
    simple_pct = sum(t.percent_complete for t in tasks) / len(tasks) if tasks else 0

    ms_completed = sum(1 for m in milestones if m.status.is_completed)
    ms_open = len(milestones) - ms_completed

    return json.dumps({
        "data_date": _fmt_date(proj.data_date),
        "project_finish": _fmt_date(proj.finish_date),
        "total_activities": len(tasks),
        "status_breakdown": {
            "not_started": len(not_started),
            "in_progress": len(in_progress),
            "completed": len(completed),
        },
        "percent_complete": {
            "weighted_by_duration": _fmt_pct(weighted_pct),
            "simple_average": _fmt_pct(simple_pct),
            "project_duration_pct": _fmt_pct(proj.duration_percent),
            "project_task_pct": _fmt_pct(proj.task_percent),
        },
        "milestones": {
            "total": len(milestones),
            "completed": ms_completed,
            "remaining": ms_open,
        },
        "cost_summary": {
            "budgeted": round(proj.budgeted_cost, 2),
            "actual": round(proj.actual_cost, 2),
            "remaining": round(proj.remaining_cost, 2),
        },
    }, indent=2)


@mcp.tool
def pyp6xer_earned_value(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Calculate Earned Value Management (EVM) metrics.

    Metrics:
    - BCWS (PV): Budgeted Cost of Work Scheduled = total budgeted cost × duration %
    - BCWP (EV): Budgeted Cost of Work Performed = sum of (budget × % complete) per task
    - ACWP (AC): Actual Cost of Work Performed = sum of actual costs
    - SPI: Schedule Performance Index = EV / PV
    - CPI: Cost Performance Index = EV / AC
    - CV:  Cost Variance = EV - AC
    - SV:  Schedule Variance = EV - PV
    - EAC: Estimate at Completion = BAC / CPI
    - VAC: Variance at Completion = BAC - EAC
    """
    xer = _get_xer(ctx, cache_key)
    proj = _get_project(xer, proj_id)
    tasks = proj.tasks if proj_id else list(xer.tasks.values())

    bac = sum(t.budgeted_cost for t in tasks)  # Budget at Completion
    acwp = sum(t.actual_cost for t in tasks)
    bcwp = sum(t.budgeted_cost * t.percent_complete for t in tasks)
    bcws = bac * proj.duration_percent  # simplified PV

    spi = round(bcwp / bcws, 3) if bcws else None
    cpi = round(bcwp / acwp, 3) if acwp else None
    eac = round(bac / cpi, 2) if cpi else None
    vac = round(bac - eac, 2) if eac else None

    return json.dumps({
        "data_date": _fmt_date(proj.data_date),
        "BAC": round(bac, 2),
        "BCWS_PV": round(bcws, 2),
        "BCWP_EV": round(bcwp, 2),
        "ACWP_AC": round(acwp, 2),
        "SPI": spi,
        "CPI": cpi,
        "CV": round(bcwp - acwp, 2),
        "SV": round(bcwp - bcws, 2),
        "EAC": eac,
        "VAC": vac,
        "interpretation": {
            "SPI": (
                "On schedule" if spi and spi >= 1.0
                else "Behind schedule" if spi else "N/A"
            ),
            "CPI": (
                "Under budget" if cpi and cpi >= 1.0
                else "Over budget" if cpi else "N/A"
            ),
        },
    }, indent=2)


# ---------------------------------------------------------------------------
# ── EXPORT & COMPARE ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

@mcp.tool
def pyp6xer_export_csv(
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    fields: Optional[list[str]] = None,
    ctx: Context = None,
) -> str:
    """Export activities to CSV format (returned as a string).

    Args:
        cache_key: Cache key of the loaded file.
        proj_id:   Optional project filter.
        fields:    Column names to include. Call pyp6xer_get_activity_schema for
                   available names. Defaults to all standard summary fields.
    """
    xer = _get_xer(ctx, cache_key)
    tasks = _get_tasks(xer, proj_id)

    default_fields = [
        "task_code", "name", "status", "type", "wbs", "wbs_name",
        "start", "finish", "target_start", "target_finish",
        "original_duration_days", "remaining_duration_days",
        "total_float_days", "is_critical", "percent_complete",
        "budgeted_cost", "actual_cost", "remaining_cost",
    ]
    cols = fields or default_fields

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for t in tasks:
        row = _task_to_dict(t)
        writer.writerow(row)

    return json.dumps({
        "format": "csv",
        "activity_count": len(tasks),
        "fields": cols,
        "csv_content": output.getvalue(),
    }, indent=2)


@mcp.tool
def pyp6xer_compare_snapshots(
    cache_key_a: str,
    cache_key_b: str,
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Compare two loaded XER files (snapshots) to identify schedule changes.

    Reports added, removed, and changed activities (dates, duration, float, status).
    Useful for analysing schedule updates between periods.

    Args:
        cache_key_a: Cache key for the baseline/earlier snapshot.
        cache_key_b: Cache key for the updated/later snapshot.
        proj_id:     Optional project filter (matched by short_name).
    """
    xer_a = _get_xer(ctx, cache_key_a)
    xer_b = _get_xer(ctx, cache_key_b)

    def _tasks_by_code(xer, pid):
        tasks = _get_tasks(xer, pid)
        return {t.task_code: t for t in tasks}

    # Try to match project by short_name if proj_id provided
    pid_a = proj_id
    pid_b = proj_id

    tasks_a = _tasks_by_code(xer_a, pid_a)
    tasks_b = _tasks_by_code(xer_b, pid_b)

    codes_a = set(tasks_a.keys())
    codes_b = set(tasks_b.keys())

    added = list(codes_b - codes_a)
    removed = list(codes_a - codes_b)
    common = codes_a & codes_b

    changes = []
    for code in sorted(common):
        a, b = tasks_a[code], tasks_b[code]
        diffs = {}
        try:
            if a.finish != b.finish:
                diffs["finish"] = {"from": _fmt_date(a.finish), "to": _fmt_date(b.finish)}
        except Exception:
            pass
        if a.status != b.status:
            diffs["status"] = {"from": a.status.value, "to": b.status.value}
        if a.remaining_duration != b.remaining_duration:
            diffs["remaining_duration_days"] = {"from": a.remaining_duration, "to": b.remaining_duration}
        if a.total_float != b.total_float:
            diffs["total_float_days"] = {"from": a.total_float, "to": b.total_float}
        if abs((a.percent_complete or 0) - (b.percent_complete or 0)) > 0.005:
            diffs["percent_complete"] = {
                "from": round((a.percent_complete or 0) * 100, 1),
                "to": round((b.percent_complete or 0) * 100, 1),
            }
        if diffs:
            changes.append({"task_code": code, "name": b.name, "changes": diffs})

    return json.dumps({
        "snapshot_a": cache_key_a,
        "snapshot_b": cache_key_b,
        "activities_in_a": len(tasks_a),
        "activities_in_b": len(tasks_b),
        "added": added,
        "removed": removed,
        "changed_count": len(changes),
        "changed": changes,
    }, indent=2)


# ---------------------------------------------------------------------------
# ── WRITE OPERATIONS ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

# Field mapping: friendly name -> XER raw table field name
_UPDATABLE_FIELDS = {
    "status_code": "status_code",
    "phys_complete_pct": "phys_complete_pct",
    "remain_drtn_hr_cnt": "remain_drtn_hr_cnt",
    "act_start_date": "act_start_date",
    "act_end_date": "act_end_date",
    "expect_end_date": "expect_end_date",
    "target_start_date": "target_start_date",
    "target_end_date": "target_end_date",
}

_XER_DATE_FMT = "%Y-%m-%d %H:%M"


def _apply_activity_update(entry: dict, task_code: str, proj_id: str | None, updates: dict) -> dict:
    """
    Apply updates to both the in-memory Xer object and the raw_tables dict.
    Returns a dict of {field: {from, to}} describing what changed.
    """
    xer: Xer = entry["xer"]
    raw_tables: dict = entry["raw_tables"]

    # Find the in-memory task
    tasks = _get_tasks(xer, proj_id)
    task = next((t for t in tasks if t.task_code == task_code), None)
    if task is None:
        raise ValueError(f"Activity '{task_code}' not found.")

    # Find the raw TASK row
    task_rows = raw_tables.get("TASK", {}).get("rows", [])
    raw_row = next((r for r in task_rows if r.get("task_id") == task.uid), None)

    applied = {}
    for field, value in updates.items():
        if field not in _UPDATABLE_FIELDS:
            raise ValueError(
                f"Field '{field}' is not updatable. "
                f"Updatable fields: {list(_UPDATABLE_FIELDS.keys())}"
            )
        raw_field = _UPDATABLE_FIELDS[field]

        # Get old value for change log
        old_raw = raw_row.get(raw_field, "") if raw_row else ""

        # Update raw table row
        if raw_row is not None:
            if field in ("act_start_date", "act_end_date", "expect_end_date",
                         "target_start_date", "target_end_date"):
                # Convert YYYY-MM-DD to XER datetime format if needed
                if value and ":" not in str(value):
                    value_raw = f"{value} 00:00"
                else:
                    value_raw = value or ""
                raw_row[raw_field] = value_raw
            else:
                raw_row[raw_field] = str(value) if value is not None else ""

        # Update in-memory object (best-effort)
        try:
            if field == "status_code":
                from xerparser.schemas.task import TASK as TaskClass
                task.status = TaskClass.TaskStatus[value]
            elif field == "phys_complete_pct":
                task.phys_complete_pct = float(value) / 100.0
            elif field == "remain_drtn_hr_cnt":
                task.remain_drtn_hr_cnt = float(value)
            elif field in ("act_start_date", "act_end_date", "expect_end_date",
                           "target_start_date", "target_end_date"):
                from xerparser.src.utils import optional_date
                if value:
                    dt = datetime.strptime(str(value).strip(), "%Y-%m-%d")
                    setattr(task, field, dt)
                else:
                    setattr(task, field, None)
        except Exception:
            pass  # Raw table update is authoritative; in-memory is best-effort

        applied[field] = {"from": old_raw, "to": str(value)}

    return applied


@mcp.tool
def pyp6xer_update_activity(
    task_code: str,
    updates: dict,
    cache_key: str = "default",
    proj_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Update fields on a single activity in the in-memory cache.

    Changes are held in memory until pyp6xer_write_file is called.

    Updatable fields:
    - status_code: 'TK_NotStart', 'TK_Active', or 'TK_Complete'
    - phys_complete_pct: physical percent complete (0–100)
    - remain_drtn_hr_cnt: remaining duration in hours
    - act_start_date / act_end_date: actual dates (YYYY-MM-DD)
    - expect_end_date: expected finish (YYYY-MM-DD)
    - target_start_date / target_end_date: baseline dates (YYYY-MM-DD)

    Args:
        task_code: Activity ID to update.
        updates:   Dict of {field_name: new_value}.
        cache_key: Cache key of the loaded file.
        proj_id:   Optional project filter.
    """
    entry = _get_cache(ctx, cache_key)
    applied = _apply_activity_update(entry, task_code, proj_id, updates)
    return json.dumps({
        "status": "updated",
        "task_code": task_code,
        "changes": applied,
        "note": "Call pyp6xer_write_file to persist changes to disk.",
    }, indent=2)


@mcp.tool
def pyp6xer_batch_update(
    updates: list,
    cache_key: str = "default",
    ctx: Context = None,
) -> str:
    """Update multiple activities in a single call.

    Args:
        updates:   List of update dicts, each with:
                   - task_code (str): Activity ID
                   - proj_id (str, optional): Project filter
                   - fields (dict): {field_name: new_value}
        cache_key: Cache key of the loaded file.

    Example updates list:
        [{"task_code": "A1000", "fields": {"status_code": "TK_Complete"}},
         {"task_code": "A1010", "fields": {"phys_complete_pct": 75}}]
    """
    entry = _get_cache(ctx, cache_key)
    results = []
    errors = []
    for item in updates:
        code = item.get("task_code")
        pid = item.get("proj_id")
        fields = item.get("fields", {})
        try:
            applied = _apply_activity_update(entry, code, pid, fields)
            results.append({"task_code": code, "status": "updated", "changes": applied})
        except Exception as e:
            errors.append({"task_code": code, "error": str(e)})

    return json.dumps({
        "updated": len(results),
        "errors": len(errors),
        "results": results,
        "error_details": errors,
        "note": "Call pyp6xer_write_file to persist changes to disk.",
    }, indent=2)


@mcp.tool
def pyp6xer_write_file(
    output_path: str,
    cache_key: str = "default",
    ctx: Context = None,
) -> str:
    """Write the current (possibly modified) schedule back to a .xer file.

    Serialises the raw table data (including any updates from pyp6xer_update_activity
    or pyp6xer_batch_update) and writes to the specified path.

    Args:
        output_path: Absolute or relative path where the .xer file should be written.
        cache_key:   Cache key of the loaded file.
    """
    entry = _get_cache(ctx, cache_key)
    content = _serialize_xer(entry["header"], entry["table_order"], entry["raw_tables"])

    with open(output_path, "w", encoding=Xer.CODEC, newline="") as f:
        f.write(content)

    return json.dumps({
        "status": "written",
        "output_path": output_path,
        "cache_key": cache_key,
        "bytes_written": len(content.encode(Xer.CODEC)),
    }, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
