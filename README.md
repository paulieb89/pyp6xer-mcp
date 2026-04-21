# PyP6Xer MCP Server

[![SafeSkill 93/100](https://img.shields.io/badge/SafeSkill-93%2F100_Verified%20Safe-brightgreen)](https://safeskill.dev/scan/paulieb89-pyp6xer-mcp)
An MCP server wrapping [PyP6Xer](https://github.com/HassanEmam/PyP6Xer) (`xerparser`) for loading, analysing, and editing Oracle Primavera P6 `.xer` schedule files — directly from Claude or any MCP-compatible client.

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## Install

```bash
uv sync
```

## Connect to Claude Code

Add to your `.mcp.json`:

```json
{
  "mcpServers": {
    "pyp6xer": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "python", "/path/to/p6-mcp-2/server.py"]
    }
  }
}
```

## Usage

```
1. pyp6xer_load_file       — load a .xer from a local path, URL, or base64
2. <analysis tools>        — analyse the loaded schedule
3. pyp6xer_write_file      — persist any edits back to disk
```

Multiple files can be loaded simultaneously using different `cache_key` values.

## Tools (25)

| Category | Tools |
|---|---|
| File | `load_file`, `list_projects`, `clear_cache`, `get_upload_url` |
| Activities | `list_activities`, `get_activity`, `search_activities` |
| Analysis | `critical_path`, `float_analysis`, `schedule_quality`, `schedule_health_check`, `slipping_activities`, `relationship_analysis` |
| Progress / EVM | `progress_summary`, `earned_value` |
| Resources | `list_resources`, `resource_utilization` |
| Calendars | `list_calendars` |
| WBS | `wbs_analysis`, `work_package_summary` |
| Export | `export_csv`, `compare_snapshots` |
| Write | `update_activity`, `batch_update`, `write_file` |

All tool names are prefixed `pyp6xer_` to avoid conflicts when used alongside other MCP servers.

## Dependencies

- [fastmcp](https://github.com/jlowin/fastmcp) ≥ 3.0.0
- [xerparser](https://github.com/HassanEmam/PyP6Xer) ≥ 0.13.0
- [httpx](https://www.python-httpx.org/) ≥ 0.28.0
