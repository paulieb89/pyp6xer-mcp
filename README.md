# PyP6Xer MCP Server

<!-- mcp-name: io.github.paulieb89/pyp6xer-mcp -->

[![PyPI](https://img.shields.io/pypi/v/pyp6xer-mcp)](https://pypi.org/project/pyp6xer-mcp/)
[![SafeSkill](https://safeskill.dev/api/badge/paulieb89-pyp6xer-mcp)](https://safeskill.dev/scan/paulieb89-pyp6xer-mcp)
[![Glama](https://img.shields.io/badge/Glama-listed-orange?style=flat-square)](https://glama.ai/mcp/servers/paulieb89/pyp6xer-mcp)
[![smithery badge](https://smithery.ai/badge/bouch/pyp6xer-mcp)](https://smithery.ai/servers/bouch/pyp6xer-mcp)
[![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_Server-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://vscode.dev/redirect/mcp/install?name=pyp6xer&config=%7B%22type%22%3A%22http%22%2C%22url%22%3A%22https%3A%2F%2Fpyp6xer-mcp.fly.dev%2Fmcp%22%7D)
[![Install in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-Install_Server-24bfa5?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=pyp6xer&config=%7B%22type%22%3A%22http%22%2C%22url%22%3A%22https%3A%2F%2Fpyp6xer-mcp.fly.dev%2Fmcp%22%7D&quality=insiders)
[![Install in Cursor](https://img.shields.io/badge/Cursor-Install_Server-000000?style=flat-square&logoColor=white)](https://cursor.com/en/install-mcp?name=pyp6xer&config=eyJ0eXBlIjoiaHR0cCIsInVybCI6Imh0dHBzOi8vcHlwNnhlci1tY3AuZmx5LmRldi9tY3AifQ==)
[![Install in VS Code (local)](https://img.shields.io/badge/VS_Code-Install_Local-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://vscode.dev/redirect/mcp/install?name=pyp6xer&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22pyp6xer-mcp%22%5D%7D)

AI-agent tools for Primavera P6 XER schedules. Load, analyse, compare, edit, and export `.xer` files from Claude, ChatGPT, Cursor, or any MCP-compatible client.

PyP6Xer MCP is workflow-oriented, not just a parser. Unlike basic XER readers, it gives AI agents the full project-controls loop: schedule health checks, critical path, delay comparison, progress updates, relationship edits, and write-back to XER.

> Prefer a web interface? [p6.bouch.dev](https://p6.bouch.dev) — upload an XER and analyse it without configuring MCP.

## Use cases

- Ask questions about a Primavera P6 XER schedule
- Run critical path and float analysis
- Compare baseline vs update XER files and identify slipping activities
- Run schedule quality and health checks
- Edit activity fields safely and batch-update progress
- Export modified XER files
- Analyse relationship changes between two XER snapshots
- Build AI workflows for delay analysis and project controls

## Example prompts

- *Load this XER and show the critical path.*
- *Compare baseline.xer and update-03.xer and list major slippages.*
- *Find activities with high float, missing logic, or long durations.*
- *Update activity A1020 to 60% complete and export the edited XER.*
- *Analyse relationship changes between two XER snapshots.*

## Connect

### Hosted (no install)

```json
{
  "mcpServers": {
    "pyp6xer": {
      "type": "http",
      "url": "https://pyp6xer-mcp.fly.dev/mcp"
    }
  }
}
```

### Local (uvx — no clone needed)

```json
{
  "mcpServers": {
    "pyp6xer": {
      "type": "stdio",
      "command": "uvx",
      "args": ["pyp6xer-mcp"]
    }
  }
}
```

### Local (from source)

Clone the repo, then point your MCP client at it:

```bash
git clone https://github.com/paulieb89/pyp6xer-mcp.git
cd pyp6xer-mcp
uv sync
```

```json
{
  "mcpServers": {
    "pyp6xer": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "server.py"],
      "cwd": "/path/to/pyp6xer-mcp"
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

## Tools (29)

| Category | Tools |
|---|---|
| File | `load_file`, `list_projects`, `clear_cache`, `get_upload_url` |
| Activities | `list_activities`, `get_activity`, `search_activities`, `get_activity_schema` |
| Analysis | `critical_path`, `float_analysis`, `schedule_quality`, `schedule_health_check`, `slipping_activities`, `relationship_analysis`, `lookahead` |
| Progress / EVM | `progress_summary`, `earned_value` |
| Resources | `list_resources`, `resource_utilization` |
| Calendars | `list_calendars` |
| WBS | `wbs_analysis`, `work_package_summary` |
| Export | `export_csv`, `export_xer`, `compare_snapshots`, `generate_report` |
| Write | `update_activity`, `batch_update`, `write_file` |

All tool names are prefixed `pyp6xer_` to avoid conflicts when used alongside other MCP servers.

## Dependencies

- [fastmcp](https://github.com/jlowin/fastmcp) 3.2.4
- [xerparser](https://github.com/HassanEmam/PyP6Xer) ≥ 0.13.0
- [httpx](https://www.python-httpx.org/) ≥ 0.28.0

## Licence

MIT
