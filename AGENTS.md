# AGENTS.md — pyp6xer-mcp

AI agent instructions for working in this repo. See `/home/bch/dev/ops/OPS.md` for credentials, fleet overview, and release tooling.

## Repo shape

Single `server.py`. Wraps xerparser for Primavera P6 XER schedule analysis.
GitHub repo: paulieb89/pyp6xer-mcp (previously also on GitLab — GitLab is legacy, GitHub is canonical).

## Deploy

```bash
fly deploy --ha=false
```

Single instance, lhr region. App name: `pyp6xer-mcp`. Fly.io account: articat1066@gmail.com.

IMPORTANT: `fly.toml` must have `internal_port = 8080` and `PORT = "8080"`. The server reads `PORT`, not `FASTMCP_PORT`.

## Version bump

1. Update `version` in `pyproject.toml`
2. Update version string in the `smithery_server_card` route in `server.py`
3. Commit, tag `vX.Y.Z`, push + push tags
4. GitHub Actions publishes to PyPI automatically on tag
5. `fly deploy --ha=false`
6. Cut a new Glama release

## Standard routes (must always be present)

- `/.well-known/mcp/server-card.json` — Smithery metadata
- `/.well-known/glama.json` — Glama maintainer claim
- `/health` — Fly health check

Verify after deploy:
```bash
curl https://pyp6xer-mcp.fly.dev/.well-known/mcp/server-card.json
curl https://pyp6xer-mcp.fly.dev/.well-known/glama.json
curl https://pyp6xer-mcp.fly.dev/health
```

## README badge order

```
PyPI → SafeSkill → Glama card → Smithery
```

## Do not

- Do not use `FASTMCP_PORT` — the server reads `PORT` env var only
- Do not use the GitLab repo — it is legacy
- Do not commit API keys — all secrets are in Fly secrets (`fly secrets list`)
