# metaflow-sandbox

Pluggable sandbox backends for Metaflow. Run steps in fast, isolated containers via Daytona, E2B, or other providers.

## Architecture

Layers (dependencies flow downward only):

```
Decorators   — src/.../plugins/sandbox_decorator.py
    ↓
CLI + Exec   — src/.../plugins/sandbox_cli.py, sandbox_executor.py
    ↓
Registry     — src/.../plugins/backends/__init__.py
    ↓
Backends     — src/.../plugins/backends/{daytona,e2b}.py
    ↓
ABC          — src/.../plugins/backend.py
```

**Rule: no upward imports.** A backend must never import from the decorator layer. The registry must never import from the decorator. Structural tests enforce this.

## Key files

| What | Where |
|------|-------|
| Abstract interface | `src/metaflow_extensions/sandbox/plugins/backend.py` |
| Backend registry | `src/metaflow_extensions/sandbox/plugins/backends/__init__.py` |
| Daytona backend | `src/metaflow_extensions/sandbox/plugins/backends/daytona.py` |
| E2B backend | `src/metaflow_extensions/sandbox/plugins/backends/e2b.py` |
| Metaflow decorators | `src/metaflow_extensions/sandbox/plugins/sandbox_decorator.py` |
| CLI command handler | `src/metaflow_extensions/sandbox/plugins/sandbox_cli.py` |
| Sandbox executor | `src/metaflow_extensions/sandbox/plugins/sandbox_executor.py` |
| Plugin registration | `src/metaflow_extensions/sandbox/plugins/__init__.py` |

## Deeper docs

- [docs/architecture.md](docs/architecture.md) — layer boundaries, data flow, design decisions
- [docs/adding-a-backend.md](docs/adding-a-backend.md) — step-by-step guide to adding a new provider
- [docs/testing.md](docs/testing.md) — how to run tests, what structural tests enforce

## Commands

```bash
# Lint
ruff check src/ tests/

# Type check
mypy src/

# Unit tests (no credentials needed)
pytest tests/unit/

# Structural tests (enforce architecture)
pytest tests/structural/ -m structural

# Integration tests (need sandbox API keys)
DAYTONA_API_KEY=... pytest tests/ -m integration
```

## Conventions

- Lazy-import SDK dependencies. Never import `daytona` or `e2b` at module top level in backend files — import inside methods or `__init__`.
- Error messages must include remediation steps (install command, env var to set, doc link).
- Backend files must start with a docstring stating their Layer and allowed imports.
- All new backends must be registered in `backends/__init__.py` `_BACKENDS` dict.



## Claude Retro Suggestions
<!-- claude-retro-auto -->
- Before implementing features requiring data queries or API calls, always run a quick query/API call first to confirm the data structure matches what the UI/output will display, even if it seems obvious. Don't discover mid-implementation that the API returns different field names or shapes.
- When the user says 'DO EVERYTHING', 'just finish it', 'continue', or 'work autonomously', do not pause to ask clarifying questions unless truly blocked. Instead, proceed with best judgment and include a checkpoint sentence like 'I'm proceeding autonomously; I'll run [VERIFICATION STEP] and circle back if blocked.' Re-read the conversation for tone signals before asking.
- When investigating backend failures (data not appearing, API returning wrong shapes, queries failing), start by directly querying the data source (run a test query, check database rows, inspect API response) before investigating application code. A 2-minute data check often reveals the root cause faster than 20 turns of code review.
- After implementing any feature that involves LLM calls, database writes, or API integrations, run a complete end-to-end test with real data (not code inspection): verify rows appear in the actual database table, API responses contain expected fields, and the UI displays the data. Do not rely on code review or mock tests to confirm data flow.
- When setting up integrations with external tools (MCP servers, Jira APIs, credentials, GitHub actions), immediately ask the user for all required configuration details (cloud IDs, API keys, service URLs) before attempting any tool calls. A 2-minute credential clarification beats 60 turns of failed attempts.
<!-- claude-retro-auto -->
