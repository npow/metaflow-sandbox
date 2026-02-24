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
