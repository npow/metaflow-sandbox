# Testing

## Test categories

### Unit tests (`tests/unit/`)
Mock external SDKs. No credentials needed. Run fast.

```bash
pytest tests/unit/
```

### Structural tests (`tests/structural/`)
Enforce architectural invariants mechanically. No credentials needed.

```bash
pytest tests/structural/ -m structural
```

What they check:
- Every class in `backends/` subclasses `SandboxBackend`
- Every abstract method from `SandboxBackend` is implemented
- No backend file imports from the decorator layer (layer violation)
- Every backend in `_BACKENDS` registry can be imported
- Error messages in backends contain install instructions

### Integration tests (`tests/` with `-m integration`)
Actually create sandboxes, run commands, transfer files. Requires API keys.

```bash
DAYTONA_API_KEY=xxx pytest tests/ -m integration
E2B_API_KEY=xxx pytest tests/ -m integration
```

## Adding tests for a new backend

1. Add unit tests in `tests/unit/backends/test_<name>.py`
2. Structural tests auto-discover backends from `_BACKENDS` registry — no action needed
3. Add integration tests with `@pytest.mark.integration` marker
