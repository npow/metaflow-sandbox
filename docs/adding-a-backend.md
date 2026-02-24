# Adding a new sandbox backend

## 1. Create the backend file

Create `src/metaflow_extensions/sandbox/plugins/backends/yourprovider.py`:

```python
"""YourProvider sandbox backend.

Layer: Concrete Backend
May only import from: ..backend (ABC + dataclasses), yourprovider SDK

Install: pip install metaflow-sandbox[yourprovider]
Docs:    https://yourprovider.dev/docs
"""

from metaflow_extensions.sandbox.plugins.backend import ExecResult
from metaflow_extensions.sandbox.plugins.backend import SandboxBackend
from metaflow_extensions.sandbox.plugins.backend import SandboxConfig

_INSTALL_HINT = (
    "YourProvider SDK not found. Install it with:\n"
    "\n"
    "    pip install metaflow-sandbox[yourprovider]\n"
    "\n"
    "Then set YOURPROVIDER_API_KEY in your environment.\n"
    "See https://yourprovider.dev/docs for details."
)


class YourProviderBackend(SandboxBackend):
    def __init__(self):
        try:
            from yourprovider import Client
        except ImportError:
            raise ImportError(_INSTALL_HINT) from None
        self._client = Client()

    def create(self, config: SandboxConfig) -> str: ...
    def exec(self, sandbox_id, cmd, cwd="/", timeout=300) -> ExecResult: ...
    def upload(self, sandbox_id, local_path, remote_path) -> None: ...
    def download(self, sandbox_id, remote_path, local_path) -> None: ...
    def destroy(self, sandbox_id) -> None: ...
```

## 2. Register it

In `backends/__init__.py`, add to `_BACKENDS`:

```python
_BACKENDS = {
    "daytona": (".daytona", "DaytonaBackend"),
    "e2b": (".e2b", "E2BBackend"),
    "yourprovider": (".yourprovider", "YourProviderBackend"),  # ← add
}
```

## 3. Add the extra dependency

In `pyproject.toml`:

```toml
[project.optional-dependencies]
yourprovider = ["yourprovider-sdk>=1.0"]
```

## 4. (Optional) Add a decorator alias

In `sandbox_decorator.py`:

```python
class YourProviderDecorator(SandboxDecorator):
    name = "yourprovider"
    defaults = {**SandboxDecorator.defaults, "backend": "yourprovider"}
```

Register it in `plugins/__init__.py`:

```python
STEP_DECORATORS_DESC = [
    ...
    ("yourprovider", ".sandbox_decorator.YourProviderDecorator"),
]
```

## 5. Add tests

- `tests/unit/backends/test_yourprovider.py` — mock the SDK, test the mapping logic
- The structural tests will automatically verify your backend implements all ABC methods

## Checklist

- [ ] Backend file has Layer docstring
- [ ] SDK import is lazy (inside `__init__`, not module level)
- [ ] `_INSTALL_HINT` includes: install command, env var, doc link
- [ ] Registered in `_BACKENDS`
- [ ] Extra added to `pyproject.toml`
- [ ] Unit tests pass
- [ ] Structural tests pass (`pytest tests/structural/ -m structural`)
