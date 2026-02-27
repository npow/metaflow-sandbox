# metaflow-sandbox

[![CI](https://github.com/npow/metaflow-sandbox/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-sandbox/actions/workflows/ci.yml)
[![Publish](https://github.com/npow/metaflow-sandbox/actions/workflows/publish.yml/badge.svg)](https://github.com/npow/metaflow-sandbox/actions/workflows/publish.yml)
[![PyPI version](https://img.shields.io/pypi/v/metaflow-sandbox.svg)](https://pypi.org/project/metaflow-sandbox/)
[![Python](https://img.shields.io/pypi/pyversions/metaflow-sandbox.svg)](https://pypi.org/project/metaflow-sandbox/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Run selected Metaflow steps in isolated sandboxes — locally or in the cloud — without changing how the rest of your flow works.

Decorate a step, pick a backend, and the step runs in a clean VM or container. Artifacts, retries, and `self.*` state pass through unchanged.

```python
from metaflow import FlowSpec, step, boxlite, daytona, e2b

class MyFlow(FlowSpec):
    @step
    def start(self):
        self.data = load_data()
        self.next(self.process)

    @boxlite          # local VM — no API key needed
    @step
    def process(self):
        import heavy_lib   # safe: isolated from your machine
        self.result = heavy_lib.run(self.data)
        self.next(self.end)

    @step
    def end(self):
        print(self.result)
```

## When to Reach for This

**Run tool-heavy or untrusted code without touching your machine.**
Steps that install system packages, call LLM APIs, or execute agent actions run in a throwaway VM. Your laptop stays clean.

**Fan out evals or agent tasks at scale.**
`@foreach` spins up a sandbox per branch. Daytona and E2B cold-start in under 150ms, so hundreds of parallel tasks are cheap.

**Iterate locally with real isolation.**
Use `@boxlite` (KVM/HVF microVM, no cloud account) to get the same sandboxed environment on your laptop before pushing to production. Flip to `@daytona` or `@e2b` by changing one word.

**Keep your existing `@conda` and `@pypi` decorators.**
Dependencies resolve normally — `metaflow-sandbox` installs them inside the sandbox before the step runs.

## Quick Start

### Local sandbox — no API key (boxlite)

Requires KVM (Linux) or Apple Hypervisor Framework (macOS, M-series).

```bash
pip install metaflow-sandbox[boxlite] metaflow-local-service
metaflow-local-service run python my_flow.py run
```

```python
from metaflow import FlowSpec, step, boxlite

class Demo(FlowSpec):
    @step
    def start(self):
        self.msg = "hello"
        self.next(self.remote)

    @boxlite
    @step
    def remote(self):
        self.msg += " from a local VM"
        self.next(self.end)

    @step
    def end(self):
        print(self.msg)

if __name__ == "__main__":
    Demo()
```

### Cloud sandbox — Daytona (<100ms cold start)

```bash
pip install metaflow-sandbox[daytona] metaflow-local-service

export DAYTONA_API_KEY=...
export METAFLOW_DEFAULT_DATASTORE=s3
export METAFLOW_DATASTORE_SYSROOT_S3=s3://your-bucket/metaflow
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

metaflow-local-service run python my_flow.py run
```

```python
from metaflow import FlowSpec, step, daytona

class Demo(FlowSpec):
    @step
    def start(self):
        self.msg = "hello"
        self.next(self.remote)

    @daytona(cpu=1, memory=2048)
    @step
    def remote(self):
        self.msg += " from Daytona"
        self.next(self.end)

    @step
    def end(self):
        print(self.msg)

if __name__ == "__main__":
    Demo()
```

### Cloud sandbox — E2B (Firecracker microVM)

```bash
pip install metaflow-sandbox[e2b] metaflow-local-service
export E2B_API_KEY=...
```

Replace `@daytona` with `@e2b`. Everything else is identical.

## Dependencies in the Sandbox

Your `@conda` and `@pypi` decorators work as-is. `metaflow-sandbox` installs them inside the sandbox before your step code runs:

```python
@daytona
@pypi(packages={"pydash": "==8.0.5"})
@step
def pypi_step(self):
    import pydash
    self.x = pydash.camel_case("hello_world")
    self.next(self.conda_step)

@daytona
@conda(libraries={"numpy": "1.26.4"}, python="3.12.12")
@step
def conda_step(self):
    import numpy as np
    print(self.x, int(np.array([1, 2, 3]).sum()))
```

## Backends

| Decorator | Install | Requires | Cold start |
|-----------|---------|----------|------------|
| `@boxlite` | `metaflow-sandbox[boxlite]` | KVM or HVF (local) | ~1–2s |
| `@daytona` | `metaflow-sandbox[daytona]` | `DAYTONA_API_KEY` + S3 | <100ms |
| `@e2b` | `metaflow-sandbox[e2b]` | `E2B_API_KEY` + S3 | ~150ms |
| `@sandbox` | any of the above | depends on backend | — |

`@sandbox(backend="daytona")` is equivalent to `@daytona`. Use it to set the backend at runtime via `METAFLOW_SANDBOX_BACKEND`.

## Configuration

For decorator parameters, environment variables, and advanced toggles, see [docs/configuration.md](docs/configuration.md).

**Cloudflare R2 / S3-compatible storage:**

```bash
export METAFLOW_S3_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=auto
```

## Troubleshooting

**Auth error from backend API**
Set the right key (`DAYTONA_API_KEY`, `E2B_API_KEY`) in the shell that runs the flow.

**`@sandbox` says remote datastore is required**
Set `METAFLOW_DEFAULT_DATASTORE` and its remote root. `@boxlite` has the same requirement — steps need somewhere to write artifacts.

**Datastore access errors (`403`, missing objects)**
Check cloud credentials and `METAFLOW_S3_ENDPOINT_URL` if using R2 or a custom S3 endpoint.

**boxlite: "KVM not available" or "HVF not available"**
On Linux, ensure `/dev/kvm` exists and your user has permission. On macOS, requires Apple Silicon or Intel Mac with Hypervisor.framework (macOS 11+).

## How It Works

`metaflow-sandbox` intercepts the Metaflow step CLI and re-runs the step inside a sandbox. The sandbox gets the code package, runs the step, and writes artifacts to the remote datastore — the same way `@batch` or `@kubernetes` work, but with pluggable VM backends.

**Metadata relay (cloud backends):** Sandboxes can't reach `localhost`, so metadata written during a step is relayed through S3 and replayed to `metaflow-local-service` on your machine. `metaflow-sandbox` handles this automatically.

```
Sandbox                           Your machine
───────────────                   ─────────────────────────────────
step runs                         metaflow-local-service
writes .metaflow/ locally
        │
        ▼
sync to S3  ──────────────────►  pull from S3 → replay to service
```

Architecture details: [docs/architecture.md](docs/architecture.md)
Adding a backend: [docs/adding-a-backend.md](docs/adding-a-backend.md)

## Development

```bash
ruff check src/ tests/
pytest tests/unit/ tests/structural/
pytest tests/integration/ -m integration   # needs sandbox API keys
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
