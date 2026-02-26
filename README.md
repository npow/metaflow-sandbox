# metaflow-sandbox

[![CI](https://github.com/npow/metaflow-sandbox/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-sandbox/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/metaflow-sandbox.svg)](https://pypi.org/project/metaflow-sandbox/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Run selected Metaflow steps in fast remote sandboxes ([Daytona](https://www.daytona.io) or [E2B](https://e2b.dev)) while keeping normal Metaflow behavior for artifacts, retries, and flow state.

## Why Use It

- ⚡ Speed + scale: launch sandboxes in milliseconds (`<100ms`) and fan out to thousands of containers.
- 🔒 Isolation: run tool-heavy or untrusted code without polluting the launcher machine.
- 📦 Dependency management: keep runtime dependencies consistent across local runs, CI, and remote execution.
- 🧪 Throughput for evals: run many short-lived agent tasks in parallel for benchmark and regression loops.
- 🔁 Continuity: keep normal step-to-step state and result passing.

## Quick Start (Daytona) 🚀

```bash
pip install metaflow-sandbox[daytona]

export DAYTONA_API_KEY=...
export METAFLOW_DEFAULT_DATASTORE=s3
export METAFLOW_DATASTORE_SYSROOT_S3=s3://your-bucket/metaflow
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

python my_flow.py run
```

Minimal example:

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
        self.msg = self.msg + " from sandbox"
        self.next(self.end)

    @step
    def end(self):
        print(self.msg)

if __name__ == "__main__":
    Demo()
```

## R2 / S3-Compatible Setup ☁️

For Cloudflare R2, set:

```bash
export METAFLOW_S3_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
# optional for R2:
export AWS_DEFAULT_REGION=auto
```

## Dependency Hydration in Sandbox 📦

Use your normal Metaflow decorators:

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

## Backends 🔌

- Daytona: `pip install metaflow-sandbox[daytona]`, use `@daytona`
- E2B: `pip install metaflow-sandbox[e2b]`, use `@e2b`
- Generic: `@sandbox(backend="daytona", cpu=2, memory=4096)`

## Configuration 🧭

For the full list of decorator parameters, environment variables, defaults, and advanced toggles, see [docs/configuration.md](docs/configuration.md).

## Troubleshooting 🛠️

- Symptom: auth error from backend API
- Fix: set the right key (`DAYTONA_API_KEY` or `E2B_API_KEY`) in the shell that runs the flow.

- Symptom: `@sandbox`/`@daytona` says remote datastore is required
- Fix: set `METAFLOW_DEFAULT_DATASTORE` and its remote datastore root.

- Symptom: datastore access errors (`403`, missing objects, endpoint errors)
- Fix: verify cloud credentials and endpoint config (`METAFLOW_S3_ENDPOINT_URL` for R2/custom S3).

## Development 🧪

```bash
ruff check src/ tests/
pytest tests/unit/ tests/structural/
pytest tests/integration/ -m integration
```

Architecture details: [docs/architecture.md](docs/architecture.md)  
Backend interface: [docs/adding-a-backend.md](docs/adding-a-backend.md)

## License

Apache License 2.0. See [LICENSE](LICENSE).
