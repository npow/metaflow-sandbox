# metaflow-sandbox

Spin up isolated containers for your Metaflow steps in milliseconds.

Each step gets its own sandbox — a fresh container that starts in <100ms, runs your code, and disappears. Spin up dozens in parallel without waiting for pods to schedule or containers to pull. Think `@batch` but with instant cold starts.

Pluggable backends — currently [Daytona](https://daytona.io) and [E2B](https://e2b.dev), with a simple interface to add your own.

```python
from metaflow import FlowSpec, step

class TrainingFlow(FlowSpec):

    @sandbox(cpu=4, memory=8192)
    @step
    def train(self):
        import torch
        self.model = train_model()
        self.next(self.end)

    @step
    def end(self):
        print(f"Model accuracy: {self.model.accuracy}")

if __name__ == "__main__":
    TrainingFlow()
```

`train` runs in a cloud sandbox with 4 CPUs and 8GB RAM. `end` runs locally. Artifacts, metadata, retries, and `@catch` all work exactly like `@batch`.

## Quick start

```bash
pip install metaflow-sandbox[daytona]
export DAYTONA_API_KEY=your-key          # from daytona.io
export METAFLOW_DEFAULT_DATASTORE=s3     # remote datastore required
export METAFLOW_DATASTORE_SYSROOT_S3=s3://your-bucket/metaflow
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

python my_flow.py run
```

Works with S3, GCS, Azure Blob, or any S3-compatible store (Cloudflare R2, MinIO).

## Backends

| Backend | Install | Cold start | Provider |
|---------|---------|-----------|----------|
| [Daytona](https://daytona.io) | `pip install metaflow-sandbox[daytona]` | <100ms | Daytona Cloud |
| [E2B](https://e2b.dev) | `pip install metaflow-sandbox[e2b]` | ~150ms | E2B Firecracker microVMs |

Use the backend-specific decorator or the generic one:

```python
@daytona(cpu=2)           # Daytona
@e2b                      # E2B
@sandbox(backend="daytona", cpu=4, memory=8192)  # generic
```

## How it works

Same pattern as `@batch` and `@kubernetes` — your code package is uploaded to your datastore, a sandbox is created, the step runs inside it, artifacts are saved back. You get logs, metadata, retries, and `@catch` — everything works the same.

```
Your laptop                              Sandbox
──────────                               ───────
1. Upload code package to S3
2. Create sandbox (<100ms)        →      3. Download code package
                                         4. pip install dependencies
                                         5. Run: python flow.py step train
                                         6. Save artifacts to S3
7. Stream logs ←
8. Destroy sandbox
```

Cloud credentials are forwarded automatically from your local environment (AWS, GCP, Azure).

## Requirements

- **Remote datastore**: S3, GCS, Azure Blob, or S3-compatible (R2, MinIO). Local datastore won't work — the sandbox can't access your filesystem.
- **Backend API key**: `DAYTONA_API_KEY` or `E2B_API_KEY`
- **Cloud credentials**: For the datastore (e.g. `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`)

## Adding a new backend

The backend interface is 5 methods: `create`, `exec`, `upload`, `download`, `destroy`. See [docs/adding-a-backend.md](docs/adding-a-backend.md).

## Development

```bash
ruff check src/ tests/                          # lint
pytest tests/unit/ tests/structural/            # tests (no credentials needed)
pytest tests/integration/ -m integration        # e2e (needs API keys + remote datastore)
```

See [docs/architecture.md](docs/architecture.md) for internals.
