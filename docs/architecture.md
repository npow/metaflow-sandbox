# Architecture

## Layer diagram

```
┌─────────────────────────────────────────┐
│  @sandbox / @daytona / @e2b decorators  │  ← Metaflow integration
│  sandbox_decorator.py                   │
└──────────────────┬──────────────────────┘
                   │ uses
┌──────────────────▼──────────────────────┐
│  CLI + Executor                         │  ← Remote execution
│  sandbox_cli.py, sandbox_executor.py    │
└──────────────────┬──────────────────────┘
                   │ uses
┌──────────────────▼──────────────────────┐
│  Backend registry                       │  ← Lazy resolution
│  backends/__init__.py                   │
└──────────────────┬──────────────────────┘
                   │ imports
┌──────────────────▼──────────────────────┐
│  Concrete backends                      │  ← Provider SDKs
│  backends/daytona.py, backends/e2b.py   │
└──────────────────┬──────────────────────┘
                   │ implements
┌──────────────────▼──────────────────────┐
│  SandboxBackend ABC + dataclasses       │  ← Core abstraction
│  backend.py                             │
└─────────────────────────────────────────┘
```

## Invariants

1. **Dependencies flow down only.** A lower layer must never import from a higher layer.
2. **SDK imports are lazy.** The `daytona` or `e2b` packages are imported inside methods, not at module level. This means `import metaflow_extensions.sandbox` never triggers an ImportError even if no backend SDK is installed.
3. **Error messages teach.** Every ImportError, ValueError, or configuration error includes the exact command to fix it and a link to docs.
4. **Backends are stateless across tasks.** A backend instance may cache sandbox references within a single task lifecycle, but must not assume state persists between Metaflow tasks.
5. **Remote datastore required.** Sandbox backends run on third-party infrastructure and cannot access the local filesystem. A remote datastore (S3, GCS, Azure Blob) is required — same as `@batch` and `@kubernetes`.

## Execution flow (compute backend)

The sandbox decorator operates as a full compute backend, following the same pattern as `@batch`:

```
Local runtime                                  Remote (sandbox container)
─────────────                                  ──────────────────────────

1. step_init()
   → validate remote datastore
   → resolve backend name

2. runtime_init()
   → store flow, graph, package, run_id

3. runtime_task_created()
   → upload code package to datastore
     (once per flow run, class-level)

4. runtime_step_cli()
   → cli_args.commands = ["sandbox", "step"]
   → append package metadata/sha/url
   → append all decorator options

5. Metaflow executes subprocess:
   python flow.py sandbox step train \
     <metadata> <sha> <url> \
     --cpu=2 --memory=4096 ...

6. sandbox_cli.py step() handler:
   → builds inner step command               7. Inside sandbox:
   → creates SandboxExecutor                     → download code package
   → executor.launch():                          → extract + set PYTHONPATH
     → backend.create(config)                    → bootstrap (conda/pypi)
     → backend.exec_script(full_cmd)     →       → run: python flow.py step train ...
   → executor.wait():                            → artifacts saved to datastore
     → stream logs to terminal                   → logs saved via mflog
     → destroy sandbox on completion

8. task_pre_step() [inside sandbox]:
   → emit metadata (sandbox-id, backend)

9. task_finished() [inside sandbox]:
   → sync local metadata to datastore
```

### Environment variables

The executor forwards a comprehensive set of environment variables into the sandbox:

- **Code package**: `METAFLOW_CODE_METADATA`, `METAFLOW_CODE_SHA`, `METAFLOW_CODE_URL`, `METAFLOW_CODE_DS`
- **Identity**: `METAFLOW_USER`, `METAFLOW_FLOW_FILENAME`
- **Datastore**: `METAFLOW_DEFAULT_DATASTORE`, `METAFLOW_DATASTORE_SYSROOT_*`
- **Metadata**: `METAFLOW_DEFAULT_METADATA`, `METAFLOW_SERVICE_URL`
- **Sandbox tracking**: `METAFLOW_SANDBOX_WORKLOAD=1`, `METAFLOW_SANDBOX_BACKEND`, `METAFLOW_SANDBOX_ID`
- **Cloud credentials**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_DEFAULT_REGION`, etc.

### Cloud credentials strategy

Since sandbox providers (Daytona, E2B) run on third-party infrastructure with no native IAM integration, we forward cloud credentials from the user's local environment. This is necessary for the sandbox to access the remote datastore (S3/GCS/Azure).

## Design decisions

### Why not one package per backend?

We considered `metaflow-daytona`, `metaflow-e2b` as separate packages. The shared abstraction layer (backend.py, registry, decorator) would need to live somewhere — either duplicated or in a shared base package. Extras on a single package are simpler: one install, one version, shared tests.

### Why lazy imports?

A user who installs `metaflow-sandbox[daytona]` should never see an ImportError about `e2b`. Lazy imports ensure only the chosen backend's SDK is loaded.

### Why aliases (@daytona) instead of just @sandbox(backend="...")?

Follows Metaflow convention: `@kubernetes`, `@batch` — not `@compute(backend="kubernetes")`. Users think in providers, not abstractions.

### Why a full compute backend instead of in-process execution?

The original decorator created a sandbox and ran commands in `task_pre_step()`. This doesn't work for real compute offload because:
1. The code package needs to be transferred to the sandbox
2. Metaflow's bootstrap sequence (conda, pypi) needs to run inside the sandbox
3. Artifacts must be saved to the remote datastore from inside the sandbox
4. Log capture (mflog) must work end-to-end

The compute backend pattern (CLI command → executor → sandbox) solves all of these by running the full `python flow.py step <name>` command inside the sandbox, just like `@batch` does inside an AWS Batch container.
