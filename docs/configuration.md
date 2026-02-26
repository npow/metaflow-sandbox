# Configuration

This page documents runtime settings for `metaflow-sandbox`.

## Decorator Parameters

Applies to `@sandbox`, `@daytona`, and `@e2b`.

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `backend` | `str` | `daytona` | `daytona` or `e2b`. |
| `cpu` | `int` | `1` | Requested vCPU count. |
| `memory` | `int` | `1024` | Memory in MB. |
| `gpu` | `str\|None` | `None` | Backend/provider specific. |
| `image` | `str\|None` | `None` | Backend default image when unset. |
| `timeout` | `int` | `600` | Step timeout in seconds. |
| `env` | `dict` | `{}` | Extra env vars injected into sandbox. |

## Required Environment Variables

| Variable | Required | Notes |
|---|---|---|
| `DAYTONA_API_KEY` or `E2B_API_KEY` | Yes | Choose the backend you use. |
| `METAFLOW_DEFAULT_DATASTORE` | Yes | Must be remote: `s3`, `gs`, or `azure`. |
| `METAFLOW_DATASTORE_SYSROOT_S3` / `METAFLOW_DATASTORE_SYSROOT_GS` / `METAFLOW_DATASTORE_SYSROOT_AZURE` | Yes | Match your datastore backend. |
| Cloud provider credentials | Yes | Credentials for chosen datastore. |

## S3 / R2

| Variable | Default | Notes |
|---|---|---|
| `METAFLOW_S3_ENDPOINT_URL` | unset | Required for R2/MinIO/custom S3 endpoints. |
| `AWS_ACCESS_KEY_ID` | unset | Required for key-based S3/R2 auth. |
| `AWS_SECRET_ACCESS_KEY` | unset | Required for key-based S3/R2 auth. |
| `AWS_DEFAULT_REGION` | provider default | `auto` is common for R2. |
| `METAFLOW_S3_WORKER_COUNT` | Metaflow default | Can reduce concurrency if endpoint throttles/stalls. |

## Public Sandbox Variables

| Variable | Default | Notes |
|---|---|---|
| `METAFLOW_SANDBOX_BACKEND` | `daytona` | Default backend when decorator doesn't specify one. |
| `METAFLOW_SANDBOX_TARGET_PLATFORM` | derived from local arch | Common values: `linux-64`, `linux-aarch64`. |
| `METAFLOW_SANDBOX_STAGE_MICROMAMBA` | enabled | Set `0` to disable staging. |
| `METAFLOW_SANDBOX_AUTO_DOWNLOAD_MICROMAMBA` | enabled | Auto-download compatible Linux `micromamba` when needed. |
| `METAFLOW_SANDBOX_MICROMAMBA_PATH` | unset | Explicit path to `micromamba`. |
| `METAFLOW_SANDBOX_MICROMAMBA_CACHE_DIR` | `~/.cache/metaflow-sandbox/micromamba` | Cache for auto-downloaded binary. |
| `METAFLOW_SANDBOX_R2_WORKER_COUNT` | `8` | Applied in sandbox for R2 unless `METAFLOW_S3_WORKER_COUNT` is set. |

## Advanced / Rarely Needed

These are supported but most users should not set them.

| Variable | Default | Notes |
|---|---|---|
| `METAFLOW_SANDBOX_UPLOADS` | unset | JSON list of extra files to stage. |
| `METAFLOW_SANDBOX_FORWARD_AWS_SESSION_TOKEN` | disabled on R2 | Enable only if endpoint requires session token. |
| `METAFLOW_SANDBOX_MAX_INFRA_RETRIES` | `1` | Retries sandbox recreation on backend infra failures. |

## Debugging

Use debug mode only when troubleshooting:

- `METAFLOW_SANDBOX_DEBUG`
  - `1` / `true`: keep sandbox and dump script/env to `/tmp/metaflow-sandbox-debug`
  - `<path>`: keep sandbox and dump script/env to the given path
  - `0` / `false`: disable debug mode

## Metadata Behavior

When using service metadata, sandbox tasks write local metadata first, sync through datastore, and the launcher replays payloads to the service endpoint.
