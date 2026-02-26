"""Click CLI for sandbox execution: ``python flow.py sandbox step ...``

Layer: CLI (same level as Metaflow Integration)
May only import from: .sandbox_executor, metaflow stdlib

Follows the exact same pattern as ``metaflow.plugins.aws.batch.batch_cli``.
Metaflow discovers this via the ``CLIS_DESC`` entry in ``plugins/__init__.py``.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import traceback

from metaflow import util
from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.metadata_provider.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.unbounded_foreach import UBF_TASK

from .sandbox_executor import SandboxExecutor


def _replay_task_metadata_to_service(ctx, run_id, step_name, task_id):
    if ctx.obj.metadata.TYPE != "service":
        return

    from metaflow.plugins.metadata_providers.local import LocalMetadataProvider

    meta_dir = LocalMetadataProvider._get_metadir(
        ctx.obj.flow.name, run_id, step_name, task_id
    )
    if not meta_dir:
        return

    metadata_payload = []
    artifact_payload = []
    for path in glob.glob(os.path.join(meta_dir, "sysmeta_*.json")):
        with open(path) as f:
            metadata_payload.append(json.load(f))
    for path in glob.glob(os.path.join(meta_dir, "*_artifact_*.json")):
        with open(path) as f:
            artifact_payload.append(json.load(f))

    if not metadata_payload and not artifact_payload:
        return

    provider_cls = ctx.obj.metadata.__class__
    base_url = provider_cls._obj_path(ctx.obj.flow.name, run_id, step_name, task_id)

    # Ensure run/task exists on the service before replaying payloads.
    ctx.obj.metadata.register_task_id(run_id, step_name, task_id)
    if metadata_payload:
        provider_cls._request(
            ctx.obj.monitor,
            base_url + "/metadata",
            "POST",
            metadata_payload,
        )
    if artifact_payload:
        provider_cls._request(
            ctx.obj.monitor,
            base_url + "/artifact",
            "POST",
            artifact_payload,
        )


@click.group()
def cli():
    pass


@cli.group(help="Commands related to sandbox execution.")
def sandbox():
    pass


@sandbox.command(
    help="Execute a single task inside a sandbox. "
    "This command calls the top-level step command inside a sandbox "
    "with the given options. Typically you do not call this command "
    "directly; it is used internally by Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-metadata")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--backend", default="daytona", help="Sandbox backend to use.")
@click.option("--executable", help="Executable requirement for sandbox.")
@click.option("--image", help="Container image for the sandbox.")
@click.option("--cpu", default=1, type=int, help="CPU requirement.")
@click.option("--memory", default=1024, type=int, help="Memory in MB.")
@click.option("--gpu", default=None, help="GPU requirement.")
@click.option("--timeout", default=600, type=int, help="Timeout in seconds.")
@click.option(
    "--env-var",
    "env_vars",
    multiple=True,
    default=None,
    help="User env vars from @sandbox(env={}). Format: KEY=VALUE, multiple allowed.",
)
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--input-paths", help="Passed to the top-level 'step'.")
@click.option("--split-index", help="Passed to the top-level 'step'.")
@click.option("--clone-path", help="Passed to the top-level 'step'.")
@click.option("--clone-run-id", help="Passed to the top-level 'step'.")
@click.option(
    "--tag", multiple=True, default=None, help="Passed to the top-level 'step'."
)
@click.option("--namespace", default=None, help="Passed to the top-level 'step'.")
@click.option("--retry-count", default=0, help="Passed to the top-level 'step'.")
@click.option(
    "--max-user-code-retries", default=0, help="Passed to the top-level 'step'."
)
@click.option(
    "--ubf-context",
    default=None,
    type=click.Choice(["none", UBF_CONTROL, UBF_TASK]),
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_metadata,
    code_package_sha,
    code_package_url,
    backend="daytona",
    executable=None,
    image=None,
    cpu=1,
    memory=1024,
    gpu=None,
    timeout=600,
    env_vars=None,
    **kwargs,
):
    def echo(msg, stream="stderr", **kw):
        msg = util.to_unicode(msg)
        ctx.obj.echo_always(msg, err=(stream == "stderr"), **kw)

    # Build the inner step command (same pattern as batch_cli.py)
    executable = ctx.obj.environment.executable(step_name, executable)
    entrypoint = f"{executable} -u {os.path.basename(sys.argv[0])}"

    top_params = dict(ctx.parent.parent.params)
    # Remote sandboxes may not have egress to the metadata service.
    # Force local metadata in-sandbox and replay it from the launcher process.
    if ctx.obj.metadata.TYPE == "service":
        top_params["metadata"] = "local"
    top_args = " ".join(util.dict_to_cli_options(top_params))

    # Handle long input_paths by splitting into env vars
    input_paths = kwargs.get("input_paths")
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            f"METAFLOW_INPUT_PATHS_{i // max_size}": input_paths[i : i + max_size]
            for i in range(0, len(input_paths), max_size)
        }
        kwargs["input_paths"] = "".join(f"${{{s}}}" for s in split_vars)

    step_args = " ".join(util.dict_to_cli_options(kwargs))
    step_cli = f"{entrypoint} {top_args} step {step_name} {step_args}"

    node = ctx.obj.graph[step_name]
    retry_count = kwargs.get("retry_count", 0)

    task_spec = {
        "flow_name": ctx.obj.flow.name,
        "step_name": step_name,
        "run_id": kwargs["run_id"],
        "task_id": kwargs["task_id"],
        "retry_count": str(retry_count),
    }

    # Collect env vars from @environment decorator
    env = {"METAFLOW_FLOW_FILENAME": os.path.basename(sys.argv[0])}
    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env.update(env_deco[0].attributes["vars"])
    if split_vars:
        env.update(split_vars)
    # Add user env vars from @sandbox(env={"KEY": "VALUE"})
    if env_vars:
        for item in list(env_vars):
            key, _, value = item.partition("=")
            if key:
                env[key] = value

    if not env.get("DAYTONA_API_KEY"):
        env["DAYTONA_API_KEY"] = os.environ.get("METAFLOW_DAYTONA_API_KEY", "")
    if not env.get("E2B_API_KEY"):
        env["E2B_API_KEY"] = os.environ.get("METAFLOW_E2B_API_KEY", "")

    # Backend auth env vars are needed by sandbox backend SDK constructors
    # in this process (before sandbox env injection happens).
    for key in ("DAYTONA_API_KEY", "DAYTONA_API_URL", "E2B_API_KEY"):
        value = env.get(key)
        if value:
            os.environ[key] = value

    def _sync_metadata():
        if ctx.obj.metadata.TYPE in ("local", "service"):
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, kwargs["task_id"]
                ),
            )
            if ctx.obj.metadata.TYPE == "service":
                try:
                    _replay_task_metadata_to_service(
                        ctx, kwargs["run_id"], step_name, kwargs["task_id"]
                    )
                except Exception as e:
                    echo(f"Sandbox metadata replay to service failed: {util.to_unicode(e)}")

    def _on_log(line: str, _stream: str) -> None:
        # Always route to stderr (same stream as non-streaming wait() path).
        echo(line, stream="stderr")

    executor = SandboxExecutor(backend, ctx.obj.environment)
    try:
        executor.launch(
            step_name,
            step_cli,
            task_spec,
            code_package_metadata,
            code_package_sha,
            code_package_url,
            ctx.obj.flow_datastore.TYPE,
            image=image,
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            timeout=timeout,
            env=env,
            on_log=_on_log,
        )
    except Exception:
        traceback.print_exc()
        executor.cleanup()
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        executor.wait(echo=echo)
    except SystemExit:
        # wait() calls sys.exit(exit_code) on task failure —
        # propagate this so the runtime can handle retries.
        # Don't call _sync_metadata here — finally handles it.
        raise
    except Exception:
        # Infra-level failure (e.g. SDK crash) — don't retry.
        traceback.print_exc()
        executor.cleanup()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        _sync_metadata()
