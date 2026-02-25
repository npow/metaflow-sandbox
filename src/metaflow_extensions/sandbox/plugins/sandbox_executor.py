"""Sandbox executor — creates a sandbox, uploads code, runs a step.

Layer: Execution (same level as Metaflow Integration)
May only import from: .backend, .backends (registry), metaflow stdlib

This is the sandbox equivalent of ``metaflow.plugins.aws.batch.batch.Batch``.
It builds the full bash command (mflog setup, code download, bootstrap,
step execution, log save) and runs it inside a sandbox via the backend API.
"""

from __future__ import annotations

import contextlib
import os
import shlex
import sys
from typing import Any

from metaflow import util
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DEFAULT_METADATA
from metaflow.metaflow_config import SERVICE_INTERNAL_URL
from metaflow.mflog import BASH_SAVE_LOGS
from metaflow.mflog import bash_capture_logs
from metaflow.mflog import export_mflog_env_vars

from metaflow_extensions.sandbox.plugins.backend import ExecResult
from metaflow_extensions.sandbox.plugins.backend import Resources
from metaflow_extensions.sandbox.plugins.backend import SandboxConfig
from metaflow_extensions.sandbox.plugins.backends import get_backend

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)

# Cloud credential env vars to forward into the sandbox.
# The sandbox runs on third-party infra with no native IAM integration,
# so we forward credentials from the local environment.
_FORWARDED_CREDENTIAL_VARS = [
    # AWS
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_DEFAULT_REGION",
    # GCS
    "GOOGLE_APPLICATION_CREDENTIALS",
    "CLOUDSDK_CONFIG",
    # Azure
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_KEY",
]


class SandboxException(MetaflowException):
    headline = "Sandbox execution error"


class SandboxExecutor:
    """Create a sandbox, ship code, execute a Metaflow step inside it.

    Mirrors the ``Batch`` class from
    ``metaflow.plugins.aws.batch.batch``. The key difference is that we
    use the pluggable :class:`SandboxBackend` interface instead of AWS
    Batch APIs.
    """

    def __init__(self, backend_name: str, environment: Any) -> None:
        self._backend_name = backend_name
        self._environment = environment
        self._sandbox_id: str | None = None
        self._result: ExecResult | None = None
        self._backend: Any = None

    # ------------------------------------------------------------------
    # Command building (mirrors Batch._command)
    # ------------------------------------------------------------------

    def _command(
        self,
        code_package_metadata: str,
        code_package_url: str,
        step_name: str,
        step_cmds: list[str],
        task_spec: dict[str, str],
        datastore_type: str,
    ) -> str:
        """Build the full bash command that runs inside the sandbox.

        Structure:
        1. Set mflog environment variables
        2. Download and extract the code package
        3. Bootstrap the environment (conda/pypi)
        4. Execute the step with log capture
        5. Save logs and propagate exit code
        """
        mflog_expr = export_mflog_env_vars(
            datastore_type=datastore_type,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
            **task_spec,
        )
        init_cmds = self._environment.get_package_commands(
            code_package_url, datastore_type, code_package_metadata
        )
        # Avoid creating/running under a directory literally named "metaflow",
        # which can shadow the installed metaflow package in Python import path.
        init_cmds = [
            cmd.replace("mkdir metaflow && cd metaflow", "mkdir mf_sandbox && cd mf_sandbox")
            for cmd in init_cmds
        ]
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                self._environment.bootstrap_commands(step_name, datastore_type)
                + step_cmds
            )
        )

        cmd_str = f"true && mkdir -p {LOGS_DIR} && {mflog_expr} && {init_expr} && {step_expr}; "
        cmd_str += f"c=$?; {BASH_SAVE_LOGS}; exit $c"

        # Metaflow's get_package_commands() uses \\" escaping designed for
        # the shlex round-trip in batch.py: shlex.split('bash -c "%s"' % cmd).
        # Apply the same transformation so escaped quotes resolve correctly.
        cmd_str = shlex.split(f'bash -c "{cmd_str}"')[-1]

        return cmd_str

    # ------------------------------------------------------------------
    # Environment variable assembly
    # ------------------------------------------------------------------

    @staticmethod
    def _build_env(
        code_package_metadata: str,
        code_package_sha: str,
        code_package_url: str,
        datastore_type: str,
        backend_name: str,
        sandbox_env: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Assemble the environment variables for the sandbox."""
        env: dict[str, str] = {
            "METAFLOW_CODE_METADATA": code_package_metadata,
            "METAFLOW_CODE_SHA": code_package_sha,
            "METAFLOW_CODE_URL": code_package_url,
            "METAFLOW_CODE_DS": datastore_type,
            "METAFLOW_USER": util.get_username(),
            "METAFLOW_DEFAULT_DATASTORE": datastore_type,
            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
            "METAFLOW_SANDBOX_WORKLOAD": "1",
            "METAFLOW_SANDBOX_BACKEND": backend_name,
        }

        if SERVICE_INTERNAL_URL:
            env["METAFLOW_SERVICE_URL"] = SERVICE_INTERNAL_URL

        # Forward datastore-specific configuration from the local env
        # (e.g. METAFLOW_DATASTORE_SYSROOT_S3, METAFLOW_DATATOOLS_S3ROOT).
        from metaflow.metaflow_config_funcs import config_values

        for k, v in config_values():
            if k.startswith("METAFLOW_DATASTORE_SYSROOT_") or k.startswith(
                "METAFLOW_DATATOOLS_"
            ) or k.startswith("METAFLOW_S3") or k.startswith("METAFLOW_CARD_S3"):
                env[k] = v

        # Forward cloud credentials
        for var in _FORWARDED_CREDENTIAL_VARS:
            val = os.environ.get(var)
            if val:
                env[var] = val

        # User-specified env vars from the decorator
        if sandbox_env:
            env.update(sandbox_env)

        return env

    # ------------------------------------------------------------------
    # Launch + wait
    # ------------------------------------------------------------------

    def launch(
        self,
        step_name: str,
        step_cli: str,
        task_spec: dict[str, str],
        code_package_metadata: str,
        code_package_sha: str,
        code_package_url: str,
        datastore_type: str,
        image: str | None = None,
        cpu: int = 1,
        memory: int = 1024,
        gpu: str | None = None,
        timeout: int = 600,
        env: dict[str, str] | None = None,
    ) -> None:
        """Create a sandbox and run the step command inside it."""
        self._backend = get_backend(self._backend_name)

        cmd_str = self._command(
            code_package_metadata,
            code_package_url,
            step_name,
            [step_cli],
            task_spec,
            datastore_type,
        )

        sandbox_env = self._build_env(
            code_package_metadata,
            code_package_sha,
            code_package_url,
            datastore_type,
            self._backend_name,
            sandbox_env=env,
        )

        config = SandboxConfig(
            image=image or "python:3.11-slim",
            env=sandbox_env,
            resources=Resources(cpu=cpu, memory_mb=memory, gpu=gpu),
            timeout=timeout,
        )

        self._sandbox_id = self._backend.create(config)

        # Preflight inside sandbox: ensure Metaflow runtime deps are installed
        # and patch S3 handling for R2 responses that omit ContentType/Metadata.
        preflight = """python - <<'PY'
from pathlib import Path
import subprocess
import sys

subprocess.check_call(
    [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-qqq",
        "--no-compile",
        "--no-cache-dir",
        "--disable-pip-version-check",
        "metaflow",
        "boto3",
        "requests",
    ]
)

import metaflow

root = Path(metaflow.__file__).resolve().parent
files = (
    root / "plugins" / "datatools" / "s3" / "s3.py",
    root / "plugins" / "datatools" / "s3" / "s3op.py",
)
replacements = (
    ('resp["ContentType"]', 'resp.get("ContentType")'),
    ('resp["Metadata"]', 'resp.get("Metadata", {})'),
    ('head["ContentType"]', 'head.get("ContentType")'),
    ('head["Metadata"]', 'head.get("Metadata", {})'),
)
for file_path in files:
    if not file_path.exists():
        continue
    text = file_path.read_text()
    for old, new in replacements:
        text = text.replace(old, new)
    file_path.write_text(text)

PY"""
        preflight_result = self._backend.exec_script(
            self._sandbox_id,
            preflight,
            timeout=min(timeout, 180),
        )
        if preflight_result.exit_code != 0:
            raise SandboxException(
                "Sandbox preflight failed.\n"
                f"stdout: {preflight_result.stdout}\n"
                f"stderr: {preflight_result.stderr}"
            )

        # Inject sandbox ID into the script — we can't know the ID
        # before create(), so we prepend an export to the script.
        cmd_str = (
            f"export METAFLOW_SANDBOX_ID={shlex.quote(self._sandbox_id)} && {cmd_str}"
        )

        self._result = self._backend.exec_script(
            self._sandbox_id,
            cmd_str,
            timeout=timeout,
        )

    def cleanup(self) -> None:
        """Destroy the sandbox if it exists. Best-effort, never raises."""
        if os.environ.get("METAFLOW_SANDBOX_DEBUG_KEEP") == "1":
            return
        if self._sandbox_id and self._backend:
            with contextlib.suppress(Exception):
                self._backend.destroy(self._sandbox_id)
            self._sandbox_id = None

    def wait(self, echo: Any) -> None:
        """Stream output, clean up, and return.

        Does NOT raise on non-zero exit codes — the Metaflow runtime
        inspects the subprocess exit code itself and handles retry
        logic. Raising here would cause the CLI handler to call
        ``sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)`` which would break
        ``@retry``.
        """
        if self._result is None:
            raise SandboxException("No result — was launch() called?")

        # Stream stdout to the console
        if self._result.stdout:
            for line in self._result.stdout.splitlines():
                echo(line, stream="stderr")
        if self._result.stderr:
            for line in self._result.stderr.splitlines():
                echo(line, stream="stderr")

        exit_code = self._result.exit_code

        self.cleanup()

        if exit_code != 0:
            echo(
                f"Sandbox task finished with exit code {exit_code}.",
                stream="stderr",
            )
            sys.exit(exit_code)
