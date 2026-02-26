"""Sandbox executor — creates a sandbox, uploads code, runs a step.

Layer: Execution (same level as Metaflow Integration)
May only import from: .backend, .backends (registry), metaflow stdlib

This is the sandbox equivalent of ``metaflow.plugins.aws.batch.batch.Batch``.
It builds the full bash command (mflog setup, code download, bootstrap,
step execution, log save) and runs it inside a sandbox via the backend API.
"""

from __future__ import annotations

import bz2
import contextlib
import io
import json
import os
import shlex
import shutil
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Any
from typing import Callable
from urllib import request

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
    # Sandbox backends (needed for nested backend SDK usage in remote steps)
    "DAYTONA_API_KEY",
    "E2B_API_KEY",
]

_STAGING_BIN_DIR = "/tmp/metaflow-sandbox/bin"
_DEFAULT_DEBUG_DIR = "/tmp/metaflow-sandbox-debug"


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes", "on")


def _debug_settings() -> tuple[bool, str | None, str | None]:
    """
    Returns:
      keep_sandbox, dump_script_target, dump_env_target

    Control:
      METAFLOW_SANDBOX_DEBUG
        - 0/false/no/off: disabled
        - 1/true/yes/on: enable keep + dump script/env to default dir
        - <path>: enable keep + dump script/env to the given directory/path
    """
    debug_cfg = os.environ.get("METAFLOW_SANDBOX_DEBUG", "").strip()
    if debug_cfg:
        if debug_cfg.lower() in ("0", "false", "no", "off"):
            return False, None, None
        if debug_cfg.lower() in ("1", "true", "yes", "on"):
            return True, _DEFAULT_DEBUG_DIR, _DEFAULT_DEBUG_DIR
        # Any other non-empty value is treated as output directory/path.
        return True, debug_cfg, debug_cfg

    return False, None, None


def _skip_aws_session_token_for_endpoint() -> bool:
    # R2 static access keys don't require STS session tokens; forwarding one can
    # break PutObject with InvalidArgument on X-Amz-Security-Token.
    endpoint = (os.environ.get("METAFLOW_S3_ENDPOINT_URL") or "").lower()
    if not endpoint:
        return False
    if "cloudflarestorage.com" in endpoint:
        return not _env_flag("METAFLOW_SANDBOX_FORWARD_AWS_SESSION_TOKEN")
    return False


def _is_cloudflare_r2_endpoint(endpoint: str | None) -> bool:
    return "cloudflarestorage.com" in (endpoint or "").lower()


def _target_linux_arch() -> str | None:
    target = (os.environ.get("METAFLOW_SANDBOX_TARGET_PLATFORM") or "").lower()
    if "linux-aarch64" in target or "linux-arm64" in target:
        return "aarch64"
    if "linux-64" in target or "linux-x86_64" in target or "linux-amd64" in target:
        return "x86_64"
    return None


def _elf_arch(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            hdr = f.read(20)
    except OSError:
        return None
    if len(hdr) < 20 or hdr[:4] != b"\x7fELF":
        return None
    # EI_DATA byte: 1=little, 2=big endian
    endianness = "<" if hdr[5] == 1 else ">" if hdr[5] == 2 else None
    if endianness is None:
        return None
    e_machine = int.from_bytes(hdr[18:20], byteorder="little" if endianness == "<" else "big")
    if e_machine == 62:
        return "x86_64"
    if e_machine == 183:
        return "aarch64"
    return "unknown"


def _is_compatible_linux_micromamba(path: str) -> bool:
    target_arch = _target_linux_arch()
    binary_arch = _elf_arch(path)
    if binary_arch is None:
        return False
    if target_arch is None:
        # If target arch is unknown, allow any Linux ELF micromamba binary.
        return binary_arch in ("x86_64", "aarch64")
    return binary_arch == target_arch


def _target_micromamba_platform() -> str:
    target_arch = _target_linux_arch()
    if target_arch == "aarch64":
        return "linux-aarch64"
    return "linux-64"


def _auto_download_micromamba() -> str | None:
    auto_download_cfg = os.environ.get("METAFLOW_SANDBOX_AUTO_DOWNLOAD_MICROMAMBA", "")
    if auto_download_cfg and not _env_flag("METAFLOW_SANDBOX_AUTO_DOWNLOAD_MICROMAMBA"):
        return None

    platform_id = _target_micromamba_platform()
    cache_root = Path(
        os.environ.get(
            "METAFLOW_SANDBOX_MICROMAMBA_CACHE_DIR",
            os.path.join(Path.home(), ".cache", "metaflow-sandbox", "micromamba"),
        )
    )
    final_path = cache_root / platform_id / "micromamba"
    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.is_file() and _is_compatible_linux_micromamba(str(final_path)):
        return str(final_path)

    url = f"https://micro.mamba.pm/api/micromamba/{platform_id}/latest"
    with request.urlopen(url, timeout=30) as resp:
        payload = resp.read()
    tar_bytes = bz2.decompress(payload)
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tf:
        member = tf.getmember("bin/micromamba")
        extracted = tf.extractfile(member)
        if extracted is None:
            raise SandboxException("Failed to extract micromamba from download payload.")
        fd, tmp_name = tempfile.mkstemp(
            prefix="micromamba.",
            suffix=".tmp",
            dir=str(final_path.parent),
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "wb") as out:
            shutil.copyfileobj(extracted, out)
    os.chmod(tmp_path, 0o755)
    os.replace(tmp_path, final_path)
    return str(final_path)


def _is_hard_minus_one(result: ExecResult) -> bool:
    if result.exit_code != -1:
        return False
    if (result.stdout or "").strip():
        return False
    stderr = (result.stderr or "").strip()
    if not stderr:
        return True
    remaining = [
        line
        for line in stderr.splitlines()
        if line.strip() and not line.lstrip().startswith("[daytona-debug]")
    ]
    return len(remaining) == 0


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
        self._log_streamed: bool = False

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
    # Bootstrap artifact staging
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_upload_specs() -> list[dict[str, str | None]]:
        raw = os.environ.get("METAFLOW_SANDBOX_UPLOADS")
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            raise SandboxException(
                "METAFLOW_SANDBOX_UPLOADS must be valid JSON. "
                "Expected a list of objects: "
                '[{"local": "/path/file", "remote": "/tmp/file", "mode": "0755"}]'
            ) from e
        if not isinstance(parsed, list):
            raise SandboxException("METAFLOW_SANDBOX_UPLOADS must be a JSON list.")

        specs: list[dict[str, str | None]] = []
        for item in parsed:
            if not isinstance(item, dict):
                raise SandboxException(
                    "Each entry in METAFLOW_SANDBOX_UPLOADS must be an object."
                )
            local = item.get("local")
            remote = item.get("remote")
            mode = item.get("mode")
            if not isinstance(local, str) or not local:
                raise SandboxException(
                    "Each upload entry must contain a non-empty string 'local'."
                )
            if not isinstance(remote, str) or not remote:
                raise SandboxException(
                    "Each upload entry must contain a non-empty string 'remote'."
                )
            if mode is not None and not isinstance(mode, (str, int)):
                raise SandboxException("Upload 'mode' must be a string or integer.")
            mode_s: str | None = str(mode) if mode is not None else None
            specs.append(
                {"local": local, "remote": remote, "mode": mode_s, "optional": None}
            )
        return specs

    @staticmethod
    def _resolve_staged_uploads() -> tuple[list[dict[str, str | None]], bool]:
        uploads = SandboxExecutor._parse_upload_specs()
        stage_cfg = os.environ.get("METAFLOW_SANDBOX_STAGE_MICROMAMBA", "").lower()
        if stage_cfg in ("0", "false", "no", "off"):
            return uploads, False
        force_stage = stage_cfg in ("1", "true", "yes", "on")

        micromamba_path = os.environ.get("METAFLOW_SANDBOX_MICROMAMBA_PATH") or shutil.which(
            "micromamba"
        )
        compatible = bool(micromamba_path) and _is_compatible_linux_micromamba(
            str(micromamba_path)
        )
        auto_download_err: Exception | None = None
        if not compatible:
            try:
                micromamba_path = _auto_download_micromamba()
                compatible = bool(micromamba_path) and _is_compatible_linux_micromamba(
                    str(micromamba_path)
                )
            except Exception as e:
                auto_download_err = e
        if not compatible:
            if force_stage:
                err_msg = (
                    f" Auto-download error: {auto_download_err}"
                    if auto_download_err is not None
                    else ""
                )
                raise SandboxException(
                    "METAFLOW_SANDBOX_STAGE_MICROMAMBA=1 but no compatible Linux "
                    "micromamba binary is available locally or via auto-download. "
                    "Set METAFLOW_SANDBOX_MICROMAMBA_PATH to a compatible binary."
                    f"{err_msg}"
                )
            return uploads, False
        if any(spec.get("remote") == f"{_STAGING_BIN_DIR}/micromamba" for spec in uploads):
            return uploads, True
        uploads.append(
            {
                "local": str(micromamba_path),
                "remote": f"{_STAGING_BIN_DIR}/micromamba",
                "mode": "0755",
                "optional": None if force_stage else "1",
            }
        )
        return uploads, True

    def _stage_uploads(
        self, sandbox_id: str, uploads: list[dict[str, str | None]]
    ) -> list[str]:
        chmod_cmds: list[str] = []
        for spec in uploads:
            local = spec["local"]
            remote = spec["remote"]
            mode = spec.get("mode")
            optional = spec.get("optional") in ("1", "true", "yes", "on")
            if local is None or remote is None:
                raise SandboxException("Invalid upload spec: missing local or remote path.")
            if not os.path.isfile(local):
                if optional:
                    continue
                raise SandboxException(
                    f"Configured upload file does not exist: {local!r}. "
                    "Check METAFLOW_SANDBOX_UPLOADS or METAFLOW_SANDBOX_MICROMAMBA_PATH."
                )
            try:
                self._backend.upload(sandbox_id, local, remote)
            except Exception as e:
                if optional:
                    continue
                raise SandboxException(
                    "Failed to upload staged file.\n"
                    f"local_path: {local}\n"
                    f"remote_path: {remote}\n"
                    f"error: {e}"
                ) from e
            if mode:
                chmod_cmds.append(f"chmod {shlex.quote(mode)} {shlex.quote(remote)}")
        return chmod_cmds

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
        prepend_path: str | None = None,
    ) -> dict[str, str]:
        """Assemble the environment variables for the sandbox."""
        proxy_service_metadata = DEFAULT_METADATA == "service"
        env: dict[str, str] = {
            "METAFLOW_CODE_METADATA": code_package_metadata,
            "METAFLOW_CODE_SHA": code_package_sha,
            "METAFLOW_CODE_URL": code_package_url,
            "METAFLOW_CODE_DS": datastore_type,
            "METAFLOW_USER": util.get_username(),
            "METAFLOW_DEFAULT_DATASTORE": datastore_type,
            "METAFLOW_DEFAULT_METADATA": (
                "local" if proxy_service_metadata else DEFAULT_METADATA
            ),
            "METAFLOW_SANDBOX_WORKLOAD": "1",
            "METAFLOW_SANDBOX_BACKEND": backend_name,
        }
        daytona_key = os.environ.get("DAYTONA_API_KEY")
        if daytona_key:
            env["METAFLOW_DAYTONA_API_KEY"] = daytona_key
        e2b_key = os.environ.get("E2B_API_KEY")
        if e2b_key:
            env["METAFLOW_E2B_API_KEY"] = e2b_key

        if SERVICE_INTERNAL_URL:
            env["METAFLOW_SERVICE_URL"] = SERVICE_INTERNAL_URL

        # Forward datastore-specific configuration from the local env
        # (e.g. METAFLOW_DATASTORE_SYSROOT_S3, METAFLOW_DATATOOLS_S3ROOT).
        from metaflow.metaflow_config_funcs import config_values

        for k, v in config_values():
            if k.startswith("METAFLOW_DATASTORE_SYSROOT_") or k.startswith(
                "METAFLOW_DATATOOLS_"
            ) or k.startswith("METAFLOW_S3") or k.startswith(
                "METAFLOW_CARD_S3"
            ) or k.startswith("METAFLOW_CONDA") or k.startswith(
                "METAFLOW_SERVICE"
            ):
                env[k] = v

        # Forward cloud credentials
        skip_aws_session_token = _skip_aws_session_token_for_endpoint()
        for var in _FORWARDED_CREDENTIAL_VARS:
            val = os.environ.get(var)
            if val:
                if var == "AWS_SESSION_TOKEN" and skip_aws_session_token:
                    continue
                env[var] = val

        # User-specified env vars from the decorator
        if sandbox_env:
            env.update(sandbox_env)

        # R2 can stall under high parallel S3 fetch fan-out during conda bootstrap.
        # Apply a conservative default for sandbox workloads unless explicitly set.
        if "METAFLOW_S3_WORKER_COUNT" not in env and _is_cloudflare_r2_endpoint(
            env.get("METAFLOW_S3_ENDPOINT_URL") or os.environ.get("METAFLOW_S3_ENDPOINT_URL")
        ):
            env["METAFLOW_S3_WORKER_COUNT"] = os.environ.get(
                "METAFLOW_SANDBOX_R2_WORKER_COUNT", "8"
            )

        if prepend_path:
            # Sandbox-level PATH must retain standard system locations.
            # If we overwrite it with only a staged bin directory, some
            # backends (e.g. Daytona process wrapper) fail to exec shell
            # commands and return exit_code=-1.
            base_path = env.get("PATH") or os.environ.get(
                "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            )
            env["PATH"] = (
                f"{prepend_path}:{base_path}"
            )

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
        on_log: Callable[[str, str], None] | None = None,
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

        staged_uploads, prepend_bin_dir = self._resolve_staged_uploads()
        sandbox_env = self._build_env(
            code_package_metadata,
            code_package_sha,
            code_package_url,
            datastore_type,
            self._backend_name,
            sandbox_env=env,
            prepend_path=_STAGING_BIN_DIR if prepend_bin_dir else None,
        )

        config = SandboxConfig(
            image=image or "python:3.11-slim",
            env=sandbox_env,
            resources=Resources(cpu=cpu, memory_mb=memory, gpu=gpu),
            timeout=timeout,
        )

        max_recreates = int(os.environ.get("METAFLOW_SANDBOX_MAX_INFRA_RETRIES", "1"))
        attempts = max(1, max_recreates + 1)
        last_result: ExecResult | None = None
        _, debug_dump, debug_env_dump = _debug_settings()

        for attempt in range(attempts):
            self._sandbox_id = self._backend.create(config)
            chmod_cmds: list[str] = []
            if staged_uploads:
                chmod_cmds = self._stage_uploads(self._sandbox_id, staged_uploads)

            # Inject sandbox ID into the script — we can't know the ID
            # before create(), so we prepend an export to the script.
            setup_prefix_parts: list[str] = []
            if prepend_bin_dir:
                # Login shells can reset PATH and drop env-injected entries.
                # Re-apply staged bin path inside the executed script.
                setup_prefix_parts.append(
                    f"export PATH={shlex.quote(_STAGING_BIN_DIR)}:$PATH"
                )
            if chmod_cmds:
                setup_prefix_parts.extend(chmod_cmds)
            setup_prefix = " && ".join(setup_prefix_parts)
            if setup_prefix:
                setup_prefix += " && "
            run_cmd = (
                f"export METAFLOW_SANDBOX_ID={shlex.quote(self._sandbox_id)} && "
                f"{setup_prefix}{cmd_str}"
            )

            if debug_dump:
                dump_path = debug_dump
                if os.path.isdir(debug_dump):
                    ts = int(time.time() * 1000)
                    dump_path = os.path.join(
                        debug_dump, f"{self._backend_name}-{step_name}-{ts}.sh"
                    )
                with open(dump_path, "w", encoding="utf-8") as f:
                    f.write(run_cmd)
            if debug_env_dump:
                env_dump_path = debug_env_dump
                if os.path.isdir(debug_env_dump):
                    ts = int(time.time() * 1000)
                    env_dump_path = os.path.join(
                        debug_env_dump, f"{self._backend_name}-{step_name}-{ts}.env.json"
                    )
                with open(env_dump_path, "w", encoding="utf-8") as f:
                    json.dump(sandbox_env, f, sort_keys=True, indent=2)

            if on_log is not None:
                def _on_stdout(line: str) -> None:
                    on_log(line, "stdout")

                def _on_stderr(line: str) -> None:
                    on_log(line, "stderr")
            else:
                _on_stdout = _on_stderr = None

            result = self._backend.exec_script_streaming(
                self._sandbox_id,
                run_cmd,
                timeout=timeout,
                on_stdout=_on_stdout,
                on_stderr=_on_stderr,
            )
            last_result = result
            hard_minus_one = _is_hard_minus_one(result)
            if not hard_minus_one or attempt == attempts - 1:
                break
            with contextlib.suppress(Exception):
                self._backend.destroy(self._sandbox_id)
            self._sandbox_id = None

        if last_result is None:
            raise SandboxException("Sandbox execution did not produce a result.")
        self._result = last_result
        self._log_streamed = on_log is not None

    def cleanup(self) -> None:
        """Destroy the sandbox if it exists. Best-effort, never raises."""
        keep_debug_sandbox, _, _ = _debug_settings()
        if keep_debug_sandbox:
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

        # Echo buffered output only when streaming didn't already emit it live.
        if not self._log_streamed:
            if self._result.stdout:
                for line in self._result.stdout.splitlines():
                    echo(line, stream="stderr")
            if self._result.stderr:
                for line in self._result.stderr.splitlines():
                    echo(line, stream="stderr")

        exit_code = self._result.exit_code
        sandbox_id = self._sandbox_id

        self.cleanup()

        if exit_code != 0:
            echo(
                "Sandbox task finished with exit code "
                f"{exit_code}. sandbox_id={sandbox_id or '<unknown>'}",
                stream="stderr",
            )
            sys.exit(exit_code)
