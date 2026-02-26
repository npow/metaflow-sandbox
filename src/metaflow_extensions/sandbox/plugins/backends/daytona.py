"""Daytona sandbox backend.

Layer: Concrete Backend
May only import from: ..backend (ABC + dataclasses), daytona SDK

Install: pip install metaflow-sandbox[daytona]
Docs:    https://www.daytona.io/docs/en/python-sdk/
"""

from __future__ import annotations

import json
import shlex
import time
import uuid
from math import ceil
from pathlib import Path

from metaflow_extensions.sandbox.plugins.backend import ExecResult
from metaflow_extensions.sandbox.plugins.backend import SandboxBackend
from metaflow_extensions.sandbox.plugins.backend import SandboxConfig

_INSTALL_HINT = (
    "Daytona SDK not found. Install it with:\n"
    "\n"
    "    pip install metaflow-sandbox[daytona]\n"
    "\n"
    "Then set DAYTONA_API_KEY and (optionally) DAYTONA_API_URL.\n"
    "See https://www.daytona.io/docs/ for details."
)


def _get_client():  # type: ignore[no-untyped-def]
    try:
        from daytona import Daytona
    except ImportError:
        raise ImportError(_INSTALL_HINT) from None
    return Daytona()


class DaytonaBackend(SandboxBackend):
    """Runs Metaflow tasks in Daytona sandboxes (<100ms cold start)."""

    def __init__(self) -> None:
        self._client = _get_client()
        self._sandboxes: dict[str, object] = {}

    @staticmethod
    def _response_debug_blob(response) -> str:  # type: ignore[no-untyped-def]
        if hasattr(response, "model_dump"):
            try:
                return json.dumps(response.model_dump(), default=str)
            except Exception:
                return str(response.model_dump())
        return str(response)

    @staticmethod
    def _normalize_exec_response(response) -> ExecResult:  # type: ignore[no-untyped-def]
        exit_code = getattr(response, "exit_code", -1)
        result = getattr(response, "result", "") or ""
        stdout = result
        stderr = ""
        if hasattr(response, "model_dump"):
            dumped = response.model_dump()
            artifacts = dumped.get("artifacts") or {}
            if not stdout:
                stdout = artifacts.get("stdout") or ""
            stderr = artifacts.get("stderr") or ""
            if not stderr and dumped.get("additional_properties"):
                stderr = str(dumped.get("additional_properties"))
        if exit_code == -1:
            debug_blob = DaytonaBackend._response_debug_blob(response)
            stderr = f"{stderr}\n[daytona-debug] {debug_blob}".strip()
        return ExecResult(exit_code=exit_code, stdout=stdout, stderr=stderr)

    def create(self, config: SandboxConfig) -> str:
        from daytona import CreateSandboxFromImageParams
        from daytona import Resources

        gpu_count = None
        if config.resources.gpu:
            try:
                gpu_count = int(config.resources.gpu)
            except ValueError:
                gpu_count = None

        # Metaflow uses memory in MB; Daytona resources.memory expects GB.
        memory_gb = max(1, ceil(config.resources.memory_mb / 1024))
        resources = Resources(
            cpu=config.resources.cpu,
            memory=memory_gb,
            gpu=gpu_count,
        )

        params = CreateSandboxFromImageParams(
            image=config.image or "python:3.11-slim",
            env_vars=config.env or None,
            auto_stop_interval=max(1, config.timeout // 60),
            resources=resources,
        )
        sandbox = self._client.create(params)
        sandbox_id = sandbox.id
        self._sandboxes[sandbox_id] = sandbox
        return sandbox_id

    def _get_sandbox(self, sandbox_id: str) -> object:
        if sandbox_id not in self._sandboxes:
            self._sandboxes[sandbox_id] = self._client.get(sandbox_id)
        return self._sandboxes[sandbox_id]

    def exec(
        self,
        sandbox_id: str,
        cmd: list[str],
        cwd: str = "/",
        timeout: int = 300,
    ) -> ExecResult:
        sandbox = self._get_sandbox(sandbox_id)
        command_str = shlex.join(cmd)
        for attempt in range(3):
            response = sandbox.process.exec(
                f"bash -lc {shlex.quote(command_str)}", cwd=cwd, timeout=timeout
            )
            result = self._normalize_exec_response(response)
            if result.exit_code != -1:
                return result
            if attempt < 2:
                time.sleep(2**attempt)
        return result

    def upload(self, sandbox_id: str, local_path: str, remote_path: str) -> None:
        sandbox = self._get_sandbox(sandbox_id)
        content = Path(local_path).read_bytes()
        sandbox.fs.upload_file(content, remote_path)

    def download(self, sandbox_id: str, remote_path: str, local_path: str) -> None:
        sandbox = self._get_sandbox(sandbox_id)
        content = sandbox.fs.download_file(remote_path)
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    def exec_script(
        self,
        sandbox_id: str,
        script: str,
        timeout: int = 600,
    ) -> ExecResult:
        """Run a bash script directly via Daytona's shell execution.

        Metaflow generates a bash script (uses bash-specific redirections),
        so execute under ``bash -lc`` explicitly.
        """
        sandbox = self._get_sandbox(sandbox_id)
        remote_script = f"/tmp/metaflow-sandbox-{uuid.uuid4().hex}.sh"
        sandbox.fs.upload_file(script.encode("utf-8"), remote_script)
        command = (
            f"bash -lc {shlex.quote(f'chmod 700 {remote_script} && bash {remote_script}')}"
        )
        try:
            for attempt in range(3):
                response = sandbox.process.exec(command, timeout=timeout)
                result = self._normalize_exec_response(response)
                if result.exit_code != -1:
                    return result
                if attempt < 2:
                    time.sleep(2**attempt)
            return result
        finally:
            # Best effort cleanup.
            sandbox.process.exec(
                f"bash -lc {shlex.quote(f'rm -f {remote_script}')}",
                timeout=min(timeout, 30),
            )

    def destroy(self, sandbox_id: str) -> None:
        sandbox = self._sandboxes.pop(sandbox_id, None)
        if sandbox is not None:
            self._client.delete(sandbox)
