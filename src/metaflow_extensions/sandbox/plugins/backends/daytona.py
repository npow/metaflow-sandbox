"""Daytona sandbox backend.

Layer: Concrete Backend
May only import from: ..backend (ABC + dataclasses), daytona SDK

Install: pip install metaflow-sandbox[daytona]
Docs:    https://www.daytona.io/docs/en/python-sdk/
"""

from __future__ import annotations

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

    def create(self, config: SandboxConfig) -> str:
        from daytona import CreateSandboxFromImageParams

        params = CreateSandboxFromImageParams(
            image=config.image or "python:3.11-slim",
            env_vars=config.env or None,
            auto_stop_interval=max(1, config.timeout // 60),
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
        command_str = " ".join(cmd)
        response = sandbox.process.exec(command_str, cwd=cwd, timeout=timeout)
        # ExecuteResponse has exit_code: int and result: str (combined output)
        return ExecResult(
            exit_code=response.exit_code,
            stdout=response.result or "",
            stderr="",  # Daytona combines stdout/stderr into result
        )

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

        Daytona's ``process.exec()`` already passes the string through a
        shell, so we send the script directly instead of wrapping it in
        ``["bash", "-c", ...]`` which breaks due to naive space-joining
        in :meth:`exec`.
        """
        sandbox = self._get_sandbox(sandbox_id)
        response = sandbox.process.exec(script, timeout=timeout)
        return ExecResult(
            exit_code=response.exit_code,
            stdout=response.result or "",
            stderr="",
        )

    def destroy(self, sandbox_id: str) -> None:
        sandbox = self._sandboxes.pop(sandbox_id, None)
        if sandbox is not None:
            self._client.delete(sandbox)
