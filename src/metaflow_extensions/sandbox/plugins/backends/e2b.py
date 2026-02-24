"""E2B sandbox backend.

Layer: Concrete Backend
May only import from: ..backend (ABC + dataclasses), e2b SDK

Install: pip install metaflow-sandbox[e2b]
Docs:    https://e2b.dev/docs
"""

from __future__ import annotations

from pathlib import Path

from metaflow_extensions.sandbox.plugins.backend import ExecResult
from metaflow_extensions.sandbox.plugins.backend import SandboxBackend
from metaflow_extensions.sandbox.plugins.backend import SandboxConfig

_INSTALL_HINT = (
    "E2B SDK not found. Install it with:\n"
    "\n"
    "    pip install metaflow-sandbox[e2b]\n"
    "\n"
    "Then set E2B_API_KEY in your environment.\n"
    "Get a free key ($100 credit) at https://e2b.dev"
)


def _get_sandbox_class():  # type: ignore[no-untyped-def]
    try:
        from e2b_code_interpreter import Sandbox
    except ImportError:
        raise ImportError(_INSTALL_HINT) from None
    return Sandbox


class E2BBackend(SandboxBackend):
    """Runs Metaflow tasks in E2B Firecracker microVMs (~150ms cold start)."""

    def __init__(self) -> None:
        self._sandbox_cls = _get_sandbox_class()
        self._sandboxes: dict[str, object] = {}

    def create(self, config: SandboxConfig) -> str:
        sandbox = self._sandbox_cls.create(
            timeout=config.timeout,
            envs=config.env,
        )
        self._sandboxes[sandbox.sandbox_id] = sandbox
        return sandbox.sandbox_id

    def _get_sandbox(self, sandbox_id: str) -> object:
        if sandbox_id not in self._sandboxes:
            self._sandboxes[sandbox_id] = self._sandbox_cls.connect(sandbox_id)
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
        if cwd != "/":
            command_str = f"cd {cwd} && {command_str}"
        result = sandbox.commands.run(command_str, timeout=timeout)
        return ExecResult(
            exit_code=result.exit_code,
            stdout=getattr(result, "stdout", "") or "",
            stderr=getattr(result, "stderr", "") or "",
        )

    def upload(self, sandbox_id: str, local_path: str, remote_path: str) -> None:
        sandbox = self._get_sandbox(sandbox_id)
        content = Path(local_path).read_bytes()
        sandbox.files.write(remote_path, content)

    def download(self, sandbox_id: str, remote_path: str, local_path: str) -> None:
        sandbox = self._get_sandbox(sandbox_id)
        content = sandbox.files.read(remote_path, format="bytes")
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    def exec_script(
        self,
        sandbox_id: str,
        script: str,
        timeout: int = 600,
    ) -> ExecResult:
        """Run a bash script directly via E2B's shell execution.

        E2B's ``commands.run()`` already passes the string through a
        shell, so we send the script directly instead of wrapping it in
        ``["bash", "-c", ...]`` which breaks due to naive space-joining
        in :meth:`exec`.
        """
        sandbox = self._get_sandbox(sandbox_id)
        result = sandbox.commands.run(script, timeout=timeout)
        return ExecResult(
            exit_code=result.exit_code,
            stdout=getattr(result, "stdout", "") or "",
            stderr=getattr(result, "stderr", "") or "",
        )

    def destroy(self, sandbox_id: str) -> None:
        sandbox = self._sandboxes.pop(sandbox_id, None)
        if sandbox is not None:
            sandbox.kill()
