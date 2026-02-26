"""E2B sandbox backend.

Layer: Concrete Backend
May only import from: ..backend (ABC + dataclasses), e2b SDK

Install: pip install metaflow-sandbox[e2b]
Docs:    https://e2b.dev/docs
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

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
        return self.exec_script_streaming(sandbox_id, script, timeout)

    def exec_script_streaming(
        self,
        sandbox_id: str,
        script: str,
        timeout: int = 600,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        """Run a bash script via E2B with optional real-time log streaming.

        Uses ``commands.run(background=True)`` to get a ``CommandHandle``,
        then calls ``handle.wait(on_stdout=..., on_stderr=...)`` which
        delivers events from E2B's gRPC stream as they arrive.  The
        callbacks receive raw byte-aligned chunks, so we line-buffer them
        before forwarding.

        Also fixes a pre-existing issue: E2B raises ``CommandExitException``
        for non-zero exit codes, which the old ``commands.run()`` call let
        propagate.  We catch it here and convert to an ``ExecResult``.
        """
        try:
            from e2b.sandbox.commands.command_handle import CommandExitException
        except ImportError:
            raise ImportError(_INSTALL_HINT) from None

        sandbox = self._get_sandbox(sandbox_id)
        handle = sandbox.commands.run(script, background=True, timeout=timeout)

        # Line-buffer raw E2B chunks so callers receive one complete line
        # at a time (matching the Daytona streaming contract).
        stdout_buf = ""
        stderr_buf = ""

        def _on_stdout_chunk(chunk: str) -> None:
            nonlocal stdout_buf
            stdout_buf += chunk
            parts = stdout_buf.split("\n")
            for line in parts[:-1]:
                on_stdout(line)  # type: ignore[misc]
            stdout_buf = parts[-1]

        def _on_stderr_chunk(chunk: str) -> None:
            nonlocal stderr_buf
            stderr_buf += chunk
            parts = stderr_buf.split("\n")
            for line in parts[:-1]:
                on_stderr(line)  # type: ignore[misc]
            stderr_buf = parts[-1]

        exit_code = 0
        full_stdout = ""
        full_stderr = ""
        try:
            result = handle.wait(
                on_stdout=_on_stdout_chunk if on_stdout else None,
                on_stderr=_on_stderr_chunk if on_stderr else None,
            )
            exit_code = result.exit_code
            full_stdout = getattr(result, "stdout", "") or ""
            full_stderr = getattr(result, "stderr", "") or ""
        except CommandExitException as e:
            # All output events are delivered to the callbacks before the
            # exception is raised — we only need to capture the exit code.
            exit_code = e.exit_code
            full_stdout = getattr(e, "stdout", "") or ""
            full_stderr = getattr(e, "stderr", "") or ""

        # Flush any partial last line that had no trailing newline.
        if stdout_buf and on_stdout:
            on_stdout(stdout_buf)
        if stderr_buf and on_stderr:
            on_stderr(stderr_buf)

        return ExecResult(exit_code=exit_code, stdout=full_stdout, stderr=full_stderr)

    def destroy(self, sandbox_id: str) -> None:
        sandbox = self._sandboxes.pop(sandbox_id, None)
        if sandbox is not None:
            sandbox.kill()
