"""Unit tests for the E2B backend.

Mocks the E2B SDK so no credentials or network needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

from metaflow_extensions.sandbox.plugins.backend import ExecResult
from metaflow_extensions.sandbox.plugins.backend import SandboxConfig


def _make_backend():
    """Create an E2BBackend with a mocked sandbox class, bypassing SDK import."""
    with patch.dict("sys.modules", {"e2b_code_interpreter": MagicMock()}), patch(
        "metaflow_extensions.sandbox.plugins.backends.e2b._get_sandbox_class",
    ) as mock_get:
        mock_sandbox_cls = MagicMock()
        mock_get.return_value = mock_sandbox_cls

        from metaflow_extensions.sandbox.plugins.backends.e2b import E2BBackend

        backend = E2BBackend()
    return backend, mock_sandbox_cls


class TestE2BBackend:
    def test_create_returns_sandbox_id(self) -> None:
        backend, mock_cls = _make_backend()
        mock_sandbox = MagicMock()
        mock_sandbox.sandbox_id = "e2b-456"
        mock_cls.create.return_value = mock_sandbox

        sandbox_id = backend.create(SandboxConfig())

        assert sandbox_id == "e2b-456"
        mock_cls.create.assert_called_once()

    def test_exec_returns_exec_result(self) -> None:
        backend, _ = _make_backend()

        mock_sandbox = MagicMock()
        mock_result = MagicMock()
        mock_result.exit_code = 0
        mock_result.stdout = "world\n"
        mock_result.stderr = ""
        mock_sandbox.commands.run.return_value = mock_result
        backend._sandboxes["e2b-456"] = mock_sandbox

        result = backend.exec("e2b-456", ["echo", "world"])

        assert isinstance(result, ExecResult)
        assert result.ok
        assert result.stdout == "world\n"

    def test_exec_with_cwd(self) -> None:
        backend, _ = _make_backend()

        mock_sandbox = MagicMock()
        mock_result = MagicMock()
        mock_result.exit_code = 0
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_sandbox.commands.run.return_value = mock_result
        backend._sandboxes["e2b-456"] = mock_sandbox

        backend.exec("e2b-456", ["ls"], cwd="/tmp")

        call_args = mock_sandbox.commands.run.call_args
        assert "/tmp" in call_args[0][0]

    def test_destroy_kills_sandbox(self) -> None:
        backend, _ = _make_backend()

        mock_sandbox = MagicMock()
        backend._sandboxes["e2b-456"] = mock_sandbox

        backend.destroy("e2b-456")

        mock_sandbox.kill.assert_called_once()
        assert "e2b-456" not in backend._sandboxes


class _FakeCommandExitException(Exception):
    """Minimal stand-in for e2b's CommandExitException."""

    def __init__(self, exit_code: int, stdout: str, stderr: str) -> None:
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr


def _e2b_modules_patch(exc_cls=_FakeCommandExitException):
    """Return a patch.dict context that satisfies the lazy e2b import in exec_script_streaming."""
    import sys

    cmd_handle_mock = MagicMock()
    cmd_handle_mock.CommandExitException = exc_cls
    return patch.dict(
        sys.modules,
        {
            "e2b": MagicMock(),
            "e2b.sandbox": MagicMock(),
            "e2b.sandbox.commands": MagicMock(),
            "e2b.sandbox.commands.command_handle": cmd_handle_mock,
        },
    )


def _make_handle(chunks: list[tuple[str | None, str | None]], exit_code: int = 0):
    """Build a mock CommandHandle that replays (stdout_chunk, stderr_chunk) pairs."""

    def _wait(on_stdout=None, on_stderr=None, **_kw):
        stdout_acc = ""
        stderr_acc = ""
        for out, err in chunks:
            if out is not None:
                stdout_acc += out
                if on_stdout:
                    on_stdout(out)
            if err is not None:
                stderr_acc += err
                if on_stderr:
                    on_stderr(err)
        if exit_code != 0:
            raise _FakeCommandExitException(exit_code, stdout_acc, stderr_acc)
        result = MagicMock()
        result.exit_code = exit_code
        result.stdout = stdout_acc
        result.stderr = stderr_acc
        return result

    handle = MagicMock()
    handle.wait.side_effect = _wait
    return handle


class TestE2BStreaming:
    """Tests for exec_script_streaming with mocked E2B SDK."""

    def _backend(self):
        backend, _ = _make_backend()
        mock_sandbox = MagicMock()
        backend._sandboxes["e2b-456"] = mock_sandbox
        return backend, mock_sandbox

    def test_streaming_delivers_complete_lines(self) -> None:
        backend, mock_sandbox = self._backend()
        mock_sandbox.commands.run.return_value = _make_handle(
            [("hello\nwor", None), ("ld\n", None)]
        )
        stdout_lines: list[str] = []
        with _e2b_modules_patch():
            result = backend.exec_script_streaming(
                "e2b-456", "echo hello", on_stdout=stdout_lines.append
            )
        assert stdout_lines == ["hello", "world"]
        assert result.exit_code == 0

    def test_streaming_nonzero_exit_returns_exec_result(self) -> None:
        backend, mock_sandbox = self._backend()
        mock_sandbox.commands.run.return_value = _make_handle(
            [("output\n", None)], exit_code=1
        )
        stdout_lines: list[str] = []
        with _e2b_modules_patch():
            result = backend.exec_script_streaming(
                "e2b-456", "false", on_stdout=stdout_lines.append
            )
        assert result.exit_code == 1
        assert stdout_lines == ["output"]

    def test_streaming_flushes_partial_last_line(self) -> None:
        # Chunk with no trailing newline — should still be emitted at the end.
        backend, mock_sandbox = self._backend()
        mock_sandbox.commands.run.return_value = _make_handle([("no newline", None)])
        stdout_lines: list[str] = []
        with _e2b_modules_patch():
            result = backend.exec_script_streaming(
                "e2b-456", "printf 'no newline'", on_stdout=stdout_lines.append
            )
        assert stdout_lines == ["no newline"]
        assert result.exit_code == 0

    def test_no_callbacks_uses_background_handle(self) -> None:
        backend, mock_sandbox = self._backend()
        mock_sandbox.commands.run.return_value = _make_handle([])
        with _e2b_modules_patch():
            backend.exec_script_streaming("e2b-456", "true")
        # Even without callbacks, background=True must be used (no blocking run).
        assert mock_sandbox.commands.run.call_args.kwargs.get("background") is True
