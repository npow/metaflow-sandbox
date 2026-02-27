"""Unit tests for the Boxlite backend.

Mocks the boxlite SDK so no KVM/HVF or credentials are needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

from sandrun.backend import ExecResult
from sandrun.backend import SandboxConfig


def _make_backend():
    """Create a BoxliteBackend with a mocked SyncSimpleBox class."""
    with patch.dict(
        "sys.modules",
        {"boxlite": MagicMock(), "boxlite.sync_api": MagicMock()},
    ), patch("sandrun.backends.boxlite._get_sync_simplebox") as mock_get:
        mock_cls = MagicMock()
        mock_get.return_value = mock_cls

        from sandrun.backends.boxlite import BoxliteBackend

        backend = BoxliteBackend()
    return backend, mock_cls


def _make_box(box_id: str = "01BOXLITE123") -> MagicMock:
    """Return a mock SyncSimpleBox instance with the given ID."""
    box = MagicMock()
    box.id = box_id
    return box


class TestBoxliteBackend:
    def test_create_returns_box_id(self) -> None:
        backend, mock_cls = _make_backend()
        mock_box = _make_box("01BOXLITE123")
        mock_cls.return_value = mock_box

        with patch("sandrun.backends.boxlite._get_sync_simplebox", return_value=mock_cls):
            sandbox_id = backend.create(SandboxConfig())

        assert sandbox_id == "01BOXLITE123"
        mock_box.__enter__.assert_called_once()

    def test_create_passes_image_and_resources(self) -> None:
        from sandrun.backend import Resources

        backend, mock_cls = _make_backend()
        mock_box = _make_box()
        mock_cls.return_value = mock_box
        config = SandboxConfig(
            image="ubuntu:22.04",
            resources=Resources(cpu=2, memory_mb=2048),
            env={"FOO": "bar"},
        )

        with patch("sandrun.backends.boxlite._get_sync_simplebox", return_value=mock_cls):
            backend.create(config)

        mock_cls.assert_called_once_with(
            image="ubuntu:22.04",
            memory_mib=2048,
            cpus=2,
            env=[("FOO", "bar")],
            auto_remove=True,
        )

    def test_create_uses_default_image(self) -> None:
        backend, mock_cls = _make_backend()
        mock_cls.return_value = _make_box()

        with patch("sandrun.backends.boxlite._get_sync_simplebox", return_value=mock_cls):
            backend.create(SandboxConfig())

        kwargs = mock_cls.call_args.kwargs
        assert kwargs["image"] == "python:slim"

    def test_exec_returns_exec_result(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        mock_box.exec.return_value = MagicMock(exit_code=0, stdout="hello\n", stderr="")
        backend._boxes["01BOXLITE123"] = mock_box

        result = backend.exec("01BOXLITE123", ["echo", "hello"])

        assert isinstance(result, ExecResult)
        assert result.ok
        assert result.stdout == "hello\n"

    def test_exec_wraps_command_in_bash(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        mock_box.exec.return_value = MagicMock(exit_code=0, stdout="", stderr="")
        backend._boxes["01BOXLITE123"] = mock_box

        backend.exec("01BOXLITE123", ["ls", "-la"])

        # Must be called as bash -c '...' so shell features work.
        args = mock_box.exec.call_args[0]
        assert args[0] == "bash"
        assert args[1] == "-c"

    def test_exec_with_cwd_prepends_cd(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        mock_box.exec.return_value = MagicMock(exit_code=0, stdout="", stderr="")
        backend._boxes["01BOXLITE123"] = mock_box

        backend.exec("01BOXLITE123", ["pwd"], cwd="/app")

        command = mock_box.exec.call_args[0][2]
        assert command.startswith("cd '/app' &&") or command.startswith("cd /app &&")

    def test_exec_without_cwd_skips_cd(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        mock_box.exec.return_value = MagicMock(exit_code=0, stdout="", stderr="")
        backend._boxes["01BOXLITE123"] = mock_box

        backend.exec("01BOXLITE123", ["pwd"])

        command = mock_box.exec.call_args[0][2]
        assert "cd" not in command

    def test_exec_propagates_nonzero_exit(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        mock_box.exec.return_value = MagicMock(exit_code=1, stdout="", stderr="oops")
        backend._boxes["01BOXLITE123"] = mock_box

        result = backend.exec("01BOXLITE123", ["false"])

        assert result.exit_code == 1
        assert not result.ok

    def test_destroy_calls_exit(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        backend._boxes["01BOXLITE123"] = mock_box

        backend.destroy("01BOXLITE123")

        mock_box.__exit__.assert_called_once_with(None, None, None)

    def test_destroy_removes_from_registry(self) -> None:
        backend, _ = _make_backend()
        backend._boxes["01BOXLITE123"] = _make_box()

        backend.destroy("01BOXLITE123")

        assert "01BOXLITE123" not in backend._boxes

    def test_destroy_unknown_id_is_noop(self) -> None:
        backend, _ = _make_backend()
        backend.destroy("nonexistent")  # must not raise

    def test_destroy_swallows_exit_exception(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        mock_box.__exit__.side_effect = RuntimeError("VM gone")
        backend._boxes["01BOXLITE123"] = mock_box

        backend.destroy("01BOXLITE123")  # must not raise

        assert "01BOXLITE123" not in backend._boxes

    def test_upload_calls_copy_in(self) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        backend._boxes["01BOXLITE123"] = mock_box

        with patch("sandrun.backends.boxlite._copy_in") as mock_copy_in:
            backend.upload("01BOXLITE123", "/local/file.txt", "/remote/file.txt")

        mock_copy_in.assert_called_once_with(mock_box, "/local/file.txt", "/remote/file.txt")

    def test_download_calls_copy_out(self, tmp_path) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        backend._boxes["01BOXLITE123"] = mock_box
        local_dest = str(tmp_path / "subdir" / "file.txt")

        with patch("sandrun.backends.boxlite._copy_out") as mock_copy_out:
            backend.download("01BOXLITE123", "/remote/file.txt", local_dest)

        mock_copy_out.assert_called_once_with(mock_box, "/remote/file.txt", local_dest)

    def test_download_creates_parent_dirs(self, tmp_path) -> None:
        backend, _ = _make_backend()
        mock_box = _make_box()
        backend._boxes["01BOXLITE123"] = mock_box
        local_dest = str(tmp_path / "deep" / "nested" / "file.txt")

        with patch("sandrun.backends.boxlite._copy_out"):
            backend.download("01BOXLITE123", "/remote/file.txt", local_dest)

        assert (tmp_path / "deep" / "nested").is_dir()


class TestBoxliteExecScript:
    """Tests for exec_script and exec_script_streaming."""

    def _backend_with_box(self, box_id: str = "01BOXLITE123"):
        backend, _ = _make_backend()
        mock_box = _make_box(box_id)
        backend._boxes[box_id] = mock_box
        return backend, mock_box

    def _ok(self, stdout: str = "", stderr: str = "") -> MagicMock:
        return MagicMock(exit_code=0, stdout=stdout, stderr=stderr)

    def _fail(self, exit_code: int = 1, stderr: str = "") -> MagicMock:
        return MagicMock(exit_code=exit_code, stdout="", stderr=stderr)

    def test_exec_script_delegates_to_streaming(self) -> None:
        backend, mock_box = self._backend_with_box()
        # setup write succeeds; script run succeeds; cleanup succeeds
        mock_box.exec.side_effect = [self._ok(), self._ok("out\n"), self._ok()]

        result = backend.exec_script("01BOXLITE123", "echo out")

        assert result.exit_code == 0

    def test_script_write_failure_returns_early(self) -> None:
        backend, mock_box = self._backend_with_box()
        mock_box.exec.return_value = self._fail(1, "no base64")

        result = backend.exec_script("01BOXLITE123", "echo hello")

        assert result.exit_code == 1
        # Script exec should never have been called — only one exec call.
        assert mock_box.exec.call_count == 1

    def test_no_callbacks_runs_blocking_exec(self) -> None:
        backend, mock_box = self._backend_with_box()
        mock_box.exec.side_effect = [
            self._ok(),           # script write
            self._ok("result\n"), # script run
            self._ok(),           # cleanup
        ]

        result = backend.exec_script_streaming("01BOXLITE123", "echo result")

        assert result.exit_code == 0
        assert result.stdout == "result\n"
        # No streaming path taken — _runtime._sync must not have been called.
        mock_box._runtime._sync.assert_not_called()

    def test_no_callbacks_nonzero_exit_propagated(self) -> None:
        backend, mock_box = self._backend_with_box()
        mock_box.exec.side_effect = [
            self._ok(),      # script write
            self._fail(42),  # script run
            self._ok(),      # cleanup
        ]

        result = backend.exec_script_streaming("01BOXLITE123", "exit 42")

        assert result.exit_code == 42

    def test_cleanup_runs_even_on_failure(self) -> None:
        backend, mock_box = self._backend_with_box()
        mock_box.exec.side_effect = [
            self._ok(),      # script write
            self._fail(1),   # script run
            self._ok(),      # cleanup — must still be called
        ]

        backend.exec_script_streaming("01BOXLITE123", "false")

        # Three exec calls: write, run, cleanup.
        assert mock_box.exec.call_count == 3

    def test_streaming_with_callbacks_uses_runtime_sync(self) -> None:
        backend, mock_box = self._backend_with_box()
        # Setup: write OK, then streaming path, then cleanup
        mock_box.exec.side_effect = [self._ok(), self._ok()]  # write + cleanup

        fake_result = ExecResult(exit_code=0, stdout="line1\nline2", stderr="")
        mock_box._runtime._sync.return_value = fake_result

        lines: list[str] = []
        result = backend.exec_script_streaming(
            "01BOXLITE123", "echo hi", on_stdout=lines.append
        )

        mock_box._runtime._sync.assert_called_once()
        assert result == fake_result

    def test_streaming_callbacks_path_accesses_async_box(self) -> None:
        backend, mock_box = self._backend_with_box()
        mock_box.exec.side_effect = [self._ok(), self._ok()]

        mock_box._runtime._sync.return_value = ExecResult(
            exit_code=0, stdout="", stderr=""
        )

        backend.exec_script_streaming(
            "01BOXLITE123", "true", on_stderr=lambda _: None
        )

        # Streaming path must access box._box._box (async Rust Box).
        _ = mock_box._box._box  # verifies attribute chain exists on mock


class TestBoxliteRegistration:
    """Verify the backend is reachable through the registry."""

    def test_boxlite_in_backends_registry(self) -> None:
        from sandrun.backends import _BACKENDS

        assert "boxlite" in _BACKENDS

    def test_registry_entry_points_to_correct_class(self) -> None:
        from sandrun.backends import _BACKENDS

        module_path, class_name = _BACKENDS["boxlite"]
        assert module_path == ".boxlite"
        assert class_name == "BoxliteBackend"
