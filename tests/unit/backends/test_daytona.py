"""Unit tests for the Daytona backend.

Mocks the Daytona SDK so no credentials or network needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from metaflow_extensions.sandbox.plugins.backend import ExecResult
from metaflow_extensions.sandbox.plugins.backend import Resources
from metaflow_extensions.sandbox.plugins.backend import SandboxConfig

_daytona_mock = MagicMock()


@pytest.fixture(autouse=True)
def _mock_daytona_module():
    with patch.dict("sys.modules", {"daytona": _daytona_mock}):
        yield


def _make_backend():
    """Create a DaytonaBackend with a mocked client."""
    with patch(
        "metaflow_extensions.sandbox.plugins.backends.daytona._get_client",
        return_value=MagicMock(),
    ):
        from metaflow_extensions.sandbox.plugins.backends.daytona import DaytonaBackend

        backend = DaytonaBackend()

    # Replace with a fresh mock to avoid any state leakage
    mock_client = MagicMock()
    backend._client = mock_client
    return backend, mock_client


class TestDaytonaBackend:
    def test_create_returns_sandbox_id(self) -> None:
        backend, mock_client = _make_backend()
        mock_sandbox = MagicMock()
        mock_sandbox.id = "sb-123"
        mock_client.create.return_value = mock_sandbox

        sandbox_id = backend.create(
            SandboxConfig(
                image="python:3.11-slim",
                resources=Resources(cpu=2, memory_mb=8192, gpu="1"),
            )
        )

        assert sandbox_id == "sb-123"
        mock_client.create.assert_called_once()
        _daytona_mock.Resources.assert_called_once_with(cpu=2, memory=8, gpu=1)
        kwargs = _daytona_mock.CreateSandboxFromImageParams.call_args.kwargs
        assert "resources" in kwargs

    def test_exec_returns_exec_result(self) -> None:
        backend, _ = _make_backend()

        mock_sandbox = MagicMock()
        mock_response = MagicMock()
        mock_response.exit_code = 0
        mock_response.result = "hello\n"
        mock_sandbox.process.exec.return_value = mock_response
        backend._sandboxes["sb-123"] = mock_sandbox

        result = backend.exec("sb-123", ["echo", "hello"])

        assert isinstance(result, ExecResult)
        assert result.ok
        assert result.exit_code == 0
        assert result.stdout == "hello\n"

    def test_exec_failure(self) -> None:
        backend, _ = _make_backend()

        mock_sandbox = MagicMock()
        mock_response = MagicMock()
        mock_response.exit_code = 1
        mock_response.result = "not found"
        mock_sandbox.process.exec.return_value = mock_response
        backend._sandboxes["sb-123"] = mock_sandbox

        result = backend.exec("sb-123", ["bad", "cmd"])

        assert not result.ok
        assert result.exit_code == 1
        # Daytona combines stdout/stderr into result
        assert result.stdout == "not found"

    def test_destroy_cleans_up(self) -> None:
        backend, mock_client = _make_backend()

        mock_sandbox = MagicMock()
        backend._sandboxes["sb-123"] = mock_sandbox

        backend.destroy("sb-123")

        mock_client.delete.assert_called_once_with(mock_sandbox)
        assert "sb-123" not in backend._sandboxes

    def test_destroy_nonexistent_is_noop(self) -> None:
        backend, mock_client = _make_backend()
        backend.destroy("does-not-exist")
        mock_client.delete.assert_not_called()


    def test_exec_script_uploads_and_executes_script_file(self) -> None:
        backend, _ = _make_backend()

        mock_sandbox = MagicMock()
        mock_response = MagicMock()
        mock_response.exit_code = 0
        mock_response.result = "done"
        mock_sandbox.process.exec.return_value = mock_response
        backend._sandboxes["sb-123"] = mock_sandbox

        script = "echo hello && echo world"
        result = backend.exec_script("sb-123", script)

        assert result.ok
        mock_sandbox.fs.upload_file.assert_called_once()
        # one call to execute script, one best-effort cleanup call
        assert mock_sandbox.process.exec.call_count == 2
        first_call = mock_sandbox.process.exec.call_args_list[0]
        assert first_call[0][0].startswith("bash -lc ")


class TestDaytonaImportError:
    def test_install_hint_contains_pip_command(self) -> None:
        from metaflow_extensions.sandbox.plugins.backends.daytona import _INSTALL_HINT

        assert "pip install metaflow-sandbox[daytona]" in _INSTALL_HINT
        assert "DAYTONA_API_KEY" in _INSTALL_HINT
