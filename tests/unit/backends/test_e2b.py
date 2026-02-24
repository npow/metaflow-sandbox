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
