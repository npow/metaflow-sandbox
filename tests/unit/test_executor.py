"""Unit tests for the SandboxExecutor.

These tests use AST-based source inspection to avoid importing metaflow
directly (which fails in dev due to namespace package conflicts).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

EXECUTOR_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "metaflow_extensions"
    / "sandbox"
    / "plugins"
    / "sandbox_executor.py"
)


class TestExecutorStructure:
    """Verify the executor module has the expected classes and methods."""

    def test_file_exists(self) -> None:
        assert EXECUTOR_FILE.exists()

    def test_has_sandbox_executor_class(self) -> None:
        tree = ast.parse(EXECUTOR_FILE.read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "SandboxExecutor" in class_names

    def test_has_sandbox_exception_class(self) -> None:
        tree = ast.parse(EXECUTOR_FILE.read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "SandboxException" in class_names

    @pytest.fixture()
    def executor_methods(self) -> set[str]:
        tree = ast.parse(EXECUTOR_FILE.read_text())
        methods: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "SandboxExecutor":
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        methods.add(item.name)
        return methods

    def test_has_command_method(self, executor_methods: set[str]) -> None:
        assert "_command" in executor_methods

    def test_has_build_env_method(self, executor_methods: set[str]) -> None:
        assert "_build_env" in executor_methods

    def test_has_launch_method(self, executor_methods: set[str]) -> None:
        assert "launch" in executor_methods

    def test_has_wait_method(self, executor_methods: set[str]) -> None:
        assert "wait" in executor_methods


class TestExecutorCommandBuilding:
    """Verify the _command method builds proper bash commands."""

    def test_source_uses_mflog_env_vars(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "export_mflog_env_vars" in source

    def test_source_uses_bash_capture_logs(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "bash_capture_logs" in source

    def test_source_uses_bash_save_logs(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "BASH_SAVE_LOGS" in source

    def test_source_creates_logs_dir(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "mkdir -p" in source

    def test_source_preserves_exit_code(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "exit $c" in source


class TestExecutorEnvVars:
    """Verify environment variable assembly."""

    def test_includes_code_package_vars(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "METAFLOW_CODE_METADATA" in source
        assert "METAFLOW_CODE_SHA" in source
        assert "METAFLOW_CODE_URL" in source
        assert "METAFLOW_CODE_DS" in source

    def test_includes_sandbox_workload_marker(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "METAFLOW_SANDBOX_WORKLOAD" in source

    def test_includes_credential_forwarding(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "AWS_ACCESS_KEY_ID" in source
        assert "AWS_SECRET_ACCESS_KEY" in source
        assert "AWS_SESSION_TOKEN" in source
        assert "DAYTONA_API_KEY" in source
        assert "E2B_API_KEY" in source

    def test_includes_datastore_vars(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "METAFLOW_DEFAULT_DATASTORE" in source
        assert "METAFLOW_USER" in source
        assert "METAFLOW_CONDA" in source

    def test_supports_force_local_metadata_flag(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "proxy_service_metadata = DEFAULT_METADATA == \"service\"" in source
        assert '"METAFLOW_DEFAULT_METADATA": (' in source

    def test_skips_session_token_for_r2_by_default(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "cloudflarestorage.com" in source
        assert "METAFLOW_SANDBOX_FORWARD_AWS_SESSION_TOKEN" in source
        assert "METAFLOW_SANDBOX_R2_WORKER_COUNT" in source
        assert "METAFLOW_S3_WORKER_COUNT" in source

    def test_stashes_backend_keys_under_metaflow_namespace(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "METAFLOW_DAYTONA_API_KEY" in source
        assert "METAFLOW_E2B_API_KEY" in source

    def test_auto_stages_micromamba_with_opt_out(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "METAFLOW_SANDBOX_STAGE_MICROMAMBA" in source
        assert "if stage_cfg in (\"0\", \"false\", \"no\", \"off\")" in source
        assert "_is_compatible_linux_micromamba" in source
        assert "_auto_download_micromamba" in source
        assert "METAFLOW_SANDBOX_AUTO_DOWNLOAD_MICROMAMBA" in source
        assert "METAFLOW_SANDBOX_MICROMAMBA_CACHE_DIR" in source
        assert "_elf_arch" in source
        assert "_target_linux_arch" in source
        assert "optional" in source


class TestExecutorLaunch:
    """Verify the launch method uses backend correctly."""

    def test_source_calls_backend_create(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "self._backend.create" in source

    def test_source_calls_backend_exec_script_streaming(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "self._backend.exec_script_streaming" in source

    def test_source_calls_backend_upload(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "self._backend.upload" in source

    def test_source_uses_get_backend(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "get_backend" in source

    def test_source_supports_staged_uploads(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "METAFLOW_SANDBOX_UPLOADS" in source
        assert "METAFLOW_SANDBOX_STAGE_MICROMAMBA" in source
        assert "METAFLOW_SANDBOX_MAX_INFRA_RETRIES" in source
        assert "METAFLOW_SANDBOX_DEBUG" in source
        assert "export PATH=" in source
        assert "_STAGING_BIN_DIR" in source


class TestExecutorWait:
    """Verify the wait method handles success/failure."""

    def test_source_cleans_up_sandbox(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "self._backend.destroy" in source

    def test_source_raises_on_failure(self) -> None:
        source = EXECUTOR_FILE.read_text()
        assert "SandboxException" in source
        assert "exit code" in source
