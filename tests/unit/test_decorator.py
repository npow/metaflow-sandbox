"""Unit tests for sandbox decorators.

These tests avoid importing metaflow directly (which fails in dev
due to namespace package conflicts). Instead they test the decorator
class behavior that doesn't require metaflow runtime.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DECORATOR_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "metaflow_extensions"
    / "sandbox"
    / "plugins"
    / "sandbox_decorator.py"
)


class TestDecoratorStructure:
    """Structural tests on the decorator source file."""

    def test_file_exists(self) -> None:
        assert DECORATOR_FILE.exists()

    def test_has_sandbox_decorator_class(self) -> None:
        tree = ast.parse(DECORATOR_FILE.read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "SandboxDecorator" in class_names

    def test_has_daytona_decorator_class(self) -> None:
        tree = ast.parse(DECORATOR_FILE.read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "DaytonaDecorator" in class_names

    def test_has_e2b_decorator_class(self) -> None:
        tree = ast.parse(DECORATOR_FILE.read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "E2BDecorator" in class_names

    def test_has_sandbox_exception_class(self) -> None:
        tree = ast.parse(DECORATOR_FILE.read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "SandboxException" in class_names


class TestDecoratorLifecycleMethods:
    """Verify that all required lifecycle hooks are defined."""

    @pytest.fixture()
    def methods(self) -> set[str]:
        tree = ast.parse(DECORATOR_FILE.read_text())
        methods: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "SandboxDecorator":
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        methods.add(item.name)
        return methods

    def test_has_step_init(self, methods: set[str]) -> None:
        assert "step_init" in methods

    def test_has_runtime_init(self, methods: set[str]) -> None:
        assert "runtime_init" in methods

    def test_has_runtime_task_created(self, methods: set[str]) -> None:
        assert "runtime_task_created" in methods

    def test_has_runtime_step_cli(self, methods: set[str]) -> None:
        assert "runtime_step_cli" in methods

    def test_has_task_pre_step(self, methods: set[str]) -> None:
        assert "task_pre_step" in methods

    def test_has_task_finished(self, methods: set[str]) -> None:
        assert "task_finished" in methods

    def test_has_save_package_once(self, methods: set[str]) -> None:
        assert "_save_package_once" in methods


class TestDecoratorCliRedirect:
    """Verify the runtime_step_cli method redirects to sandbox step."""

    def test_source_contains_sandbox_step_redirect(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert '"sandbox"' in source
        assert '"step"' in source
        assert 'cli_args.commands = ["sandbox", "step"]' in source

    def test_source_appends_package_args(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "self.package_metadata" in source
        assert "self.package_sha" in source
        assert "self.package_url" in source

    def test_source_preserves_backend_auth_env_vars(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "_BACKEND_AUTH_ENV_VARS" in source
        assert "DAYTONA_API_KEY" in source
        assert "E2B_API_KEY" in source
        assert "METAFLOW_DAYTONA_API_KEY" in source
        assert "METAFLOW_E2B_API_KEY" in source


class TestDatastoreValidation:
    """Verify that step_init rejects local datastore."""

    def test_source_checks_local_datastore(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert 'flow_datastore.TYPE == "local"' in source
        assert "remote datastore" in source


class TestClassLevelPackageState:
    """Verify class-level package_url/sha/metadata pattern."""

    def test_has_class_level_package_vars(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "package_metadata" in source
        assert "package_url" in source
        assert "package_sha" in source

    def test_has_class_level_package_local_path(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "package_local_path" in source

    def test_save_package_once_saves_locally(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "package_local_path" in source
        assert "tempfile" in source
        assert "package.blob" in source

    def test_has_class_level_prepared_deps(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "_prepared_deps" in source


class TestDepInstallerWiring:
    """Verify dep installer preparation and CLI option injection."""

    def test_has_prepare_deps_once_method(self) -> None:
        tree = ast.parse(DECORATOR_FILE.read_text())
        methods: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "SandboxDecorator":
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        methods.add(item.name)
        assert "_prepare_deps_once" in methods

    def test_prepare_deps_once_called_from_runtime_task_created(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "_prepare_deps_once" in source
        assert "runtime_task_created" in source

    def test_source_uses_conda_offline_installer(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "CondaOfflineInstaller" in source

    def test_source_calls_installer_prepare(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "installer.prepare" in source

    def test_prepare_deps_keyed_by_step_name(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "_prepared_deps" in source
        assert "step_name" in source

    def test_prep_failure_is_nonfatal(self) -> None:
        """Dep preparation failures must be non-fatal — fall back to bootstrap."""
        source = DECORATOR_FILE.read_text()
        # Exception caught and logged, not re-raised
        assert "Falling back to bootstrap_commands" in source

    def test_runtime_step_cli_passes_deps_staging_dir(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "deps-staging-dir" in source

    def test_runtime_step_cli_passes_code_package_local_path(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "code-package-local-path" in source

    def test_source_calls_get_resolved_package_specs(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "_get_resolved_package_specs" in source

    def test_get_resolved_package_specs_uses_extract_merged_reqs(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "extract_merged_reqs_for_step" in source

    def test_get_resolved_package_specs_converts_to_package_spec(self) -> None:
        source = DECORATOR_FILE.read_text()
        assert "PackageSpec" in source
