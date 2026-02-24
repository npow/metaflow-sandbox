"""Structural tests that enforce architectural invariants.

These tests require no credentials or external services. They inspect
the source code and module structure to catch layer violations, missing
implementations, and documentation gaps mechanically.

Run: pytest tests/structural/ -m structural
"""

from __future__ import annotations

import ast
import importlib
import inspect
from pathlib import Path

import pytest

from metaflow_extensions.sandbox.plugins.backend import SandboxBackend
from metaflow_extensions.sandbox.plugins.backends import _BACKENDS

BACKENDS_DIR = (
    Path(__file__).resolve().parents[2]
    / "src" / "metaflow_extensions" / "sandbox" / "plugins" / "backends"
)
PLUGINS_DIR = BACKENDS_DIR.parent


def _get_abstract_methods() -> set[str]:
    """Return names of all abstract methods on SandboxBackend."""
    return {
        name
        for name, method in inspect.getmembers(SandboxBackend, predicate=inspect.isfunction)
        if getattr(method, "__isabstractmethod__", False)
    }


def _parse_imports(filepath: Path) -> set[str]:
    """Return all imported module names from a Python file."""
    tree = ast.parse(filepath.read_text())
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


# -- Tests ------------------------------------------------------------------


@pytest.mark.structural
class TestBackendRegistry:
    """Every backend in _BACKENDS must be importable and well-formed."""

    def test_registry_is_not_empty(self) -> None:
        assert len(_BACKENDS) > 0, (
            "No backends registered in _BACKENDS. "
            "See docs/adding-a-backend.md for how to add one."
        )

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_backend_module_exists(self, name: str) -> None:
        module_path, class_name = _BACKENDS[name]
        try:
            mod = importlib.import_module(
                module_path, package="metaflow_extensions.sandbox.plugins.backends"
            )
        except ImportError:
            # SDK not installed is OK — we just check the module file exists
            backend_file = BACKENDS_DIR / f"{name}.py"
            assert backend_file.exists(), (
                f"Backend '{name}' registered in _BACKENDS but "
                f"{backend_file} does not exist."
            )
            return
        assert hasattr(mod, class_name), (
            f"Module {module_path} does not export class '{class_name}'. "
            f"Check the class name in _BACKENDS."
        )


@pytest.mark.structural
class TestBackendImplementations:
    """Every backend must implement all abstract methods."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_implements_all_abstract_methods(self, name: str) -> None:
        backend_file = BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        tree = ast.parse(backend_file.read_text())
        required = _get_abstract_methods()

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                defined_methods = {
                    item.name
                    for item in node.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                }
                # Check if this class likely subclasses SandboxBackend
                for base in node.bases:
                    base_name = ""
                    if isinstance(base, ast.Name):
                        base_name = base.id
                    elif isinstance(base, ast.Attribute):
                        base_name = base.attr
                    if base_name == "SandboxBackend":
                        missing = required - defined_methods
                        assert not missing, (
                            f"Backend '{name}' class {node.name} is missing "
                            f"implementations for: {', '.join(sorted(missing))}. "
                            f"See backend.py for the required interface."
                        )


@pytest.mark.structural
class TestLayerBoundaries:
    """Backends must not import from the decorator layer."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_no_upward_imports(self, name: str) -> None:
        backend_file = BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        imports = _parse_imports(backend_file)
        forbidden = {"metaflow_extensions.sandbox.plugins.sandbox_decorator"}

        violations = imports & forbidden
        assert not violations, (
            f"Backend '{name}' imports from the decorator layer: {violations}. "
            f"This violates the layer boundary. Backends may only import "
            f"from ..backend (the ABC). See docs/architecture.md."
        )


@pytest.mark.structural
class TestErrorMessages:
    """Backend error messages must include remediation instructions."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_install_hint_exists(self, name: str) -> None:
        backend_file = BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        source = backend_file.read_text()
        assert "_INSTALL_HINT" in source, (
            f"Backend '{name}' is missing _INSTALL_HINT. Every backend must "
            f"define an _INSTALL_HINT string with: (1) the pip install command, "
            f"(2) required env vars, (3) a doc link. "
            f"See docs/adding-a-backend.md."
        )
        assert "pip install" in source, (
            f"Backend '{name}' _INSTALL_HINT must include a pip install command "
            f"so users know exactly how to fix the ImportError."
        )


@pytest.mark.structural
class TestPluginRegistration:
    """CLIS_DESC and STEP_DECORATORS_DESC must be well-formed."""

    def test_clis_desc_exists(self) -> None:
        from metaflow_extensions.sandbox.plugins import CLIS_DESC

        assert len(CLIS_DESC) > 0, "CLIS_DESC is empty — CLI commands are not registered."

    def test_clis_desc_sandbox_entry(self) -> None:
        from metaflow_extensions.sandbox.plugins import CLIS_DESC

        names = [name for name, _ in CLIS_DESC]
        assert "sandbox" in names, (
            "CLIS_DESC must contain a 'sandbox' entry pointing to sandbox_cli.cli"
        )

    def test_step_decorators_desc_has_all_aliases(self) -> None:
        from metaflow_extensions.sandbox.plugins import STEP_DECORATORS_DESC

        names = [name for name, _ in STEP_DECORATORS_DESC]
        for expected in ("sandbox", "daytona", "e2b"):
            assert expected in names, (
                f"STEP_DECORATORS_DESC is missing '{expected}' entry"
            )

    def test_cli_module_exists(self) -> None:
        cli_file = PLUGINS_DIR / "sandbox_cli.py"
        assert cli_file.exists(), (
            "sandbox_cli.py not found. The 'sandbox' CLI command "
            "requires this file to exist."
        )

    def test_executor_module_exists(self) -> None:
        executor_file = PLUGINS_DIR / "sandbox_executor.py"
        assert executor_file.exists(), (
            "sandbox_executor.py not found. The sandbox executor "
            "is required for remote step execution."
        )


@pytest.mark.structural
class TestDocstrings:
    """Every backend file must declare its layer in the module docstring."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_layer_declared(self, name: str) -> None:
        backend_file = BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        tree = ast.parse(backend_file.read_text())
        docstring = ast.get_docstring(tree)
        assert docstring is not None, (
            f"Backend '{name}' is missing a module docstring. "
            f"Start with: Layer: Concrete Backend"
        )
        assert "Layer:" in docstring, (
            f"Backend '{name}' module docstring must declare its layer. "
            f"Add 'Layer: Concrete Backend' to the docstring. "
            f"See docs/architecture.md for the layer diagram."
        )
