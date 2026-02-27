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

# sandrun is the canonical home for SandboxBackend and the backend implementations.
from sandrun.backend import SandboxBackend
from sandrun.backends import _BACKENDS

SANDRUN_BACKENDS_DIR = (
    Path(__file__).resolve().parents[2]
    / "packages"
    / "sandrun"
    / "src"
    / "sandrun"
    / "backends"
)

# metaflow-sandbox re-exports from sandrun; structural tests check both layers.
MF_PLUGINS_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "metaflow_extensions"
    / "sandbox"
    / "plugins"
)
MF_BACKENDS_DIR = MF_PLUGINS_DIR / "backends"


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
    def test_sandrun_backend_module_exists(self, name: str) -> None:
        backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
        assert backend_file.exists(), (
            f"Backend '{name}' registered in _BACKENDS but "
            f"{backend_file} does not exist."
        )

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_metaflow_sandbox_reexport_exists(self, name: str) -> None:
        reexport_file = MF_BACKENDS_DIR / f"{name}.py"
        assert reexport_file.exists(), (
            f"Backend '{name}' is missing metaflow-sandbox re-export at {reexport_file}."
        )

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_backend_importable_or_file_exists(self, name: str) -> None:
        module_path, class_name = _BACKENDS[name]
        try:
            mod = importlib.import_module(
                module_path, package="sandrun.backends"
            )
            assert hasattr(mod, class_name), (
                f"Module {module_path} does not export class '{class_name}'."
            )
        except ImportError:
            # SDK not installed — just verify the file exists.
            backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
            assert backend_file.exists()


@pytest.mark.structural
class TestBackendImplementations:
    """Every sandrun backend must implement all abstract methods."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_implements_all_abstract_methods(self, name: str) -> None:
        backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
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
                            f"See sandrun/backend.py for the required interface."
                        )


@pytest.mark.structural
class TestLayerBoundaries:
    """sandrun backends must not import from the decorator or executor layers."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_sandrun_no_upward_imports(self, name: str) -> None:
        backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        imports = _parse_imports(backend_file)
        forbidden = {
            "metaflow_extensions.sandbox.plugins.sandbox_decorator",
            "metaflow_extensions.sandbox.plugins.sandbox_executor",
            "metaflow_extensions.sandbox.plugins.sandbox_cli",
        }
        violations = imports & forbidden
        assert not violations, (
            f"sandrun backend '{name}' imports from the metaflow integration layer: "
            f"{violations}. Backends may only import from sandrun.backend."
        )

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_sandrun_no_metaflow_imports(self, name: str) -> None:
        """sandrun backends must have zero Metaflow dependency."""
        backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        imports = _parse_imports(backend_file)
        metaflow_imports = {i for i in imports if i.startswith("metaflow")}
        assert not metaflow_imports, (
            f"sandrun backend '{name}' imports from Metaflow: {metaflow_imports}. "
            f"sandrun must have zero Metaflow dependency."
        )


@pytest.mark.structural
class TestErrorMessages:
    """Backend error messages must include remediation instructions."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_install_hint_exists(self, name: str) -> None:
        backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        source = backend_file.read_text()
        assert "_INSTALL_HINT" in source, (
            f"Backend '{name}' is missing _INSTALL_HINT. Every backend must "
            f"define an _INSTALL_HINT string with: (1) the pip install command, "
            f"(2) required env vars, (3) a doc link."
        )
        assert "pip install" in source, (
            f"Backend '{name}' _INSTALL_HINT must include a pip install command."
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
        assert "sandbox" in names

    def test_step_decorators_desc_has_all_aliases(self) -> None:
        from metaflow_extensions.sandbox.plugins import STEP_DECORATORS_DESC

        names = [name for name, _ in STEP_DECORATORS_DESC]
        for expected in ("sandbox", "daytona", "e2b"):
            assert expected in names, (
                f"STEP_DECORATORS_DESC is missing '{expected}' entry"
            )

    def test_cli_module_exists(self) -> None:
        cli_file = MF_PLUGINS_DIR / "sandbox_cli.py"
        assert cli_file.exists()

    def test_executor_module_exists(self) -> None:
        executor_file = MF_PLUGINS_DIR / "sandbox_executor.py"
        assert executor_file.exists()


@pytest.mark.structural
class TestDocstrings:
    """Every sandrun backend file must declare its layer in the module docstring."""

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_layer_declared_in_sandrun(self, name: str) -> None:
        backend_file = SANDRUN_BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Backend file {backend_file} not found")

        tree = ast.parse(backend_file.read_text())
        docstring = ast.get_docstring(tree)
        assert docstring is not None, (
            f"sandrun backend '{name}' is missing a module docstring."
        )
        assert "Layer:" in docstring, (
            f"sandrun backend '{name}' module docstring must declare its layer."
        )

    @pytest.mark.parametrize("name", list(_BACKENDS.keys()))
    def test_layer_declared_in_metaflow_reexport(self, name: str) -> None:
        backend_file = MF_BACKENDS_DIR / f"{name}.py"
        if not backend_file.exists():
            pytest.skip(f"Re-export file {backend_file} not found")

        tree = ast.parse(backend_file.read_text())
        docstring = ast.get_docstring(tree)
        assert docstring is not None, (
            f"metaflow-sandbox re-export '{name}' is missing a module docstring."
        )
        assert "Layer:" in docstring, (
            f"metaflow-sandbox re-export '{name}' module docstring must declare its layer."
        )


@pytest.mark.structural
class TestSandrunZeroMetaflowDependency:
    """The sandrun package must have zero Metaflow imports across all its modules."""

    def _sandrun_py_files(self) -> list[Path]:
        sandrun_src = (
            Path(__file__).resolve().parents[2]
            / "packages"
            / "sandrun"
            / "src"
            / "sandrun"
        )
        return list(sandrun_src.rglob("*.py"))

    def test_no_metaflow_imports_anywhere(self) -> None:
        violations: list[str] = []
        for py_file in self._sandrun_py_files():
            imports = _parse_imports(py_file)
            mf = {i for i in imports if i.startswith("metaflow")}
            if mf:
                violations.append(f"{py_file.name}: {mf}")
        assert not violations, (
            "sandrun has Metaflow imports — it must be zero-dependency:\n"
            + "\n".join(violations)
        )

    def test_no_metaflow_extensions_imports_anywhere(self) -> None:
        violations: list[str] = []
        for py_file in self._sandrun_py_files():
            imports = _parse_imports(py_file)
            mfe = {i for i in imports if i.startswith("metaflow_extensions")}
            if mfe:
                violations.append(f"{py_file.name}: {mfe}")
        assert not violations, (
            "sandrun has metaflow_extensions imports — it must be zero-dependency:\n"
            + "\n".join(violations)
        )
