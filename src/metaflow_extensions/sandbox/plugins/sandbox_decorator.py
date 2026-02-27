"""Metaflow step decorators for sandbox execution.

Layer: Metaflow Integration
May only import from: sandrun, .backend, .backends (registry)

Provides ``@sandbox``, ``@daytona``, and ``@e2b`` decorators that execute
Metaflow steps inside remote sandbox environments. Follows the same
lifecycle-hook pattern as ``@batch`` and ``@kubernetes``.

Usage:
    @sandbox(backend="daytona", cpu=2, memory=4096)
    @step
    def my_step(self):
        ...

    # Or use the backend-specific alias:
    @daytona(cpu=2)
    @step
    def my_step(self):
        ...
"""

from __future__ import annotations

import os
import platform
import sys
from importlib import import_module
from pathlib import Path
from typing import Any
from typing import ClassVar

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

_DEFAULT_BACKEND = os.environ.get("METAFLOW_SANDBOX_BACKEND", "daytona")
_BACKEND_RUNTIME_PYPI_PINS = {
    "daytona": ("daytona", ">=0.1"),
    "e2b": ("e2b-code-interpreter", ">=1.0"),
}
_SANDBOX_RUNTIME_PYPI_PINS = {
    "requests": ">=2.21.0",
}
_SANDBOX_REMOTE_COMMAND_ALIASES = ("sandbox", "daytona", "e2b", "boxlite")
_BACKEND_AUTH_ENV_VARS = ("DAYTONA_API_KEY", "DAYTONA_API_URL", "E2B_API_KEY")
_INITIAL_BACKEND_AUTH_ENV = {
    k: v for k, v in ((var, os.environ.get(var)) for var in _BACKEND_AUTH_ENV_VARS) if v
}
_BACKEND_AUTH_ALIAS_ENV = {
    "DAYTONA_API_KEY": "METAFLOW_DAYTONA_API_KEY",
    "E2B_API_KEY": "METAFLOW_E2B_API_KEY",
}


def _default_target_platform() -> str:
    machine = platform.machine().lower()
    if machine in ("aarch64", "arm64"):
        return "linux-aarch64"
    return "linux-64"


def _ensure_conda_remote_command_aliases() -> None:
    """Ensure nflxext conda remote-command checks include sandbox backends.

    ``CONDA_REMOTE_COMMANDS`` in nflx-extensions gates the target-arch
    selection in ``extract_merged_reqs_for_step``.  Without this patch,
    sandbox steps would receive a native-arch (macOS) ``ResolvedEnvironment``
    instead of a ``linux-64`` one.  Must be kept.
    """
    module_names = (
        "metaflow_extensions.netflix_ext.plugins.conda.conda_environment",
        "metaflow_extensions.netflix_ext.plugins.conda.conda_step_decorator",
    )
    for module_name in module_names:
        try:
            module = import_module(module_name)
        except Exception:
            continue
        current = tuple(getattr(module, "CONDA_REMOTE_COMMANDS", ()))
        merged = tuple(dict.fromkeys([*current, *_SANDBOX_REMOTE_COMMAND_ALIASES]))
        module.CONDA_REMOTE_COMMANDS = merged


def _get_resolved_package_specs(
    environment: Any,
    flow: Any,
    datastore_type: str,
    step_name: str,
) -> tuple[list[Any], str]:
    """Return (PackageSpec list, target_arch) from a nflx CondaEnvironment.

    Returns an empty list when nflx-extensions is not installed or the
    step has no conda/pypi dependencies.  The caller falls back to the
    classic ``bootstrap_commands()`` path in that case.
    """
    try:
        from metaflow_extensions.netflix_ext.plugins.conda.conda_environment import (
            CondaEnvironment,
        )
    except ImportError:
        return [], "linux-64"

    if not isinstance(environment, CondaEnvironment) or environment.conda is None:
        return [], "linux-64"

    try:
        _, arch, _, resolved_env = CondaEnvironment.extract_merged_reqs_for_step(
            environment.conda,
            flow,
            datastore_type,
            step_name,
        )
    except Exception:
        return [], "linux-64"

    if resolved_env is None:
        return [], arch or "linux-64"

    from sandrun._types import PackageSpec

    specs = [
        PackageSpec(
            url=spec.url,
            filename=spec.filename,
            pkg_type=spec.TYPE,
            hashes=dict(spec.pkg_hashes),
            is_real_url=getattr(spec, "is_real_url", True),
            url_format=getattr(spec, "url_format", "") or "",
            environment_marker=getattr(spec, "environment_marker", None),
        )
        for spec in resolved_env.packages
        if getattr(spec, "is_real_url", True)
    ]
    return specs, arch or "linux-64"


class SandboxException(MetaflowException):
    headline = "Sandbox error"


class SandboxDecorator(StepDecorator):
    """Run a Metaflow step inside a fast, isolated sandbox.

    Supports pluggable backends (Daytona, E2B, etc.) selected via
    the ``backend`` parameter or METAFLOW_SANDBOX_BACKEND env var.
    """

    name = "sandbox"

    defaults: ClassVar[dict[str, Any]] = {
        "backend": None,  # resolved at runtime from env or "daytona"
        "cpu": 1,
        "memory": 1024,
        "gpu": None,
        "image": None,
        "timeout": 600,
        "executable": None,
        "env": {},
    }
    supports_conda_environment = True
    target_platform = os.environ.get(
        "METAFLOW_SANDBOX_TARGET_PLATFORM", _default_target_platform()
    )

    # Class-level code-package state (shared across all instances,
    # uploaded once per flow run — same pattern as BatchDecorator).
    package_metadata: ClassVar[str | None] = None
    package_url: ClassVar[str | None] = None
    package_sha: ClassVar[str | None] = None
    package_local_path: ClassVar[str | None] = None

    # Class-level dep-staging state: step_name -> staging_dir path.
    # Populated by _prepare_deps_once() in runtime_task_created.
    _prepared_deps: ClassVar[dict[str, str]] = {}

    def step_init(
        self,
        flow: Any,
        graph: Any,
        step_name: str,
        decorators: Any,
        environment: Any,
        flow_datastore: Any,
        logger: Any,
    ) -> None:
        _ensure_conda_remote_command_aliases()

        self._backend_name = self.attributes.get("backend") or _DEFAULT_BACKEND
        self._step_name = step_name
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.logger = logger
        self.attributes.setdefault("env", {})

        for src, dst in _BACKEND_AUTH_ALIAS_ENV.items():
            val = os.environ.get(src) or _INITIAL_BACKEND_AUTH_ENV.get(src)
            if val and not os.environ.get(dst):
                os.environ[dst] = val

        conda_deco = next((d for d in decorators if d.name == "conda"), None)
        pypi_deco = next((d for d in decorators if d.name == "pypi"), None)

        runtime_pin = _BACKEND_RUNTIME_PYPI_PINS.get(self._backend_name)
        if runtime_pin and pypi_deco is not None:
            for package_name, version_spec in _SANDBOX_RUNTIME_PYPI_PINS.items():
                pypi_deco.attributes.setdefault("packages", {}).setdefault(
                    package_name, version_spec
                )
            package_name, version_spec = runtime_pin
            pypi_deco.attributes.setdefault("packages", {}).setdefault(
                package_name, version_spec
            )
        elif runtime_pin and conda_deco is not None:
            for package_name, version_spec in _SANDBOX_RUNTIME_PYPI_PINS.items():
                conda_deco.attributes.setdefault("pip_packages", {}).setdefault(
                    package_name, version_spec
                )
            package_name, version_spec = runtime_pin
            conda_deco.attributes.setdefault("pip_packages", {}).setdefault(
                package_name, version_spec
            )

        # Preserve backend auth env vars through step-runtime transitions.
        for var in _BACKEND_AUTH_ENV_VARS:
            val = os.environ.get(var)
            if val:
                self.attributes["env"].setdefault(var, val)

        # Sandbox backends require a remote datastore for Metaflow artifacts
        # (steps write outputs to the datastore).  Code package delivery no
        # longer requires S3 (TarballStager delivers via backend.upload()),
        # but artifact persistence still does.
        if flow_datastore.TYPE == "local":
            raise SandboxException(
                f"@{self.name} requires a remote datastore (s3, azure, gs). "
                "Configure with: METAFLOW_DEFAULT_DATASTORE=s3\n"
                "See https://docs.metaflow.org/scaling/remote-tasks/introduction"
            )

    def runtime_init(self, flow: Any, graph: Any, package: Any, run_id: str) -> None:
        """Store flow-level state needed for code-package upload."""
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self,
        task_datastore: Any,
        task_id: str,
        split_index: Any,
        input_paths: Any,
        is_cloned: bool,
        ubf_context: Any,
    ) -> None:
        """Upload the code package and prepare dep staging (once per step)."""
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)
            self._prepare_deps_once(self._step_name)

    def _prepare_deps_once(self, step_name: str) -> None:
        """Download + locally stage dep packages for *step_name*.

        Keyed by step name so each step's unique conda environment is
        handled independently.  Idempotent — subsequent calls for the
        same step (e.g. foreach tasks) reuse the existing staging dir.
        """
        if step_name in SandboxDecorator._prepared_deps:
            return

        specs, target_arch = _get_resolved_package_specs(
            self.environment,
            self.flow,
            self.flow_datastore.TYPE,
            step_name,
        )
        if not specs:
            return

        from sandrun.installer import CondaOfflineInstaller

        installer = CondaOfflineInstaller()
        try:
            installer.prepare(specs, target_arch)
        except Exception as e:
            # Dep preparation failures are non-fatal — fall back to bootstrap_commands().
            self.logger(
                f"[sandbox] Offline dep staging failed for step '{step_name}': {e}. "
                "Falling back to bootstrap_commands().",
                head="",
                bad=False,
            )
            return

        SandboxDecorator._prepared_deps[step_name] = installer._staging_dir

    def runtime_step_cli(
        self,
        cli_args: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
    ) -> None:
        """Redirect execution through ``sandbox step`` CLI command."""
        if os.environ.get("METAFLOW_SANDBOX_WORKLOAD"):
            return

        if retry_count <= max_user_code_retries:
            cli_args.commands = ["sandbox", "step"]
            cli_args.command_args.append(self.package_metadata)
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            _skip_keys = {"env"}
            cli_args.command_options.update(
                {k: v for k, v in self.attributes.items() if k not in _skip_keys}
            )
            # Serialize user env vars as repeated --env-var KEY=VALUE
            user_env = dict(self.attributes.get("env") or {})
            for var in _BACKEND_AUTH_ENV_VARS:
                val = os.environ.get(var) or _INITIAL_BACKEND_AUTH_ENV.get(var)
                if val:
                    user_env.setdefault(var, val)
            for src, dst in _BACKEND_AUTH_ALIAS_ENV.items():
                val = os.environ.get(dst) or (
                    os.environ.get(src) or _INITIAL_BACKEND_AUTH_ENV.get(src)
                )
                if val:
                    user_env.setdefault(dst, val)
            if user_env:
                cli_args.command_options["env-var"] = [
                    f"{k}={v}" for k, v in user_env.items()
                ]

            # Pass code-package local path for TarballStager delivery.
            if SandboxDecorator.package_local_path:
                cli_args.command_options[
                    "code-package-local-path"
                ] = SandboxDecorator.package_local_path

            # Pass dep staging dir for offline DepInstaller.
            staging_dir = SandboxDecorator._prepared_deps.get(self._step_name)
            if staging_dir:
                cli_args.command_options["deps-staging-dir"] = staging_dir

            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: Any,
        metadata: Any,
        run_id: str,
        task_id: str,
        flow: Any,
        graph: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
        inputs: Any,
    ) -> None:
        """Runs inside the sandbox. Emit execution metadata."""
        self.metadata = metadata
        self.task_datastore = task_datastore

        if os.environ.get("METAFLOW_SANDBOX_WORKLOAD"):
            from metaflow.metadata_provider import MetaDatum

            meta = {
                "sandbox-backend": os.environ.get("METAFLOW_SANDBOX_BACKEND", ""),
                "sandbox-id": os.environ.get("METAFLOW_SANDBOX_ID", ""),
            }
            entries = [
                MetaDatum(
                    field=k,
                    value=v,
                    type=k,
                    tags=[f"attempt_id:{retry_count}"],
                )
                for k, v in meta.items()
            ]
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self,
        step_name: str,
        flow: Any,
        graph: Any,
        is_task_ok: bool,
        retry_count: int,
        max_retries: int,
    ) -> None:
        """Sync local metadata from datastore when running in sandbox."""
        if (
            os.environ.get("METAFLOW_SANDBOX_WORKLOAD")
            and hasattr(self, "metadata")
            and self.metadata.TYPE == "local"
        ):
            from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
            from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

            sync_local_metadata_to_datastore(
                DATASTORE_LOCAL_DIR, self.task_datastore
            )

    @classmethod
    def _save_package_once(cls, flow_datastore: Any, package: Any) -> None:
        """Upload code package to remote datastore and save a local copy.

        Always stores on ``SandboxDecorator`` (the base class) so that
        subclasses (DaytonaDecorator, E2BDecorator) share one upload
        even if a flow mixes backends across steps.

        Local copy (package_local_path) enables TarballStager to deliver
        the code package via backend.upload() without requiring S3.
        """
        if SandboxDecorator.package_url is None:
            import tempfile

            # Save locally for TarballStager delivery.
            fd, local_path = tempfile.mkstemp(suffix=".tar", prefix="mf-sandbox-code-")
            try:
                import os

                with os.fdopen(fd, "wb") as f:
                    f.write(package.blob)
            except Exception:
                import os

                try:
                    os.unlink(local_path)
                except OSError:
                    pass
                raise
            SandboxDecorator.package_local_path = local_path

            # Also upload to the remote datastore (for bootstrap_commands fallback
            # and Metaflow's internal bookkeeping).
            url, sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
            SandboxDecorator.package_url = url
            SandboxDecorator.package_sha = sha
            SandboxDecorator.package_metadata = package.package_metadata


class DaytonaDecorator(SandboxDecorator):
    """Shorthand: @daytona is equivalent to @sandbox(backend="daytona")."""

    name = "daytona"
    defaults: ClassVar[dict[str, Any]] = {
        **SandboxDecorator.defaults,
        "backend": "daytona",
    }


class E2BDecorator(SandboxDecorator):
    """Shorthand: @e2b is equivalent to @sandbox(backend="e2b")."""

    name = "e2b"
    defaults: ClassVar[dict[str, Any]] = {
        **SandboxDecorator.defaults,
        "backend": "e2b",
    }


class BoxliteDecorator(SandboxDecorator):
    """Shorthand: @boxlite is equivalent to @sandbox(backend="boxlite").

    Runs the step in a local microVM via boxlite (KVM on Linux, HVF on macOS).
    No cloud account or API key required.
    """

    name = "boxlite"
    defaults: ClassVar[dict[str, Any]] = {
        **SandboxDecorator.defaults,
        "backend": "boxlite",
    }
