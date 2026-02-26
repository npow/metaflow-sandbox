"""Metaflow step decorators for sandbox execution.

Layer: Metaflow Integration
May only import from: .backend, .backends (registry)

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
_SANDBOX_REMOTE_COMMAND_ALIASES = ("sandbox", "daytona", "e2b")
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
    """Ensure nflxext conda remote-command checks include sandbox backends."""
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
        setattr(module, "CONDA_REMOTE_COMMANDS", merged)


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
    package_metadata = None
    package_url = None
    package_sha = None

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

        # If a step uses @pypi and runs in a sandbox backend, ensure the backend SDK
        # is part of the resolved PyPI environment for that step.
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
            # Netflix @conda supports pip_packages; inject runtime deps there
            # so sandbox backends work without requiring users to specify them.
            for package_name, version_spec in _SANDBOX_RUNTIME_PYPI_PINS.items():
                conda_deco.attributes.setdefault("pip_packages", {}).setdefault(
                    package_name, version_spec
                )
            package_name, version_spec = runtime_pin
            conda_deco.attributes.setdefault("pip_packages", {}).setdefault(
                package_name, version_spec
            )

        # Preserve backend auth env vars through step-runtime transitions.
        # These are consumed by sandbox_cli *before* sandbox env injection.
        for var in _BACKEND_AUTH_ENV_VARS:
            val = os.environ.get(var)
            if val:
                self.attributes["env"].setdefault(var, val)

        # Sandbox backends require a remote datastore — the sandbox
        # cannot access the local filesystem.
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
        """Upload the code package once (class-level) per flow run."""
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self,
        cli_args: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
    ) -> None:
        """Redirect execution through ``sandbox step`` CLI command.

        After all user-code retries are exhausted Metaflow may fall back
        to local execution (e.g. ``@catch``), so we only redirect while
        ``retry_count <= max_user_code_retries``.
        """
        # Prevent recursive sandbox-in-sandbox routing when already running
        # inside a sandbox workload.
        if os.environ.get("METAFLOW_SANDBOX_WORKLOAD"):
            return

        if retry_count <= max_user_code_retries:
            cli_args.commands = ["sandbox", "step"]
            cli_args.command_args.append(self.package_metadata)
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            # Skip dict-valued attributes — they can't be serialized as
            # CLI options. User env vars are passed via --env-var instead.
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
        """Upload code package to remote datastore (once per flow run).

        Always stores on ``SandboxDecorator`` (the base class) so that
        subclasses (DaytonaDecorator, E2BDecorator) share one upload
        even if a flow mixes backends across steps.
        """
        if SandboxDecorator.package_url is None:
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
