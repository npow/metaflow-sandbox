"""Abstract sandbox backend interface — re-exported from sandrun.

Layer: Core Abstraction
May only import from: sandrun.backend

This module re-exports the canonical types from ``sandrun.backend`` so
that existing imports from ``metaflow_extensions.sandbox.plugins.backend``
continue to work without modification.
"""

from sandrun.backend import ExecResult as ExecResult
from sandrun.backend import Resources as Resources
from sandrun.backend import SandboxBackend as SandboxBackend
from sandrun.backend import SandboxConfig as SandboxConfig

__all__ = ["ExecResult", "Resources", "SandboxBackend", "SandboxConfig"]
