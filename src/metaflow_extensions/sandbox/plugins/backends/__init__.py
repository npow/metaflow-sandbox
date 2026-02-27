"""Sandbox backend registry — re-exported from sandrun.

Layer: Backend Resolution
May only import from: sandrun.backends

This module re-exports the registry from ``sandrun.backends`` so that
existing imports from ``metaflow_extensions.sandbox.plugins.backends``
continue to work without modification.
"""

from sandrun.backends import _BACKENDS as _BACKENDS
from sandrun.backends import get_backend as get_backend

__all__ = ["_BACKENDS", "get_backend"]
