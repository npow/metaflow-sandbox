"""E2B sandbox backend — re-exported from sandrun.

Layer: Concrete Backend
May only import from: sandrun.backends.e2b

Install: pip install metaflow-sandbox[e2b]
Docs:    https://e2b.dev/docs
"""

from sandrun.backends.e2b import E2BBackend as E2BBackend

__all__ = ["E2BBackend"]
