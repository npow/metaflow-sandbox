"""Boxlite sandbox backend — re-exported from sandrun.

Layer: Concrete Backend
May only import from: sandrun.backends.boxlite

Install: pip install metaflow-sandbox[boxlite]
Docs:    https://github.com/boxlite-ai/boxlite
"""

from sandrun.backends.boxlite import BoxliteBackend as BoxliteBackend

__all__ = ["BoxliteBackend"]
