"""
py-ipfs-lite HTTP API.

The API is organised as domain routers under
:mod:`py_ipfs_lite.api.routers`; this package exposes the assembled
FastAPI ``app`` (and ``create_app`` for custom builds).
"""

from py_ipfs_lite.api.main import app, create_app

__all__ = ["app", "create_app"]
