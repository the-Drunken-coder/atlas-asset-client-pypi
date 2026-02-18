"""Pytest helpers for the asset HTTP client package tests."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_project_on_path() -> None:
    """Guarantee the local src layout is at the front of sys.path."""
    src_dir = str(Path(__file__).resolve().parents[1] / "src")
    try:
        sys.path.remove(src_dir)
    except ValueError:
        pass
    sys.path.insert(0, src_dir)


_ensure_project_on_path()
