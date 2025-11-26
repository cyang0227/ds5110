"""
Helpers for ensuring project modules are importable when running scripts directly.
"""

from __future__ import annotations

import sys
from pathlib import Path


def add_project_root(relative_to: str, levels: int) -> Path:
    """
    Ensure the project root is on sys.path and return it.

    Parameters
    ----------
    relative_to : str
        __file__ path of the caller (ensure pass __file__).
    levels : int
        Number of parent directories to ascend from relative_to.
    """
    caller = Path(relative_to).resolve()
    if levels < 0 or levels >= len(caller.parents):
        raise ValueError("levels must reference a valid parent directory.")

    target = caller.parents[levels]
    if str(target) not in sys.path:
        sys.path.append(str(target))
    return target
