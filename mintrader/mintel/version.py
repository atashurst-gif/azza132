"""What code is actually running.

The status page shows this stamp so "is the new build in place?" is a fact
you can read off the page, not a guess. It is computed once, when the
process starts, from every source file of the package - so it describes
the code this process loaded, not whatever is on disk now.
"""
from __future__ import annotations

import hashlib
from pathlib import Path


def running_stamp(root: Path | None = None) -> str:
    root = Path(root or Path(__file__).resolve().parent)
    h = hashlib.sha256()
    for p in sorted(root.rglob("*.py")):
        try:
            h.update(str(p.relative_to(root)).encode())
            h.update(p.read_bytes())
        except OSError:
            pass
    return h.hexdigest()[:10]


RUNNING_STAMP = running_stamp()
