"""A fingerprint of the bridge's code, so a stale bridge can be recognised.

The bridge runs as a separate Windows process inside Wine. When the program
is upgraded, that process keeps running the OLD code unless something stops
it - and an old bridge answers new questions ("what did we earn today?")
with silence. The bridge reports the stamp of the code it LOADED; the
trader and the watchdog compare it with the code on disk.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

FILES = ("bridge_server.py", "mt5_adapter.py", "wire.py", "base.py")


def code_stamp(root: Path | None = None) -> str:
    root = Path(root or Path(__file__).resolve().parent)
    h = hashlib.sha256()
    for name in FILES:
        p = root / name
        try:
            h.update(name.encode())
            h.update(p.read_bytes())
        except OSError:
            h.update(b"missing")
    return h.hexdigest()[:12]
