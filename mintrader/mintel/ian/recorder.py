"""Record the normalised event stream so a session can be replayed exactly.

    data/ian/recordings/YYYY-MM-DD/<instrument>.jsonl.gz

One JSON object per line (the format of :func:`mintel.ian.book.to_dict`),
gzip-compressed, one file per instrument per UTC day of arrival. Feed-level
notices go to ``_feed.jsonl.gz``. Appending re-opens the file as a new gzip
member, which gzip readers join transparently, so a restart never corrupts a
day's file.

What is recorded is what the engine saw, in the order it saw it - including
the gaps and the out-of-order events - so a replay reproduces the same
decisions, data-quality states included.
"""
from __future__ import annotations

import gzip
import json
import logging
import re
from pathlib import Path
from .book import Event, received_at, to_dict

log = logging.getLogger("mintel.ian.recorder")

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_name(instrument: str) -> str:
    return _SAFE.sub("_", instrument or "_feed") or "_feed"


def recording_dir(data_dir: str | Path) -> Path:
    return Path(data_dir) / "ian" / "recordings"


class Recorder:
    def __init__(self, base_dir: str | Path, flush_every: int = 500):
        self.base = Path(base_dir)
        self.flush_every = max(1, int(flush_every))
        self._files: dict[tuple[str, str], gzip.GzipFile] = {}
        self.written = 0
        self.errors = 0
        self.last_error = ""

    def path_for(self, ev: Event) -> Path:
        day = received_at(ev).date().isoformat()
        return self.base / day / f"{safe_name(ev.instrument)}.jsonl.gz"

    def write(self, ev: Event) -> None:
        try:
            day = received_at(ev).date().isoformat()
            key = (day, ev.instrument or "_feed")
            fh = self._files.get(key)
            if fh is None:
                for k in [k for k in self._files if k[0] != day]:       # a new day: close yesterday's files
                    self._files.pop(k).close()
                p = self.base / day / f"{safe_name(ev.instrument)}.jsonl.gz"
                p.parent.mkdir(parents=True, exist_ok=True)
                fh = gzip.open(p, "at", encoding="utf-8")
                self._files[key] = fh
            fh.write(json.dumps(to_dict(ev), separators=(",", ":")) + "\n")
            self.written += 1
            if self.written % self.flush_every == 0:
                self.flush()
        except Exception as exc:                       # recording must never stop trading decisions
            self.errors += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            if self.errors <= 3:
                log.warning("recorder: %s", self.last_error)

    def write_many(self, events) -> None:
        for ev in events:
            self.write(ev)

    def flush(self) -> None:
        for fh in self._files.values():
            try:
                fh.flush()
            except Exception:
                pass

    def close(self) -> None:
        for fh in self._files.values():
            try:
                fh.close()
            except Exception:
                pass
        self._files.clear()


def record_events(events, base_dir: str | Path) -> list[Path]:
    """Write a finished list of events (a scenario, a download) and return
    the files written."""
    r = Recorder(base_dir)
    paths = set()
    for ev in events:
        r.write(ev)
        paths.add(r.path_for(ev))
    r.close()
    return sorted(paths)
