"""Publish the research report to the GitHub ``reports`` branch:

    reports/rider-research/<date>/report.md
    reports/rider-research/<date>/summary.json
    reports/rider-research/<date>/trades.csv

It reuses mintel.ops.report_upload (the same repository, branch and token as
the nightly report). The token is read from secrets.json next to the config
("github_token") - never from the source. Two uploads at once (the nightly
report and this one) can collide on the branch: GitHub answers 409 or 422,
and the file's current sha is read again and the write retried, up to five
times.
"""
from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Callable, Optional

RETRY_CODES = (409, 422)


def put_with_retry(gh, branch: str, path: str, content: str, message: str, attempts: int = 5,
                   sleep: Callable[[float], None] = time.sleep) -> str:
    """'written' or 'unchanged'; raises after ``attempts`` failed tries."""
    last = ""
    for i in range(max(1, attempts)):
        code, existing = gh.call("GET", f"/repos/{gh.repo}/contents/{path}?ref={branch}")
        body = {"message": message, "branch": branch,
                "content": base64.b64encode(content.encode("utf-8")).decode("ascii")}
        if code == 200 and isinstance(existing, dict) and existing.get("sha"):
            raw = existing.get("content")
            if raw:
                try:
                    if base64.b64decode(raw.replace("\n", "")).decode("utf-8", "replace") == content:
                        return "unchanged"
                except Exception:
                    pass
            body["sha"] = existing["sha"]
        code, resp = gh.call("PUT", f"/repos/{gh.repo}/contents/{path}", body)
        if code in (200, 201):
            return "written"
        last = f"{code}: {(resp or {}).get('message', '') if isinstance(resp, dict) else resp}"
        if code in RETRY_CODES and i < attempts - 1:
            sleep(min(0.5 * (2 ** i), 8.0))          # someone else moved the branch: re-read the sha and retry
            continue
        break
    raise RuntimeError(f"could not write {path} ({last})")


def publish(files: dict, config_path: str, folder: str, *, repo: Optional[str] = None, branch: Optional[str] = None,
            opener: Optional[Callable] = None, attempts: int = 5, sleep: Callable[[float], None] = time.sleep,
            token: Optional[str] = None) -> list[str]:
    """``files``: {file name: text}. Returns the repository paths written."""
    from ...config import Config
    from ...ops.report_upload import GitHub, read_token
    tok = token if token is not None else read_token(config_path)
    if not tok:
        raise RuntimeError("no GitHub token in secrets.json - run: python -m mintel.ops.report_upload "
                           f"--config {config_path} --set-token")
    cfg = Config.load(config_path) if Path(config_path).exists() else Config()
    repo = repo or cfg.report_repo
    branch = branch or cfg.report_branch
    gh = GitHub(repo, tok, opener)
    gh.ensure_branch(branch)
    written = []
    for name, text in files.items():
        path = f"reports/rider-research/{folder}/{name}"
        put_with_retry(gh, branch, path, text, f"rider research {folder}: {name}", attempts, sleep)
        written.append(path)
    return written
