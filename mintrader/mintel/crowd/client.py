"""A small client for the Coinversa Pulse REST API.

The same backend the Claude connector uses: ``https://api.coinversa.ai``,
``/api/public/v1``, the key in an ``X-API-Key`` header. Keys start with
``cvsa_`` and are issued at coinversa.ai/developers. The key is read from
``secrets.json`` next to ``config.json`` (``"coinversa_api_key"``) and is
never written anywhere else.

Only the standard library: this runs on the Mac side of the bridge, where
nothing but Python is guaranteed.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Optional

DEFAULT_BASE = "https://api.coinversa.ai/api/public/v1"
KEY_NAME = "coinversa_api_key"


class CoinversaError(Exception):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


def normalize_coin(raw: str) -> str:
    """As the connector does: a dex prefix lower-case, the coin upper-case."""
    raw = (raw or "").strip()
    if ":" in raw:
        pre, coin = raw.split(":", 1)
        return pre.lower() + ":" + coin.upper()
    return raw.upper()


def read_api_key(data_dir: str | Path) -> str:
    try:
        s = json.loads((Path(data_dir) / "secrets.json").read_text())
        return str(s.get(KEY_NAME) or "") if isinstance(s, dict) else ""
    except Exception:
        return ""


def save_api_key(data_dir: str | Path, key: str) -> Path:
    """Write the key into secrets.json, keeping everything else in it."""
    p = Path(data_dir) / "secrets.json"
    existing: dict = {}
    if p.exists():
        try:
            existing = json.loads(p.read_text())
        except Exception:
            existing = {}
        if not isinstance(existing, dict):
            existing = {}
    existing[KEY_NAME] = key.strip()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, indent=2))
    tmp.replace(p)
    try:
        os.chmod(p, 0o600)
    except Exception:
        pass
    return p


class CoinversaClient:
    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE, timeout: float = 15.0,
                 opener: Optional[Callable] = None, sleep: Callable[[float], None] = time.sleep):
        if not api_key:
            raise CoinversaError("no Coinversa API key")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._open = opener or urllib.request.urlopen
        self._sleep = sleep
        self.calls = 0
        self.last_status = 0
        self.remaining: Optional[int] = None          # X-RateLimit-Remaining, when sent

    # -------------------------------------------------------------- http --
    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        url = self.base_url + path
        q = {k: _param(v) for k, v in (params or {}).items() if v not in (None, "")}
        if q:
            url += "?" + urllib.parse.urlencode(q)
        where = path + (("?" + urllib.parse.urlencode(q)) if q else "")
        last: Optional[Exception] = None
        for attempt in range(2):
            req = urllib.request.Request(url, headers={"X-API-Key": self.api_key, "Accept": "application/json",
                                                       "User-Agent": "mintel-crowd-fader/1"})
            try:
                self.calls += 1
                with self._open(req, timeout=self.timeout) as resp:
                    self.last_status = int(getattr(resp, "status", 200) or 200)
                    rem = resp.headers.get("X-RateLimit-Remaining") if hasattr(resp, "headers") else None
                    if rem is not None:
                        try:
                            self.remaining = int(rem)
                        except ValueError:
                            pass
                    body = resp.read()
                return json.loads(body.decode("utf-8")) if body else None
            except urllib.error.HTTPError as exc:
                self.last_status = exc.code
                detail = ""
                try:
                    raw = exc.read().decode("utf-8")
                    try:
                        d = json.loads(raw)
                        detail = str(d.get("detail") or d.get("error") or d.get("title") or d.get("message") or "")
                        extra = d.get("errors") or d.get("issues") or d.get("fields")
                        if extra:
                            detail = (detail + " " + json.dumps(extra)).strip()
                        if not detail:
                            detail = raw
                    except Exception:
                        detail = raw
                    detail = detail[:300]
                except Exception:
                    pass
                if exc.code == 429 and attempt == 0:
                    wait = 2.0
                    try:
                        wait = float(exc.headers.get("Retry-After") or 2.0)
                    except Exception:
                        pass
                    self._sleep(min(max(wait, 1.0), 30.0))
                    last = CoinversaError(f"rate limited: {detail or 'too many requests'}", 429)
                    continue
                if exc.code in (401, 403):
                    raise CoinversaError(f"API key rejected ({exc.code}): {detail or 'check the key in secrets.json'}", exc.code)
                if exc.code == 404:
                    raise CoinversaError(f"not found: {path} ({detail or 'check the coin symbol'})", 404)
                if exc.code >= 500 and attempt == 0:
                    self._sleep(2.0)
                    last = CoinversaError(f"server error {exc.code}: {detail}", exc.code)
                    continue
                raise CoinversaError(f"request failed ({exc.code}) on {where}: {detail}", exc.code)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last = CoinversaError(f"cannot reach Coinversa: {exc}")
                if attempt == 0:
                    self._sleep(2.0)
                    continue
        raise last or CoinversaError("request failed")

    # ------------------------------------------------------------- routes --
    def cohort_bias(self, coin: str) -> list[dict]:
        d = self._get(f"/live/cohort-bias/{urllib.parse.quote(normalize_coin(coin), safe=':')}")
        return _rows(d, ("rows", "data", "cohorts", "tiers"))

    def heatmap(self, coin: str, range_pct: float = 3.0, buckets: int = 20) -> dict:
        d = self._get(f"/live/liquidation-heatmap/{urllib.parse.quote(normalize_coin(coin), safe=':')}",
                      {"buckets": int(buckets), "range": range_pct})
        return d if isinstance(d, dict) else {}

    def long_short(self, coin: str) -> dict:
        d = self._get(f"/live/coins/{urllib.parse.quote(normalize_coin(coin), safe=':')}/long-short")
        return d if isinstance(d, dict) else {}

    def market_overview(self, dex: str = "xyz") -> dict:
        d = self._get("/pulse/market-overview", {"dex": dex})
        return d if isinstance(d, dict) else {}


def _param(v: Any) -> str:
    """Numbers the way a JavaScript client sends them: 3.0 is "3", not "3.0"
    (the API validates integers strictly and answers 422 otherwise)."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return str(int(v)) if v == int(v) else repr(v)
    return str(v)


def _rows(d: Any, keys: tuple[str, ...]) -> list[dict]:
    if isinstance(d, list):
        return [r for r in d if isinstance(r, dict)]
    if isinstance(d, dict):
        for k in keys:
            v = d.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
    return []
