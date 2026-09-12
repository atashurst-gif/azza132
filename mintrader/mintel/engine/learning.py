"""Historical learning.

Two distinct jobs, deliberately kept apart:

* :class:`HistoricalMatcher` answers "has this *kind* of setup worked before?"
  and returns a score plus a **confidence**.  Crucially, a market with no
  history is still eligible to trade - low sample size reduces confidence, it
  does not withhold permission.  The previous generation of this bot required
  markets to "earn the right" to be traded and that mistake is not repeated.

* :class:`MetaLabeller` is an optional, explainable model over tabular features
  (gradient-boosted trees) that predicts whether a *candidate the engine has
  already approved* will reach its objective.  Its output is one evidence
  component, it must be probability-calibrated, and it can never override a
  safety check.  Deep networks are deliberately not used: the feature set is
  small, tabular and needs to stay interpretable.
"""
from __future__ import annotations

import json
import logging
import math
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

from ..broker.base import Side
from .evidence import Family, SymbolContext

log = logging.getLogger("mintel.learning")


@dataclass
class MatchResult:
    score: float                 # 0..100, 50 = no opinion
    confidence: float            # 0..1
    note: str
    samples: int = 0
    width: str = "none"


class HistoricalMatcher:
    """Scores a candidate against comparable completed trades."""

    def __init__(self, journal, min_samples_full: int = 30):
        self.journal = journal
        self.min_samples_full = min_samples_full
        self._cache: dict[tuple, MatchResult] = {}

    def invalidate(self) -> None:
        self._cache.clear()

    def __call__(self, symbol: str, regime: str, tactic: str, side: int,
                 ctx: Optional[SymbolContext] = None) -> tuple[float, float, str]:
        r = self.evaluate(symbol, regime, tactic,
                          Side.BUY if side > 0 else Side.SELL)
        return r.score, r.confidence, r.note

    def evaluate(self, symbol: str, regime: str, tactic: str,
                 side: Side) -> MatchResult:
        key = (symbol, regime, tactic, side.value)
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        try:
            stats = self.journal.match_stats(symbol, regime, tactic, side)
        except Exception:
            stats = {}
        # Prefer the tightest match that has enough evidence to say anything.
        for width in ("exact", "tactic_regime", "tactic"):
            s = stats.get(width) or {}
            n = int(s.get("n") or 0)
            er = s.get("expectancy_r")
            if n >= 5 and er is not None:
                # Map expectancy in R to a 0..100 score, saturating around
                # +/-1R so one outlier trade cannot produce a 99.
                score = max(0.0, min(100.0, 50.0 + 40.0 * math.tanh(er / 0.6)))
                confidence = min(1.0, n / self.min_samples_full) * {
                    "exact": 1.0, "tactic_regime": 0.8, "tactic": 0.6}[width]
                wr = s.get("win_rate")
                note = (f"{n} comparable trades ({width.replace('_', '+')}): "
                        f"expectancy {er:+.2f}R"
                        + (f", {wr * 100:.0f}% win rate" if wr is not None else ""))
                res = MatchResult(round(score, 1), round(confidence, 3), note,
                                  n, width)
                self._cache[key] = res
                return res
        # No history: explicitly neutral with zero confidence, which shrinks the
        # family to exactly 50 in the aggregate and therefore changes nothing.
        res = MatchResult(50.0, 0.0,
                          "no comparable history yet - this does not block the "
                          "trade, it only means history adds nothing here", 0,
                          "none")
        self._cache[key] = res
        return res


# ---------------------------------------------------------- meta-labelling ----

FEATURES: tuple[str, ...] = (
    "structure", "momentum", "volatility", "participation", "session",
    "multi_tf", "cross_market", "news", "execution", "regime_conf",
    "headroom_atr", "range_consumed", "reward_risk", "opportunity",
    "cost_over_atr", "stop_atr", "hour_utc", "vol_percentile", "adx",
    "efficiency",
)


def features_from(state, ctx: Optional[SymbolContext] = None) -> dict:
    """Flatten a MarketState into the model's feature vector.

    Only quantities available *before* the entry are used.  Nothing derived
    from the outcome may appear here, or the model leaks the future and every
    backtest becomes fiction.
    """
    mo = ctx.mo(ctx.decision_tf) if ctx else None
    atr = (ctx.atr_ref if ctx else 0.0) or 1e-12
    pip = (ctx.spec.pip_size if ctx else 1e-4) or 1e-4
    return {
        "structure": state.family(Family.STRUCTURE),
        "momentum": state.family(Family.MOMENTUM),
        "volatility": state.family(Family.VOLATILITY),
        "participation": state.family(Family.PARTICIPATION),
        "session": state.family(Family.SESSION),
        "multi_tf": state.family(Family.MULTI_TF),
        "cross_market": state.family(Family.CROSS_MARKET),
        "news": state.family(Family.NEWS),
        "execution": state.family(Family.EXECUTION),
        "regime_conf": (ctx.regime.confidence * 100.0
                        if ctx and ctx.regime else 50.0),
        "headroom_atr": min(6.0, state.headroom_pips * pip / atr),
        "range_consumed": ctx.range_consumed if ctx else 0.0,
        "reward_risk": state.reward_risk or 0.0,
        "opportunity": state.opportunity,
        "cost_over_atr": state.cost_pips * pip / atr,
        "stop_atr": abs(state.entry - state.stop) / atr,
        "hour_utc": float(state.as_of.hour),
        "vol_percentile": mo.vol_percentile if mo else 0.5,
        "adx": mo.adx if mo else 0.0,
        "efficiency": mo.efficiency if mo else 0.0,
    }


class MetaLabeller:
    """Calibrated gradient-boosted classifier over ``FEATURES``.

    Optional in every sense: absent scikit-learn, an untrained model or too few
    samples all result in ``predict`` returning ``None``, and the engine simply
    proceeds without this evidence family.
    """

    MIN_TRAIN_SAMPLES = 200

    def __init__(self, path: Optional[str | Path] = None):
        self.path = Path(path) if path else None
        self.model = None
        self.trained_on = 0
        self.metrics: dict = {}
        if self.path and self.path.exists():
            self.load()

    @property
    def available(self) -> bool:
        return self.model is not None

    def load(self) -> bool:
        try:
            with open(self.path, "rb") as fh:
                blob = pickle.load(fh)
            self.model = blob["model"]
            self.trained_on = blob.get("trained_on", 0)
            self.metrics = blob.get("metrics", {})
            return True
        except Exception as exc:
            log.warning("could not load the model: %s", exc)
            return False

    def save(self) -> bool:
        if self.path is None or self.model is None:
            return False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "wb") as fh:
                pickle.dump({"model": self.model, "trained_on": self.trained_on,
                             "metrics": self.metrics}, fh)
            return True
        except Exception as exc:
            log.warning("could not save the model: %s", exc)
            return False

    def train(self, rows: Sequence[dict], labels: Sequence[int],
              *, embargo_fraction: float = 0.2) -> dict:
        """Fit with probability calibration and a time-ordered holdout.

        ``rows`` must be in chronological order.  The split is a plain
        time-ordered holdout with an embargo gap, never a random split:
        shuffling market data lets the model see the future.
        """
        if len(rows) < self.MIN_TRAIN_SAMPLES:
            return {"trained": False,
                    "reason": f"only {len(rows)} samples, need "
                              f"{self.MIN_TRAIN_SAMPLES}"}
        try:
            import numpy as np
            from sklearn.calibration import CalibratedClassifierCV
            from sklearn.ensemble import HistGradientBoostingClassifier
            from sklearn.metrics import brier_score_loss, roc_auc_score
        except Exception as exc:
            return {"trained": False, "reason": f"scikit-learn unavailable: {exc}"}

        X = np.array([[float(r.get(f, 0.0)) for f in FEATURES] for r in rows])
        y = np.array([int(v) for v in labels])
        if len(set(y.tolist())) < 2:
            return {"trained": False, "reason": "only one outcome class present"}

        n = len(X)
        cut = int(n * (1.0 - embargo_fraction))
        embargo = max(1, int(n * 0.02))
        X_tr, y_tr = X[:cut - embargo], y[:cut - embargo]
        X_te, y_te = X[cut:], y[cut:]
        if len(set(y_tr.tolist())) < 2 or len(X_te) < 20:
            return {"trained": False,
                    "reason": "not enough usable data after the embargo split"}

        base = HistGradientBoostingClassifier(
            max_depth=3, max_iter=200, learning_rate=0.06,
            l2_regularization=1.0, random_state=0)
        model = CalibratedClassifierCV(base, method="isotonic", cv=3)
        model.fit(X_tr, y_tr)

        metrics: dict = {"trained": True, "n_train": int(len(X_tr)),
                         "n_test": int(len(X_te))}
        try:
            p = model.predict_proba(X_te)[:, 1]
            metrics["holdout_auc"] = round(float(roc_auc_score(y_te, p)), 4)
            metrics["holdout_brier"] = round(float(brier_score_loss(y_te, p)), 4)
            # Calibration quality: mean |predicted - observed| over deciles.
            import numpy as _np
            bins = _np.clip((p * 10).astype(int), 0, 9)
            errs = []
            for b in range(10):
                m = bins == b
                if m.sum() >= 5:
                    errs.append(abs(p[m].mean() - y_te[m].mean()))
            metrics["calibration_error"] = (round(float(sum(errs) / len(errs)), 4)
                                            if errs else None)
        except Exception as exc:
            metrics["holdout_error"] = str(exc)

        self.model = model
        self.trained_on = n
        self.metrics = metrics
        self.save()
        return metrics

    def predict(self, row: dict) -> Optional[float]:
        """Calibrated probability of reaching the objective, or ``None``."""
        if self.model is None:
            return None
        try:
            import numpy as np
            X = np.array([[float(row.get(f, 0.0)) for f in FEATURES]])
            return float(self.model.predict_proba(X)[0, 1])
        except Exception as exc:
            log.warning("model prediction failed: %s", exc)
            return None

    def evidence_score(self, row: dict) -> Optional[tuple[float, float, str]]:
        """As an evidence family: (score 0-100, confidence, note).

        Confidence is capped well below 1.0 on purpose.  A calibrated model is
        useful additional evidence, not an oracle, and it must never be able to
        dominate the aggregate on its own.
        """
        p = self.predict(row)
        if p is None:
            return None
        cal_err = self.metrics.get("calibration_error")
        conf = 0.6
        if cal_err is not None:
            conf = max(0.15, min(0.75, 0.75 - float(cal_err) * 2.0))
        return (round(p * 100.0, 1), round(conf, 3),
                f"model probability {p * 100:.0f}% "
                f"(trained on {self.trained_on} trades"
                + (f", AUC {self.metrics['holdout_auc']:.2f}"
                   if "holdout_auc" in self.metrics else "") + ")")


def label_from_trade(row: dict, target_r: float = 1.0) -> int:
    """Meta-label: did the trade achieve ``target_r``?

    Uses MFE rather than the realised result on purpose: the question being
    learned is "was this a good *entry*", which is separable from whether the
    exit logic captured it.
    """
    mfe_r = float(row.get("mfe_r") or 0.0)
    return 1 if mfe_r >= target_r else 0
