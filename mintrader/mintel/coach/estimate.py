"""The estimator: how much of a key's record is edge and how much is luck.

Spec section 5, standard library only. For every node of the tree
(L0 the bot, L1 its approach, L2 the kind of market, L3 the condition; the key
is L3), from its weighted trades (weights and units from ``records``):

    n      = number of trades;  W = sum w;  n_eff = W^2 / sum w^2
    ybar   = sum w.x / W
    sigma  = sqrt(sum w.(x - ybar)^2 / W), floored at max(1.0, pooled sigma of its bot x kind)
    v_clu  = sum over clusters (sum in c of w.(x - ybar))^2 / W^2        (cluster-robust)
    v_data = max(v_clu, sigma^2 / n_eff)
    c      = clip(n / number of clusters, 1, 3)

Root prior N(-0.05, 0.10^2): a rule with no skill loses about its costs. Each
level's prior is its parent's posterior computed LEAVE-ONE-OUT (the parent's
trades less this child's), mean m_par and variance v_par + tau_L^2; then

    v_post = 1 / (1 / (v_par + tau^2) + 1 / v_data)
    m_post = v_post . (m_par / (v_par + tau^2) + ybar / v_data)

A node with no trades keeps m_par with variance v_par + tau^2. tau defaults
0.10 (approach), 0.10 (kind), 0.15 (condition) R, re-estimated nightly by
DerSimonian-Laird across siblings with n_eff >= 5 when there are at least 5,
clipped to [0.05, 0.30] and held between nights. Sceptical cap: a key never
real under the coach and not pinned has m_par capped at +0.05 R before its own
data is added, so history used to pick a key never also promotes it.

Outputs per key: m, s, P+ = Phi(m/s), LCB80, LCB90, m_cost15 (commission x
1.5), lambda (trades a UK trading day), sigma, c, the real-only chain
(P+_real, LCB80_real: practice weight 0, real-only parents; every real-money
size decision and every demotion of something live uses these) and the
forward-only posterior (trades closed after the key's registration, from the
root prior, no pooling).

Nothing here reads today's profit or loss, the £100 line, or anything but
closed trades.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence

from . import (CLUSTER_RATIO_MAX, CLUSTER_RATIO_MIN, G1_LOOKBACK_DAYS, KELLY_FRACTION, LAB_FAMILY_FLOOR_R,
               PRIOR_M0_R, PRIOR_S0_R, SCEPTICAL_CAP_R, SIGMA_FLOOR_R, TAU_DEFAULT, TAU_MAX, TAU_MIN,
               TAU_MIN_SIBLINGS, TAU_SIBLING_MIN_NEFF, WILD, Z80, Z90)
from . import epochs as E
from . import records as R

LEVELS = (0, 1, 2, 3)


# ----------------------------------------------------------------- maths --
def phi(z: float) -> float:
    """The standard normal distribution function."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def posterior(m_prior: float, v_prior: float, ybar: float, v_data: float) -> tuple[float, float]:
    """The normal-normal update: (m_post, v_post)."""
    if not (v_data > 0) or math.isinf(v_data):
        return m_prior, v_prior
    v_post = 1.0 / (1.0 / v_prior + 1.0 / v_data)
    return v_post * (m_prior / v_prior + ybar / v_data), v_post


def kelly_pct(lcb80: float, sigma: float, cbar: float, fraction: float = KELLY_FRACTION) -> float:
    """The Kelly cap on the lower bound, divided by the cluster size (section
    6.1): ``k% = 100 x fraction x max(0, LCB80) / (sigma^2 x c)``. 0.25 in v1;
    the demo ladder uses half-Kelly (0.5)."""
    denom = float(sigma) ** 2 * max(float(cbar), CLUSTER_RATIO_MIN)
    if denom <= 0:
        return 0.0
    return 100.0 * float(fraction) * max(0.0, float(lcb80)) / denom


def family_root_mean(m0: float, median_past_r: Optional[float]) -> float:
    """The lab family prior (cut-able): ``min(m0, median past net R)``,
    floored at -0.15. It can only make the coach more sceptical."""
    if median_past_r is None or not math.isfinite(float(median_past_r)):
        return m0
    return max(LAB_FAMILY_FLOOR_R, min(m0, float(median_past_r)))


# ---------------------------------------------------------- observations --
@dataclass(frozen=True)
class Obs:
    """One trade ready for the estimator: weight, outcome (practice haircut
    taken off) and the cost-stressed outcome."""
    key: str
    bot: str
    approach: str
    kind: str
    condition: str
    x: float
    x_c15: float
    w: float
    cluster: str
    real: bool
    closed_utc: dt.datetime
    day: dt.date
    net_gbp: float
    risk_gbp: float
    trade: object = None

    def path(self) -> tuple[str, str, str, str]:
        return node_path(self.bot, self.approach, self.kind, self.condition)


def node_path(bot: str, approach: str, kind: str, condition: str) -> tuple[str, str, str, str]:
    """The four node ids from the bot down to the key."""
    l0 = bot
    l1 = f"{bot}|{approach}"
    l2 = f"{l1}|{kind}"
    return l0, l1, l2, f"{l2}|{condition}"


def _haircut_of(haircuts: Optional[Mapping], key: str) -> float:
    if not haircuts or key not in haircuts:
        return R.HAIRCUT_DEFAULT
    h = haircuts[key]
    return float(getattr(h, "h", h))


def prepare(trades: Iterable, as_of: dt.datetime, *, haircuts: Optional[Mapping] = None,
            epochs: Optional[Sequence] = None) -> list[Obs]:
    """Weights (``records.weight``) and outcomes for the estimator. Practice
    outcomes have the key's haircut ``h`` taken off (0.10 R by default). A
    trade that closed after ``as_of`` is not seen (a replay decides each day
    from earlier data only)."""
    as_of = R.to_utc(as_of)
    ep = epochs if epochs is not None else E.EPOCHS
    today = R.uk_date(as_of)
    epoch_cache: dict[str, Optional[dt.datetime]] = {}
    out = []
    for t in trades:
        if t.closed_utc > as_of:
            continue
        key = t.key
        if key not in epoch_cache:
            epoch_cache[key] = E.latest_epoch(key, as_of, ep)
        start = epoch_cache[key]
        a = 1.0 if t.real else R.PRACTICE_WEIGHT
        day = t.uk_day
        d = 0.5 ** (R.trading_days_between(day, today) / R.DECAY_HALF_LIFE_DAYS)
        v = E.OLD_EPOCH_WEIGHT if (start is not None and t.closed_utc < start) else 1.0
        h = 0.0 if t.real else _haircut_of(haircuts, key)
        x = t.x - h
        out.append(Obs(key, t.bot, t.approach, t.kind, t.condition, x, t.x_cost15 - h, a * d * v, t.cluster,
                       t.real, t.closed_utc, day, t.net_gbp, t.risk_gbp, t))
    return out


# ------------------------------------------------------------ aggregates --
# Two value streams are carried side by side: "x" (the outcome) and "y" (the
# outcome with commission x 1.5), so one pass serves the main and the
# cost-stressed chains.
X, Y = 0, 1


class Agg:
    """Additive sums of one node's weighted trades (so a leave-one-out
    parent is a subtraction)."""
    __slots__ = ("n", "sw", "sw2", "s", "s2", "clu")

    def __init__(self):
        self.n = 0
        self.sw = self.sw2 = 0.0
        self.s = [0.0, 0.0]                     # sum w.x, sum w.y
        self.s2 = [0.0, 0.0]                    # sum w.x^2, sum w.y^2
        self.clu: dict = {}                     # cluster -> [sum w.x, sum w.y, sum w, count]

    def add(self, w: float, x: float, cluster: str, y: Optional[float] = None) -> None:
        y = x if y is None else y
        self.n += 1
        self.sw += w
        self.sw2 += w * w
        wx, wy = w * x, w * y
        s, s2 = self.s, self.s2
        s[0] += wx
        s[1] += wy
        s2[0] += wx * x
        s2[1] += wy * y
        c = self.clu.get(cluster)
        if c is None:
            self.clu[cluster] = [wx, wy, w, 1]
        else:
            c[0] += wx
            c[1] += wy
            c[2] += w
            c[3] += 1

    def merge(self, other: "Agg") -> None:
        """Add another node's sums into this one."""
        self.n += other.n
        self.sw += other.sw
        self.sw2 += other.sw2
        for i in (0, 1):
            self.s[i] += other.s[i]
            self.s2[i] += other.s2[i]
        clu = self.clu
        for cid, (ax, ay, b, k) in other.clu.items():
            c = clu.get(cid)
            if c is None:
                clu[cid] = [ax, ay, b, k]
            else:
                c[0] += ax
                c[1] += ay
                c[2] += b
                c[3] += k

    def minus(self, other: Optional["Agg"]) -> "Agg":
        if other is None or other.n == 0:
            return self
        out = Agg()
        out.n = self.n - other.n
        if out.n <= 0:
            return out
        out.sw = self.sw - other.sw
        out.sw2 = self.sw2 - other.sw2
        out.s = [self.s[0] - other.s[0], self.s[1] - other.s[1]]
        out.s2 = [self.s2[0] - other.s2[0], self.s2[1] - other.s2[1]]
        oc = other.clu
        clu = {}
        for cid, c in self.clu.items():
            o = oc.get(cid)
            if o is None:
                clu[cid] = c
            elif c[3] - o[3] > 0:
                clu[cid] = [c[0] - o[0], c[1] - o[1], c[2] - o[2], c[3] - o[3]]
        out.clu = clu
        return out

    # the classic single-stream names, for readers of one stream
    @property
    def swx(self) -> float:
        return self.s[0]

    @property
    def swx2(self) -> float:
        return self.s2[0]


@dataclass(frozen=True)
class DataStats:
    """What a node's own trades say (before any prior)."""
    n: int = 0
    sw: float = 0.0
    n_eff: float = 0.0
    ybar: float = 0.0
    sigma: float = 0.0                # floored
    sigma_raw: float = 0.0
    v_clu: float = 0.0
    v_data: float = math.inf
    clusters: int = 0
    cbar: float = 1.0


def data_stats(a: Optional[Agg], sigma_floor: float = SIGMA_FLOOR_R, stream: int = X) -> DataStats:
    if a is None or a.n <= 0 or a.sw <= 0:
        return DataStats()
    W = a.sw
    ybar = a.s[stream] / W
    var = max(0.0, a.s2[stream] / W - ybar * ybar)
    sig_raw = math.sqrt(var)
    sig = max(sig_raw, float(sigma_floor), SIGMA_FLOOR_R)
    n_eff = W * W / a.sw2 if a.sw2 > 0 else 0.0
    v_clu = sum((c[stream] - ybar * c[2]) ** 2 for c in a.clu.values()) / (W * W)
    v_data = max(v_clu, sig * sig / n_eff) if n_eff > 0 else math.inf
    k = len(a.clu)
    cbar = min(CLUSTER_RATIO_MAX, max(CLUSTER_RATIO_MIN, a.n / k)) if k else 1.0
    return DataStats(a.n, W, n_eff, ybar, sig, sig_raw, v_clu, v_data, k, cbar)


def pooled_sigma(a: Optional[Agg], stream: int = X) -> float:
    """The weighted standard deviation of a pool (unfloored); 0 when empty."""
    if a is None or a.n <= 0 or a.sw <= 0:
        return 0.0
    ybar = a.s[stream] / a.sw
    return math.sqrt(max(0.0, a.s2[stream] / a.sw - ybar * ybar))


# ---------------------------------------------------------------- outputs --
@dataclass(frozen=True)
class Posterior:
    m: float
    s: float

    @property
    def p_plus(self) -> float:
        return phi(self.m / self.s) if self.s > 0 else (1.0 if self.m > 0 else 0.0)

    @property
    def lcb80(self) -> float:
        return self.m - Z80 * self.s

    @property
    def lcb90(self) -> float:
        return self.m - Z90 * self.s


@dataclass(frozen=True)
class NodeEstimate:
    """A parent node's own posterior (for the page and the digest)."""
    node: str
    level: int
    n: int
    n_eff: float
    ybar: float
    m: float
    s: float

    @property
    def p_plus(self) -> float:
        return Posterior(self.m, self.s).p_plus


@dataclass(frozen=True)
class KeyEstimate:
    """Everything section 5 says about one key."""
    key: str
    n: int
    n_real: int
    n_practice: int
    n_eff: float
    ybar: float
    sigma: float
    v_clu: float
    v_data: float
    clusters: int
    cbar: float
    days: int                          # distinct UK days with a close
    m_par: float                       # the prior's mean (after the sceptical cap)
    s_par: float
    capped: bool                       # the sceptical cap bound
    m: float
    s: float
    p_plus: float
    lcb80: float
    lcb90: float
    m_cost15: float
    s_cost15: float
    lam: float                         # trades a UK trading day
    # the real-only chain
    n_real_eff: float
    m_real: float
    s_real: float
    p_plus_real: float
    lcb80_real: float
    sigma_real: float
    cbar_real: float
    # forward only (after registration)
    registered_utc: Optional[dt.datetime]
    n_fwd: int
    m_fwd: Optional[float]
    s_fwd: Optional[float]
    p_plus_fwd: Optional[float]
    n_forward_epoch: int               # closed after registration AND inside the current epoch
    # the last 30 UK trading days (weighted R)
    recent_net: float
    recent_up: int
    recent_down: int
    recent_best: float
    recent_days: int

    def kelly_pct(self, fraction: float = KELLY_FRACTION, real: bool = True) -> float:
        """Kelly on LCB80 (real-only by default: every real-money size decision)."""
        if real:
            return kelly_pct(self.lcb80_real, self.sigma_real, self.cbar_real, fraction)
        return kelly_pct(self.lcb80, self.sigma, self.cbar, fraction)


@dataclass
class Fit:
    """The nightly refit (or one key's update with tau held)."""
    as_of: dt.datetime
    keys: dict = field(default_factory=dict)          # key -> KeyEstimate
    nodes: dict = field(default_factory=dict)         # node -> NodeEstimate (L0-L2)
    taus: dict = field(default_factory=dict)          # level -> tau
    tau_estimated: dict = field(default_factory=dict) # level -> True when DerSimonian-Laird set it tonight

    def get(self, key: str) -> Optional[KeyEstimate]:
        return self.keys.get(str(key))


# ------------------------------------------------------------- the chain --
class _Tree:
    """Aggregates for every node of one set of observations: each key's own
    sums, then each parent as the merge of its children."""

    def __init__(self, obs: Sequence[Obs]):
        self.aggs: dict[str, Agg] = {}
        self.levels: dict[str, int] = {}
        self.parent: dict[str, str] = {}
        paths: dict[str, tuple] = {}
        for o in obs:
            a = self.aggs.get(o.key)
            if a is None:
                a = self.aggs[o.key] = Agg()
                paths[o.key] = (o.path(), o.bot, o.kind)
            a.add(o.w, o.x, o.cluster, o.x_c15)
        self.kind_pool: dict[tuple[str, str], Agg] = {}
        keys = list(self.aggs.items())
        for key, a in keys:
            path, bot, kind = paths[key]
            self.levels[key] = 3
            self.parent[key] = path[2]
            for lvl in (2, 1, 0):
                node = path[lvl]
                p = self.aggs.get(node)
                if p is None:
                    p = self.aggs[node] = Agg()
                    self.levels[node] = lvl
                    if lvl:
                        self.parent[node] = path[lvl - 1]
                p.merge(a)
            kp = self.kind_pool.get((bot, kind))
            if kp is None:
                kp = self.kind_pool[(bot, kind)] = Agg()
            kp.merge(a)
        self._sigma: dict = {}

    def floor(self, bot: str, kind: Optional[str], stream: int = X, without: Optional[str] = None) -> float:
        """max(1.0, the pooled sigma of the bot x kind); of the bot for L0/L1.
        ``without``: a key whose trades are left out of the pool (a parent's
        leave-one-out piece never sees the child, not even through its spread)."""
        k = (bot, kind, stream, without)
        if k not in self._sigma:
            pool = self.aggs.get(bot) if kind is None else self.kind_pool.get((bot, kind))
            if pool is not None and without is not None:
                pool = pool.minus(self.aggs.get(without))
            self._sigma[k] = pooled_sigma(pool, stream)
        return max(SIGMA_FLOOR_R, self._sigma[k])

    def chain(self, path: Sequence[str], kind: str, taus: Mapping[int, float], m0: float, s0: float,
              cap: bool, stream: int = X) -> tuple[float, float, float, float, DataStats, bool]:
        """The posterior of the last node of ``path`` (m, v), its prior (m_par,
        v_par incl. tau), its own data stats and whether the cap bound."""
        bot = path[0]
        m, v = m0, s0 * s0
        last = len(path) - 1
        for lvl, node in enumerate(path):
            tau = float(taus.get(lvl, 0.0)) if lvl else 0.0
            v_prior = v + tau * tau
            m_prior = m
            capped = False
            own_all = self.aggs.get(node)
            if lvl == last:
                if cap and m_prior > SCEPTICAL_CAP_R:
                    m_prior, capped = SCEPTICAL_CAP_R, True
                own = own_all
            else:
                own = own_all.minus(self.aggs.get(path[lvl + 1])) if own_all is not None else None
            fl = self.floor(bot, kind if lvl >= 2 else None, stream, None if lvl == last else path[last])
            ds = data_stats(own, fl, stream)
            if ds.n > 0:
                m, v = posterior(m_prior, v_prior, ds.ybar, ds.v_data)
            else:
                m, v = m_prior, v_prior
            if lvl == last:
                return m, v, m_prior, v_prior, ds, capped
        raise ValueError("empty path")                    # not reached


def _is_capped(key: str, live_keys: Iterable[str], pins: Iterable[str]) -> bool:
    """The sceptical cap applies to a key never real under the coach and not pinned."""
    if key in set(live_keys):
        return False
    for p in pins:
        try:
            if E.matches(str(getattr(p, "key", p)), key):
                return False
        except Exception:
            continue
    return True


# ------------------------------------------------------------ tau (DL) --
def dersimonian_laird(groups: Sequence[Sequence[tuple[float, float]]]) -> Optional[float]:
    """Between-node standard deviation by DerSimonian-Laird, pooled over
    sibling groups: each group is [(mean, variance)] of nodes sharing a
    parent. None when nothing can be estimated."""
    Q = 0.0
    df = 0
    C = 0.0
    for g in groups:
        g = [(y, v) for y, v in g if v > 0 and math.isfinite(v) and math.isfinite(y)]
        if len(g) < 2:
            continue
        ws = [1.0 / v for _, v in g]
        sw = sum(ws)
        yb = sum(w * y for w, (y, _) in zip(ws, g)) / sw
        Q += sum(w * (y - yb) ** 2 for w, (y, _) in zip(ws, g))
        df += len(g) - 1
        C += sw - sum(w * w for w in ws) / sw
    if df <= 0 or C <= 0:
        return None
    return math.sqrt(max(0.0, (Q - df) / C))


def estimate_taus(tree: _Tree, previous: Optional[Mapping[int, float]] = None) -> tuple[dict, dict]:
    """tau for levels 1-3: DerSimonian-Laird across siblings with n_eff >= 5
    when at least 5 such siblings exist, clipped to [0.05, 0.30]; otherwise the
    previous night's value (or the default)."""
    taus = {lvl: float((previous or {}).get(lvl, TAU_DEFAULT[lvl])) for lvl in (1, 2, 3)}
    done = {lvl: False for lvl in (1, 2, 3)}
    for lvl in (1, 2, 3):
        groups: dict[str, list] = {}
        count = 0
        for node, a in tree.aggs.items():
            if tree.levels[node] != lvl:
                continue
            bot = node.split("|")[0]
            kind = node.split("|")[2] if lvl >= 2 else None
            ds = data_stats(a, tree.floor(bot, kind))
            if ds.n_eff < TAU_SIBLING_MIN_NEFF:
                continue
            groups.setdefault(tree.parent[node], []).append((ds.ybar, ds.v_data))
        usable = [g for g in groups.values() if len(g) >= 2]
        count = sum(len(g) for g in usable)
        if count < TAU_MIN_SIBLINGS:
            continue
        tau = dersimonian_laird(usable)
        if tau is None:
            continue
        taus[lvl] = min(TAU_MAX, max(TAU_MIN, tau))
        done[lvl] = True
    return taus, done


# ------------------------------------------------------------------ fit --
def nightly_fit(trades: Iterable, as_of: dt.datetime, *, held_taus: Optional[Mapping[int, float]] = None,
                **kw) -> tuple["Fit", dict]:
    """The nightly refit, made with tau HELD at last night's value (the defaults on the first night), and
    tonight's DerSimonian-Laird estimate of tau returned beside it, to be held from the next night.

    Why: tau is re-estimated from every key's record, so with tonight's estimate used tonight, one sibling's
    trade made worse can raise tau and with it another key's P+, P+_real or LCB80 (shrinking it less) - a worse
    outcome raising a route or size (spec 18). Held between nights, tonight's decisions cannot be moved by
    tonight's data through tau. Returns (fit with held tau, tonight's tau)."""
    kw.pop("reestimate_tau", None)
    kw.pop("taus", None)
    obs = kw.pop("obs", None)
    if obs is None:
        obs = prepare(trades, as_of, haircuts=kw.get("haircuts"), epochs=kw.get("epochs"))
    tree = _Tree(obs)
    new_taus, _ = estimate_taus(tree, held_taus)
    held = {lvl: float((held_taus or {}).get(lvl, TAU_DEFAULT[lvl])) for lvl in (1, 2, 3)}
    f = fit(trades, as_of, taus=held, reestimate_tau=False, obs=obs, **kw)
    return f, dict(new_taus)


def fit(trades: Iterable, as_of: dt.datetime, *, registered: Optional[Mapping[str, dt.datetime]] = None,
        live_keys: Iterable[str] = (), pins: Iterable = (), haircuts: Optional[Mapping] = None,
        taus: Optional[Mapping[int, float]] = None, reestimate_tau: bool = True,
        family_root: Optional[Mapping[str, float]] = None, only_keys: Optional[Iterable[str]] = None,
        epochs: Optional[Sequence] = None, obs: Optional[Sequence[Obs]] = None,
        real_chain: bool = True) -> Fit:
    """The full refit (nightly), or with ``taus`` given and ``reestimate_tau``
    False, the update of ``only_keys`` with tau held (after a close).

    ``registered``: key -> its registration time (ledger ``segments``), for
    the forward-only posterior. ``live_keys``: keys that have been real under
    the coach; ``pins``: Aaron's pinned key patterns (neither is capped).
    ``family_root``: approach -> the lab family's median past net R (the
    cut-able family prior for Trend & Breakout keys)."""
    as_of = R.to_utc(as_of)
    if obs is None:
        obs = prepare(trades, as_of, haircuts=haircuts, epochs=epochs)
    registered = dict(registered or {})
    live_keys = set(live_keys or ())
    pins = list(pins or ())
    ep = epochs if epochs is not None else E.EPOCHS
    tree = _Tree(obs)
    if reestimate_tau:
        tau_now, done = estimate_taus(tree, taus)
    else:
        tau_now = {lvl: float((taus or {}).get(lvl, TAU_DEFAULT[lvl])) for lvl in (1, 2, 3)}
        done = {lvl: False for lvl in (1, 2, 3)}
    tree_real = _Tree([o for o in obs if o.real] if real_chain else [])
    by_key: dict[str, list[Obs]] = {}
    for o in obs:
        by_key.setdefault(o.key, []).append(o)
    wanted = set(by_key) if only_keys is None else {str(k) for k in only_keys}
    out = Fit(as_of, taus=dict(tau_now), tau_estimated=done)
    today = R.uk_date(as_of)
    window_start = R.trading_days_back(today, G1_LOOKBACK_DAYS)
    s0 = PRIOR_S0_R
    for key in sorted(wanted):
        mine = by_key.get(key, [])
        bot, approach, kind, condition = R.split_key(key)
        path = node_path(bot, approach, kind, condition)
        m0 = PRIOR_M0_R
        if family_root and bot == "tnb" and approach in family_root:
            m0 = family_root_mean(PRIOR_M0_R, family_root[approach])
        cap = _is_capped(key, live_keys, pins)
        m, v, m_par, v_par, ds, capped = tree.chain(path, kind, tau_now, m0, s0, cap)
        m15, v15, *_ = tree.chain(path, kind, tau_now, m0, s0, cap, Y)
        if real_chain:
            mr, vr, _, _, dsr, _ = tree_real.chain(path, kind, tau_now, m0, s0, cap)
        else:
            mr, vr, dsr = m_par, v_par, DataStats()
        post, post_r = Posterior(m, math.sqrt(v)), Posterior(mr, math.sqrt(vr))
        # forward only: after registration, from the root prior, no pooling
        reg = registered.get(key)
        reg = R.to_utc(reg) if isinstance(reg, dt.datetime) else (R.parse_utc(reg) if reg else None)
        fwd = [o for o in mine if reg is not None and o.closed_utc > reg]
        start = E.latest_epoch(key, as_of, ep)
        n_fwd_epoch = sum(1 for o in fwd if start is None or o.closed_utc >= start)
        m_f = s_f = p_f = None
        if reg is not None:
            fa = Agg()
            for o in fwd:
                fa.add(o.w, o.x, o.cluster)
            fds = data_stats(fa, tree.floor(bot, kind))
            if fds.n > 0:
                mf, vf = posterior(m0, s0 * s0, fds.ybar, fds.v_data)
            else:
                mf, vf = m0, s0 * s0
            m_f, s_f = mf, math.sqrt(vf)
            p_f = Posterior(m_f, s_f).p_plus
        # the last 30 UK trading days, weighted R by UK day
        days: dict[dt.date, float] = {}
        for o in mine:
            if o.day >= window_start:
                days[o.day] = days.get(o.day, 0.0) + o.w * o.x
        net = sum(days.values())
        first = min((o.day for o in mine), default=today)
        active = R.trading_days_between(first, today) + (1 if R.is_trading_day(first) else 0)
        n_real = sum(1 for o in mine if o.real)
        out.keys[key] = KeyEstimate(
            key=key, n=ds.n, n_real=n_real, n_practice=len(mine) - n_real, n_eff=ds.n_eff, ybar=ds.ybar,
            sigma=ds.sigma, v_clu=ds.v_clu, v_data=ds.v_data, clusters=ds.clusters, cbar=ds.cbar,
            days=len({o.day for o in mine}), m_par=m_par, s_par=math.sqrt(v_par), capped=capped,
            m=post.m, s=post.s, p_plus=post.p_plus, lcb80=post.lcb80, lcb90=post.lcb90,
            m_cost15=m15, s_cost15=math.sqrt(v15), lam=(1.0 + len(mine)) / (1.0 + max(0, active)),
            n_real_eff=dsr.n_eff, m_real=post_r.m, s_real=post_r.s, p_plus_real=post_r.p_plus,
            lcb80_real=post_r.lcb80, sigma_real=dsr.sigma if dsr.n else max(SIGMA_FLOOR_R, ds.sigma),
            cbar_real=dsr.cbar, registered_utc=reg, n_fwd=len(fwd), m_fwd=m_f, s_fwd=s_f, p_plus_fwd=p_f,
            n_forward_epoch=n_fwd_epoch, recent_net=net, recent_up=sum(1 for x in days.values() if x > 0),
            recent_down=sum(1 for x in days.values() if x < 0), recent_best=max(days.values(), default=0.0),
            recent_days=len(days))
    if only_keys is None:
        for node, lvl in tree.levels.items():
            if lvl == 3:
                continue
            parts = node.split("|")
            path = node_path(*(parts + [WILD] * (4 - len(parts))))[:lvl + 1]
            kind = parts[2] if lvl >= 2 else None
            m, v, _, _, ds, _ = tree.chain(path, kind or "", tau_now, PRIOR_M0_R, s0, False)
            out.nodes[node] = NodeEstimate(node, lvl, ds.n, ds.n_eff, ds.ybar, m, math.sqrt(v))
    return out


def refit_key(trades: Iterable, key: str, as_of: dt.datetime, taus: Mapping[int, float], **kw) -> Optional[KeyEstimate]:
    """After a close: only this key's chain, with tau held at the nightly value."""
    f = fit(trades, as_of, taus=taus, reestimate_tau=False, only_keys=[key], **kw)
    return f.keys.get(str(key))

