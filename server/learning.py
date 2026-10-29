"""Self-learning: replay the strategy on real history, find where and why it lost, and propose bounded
corrections that also hold on data that was not used to choose them (docs/SELF_LEARNING.md).

Pure functions on DataFrames: no database, no network, no asyncio. The engine runs learn_job() / regret_job()
in a child process and calls features() / blocked_by() / scaled_levels() when it produces a signal, so live
signals and the replay share one set of rules: walk_bar() is also what engine._track_outcomes uses.
"""
import bisect
import hashlib
import itertools
import json
import math
import multiprocessing
import os
import threading
from collections import Counter, defaultdict

import pandas as pd

from bot.indicators import atr, ema
from strategies import PositionView

from .feeds.prices import quiet_until

SESSIONS = (("asia", 0, 7), ("london", 7, 12), ("overlap", 12, 16), ("newyork", 16, 21), ("late", 21, 24))
DIMS = {"session": [s[0] for s in SESSIONS], "vol": ["low", "mid", "high"], "trend": ["with", "against"]}
RANK_BARS = 250                 # ATR percentile window
CTX_BARS = RANK_BARS + 50       # bars the features need before the first usable one (EMA 200 settles too)
MIN_HISTORY = CTX_BARS + 500    # bars a symbol needs to be learned from
LIVE_BARS = 600                 # what the engine fetches at least, so live features match the replay's
SCALE_STEP, SCALE_MIN, SCALE_MAX = 0.25, 0.75, 1.5
AUTO_SCALES = (0.75, 1.25)      # auto mode applies scales inside this range by itself; beyond it an admin decides
MAX_FILTERS, MAX_BLOCKED = 2, 0.25
HORIZON = 48                    # bars after a stop-out in which reaching the target means "stop too tight"
SPLITS = (0.6, 0.8)             # train | validation | test, by time
EMBARGO_BARS = 48               # trades entered this soon after a split are dropped (and any trade spanning one)
MARGIN_R = 0.02                 # on validation a correction must gain >= this per trade (counted per week)
T_STEP, T_VALID, T_TEST = 1.0, 1.5, 1.28
MIN_TEST_TRADES, MIN_DIFF_TRADES = 30, 50
FILTER_T, FILTER_SHRUNK, FILTER_MIN_N, FILTER_VALID_N = -2.58, -0.10, 40, 100
KEEP_T, KEEP_SHRUNK = -1.645, -0.05     # a filter already in use stays on weaker evidence than a new one needs
SHRINK_K = 50                   # bucket means are pulled toward the overall mean as if by 50 average trades
DD_RATIO, DD_SLACK = 1.1, 1.0   # test drawdown may grow by at most 10 % (+1R)
# re-tested every week, so stricter than a one-off 5 % test: a model with no real effect is reverted by
# mistake in about 6 % of cases within a year, one doing 0.5 SD/week worse than the plain strategy in 2 of 3
# within 26 weeks
REGRET_WEEKS, REGRET_T = 8, -2.58
EDGE_T = -2.0
BASE_MODEL = {"sl_scale": 1.0, "tp_scale": 1.0, "filters": [], "lessons": []}
LABELS = ("gave_back", "stop_too_tight", "exit_rule", "wrong_way", "chop")
DAY_NS = 86_400_000_000_000


# ── rules shared with the live engine ──────────────────────────
def walk_bar(side: str, sl: float, tp: float | None, high: float, low: float,
             open_: float | None = None) -> tuple[float, str] | None:
    """Did this bar close the signal? Bid bars; the stop-loss is assumed hit first when both are touched.
    A bar that opens beyond a level (a gap) fills at its open, as a stop order or a limit would."""
    if side == "buy":
        if open_ is not None and open_ <= sl:
            return open_, "stop-loss"
        if open_ is not None and tp is not None and open_ >= tp:
            return open_, "take-profit"
        if low <= sl:
            return sl, "stop-loss"
        if tp is not None and high >= tp:
            return tp, "take-profit"
    else:
        if open_ is not None and open_ >= sl:
            return open_, "stop-loss"
        if open_ is not None and tp is not None and open_ <= tp:
            return open_, "take-profit"
        if high >= sl:
            return sl, "stop-loss"
        if tp is not None and low <= tp:
            return tp, "take-profit"
    return None


def session_of(hour: int) -> str:
    return next(name for name, lo, hi in SESSIONS if lo <= hour < hi)


def week_of(ts) -> int:
    """Monday-based week number (1970-01-01 was a Thursday)."""
    return int((pd.Timestamp(ts).value // DAY_NS + 3) // 7)


def _num(v):
    """Numbers as floats: params saved back through JSON (2.0 -> 2) are still the same parameters."""
    if isinstance(v, dict):
        return {k: _num(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_num(x) for x in v]
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def fingerprint(strategy: str, params: dict | None, timeframe: str) -> str:
    """What a model was learned for: it is not applied to another strategy, parameter set or timeframe."""
    raw = json.dumps([strategy, _num(params or {}), timeframe], sort_keys=True, default=str)
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


def fingerprints(strategy: str, params: dict | None, timeframe: str) -> set[str]:
    """The fingerprints a model learned for these settings may carry: fingerprint(), or the one models stored
    before numbers were compared by value got (the params hashed as given)."""
    raw = json.dumps([strategy, params or {}, timeframe], sort_keys=True, default=str)
    return {fingerprint(strategy, params, timeframe), hashlib.sha1(raw.encode()).hexdigest()[:12]}


def add_context(df: pd.DataFrame) -> pd.DataFrame:
    """Columns the features read (prefixed so they never clash with a strategy's own columns)."""
    a = atr(df, 14)
    df["_l_atr"] = a
    df["_l_atr_rank"] = a.rolling(RANK_BARS, min_periods=RANK_BARS).rank(pct=True)
    df["_l_ema200"] = ema(df["close"], 200)
    return df


def features(df: pd.DataFrame, i: int, side: str, bar_secs: float) -> dict | None:
    """Context of a signal produced at the close of bar i, or None while the indicators warm up."""
    rank, trend_line = df["_l_atr_rank"].iat[i], df["_l_ema200"].iat[i]
    if i < CTX_BARS or pd.isna(rank) or pd.isna(trend_line):
        return None
    decided = df["time"].iat[i] + pd.Timedelta(seconds=bar_secs)          # the bar's close: when it is traded
    above = df["close"].iat[i] > trend_line
    return {"session": session_of(decided.hour),
            "vol": "low" if rank < 1 / 3 else "high" if rank > 2 / 3 else "mid",
            "trend": "with" if above == (side == "buy") else "against"}


def blocked_by(model: dict, symbol: str, f: dict | None) -> dict | None:
    """The model's filter that rejects a signal with features `f`, if any."""
    if not f:
        return None
    return next((x for x in model.get("filters") or [] if x["scope"] in ("*", symbol) and f.get(x["dim"]) == x["value"]),
                None)


def scaled_levels(model: dict, side: str, price: float, sl: float, tp: float | None) -> tuple[float, float | None]:
    """Stop / target distances multiplied by the model's scales, anchored on the signal price."""
    sl2 = price - (price - sl) * float(model.get("sl_scale", 1.0))
    tp2 = price + (tp - price) * float(model.get("tp_scale", 1.0)) if tp is not None else None
    return sl2, tp2


def auto_ok(model: dict) -> bool:
    """May auto mode apply this model without an admin? (scales close to the strategy's own)"""
    lo, hi = AUTO_SCALES
    return all(lo <= float(model.get(k, 1.0)) <= hi for k in ("sl_scale", "tp_scale"))


def clean(model: dict | None) -> dict:
    """Only the parts a model is made of (no id / mode / provenance)."""
    m = model or {}
    return {"sl_scale": float(m.get("sl_scale", 1.0)), "tp_scale": float(m.get("tp_scale", 1.0)),
            "filters": [{"scope": x["scope"], "dim": x["dim"], "value": x["value"]} for x in m.get("filters") or []],
            "lessons": list(m.get("lessons") or [])}


def filter_text(x: dict, lang: str = "en") -> str:
    where = ("all pairs" if lang == "en" else "toate perechile") if x["scope"] == "*" else x["scope"]
    names = {"en": {"session": "session", "vol": "volatility", "trend": "trend"},
             "ro": {"session": "sesiunea", "vol": "volatilitatea", "trend": "trend"}}[lang]
    values = {"en": {"with": "with the trend", "against": "against the trend", "low": "low", "mid": "normal",
                     "high": "high", "asia": "Asia", "london": "London", "overlap": "London+New York",
                     "newyork": "New York", "late": "late evening"},
              "ro": {"with": "în sensul trendului", "against": "contra trendului", "low": "scăzută", "mid": "normală",
                     "high": "ridicată", "asia": "asiatică", "london": "Londra", "overlap": "Londra+New York",
                     "newyork": "New York", "late": "de seară"}}[lang]
    v = values.get(x["value"], x["value"])
    if x["dim"] == "trend":
        return f"{where}: no trades {v}" if lang == "en" else f"{where}: fără tranzacții {v}"
    return (f"{where}: no trades when the {names[x['dim']]} is {v}" if lang == "en"
            else f"{where}: fără tranzacții când {names[x['dim']]} este {v}")


# ── replay ─────────────────────────────────────────────────────
class History:
    """One symbol's history, prepared once: strategy columns, context columns and the entry signals.
    `guard`: (start, end) windows around high-impact releases in which the live event guard opens nothing."""
    def __init__(self, symbol: str, raw: pd.DataFrame, strategy, pip: float, bar_secs: float, guard=None,
                 defer: bool = False):
        self.symbol, self.strategy, self.pip, self.bar_secs = symbol, strategy, pip, bar_secs
        self.df = add_context(strategy.prepare(raw.copy().reset_index(drop=True)))
        self.o, self.h, self.l, self.c = (self.df[k].to_numpy(float) for k in ("open", "high", "low", "close"))
        self.t = self.df["time"].to_numpy()
        # nanoseconds whatever the column's unit (pandas 3 keeps MT5 times as datetime64[s]), like Timestamp.value
        self.tn = self.df["time"].dt.as_unit("ns").astype("int64").to_numpy()
        # the broker's recorded spread (MT5 feed) is a per-trade cost floor raised at illiquid hours
        self.spread = (self.df["spread_price"].fillna(0).to_numpy(float) / pip
                       if "spread_price" in self.df.columns else None)
        self.start = max(int(strategy.warmup_bars), CTX_BARS)
        # entry_signal(df, i) depends only on the bars, so it is asked once per bar, not once per replay
        self.entries: dict[int, tuple] = {}
        for i in range(self.start, len(self.df)):
            sig = strategy.entry_signal(self.df, i)
            if sig is not None:
                self.entries[i] = (sig, features(self.df, i, sig.side, bar_secs))
        # entries the live event guard skips: the bar closes (the decision) inside a window around a release.
        # The live engine decides seconds after the close, so a window that ends at the close does not count.
        # defer: a mode on its own timeframe decides only once the rollover's wide spreads are over (D1: 3 h after
        # the close). reach[k]: the latest end of the windows starting up to the k-th
        wins = sorted((pd.Timestamp(a).value, pd.Timestamp(b).value) for a, b in guard or ())
        starts, reach = [a for a, _ in wins], list(itertools.accumulate((b for _, b in wins), max))
        closes = {i: int(self.tn[i]) + int(bar_secs * 1e9) for i in self.entries}
        if defer:
            closes = {i: pd.Timestamp(q).value if (q := quiet_until(pd.Timestamp(c).to_pydatetime())) else c
                      for i, c in closes.items()}
        self.guarded = {i for i, c in closes.items() if (k := bisect.bisect_right(starts, c) - 1) >= 0 and reach[k] > c}
        self._exits: dict[tuple, bool] = {}

    def exit_rule(self, i: int, side: str, entry: float, sl: float, tp: float | None, opened: int) -> bool:
        key = (i, side, opened) if not getattr(self.strategy, "exit_uses_levels", True) else None
        if key is not None and key in self._exits:
            return self._exits[key]
        view = PositionView(self.symbol, side, entry, sl, tp, pd.Timestamp(self.t[opened]).to_pydatetime(), i - opened)
        out = bool(self.strategy.exit_signal(self.df, i, view))
        if key is not None:
            self._exits[key] = out
        return out


def replay(s: History, model: dict, cost_pips: float, cost_mult: float = 1.0) -> tuple[list[dict], int, int]:
    """Trade the history the way the live engine would. Returns (trades, signals seen while flat, blocked)."""
    trades, seen, blocked = [], 0, 0
    pos = None
    for i in range(s.start, len(s.c)):
        if pos is not None and i > pos["i"]:
            hit = walk_bar(pos["side"], pos["sl"], pos["tp"], s.h[i], s.l[i], s.o[i])
            if hit:
                trades.append(_close(s, pos, i, hit[0], hit[1]))
                pos = None
            else:
                fav = (s.h[i] - pos["entry"]) if pos["side"] == "buy" else (pos["entry"] - s.l[i])
                adv = (pos["entry"] - s.l[i]) if pos["side"] == "buy" else (s.h[i] - pos["entry"])
                pos["mfe"], pos["mae"] = max(pos["mfe"], fav), max(pos["mae"], adv)
        if pos is not None:
            if not (i > pos["i"] and s.exit_rule(i, pos["side"], pos["entry"], pos["sl"], pos["tp"], pos["i"])):
                continue
            # closed by its exit rule: the bar may open the next trade, as live (a crossover's exit is the
            # opposite crossover's entry)
            trades.append(_close(s, pos, i, s.c[i], "exit rule"))
            pos = None
        e = s.entries.get(i)
        if e is None or i in s.guarded:
            continue
        sig, f = e
        if f is None:
            continue
        seen += 1
        if blocked_by(model, s.symbol, f):
            blocked += 1
            continue
        entry = s.c[i]
        sl, tp = scaled_levels(model, sig.side, entry, float(sig.sl), float(sig.tp) if sig.tp is not None else None)
        if abs(entry - sl) <= 0:
            continue
        cost = max(cost_pips, float(s.spread[i]) if s.spread is not None else 0.0) * cost_mult
        pos = {"i": i, "side": sig.side, "entry": entry, "sl": sl, "tp": tp, "f": f, "mfe": 0.0, "mae": 0.0,
               "cost": cost}
    return trades, seen, blocked


def _close(s: History, pos: dict, i: int, price: float, reason: str) -> dict:
    risk = abs(pos["entry"] - pos["sl"])
    sign = 1 if pos["side"] == "buy" else -1
    r = (price - pos["entry"]) * sign / risk - pos["cost"] * s.pip / risk
    after = None
    if reason == "stop-loss" and pos["tp"] is not None:      # did the market reach the target right after?
        hi = s.h[i + 1:i + 1 + HORIZON]
        lo = s.l[i + 1:i + 1 + HORIZON]
        if len(hi):
            after = bool(hi.max() >= pos["tp"]) if pos["side"] == "buy" else bool(lo.min() <= pos["tp"])
    t = {"sym": s.symbol, "i": pos["i"], "time": pd.Timestamp(s.t[pos["i"]]), "exit_time": pd.Timestamp(s.t[i]),
         "week": int((s.tn[pos["i"]] // DAY_NS + 3) // 7), "side": pos["side"],
         "entry": pos["entry"], "sl": pos["sl"], "tp": pos["tp"], "exit": float(price), "exit_i": i,
         "reason": reason, "r": float(r), "mfe_r": pos["mfe"] / risk, "mae_r": pos["mae"] / risk,
         "risk_pips": risk / s.pip, "cost_pips": pos["cost"], "f": pos["f"], "tp_after": after}
    t["label"] = label(t)
    return t


def label(t: dict) -> str | None:
    """What went wrong with a losing trade (None for a winner)."""
    if t["r"] >= 0:
        return None
    if t["mfe_r"] >= 1.0:
        return "gave_back"
    if t["reason"] == "stop-loss" and t["tp_after"]:
        return "stop_too_tight"
    if t["reason"] == "exit rule":
        return "exit_rule"
    if t["mfe_r"] < 0.3:
        return "wrong_way"
    return "chop"


# ── statistics ─────────────────────────────────────────────────
def stats(rs: list[float]) -> dict:
    n = len(rs)
    if not n:
        return {"n": 0, "mean": 0.0, "total": 0.0, "se": None, "t": None, "win_rate": None}
    mean = sum(rs) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in rs) / (n - 1)) if n > 1 else 0.0
    se = sd / math.sqrt(n) if n > 1 else None
    return {"n": n, "mean": round(mean, 4), "total": round(sum(rs), 2), "se": round(se, 4) if se else None,
            "t": round(mean / se, 2) if se else None, "win_rate": round(100 * sum(x > 0 for x in rs) / n, 1)}


def clustered(trades: list[dict]) -> dict:
    """Mean R per trade with a standard error clustered by week: trades of one week share one market,
    so counting them as independent would overstate the evidence."""
    n = len(trades)
    if not n:
        return {"n": 0, "weeks": 0, "mean": 0.0, "se": None, "t": None}
    mean = sum(t["r"] for t in trades) / n
    by_week = defaultdict(lambda: [0.0, 0])
    for t in trades:
        w = by_week[t["week"]]
        w[0] += t["r"]
        w[1] += 1
    g = len(by_week)
    if g < 2:
        return {"n": n, "weeks": g, "mean": round(mean, 4), "se": None, "t": None}
    var = g / (g - 1) * sum((s - k * mean) ** 2 for s, k in by_week.values()) / n ** 2
    se = math.sqrt(var)
    return {"n": n, "weeks": g, "mean": round(mean, 4), "se": round(se, 4) if se else None,
            "t": round(mean / se, 2) if se else None}


def shrunk(mean: float, n: int, prior: float) -> float:
    return prior + (mean - prior) * n / (n + SHRINK_K)


def max_dd(trades: list[dict]) -> float:
    peak = cum = dd = 0.0
    for t in sorted(trades, key=lambda t: t["exit_time"]):
        cum += t["r"]
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    return round(dd, 2)


def _period(trades: list[dict], cuts: tuple, embargo: pd.Timedelta) -> tuple[dict[str, list[dict]], int]:
    """Trades by the period they were entered in. A trade still open at a split point, or entered within the
    embargo after one, belongs to neither side (its outcome would leak across the split)."""
    out, dropped = {"train": [], "valid": [], "test": []}, 0
    for t in trades:
        e, x = t["time"], t["exit_time"]
        if e < cuts[0]:
            p = "train" if x < cuts[0] else None
        elif e < cuts[1]:
            p = "valid" if e >= cuts[0] + embargo and x < cuts[1] else None
        else:
            p = "test" if e >= cuts[1] + embargo else None
        if p:
            out[p].append(t)
        else:
            dropped += 1
    return out, dropped


class Run:
    """All symbols replayed under one model."""
    def __init__(self, series: list[History], model: dict, cost_pips: float, cuts: tuple, embargo: pd.Timedelta,
                 cost_mult: float = 1.0):
        self.model = model
        self.trades, self.seen, self.blocked = [], 0, 0
        for s in series:
            tr, seen, blocked = replay(s, model, cost_pips, cost_mult)
            self.trades += tr
            self.seen += seen
            self.blocked += blocked
        self.periods, self.dropped = _period(self.trades, cuts, embargo)

    def stat(self, period: str) -> dict:
        return stats([t["r"] for t in self.periods[period]])

    def weekly(self, period: str) -> dict[int, float]:
        out = defaultdict(float)
        for t in self.periods[period]:
            out[t["week"]] += t["r"]
        return out

    def summary(self) -> dict:
        return {p: self.stat(p) for p in ("train", "valid", "test")} | {
            "all": stats([t["r"] for t in self.trades]),
            "blocked_share": round(self.blocked / self.seen, 3) if self.seen else 0.0}


def paired(a: Run, b: Run, period: str, weeks: list[int]) -> dict:
    """How much better b did than a, week by week over the same weeks (R per week).
    Both runs saw the same market each week, so the market's own swings cancel out of the difference."""
    wa, wb = a.weekly(period), b.weekly(period)
    st = stats([wb.get(w, 0.0) - wa.get(w, 0.0) for w in weeks])
    return {"weeks": st["n"], "mean": st["mean"], "se": st["se"], "t": st["t"], "total": st["total"]}


def _key(t: dict) -> tuple:
    return t["sym"], t["i"], t["side"], round(t["r"], 6)


def differing(a: Run, b: Run, periods=("valid", "test")) -> int:
    ka = {_key(t) for p in periods for t in a.periods[p]}
    kb = {_key(t) for p in periods for t in b.periods[p]}
    return len(ka ^ kb)


def _same(a: dict, b: dict) -> bool:
    key = lambda m: (round(float(m.get("sl_scale", 1)), 2), round(float(m.get("tp_scale", 1)), 2),
                     sorted((x["scope"], x["dim"], x["value"]) for x in m.get("filters") or []))
    return key(a) == key(b)


def _grid(v: float) -> list[float]:
    return sorted({round(min(SCALE_MAX, max(SCALE_MIN, v + d)), 2) for d in (-SCALE_STEP, 0.0, SCALE_STEP)})


def _banned(blacklist, kind: str, **kv) -> bool:
    return any(b.get("kind") == kind and all(b.get(k) == v for k, v in kv.items()) for b in blacklist or [])


# ── the learning run ───────────────────────────────────────────
def learn(data: dict[str, pd.DataFrame], make_strategy, active: dict, *, cost_pips: float, bar_secs: float,
          pip_of, blacklist=(), progress=None, guard=None, defer=False) -> tuple[dict, dict]:
    """Returns (report, proposal). `active` is the model in use now (BASE_MODEL when none); the proposal is a
    full model (the active one again when nothing passed). `guard`: event-guard windows by symbol; `defer`: the
    mode decides after the rollover's wide spreads (History)."""
    say = progress or (lambda _m: None)
    series = []
    for sym, raw in data.items():
        if len(raw) < MIN_HISTORY:
            continue
        say(f"preparing {sym}")
        series.append(History(sym, raw, make_strategy(), pip_of(sym), bar_secs, (guard or {}).get(sym), defer))
    if not series:
        raise ValueError("not enough price history to learn from")
    lo = max(pd.Timestamp(s.t[s.start]) for s in series)
    hi = min(pd.Timestamp(s.t[-1]) for s in series)
    if hi <= lo:
        raise ValueError("the symbols' histories do not overlap")
    cuts = (lo + (hi - lo) * SPLITS[0], lo + (hi - lo) * SPLITS[1])
    embargo = pd.Timedelta(seconds=EMBARGO_BARS * bar_secs)
    weeks = {"train": list(range(week_of(lo), week_of(cuts[0]) + 1)),
             "valid": list(range(week_of(cuts[0]), week_of(cuts[1]) + 1)),
             "test": list(range(week_of(cuts[1]), week_of(hi) + 1))}
    act = clean(active)
    run = lambda m, mult=1.0: Run(series, m, cost_pips, cuts, embargo, mult)

    say("replaying the active model")
    base = run(BASE_MODEL)
    current = base if _same(act, BASE_MODEL) else run(act)

    # 0. does the strategy itself still make money? (no correction can create an edge that is not there)
    e_test, e_all = clustered(base.periods["test"]), clustered(base.trades)
    edge = {"test": e_test, "all": e_all,
            "alert": bool(e_test["t"] is not None and e_test["mean"] < 0 and e_test["t"] <= EDGE_T)}

    # 1. what went wrong (the active model's own trades)
    losses = [t for t in current.trades if t["label"]]
    counts = Counter(t["label"] for t in losses)
    mistakes = {"losses": len(losses), "counts": {k: counts.get(k, 0) for k in LABELS},
                "share": {k: round(100 * counts.get(k, 0) / len(losses), 1) if losses else 0.0 for k in LABELS},
                "by_symbol": {s.symbol: dict(Counter(t["label"] for t in losses if t["sym"] == s.symbol))
                              for s in series}}

    # 2. stop / target scales: neighbours of the active scales, chosen on train, confirmed on validation
    say("trying stop/target corrections")
    no_filters = {**act, "filters": []}
    here_key = (round(act["sl_scale"], 2), round(act["tp_scale"], 2))
    here = base if _same(no_filters, BASE_MODEL) else (current if not act["filters"] else run(no_filters))
    grid, runs = [], {here_key: here}
    for sl in _grid(act["sl_scale"]):
        for tp in _grid(act["tp_scale"]):
            if (sl, tp) != here_key and not _banned(blacklist, "scale", sl_scale=sl, tp_scale=tp):
                m = {**no_filters, "sl_scale": sl, "tp_scale": tp}
                runs[(sl, tp)] = base if _same(m, BASE_MODEL) else run(m)
    for (sl, tp), r in runs.items():
        grid.append({"sl_scale": sl, "tp_scale": tp, "train": r.stat("train"), "valid": r.stat("valid"),
                     "train_gain": paired(here, r, "train", weeks["train"]),
                     "valid_gain": paired(here, r, "valid", weeks["valid"])})
    contenders = [g for g in grid if (g["sl_scale"], g["tp_scale"]) != here_key
                  and g["train_gain"]["mean"] > 0 and (g["train_gain"]["t"] or 0) >= T_STEP]
    chosen_key, chosen = here_key, here
    if contenders:
        best = max(contenders, key=lambda g: g["train_gain"]["mean"])
        if best["valid_gain"]["mean"] > 0 and (best["valid_gain"]["t"] or 0) >= T_STEP:
            chosen_key = (best["sl_scale"], best["tp_scale"])
            chosen = runs[chosen_key]
    scaled = {**no_filters, "sl_scale": chosen_key[0], "tp_scale": chosen_key[1]}

    # 3. filters (all pairs together: a single pair has too few trades to tell a pattern from luck)
    say("looking for conditions to avoid")
    train, valid = chosen.periods["train"], chosen.periods["valid"]
    prior = sum(t["r"] for t in train) / len(train) if train else 0.0
    in_use = {(x["dim"], x["value"]) for x in act["filters"]}
    cands = []
    for dim, values in DIMS.items():
        for value in values:
            if _banned(blacklist, "filter", dim=dim, value=value):
                continue
            tr = clustered([t for t in train if t["f"][dim] == value])
            va = stats([t["r"] for t in valid if t["f"][dim] == value])
            sh = shrunk(tr["mean"], tr["n"], prior)
            need_t, need_sh = (KEEP_T, KEEP_SHRUNK) if (dim, value) in in_use else (FILTER_T, FILTER_SHRUNK)
            if (tr["n"] >= FILTER_MIN_N and tr["t"] is not None and tr["t"] <= need_t and sh <= need_sh
                    and va["n"] >= FILTER_VALID_N and va["mean"] < 0):
                cands.append({"scope": "*", "dim": dim, "value": value,
                              "train": {"n": tr["n"], "weeks": tr["weeks"], "mean": tr["mean"], "t": tr["t"],
                                        "shrunk": round(sh, 4)},
                              "valid": {"n": va["n"], "mean": va["mean"]}})
    cands.sort(key=lambda x: x["train"]["t"])
    picked, best_run, tried = [], chosen, []
    for c in cands:
        if len(picked) >= MAX_FILTERS:
            tried.append({**c, "kept": False, "why": "limit of filters reached"})
            continue
        trial = run({**scaled, "filters": [*picked, c]})
        gain = paired(best_run, trial, "valid", weeks["valid"])
        share = trial.summary()["blocked_share"]
        ok = gain["mean"] > 0 and (gain["t"] or 0) >= T_STEP and share <= MAX_BLOCKED
        tried.append({**c, "kept": ok, "valid_gain": gain, "blocked_share": share})
        if ok:
            picked.append(c)
            best_run = trial
    candidate = {**scaled, "filters": [{"scope": "*", "dim": c["dim"], "value": c["value"]} for c in picked]}
    cand_run = best_run

    # 4. validation as a whole, then the gate on the newest data (never used for choosing)
    say("checking on the newest data")
    a_test, c_test = current.stat("test"), cand_run.stat("test")
    v_gain = paired(current, cand_run, "valid", weeks["valid"])
    t_gain = paired(current, cand_run, "test", weeks["test"])
    margin = round(MARGIN_R * len(current.periods["valid"]) / max(1, len(weeks["valid"])), 4)
    checks = []
    if _same(candidate, act):
        decision, why = "no_change", ("no correction beat the active model on train and validation",
                                      "nicio corecție nu a fost mai bună pe antrenare și validare")
    elif v_gain["mean"] < margin or (v_gain["t"] or 0) < T_VALID:
        decision, why = "no_change", (
            f"the corrections found gain too little on validation: {v_gain['mean']:+.2f}R/week "
            f"(need {margin:+.2f}), t={v_gain['t']} (need {T_VALID})",
            f"corecțiile găsite câștigă prea puțin la validare: {_r(v_gain['mean'], 'ro')}/săptămână "
            f"(minim {_r(margin, 'ro')}), t={v_gain['t']} (minim {T_VALID})")
    else:
        a2, c2 = run(act, 2.0), run(candidate, 2.0)
        cost2 = paired(a2, c2, "test", weeks["test"])
        dd_a, dd_c = max_dd(current.periods["test"]), max_dd(cand_run.periods["test"])
        diff = differing(current, cand_run)
        checks = [
            {"id": "test_trades", "ok": c_test["n"] >= MIN_TEST_TRADES, "value": c_test["n"], "need": MIN_TEST_TRADES},
            {"id": "differing_trades", "ok": diff >= MIN_DIFF_TRADES, "value": diff, "need": MIN_DIFF_TRADES},
            {"id": "test_gain", "ok": t_gain["mean"] > 0 and (t_gain["t"] or 0) >= T_TEST,
             "value": t_gain["t"], "need": T_TEST},
            {"id": "double_cost", "ok": cost2["mean"] > 0, "value": cost2["mean"], "need": 0},
            {"id": "drawdown", "ok": dd_c <= DD_RATIO * dd_a + DD_SLACK, "value": dd_c,
             "need": round(DD_RATIO * dd_a + DD_SLACK, 2)},
        ]
        failed = [c["id"] for c in checks if not c["ok"]]
        if failed:
            decision, why = "rejected", (f"failed on the newest data: {', '.join(failed)}",
                                         f"nu a trecut pe datele cele mai noi: {', '.join(failed)}")
        else:
            decision, why = "improved", ("better on validation and on the newest data, which was not used to choose it",
                                         "mai bun la validare și pe datele cele mai noi, nefolosite la alegere")
    out_model = candidate if decision == "improved" else {k: act[k] for k in ("sl_scale", "tp_scale", "filters")}
    proposal = {**out_model, "lessons": act["lessons"],
                "expect": {"test_mean": (c_test if decision == "improved" else a_test)["mean"],
                           "weekly_gain": t_gain["mean"] if decision == "improved" else 0.0}}

    report = {
        "summary": {"symbols": [s.symbol for s in series], "bars": int(max(len(s.c) for s in series)),
                    "period": [lo.isoformat(), hi.isoformat()], "cuts": [cuts[0].isoformat(), cuts[1].isoformat()],
                    "weeks": {k: len(v) for k, v in weeks.items()}, "dropped_at_splits": current.dropped,
                    "cost_pips": cost_pips, "trades": len(current.trades),
                    "baseline": base.summary(), "active": current.summary(), "candidate": cand_run.summary()},
        "edge": edge,
        "mistakes": mistakes,
        "scales": {"grid": grid, "chosen": {"sl_scale": chosen_key[0], "tp_scale": chosen_key[1]}},
        "filters": {"candidates": tried, "chosen": candidate["filters"]},
        "gate": {"decision": decision, "why": why[0], "why_ro": why[1], "margin": margin, "valid_gain": v_gain,
                 "test_gain": t_gain, "checks": checks, "active_test": a_test, "candidate_test": c_test},
        "examples": [_example(t) for t in sorted(current.periods["test"], key=lambda t: t["time"], reverse=True)
                     if t["label"]][:12],
    }
    report["explain"] = explain(report, act, candidate)
    return report, proposal


def _example(t: dict) -> dict:
    return {"symbol": t["sym"], "time": t["time"].isoformat(), "side": t["side"], "r": round(t["r"], 2),
            "reason": t["reason"], "label": t["label"], "mfe_r": round(t["mfe_r"], 2), "features": t["f"]}


# ── explanations (EN / RO) ─────────────────────────────────────
LABEL_TEXT = {
    "gave_back": ("gave back profit: was at least +1R, then lost", "profit dat înapoi: a fost peste +1R, apoi a pierdut"),
    "stop_too_tight": ("stop too tight: stopped out, then price reached the target within 48 bars",
                       "stop prea strâns: a luat stop, apoi prețul a atins ținta în 48 de bare"),
    "exit_rule": ("closed at a loss by the strategy's exit rule", "închisă pe pierdere de regula de ieșire a strategiei"),
    "wrong_way": ("wrong direction from the start (never +0.3R)", "direcție greșită de la început (niciodată +0,3R)"),
    "chop": ("went both ways, then hit the stop", "a mers în ambele direcții, apoi a luat stop"),
}


def _r(x: float, lang: str) -> str:
    s = f"{x:+.2f}R"
    return s.replace(".", ",") if lang == "ro" else s


def explain(rep: dict, act: dict, cand: dict) -> dict:
    out = {}
    m, g, sm, edge = rep["mistakes"], rep["gate"], rep["summary"], rep["edge"]
    top = sorted(((k, v) for k, v in m["share"].items() if v), key=lambda kv: -kv[1])[:3]
    for lang in ("en", "ro"):
        en = lang == "en"
        a = sm["active"]["all"]
        lines = [f"Replayed {a['n']} trades on {', '.join(sm['symbols'])} "
                 f"({sm['period'][0][:10]} → {sm['period'][1][:10]}): {m['losses']} losses, "
                 f"{_r(a['mean'], lang)} per trade after a cost of at least {sm['cost_pips']} pip." if en else
                 f"Am reluat {a['n']} tranzacții pe {', '.join(sm['symbols'])} "
                 f"({sm['period'][0][:10]} → {sm['period'][1][:10]}): {m['losses']} pierderi, "
                 f"{_r(a['mean'], lang)} pe tranzacție după un cost de minim {sm['cost_pips']} pip."]
        if edge["alert"]:
            e = edge["test"]
            lines.append(f"Warning: the strategy itself lost money in the newest period ({_r(e['mean'], lang)} per "
                         f"trade, t={e['t']}). Corrections can cut losses, they cannot create an edge." if en else
                         f"Atenție: strategia însăși a pierdut bani în perioada cea mai nouă ({_r(e['mean'], lang)} "
                         f"pe tranzacție, t={e['t']}). Corecțiile pot reduce pierderile, nu pot crea un avantaj.")
        for k, v in top:
            lines.append((f"{v:.0f}% of losses — " if en else f"{v:.0f}% din pierderi — ")
                         + LABEL_TEXT[k][0 if en else 1] + ".")
        if (cand["sl_scale"], cand["tp_scale"]) != (act["sl_scale"], act["tp_scale"]):
            lines.append(f"Correction found: stop ×{cand['sl_scale']:g}, target ×{cand['tp_scale']:g} "
                         f"(was ×{act['sl_scale']:g} / ×{act['tp_scale']:g})." if en else
                         f"Corecție găsită: stop ×{cand['sl_scale']:g}, țintă ×{cand['tp_scale']:g} "
                         f"(era ×{act['sl_scale']:g} / ×{act['tp_scale']:g}).")
        for c in rep["filters"]["candidates"]:
            if c.get("kept"):
                lines.append(f"Avoid — {filter_text(c, lang)}: train {c['train']['n']} trades "
                             f"{_r(c['train']['mean'], lang)} each, validation {c['valid']['n']} trades "
                             f"{_r(c['valid']['mean'], lang)}." if en else
                             f"De evitat — {filter_text(c, lang)}: antrenare {c['train']['n']} tranzacții "
                             f"{_r(c['train']['mean'], lang)} fiecare, validare {c['valid']['n']} tranzacții "
                             f"{_r(c['valid']['mean'], lang)}.")
        verdict = {"improved": ("Accepted", "Acceptat"), "no_change": ("No change", "Nicio schimbare"),
                   "rejected": ("Rejected", "Respins")}[g["decision"]][0 if en else 1]
        lines.append(f"{verdict}: {g['why'] if en else g['why_ro']}.")
        if g["decision"] != "no_change" or not _same(cand, act):
            vg, tg = g["valid_gain"], g["test_gain"]
            lines.append(f"Gain per week vs. the active model: validation {_r(vg['mean'], lang)} (t={vg['t']}), "
                         f"newest period {_r(tg['mean'], lang)} (t={tg['t']})." if en else
                         f"Câștig pe săptămână față de modelul activ: validare {_r(vg['mean'], lang)} (t={vg['t']}), "
                         f"perioada cea mai nouă {_r(tg['mean'], lang)} (t={tg['t']}).")
        out[lang] = lines
    return out


# ── after activation: is the model still better than the plain strategy? ──
def regret(data: dict[str, pd.DataFrame], make_strategy, model: dict, since, *, cost_pips: float, bar_secs: float,
           pip_of, live=(), guard=None, defer=False) -> dict:
    """Replay the active model and the plain strategy over the completed weeks since the model was applied and
    compare them week by week. `live`: the model's own closed live signals [{symbol, side, bar_time}] to check
    that the replay matches what the engine really did. `guard`: event-guard windows by symbol."""
    since = pd.Timestamp(since)
    series = [History(sym, raw, make_strategy(), pip_of(sym), bar_secs, (guard or {}).get(sym), defer)
              for sym, raw in data.items() if len(raw) >= CTX_BARS + 50]
    if not series:
        return {"weeks": 0, "revert": False, "why": "no price history"}
    # the week under way is left out: looked at daily, the verdict changes only when a week completes
    last = week_of(max(pd.Timestamp(s.t[-1]) for s in series))
    weeks = list(range(week_of(since), last))
    m = clean(model)
    mt, bt = [], []
    for s in series:
        mt += [t for t in replay(s, m, cost_pips)[0] if t["time"] >= since]
        bt += [t for t in replay(s, BASE_MODEL, cost_pips)[0] if t["time"] >= since]
    done = {(t["sym"], t["side"], t["time"]) for t in mt}
    mt, bt = [t for t in mt if t["week"] < last], [t for t in bt if t["week"] < last]
    wm, wb = defaultdict(float), defaultdict(float)
    for t in mt:
        wm[t["week"]] += t["r"]
    for t in bt:
        wb[t["week"]] += t["r"]
    d = stats([wm.get(w, 0.0) - wb.get(w, 0.0) for w in weeks])
    out = {"since": since.isoformat(), "weeks": len(weeks), "model": stats([t["r"] for t in mt]),
           "base": stats([t["r"] for t in bt]), "weekly_gain": d, "revert": False, "why": ""}
    if len(weeks) >= REGRET_WEEKS and d["t"] is not None and d["t"] <= REGRET_T:
        out["revert"] = True
        out["why"] = (f"over {len(weeks)} weeks the model did {d['mean']:+.2f}R/week worse than the plain "
                      f"strategy (t={d['t']})")
    # like the replayed trades, only signals of bars that opened after `since` (the one published at the first
    # close after it has no replayed trade)
    live = [x for x in live if pd.Timestamp(x["bar_time"]) >= since]
    if live:
        hit = sum((x["symbol"], x["side"], pd.Timestamp(x["bar_time"])) in done for x in live)
        out["fidelity"] = {"live": len(live), "matched": hit, "share": round(hit / len(live), 3)}
    return out


# ── child-process entry points (picklable arguments only) ──────
def die_with_parent():
    """Initializer of the engine's learning process: it ends as soon as the server does, also when the server is
    killed (the Forex Bot app stops it so) and engine.stop() never shuts the pool down. Without this the idle child
    waits on its queue forever, outside the app's job, holding the folder's files open."""
    parent = multiprocessing.parent_process()
    if parent is not None:
        threading.Thread(target=lambda: (parent.join(), os._exit(1)), name="die-with-parent", daemon=True).start()


def _setup(strategy: str, params: dict | None):
    from strategies import load_strategy
    from .feeds.prices import pip_size
    return (lambda: load_strategy(strategy, params or None)), pip_size


def learn_job(data, strategy, params, active, blacklist, cost_pips, bar_secs, guard=None, defer=False):
    make, pip_of = _setup(strategy, params)
    return learn(data, make, active, cost_pips=cost_pips, bar_secs=bar_secs, pip_of=pip_of, blacklist=blacklist,
                 guard=guard, defer=defer)


def regret_job(data, strategy, params, model, since, cost_pips, bar_secs, live, guard=None, defer=False):
    make, pip_of = _setup(strategy, params)
    return regret(data, make, model, since, cost_pips=cost_pips, bar_secs=bar_secs, pip_of=pip_of, live=live,
                  guard=guard, defer=defer)


# ── AI news signals: report only ───────────────────────────────
def signal_r(sig: dict, cost_pips: float, pip: float) -> float | None:
    """A closed live signal's result in R after cost (same measure as the replay)."""
    if sig.get("result_pips") is None or sig.get("sl") is None:
        return None
    risk = abs(sig["price"] - sig["sl"]) / pip
    return (sig["result_pips"] - cost_pips) / risk if risk > 0 else None


def confidence_bands(closed: list[tuple[int, float]], base_min: int) -> dict:
    """Results of closed live AI signals by confidence band, and the minimum confidence the data supports
    (a suggestion for the admin: the floor is never raised automatically)."""
    bands = []
    for lo in range(0, 100, 10):
        rs = [r for c, r in closed if lo <= c < lo + 10 or (lo == 90 and c == 100)]
        if rs:
            bands.append({"from": lo, "to": lo + 9 if lo < 90 else 100, **stats(rs)})
    level = base_min
    while level < 90:
        st = stats([r for c, r in closed if level <= c < level + 10])
        if st["n"] < 15 or st["se"] is None or st["mean"] + 1.28 * st["se"] >= 0:
            break
        level += 10
    return {"closed": len(closed), "bands": bands, "min_confidence": base_min,
            "suggested_min_confidence": level if level > base_min else None}
