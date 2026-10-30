"""Self-learning v2 (server/learning.py) and how the engine and the API use it, plus the auto-trading check
(POST /api/test-signal) and the connector's skip reports.

    .venv\\Scripts\\python tests\\test_learning.py

Synthetic prices only (seeded numpy): a stub strategy reads a planned entry / side / risk from extra columns and
the bars after each entry follow a planned path (win, loss, or a dip through the stop before the target), so the
outcome of every trade is known. Runs on a throw-away SQLite file with the demo price feed, or an in-memory fake
feed for the engine's learning runs; never writes to data/ or logs/, never uses the network, never starts the
engine's loops, never touches MetaTrader 5. Learning runs stay in this process (FX_LEARN_IN_THREAD=1).
Exit code 0 = every check passed. Checks labelled [PRODUCT BUG] fail because of a defect in the product code.
"""
import faulthandler
import hashlib
import itertools
import json
import logging
import math
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import types
from datetime import timedelta
from pathlib import Path

for _stream in (sys.stdout, sys.stderr):    # section titles and EN/RO texts are not cp1252 (Windows console)
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-learning-"))
DB_FILE = TMP / "platform.db"
DATA_DIR = ROOT / "data"

# settings are read when server modules are imported, so configure first (.env never overrides these)
os.environ.update(DATABASE_URL=f"sqlite:///{DB_FILE.as_posix()}", PRICE_FEED="demo", COOKIE_SECURE="false",
                  SECRET_KEY="learning-test-" + "k" * 40, DEMO_SECONDS_PER_BAR="0", ANTHROPIC_API_KEY="",
                  ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="", FX_LEARN_IN_THREAD="1")
# server.main attaches logs/server.log unless the root logger already has a handler for that file
_guard = logging.NullHandler()
_guard.baseFilename = str((ROOT / "logs" / "server.log").resolve())
_console = logging.StreamHandler()
_console.setLevel(logging.CRITICAL)        # the engine logs expected errors (fidelity warning, ...) on purpose
logging.getLogger().handlers[:0] = [_guard, _console]
_data_before = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}

import numpy as np                          # noqa: E402
import pandas as pd                         # noqa: E402
from fastapi.testclient import TestClient   # noqa: E402
from sqlalchemy import func, select         # noqa: E402

import server.engine as engine_mod         # noqa: E402
import server.main as main                  # noqa: E402
from server import ai as AI                 # noqa: E402
from server import auth                     # noqa: E402
from server import learning as L            # noqa: E402
from server.db import (AuditLog, CalendarEvent, LearnRun, SessionLocal, Signal, SignalSkip, get_app_settings,  # noqa: E402
                       save_app_settings, utcnow)
from server.db import engine as db_engine   # noqa: E402
from server.engine import engine            # noqa: E402
from server.settings import DATABASE_URL, DEFAULT_APP_SETTINGS  # noqa: E402
from strategies import Signal as StratSignal  # noqa: E402
from strategies import Strategy, load_strategy  # noqa: E402

H = {"x-fx": "1"}                           # the CSRF header every state-changing API call needs
PIP = 1e-4
SYMS = ("EURUSD", "GBPUSD", "AUDUSD", "USDCAD")
REPORT_KEYS = {"summary", "edge", "mistakes", "scales", "filters", "gate", "examples", "explain"}
OVERLAP = {"scope": "*", "dim": "session", "value": "overlap"}
PASSED: list[str] = []
FAILED: list[str] = []


def check(ok, label: str, detail=""):
    d = repr(detail) if not isinstance(detail, str) else detail
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {d[:600]}" if not ok and detail != "" else ""))
    (PASSED if ok else FAILED).append(label)


# ── synthetic market ───────────────────────────────────────────
SLOT = 5            # one planned trade every 5 bars: entry bar + 4 bars of path; 24 h / 5 rotates the entry hour
# a path is 4 bars of (open, highest in the trade's favour, deepest against it, close), in R from the entry price
PATHS = {
    "win": [(0.0, 0.3, 0.8, -0.3), (0.2, 1.1, -0.1, 1.0), (1.0, 1.0, -0.6, 0.6), (0.6, 0.7, -0.3, 0.3)],
    "loss": [(0.0, 0.3, 0.4, -0.2), (-0.2, 0.0, 1.6, -1.4), (-1.4, -1.0, 1.7, -1.3), (-1.3, -0.9, 1.5, -1.1)],
    # stop too tight: dips 1.1R (through the stop at 1R, not through one at 1.25R), then reaches the 1R target
    "tight": [(0.0, 0.2, 1.1, -0.9), (-0.9, 1.2, 0.9, 1.0), (1.0, 1.0, -0.6, 0.6), (0.6, 0.7, -0.3, 0.3)],
}


def planned_frame(n: int, seed: int, outcome_of, *, start="2025-01-06", end=None, unit="s") -> pd.DataFrame:
    """H1 bars; at every SLOT-th bar the stub strategy enters (plan_* columns) and the next bars follow
    PATHS[outcome_of(session the signal is traded in, rng)]."""
    rng = np.random.default_rng(seed)
    times = (pd.date_range(end=end, periods=n, freq="h") if end is not None
             else pd.date_range(start, periods=n, freq="h")).as_unit(unit)
    hours = (times + pd.Timedelta(hours=1)).hour.to_numpy()     # a signal is traded at its bar's close
    o, h, lo, c, risk = (np.zeros(n) for _ in range(5))
    entry, side = np.zeros(n, bool), np.full(n, "buy", dtype=object)
    p = 1.2
    for i in range(0, n, SLOT):
        r = PIP * rng.uniform(15, 25)
        o[i], c[i] = p, p + rng.normal(0, 0.3) * r
        h[i] = max(o[i], c[i]) + abs(rng.normal(0, 0.2)) * r
        lo[i] = min(o[i], c[i]) - abs(rng.normal(0, 0.2)) * r
        entry[i], side[i], risk[i] = True, "buy" if rng.random() < 0.5 else "sell", r
        sg, base = (1 if side[i] == "buy" else -1), c[i]
        kind = outcome_of(L.session_of(int(hours[i])), rng)
        for k, (op, fav, adv, cl) in enumerate(PATHS[kind], start=1):
            j = i + k
            if j >= n:
                break
            pts = (base + sg * op * r, base + sg * cl * r, base + sg * fav * r, base - sg * adv * r)
            o[j], c[j], h[j], lo[j] = pts[0], pts[1], max(pts), min(pts)
        p = c[min(i + SLOT - 1, n - 1)]
    return pd.DataFrame({"time": times, "open": o, "high": h, "low": lo, "close": c,
                         "plan_entry": entry, "plan_side": side, "plan_risk": risk})


def session_pattern(sess, rng):          # London+New York overlap loses a lot, every other session wins slightly
    if sess == "overlap":
        return "loss" if rng.random() < 0.8 else "win"
    return "win" if rng.random() < 0.58 else "loss"


def tight_pattern(_sess, rng):           # 35 % of trades are stopped out just before running to the target
    x = rng.random()
    return "win" if x < 0.4 else "tight" if x < 0.75 else "loss"


def coin(_sess, rng):                    # no edge at all (minus the cost)
    return "win" if rng.random() < 0.5 else "loss"


def wins_in(start, weeks):
    """Every trade loses, except the first one of each week in `weeks` (week_of numbers). Without cost a stop
    x0.75 loses the same 1R on a loss and turns a win (+1R) into a loss: 2R/week worse in those weeks only."""
    slot, left = itertools.count(), set(weeks)

    def outcome(_sess, _rng):
        w = L.week_of(pd.Timestamp(start) + pd.Timedelta(hours=next(slot) * SLOT))
        if w in left:
            left.discard(w)
            return "win"
        return "loss"
    return outcome


def frames(pattern, n, seed0, **kw) -> dict:
    return {s: planned_frame(n, seed0 + k, pattern, **kw) for k, s in enumerate(SYMS)}


def random_walk(n: int, seed: int, unit="s") -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 1.1 + np.cumsum(rng.normal(0, 10 * PIP, n))
    open_ = np.r_[1.1, close[:-1]]
    return pd.DataFrame({"time": pd.date_range("2025-01-06", periods=n, freq="h").as_unit(unit), "open": open_,
                         "high": np.maximum(open_, close) + np.abs(rng.normal(0, 4 * PIP, n)),
                         "low": np.minimum(open_, close) - np.abs(rng.normal(0, 4 * PIP, n)), "close": close})


class Planned(Strategy):
    """Test stub: trades the planned entries; closes after SLOT-1 bars (a time stop) so every slot starts flat."""
    name = "planned (test stub)"
    warmup_bars = 0

    def entry_signal(self, df, i):
        cols = self.__dict__.get("_cols")
        if cols is None or cols[0] is not df:       # numpy views once per frame: df[...].iat per bar is slow
            cols = self._cols = (df, df["plan_entry"].to_numpy(bool), df["plan_side"].to_numpy(),
                                 df["plan_risk"].to_numpy(float), df["close"].to_numpy(float))
        _df, entry, sides, risk, close = cols
        if not entry[i]:
            return None
        c, r, side = float(close[i]), float(risk[i]), sides[i]
        sg = 1 if side == "buy" else -1
        return StratSignal(side, sl=c - sg * r, tp=c + sg * r, comment="planned")

    def exit_signal(self, df, i, position):
        return position.bars_held >= SLOT - 1


# registered as strategies.<STUB> in memory only (no file), so load_strategy() and the engine can use it
STUB = "zz_test_planned_stub"
_mod = types.ModuleType(f"strategies.{STUB}")
Planned.__module__ = _mod.__name__
_mod.Planned = Planned
sys.modules[_mod.__name__] = _mod


class Warm(Planned):
    """Test stub: needs 900 bars before its first decision (more than learning.LIVE_BARS)."""
    name = "warm (test stub)"
    warmup_bars = 900


WARM = "zz_test_warm_stub"
_wmod = types.ModuleType(f"strategies.{WARM}")
Warm.__module__ = _wmod.__name__
_wmod.Warm = Warm
sys.modules[_wmod.__name__] = _wmod


def learn(data, make=Planned, active=L.BASE_MODEL, **kw):
    return L.learn(data, make, active, cost_pips=1.0, bar_secs=3600, pip_of=lambda _s: PIP, **kw)


def run_pair(hist, model_b):
    """Two Runs over one History (base and model_b) with learn()'s split points, plus learn()'s week lists."""
    lo, hi = pd.Timestamp(hist.t[hist.start]), pd.Timestamp(hist.t[-1])
    cuts = (lo + (hi - lo) * L.SPLITS[0], lo + (hi - lo) * L.SPLITS[1])
    emb = pd.Timedelta(seconds=L.EMBARGO_BARS * 3600)
    a, b = (L.Run([hist], m, 1.0, cuts, emb) for m in (L.BASE_MODEL, model_b))
    weeks = {"train": list(range(L.week_of(lo), L.week_of(cuts[0]) + 1)),
             "valid": list(range(L.week_of(cuts[0]), L.week_of(cuts[1]) + 1))}
    return a, b, cuts, emb, weeks


TIGHT = {}       # learn() on the stop-too-tight data, reused by the planted-pattern section


# ── 1. time units (pandas 3 keeps MT5 times as datetime64[s], parsed strings as [us]) ──
def test_units():
    base = planned_frame(3000, 7, tight_pattern, unit="ns")
    for unit in ("s", "us"):
        df = base.assign(time=base["time"].astype(f"datetime64[{unit}]"))
        hist = L.History("EURUSD", df, Planned(), PIP, 3600)
        check(str(hist.df["time"].dtype) == f"datetime64[{unit}]", f"[{unit}] History keeps the column's unit (the bug's condition)",
              str(hist.df["time"].dtype))
        check(all(int(hist.tn[k]) == pd.Timestamp(hist.t[k]).value for k in (0, 1, len(hist.t) // 2, -1)),
              f"[{unit}] History.tn is nanoseconds, like Timestamp.value")
        trades = L.replay(hist, L.BASE_MODEL, 1.0)[0]
        bad = [(t["time"], t["week"], L.week_of(t["time"])) for t in trades if t["week"] != L.week_of(t["time"])]
        check(len(trades) > 300 and not bad, f"[{unit}] every trade's week == week_of(its entry time)", bad[:3])
        check(len({t["week"] for t in trades}) >= 15, f"[{unit}] trades spread over many weeks",
              len({t["week"] for t in trades}))
        a, b, _cuts, _emb, weeks = run_pair(hist, {**L.BASE_MODEL, "sl_scale": 1.25})
        p = L.paired(a, b, "train", weeks["train"])
        check(p["mean"] > 0 and p["t"] is not None and p["t"] > 3 and p["weeks"] == len(weeks["train"]),
              f"[{unit}] paired(): weekly gain of a different model is non-zero with a t-stat", p)
        cl = L.clustered(a.trades)
        check(cl["weeks"] >= 15 and cl["se"] is not None and cl["t"] is not None,
              f"[{unit}] clustered(): many weeks and a standard error", cl)
    # what the code did before the fix: astype(int64) of a [s] column gives seconds, so every bar lands in one "week"
    secs = base["time"].astype("datetime64[s]").astype("int64")
    check(len({int((v // L.DAY_NS + 3) // 7) for v in secs}) == 1,
          "(sanity) the pre-fix formula puts every datetime64[s] bar in the same week")

    reps = {}
    for unit in ("s", "us", "ns"):
        data = {s: f.assign(time=f["time"].astype(f"datetime64[{unit}]")) for s, f in frames(tight_pattern, 3000, 20).items()}
        reps[unit] = learn(data)
    dumps = {u: json.dumps(r, sort_keys=True, default=str) for u, r in reps.items()}
    check(dumps["s"] == dumps["us"] == dumps["ns"], "learn(): identical report and proposal for [s] / [us] / [ns] times",
          {u: d[:200] for u, d in dumps.items()} if len(set(dumps.values())) > 1 else "")
    TIGHT["rep"], TIGHT["prop"] = reps["s"]

    # the broker's recorded spread is a cost floor
    df = base.assign(time=base["time"].astype("datetime64[s]"))
    costs = {}
    for spread, mult in ((3 * PIP, 1.0), (0.5 * PIP, 1.0), (3 * PIP, 2.0)):
        hist = L.History("EURUSD", df.assign(spread_price=spread), Planned(), PIP, 3600)
        costs[(spread, mult)] = {round(t["cost_pips"], 6) for t in L.replay(hist, L.BASE_MODEL, 1.0, mult)[0]}
    check(list(costs.values()) == [{3.0}, {1.0}, {6.0}], "spread_price raises the cost above cost_pips; cost_mult doubles it",
          costs)


# ── 2. walk_bar ────────────────────────────────────────────────
def test_walk_bar():
    W = L.walk_bar
    cases = [
        (("buy", 1.0, 1.2, 1.1, 1.05, 1.1), None, "buy: inside the levels"),
        (("buy", 1.0, 1.2, 1.25, 0.95, 1.1), (1.0, "stop-loss"), "buy: both touched -> stop-loss first"),
        (("buy", 1.0, 1.2, 1.25, 0.95, None), (1.0, "stop-loss"), "buy: both touched, no open -> stop-loss first"),
        (("buy", 1.0, 1.2, 1.2, 1.05, 1.1), (1.2, "take-profit"), "buy: target touched exactly"),
        (("buy", 1.0, 1.2, 1.05, 1.0, 1.02), (1.0, "stop-loss"), "buy: stop touched exactly"),
        (("buy", 1.0, 1.2, 1.0, 0.95, 0.98), (0.98, "stop-loss"), "buy: gap below the stop fills at the open"),
        (("buy", 1.0, 1.2, 1.3, 1.19, 1.21), (1.21, "take-profit"), "buy: gap above the target fills at the open"),
        (("buy", 1.0, None, 9.9, 1.01, 1.1), None, "buy: no target, no stop"),
        (("buy", 1.0, None, 9.9, 0.9, 1.1), (1.0, "stop-loss"), "buy: no target, stop"),
        (("sell", 1.2, 1.0, 1.1, 1.05, 1.1), None, "sell: inside the levels"),
        (("sell", 1.2, 1.0, 1.25, 0.95, 1.1), (1.2, "stop-loss"), "sell: both touched -> stop-loss first"),
        (("sell", 1.2, 1.0, 1.15, 1.0, 1.1), (1.0, "take-profit"), "sell: target touched exactly"),
        (("sell", 1.2, 1.0, 1.25, 1.21, 1.22), (1.22, "stop-loss"), "sell: gap above the stop fills at the open"),
        (("sell", 1.2, 1.0, 1.0, 0.9, 0.99), (0.99, "take-profit"), "sell: gap below the target fills at the open"),
        (("sell", 1.2, None, 1.21, 0.1, 1.1), (1.2, "stop-loss"), "sell: no target, stop"),
    ]
    for args, want, label in cases:
        side, sl, tp, high, low, open_ = args
        got = W(side, sl, tp, high, low, open_)
        check(got == want, f"walk_bar {label}", got)
    # in the replay a gap through the stop costs more than 1R
    df = planned_frame(400, 3, coin)
    i = next(k for k in range(L.CTX_BARS + 5, 390) if df["plan_entry"].iat[k])
    sg = 1 if df["plan_side"].iat[i] == "buy" else -1
    e, r = df["close"].iat[i], df["plan_risk"].iat[i]
    gap = e - sg * 2 * r
    df.loc[i + 1, ["open", "high", "low", "close"]] = [gap, gap + r * 0.1, gap - r * 0.1, gap]
    tr = [t for t in L.replay(L.History("EURUSD", df, Planned(), PIP, 3600), L.BASE_MODEL, 0.0)[0] if t["i"] == i]
    check(len(tr) == 1 and tr[0]["reason"] == "stop-loss" and tr[0]["exit"] == gap and abs(tr[0]["r"] + 2) < 1e-9,
          "replay: a bar opening 2R beyond the stop closes the trade at its open (-2R)", tr)


# ── 3. split points and embargo ────────────────────────────────
def test_period():
    c0, c1 = pd.Timestamp("2025-03-03 00:00"), pd.Timestamp("2025-04-07 00:00")
    emb, hr = pd.Timedelta(hours=48), pd.Timedelta(hours=1)
    mk = lambda name, e, x: {"name": name, "time": e, "exit_time": x}       # noqa: E731
    trades = [mk("train", c0 - 10 * hr, c0 - hr), mk("spans_cut0", c0 - 5 * hr, c0 + hr),
              mk("embargo0", c0 + 10 * hr, c0 + 12 * hr), mk("valid_at_embargo_end", c0 + 48 * hr, c0 + 50 * hr),
              mk("spans_cut1", c1 - 3 * hr, c1 + 2 * hr), mk("embargo1", c1 + 47 * hr, c1 + 49 * hr),
              mk("test_at_embargo_end", c1 + 48 * hr, c1 + 60 * hr), mk("test_late", c1 + 100 * hr, c1 + 101 * hr),
              mk("exits_at_cut0", c0 - 2 * hr, c0)]
    out, dropped = L._period(trades, (c0, c1), emb)
    got = {k: [t["name"] for t in v] for k, v in out.items()}
    check(got == {"train": ["train"], "valid": ["valid_at_embargo_end"], "test": ["test_at_embargo_end", "test_late"]}
          and dropped == 5, "_period: trades spanning a split, exiting on it or entered within the embargo are dropped",
          (got, dropped))
    hist = L.History("EURUSD", planned_frame(3000, 5, coin), Planned(), PIP, 3600)
    a, _b, cuts, emb, _w = run_pair(hist, L.BASE_MODEL)
    p = a.periods
    check(all(t["exit_time"] < cuts[0] for t in p["train"]) and all(cuts[0] + emb <= t["time"] and t["exit_time"] < cuts[1]
                                                                     for t in p["valid"])
          and all(t["time"] >= cuts[1] + emb for t in p["test"]) and a.dropped > 0
          and len(p["train"]) + len(p["valid"]) + len(p["test"]) + a.dropped == len(a.trades),
          "Run: every period respects its split and embargo; every trade is in one period or dropped",
          {k: len(v) for k, v in p.items()} | {"dropped": a.dropped})


# ── 4. clustered standard error ────────────────────────────────
def test_clustered():
    check(L.clustered([]) == {"n": 0, "weeks": 0, "mean": 0.0, "se": None, "t": None}, "clustered([]): empty")
    one = L.clustered([{"r": 1.0, "week": 7}, {"r": -0.5, "week": 7}, {"r": 2.0, "week": 7}])
    check(one == {"n": 3, "weeks": 1, "mean": 0.8333, "se": None, "t": None}, "clustered: one week -> se None", one)
    rs = {10: [1.0, -1.0], 11: [2.0], 12: [-1.0, 0.0, 1.0]}
    got = L.clustered([{"r": r, "week": w} for w, xs in rs.items() for r in xs])
    # by hand: n=6, mean=1/3; week sums minus k*mean: 0-2/3, 2-1/3, 0-3/3 -> squares 4/9 + 25/9 + 1 = 38/9
    # var = g/(g-1) * 38/9 / n^2 = 1.5 * 38/9 / 36 = 57/324 ; se = 0.41944 ; t = (1/3)/0.41944 = 0.79
    se = math.sqrt(57 / 324)
    check(got == {"n": 6, "weeks": 3, "mean": round(1 / 3, 4), "se": round(se, 4), "t": round((1 / 3) / se, 2)}
          and got["se"] == 0.4194 and got["t"] == 0.79, "clustered: matches the hand computation (se 0.4194, t 0.79)", got)
    same = L.clustered([{"r": 0.5, "week": w} for w in (1, 1, 2, 2)])
    check(same["se"] is None and same["t"] is None, "clustered: zero variance -> se None (no division by zero)", same)
    st = L.stats([1.0, -1.0, 2.0])
    check(st == {"n": 3, "mean": 0.6667, "total": 2.0, "se": 0.8819, "t": 0.76, "win_rate": 66.7}, "stats()", st)


# ── 5. learn() on data without an edge ─────────────────────────
def _report_shape(rep, prop, label):
    check(set(rep) == REPORT_KEYS, f"{label}: report keys", sorted(rep))
    ex = rep.get("explain", {})
    ok = (set(ex) == {"en", "ro"} and all(isinstance(x, list) and len(x) >= 2 and all(isinstance(s, str) and s for s in x)
                                          for x in ex.values()) and ex["en"] != ex["ro"])
    check(ok, f"{label}: explain has English and Romanian lines", ex)
    try:
        json.dumps([rep, prop], allow_nan=False)
        check(True, f"{label}: report and proposal are strict JSON (stored, then sent by the API)")
    except (TypeError, ValueError) as e:
        check(False, f"{label}: report and proposal are strict JSON (stored, then sent by the API)", e)
    m = rep["mistakes"]
    check(set(m["counts"]) == set(L.LABELS) and sum(m["counts"].values()) == m["losses"]
          and len(rep["examples"]) <= 12 and all(x["label"] in L.LABELS for x in rep["examples"]),
          f"{label}: mistakes add up, examples are labelled losses", m["counts"])
    check(set(prop) == {"sl_scale", "tp_scale", "filters", "lessons", "expect"}, f"{label}: proposal keys", sorted(prop))


def test_learn_edgeless():
    t0 = time.time()
    data = {s: random_walk(3000, 50 + k) for k, s in enumerate(("EURUSD", "GBPUSD", "AUDUSD"))}
    rep, prop = learn(data, lambda: load_strategy("example_ema_cross"))
    g = rep["gate"]
    check(g["decision"] == "no_change" and g["checks"] == [], "random walk + EMA cross: decision no_change", g["decision"])
    check((prop["sl_scale"], prop["tp_scale"], prop["filters"], prop["lessons"]) == (1.0, 1.0, [], [])
          and prop["expect"]["weekly_gain"] == 0.0, "random walk: the proposal is the active (base) model", prop)
    _report_shape(rep, prop, "random walk")
    check(rep["explain"]["en"][-1].startswith("No change:") and rep["explain"]["ro"][-1].startswith("Nicio schimbare:"),
          "random walk: the explanation ends with the verdict in both languages", [rep["explain"][k][-1] for k in ("en", "ro")])

    rep, prop = learn(frames(coin, 5000, 300))
    check(rep["gate"]["decision"] == "no_change" and not prop["filters"] and prop["sl_scale"] == 1.0,
          "planned coin flips (many trades, no edge): decision no_change", (rep["gate"]["decision"], rep["filters"]))
    check(not any(c.get("kept") for c in rep["filters"]["candidates"]), "coin flips: no filter kept")
    _report_shape(rep, prop, "coin flips")
    print(f"     (edgeless learn runs: {time.time() - t0:.1f} s)")

    for data, msg in (({"EURUSD": random_walk(L.CTX_BARS + 499, 1)}, "not enough price history"),
                      ({"EURUSD": random_walk(900, 1), "GBPUSD": random_walk(900, 2).assign(
                          time=lambda d: d["time"] + pd.Timedelta(days=60))}, "do not overlap")):
        try:
            learn(data, lambda: load_strategy("example_ema_cross"))
            check(False, f"learn() raises ValueError: {msg}")
        except ValueError as e:
            check(msg in str(e), f"learn() raises ValueError: {msg}", e)


# ── 6. learn() finds planted patterns ──────────────────────────
def test_learn_planted():
    t0 = time.time()
    data = frames(session_pattern, 5000, 10)
    rep, prop = learn(data)
    g, f = rep["gate"], rep["filters"]
    kept = [c for c in f["candidates"] if c.get("kept")]
    check([(c["dim"], c["value"]) for c in kept] == [("session", "overlap")],
          "session pattern: the overlap-session filter is proposed and kept", f["candidates"])
    check(g["decision"] == "improved" and prop["filters"] == [OVERLAP] and (prop["sl_scale"], prop["tp_scale"]) == (1.0, 1.0),
          "session pattern: decision improved, proposal = that filter only", (g["decision"], g["why"], prop))
    check(g["checks"] and all(c["ok"] for c in g["checks"]) and {c["id"] for c in g["checks"]}
          == {"test_trades", "differing_trades", "test_gain", "double_cost", "drawdown"}, "session pattern: every gate check passed",
          g["checks"])
    share = rep["summary"]["candidate"]["blocked_share"]
    check(0.1 < share <= L.MAX_BLOCKED and g["test_gain"]["mean"] > 0, "session pattern: blocks 10-25 % of signals, gains on test",
          (share, g["test_gain"]))
    en, ro = "\n".join(rep["explain"]["en"]), "\n".join(rep["explain"]["ro"])
    check("Avoid — all pairs: no trades when the session is London+New York" in en and "Accepted:" in en
          and "De evitat — toate perechile: fără tranzacții când sesiunea este Londra+New York" in ro and "Acceptat:" in ro,
          "session pattern: explanation names the filter (EN + RO)", rep["explain"])
    _report_shape(rep, prop, "session pattern")

    rep2, prop2 = learn(data, active=L.clean(prop))
    check(rep2["gate"]["decision"] == "no_change" and prop2["filters"] == [OVERLAP] and rep2["filters"]["chosen"] == [OVERLAP],
          "session pattern, learned again with that model active: no_change, the filter stays",
          (rep2["gate"]["decision"], rep2["filters"]["chosen"]))
    rep3, prop3 = learn(data, blacklist=[{"kind": "filter", "dim": "session", "value": "overlap"}])
    check(all(c["value"] != "overlap" for c in rep3["filters"]["candidates"]) and OVERLAP not in prop3["filters"],
          "session pattern: a blacklisted filter is never proposed", rep3["filters"])

    rep, prop = TIGHT["rep"], TIGHT["prop"]
    grid = {(x["sl_scale"], x["tp_scale"]): x for x in rep["scales"]["grid"]}
    best = max(grid, key=lambda k: grid[k]["train_gain"]["mean"])
    check(best == (1.25, 1.0) and grid[best]["train_gain"]["t"] >= L.T_STEP and grid[best]["valid_gain"]["mean"] > 0,
          "stop-too-tight pattern: stop x1.25 has the best train gain and holds on validation",
          {k: v["train_gain"]["mean"] for k, v in grid.items()})
    check(set(grid) == {(a, b) for a in (0.75, 1.0, 1.25) for b in (0.75, 1.0, 1.25)},
          "scale grid: the 3x3 neighbourhood of the active scales", sorted(grid))
    check(rep["scales"]["chosen"] == {"sl_scale": 1.25, "tp_scale": 1.0} and rep["gate"]["decision"] == "improved"
          and (prop["sl_scale"], prop["tp_scale"]) == (1.25, 1.0) and L.auto_ok(prop),
          "stop-too-tight pattern: stop x1.25 chosen, accepted, within the auto range",
          (rep["scales"]["chosen"], rep["gate"]["decision"], rep["gate"]["why"]))
    counts = rep["mistakes"]["counts"]
    check(max(counts, key=counts.get) == "stop_too_tight", "stop-too-tight pattern: most losses labelled stop_too_tight", counts)
    check(any("Correction found: stop ×1.25, target ×1 (was ×1 / ×1)." in s for s in rep["explain"]["en"])
          and any("Corecție găsită: stop ×1.25, țintă ×1" in s for s in rep["explain"]["ro"]),
          "stop-too-tight pattern: explanation names the correction (EN + RO)", rep["explain"])
    data = frames(tight_pattern, 3000, 20)
    rep4, _ = learn(data, blacklist=[{"kind": "scale", "sl_scale": 1.25, "tp_scale": 1.0}])
    keys = {(x["sl_scale"], x["tp_scale"]) for x in rep4["scales"]["grid"]}
    check((1.25, 1.0) not in keys and rep4["scales"]["chosen"] != {"sl_scale": 1.25, "tp_scale": 1.0},
          "stop-too-tight pattern: a blacklisted scale pair is not tried", rep4["scales"]["chosen"])
    rep5, prop5 = learn(data, active={**L.BASE_MODEL, "sl_scale": 1.25})
    keys = {(x["sl_scale"], x["tp_scale"]) for x in rep5["scales"]["grid"]}
    check(keys == {(a, b) for a in (1.0, 1.25, 1.5) for b in (0.75, 1.0, 1.25)}
          and (rep5["gate"]["decision"] != "improved" or prop5["sl_scale"] >= 1.25),
          "grid around an active stop x1.25 is 1.0..1.5; the stop does not shrink back",
          (sorted(keys), rep5["gate"]["decision"], prop5["sl_scale"]))
    print(f"     (planted-pattern learn runs: {time.time() - t0:.1f} s)")


# ── 7. small helpers ───────────────────────────────────────────
def test_helpers():
    model = {"filters": [{"scope": "*", "dim": "session", "value": "asia"},
                         {"scope": "EURUSD", "dim": "vol", "value": "high"}]}
    f1 = {"session": "asia", "vol": "low", "trend": "with"}
    f2 = {"session": "london", "vol": "high", "trend": "with"}
    check(L.blocked_by(model, "GBPUSD", f1) == model["filters"][0] and L.blocked_by(model, "EURUSD", f2) == model["filters"][1]
          and L.blocked_by(model, "GBPUSD", f2) is None and L.blocked_by(model, "EURUSD", None) is None
          and L.blocked_by(L.BASE_MODEL, "EURUSD", f1) is None and L.blocked_by({}, "EURUSD", f1) is None,
          "blocked_by: '*' or the symbol's own scope, dim == value; no features or no filters -> None")
    near = lambda a, b: all(abs(x - y) < 1e-12 for x, y in zip(a, b))       # noqa: E731
    m = {"sl_scale": 1.5, "tp_scale": 0.5}
    check(near(L.scaled_levels(m, "buy", 1.1, 1.09, 1.13), (1.085, 1.115))
          and near(L.scaled_levels(m, "sell", 1.1, 1.11, 1.07), (1.115, 1.085))
          and L.scaled_levels(m, "buy", 1.1, 1.09, None)[1] is None
          and near(L.scaled_levels(L.BASE_MODEL, "sell", 150.0, 150.4, 149.0), (150.4, 149.0))
          and near(L.scaled_levels({}, "buy", 1.1, 1.09, 1.13), (1.09, 1.13)),
          "scaled_levels: distances scaled from the signal price, both sides; no target stays None")
    check([L.auto_ok(x) for x in ({"sl_scale": 1.0, "tp_scale": 1.0}, {"sl_scale": 1.25, "tp_scale": 0.75}, {},
                                   {"sl_scale": 1.5, "tp_scale": 1.0}, {"sl_scale": 1.0, "tp_scale": 0.5},
                                   {"sl_scale": 0.75, "tp_scale": 1.5})] == [True, True, True, False, False, False],
          "auto_ok: scales within 0.75..1.25 only")
    dirty = {"id": 4, "mode": "trend", "fingerprint": "x", "expect": {}, "sl_scale": "1.25", "tp_scale": 1,
             "filters": [{**OVERLAP, "train": {}, "kept": True}], "lessons": ("a",)}
    check(L.clean(dirty) == {"sl_scale": 1.25, "tp_scale": 1.0, "filters": [OVERLAP], "lessons": ["a"]}
          and L.clean(None) == L.BASE_MODEL and L.clean({}) == L.BASE_MODEL, "clean: only the model's parts, normalised",
          L.clean(dirty))
    texts = {
        (OVERLAP["scope"], "session", "overlap"): ("all pairs: no trades when the session is London+New York",
                                                   "toate perechile: fără tranzacții când sesiunea este Londra+New York"),
        ("EURUSD", "trend", "against"): ("EURUSD: no trades against the trend", "EURUSD: fără tranzacții contra trendului"),
        ("*", "vol", "high"): ("all pairs: no trades when the volatility is high",
                               "toate perechile: fără tranzacții când volatilitatea este ridicată"),
    }
    got = {k: (L.filter_text(dict(zip(("scope", "dim", "value"), k))),
               L.filter_text(dict(zip(("scope", "dim", "value"), k)), "ro")) for k in texts}
    check(got == texts, "filter_text: English and Romanian", got)
    # an untranslated value would be printed raw at the end of the text ("low"/"high" are English words already)
    raw = [(lang, d, v) for d, vs in L.DIMS.items() for v in vs for lang in ("en", "ro")
           if L.filter_text({"scope": "*", "dim": d, "value": v}, lang).endswith(f" {v}")
           and not (lang == "en" and v in ("low", "high"))]
    distinct = all(len({L.filter_text({"scope": "*", "dim": d, "value": v}, lang) for v in vs}) == len(vs)
                   for d, vs in L.DIMS.items() for lang in ("en", "ro"))
    check(not raw and distinct, "filter_text: every dim value has its own readable name in both languages", raw)

    p = {"fast": 20, "slow": 50}
    fp = L.fingerprint("example_ema_cross", p, "H1")
    check(len(fp) == 12 and fp == L.fingerprint("example_ema_cross", {"slow": 50, "fast": 20}, "H1")
          and L.fingerprint("x", None, "H1") == L.fingerprint("x", {}, "H1"),
          "fingerprint: stable, key order and None/{} params do not matter")
    check(len({fp, L.fingerprint("example_ema_cross", {**p, "fast": 21}, "H1"), L.fingerprint("example_ema_cross", p, "H4"),
               L.fingerprint("my_strategy", p, "H1")}) == 4, "fingerprint changes with params, timeframe and strategy")
    fpn = lambda params: L.fingerprint("s", params, "H1")       # noqa: E731
    check(fpn({"a": 2}) == fpn({"a": 2.0}) and fpn({"a": [1, 2], "b": {"c": 3}}) == fpn({"a": [1.0, 2.0], "b": {"c": 3.0}})
          and len({fpn({"a": True}), fpn({"a": 1}), fpn({"a": "1"}), fpn({"a": 2})}) == 4,
          "fingerprint: numbers compare by value (2 == 2.0, nested too: JSON turns 2.0 into 2); bools and strings differ")

    check([L.session_of(h) for h in (0, 6, 7, 11, 12, 15, 16, 20, 21, 23)]
          == ["asia", "asia", "london", "london", "overlap", "overlap", "newyork", "newyork", "late", "late"], "session_of")
    w = L.week_of
    check(w("2025-01-06 00:00") == w("2025-01-12 23:59") == w(pd.Timestamp("2025-01-08").as_unit("s"))
          and w("2025-01-13") == w("2025-01-06") + 1 and w("2025-01-05 23:00") == w("2025-01-06") - 1,
          "week_of: Monday-based, any unit")

    df = L.add_context(random_walk(700, 9))
    check(L.features(df, L.CTX_BARS - 1, "buy", 3600) is None, "features: None while the indicators warm up")
    i = 650
    fb, fs = L.features(df, i, "buy", 3600), L.features(df, i, "sell", 3600)
    want_sess = L.session_of((df["time"].iat[i] + pd.Timedelta(hours=1)).hour)
    check(fb and fs and fb["session"] == fs["session"] == want_sess and fb["vol"] in ("low", "mid", "high")
          and {fb["trend"], fs["trend"]} == {"with", "against"},
          "features: session of the bar's close, volatility bucket, buy and sell trends opposite", (fb, fs))

    t = {"r": -1.0, "mfe_r": 0.1, "reason": "stop-loss", "tp_after": False}
    check([L.label({**t, "r": 0.2}), L.label({**t, "mfe_r": 1.2}), L.label({**t, "tp_after": True}),
           L.label({**t, "reason": "exit rule"}), L.label(t), L.label({**t, "mfe_r": 0.5})]
          == [None, "gave_back", "stop_too_tight", "exit_rule", "wrong_way", "chop"], "label: every class")

    # prices with the pair's own digits (gold 2, JPY pairs 3, else 5): the Telegram text and the AI's prompt
    from server import notify
    sig = {"symbol": "XAUUSD", "mode": "trend", "side": "buy", "price": 2650.123, "sl": 2640.5, "tp": None}
    texts = [notify.signal_text(s) for s in (sig, {**sig, "symbol": "EURUSD", "price": 1.08512, "sl": 1.0831,
                                                   "tp": 1.0891}, {**sig, "symbol": "USDJPY", "price": 149.512,
                                                                   "sl": 149.0})]
    check("Price 2650.12 · SL 2640.50" in texts[0] and " TP " not in texts[0]
          and "Price 1.08512 · SL 1.08310 · TP 1.08910" in texts[1] and "Price 149.512 · SL 149.000" in texts[2],
          "signal_text: prices with the pair's digits", texts)
    prompts = []
    fake = types.SimpleNamespace(beta=types.SimpleNamespace(messages=types.SimpleNamespace(
        parse=lambda **kw: prompts.append(kw["messages"][0]["content"]) or types.SimpleNamespace(stop_reason="refusal"))))
    real_client, AI.client = AI.client, lambda: fake
    try:
        AI.analyze_market(utcnow(), ["XAUUSD", "EURUSD", "USDJPY"],
                          {s: {"last": px, "change_24h_pips": 12.0, "atr_pips": 40.0}
                           for s, px in (("XAUUSD", 2650.123), ("EURUSD", 1.08512), ("USDJPY", 149.512))}, [], [], [])
    except AI.AIError:
        pass                                     # the fake declines: only the prompt is looked at
    finally:
        AI.client = real_client
    check(prompts and "XAUUSD: last 2650.12," in prompts[0] and "EURUSD: last 1.08512," in prompts[0]
          and "USDJPY: last 149.512," in prompts[0], "analyze_market: the prompt's prices with the pair's digits",
          prompts[:1])


# ── 8. regret ──────────────────────────────────────────────────
def test_regret():
    data = frames(session_pattern, 3000, 40)
    times = data["EURUSD"]["time"]
    since = times.iat[1500].to_pydatetime()                 # ~9 weeks before the end
    reg = lambda m, s=since, **kw: L.regret(data, Planned, m, s, cost_pips=1.0, bar_secs=3600,   # noqa: E731
                                            pip_of=lambda _s: PIP, **kw)
    worse = {**L.BASE_MODEL, "sl_scale": 0.75}              # every planned win dips 0.8R: a 0.75R stop turns it into a loss
    r = reg(worse)
    check(r["revert"] is True and r["weeks"] >= L.REGRET_WEEKS and r["weekly_gain"]["t"] <= L.REGRET_T
          and r["model"]["mean"] < r["base"]["mean"] and "worse than the plain strategy" in r["why"],
          "regret: a model clearly worse than the plain strategy -> revert", {k: r[k] for k in ("weeks", "weekly_gain", "why")})
    r = reg({**L.BASE_MODEL, "filters": [OVERLAP]})
    check(r["revert"] is False and r["weekly_gain"]["mean"] > 0, "regret: a better model is kept", r["weekly_gain"])
    r = reg(L.BASE_MODEL)
    check(r["revert"] is False and r["weekly_gain"]["mean"] == 0 and r["weekly_gain"]["t"] is None,
          "regret: the plain strategy itself -> no difference, kept", r["weekly_gain"])
    r = reg(worse, times.iat[-200].to_pydatetime())
    check(r["revert"] is False and r["weeks"] < L.REGRET_WEEKS, "regret: fewer than 8 weeks -> never reverts", r["weeks"])
    hist = L.History("EURUSD", data["EURUSD"], Planned(), PIP, 3600)
    mine = [t for t in L.replay(hist, L.clean(worse), 1.0)[0] if t["time"] >= pd.Timestamp(since)][:5]
    odd = (pd.Timestamp(since) + pd.Timedelta(days=3, minutes=17)).isoformat()      # a bar the replay never traded
    live = ([{"symbol": "EURUSD", "side": t["side"], "bar_time": t["time"].isoformat()} for t in mine]
            + [{"symbol": "EURUSD", "side": "buy", "bar_time": odd}])
    old = [{"symbol": "EURUSD", "side": "buy", "bar_time": "2020-01-06T00:00:00"}]       # before `since`: never replayed
    r, r_old = reg(worse, live=live + old), reg(worse, live=old)
    check(r.get("fidelity") == {"live": 6, "matched": 5, "share": 0.833} and "fidelity" not in r_old,
          "regret: fidelity of the live signals (one from before `since` is left out)", (r.get("fidelity"), r_old.get("fidelity")))

    # the verdict: completed weeks only, t <= -2.58 (re-tested every week, so stricter than a one-off 5 % test)
    start, since2 = pd.Timestamp("2025-01-06"), pd.Timestamp("2025-01-20")    # the replay starts at bar 300 (Jan 18)
    w0, under_way = L.week_of(since2), L.week_of("2025-04-14")                  # 2400 bars end on Wed Apr 16
    reg0 = lambda df: L.regret({"EURUSD": df}, Planned, worse, since2.to_pydatetime(), cost_pips=0.0,  # noqa: E731
                               bar_secs=3600, pip_of=lambda _s: PIP)
    mild = planned_frame(2400, 3, wins_in(start, [w0 + 1, w0 + 4, w0 + 7, w0 + 10, under_way]))
    hard = planned_frame(2400, 3, wins_in(start, [w0 + k for k in (0, 2, 4, 6, 8, 10)]))
    # cut at Mon Apr 14 02:00: every earlier trade has closed, the win of the week under way (bar 2355) is not there yet
    rm, rm_cut, rh = reg0(mild), reg0(mild.iloc[:2355]), reg0(hard)
    g = rm["weekly_gain"]
    check(L.REGRET_T == -2.58 and rm["weeks"] == 12 and g["mean"] == -0.6667 and g["t"] == -2.35 and rm["revert"] is False,
          "regret: 2R/week worse in 4 of 12 weeks (t -2.35) is kept (the old -1.645 reverted it)", (rm["weeks"], g))
    check(rh["revert"] is True and rh["weeks"] == 12 and rh["weekly_gain"]["t"] == -3.32,
          "regret: 2R/week worse in 6 of 12 weeks (t -3.32) -> revert", (rh["weeks"], rh["weekly_gain"]))
    check(all(rm[k] == rm_cut[k] for k in ("weeks", "weekly_gain", "model", "base", "revert")),
          "regret: the week under way is left out (its trades change nothing until it completes)",
          {k: (rm[k], rm_cut[k]) for k in ("weeks", "weekly_gain")})
    r = L.regret({"EURUSD": data["EURUSD"].iloc[:300]}, Planned, worse, since, cost_pips=1.0, bar_secs=3600,
                 pip_of=lambda _s: PIP)
    check(r["revert"] is False and r["weeks"] == 0, "regret: no usable history -> no verdict", r)


# ── 8b. event guard in the replay ──────────────────────────────
def test_guard():
    df = planned_frame(1200, 11, coin)
    plain = L.History("EURUSD", df, Planned(), PIP, 3600)
    ent = sorted(plain.entries)
    e = [ent[k] for k in (10, 20, 30, 40, 50, 60)]             # 50 hours apart: one entry per window
    at = lambda i, minutes=0: (pd.Timestamp(plain.t[i]) + pd.Timedelta(hours=1, minutes=minutes)).to_pydatetime()  # noqa: E731
    sec = timedelta(seconds=1)
    wins = [(at(e[5], -60), at(e[5], -30)),             # nested in the long one below: needs the running max of ends
            (at(e[0], -30), at(e[0], 30)),              # around the close (the decision)
            (at(e[1]), at(e[1], 60)),                   # starts at the close
            (at(e[2], -60), at(e[2])),                  # ends at the close: the live engine looks seconds later
            (at(e[3]) + sec, at(e[3], 30)),             # just after
            (at(e[4], -30), at(e[4]) - sec),            # just before
            (at(e[5], -120), at(e[5], 60))]
    g = L.History("EURUSD", df, Planned(), PIP, 3600, wins)
    check(g.guarded == {e[0], e[1], e[5]}, "History: entries whose bar closes inside a window are guarded "
          "(from its start, not at its end; unsorted, nested windows)", sorted(g.guarded))
    t0, seen0, blk0 = L.replay(plain, L.BASE_MODEL, 1.0)
    t1, seen1, blk1 = L.replay(g, L.BASE_MODEL, 1.0)
    i0, i1 = {t["i"] for t in t0}, {t["i"] for t in t1}
    check(g.guarded <= i0 and i1 == i0 - g.guarded and seen1 == seen0 - len(g.guarded) == seen0 - 3 and blk0 == blk1 == 0,
          "replay: guarded entries are not traded and count as neither seen nor blocked", (seen0, seen1, blk0, blk1))
    m = {**L.BASE_MODEL, "filters": [OVERLAP]}
    (_t, sm0, bm0), (_t, sm1, bm1) = L.replay(plain, m, 1.0), L.replay(g, m, 1.0)
    n_blk = sum(1 for i in g.guarded if L.blocked_by(m, "EURUSD", plain.entries[i][1]))
    check(sm0 - sm1 == len(g.guarded) == 3 and bm0 - bm1 == n_blk, "replay with a filter: a guarded entry is not counted as blocked either",
          (sm0, sm1, bm0, bm1, n_blk))

    since = df["time"].iat[600].to_pydatetime()
    everything = {"EURUSD": [(since, at(len(df) - 1, 60))]}
    worse = {**L.BASE_MODEL, "sl_scale": 0.75}
    reg = lambda guard: L.regret({"EURUSD": df}, Planned, worse, since, cost_pips=1.0, bar_secs=3600,   # noqa: E731
                                 pip_of=lambda _s: PIP, guard=guard)
    r0, r1 = reg(None), reg(everything)
    check(r0["model"]["n"] > 50 and r1["model"]["n"] == r1["base"]["n"] == 0 and r1["revert"] is False,
          "regret(guard): no trade inside the windows", (r0["model"]["n"], r1["model"]["n"], r1["base"]["n"]))
    data = frames(coin, 3000, 90)
    eur = len(L.replay(L.History("EURUSD", data["EURUSD"], Planned(), PIP, 3600), L.BASE_MODEL, 1.0)[0])
    lo, hi = data["EURUSD"]["time"].iat[0].to_pydatetime(), data["EURUSD"]["time"].iat[-1].to_pydatetime()
    n_plain = learn(data)[0]["summary"]["trades"]
    n_guard = learn(data, guard={"EURUSD": [(lo, hi + timedelta(hours=1))]})[0]["summary"]["trades"]
    check(eur > 300 and n_plain - n_guard == eur, "learn(guard): a symbol's windows reach its replay",
          (n_plain, n_guard, eur))


# ── 9. AI news statistics (report only) ────────────────────────
def test_news_stats():
    got = (L.signal_r({"price": 1.1, "sl": 1.098, "result_pips": 40}, 1.0, PIP),
           L.signal_r({"price": 150.0, "sl": 150.5, "result_pips": -50}, 1.0, 0.01))
    check(abs(got[0] - 39 / 20) < 1e-9 and abs(got[1] + 51 / 50) < 1e-9, "signal_r: result after cost, in R", got)
    check([L.signal_r(s, 1.0, PIP) for s in ({"price": 1.1, "sl": 1.098, "result_pips": None},
                                             {"price": 1.1, "sl": None, "result_pips": 5}, {"price": 1.1, "sl": 1.1, "result_pips": 5})]
          == [None, None, None], "signal_r: open, no stop or zero risk -> None")
    losing = [(70 + k % 10, -0.5 if k % 2 else -0.7) for k in range(20)]
    winning = [(80 + k % 10, 0.4 + 0.1 * (k % 3)) for k in range(20)]
    cb = L.confidence_bands(losing + winning + [(100, 1.0), (95, 0.2), (79, -1.0)], 70)
    bands = {(b["from"], b["to"]): b["n"] for b in cb["bands"]}
    check(cb["closed"] == 43 and bands == {(70, 79): 21, (80, 89): 20, (90, 100): 2}, "confidence_bands: bands of 10, 100 in the top band",
          cb["bands"])
    check(cb["suggested_min_confidence"] == 80 and cb["min_confidence"] == 70,
          "confidence_bands: a clearly losing 70s band suggests a floor of 80", cb)
    check(L.confidence_bands(winning + [(c, -r) for c, r in winning], 80)["suggested_min_confidence"] is None
          and L.confidence_bands(losing[:14], 70)["suggested_min_confidence"] is None
          and L.confidence_bands([], 70) == {"closed": 0, "bands": [], "min_confidence": 70, "suggested_min_confidence": None},
          "confidence_bands: no suggestion when the floor's band is not clearly losing or has < 15 trades")
    all_bad = [(c, -0.6 + 0.01 * (c % 3)) for c in range(50, 100) for _ in range(1)] * 3
    check(L.confidence_bands(all_bad, 50)["suggested_min_confidence"] == 90,
          "confidence_bands: the suggestion stops at 90")


# ── 10/11. API and engine ──────────────────────────────────────
OPEN: list = []


def take_session(client) -> str:
    tok = client.cookies.get("fx_session")
    client.cookies.clear()
    return tok


def use(client, tok: str):
    client.cookies.clear()
    client.cookies.set("fx_session", tok)


def connect(client, token: str, version="1.1.2", demo=True):
    ws = client.websocket_connect("/ws/connector").__enter__()
    OPEN.append(ws)
    ws.send_json({"type": "auth", "token": token, "version": version, "paper": False,
                  "account": {"login": 7, "server": "Test-Demo", "currency": "USD", "balance": 10000.0,
                              "equity": 10000.0, "demo": demo, "trade_allowed": True}})
    return ws, ws.receive_json()


def close(ws):
    if ws in OPEN:
        OPEN.remove(ws)
    try:
        ws.__exit__(None, None, None)
    except Exception:
        pass


def sync(ws) -> list[dict]:
    ws.send_json({"type": "ping"})
    out = []
    while (m := ws.receive_json())["type"] != "pong":
        out.append(m)
    return out


def err(r) -> str:
    try:
        return f"{r.status_code} {r.json().get('detail')}"
    except Exception:
        return f"{r.status_code} {r.text[:200]}"


def detail(r):
    try:
        return r.json().get("detail")
    except Exception:
        return None


class FakeFeed:
    """A live (non-demo) feed serving prepared frames: the engine treats its runs like real ones. `counts`: the
    bar counts asked for; `max_history` only when given (the engine assumes 44000 without one)."""
    name = "fake"

    def __init__(self, frames_, max_history=None):
        self.frames, self.counts = frames_, []
        if max_history is not None:
            self.max_history = max_history

    def bar_seconds(self, _tf):
        return 3600

    def candles(self, symbol, _tf, count):
        self.counts.append(count)
        df = self.frames.get(symbol)
        if df is None:
            return pd.DataFrame(columns=["time", "open", "high", "low", "close"])
        return df.iloc[-count:].reset_index(drop=True).copy()

    def quote(self, _symbol):
        return None


def test_api_engine():
    async def fake_loop(fn, every, first_delay=0):
        return None

    engine._loop = fake_loop                     # Engine.start creates its tasks, none of them runs
    main.SETUP_FILE = TMP / "setup_token.txt"    # not data/setup_token.txt
    with TestClient(main.app) as client:
        try:
            ctx = _api(client)
            _engine(client, ctx)
        finally:
            for ws in list(OPEN):
                close(ws)


def _api(client) -> dict:
    client.post("/api/setup", headers=H, json={"token": main._setup_token, "email": "founder@test.local",
                                               "password": "founder-pass-1", "name": "Founder"})
    founder = take_session(client)
    ids = {}
    for name in ("alice", "bob"):
        r = client.post("/api/auth/register", headers=H, json={"email": f"{name}@test.local", "accept_terms": True,
                                                               "password": f"{name}-pass-1"})
        ids[name] = (take_session(client), r.json()["id"])
    (alice, alice_id), (bob, bob_id) = ids["alice"], ids["bob"]
    tokens = {}
    for name, tok in (("alice", alice), ("bob", bob)):
        use(client, tok)
        tokens[name] = client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"]
    check(founder and alice and bob and all(tokens.values()), "founder, two clients and their connector tokens")

    # ── access control ──
    use(client, alice)
    for method, path, body in (("GET", "/api/admin/learning", None), ("GET", "/api/admin/learning/runs/1", None),
                               ("POST", "/api/admin/learning/run", None), ("POST", "/api/admin/learning/runs/1/apply", None),
                               ("POST", "/api/admin/learning/revert", {}), ("PUT", "/api/admin/engine", {"learning": {"mode": "off"}})):
        r = client.request(method, path, headers=H, json=body)
        check(r.status_code == 403 and detail(r) == "admins_only", f"client: {method} {path} -> 403", err(r))
    client.cookies.clear()
    r = client.get("/api/admin/learning")
    check(r.status_code == 401, "logged out: GET /api/admin/learning -> 401", err(r))

    # ── settings ──
    use(client, founder)
    r = client.get("/api/admin/learning")
    st = r.json()
    check(r.status_code == 200 and set(st) == {"settings", "model", "runs", "running", "step", "blocked", "regret", "feed", "limits"}
          and st["settings"] == DEFAULT_APP_SETTINGS["learning"] and st["model"] is None and st["runs"] == []
          and st["running"] is False and st["feed"] == "demo",
          "staff: GET /api/admin/learning (defaults, no model, no runs, demo feed)", st)
    check(st.get("limits") == {"scales": [0.75, 1.5], "auto_scales": [0.75, 1.25], "max_filters": 2, "max_blocked": 0.25},
          "GET /api/admin/learning: limits", st.get("limits"))
    r = client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "bogus", "cost_pips": 3}})
    check(r.status_code == 400 and detail(r) == "invalid_learning_mode", "PUT learning mode 'bogus' -> 400 invalid_learning_mode",
          err(r))
    check(client.get("/api/admin/learning").json()["settings"] == DEFAULT_APP_SETTINGS["learning"],
          "a rejected PUT saves nothing")
    r = client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "auto", "weekday": 9, "hour_utc": -3,
                                                                      "history_bars": 10, "cost_pips": 99,
                                                                      "ai_lessons": 1, "bogus": 5}})
    got = r.json()["settings"]["learning"]
    check(got == {"mode": "auto", "weekday": 6, "hour_utc": 0, "history_bars": 3000, "cost_pips": 10.0, "ai_lessons": True},
          "PUT learning: numbers clamped, unknown keys dropped", got)
    r = client.put("/api/admin/engine", headers=H, json={"learning": {"cost_pips": 1.5, "ai_lessons": False}})
    got = r.json()["settings"]["learning"]
    check(got == {"mode": "auto", "weekday": 6, "hour_utc": 0, "history_bars": 3000, "cost_pips": 1.5, "ai_lessons": False},
          "PUT learning: a partial patch keeps the other fields", got)
    raw_json = {**H, "content-type": "application/json"}        # NaN / Infinity: httpx will not encode them itself
    for bad in ('{"history_bars": "many"}', '{"weekday": null}', '{"history_bars": Infinity}'):
        r = client.put("/api/admin/engine", headers=raw_json, content='{"learning": %s}' % bad)
        check(r.status_code == 400 and detail(r) == "invalid_number", f"PUT learning {bad} -> 400 invalid_number", err(r))
    for bad in ('{"cost_pips": NaN}', '{"cost_pips": -Infinity}', '{"cost_pips": 1e400}', '{"hour_utc": NaN}'):
        r = client.put("/api/admin/engine", headers=raw_json, content='{"learning": %s}' % bad)
        v = r.json()["settings"]["learning"] if r.status_code == 200 else None
        check(r.status_code == 400 and detail(r) == "invalid_number",
              f"PUT learning {bad} -> 400 invalid_number (min(hi, nan) would save the maximum)", (err(r), v))
    r = client.put("/api/admin/engine", headers=raw_json, content='{"modes": {"trend": {"params": {"fast": NaN}}}}')
    check(r.status_code == 400 and detail(r) == "invalid_number", "PUT strategy params with NaN -> 400 invalid_number",
          err(r))
    check(client.get("/api/admin/learning").json()["settings"]["cost_pips"] == 1.5
          and client.get("/api/admin/engine").json()["settings"]["modes"]["trend"] == DEFAULT_APP_SETTINGS["modes"]["trend"],
          "rejected numbers save nothing")
    client.put("/api/admin/engine", headers=H, json={"learning": {"cost_pips": 1.5}})
    audit = client.get("/api/admin/audit").json()
    check(audit[0]["action"] == "engine_settings" and "learning" in audit[0]["details"], "learning settings change is audited")
    r = client.get("/api/admin/learning/runs/999")
    check(r.status_code == 404 and detail(r) == "not_found", "GET a missing run -> 404", err(r))
    r = client.post("/api/admin/learning/runs/999/apply", headers=H)
    check(r.status_code == 409 and detail(r) == "not_applicable", "apply a missing run -> 409 not_applicable", err(r))
    r = client.post("/api/admin/learning/revert", headers=H, json={})
    check(r.status_code == 409 and detail(r) == "no_active_model", "revert with no model -> 409 no_active_model", err(r))

    # ── a manual run on the demo feed (in a thread: FX_LEARN_IN_THREAD) ──
    t0 = time.time()
    r = client.post("/api/admin/learning/run", headers=H)
    check(r.status_code == 200 and r.json() == {"started": True}, "POST /api/admin/learning/run starts a run", err(r))
    run = None
    while time.time() - t0 < 120:
        st = client.get("/api/admin/learning").json()
        mine = [x for x in st["runs"] if x["trigger"] == "manual"]
        if mine and mine[0]["decision"] != "running" and not st["running"]:
            run = mine[0]
            break
        time.sleep(0.2)
    print(f"     (manual demo-feed run: {time.time() - t0:.1f} s)")
    check(run is not None and run["decision"] in ("no_change", "rejected", "improved") and run["finished_at"],
          "the manual run finishes with a decision", run and (run["decision"], run.get("report")))
    if run:
        check(set(run["report"] or {}) <= {"summary", "explain", "error", "gate"}, "run list: only the report's summary parts",
              sorted(run["report"] or {}))
        full = client.get(f"/api/admin/learning/runs/{run['id']}").json()
        rep, model = full["report"] or {}, full["model"] or {}
        check(REPORT_KEYS | {"news", "blacklist", "auto", "warning"} <= set(rep), "GET run: the full report", sorted(rep))
        check(rep.get("auto") == {"applied": False, "why_not": "runs started by hand are never applied automatically"}
              and "synthetic demo prices" in rep.get("warning", ""), "a manual run (auto mode, demo feed) is never applied",
              (rep.get("auto"), rep.get("warning")))
        trend = DEFAULT_APP_SETTINGS["modes"]["trend"]       # long-term: its own D1 bars on the H1 engine
        check(trend["timeframe"] == "D1" and model.get("fingerprint") == L.fingerprint(trend["strategy"], trend["params"], "D1")
              and model.get("strategy") == trend["strategy"] and model.get("timeframe") == "D1" and model.get("lessons") == [],
              "the proposal carries the strategy fingerprint (the trend mode's own timeframe)", model)
        check(rep.get("news") == L.confidence_bands([], 70) and rep.get("blacklist") == [],
              "report: AI-signal bands (none) and blacklist (empty)", (rep.get("news"), rep.get("blacklist")))
        check(engine.model is None and client.get("/api/admin/learning").json()["model"] is None, "no model became active")
        check(any(a["action"] == "learning_run" for a in client.get("/api/admin/audit").json()), "manual run is audited")

    # ── signals: meta stays on the server, skip reasons per user ──
    with SessionLocal() as db:
        sig = Signal(mode="trend", symbol="EURUSD", side="buy", price=1.1, sl=1.098, tp=1.103, timeframe="H1",
                     meta={"feed": "demo", "model": None, "features": {"session": "asia", "vol": "low", "trend": "with"}})
        db.add(sig)
        db.commit()
        sig_id = sig.id
        check("meta" not in sig.to_dict() and sig.to_dict()["id"] == sig_id, "Signal.to_dict has no meta")
    use(client, alice)
    rows = {x["id"]: x for x in client.get("/api/signals").json()}
    check(sig_id in rows and "meta" not in rows[sig_id] and rows[sig_id]["skip_reason"] is None and rows[sig_id]["for_me"],
          "GET /api/signals: no meta, no skip reason yet", rows.get(sig_id))

    # ── a client's limits: NaN / Infinity / numbers too big for a float are refused, never saved as the maximum ──
    mine0 = client.get("/api/me/settings").json()["settings"]
    for key, bad in (("risk_per_trade_pct", "NaN"), ("risk_per_trade_pct", "Infinity"), ("risk_per_trade_pct", "-Infinity"),
                     ("risk_per_trade_pct", "1e400"), ("max_open_trades", "NaN"), ("max_open_trades", "1" + "0" * 400),
                     ("daily_loss_limit_pct", "NaN")):
        r = client.put("/api/me/settings", headers=raw_json, content='{"modes": {"trend": false}, "%s": %s}' % (key, bad))
        check(r.status_code == 400 and detail(r) == "invalid_number", f"PUT /api/me/settings {key} {bad[:9]} -> 400 invalid_number",
              err(r))
    check(client.get("/api/me/settings").json()["settings"] == mine0, "a rejected client limit saves nothing (not even the rest)")
    r = client.put("/api/me/settings", headers=H, json={"risk_per_trade_pct": 99})
    check(r.status_code == 200 and r.json()["settings"]["risk_per_trade_pct"] == 5.0, "a finite risk above the limit is clamped (5 %)",
          err(r))
    client.put("/api/me/settings", headers=H, json={"risk_per_trade_pct": mine0["risk_per_trade_pct"]})

    # ── auto-trading check: POST /api/test-signal ──
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and detail(r) == "connector_offline", "test signal: connector offline -> 400", err(r))
    use(client, bob)
    bws, welcome = connect(client, tokens["bob"], version="1.1.2", demo=False)
    check(welcome["type"] == "welcome" and all("meta" not in s for s in welcome["open_signals"])
          and sig_id in {s["id"] for s in welcome["open_signals"]}, "connector welcome: open signals without meta")
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and detail(r) == "demo_only", "test signal: real account -> 400 demo_only", err(r))
    close(bws)
    use(client, alice)
    aws, _ = connect(client, tokens["alice"], version="1.1.1", demo=True)
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and detail(r) == "connector_outdated_test", "test signal: connector 1.1.1 -> 400 connector_outdated_test",
          err(r))
    close(aws)
    aws, _ = connect(client, tokens["alice"], version="1.1.2", demo=True)
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and detail(r) == "execution_not_auto", "test signal: execution 'notify' -> 400 execution_not_auto",
          err(r))
    client.put("/api/me/settings", headers=H, json={"execution": "auto", "modes": {"trend": False, "custom": False,
                                                                                  "news_ai": False}})
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and detail(r) == "no_mode_enabled", "test signal: no mode enabled -> 400 no_mode_enabled", err(r))
    client.put("/api/me/settings", headers=H, json={"modes": {"trend": True}})
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURGBP"})
    check(r.status_code == 400 and detail(r) == "invalid_symbol", "test signal: unknown symbol -> 400 invalid_symbol", err(r))
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 409 and detail(r) == "no_price_yet", "test signal: no candles yet -> 409 no_price_yet", err(r))
    use(client, bob)
    bws, _ = connect(client, tokens["bob"], version="1.1.2", demo=True)
    use(client, alice)
    sync(aws)
    sync(bws)
    engine.candles["EURUSD"] = engine.feed.candles("EURUSD", "H1", 200)
    with SessionLocal() as db:
        n_before = db.scalar(select(func.count(Signal.id)))
    now = time.time()
    r = client.post("/api/test-signal", headers=H, json={"symbol": "eurusd", "side": "sell"})
    tsig = r.json().get("signal", {}) if r.status_code == 200 else {}
    want_id = main.TEST_SIGNAL_BASE + int(now) - 1_750_000_000
    check(r.status_code == 200 and tsig.get("test") is True and tsig.get("symbol") == "EURUSD" and tsig.get("side") == "sell"
          and tsig.get("mode") == "trend" and want_id <= tsig.get("id", 0) <= want_id + 5 and tsig["id"] > 1_000_000_000
          and tsig["sl"] > tsig["price"] > tsig["tp"] and tsig.get("expires_at") and r.json().get("feed") == "demo",
          "POST /api/test-signal: a test signal (id above any real one, sell levels)", err(r) if r.status_code != 200 else tsig)
    got = [m for m in sync(aws) if m["type"] == "signal"]
    check(len(got) == 1 and got[0]["signal"] == tsig, "the test signal goes to the caller's connector", got)
    check(not [m for m in sync(bws) if m["type"] in ("signal", "signal_closed")], "another user's connector gets nothing")
    with SessionLocal() as db:
        check(db.scalar(select(func.count(Signal.id))) == n_before, "a test signal is never stored as a Signal row")
        check(db.scalar(select(AuditLog).where(AuditLog.action == "test_signal")) is not None, "test signal is audited")

    # ── the connector's skip reports ──
    for m in ({"type": "signal_skipped", "signal_id": sig_id, "reason": "spread too wide"},
              {"type": "signal_skipped", "signal_id": tsig.get("id"), "reason": "already have a position on EURUSD"},
              {"type": "signal_skipped", "signal_id": "junk", "reason": "x"},
              {"type": "signal_skipped", "reason": "no id"},
              {"type": "signal_skipped", "signal_id": 1e30, "reason": "huge"}):
        aws.send_json(m)
    sync(aws)
    with SessionLocal() as db:
        skips = [(s.user_id, s.signal_id, s.reason) for s in db.scalars(select(SignalSkip).order_by(SignalSkip.id))]
    check(skips == [(alice_id, sig_id, "spread too wide"), (alice_id, tsig.get("id"), "already have a position on EURUSD")],
          "signal_skipped -> SignalSkip rows (bad ids ignored, socket stays open)", skips)
    rows = {x["id"]: x for x in client.get("/api/signals").json()}
    check(rows.get(sig_id, {}).get("skip_reason") == "spread too wide", "GET /api/signals: the user's skip_reason", rows.get(sig_id))
    use(client, bob)
    rows = {x["id"]: x for x in client.get("/api/signals").json()}
    check(sig_id in rows and rows[sig_id]["skip_reason"] is None, "another user does not see that skip reason", rows.get(sig_id))
    use(client, alice)
    # the newest skip is shown; once the user's connector traded the signal (e.g. Execute after a skip) none is
    with SessionLocal() as db:
        sig2 = Signal(mode="trend", symbol="GBPUSD", side="sell", price=1.3, sl=1.302, tp=1.297, timeframe="H1", meta={})
        db.add(sig2)
        db.commit()
        sig2_id = sig2.id
    for reason in ("spread too wide", "price moved 9 pips"):
        aws.send_json({"type": "signal_skipped", "signal_id": sig2_id, "reason": reason})
    aws.send_json({"type": "trade_opened", "trade": {"ticket": 9100, "signal_id": sig_id, "symbol": "EURUSD", "side": "buy",
                                                     "lots": 0.01, "entry_price": 1.1, "sl": 1.098, "tp": 1.103, "demo": True}})
    sync(aws)
    rows = {x["id"]: x for x in client.get("/api/signals").json()}
    check(rows.get(sig2_id, {}).get("skip_reason") == "price moved 9 pips", "GET /api/signals: the newest skip reason",
          rows.get(sig2_id))
    check(rows.get(sig_id, {}).get("skip_reason") is None, "GET /api/signals: no skip reason once the user traded the signal",
          rows.get(sig_id))

    # ── closing a test signal ──
    r = client.post("/api/test-signal/close", headers=H)
    check(r.status_code == 200 and r.json() == {"ok": True, "signal_id": tsig.get("id"), "tickets": []},
          "POST /api/test-signal/close", err(r))
    msgs = sync(aws)
    closed = [m["signal"] for m in msgs if m["type"] == "signal_closed"]
    check(len(closed) == 1 and closed[0]["id"] == tsig.get("id") and closed[0]["status"] == "closed"
          and closed[0]["test"] is True and not [m for m in msgs if m["type"] == "close"],
          "close: signal_closed to the connector (follow_exits on: no explicit close)", msgs)
    r = client.post("/api/test-signal/close", headers=H)
    check(r.status_code == 404 and detail(r) == "no_test_signal", "close again -> 404 no_test_signal", err(r))

    client.put("/api/me/settings", headers=H, json={"follow_exits": False})
    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    t2 = r.json()["signal"]
    aws.send_json({"type": "trade_opened", "trade": {"ticket": 9001, "signal_id": t2["id"], "symbol": "EURUSD", "side": "buy",
                                                     "lots": 0.01, "entry_price": 1.1, "sl": 1.09, "tp": 1.12, "demo": True}})
    sync(aws)
    r = client.post("/api/test-signal/close", headers=H)
    msgs = sync(aws)
    check(r.status_code == 200 and r.json()["tickets"] == [9001] and {"type": "close", "ticket": 9001} in msgs
          and any(m["type"] == "signal_closed" for m in msgs),
          "close with follow_exits off: signal_closed plus an explicit close of the test trade", (err(r), msgs))

    r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    t3 = r.json().get("signal", {})
    close(aws)
    r = client.post("/api/test-signal/close", headers=H)
    check(r.status_code == 409 and detail(r) == "connector_offline", "close while the connector is offline -> 409", err(r))
    aws, _ = connect(client, tokens["alice"])
    r = client.post("/api/test-signal/close", headers=H)
    closed = {m["signal"]["id"] for m in sync(aws) if m["type"] == "signal_closed"}
    check(r.status_code == 200 and r.json().get("signal_id") == t3.get("id") and t3.get("id") in closed,
          "after a 409 connector_offline the test signal is kept and closed once the connector is back", (err(r), closed))
    aws.send_json({"type": "trade_closed", "trade": {"ticket": 9001, "exit_price": 1.1, "pnl": 0.0}})
    sync(aws)

    # ── test-signal ids: unique and increasing, also within one second and after a restart ──
    clock = lambda: main.TEST_SIGNAL_BASE + int(time.time()) - 1_750_000_000      # noqa: E731
    with SessionLocal() as db:
        a1, a2, a3 = main._new_test_id(db), main._new_test_id(db), main._new_test_id(db)
        check((a2, a3) == (a1 + 1, a1 + 2) and a1 >= clock() - 1, "test ids: one second, three different ids", (a1, a2, a3))
        ahead = clock() + 600            # a connector reported an id sent before a restart (the clock was ahead)
        db.add_all([SignalSkip(user_id=alice_id, signal_id=ahead, reason="x"),
                    SignalSkip(user_id=alice_id, signal_id=ahead + 10 * 86400, reason="junk id: ignored")])
        db.commit()
        main._last_test_id = 0
        b1 = main._new_test_id(db)
    check(b1 == ahead + 1, "test ids after a restart: past the ids connectors reported (a junk one far ahead ignored)",
          (b1, ahead))
    auth._hits.pop(f"test-signal:{alice_id}", None)        # 5 test signals per 5 minutes; the checks below send 3
    sent = []
    for side in ("buy", "sell"):
        r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD", "side": side})
        sent.append(r.json()["signal"]["id"] if r.status_code == 200 else err(r))
    got = [m["signal"]["id"] for m in sync(aws) if m["type"] == "signal"]
    check(all(isinstance(x, int) for x in sent) and sent[0] < sent[1] and got == sent,
          "two test signals in a row: two ids, both sent to the connector", (sent, got))

    # an open test position only the connector knows (a test signal sent before a server restart), and others
    ghost = sent[0] - 5
    aws.send_json({"type": "snapshot", "positions": [{"ticket": 9300, "signal_id": ghost, "symbol": "EURUSD"},
                                                     {"ticket": 9301, "signal_id": None}, {"ticket": 9302, "signal_id": 5}]})
    sync(aws)
    tries = []

    async def flaky(uid, msg):           # the link drops after the first signal_closed
        if msg.get("type") == "signal_closed":
            tries.append(msg["signal"]["id"])
            if len(tries) > 1:
                return False
        return await type(main.hub).to_connector(main.hub, uid, msg)
    main.hub.to_connector = flaky
    try:
        r = client.post("/api/test-signal/close", headers=H)
    finally:
        del main.hub.to_connector
    left = set(main._test_signals.get(alice_id, {}))
    check(r.status_code == 409 and detail(r) == "connector_offline" and tries == [ghost, sent[0]] and left == set(sent),
          "close: a failed send -> 409, the test signals not closed are kept", (err(r), tries, left))
    sync(aws)
    r = client.post("/api/test-signal/close", headers=H)
    msgs = sync(aws)
    closed = sorted(m["signal"]["id"] for m in msgs if m["type"] == "signal_closed")
    check(r.status_code == 200 and closed == sorted([ghost, *sent]) and r.json() == {"ok": True, "signal_id": sent[1], "tickets": [9300]}
          and [m["ticket"] for m in msgs if m["type"] == "close"] == [9300] and not main._test_signals.get(alice_id),
          "close: every test signal of the user together, also one only the connector's positions know "
          "(follow_exits off: its ticket closed too)", (err(r), r.json() if r.status_code == 200 else None, msgs))
    aws.send_json({"type": "snapshot", "positions": []})
    sync(aws)
    r = client.post("/api/test-signal/close", headers=H)
    check(r.status_code == 404 and detail(r) == "no_test_signal", "close with nothing left -> 404", err(r))

    # the account switched to a real one while the price was fetched: refused, nothing sent
    acct = main.hub.connector_state[alice_id]["account"]

    def quote_then_real(_symbol):
        acct["demo"] = False
        return None
    engine.feed.quote = quote_then_real
    try:
        r = client.post("/api/test-signal", headers=H, json={"symbol": "EURUSD"})
    finally:
        del engine.feed.quote
        acct["demo"] = True
    check(r.status_code == 400 and detail(r) == "demo_only" and not [m for m in sync(aws) if m["type"] == "signal"],
          "test signal: the connector is checked again after the price wait (real account now -> 400, nothing sent)", err(r))
    for ws in list(OPEN):
        close(ws)
    return {"founder": founder, "alice": alice, "alice_id": alice_id, "sig_id": sig_id}


def _rows(*where) -> list[LearnRun]:
    with SessionLocal() as db:
        return db.scalars(select(LearnRun).where(*where).order_by(LearnRun.id)).all()


def _add_run(model: dict, status="proposed", decision="improved", trigger="manual") -> int:
    with SessionLocal() as db:
        row = LearnRun(trigger=trigger, mode="trend", decision=decision, status=status, model=model, report={},
                       finished_at=utcnow())
        db.add(row)
        db.commit()
        return row.id


def _during(action, job=L.learn_job):
    """An engine._offload that runs `action` (an admin's click, say) once `job` has replayed, before the engine
    uses the result. Set as engine._offload; `del engine._offload` restores the real one."""
    real = type(engine)._offload

    async def offload(fn, *args):
        out = await real(engine, fn, *args)
        if fn is job:
            await action()
        return out
    return offload


def _set_applied(run_id: int, when):
    with SessionLocal() as db:
        db.get(LearnRun, run_id).applied_at = when
        db.commit()
    engine.load_model()


def _engine(client, ctx):
    call = client.portal.call
    use(client, ctx["founder"])
    demo_feed = engine.feed
    with SessionLocal() as db:
        settings = get_app_settings(db)
    call(engine.regret_check, settings)
    check(engine.learning["regret"] is None, "regret_check does nothing without an active model / on the demo feed")

    # the trend mode trades the stub strategy (saved directly: the API only accepts strategies/ files), on the
    # engine's timeframe (its own longer one has a section of its own: test_own_timeframe)
    with SessionLocal() as db:
        modes = get_app_settings(db)["modes"]
        modes["trend"] = {**modes["trend"], "strategy": STUB, "params": {}, "timeframe": ""}
        save_app_settings(db, {"modes": modes, "symbols": list(SYMS)})
    client.put("/api/admin/engine", headers=H, json={"learning": {"history_bars": 6000, "mode": "auto", "cost_pips": 1.0}})
    with SessionLocal() as db:
        settings = get_app_settings(db)
    fp = L.fingerprint(STUB, {}, "H1")
    check(fp == L.fingerprint(settings["modes"]["trend"]["strategy"], settings["modes"]["trend"]["params"], settings["timeframe"]),
          "engine settings: trend mode uses the stub strategy")

    # ── activate / revert ──
    m1 = {"sl_scale": 1.25, "tp_scale": 1.0, "filters": [OVERLAP], "lessons": [], "fingerprint": fp}
    r1, r_none = _add_run(m1), _add_run(m1, status="none", decision="no_change")
    check(call(engine.activate, r_none, None, "test") is False and call(engine.activate, 9999, None, "test") is False,
          "activate: a run without a proposal, or a missing run -> False")
    check(call(engine.activate, r1, None, "test one") is True and engine.model and engine.model["id"] == r1
          and engine.model["sl_scale"] == 1.25 and engine.model["filters"] == [OVERLAP] and engine.model["fingerprint"] == fp
          and engine.model["applied_at"] is not None, "activate: the run becomes the engine's model", engine.model)
    row = _rows(LearnRun.id == r1)[0]
    check(row.active and row.status == "applied" and row.model["prev_id"] is None, "activate: row active/applied, prev_id None",
          row.to_dict())
    check(engine.model_for("trend", settings) is engine.model and engine.model_for("custom", settings) is None,
          "model_for: the trend mode's model; none for another mode")
    for label, s2 in (("params", {**settings, "modes": {**settings["modes"], "trend": {**settings["modes"]["trend"],
                                                                                    "params": {"x": 1}}}}),
                      ("timeframe", {**settings, "timeframe": "H4"}),
                      ("strategy", {**settings, "modes": {**settings["modes"], "trend": {**settings["modes"]["trend"],
                                                                                      "strategy": "example_ema_cross"}}})):
        check(engine.model_for("trend", s2) is None, f"model_for: None when the {label} differ from what it was learned for")
    check(call(engine.activate, r1, None, "again") is False, "activate: an applied run cannot be applied again")
    r2 = _add_run({**m1, "sl_scale": 1.0})
    call(engine.activate, r2, None, "test two")
    a, b = _rows(LearnRun.id == r1)[0], _rows(LearnRun.id == r2)[0]
    check(not a.active and a.status == "superseded" and a.end_reason == f"replaced by #{r2}" and b.active
          and b.model["prev_id"] == r1 and len(_rows(LearnRun.active.is_(True))) == 1,
          "activate: the previous model is superseded, the new one remembers it", (a.to_dict(), b.to_dict()))
    with SessionLocal() as db:
        db.get(LearnRun, r1).applied_at = utcnow() - timedelta(days=40)
        db.commit()
    t_revert = utcnow()
    back = call(engine.revert, None, "test revert")
    a, b = _rows(LearnRun.id == r1)[0], _rows(LearnRun.id == r2)[0]
    check(back == r1 and a.active and a.status == "applied" and not b.active and b.status == "reverted"
          and b.end_reason == "test revert" and engine.model["id"] == r1, "revert: back to the model it replaced",
          (back, a.to_dict(), b.to_dict()))
    check(a.applied_at >= t_revert and engine.model["applied_at"] == a.applied_at and a.ended_at is None,
          "revert: the restored model counts as applied now (its dwell time and regret weeks start over)",
          (a.applied_at, t_revert))
    back = call(engine.revert, None, "to base", True)
    check(back is None and engine.model is None and _rows(LearnRun.id == r1)[0].status == "reverted"
          and not _rows(LearnRun.active.is_(True)), "revert(to_base): no model at all")
    check(call(engine.revert, None, "nothing") is None, "revert with no active model -> None")
    with SessionLocal() as db:
        acts = [a.action for a in db.scalars(select(AuditLog).where(AuditLog.action.like("learning_%")))]
    check(acts.count("learning_apply") == 2 and acts.count("learning_revert") == 2, "apply / revert are audited", acts)

    # through the API
    r3 = _add_run({**m1, "sl_scale": 0.75})
    r = client.post(f"/api/admin/learning/runs/{r3}/apply", headers=H)
    info = client.get("/api/admin/learning").json()["model"]
    check(r.status_code == 200 and info and info["id"] == r3 and info["applies"] is True and info["sl_scale"] == 0.75
          and info["applied_at"], "POST apply -> the model is active and applies", (err(r), info))
    r = client.post(f"/api/admin/learning/runs/{r1}/apply", headers=H)
    check(r.status_code == 409 and detail(r) == "not_applicable", "POST apply of a reverted run -> 409", err(r))
    r = client.post("/api/admin/learning/revert", headers=H, json={"to_base": True})
    check(r.status_code == 200 and r.json() == {"ok": True, "active": None} and engine.model is None,
          "POST revert {to_base} -> no model", err(r))

    # ── live signals: features/meta, filters, scaled levels ──
    end = pd.Timestamp(utcnow()).floor("h") - pd.Timedelta(hours=1)
    live = planned_frame(L.LIVE_BARS, 77, coin, end=end)
    k = len(live) - 1
    live.loc[k, "plan_entry"], live.loc[k, "plan_side"], live.loc[k, "plan_risk"] = True, "buy", 0.002
    feats = L.features(L.add_context(live.copy()), k, "buy", 3600)
    bt = pd.Timestamp(live["time"].iat[k]).isoformat()        # the bar decided on (the regret check matches it)
    close_k = float(live["close"].iat[k])
    engine.feed = FakeFeed({})
    with SessionLocal() as db:           # an open signal holds its symbol (the API section left one on EURUSD)
        for row in db.scalars(select(Signal).where(Signal.status == "open")):
            row.status = "closed"
        db.commit()
        first = db.scalar(select(func.max(Signal.id))) or 0      # the API section's signals are not the engine's

    def published(symbol):
        with SessionLocal() as db:
            return db.scalars(select(Signal).where(Signal.symbol == symbol, Signal.mode == "trend", Signal.id > first)
                              .order_by(Signal.id.desc())).first()

    call(engine._run_strategies, "EURUSD", live, settings, 3600)
    s = published("EURUSD")
    check(s and s.meta == {"feed": "fake", "model": None, "features": feats, "bar_time": bt} and abs(s.sl - (close_k - 0.002)) < 1e-12
          and abs(s.tp - (close_k + 0.002)) < 1e-12 and pd.Timestamp(s.track_from) == live["time"].iat[k] + pd.Timedelta(hours=1),
          "live signal without a model: features in meta, the strategy's levels", s and (s.meta, s.sl, s.tp, s.track_from))
    rb = _add_run({"sl_scale": 1.0, "tp_scale": 1.0, "filters": [{"scope": "*", "dim": "session", "value": feats["session"]}],
                   "lessons": [], "fingerprint": fp})
    call(engine.activate, rb, None, "blocking model")
    call(engine._run_strategies, "GBPUSD", live, settings, 3600)
    blocked = engine.learning["blocked"][0] if engine.learning["blocked"] else {}
    rule = {"scope": "*", "dim": "session", "value": feats["session"]}
    check(published("GBPUSD") is None and blocked.get("symbol") == "GBPUSD" and blocked.get("model") == rb
          and blocked.get("rule") == L.filter_text(rule) and blocked.get("rule_ro") == L.filter_text(rule, "ro"),
          "live signal blocked by the model's filter (no signal, reason kept in EN/RO)", blocked)
    rs = _add_run({"sl_scale": 1.25, "tp_scale": 0.75, "filters": [], "lessons": [], "fingerprint": fp})
    call(engine.activate, rs, None, "scaling model")
    call(engine._run_strategies, "AUDUSD", live, settings, 3600)
    s = published("AUDUSD")
    check(s and abs(s.sl - (close_k - 1.25 * 0.002)) < 1e-12 and abs(s.tp - (close_k + 0.75 * 0.002)) < 1e-12
          and s.meta == {"feed": "fake", "model": rs, "features": feats, "bar_time": bt, "sl_scale": 1.25, "tp_scale": 0.75}
          and f"Self-learning #{rs}: stop ×1.25, target ×0.75." in s.rationale and f"Auto-învățare #{rs}" in s.rationale_ro,
          "live signal with a scaling model: levels scaled, meta and rationale name the model",
          s and (s.sl, s.tp, s.meta, s.rationale))
    # learning off: the strategy runs as written; the stored model applies again once learning is back on
    aud = s.id
    client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "off"}})
    with SessionLocal() as db:
        s_off = get_app_settings(db)
        db.get(Signal, aud).status = "closed"
        db.commit()
    info = client.get("/api/admin/learning").json()["model"]
    call(engine._run_strategies, "AUDUSD", live, s_off, 3600)
    s = published("AUDUSD")
    check(engine.model_for("trend", s_off) is None and info and info["id"] == rs and info["applies"] is False
          and info["stale"] is False and s.id != aud and s.meta == {"feed": "fake", "model": None, "features": feats, "bar_time": bt}
          and abs(s.sl - (close_k - 0.002)) < 1e-12 and abs(s.tp - (close_k + 0.002)) < 1e-12,
          "learning off: no scales, no filters (the model stays stored, shown as not applied)", (info, s.meta, s.sl, s.tp))
    client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "auto"}})
    info = client.get("/api/admin/learning").json()["model"]
    check(info and info["applies"] is True and engine.model_for("trend", settings) is engine.model and engine.model["id"] == rs,
          "learning back on: the stored model applies again", info)
    rx = _add_run({"sl_scale": 1.5, "tp_scale": 1.0, "filters": [], "lessons": [],
                   "fingerprint": L.fingerprint(STUB, {}, "H4")})
    call(engine.activate, rx, None, "model for another timeframe")
    call(engine._run_strategies, "USDCAD", live, settings, 3600)
    s = published("USDCAD")
    check(s and s.meta["model"] is None and "sl_scale" not in s.meta and abs(s.sl - (close_k - 0.002)) < 1e-12,
          "live signal: a model learned for another timeframe is not applied", s and (s.meta, s.sl))
    client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "off"}})
    info = client.get("/api/admin/learning").json()["model"]
    check(info and info["id"] == rx and info["applies"] is False and info["stale"] is True,
          "learning off, a model for another timeframe: stale (the page says why it is ignored, not 'off')", info)
    client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "auto"}})
    # parameters saved back through JSON come back as 2 for 2.0: still the parameters the model was learned for
    s_num = {**settings, "modes": {**settings["modes"], "trend": {**settings["modes"]["trend"], "params": {"x": 2}}}}
    rn = _add_run({"sl_scale": 1.25, "tp_scale": 1.0, "filters": [], "lessons": [],
                   "fingerprint": L.fingerprint(STUB, {"x": 2.0}, "H1")})
    call(engine.activate, rn, None, "learned with x=2.0")
    check(engine.model_for("trend", s_num) is engine.model, "model_for: a model learned with x=2.0 applies to x=2")
    # a model stored by the code before that change (the default EMA strategy's params are numbers too)
    legacy = hashlib.sha1(json.dumps([STUB, {"x": 2}, "H1"], sort_keys=True, default=str).encode()).hexdigest()[:12]
    rl = _add_run({"sl_scale": 1.25, "tp_scale": 1.0, "filters": [], "lessons": [], "fingerprint": legacy})
    call(engine.activate, rl, None, "stored before the fingerprint change")
    check(engine.model_for("trend", s_num) is engine.model,
          "a model stored before the fingerprint change (params hashed as given) still applies to the same "
          "strategy, params and timeframe",
          (legacy, L.fingerprint(STUB, {"x": 2}, "H1")))
    use(client, ctx["alice"])
    rows = client.get("/api/signals").json()
    check(rows and all("meta" not in x for x in rows), "GET /api/signals never exposes meta")
    use(client, ctx["founder"])
    call(engine.revert, None, "clean up", True)

    # ── scheduled runs on a live (fake) feed: auto mode applies a model that passed everything ──
    data = frames(session_pattern, 6000, 60, end=end)
    engine.feed = FakeFeed(data)
    t0 = time.time()
    res = call(engine.run_learning, "scheduled")
    print(f"     (engine run_learning: {time.time() - t0:.1f} s)")
    row = _rows(LearnRun.id == res.get("id", 0))
    row = row[0] if row else None
    rep = (row.report or {}) if row else {}
    check(res.get("decision") == "improved" and row and row.status == "applied" and row.active
          and rep.get("auto") == {"applied": True, "why_not": None} and "warning" not in rep,
          "auto mode, scheduled run, live feed, passed every check -> applied", (res, rep.get("auto"), rep.get("gate", {}).get("why")))
    check(engine.model and engine.model.get("id") == res.get("id") and engine.model["filters"] == [OVERLAP]
          and engine.model_for("trend", settings) is engine.model, "the learned model is active for the trend mode",
          engine.model)
    with SessionLocal() as db:
        a = db.scalar(select(AuditLog).where(AuditLog.action == "learning_apply").order_by(AuditLog.id.desc()))
    check(a and a.actor_email == "system" and "auto mode" in a.details, "auto apply is audited as 'system'", a and a.details)
    res2 = call(engine.run_learning, "scheduled")
    rep2 = _rows(LearnRun.id == res2.get("id", 0))[0].report
    check(res2.get("decision") == "no_change" and res2.get("status") == "none" and engine.model["id"] == res.get("id")
          and rep2["auto"] == {"applied": False, "why_not": "nothing to apply"} and rep2["filters"]["chosen"] == [OVERLAP],
          "learning again with that model active: no_change, model kept", (res2, rep2["auto"]))
    res3 = call(engine.run_learning, "manual")
    rep3 = _rows(LearnRun.id == res3.get("id", 0))[0].report
    check(rep3["auto"]["why_not"] == "runs started by hand are never applied automatically" and engine.model["id"] == res.get("id"),
          "a manual run in auto mode is never applied", rep3["auto"])

    # ── an admin's apply / revert / switch to suggest while a scheduled run replays wins over auto mode ──
    call(engine.revert, None, "before the race checks", True)
    other = _add_run({"sl_scale": 1.0, "tp_scale": 1.0, "filters": [], "lessons": ["keep calm"], "lesson_ids": [7],
                      "fingerprint": L.fingerprint(STUB, {}, "H4")})     # learned for another timeframe

    async def admin_applies():
        await engine.activate(other, None, "applied by an admin")

    async def admin_reverts():
        await engine.revert(None, "reverted by an admin", to_base=True)

    async def admin_suggest():
        with SessionLocal() as db:
            save_app_settings(db, {"learning": {**get_app_settings(db)["learning"], "mode": "suggest"}})

    races = {}
    for name, action in (("apply", admin_applies), ("revert", admin_reverts), ("suggest", admin_suggest)):
        held = engine.model and engine.model["id"]
        engine._offload = _during(action)
        try:
            out = call(engine.run_learning, "scheduled")
        finally:
            del engine._offload
        row = (_rows(LearnRun.id == out.get("id", 0)) or [None])[0]
        races[name] = (held, out, row, engine.model and engine.model["id"])
    changed = {"applied": False, "why_not": "the active model changed during the run"}
    held, out, row, now_id = races["apply"]
    check(held is None and out.get("decision") == "improved" and row.status == "proposed" and not row.active
          and row.report["auto"] == changed and now_id == other,
          "auto mode: a model an admin applied during the run stays; the run is only proposed", (out, row and row.report["auto"]))
    held, out, row, now_id = races["revert"]
    check(held == other and out.get("decision") == "improved" and row.status == "proposed" and row.report["auto"] == changed
          and now_id is None, "auto mode: an admin's revert during the run stays; the run is only proposed",
          (out, row and row.report["auto"], now_id))
    check(row.model.get("lessons") == ["keep calm"] and row.model.get("lesson_ids") == [7],
          "the run carries the active model's approved AI lessons although that model was learned for another timeframe",
          row.model)
    held, out, row, now_id = races["suggest"]
    check(held is None and out.get("decision") == "improved" and row.status == "proposed" and now_id is None
          and row.report["auto"] == {"applied": False, "why_not": "suggest mode: an admin applies proposals"},
          "auto mode switched to suggest during the run: nothing applied", (out, row and row.report["auto"]))
    client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "auto"}})

    # ── daily regret check: a worse model goes back to the plain strategy ──
    bad = _add_run({"sl_scale": 0.75, "tp_scale": 1.0, "filters": [], "lessons": [], "fingerprint": fp})
    call(engine.activate, bad, None, "a bad model")
    since = utcnow() - timedelta(weeks=12)
    with SessionLocal() as db:
        db.get(LearnRun, bad).applied_at = since
        for n in range(10):              # live signals the replay never produced (odd minutes): fidelity alarm
            db.add(Signal(mode="trend", symbol="EURUSD", side="buy", price=1.2, sl=1.19, tp=1.21, status="lost",
                          track_from=since + timedelta(days=n + 1, minutes=17), meta={"model": bad}))
        # left out of the fidelity count: one still open (the replay has no result for it), one of a bar before `since`
        still_open = Signal(mode="trend", symbol="EURUSD", side="buy", price=1.2, sl=1.19, tp=1.21, status="open",
                            track_from=since + timedelta(days=20, minutes=17), meta={"model": bad})
        db.add_all([still_open, Signal(mode="trend", symbol="EURUSD", side="buy", price=1.2, sl=1.19, tp=1.21, status="won",
                                       track_from=since - timedelta(days=2), meta={"model": bad})])
        db.commit()
        still_open = still_open.id
    engine.load_model()
    with SessionLocal() as db:
        settings = get_app_settings(db)

    # an admin applies another model while the check replays: its verdict is dropped, nothing is reverted
    swap = _add_run({"sl_scale": 1.0, "tp_scale": 1.0, "filters": [OVERLAP], "lessons": [], "fingerprint": fp})

    async def admin_swaps():
        await engine.activate(swap, None, "applied by an admin")
    engine.feed = FakeFeed(data, max_history=2500)
    engine._offload = _during(admin_swaps, L.regret_job)
    try:
        call(engine.regret_check, settings)
    finally:
        del engine._offload
    with SessionLocal() as db:
        mode = get_app_settings(db)["learning"]["mode"]
    check(engine.learning["regret"] is None and engine.model["id"] == swap and _rows(LearnRun.id == bad)[0].status == "superseded"
          and mode == "auto", "regret_check: a model applied while it replays stays (verdict dropped, no revert, still auto)",
          (engine.learning["regret"], engine.model, mode))
    check(engine.feed.counts and set(engine.feed.counts) == {2500},
          "regret_check: history clamped to the feed's max_history (12 weeks + warm-up would be ~2800 bars)", engine.feed.counts)
    engine.feed = FakeFeed(data)
    call(engine.activate, bad, None, "the bad model again")
    _set_applied(bad, since)
    errors_before = len(engine.status["errors"])
    call(engine.regret_check, settings)
    reg = engine.learning["regret"] or {}
    check(reg.get("revert") is True and reg.get("model") == bad and reg.get("weeks", 0) >= L.REGRET_WEEKS,
          "regret_check: the worse model is judged", {k: reg.get(k) for k in ("revert", "weeks", "weekly_gain", "why")})
    row = _rows(LearnRun.id == bad)[0]
    with SessionLocal() as db:
        mode = get_app_settings(db)["learning"]["mode"]
        blk = engine._blacklist(db)
    check(engine.model is None and row.status == "reverted" and row.end_reason.startswith("regret check:")
          and not _rows(LearnRun.active.is_(True)), "regret_check: reverted to the plain strategy (not the previous model)",
          (engine.model, row.status, row.end_reason))
    check(mode == "suggest", "regret_check: auto mode switched to suggest", mode)
    check(blk == [{"kind": "scale", "sl_scale": 0.75, "tp_scale": 1.0}], "regret_check: the reverted change is blacklisted", blk)
    check(reg.get("fidelity", {}).get("live") == 10 and reg["fidelity"]["matched"] == 0
          and any("match the replay" in e["message"] for e in engine.status["errors"][:len(engine.status["errors"]) - errors_before]),
          "regret_check: closed live signals the replay does not match raise an engine error (an open one and one of a "
          "bar before activation are left out)", reg.get("fidelity"))
    with SessionLocal() as db:
        db.get(Signal, still_open).status = "closed"
        db.commit()
    res4 = call(engine.run_learning, "scheduled")
    rep4 = _rows(LearnRun.id == res4.get("id", 0))[0].report
    check(rep4["blacklist"] == blk and rep4["auto"]["why_not"] == "suggest mode: an admin applies proposals"
          and all((g["sl_scale"], g["tp_scale"]) != (0.75, 1.0) for g in rep4["scales"]["grid"]),
          "next run: blacklist passed to learn(), suggest mode applies nothing", (rep4["blacklist"], rep4["auto"]))
    check(engine.model is None, "suggest mode: still no model")

    # ── approved AI lessons survive a regret revert, and reach the AI whatever the trend fingerprint ──
    bad2 = _add_run({"sl_scale": 0.75, "tp_scale": 1.0, "filters": [], "lessons": ["L1", "L2"], "lesson_ids": [3, 4],
                     "fingerprint": fp})
    call(engine.activate, bad2, None, "a bad model with lessons")
    _set_applied(bad2, since)
    call(engine.regret_check, settings)
    keep = (_rows(LearnRun.active.is_(True)) or [None])[0]
    check((engine.learning["regret"] or {}).get("model") == bad2 and engine.learning["regret"]["revert"] is True
          and _rows(LearnRun.id == bad2)[0].status == "reverted" and keep and keep.trigger == "regret"
          and keep.status == "applied" and keep.decision == "no_change" and keep.applied_at is not None
          and L.clean(keep.model) == {**L.BASE_MODEL, "lessons": ["L1", "L2"]} and keep.model.get("lesson_ids") == [3, 4]
          and keep.model.get("fingerprint") == fp and engine.model and engine.model["id"] == keep.id,
          "regret revert: the plain strategy plus the reverted model's approved AI lessons (a new 'regret' run)",
          keep and keep.to_dict())
    seen = []

    def fake_analyze(*args):
        seen.append(args[-1])
        raise AI.AIError("test: no AI call")
    on = {**settings, "learning": {**settings["learning"], "mode": "suggest", "ai_lessons": True}}
    real_analyze, AI.analyze_market = AI.analyze_market, fake_analyze
    try:
        for s_ai in (on, {**on, "timeframe": "H4"}, {**on, "learning": {**on["learning"], "mode": "off"}},
                     {**on, "learning": {**on["learning"], "ai_lessons": False}}):
            call(engine._run_ai, s_ai)
    finally:
        AI.analyze_market = real_analyze
    check(seen == [["L1", "L2"], ["L1", "L2"], None, None], "AI prompt: the approved lessons, also after a strategy or "
          "timeframe change; none while learning is off or ai_lessons is off", seen)
    call(engine.revert, None, "clean up", True)

    # ── live fetch: enough bars for the slowest enabled strategy, within the feed's reach ──
    warm = {"enabled": True, "strategy": WARM, "params": {}}
    with_custom = lambda cfg: {**settings, "modes": {**settings["modes"], "custom": cfg}}    # noqa: E731
    engine.feed = FakeFeed({})
    n_warm = engine._live_bars(with_custom(warm))
    engine.feed = FakeFeed({}, max_history=700)
    n_cap = engine._live_bars(with_custom(warm))
    n_bad = engine._live_bars(with_custom({**warm, "strategy": "zz_no_such_strategy"}))
    n_off = engine._live_bars(with_custom({**warm, "enabled": False}))
    check((n_warm, n_cap, n_bad, n_off) == (950, 700, L.LIVE_BARS, L.LIVE_BARS),
          "live bars: warm-up + 50 for a slow strategy, capped by max_history; one that does not load or is off is skipped",
          (n_warm, n_cap, n_bad, n_off))
    with SessionLocal() as db:
        modes = get_app_settings(db)["modes"]
        save_app_settings(db, {"modes": {**modes, "custom": warm}})
    engine.feed = FakeFeed({})
    try:
        call(engine.price_tick)
    finally:
        with SessionLocal() as db:
            save_app_settings(db, {"modes": modes})
    check(engine.feed.counts == [950] * len(SYMS), "price tick: each symbol fetched with the slow strategy's 950 bars",
          engine.feed.counts)

    # ── history within the feed's reach; the stored calendar's event-guard windows go to the replays ──
    ev_t = (end - pd.Timedelta(hours=100)).to_pydatetime()
    with SessionLocal() as db:
        db.add_all([CalendarEvent(id="t-ecb", time=ev_t, currency="EUR", title="ECB rate decision", impact="High"),
                    CalendarEvent(id="t-pmi", time=ev_t, currency="USD", title="PMI", impact="Medium"),
                    CalendarEvent(id="t-boj", time=ev_t, currency="JPY", title="BoJ rate decision", impact="High")])
        db.commit()
    pad = timedelta(minutes=30)
    gw = engine._guard_windows(list(SYMS), 30)
    check(gw == {"EURUSD": [(ev_t - pad, ev_t + pad)], "GBPUSD": [], "AUDUSD": [], "USDCAD": []}
          and engine._guard_windows(list(SYMS), 0) == {},
          "_guard_windows: high-impact releases of either currency, ± event_guard_minutes", gw)
    captured = {}

    async def capture(fn, *args):
        captured[fn.__name__] = args
        return await type(engine)._offload(engine, fn, *args)
    engine._offload = capture
    engine.feed = FakeFeed(data, max_history=3200)
    try:
        out = call(engine.run_learning, "manual")
        rg = _add_run({"sl_scale": 1.0, "tp_scale": 1.0, "filters": [OVERLAP], "lessons": [], "fingerprint": fp})
        call(engine.activate, rg, None, "a model to check")
        _set_applied(rg, since)
        call(engine.regret_check, settings)
    finally:
        del engine._offload
    counts = engine.feed.counts
    check(out.get("decision") in ("improved", "no_change", "rejected") and counts[:len(SYMS)] == [3200] * len(SYMS),
          "run_learning: history clamped to the feed's max_history (6000 bars asked)", (out, counts))
    check(captured.get("learn_job", ())[-2:] == (gw, False) and captured.get("regret_job", ())[-2:] == (gw, False),
          "the event-guard windows go to the learning run's and the regret check's replays (no deferral on the "
          "engine's own timeframe)", {k: v[-2:] for k, v in captured.items()})
    call(engine.revert, None, "clean up", True)

    # the live guard and the replay's skip the same bars (±60 minutes; the engine polls 3 s after a close)
    gdf = planned_frame(400, 5, coin)
    gdf["plan_entry"] = True                     # every bar an entry: every close is judged
    gdf["plan_risk"] = gdf["plan_risk"].where(gdf["plan_risk"] > 0, 20 * PIP)
    k = 350
    close_at = lambda i: (gdf["time"].iat[i] + pd.Timedelta(hours=1)).to_pydatetime()     # noqa: E731
    with SessionLocal() as db:
        db.add(CalendarEvent(id="t-nfp", time=close_at(k), currency="USD", title="Non-farm payrolls", impact="High"))
        db.commit()
    hist = L.History("EURUSD", gdf, Planned(), PIP, 3600, engine._guard_windows(["EURUSD"], 60)["EURUSD"])
    live_g = {}
    try:
        for i in range(k - 4, k + 5):
            engine_mod.utcnow = lambda c=close_at(i) + timedelta(seconds=3): c
            live_g[i] = engine._event_guard("EURUSD", 60) is not None
    finally:
        engine_mod.utcnow = utcnow
    check(live_g == {i: i in hist.guarded for i in live_g} and [i for i, v in live_g.items() if v] == [k - 1, k],
          "event guard: the replay skips exactly the bars the live engine skips", (live_g, sorted(hist.guarded)))
    with SessionLocal() as db:
        for ev in db.scalars(select(CalendarEvent).where(CalendarEvent.id.like("t-%"))):
            db.delete(ev)
        db.commit()

    # ── a scheduled run without enough history stores no run and is retried in an hour; a manual one reports it ──
    engine.feed = FakeFeed({s: f.iloc[-500:] for s, f in data.items()})
    n_rows, t_now = len(_rows(LearnRun.id > 0)), time.time()
    out = call(engine.run_learning, "scheduled")
    check(out == {"error": "no_history"} and len(_rows(LearnRun.id > 0)) == n_rows and not engine.learning["running"]
          and engine.learning["retry_at"] >= t_now + engine_mod.LEARN_RETRY - 1,
          "scheduled run, not enough history: no run stored, retried in an hour", (out, engine.learning["retry_at"] - t_now))
    class BrokenFeed(FakeFeed):
        def candles(self, symbol, _tf, count):
            raise ConnectionError("terminal gone")
    engine.feed = BrokenFeed({})
    engine.learning["retry_at"], n_rows, t_now = 0.0, len(_rows(LearnRun.id > 0)), time.time()
    out = call(engine.run_learning, "scheduled")
    check("terminal gone" in (out.get("error") or "") and len(_rows(LearnRun.id > 0)) == n_rows
          and engine.learning["retry_at"] >= t_now + engine_mod.LEARN_RETRY - 1,
          "scheduled run, the feed fails loading history: no run stored, retried in an hour (not every minute)",
          (out, engine.learning["retry_at"] - t_now))
    out = call(engine.run_learning, "manual")
    row = (_rows(LearnRun.id == (out.get("id") or 0)) or [None])[0]
    check(row is not None and row.trigger == "manual" and row.decision == "error" and row.finished_at is not None
          and "ConnectionError: terminal gone" in (row.report or {}).get("error", "") and not engine.learning["running"],
          "manual run, the feed fails loading history: an error run the admin sees", (out, row and row.report))
    engine.feed = FakeFeed({s: f.iloc[-500:] for s, f in data.items()})
    out = call(engine.run_learning, "manual")
    row = (_rows(LearnRun.id == (out.get("id") or 0)) or [None])[0]
    check(row is not None and row.decision == "error" and "not enough price history" in (row.report or {}).get("error", ""),
          "manual run, not enough history: an error run the admin sees", (out, row and row.report))
    engine.learning["retry_at"] = 0.0
    engine.feed = demo_feed
    _learn_tick(client)


def _learn_tick(client):
    """The weekly run: caught up after downtime, retried hourly after a failure, at most 3 failed runs a week."""
    call = client.portal.call
    now = utcnow()
    client.put("/api/admin/engine", headers=H, json={"learning": {"mode": "suggest", "weekday": (now.weekday() - 1) % 7,
                                                                  "hour_utc": 0}})
    with SessionLocal() as db:
        cfg = get_app_settings(db)["learning"]
        due = engine._learn_due(cfg, utcnow())
        for row in db.scalars(select(LearnRun).where(LearnRun.trigger == "scheduled")):
            row.created_at = due - timedelta(days=7)            # every run so far: last week's
        db.commit()
    check(due == (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0),
          "_learn_due: the latest scheduled weekday and hour", due)
    calls = []

    async def fake_run(trigger):
        calls.append(trigger)
        return {}
    scenarios = (("not run since the scheduled time (server was down) -> runs", [], {}, 1),
                 ("failed 2 h ago -> tried again", [("error", -2)], {}, 1),
                 ("failed 10 minutes ago -> waits for the hour", [("error", -1 / 6)], {}, 0),
                 ("cut short by a restart ('running') 2 h ago -> tried again", [("running", -2)], {}, 1),
                 ("failed twice -> a third try", [("error", -3), ("error", -2)], {}, 1),
                 ("failed 3 times -> waits for next week", [("error", -5), ("error", -4), ("error", -2)], {}, 0),
                 ("done -> nothing more this week", [("no_change", -2)], {}, 0),
                 ("done after a failure -> nothing more", [("error", -5), ("improved", -3)], {}, 0),
                 ("no history 10 minutes ago (retry_at) -> waits", [], {"retry_at": time.time() + 3000}, 0),
                 ("learning off -> nothing", [], {"mode": "off"}, 0))
    engine.run_learning = fake_run
    engine.learning["last_regret_check"] = time.time()
    try:
        for label, tries, extra, want in scenarios:
            with SessionLocal() as db:
                rows = [LearnRun(trigger="scheduled", decision=d, created_at=utcnow() + timedelta(hours=h)) for d, h in tries]
                db.add_all(rows)
                db.commit()
                ids = [r.id for r in rows]
                if "mode" in extra:
                    save_app_settings(db, {"learning": {**cfg, "mode": extra["mode"]}})
            engine.learning["retry_at"] = extra.get("retry_at", 0.0)
            calls.clear()
            call(engine.learn_tick)
            check(calls == ["scheduled"] * want, f"learn_tick: {label}", calls)
            with SessionLocal() as db:
                for i in ids:
                    db.delete(db.get(LearnRun, i))
                db.commit()
                save_app_settings(db, {"learning": cfg})
    finally:
        del engine.run_learning
        engine.learning["retry_at"] = 0.0


# ── runner ─────────────────────────────────────────────────────
def _timeout():
    print("FAIL timeout: the test did not finish in 300 s", flush=True)
    faulthandler.dump_traceback(all_threads=True)
    os._exit(3)


def main_():
    started = time.time()
    watchdog = threading.Timer(300, _timeout)
    watchdog.daemon = True
    watchdog.start()
    check(DATABASE_URL == os.environ["DATABASE_URL"] and str(TMP.as_posix()) in DATABASE_URL, "test database is a temp file")
    try:
        for title, fn in (("1. time units", test_units), ("2. walk_bar", test_walk_bar), ("3. splits / embargo", test_period),
                          ("4. clustered", test_clustered), ("5. learn: no edge", test_learn_edgeless),
                          ("6. learn: planted patterns", test_learn_planted), ("7. helpers", test_helpers),
                          ("8. regret", test_regret), ("8b. event guard (replay)", test_guard),
                          ("9. AI news stats", test_news_stats),
                          ("10/11. API + engine", test_api_engine)):
            print(f"\n── {title} ──")
            t0 = time.time()
            try:
                fn()
            except Exception:
                check(False, f"{title}: raised", traceback.format_exc())
            print(f"     ({title}: {time.time() - t0:.1f} s)")
    finally:
        watchdog.cancel()
        db_engine.dispose()
        after = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}
        check(after == _data_before, "nothing written to data/")
        check(not any(getattr(h, "baseFilename", "").endswith("server.log") and not isinstance(h, logging.NullHandler)
                      for h in logging.getLogger().handlers), "no handler writing logs/server.log")
        shutil.rmtree(TMP, ignore_errors=True)
    if FAILED:
        print("\nfailed:\n  " + "\n  ".join(FAILED))
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - started:.1f} s)")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main_()
