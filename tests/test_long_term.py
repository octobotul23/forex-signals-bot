"""Long-term strategy on its own timeframe (D1 while the engine reads H1): the rollover's quiet window, which
timeframe a mode decides on, when the engine decides on a D1 bar (first close past the rollover, late looks, the
decided bar saved across restarts, stale D1 prices, weekends, no refetch before a newer bar can close, a failing
fetch), the signal it publishes (price and levels of now, timeframe D1, bar_time), exit and reverse on one bar (engine
and replay, also decided late), bars_held as the replay counts it, signals left open on the mode's old timeframe,
the replay's deferred event guard, the admin setting, the regret check's bar times, the demo feed's quote, the
user_trades migration (columns one by one, strategy back-fill and re-tag), the v1.3.0 migration (bursts.kind and
analysis, GBPJPY and XAUUSD appended once to the stored pairs), its rollback (updates/v1.3.0 deploy_130.py) and a
pair whose bars fail (price tick, learning).

    .venv\\Scripts\\python tests\\test_long_term.py

Runs on a throw-away SQLite file with the demo price feed; never writes to data/ or logs/, never uses the
network, never starts the engine's loops. Exit code 0 = every check passed.
"""
import asyncio
import copy
import importlib.util
import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import types
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-long-term-"))
DB_FILE = TMP / "platform.db"
DATA_DIR = ROOT / "data"

# settings are read when server modules are imported, so configure first (.env never overrides these)
os.environ.update(DATABASE_URL=f"sqlite:///{DB_FILE.as_posix()}", PRICE_FEED="demo", COOKIE_SECURE="false",
                  SECRET_KEY="long-test-" + "k" * 40, DEMO_SECONDS_PER_BAR="0", ANTHROPIC_API_KEY="",
                  ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="")
# server.main attaches logs/server.log unless the root logger already has a handler for that file
_guard = logging.NullHandler()
_guard.baseFilename = str((ROOT / "logs" / "server.log").resolve())
_console = logging.StreamHandler()
_console.setLevel(logging.ERROR)
logging.getLogger().handlers[:0] = [_guard, _console]
_data_before = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, inspect, select, text  # noqa: E402
from sqlalchemy.orm import Session         # noqa: E402

import server.db as D                      # noqa: E402
import server.engine as E                  # noqa: E402
import server.main as main                 # noqa: E402
from server import learning as L           # noqa: E402
from server.db import SessionLocal, Signal, get_app_settings, get_state, save_app_settings, set_state  # noqa: E402
from server.db import engine as db_engine  # noqa: E402
from server.engine import engine, mode_timeframe  # noqa: E402
from server.feeds.prices import TF_SECONDS, DemoFeed, quiet_until  # noqa: E402
from server.settings import DATABASE_URL, DEFAULT_APP_SETTINGS  # noqa: E402
from strategies import Signal as StratSignal  # noqa: E402
from strategies import Strategy            # noqa: E402

H = {"x-fx": "1"}                          # the CSRF header every state-changing API call needs
FAILED: list[str] = []
RISK = 0.005                               # the stub's stop distance (50 pips); its target is twice that
NOW = [datetime(2026, 7, 16)]              # the engine's clock (server.engine.utcnow) while it is patched
HELD: list[int] = []                       # position.bars_held the stub's exit rule was given, in order
KEY = "trend|EURUSD|D1"                    # the trend mode's decided D1 bar of EURUSD, in engine_state mode_bars


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


def dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


class Plan(Strategy):
    """Test stub: enters where the frame's `go` column says (buy/sell), stop RISK and target 2 x RISK from the close;
    exits where `out` is set (like a crossover's opposite cross, whatever the position). Bars without these columns
    (the demo feed's) never trade."""
    name = "plan (test stub)"
    warmup_bars = 0
    exit_uses_levels = False

    def entry_signal(self, df, i):
        side = df["go"].iat[i] if "go" in df else ""
        if side not in ("buy", "sell"):
            return None
        c, sg = float(df["close"].iat[i]), 1 if side == "buy" else -1
        return StratSignal(side, sl=c - sg * RISK, tp=c + 2 * sg * RISK, comment="plan")

    def exit_signal(self, df, i, position):
        HELD.append(position.bars_held)
        return bool(df["out"].iat[i]) if "out" in df else False


class Warm(Plan):
    """Test stub: needs 900 bars before its first decision (more than learning.LIVE_BARS)."""
    name = "warm (test stub)"
    warmup_bars = 900


def register(cls, name: str) -> str:
    """Make `cls` loadable as strategies.<name> (in memory only, no file)."""
    mod = types.ModuleType(f"strategies.{name}")
    cls.__module__ = mod.__name__
    setattr(mod, cls.__name__, cls)
    sys.modules[mod.__name__] = mod
    return name


STUB, WARM = register(Plan, "zz_test_long_stub"), register(Warm, "zz_test_long_warm")


def frame(times, seed: int, step: float) -> pd.DataFrame:
    """Random-walk bars at `times`, no entries and no exits (set `go` / `out` where wanted)."""
    rng = np.random.default_rng(seed)
    n = len(times)
    close = 1.1 + np.cumsum(rng.normal(0, step, n))
    open_ = np.r_[1.1, close[:-1]]
    return pd.DataFrame({"time": pd.DatetimeIndex(times).as_unit("s"), "open": open_,
                         "high": np.maximum(open_, close) + np.abs(rng.normal(0, step / 3, n)),
                         "low": np.minimum(open_, close) - np.abs(rng.normal(0, step / 3, n)), "close": close,
                         "go": np.array([""] * n, dtype=object), "out": np.zeros(n, bool)})


def d1_frame(last_open: str, n: int = 400, seed: int = 1) -> pd.DataFrame:
    """Daily bars of a New-York-close broker in the US summer: each opens at 21:00 UTC (17:00 New York)."""
    return frame(pd.date_range(end=dt(last_open), periods=n, freq="D"), seed, 0.006)


def h1_frame(last_open: str, n: int = 700, seed: int = 2, gap: tuple[str, str] | None = None) -> pd.DataFrame:
    """Hourly bars up to `last_open`, without the hours in `gap` (a weekend)."""
    times = pd.date_range(end=dt(last_open), periods=n, freq="h")
    if gap:
        times = times[(times < dt(gap[0])) | (times >= dt(gap[1]))]
    return frame(times, seed, 0.0008)


class FakeFeed:
    """A live (non-demo) feed by default, serving prepared frames by (symbol, timeframe). `asked`: the timeframes
    fetched."""

    def __init__(self, frames: dict, name: str = "fake"):
        self.frames, self.name, self.asked = frames, name, []

    def bar_seconds(self, tf):
        return TF_SECONDS[tf]

    def candles(self, symbol, tf, count):
        self.asked.append(tf)
        df = self.frames.get((symbol, tf))
        if df is None:
            return pd.DataFrame(columns=["time", "open", "high", "low", "close"])
        return df.iloc[-count:].reset_index(drop=True).copy()

    def quote(self, _symbol):
        return None


def settings_with(trend: dict | None = None, **kw) -> dict:
    s = copy.deepcopy(DEFAULT_APP_SETTINGS)
    s["symbols"] = ["EURUSD"]
    s["modes"]["trend"] = {"enabled": True, "strategy": STUB, "timeframe": "D1", "params": {}, **(trend or {})}
    s["modes"]["news_ai"]["enabled"] = False
    return {**s, **kw}


CFG = {"enabled": True, "strategy": STUB, "params": {}}


async def fake_loop(fn, every, first_delay=0):
    pass


def forget():
    """No bar decided on yet: neither in the engine's memory nor saved."""
    engine.mode_bar = {}
    with SessionLocal() as db:
        set_state(db, "mode_bars", {})


def saved() -> dict:
    with SessionLocal() as db:
        return get_state(db, "mode_bars") or {}


def restart() -> E.Engine:
    """A new engine as after a restart of the server (started, its loops never run), on the test's feed."""
    e = E.Engine()
    e._loop = fake_loop
    asyncio.run(e.start())
    e.feed = engine.feed
    return e


def mode_bars(h1: pd.DataFrame, now: str, keep: bool = True, eng: E.Engine | None = None):
    """_mode_bars for the trend mode on EURUSD D1 at `now`; `keep`: keep what was decided on before."""
    NOW[0] = dt(now)
    if not keep:
        forget()
    engine.feed.asked.clear()
    return asyncio.run((eng or engine)._mode_bars("trend", "EURUSD", "D1", CFG, h1, 3600))


class Grab(logging.Handler):
    """The engine's warnings while attached."""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.seen: list[str] = []
        self.levels: list[tuple[int, str]] = []         # (level, message) of each

    def emit(self, record):
        self.seen.append(record.getMessage())
        self.levels.append((record.levelno, record.getMessage()))


def signals(**where) -> list[Signal]:
    with SessionLocal() as db:
        q = select(Signal).order_by(Signal.id)
        for k, v in where.items():
            q = q.where(getattr(Signal, k) == v)
        return db.scalars(q).all()


# ── pure functions ──────────────────────────────────────────────
def test_quiet_until():
    cases = [  # (UTC, expected end of the rollover's quiet window or None)
        # US summer (EDT, UTC-4): New York 17:00-20:00 = 21:00-24:00 UTC
        ("2026-07-15 20:59:59", None), ("2026-07-15 21:00:00", "2026-07-16 00:00:00"),
        ("2026-07-15 23:59:59", "2026-07-16 00:00:00"), ("2026-07-16 00:00:00", None),
        # US winter (EST, UTC-5): 22:00-01:00 UTC
        ("2026-01-14 21:59:59", None), ("2026-01-14 22:00:00", "2026-01-15 01:00:00"),
        ("2026-01-15 00:59:59", "2026-01-15 01:00:00"), ("2026-01-15 01:00:00", None),
        # DST starts Sunday 2026-03-08 02:00 New York: Saturday's window in EST, Sunday's in EDT
        ("2026-03-07 22:00:00", "2026-03-08 01:00:00"), ("2026-03-08 00:30:00", "2026-03-08 01:00:00"),
        ("2026-03-08 20:30:00", None), ("2026-03-08 21:00:00", "2026-03-09 00:00:00"),
        # DST ends Sunday 2026-11-01 02:00 New York: Saturday's window in EDT, Sunday's in EST
        ("2026-10-31 21:00:00", "2026-11-01 00:00:00"), ("2026-11-01 05:30:00", None),
        ("2026-11-01 21:30:00", None), ("2026-11-01 22:00:00", "2026-11-02 01:00:00"),
    ]
    got = [(t, quiet_until(dt(t))) for t, _ in cases]
    bad = [(t, g, w) for (t, g), (_, w) in zip(got, cases) if g != (dt(w) if w else None)]
    check(not bad, "quiet_until: New York 17:00-20:00 in summer and winter, across both DST Sundays, end exclusive",
          bad)


def test_mode_timeframe():
    s = settings_with()
    cases = {"D1": "D1", "": "H1", "M15": "H1", "H1": "H1", "W1": "H1", None: "H1"}
    got = {k: mode_timeframe(settings_with({"timeframe": k}), "trend") for k in cases}
    check(got == cases, "mode_timeframe: the mode's own timeframe only when it is a known, longer one", got)
    no_key = copy.deepcopy(s)
    del no_key["modes"]["trend"]["timeframe"]
    check(mode_timeframe(no_key, "trend") == "H1" and mode_timeframe(s, "news_ai") == "H1"
          and mode_timeframe(s, "nope") == "H1" and mode_timeframe({**s, "timeframe": "D1"}, "trend") == "D1",
          "mode_timeframe: no timeframe key, a mode without one, an unknown mode; the engine already on D1")
    check(DEFAULT_APP_SETTINGS["modes"]["trend"]["timeframe"] == "D1"
          and DEFAULT_APP_SETTINGS["modes"]["custom"]["timeframe"] == ""
          and mode_timeframe(DEFAULT_APP_SETTINGS, "trend") == "D1",
          "defaults: long-term (trend) on D1, custom on the engine's timeframe")


def test_replay_reverse():
    raw = frame(pd.date_range("2026-01-05", periods=400, freq="h"), 3, 0.0001)    # calm: no stop or target hit
    raw.loc[320, "go"], raw.loc[330, "go"] = "buy", "sell"
    raw.loc[[320, 330, 340], "out"] = True              # 320: the entry bar itself never exits
    h = L.History("EURUSD", raw, Plan({}), 1e-4, 3600)
    trades, seen, _ = L.replay(h, L.BASE_MODEL, 0.0)
    got = [(t["i"], t["side"], t["exit_i"], t["reason"]) for t in trades]
    check(sorted(h.entries) == [320, 330] and seen == 2
          and got == [(320, "buy", 330, "exit rule"), (330, "sell", 340, "exit rule")],
          "replay: the bar whose exit rule closes a trade opens the next one (exit and reverse)", (got, seen))


def test_history_defer():
    d1 = d1_frame("2026-07-14 21:00")       # the newest bar closes 07-15 21:00 UTC; its decision is due at 00:00
    k = len(d1) - 1
    d1.loc[k, "go"] = "buy"
    windows = {
        "a release at the close": [(dt("2026-07-15 20:40"), dt("2026-07-15 21:40"))],
        "a release at the decision": [(dt("2026-07-15 23:45"), dt("2026-07-16 00:45"))],
        "a window ending at the decision": [(dt("2026-07-15 23:00"), dt("2026-07-16 00:00"))],
    }
    got = {name: tuple(k in L.History("EURUSD", d1, Plan({}), 1e-4, 86400, w, defer).guarded for defer in (False, True))
           for name, w in windows.items()}
    want = {"a release at the close": (True, False), "a release at the decision": (False, True),
            "a window ending at the decision": (False, False)}
    check(got == want, "History(defer): the event guard is checked when the D1 decision is made, 3 h after the close "
          "(not deferred: at the close)", got)


# ── when the engine decides on a D1 bar ─────────────────────────
def test_mode_bars():
    d1 = d1_frame("2026-07-14 21:00")       # closes 07-15 21:00 UTC, due 07-16 00:00 UTC (New York 20:00)
    bar, iso = d1["time"].iat[-1], "2026-07-14T21:00:00"
    engine.feed = FakeFeed({("EURUSD", "D1"): d1})

    out = mode_bars(h1_frame("2026-07-15 22:00"), "2026-07-15 23:00:03", keep=False)
    check(out is None and engine.feed.asked == [] and KEY not in engine.mode_bar,
          "in the rollover's quiet window: nothing decided, nothing fetched")
    out = mode_bars(h1_frame("2026-07-15 22:00"), "2026-07-16 00:00:05")
    check(out is None and engine.feed.asked == ["D1"] and KEY not in engine.mode_bar and saved() == {},
          "a look at the 23:00 close (now past the window): not yet, the bar stays undecided")
    out = mode_bars(h1_frame("2026-07-15 23:00"), "2026-07-16 00:00:03")
    check(out is not None and out["time"].iat[-1] == bar and engine.mode_bar.get(KEY) == bar and saved() == {KEY: iso},
          "the first close past the window (00:00 UTC = broker 03:00): decided on, saved as decided", saved())
    out = mode_bars(h1_frame("2026-07-16 00:00"), "2026-07-16 01:00:03")
    check(out is None and engine.feed.asked == [],
          "the next hour: not decided on again, not even fetched (no newer D1 bar can close before 07-16 21:00)",
          engine.feed.asked)
    out = mode_bars(h1_frame("2026-07-16 20:00"), "2026-07-16 20:59:59")
    check(out is None and engine.feed.asked == [], "... nor the last hour before the next D1 close")
    engine.feed = FakeFeed({("EURUSD", "D1"): d1_frame("2026-07-15 21:00")})
    out = mode_bars(h1_frame("2026-07-16 23:00"), "2026-07-17 00:00:03")
    check(out is not None and engine.feed.asked == ["D1"] and saved() == {KEY: "2026-07-15T21:00:00"},
          "the next D1 bar: fetched and decided on at its due time, saved")
    out = mode_bars(h1_frame("2026-07-17 23:00"), "2026-07-18 00:00:03")
    check(out is None and engine.feed.asked == ["D1"] and saved() == {KEY: "2026-07-15T21:00:00"},
          "a day later the D1 prices still end at that bar: fetched (a newer one is due), not decided on again")

    # late: the first look after the due time decides, whenever it comes (PC off at broker 03:00, a restart)
    engine.feed = FakeFeed({("EURUSD", "D1"): d1})
    out = mode_bars(h1_frame("2026-07-16 04:00"), "2026-07-16 05:00:05", keep=False)
    check(out is not None and out["time"].iat[-1] == bar and saved() == {KEY: iso},
          "nothing decided on since it was due 5 h ago (server started in the morning): decided on now")
    e = restart()
    out = mode_bars(h1_frame("2026-07-16 05:00"), "2026-07-16 06:00:03", eng=e)
    check(out is None and e.mode_bar.get(KEY) == bar,
          "a new engine after a restart: the saved decision is loaded, the bar is not decided on again", e.mode_bar)
    with SessionLocal() as db:            # the day before's bar was decided; the server restarts in the quiet window
        set_state(db, "mode_bars", {KEY: d1["time"].iat[-2].isoformat()})
    NOW[0] = dt("2026-07-15 22:30")
    e = restart()
    out = mode_bars(h1_frame("2026-07-15 22:00"), "2026-07-15 23:00:03", eng=e)
    check(out is None and e.mode_bar.get(KEY) == d1["time"].iat[-2], "restarted before the bar was due: not yet")
    out = mode_bars(h1_frame("2026-07-15 23:00"), "2026-07-16 00:00:03", eng=e)
    check(out is not None and saved() == {KEY: iso}, "... decided on at its due time")

    # stale D1 prices: the newest bar (07-13 21:00, due 07-15 00:00) stays the newest a whole D1 bar of closes
    engine.feed = FakeFeed({("EURUSD", "D1"): d1.iloc[:-1]})
    old_iso = "2026-07-13T21:00:00"
    out = mode_bars(h1_frame("2026-07-16 01:00"), "2026-07-16 02:00:03", keep=False)
    check(out is not None and saved() == {KEY: old_iso},
          "24 of the engine's closes since it was due (the window's 3 not counted): still decided on, late")
    grab = Grab()
    logging.getLogger("engine").addHandler(grab)
    try:
        out = mode_bars(h1_frame("2026-07-16 02:00"), "2026-07-16 03:00:03", keep=False)
    finally:
        logging.getLogger("engine").removeHandler(grab)
    check(out is None and saved() == {KEY: old_iso} and any("not decided on" in m for m in grab.seen),
          "25 closes: a newer D1 bar should be there (stale prices): saved as decided, skipped with a warning",
          (saved(), grab.seen))
    check(mode_bars(h1_frame("2026-07-16 03:00"), "2026-07-16 04:00:03") is None, "... and never decided on later")
    engine.feed = FakeFeed({("EURUSD", "D1"): d1})
    out = mode_bars(h1_frame("2026-07-16 03:00"), "2026-07-16 04:00:03")
    check(out is not None and saved() == {KEY: iso}, "the D1 prices are back: their newest bar is decided on")
    out = mode_bars(h1_frame("2026-07-16 00:00", n=1), "2026-07-16 01:00:03", keep=False)
    check(out is None and saved() == {KEY: iso}, "a D1 bar due before the engine's oldest bar: saved, not decided on")

    # Friday's bar: due Saturday 00:00 UTC, decided Monday 00:00 UTC (broker 03:00, New York Sunday 20:00)
    fri = d1_frame("2026-07-16 21:00")
    weekend = ("2026-07-17 21:00", "2026-07-19 21:00")
    engine.feed = FakeFeed({("EURUSD", "D1"): fri})
    with SessionLocal() as db:
        set_state(db, "mode_bars", {KEY: fri["time"].iat[-2].isoformat()})
    e = restart()                         # restarted on Saturday
    out = mode_bars(h1_frame("2026-07-19 21:00", gap=weekend), "2026-07-19 22:00:03", eng=e)
    check(out is None and engine.feed.asked == [], "Sunday's open (New York 17:00-20:00): the quiet window")
    out = mode_bars(h1_frame("2026-07-19 23:00", gap=weekend), "2026-07-20 00:00:03", eng=e)
    check(out is not None and out["time"].iat[-1] == fri["time"].iat[-1],
          "Friday's bar is decided on at Monday's first close past the window (2 days after it was due)")
    out = mode_bars(h1_frame("2026-07-20 09:00", gap=weekend), "2026-07-20 10:00:03", keep=False)
    check(out is not None and out["time"].iat[-1] == fri["time"].iat[-1],
          "the PC was off all weekend, first look Monday 10:00 UTC: Friday's bar is still decided on")

    # the demo feed has no rollover: due at the close
    engine.feed = FakeFeed({("EURUSD", "D1"): d1}, name="demo")
    out = mode_bars(h1_frame("2026-07-15 20:00"), "2026-07-15 21:00:03", keep=False)
    check(out is not None, "demo feed: decided on at the D1 close itself (no quiet window)")
    out = mode_bars(h1_frame("2026-07-15 21:00"), "2026-07-15 22:00:03")
    check(out is None and engine.feed.asked == ["D1"],
          "demo feed: the next hour the bar is fetched (no credits to save) but not decided on again")


# ── what the engine publishes ───────────────────────────────────
def test_run_strategies():
    d1 = d1_frame("2026-07-14 21:00")
    d1.loc[len(d1) - 1, "go"] = "buy"
    engine.feed = FakeFeed({("EURUSD", "D1"): d1})
    forget()
    engine.model = None
    s = settings_with()
    NOW[0] = dt("2026-07-15 23:30:05")
    asyncio.run(engine._run_strategies("EURUSD", h1_frame("2026-07-15 22:00"), s, 3600, first=True))
    check(signals() == [], "first look after a start, in the quiet window: no signal")

    # the first look past the window (here also the first after a start): the D1 bar is decided on
    h1 = h1_frame("2026-07-15 23:00")
    NOW[0] = dt("2026-07-16 00:00:03")
    asyncio.run(engine._run_strategies("EURUSD", h1, s, 3600, first=True))
    got = signals()
    sig = got[0] if got else None
    price, close = float(h1["close"].iat[-1]), float(d1["close"].iat[-1])
    check(len(got) == 1 and sig.mode == "trend" and sig.side == "buy" and sig.timeframe == "D1" and sig.status == "open",
          "a D1 buy: one open trend signal on timeframe D1", [(x.mode, x.side, x.timeframe, x.status) for x in got])
    if sig:
        check(sig.price == price and abs(sig.sl - (price - RISK)) < 1e-12 and abs(sig.tp - (price + 2 * RISK)) < 1e-12,
              "price = the newest H1 close (now); stop and target keep their distance from the D1 close",
              (sig.price, price, close, sig.sl, sig.tp))
        check(sig.track_from == dt("2026-07-16 00:00") and sig.meta.get("bar_time") == "2026-07-14T21:00:00"
              and sig.meta.get("feed") == "fake" and sig.meta.get("features") is not None,
              "track_from = the next H1 bar; meta.bar_time = the D1 bar decided on; features",
              (sig.track_from, sig.meta))
        check("Stop-loss 50 pips, take-profit 100 pips (risk:reward 1:2.0)" in sig.rationale,
              "rationale: pips from the price now", sig.rationale)

    NOW[0] = dt("2026-07-16 01:00:03")
    asyncio.run(engine._run_strategies("EURUSD", h1_frame("2026-07-16 00:00"), s, 3600))
    check(len(signals()) == 1, "the next hour: no second signal from the same D1 bar")

    # the next day's bar: the opposite cross closes the buy and opens a sell, on the same bar
    d2 = d1_frame("2026-07-15 21:00")
    d2.loc[len(d2) - 1, ["go", "out"]] = ["sell", True]
    engine.feed = FakeFeed({("EURUSD", "D1"): d2})
    h2 = h1_frame("2026-07-16 23:00")
    NOW[0] = dt("2026-07-17 00:00:03")
    HELD.clear()
    asyncio.run(engine._run_strategies("EURUSD", h2, s, 3600))
    got = signals()
    first, last = (got[0], got[-1]) if len(got) == 2 else (None, None)
    check(first is not None and first.status == "closed" and first.close_reason == "exit rule"
          and first.close_price == float(h2["close"].iat[-1]),
          "exit and reverse: the buy closed by its exit rule at the price now", [(x.status, x.close_reason) for x in got])
    check(last is not None and last.side == "sell" and last.status == "open" and last.meta["bar_time"] == "2026-07-15T21:00:00",
          "... and the same bar opens the sell", [(x.side, x.status) for x in got])
    check(HELD == [1], "bars_held: 1 on the D1 bar after the one decided on (the replay's i - opened), not 0 as "
          "counted from the H1 track_from", HELD)

    # back on the engine's timeframe (""): the first look is skipped, then H1 bars decide, levels from their close.
    # The D1 sell is not the H1 position: no exit rule from H1 for it; the H1 entry closes it first
    s_h1 = settings_with({"timeframe": ""})
    h3 = h1_frame("2026-07-17 05:00")
    h3.loc[len(h3) - 1, ["go", "out"]] = ["buy", True]
    NOW[0] = dt("2026-07-17 06:00:03")
    engine.feed.asked.clear()
    HELD.clear()
    asyncio.run(engine._run_strategies("EURUSD", h3, s_h1, 3600, first=True))
    n_first = len(signals())
    asyncio.run(engine._run_strategies("EURUSD", h3, s_h1, 3600))
    got = signals()
    new = got[-1]
    c3 = float(h3["close"].iat[-1])
    check(n_first == 2 and engine.feed.asked == [] and len(got) == 3 and got[1].status == "closed"
          and got[1].close_reason == "timeframe changed" and got[1].close_price == c3 and HELD == []
          and new.side == "buy" and new.timeframe == "H1" and new.price == c3 and abs(new.sl - (c3 - RISK)) < 1e-12
          and new.meta["bar_time"] == "2026-07-17T05:00:00",
          "engine's timeframe: first look skipped; then the H1 buy is published and the D1 sell left open is closed "
          "('timeframe changed', not by an H1 exit rule)",
          (n_first, engine.feed.asked, HELD, [(x.side, x.timeframe, x.status, x.close_reason) for x in got]))


def test_late_exit():
    """Nothing ran at broker 03:00 (PC off overnight): the morning's first look still exits and reverses."""
    sym = "GBPUSD"
    d1 = d1_frame("2026-07-21 21:00", seed=5)
    d1.loc[len(d1) - 2, "go"] = "buy"                      # 07-20 21:00: the buy, decided 07-22 00:00
    d1.loc[len(d1) - 1, ["go", "out"]] = ["sell", True]    # 07-21 21:00: the opposite cross, due 07-23 00:00
    engine.feed = FakeFeed({(sym, "D1"): d1.iloc[:-1]})
    forget()
    s = settings_with()
    NOW[0] = dt("2026-07-22 00:00:03")
    asyncio.run(engine._run_strategies(sym, h1_frame("2026-07-21 23:00", seed=5), s, 3600))
    buy = signals(symbol=sym)
    check(len(buy) == 1 and buy[0].side == "buy" and buy[0].status == "open", "a D1 buy is open")
    engine.feed.frames[(sym, "D1")] = d1                   # the server was off from 07-22 20:00 to 07-23 05:00
    e = restart()
    h1 = h1_frame("2026-07-23 04:00", seed=5)
    NOW[0] = dt("2026-07-23 05:00:05")
    asyncio.run(e._run_strategies(sym, h1, s, 3600, first=True))
    got = signals(symbol=sym)
    check(len(got) == 2 and got[0].status == "closed" and got[0].close_reason == "exit rule"
          and got[0].close_price == float(h1["close"].iat[-1]) and got[1].side == "sell" and got[1].status == "open"
          and got[1].meta["bar_time"] == "2026-07-21T21:00:00",
          "first look 5 h after the D1 bar was due: the buy is closed by its exit rule and the sell opened",
          [(x.side, x.status, x.close_reason) for x in got])
    NOW[0] = dt("2026-07-23 06:00:03")
    asyncio.run(e._run_strategies(sym, h1_frame("2026-07-23 05:00", seed=5), s, 3600))
    e = restart()
    NOW[0] = dt("2026-07-23 07:00:03")
    asyncio.run(e._run_strategies(sym, h1_frame("2026-07-23 06:00", seed=5), s, 3600, first=True))
    check(len(signals(symbol=sym)) == 2, "the next hour and after another restart: that bar is not decided on again")


def test_bars_held():
    """bars_held counts the bars after the one the signal was decided on (meta.bar_time), as the replay does;
    a signal without bar_time counts from its track_from."""
    d1 = d1_frame("2026-07-21 21:00", seed=7)         # decided 07-23 00:00; the signals' bar: 07-19 21:00
    metas = {"AUDUSD": {"bar_time": "2026-07-19T21:00:00"}, "NZDUSD": None}
    for sym, meta in metas.items():
        with SessionLocal() as db:
            db.add(Signal(mode="trend", symbol=sym, side="buy", timeframe="D1", price=1.1, sl=1.0, tp=1.3,
                          status="open", track_from=dt("2026-07-21 00:00"), meta=meta))
            db.commit()
    engine.feed = FakeFeed({(sym, "D1"): d1 for sym in metas})
    forget()
    s = settings_with()
    NOW[0] = dt("2026-07-23 00:00:03")
    HELD.clear()
    for sym in metas:
        asyncio.run(engine._run_strategies(sym, h1_frame("2026-07-22 23:00", seed=7), s, 3600))
    check(HELD == [2, 1], "bars_held two D1 bars after the decided one: 2 (from bar_time; the H1 track_from would "
          "give 1); without bar_time: from track_from", HELD)


def test_leftover():
    """A trend signal left open on H1 from before the mode moved to D1: not the D1 position (no D1 exit rule for it,
    no block of a D1 entry), closed when the mode enters on its symbol, its stop and target tracked meanwhile."""
    sym = "USDCAD"
    d1 = d1_frame("2026-07-14 21:00", seed=9)
    d1.loc[len(d1) - 1, "out"] = True                     # a D1 exit, no entry
    engine.feed = FakeFeed({(sym, "D1"): d1})
    forget()
    s = settings_with()
    h1 = h1_frame("2026-07-15 23:00", seed=9)
    c = float(h1["close"].iat[-1])
    with SessionLocal() as db:
        db.add(Signal(mode="trend", symbol=sym, side="sell", timeframe="H1", price=c, sl=c + 0.05, tp=c - 0.1,
                      status="open", track_from=dt("2026-07-15 20:00"), meta={"bar_time": "2026-07-15T19:00:00"}))
        db.commit()
    HELD.clear()
    NOW[0] = dt("2026-07-16 00:00:03")
    asyncio.run(engine._run_strategies(sym, h1, s, 3600))
    left = signals(symbol=sym)
    check(len(left) == 1 and left[0].status == "open" and HELD == [],
          "an H1 trend sell left open: not the D1 position, the D1 exit rule does not close it",
          ([(x.status, x.close_reason) for x in left], HELD))

    d2 = d1_frame("2026-07-15 21:00", seed=9)
    d2.loc[len(d2) - 1, "go"] = "sell"                   # a D1 sell: the same direction as the one left open
    engine.feed = FakeFeed({(sym, "D1"): d2})
    h2 = h1_frame("2026-07-16 23:00", seed=9)
    with SessionLocal() as db:                           # another mode's signal on the symbol: never a trend leftover
        news = Signal(mode="news_ai", symbol=sym, side="buy", timeframe="H1", price=c, sl=c - 0.05, tp=c + 0.1,
                      status="open", track_from=dt("2026-07-16 20:00"))
        db.add(news)
        db.commit()
        news_id = news.id
    sent = []

    async def to_connectors(msg, _ids):
        sent.append((msg["type"], msg["signal"]["id"]))
    E.hub.to_connectors = to_connectors
    try:
        NOW[0] = dt("2026-07-17 00:00:03")
        asyncio.run(engine._run_strategies(sym, h2, s, 3600))
    finally:
        del E.hub.to_connectors
    kept = signals(id=news_id)
    check(len(kept) == 1 and kept[0].status == "open" and ("signal_closed", news_id) not in sent,
          "an AI news signal open on the symbol is left alone by the trend entry (only trend leftovers close)",
          [(x.mode, x.status, x.close_reason) for x in kept])
    got = signals(symbol=sym, mode="trend")
    old, new = (got[0], got[-1]) if len(got) == 2 else (None, None)
    check(old is not None and new.side == "sell" and new.timeframe == "D1" and new.status == "open"
          and old.status == "closed" and old.close_reason == "timeframe changed"
          and old.close_price == float(h2["close"].iat[-1]),
          "a D1 sell is not blocked by it: published, and the H1 sell is closed at the price now ('timeframe changed')",
          [(x.side, x.timeframe, x.status, x.close_reason) for x in got])
    check(old is not None and sent == [("signal_closed", old.id), ("signal", new.id)],
          "connectors get the close first, then the new signal (one position per symbol)", sent)

    with SessionLocal() as db:
        db.add(Signal(mode="trend", symbol="EURGBP", side="buy", timeframe="H1", price=1.1, sl=1.0, tp=5.0,
                      status="open", track_from=dt("2026-07-16 00:00")))
        db.commit()
    bars = h1_frame("2026-07-16 02:00", n=3, seed=9)
    bars.loc[2, "low"] = 0.95
    asyncio.run(engine._track_outcomes("EURGBP", bars, 3600))
    got = signals(symbol="EURGBP")
    check(len(got) == 1 and got[0].status == "lost" and got[0].close_reason == "stop-loss",
          "an H1 trend signal left open keeps its stop and target tracking on H1 bars",
          [(x.status, x.close_reason) for x in got])


def test_failing_fetch():
    """A D1 fetch that fails (Twelve Data out of credits) is logged; the tick goes on with the next mode."""

    class Broken(FakeFeed):
        def candles(self, symbol, tf, count):
            if tf == "D1":
                raise RuntimeError("Twelve Data error for EURCHF: out of credits")
            return super().candles(symbol, tf, count)

    sym = "EURCHF"
    engine.feed = Broken({})
    forget()
    s = settings_with()
    s["modes"]["custom"] = {"enabled": True, "strategy": STUB, "timeframe": "", "params": {}}
    h1 = h1_frame("2026-07-16 00:00", seed=11)
    h1.loc[len(h1) - 1, "go"] = "buy"
    NOW[0] = dt("2026-07-16 01:00:03")
    try:
        asyncio.run(engine._run_strategies(sym, h1, s, 3600))
        raised = None
    except Exception as e:
        raised = e
    got = signals(symbol=sym)
    err = engine.status["errors"][0]["message"] if engine.status["errors"] else ""
    check(raised is None and [(x.mode, x.side, x.timeframe) for x in got] == [("custom", "buy", "H1")]
          and "out of credits" in err and "trend" in err,
          "a failing D1 fetch: reported, the next mode (custom on H1) still decides", (raised, got, err))


def test_engine_helpers():
    engine.feed = FakeFeed({})
    s = settings_with()
    check(engine._defers(s, "trend") and not engine._defers(settings_with({"timeframe": ""}), "trend")
          and not engine._defers(s, "custom"),
          "_defers: a live feed and a mode on its own timeframe (what the replay models)")
    engine.feed = FakeFeed({}, name="demo")
    check(not engine._defers(s, "trend"), "_defers: never on the demo feed (no rollover)")
    engine.feed = FakeFeed({})
    warm_d1 = settings_with({"strategy": WARM})
    warm_h1 = settings_with({"strategy": WARM, "timeframe": ""})
    check(engine._live_bars(warm_d1) == L.LIVE_BARS and engine._live_bars(warm_h1) == 950,
          "_live_bars: a mode on its own timeframe does not size the engine's H1 fetch",
          (engine._live_bars(warm_d1), engine._live_bars(warm_h1)))


def test_regret_bar_times():
    since = dt("2026-07-01")
    engine.feed = FakeFeed({})
    engine.model = {"id": 77, "mode": "trend", "applied_at": since, **L.BASE_MODEL}
    with SessionLocal() as db:
        db.add_all([Signal(mode="trend", symbol="EURUSD", side="buy", timeframe="D1", price=1.1, sl=1.09, tp=1.12,
                           status="won", created_at=dt("2026-07-16 00:00:03"), track_from=dt("2026-07-16 00:00"),
                           meta={"model": 77, "bar_time": "2026-07-14T21:00:00"}),
                    Signal(mode="trend", symbol="GBPUSD", side="sell", timeframe="D1", price=1.3, sl=1.31, tp=1.28,
                           status="lost", created_at=dt("2026-07-17 00:00:03"), track_from=dt("2026-07-17 00:00"),
                           meta={"model": 77})])
        db.commit()
    captured = {}

    async def capture(fn, *args):
        captured[fn.__name__] = args
        return {}
    engine._offload = capture
    engine.model_for = lambda mode, settings: engine.model
    try:
        NOW[0] = dt("2026-07-20 12:00")
        asyncio.run(engine.regret_check(settings_with()))
    finally:
        del engine._offload, engine.model_for
        engine.model = None
    args = captured.get("regret_job", ())
    live = sorted(args[7], key=lambda x: x["symbol"]) if len(args) > 9 else None
    check(live == [{"symbol": "EURUSD", "side": "buy", "bar_time": "2026-07-14T21:00:00"},
                   {"symbol": "GBPUSD", "side": "sell", "bar_time": "2026-07-16T00:00:00"}]
          and args[6] == 86400 and args[9] is True,
          "regret check: a signal's D1 bar from meta.bar_time (older ones: the bar before track_from), D1 bars, "
          "deferred replay", args[6:] if args else captured)


def test_demo_quote():
    """The demo feed's quote follows the engine's bars (M30 here), not a mode's D1 bars read after them; an engine
    timeframe change keeps what modes on their own timeframe decided on."""
    with SessionLocal() as db:
        before = get_app_settings(db)
        modes = copy.deepcopy(before["modes"])
        modes["trend"] = {"enabled": True, "strategy": STUB, "timeframe": "D1", "params": {}}
        modes["custom"]["enabled"] = modes["news_ai"]["enabled"] = False
        save_app_settings(db, {"timeframe": "M30", "symbols": ["EURUSD"], "modes": modes})
    feed = engine.feed = DemoFeed()
    engine.mode_bar = {"trend|GBPUSD|D1": pd.Timestamp("2026-07-01")}
    engine.timeframe = "H1"                  # the engine read H1 bars; an admin moved it to M30
    try:
        asyncio.run(engine.price_tick())
    finally:
        with SessionLocal() as db:
            save_app_settings(db, {k: before[k] for k in ("timeframe", "symbols", "modes")})
    m30, d1 = engine.candles.get("EURUSD"), feed._cache.get(("EURUSD", "D1"))
    q = feed.quote("EURUSD")
    check(m30 is not None and d1 is not None and q == float(m30["close"].iat[-1]) != float(d1["close"].iat[-1]),
          "demo feed: the quote is the price of the engine's M30 bars, also after the mode's D1 bars were read",
          (q, m30 is not None and float(m30["close"].iat[-1]), d1 is not None and float(d1["close"].iat[-1])))
    check({"trend|GBPUSD|D1", KEY} <= set(engine.mode_bar),
          "an engine timeframe change keeps the bars modes on their own timeframe decided on", engine.mode_bar)


def test_migration():
    """init_db on a database whose last migration was cut short (user_trades.strategy added; risk, confidence and
    the index not; signals without meta): every column added on its own, the index made; trades get their strategy
    from their burst or their signal's mode, also those an earlier version booked as long-term."""
    path = TMP / "partial.db"
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE signals (id INTEGER NOT NULL PRIMARY KEY, mode VARCHAR(20) NOT NULL, "
              "symbol VARCHAR(20) NOT NULL)")
    c.execute("""CREATE TABLE user_trades (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL,
        signal_id INTEGER, ticket INTEGER NOT NULL, symbol VARCHAR(20) NOT NULL, side VARCHAR(4) NOT NULL,
        lots FLOAT NOT NULL, entry_price FLOAT NOT NULL, sl FLOAT, tp FLOAT, opened_at DATETIME NOT NULL,
        closed_at DATETIME, exit_price FLOAT, pnl FLOAT, status VARCHAR(10) NOT NULL, demo BOOLEAN NOT NULL,
        burst_id INTEGER, strategy VARCHAR(8))""")
    c.executemany("INSERT INTO signals (id, mode, symbol) VALUES (?, ?, 'EURUSD')",
                  [(1, "trend"), (2, "news_ai"), (3, "custom"), (4, "other")])
    rows = {   # trade id: (signal_id, burst_id, strategy before, strategy after)
        1: (1, 7, None, "burst"), 2: (1_000_000_123, None, None, "test"), 3: (1, None, None, "long"),
        4: (2, None, None, "news"), 5: (3, None, None, "custom"), 6: (99, None, None, "long"),
        7: (None, None, None, "long"), 8: (4, None, None, "long"),
        9: (2, None, "long", "news"), 10: (3, None, "long", "custom"), 11: (1_000_000_124, None, "long", "test"),
        12: (1, None, "long", "long"), 13: (None, None, "short", "short"), 14: (None, 8, "burst", "burst"),
    }
    c.executemany("INSERT INTO user_trades (id, user_id, signal_id, ticket, symbol, side, lots, entry_price, "
                  "opened_at, status, demo, burst_id, strategy) VALUES (?, 1, ?, ?, 'EURUSD', 'buy', 0.1, 1.1, "
                  "'2026-01-01 00:00:00', 'closed', 1, ?, ?)", [(i, s, i, b, st) for i, (s, b, st, _) in rows.items()])
    c.commit()
    c.close()
    real, D.engine = D.engine, create_engine(f"sqlite:///{path.as_posix()}")
    err, cols, idx, tables, got = None, set(), set(), [], {}
    try:
        D.init_db()
        D.init_db()                          # the next start: nothing left to add
        insp = inspect(D.engine)
        cols = {x["name"] for x in insp.get_columns("user_trades")} | {"signals." + x["name"]
                                                                      for x in insp.get_columns("signals")}
        idx = {x["name"] for x in insp.get_indexes("user_trades")}
        tables = insp.get_table_names()
        with D.engine.connect() as conn:
            got = dict(conn.execute(text("SELECT id, strategy FROM user_trades")).all())
    except Exception as e:
        err = e
    finally:
        D.engine.dispose()
        D.engine = real
    check(err is None and {"strategy", "risk", "confidence", "signals.meta"} <= cols
          and "ix_user_trades_strategy" in idx and "engine_state" in tables,
          "init_db after a cut-short migration: risk, confidence and signals.meta added, the strategy index made, "
          "engine_state created; a second start runs clean", (err, cols, idx))
    want = {i: r[3] for i, r in rows.items()}
    check(got == want, "trades' strategy: burst, test (test signal ids), long (trend, unknown or no signal), news, "
          "custom; earlier 'long' tags of AI news, custom and test signal trades corrected",
          {i: (got.get(i), w) for i, w in want.items() if got.get(i) != w})


SIX = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"]     # v1.2.0's default pairs


def v120(name: str, symbols: list[str] | None) -> Path:
    """A v1.2.0 database in TMP: one closed burst, the admin's stored pairs (None: nothing stored)."""
    path = TMP / name
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE bursts (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, event_id VARCHAR(64),
        event_title VARCHAR(200) NOT NULL, currency VARCHAR(8), symbol VARCHAR(20) NOT NULL, event_time DATETIME NOT NULL,
        test BOOLEAN NOT NULL, params TEXT, status VARCHAR(16) NOT NULL, side VARCHAR(8), orders INTEGER NOT NULL,
        fills INTEGER NOT NULL, pnl FLOAT, reason VARCHAR(300), created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL, UNIQUE (user_id, symbol, event_time))""")
    c.execute("INSERT INTO bursts (id, user_id, event_id, event_title, currency, symbol, event_time, test, params, "
              "status, orders, fills, pnl, created_at, updated_at) VALUES (1, 1, 'ev-nfp', 'Non-Farm Employment "
              "Change', 'USD', 'EURUSD', '2026-10-02 12:30:00', 0, '{\"trades\": 10}', 'closed', 10, 4, 12.5, "
              "'2026-10-02 12:15:00', '2026-10-02 12:33:00')")
    c.execute("CREATE TABLE app_settings (key VARCHAR(60) NOT NULL PRIMARY KEY, value TEXT)")
    c.execute("CREATE TABLE engine_state (key VARCHAR(60) NOT NULL PRIMARY KEY, value TEXT)")
    c.execute("INSERT INTO app_settings VALUES ('timeframe', '\"H1\"')")
    if symbols is not None:
        c.execute("INSERT INTO app_settings VALUES ('symbols', ?)", (json.dumps(symbols),))
    c.commit()
    c.close()
    return path


def start(path: Path, *steps):
    """init_db on that file as each server start would, then `steps` on a session of it; their results."""
    real, D.engine = D.engine, create_engine(f"sqlite:///{path.as_posix()}")
    try:
        D.init_db()
        with Session(D.engine) as db:
            return [step(db) for step in steps]
    finally:
        D.engine.dispose()
        D.engine = real


def pairs(db) -> list[str]:
    return get_app_settings(db)["symbols"]


def flag(db) -> dict | None:
    return get_state(db, "migr_130_symbols")


def test_migration_130():
    """init_db on a v1.2.0 database: bursts get kind ('burst' on the old rows) and analysis; the admin's stored pairs
    get GBPJPY and XAUUSD once, the flag migr_130_symbols records the pairs the upgrade brought in (a pair already
    there is not; with nothing stored, both: v1.2.0's defaults had neither), and a pair the admin removes afterwards
    stays removed."""
    six = SIX
    old = v120("v120.db", six)
    err, got = None, []
    try:
        start(old)
        got = start(old, pairs, flag, lambda db: db.get(D.Burst, 1).to_dict(),
                    lambda db: {c["name"]: c for c in inspect(db.get_bind()).get_columns("bursts")})
    except Exception as e:
        err = e
    (syms, f, b, cols) = got or (None, None, {}, {})
    check(err is None and syms == six + ["GBPJPY", "XAUUSD"] and f == {"added": ["GBPJPY", "XAUUSD"]},
          "v1.2.0 database, two starts: GBPJPY and XAUUSD appended once, the flag records both", (err, syms, f))
    check({"kind", "analysis"} <= set(cols) and not cols["kind"]["nullable"] and cols["analysis"]["nullable"]
          and b.get("kind") == "burst" and "analysis" in b and b["analysis"] is None and b.get("pnl") == 12.5,
          "bursts: kind (NOT NULL, 'burst' on the old rows) and analysis (NULL) added; to_dict returns both",
          (b, {k: (c["type"], c["nullable"]) for k, c in cols.items() if k in ("kind", "analysis")}))

    v120_insert = text("INSERT INTO bursts (user_id, event_title, symbol, event_time, test, params, status, orders, "
                       "fills, created_at, updated_at) VALUES (1, 'y', 'GBPUSD', '2026-11-06 13:30:00', 0, '{}', "
                       "'planned', 0, 0, '2026-11-06 13:20:00', '2026-11-06 13:20:00')")

    def new_row(db):
        db.add(D.Burst(user_id=1, event_title="x", symbol="XAUUSD", event_time=dt("2026-11-06 13:30"), params={}))
        db.commit()
        with db.get_bind().connect() as conn:          # what v1.2.0's code writes: no kind, no analysis
            conn.execute(v120_insert)
            conn.commit()
            return dict(conn.execute(text("SELECT symbol, kind FROM bursts WHERE id > 1")).all())
    check(start(old, new_row) == [{"XAUUSD": "burst", "GBPUSD": "burst"}],
          "a new row gets kind 'burst', also one inserted without it (a rolled-back v1.2.0 writing)")

    def v120_writes(db):
        with db.get_bind().connect() as conn:
            conn.execute(v120_insert)
            conn.commit()
            return dict(conn.execute(text("SELECT symbol, kind FROM bursts")).all())
    err, got = None, None
    try:
        got = start(TMP / "v130-fresh.db", v120_writes)
    except Exception as e:
        err = e
    check(err is None and got == [{"GBPUSD": "burst"}],
          "a database v1.3.0 created (create_all, no ALTER TABLE): v1.2.0's insert without kind still works, kind "
          "'burst' (the rollback relies on the column's DB default)", (err, got))

    gold = v120("v120-gold.db", six + ["XAUUSD"])
    got = start(gold, pairs, flag)
    check(got == [six + ["XAUUSD", "GBPJPY"], {"added": ["GBPJPY"]}],
          "a stored list that already had XAUUSD: only GBPJPY appended, and only it in the flag", got)

    def removes_gold(db):
        save_app_settings(db, {"symbols": six + ["GBPJPY"]})
    start(old, removes_gold)
    got = start(old, pairs, flag)
    check(got == [six + ["GBPJPY"], {"added": ["GBPJPY", "XAUUSD"]}],
          "the admin removes XAUUSD after the migration: the next start leaves it removed (flag unchanged)", got)

    crosses = ["EURGBP", "EURJPY", "EURCHF", "EURCAD", "EURAUD", "EURNZD", "GBPCHF", "GBPCAD", "GBPAUD", "GBPNZD",
               "AUDJPY", "AUDCAD", "AUDNZD", "NZDJPY"]
    got = [start(v120(f"v120-{n}.db", (six + crosses)[:n]), pairs, flag) for n in (19, 20)]
    check(got == [[six + crosses[:13] + ["GBPJPY"], {"added": ["GBPJPY"]}], [six + crosses, {"added": []}]]
          and D.MAX_SYMBOLS == 20,
          "a stored list near the admin's cap of 20 pairs: only what fits is appended, and only that in the flag", got)

    got = start(v120("v120-defaults.db", None), pairs, flag, lambda db: db.get(D.AppSetting, "symbols"))
    check(got == [DEFAULT_APP_SETTINGS["symbols"], {"added": ["GBPJPY", "XAUUSD"]}, None]
          and {"GBPJPY", "XAUUSD"} <= set(got[0]),
          "no stored list (the defaults in use): nothing stored, the flag records both (v1.2.0's defaults had "
          "neither)", got)


def test_rollback_130():
    """updates/v1.3.0 deploy_130.py rollback_db on a migrated v1.2.0 database: the stored pairs lose those the flag
    says the upgrade brought in, also when they came from the defaults into a list the admin saved later; a pair the
    admin had before stays. The flag goes, so a later v1.3.0 start migrates again. Refused, with nothing changed,
    while a burst is under way or a burst trade is open. bursts.kind and analysis stay."""
    spec = importlib.util.spec_from_file_location("deploy_130", ROOT / "updates" / "v1.3.0" / "deploy_130.py")
    kit = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode, was = True, sys.dont_write_bytecode      # no __pycache__ in updates/
    try:
        spec.loader.exec_module(kit)
    finally:
        sys.dont_write_bytecode = was
    check(set(kit.FINAL) == E.FINAL, "the kit's final burst statuses are server/burst.py FINAL", kit.FINAL)

    def rolled(path: Path):
        """rollback_db on that file: (what it took out, or the SystemExit it raised; the stored pairs; the flag)."""
        try:
            out = kit.rollback_db(path)
        except SystemExit as e:
            out = e
        c = sqlite3.connect(path)
        try:
            row = c.execute("SELECT value FROM app_settings WHERE key = 'symbols'").fetchone()
            f = c.execute("SELECT value FROM engine_state WHERE key = 'migr_130_symbols'").fetchone()
        finally:
            c.close()
        return out, json.loads(row[0]) if row else None, json.loads(f[0]) if f else None

    def sql(path: Path, *stmts):
        c = sqlite3.connect(path)
        try:
            for s in stmts:
                c.execute(s)
            c.commit()
        finally:
            c.close()

    old = v120("rb-v120.db", SIX)
    start(old)
    got = rolled(old)
    check(got == (["GBPJPY", "XAUUSD"], SIX, None),
          "rollback: GBPJPY and XAUUSD taken out of the stored pairs, the flag deleted", got)
    check(start(old, pairs, flag) == [SIX + ["GBPJPY", "XAUUSD"], {"added": ["GBPJPY", "XAUUSD"]}],
          "a later v1.3.0 start migrates again")
    check(rolled(old) == (["GBPJPY", "XAUUSD"], SIX, None), "and rolls back again")

    gold = v120("rb-gold.db", SIX + ["XAUUSD"])
    start(gold)
    got = rolled(gold)
    check(got == (["GBPJPY"], SIX + ["XAUUSD"], None),
          "a stored list that had XAUUSD before the upgrade: only GBPJPY taken out, the admin's gold stays", got)

    bare = v120("rb-defaults.db", None)
    start(bare)
    got = rolled(bare)
    check(got == ([], None, None), "nothing stored: nothing to take out (v1.2.0's defaults apply), the flag deleted",
          got)
    start(bare, lambda db: save_app_settings(db, {"symbols": DEFAULT_APP_SETTINGS["symbols"]}))
    got = rolled(bare)
    check(got == (["GBPJPY", "XAUUSD"], SIX, None),
          "nothing stored at the upgrade, the admin saves the pairs later (the defaults' GBPJPY and XAUUSD): both "
          "taken out", got)

    start(bare, lambda db: save_app_settings(db, {"symbols": SIX + ["GBPJPY"]}))
    sql(bare, "INSERT INTO bursts (user_id, event_title, symbol, event_time, test, params, status, orders, fills, "
              "created_at, updated_at) VALUES (1, 'CPI', 'GBPJPY', '2026-11-06 13:30:00', 0, '{}', 'armed', 10, 0, "
              "'2026-11-06 13:15:00', '2026-11-06 13:29:00')")
    got = rolled(bare)
    check(isinstance(got[0], SystemExit) and "1 burst(s) under way" in str(got[0])
          and got[1:] == (SIX + ["GBPJPY"], {"added": ["GBPJPY", "XAUUSD"]}),
          "a burst under way: refused, nothing changed", got)
    sql(bare, "UPDATE bursts SET status = 'closed' WHERE status = 'armed'",
        "INSERT INTO user_trades (user_id, ticket, symbol, side, lots, entry_price, opened_at, status, demo, burst_id, "
        "strategy) VALUES (1, 77, 'GBPJPY', 'buy', 0.1, 190.5, '2026-11-06 13:30:05', 'open', 1, 2, 'burst')")
    got = rolled(bare)
    check(isinstance(got[0], SystemExit) and "1 burst trade(s) open" in str(got[0])
          and got[1:] == (SIX + ["GBPJPY"], {"added": ["GBPJPY", "XAUUSD"]}),
          "a burst trade still open: refused, nothing changed", got)
    sql(bare, "UPDATE user_trades SET status = 'closed' WHERE ticket = 77",
        "INSERT INTO user_trades (user_id, ticket, symbol, side, lots, entry_price, opened_at, status, demo, burst_id, "
        "strategy) VALUES (1, 79, 'XAUUSD', 'sell', 0.1, 2650.5, '2026-11-06 13:30:04', 'open', 1, 2, 'rapid')")
    got = rolled(bare)
    check(isinstance(got[0], SystemExit) and "1 burst trade(s) open" in str(got[0])
          and got[1:] == (SIX + ["GBPJPY"], {"added": ["GBPJPY", "XAUUSD"]}),
          "a Rapid trade still open: refused, nothing changed", got)
    sql(bare, "UPDATE user_trades SET status = 'closed' WHERE ticket = 79",
        "INSERT INTO user_trades (user_id, ticket, symbol, side, lots, entry_price, opened_at, status, demo, "
        "strategy) VALUES (1, 78, 'EURUSD', 'buy', 0.1, 1.08, '2026-11-06 14:00:00', 'open', 1, 'long')")
    got = rolled(bare)
    c = sqlite3.connect(bare)
    try:
        cols = {r[1] for r in c.execute("PRAGMA table_info(bursts)")}
    finally:
        c.close()
    check(got == (["GBPJPY"], SIX, None) and {"kind", "analysis"} <= cols,
          "once they are over (a long-term trade open does not matter): rolled back; bursts.kind and analysis stay",
          (got, cols))


def test_failing_pair():
    """A pair whose bars fail to load (GBPJPY before the terminal has its history) is warned about once and the other
    pairs still tick; another error is warned about again, and a pair that loads again is cleared. The weekly
    learning leaves such a pair out. Only when every pair fails (the feed is down) do both raise: the price tick on
    every tick, for _loop to report at ERROR as in v1.2.0."""

    class Flaky(FakeFeed):
        broken: dict[str, str] = {}

        def candles(self, symbol, tf, count):
            if symbol in self.broken:
                raise RuntimeError(f"{symbol}: {self.broken[symbol]}")
            return super().candles(symbol, tf, count)

    trio = ["USDCHF", "GBPJPY", "USDJPY"]
    feed = engine.feed = Flaky({(s, "H1"): h1_frame("2026-07-16 00:00", n=L.CTX_BARS + 60, seed=20 + k)
                                for k, s in enumerate(trio) if s != "GBPJPY"})
    feed.broken = {"GBPJPY": "symbol not found"}
    for kept in (engine.last_bar, engine.next_due, engine.failing):
        for s in trio:
            kept.pop(s, None)
    grab = Grab()
    warned = lambda: [m for m in grab.seen if "GBPJPY" in m]                            # noqa: E731
    errors = lambda: [e for e in engine.status["errors"] if "price tick GBPJPY" in e["message"]]  # noqa: E731
    with SessionLocal() as db:                        # every mode off: only the bars are read
        before = get_app_settings(db)
        modes = copy.deepcopy(before["modes"])
        for m in modes.values():
            m["enabled"] = False
        save_app_settings(db, {"timeframe": "H1", "symbols": trio, "modes": modes})
    logging.getLogger("engine").addHandler(grab)
    seen, bug_seen = [], None
    try:
        asyncio.run(engine.price_tick())
        seen.append((sorted(s for s in trio if s in engine.last_bar), dict(engine.failing), len(warned()),
                     len(errors())))
        asyncio.run(engine.price_tick())
        seen.append((len(warned()), len(errors())))
        feed.broken = {"GBPJPY": "no bars yet"}
        asyncio.run(engine.price_tick())
        seen.append((len(warned()), len(errors())))
        feed.broken = {}
        feed.frames[("GBPJPY", "H1")] = h1_frame("2026-07-16 00:00", n=L.CTX_BARS + 60, seed=29)
        asyncio.run(engine.price_tick())
        seen.append(("GBPJPY" in engine.last_bar, "GBPJPY" in engine.failing, len(warned())))

        async def broken(symbol, *_):              # a bug after the fetch (outcomes, strategies, publishing)
            raise KeyError(f"{symbol} outcome")
        engine._track_outcomes = broken
        engine.last_bar.pop("USDCHF", None)          # a new look at USDCHF: its outcomes are tracked
        engine.next_due.pop("USDCHF", None)
        try:
            asyncio.run(engine.price_tick())
            bug = None
        except KeyError as e:
            bug = e
        finally:
            del engine._track_outcomes
        bug_seen = (bug, dict(engine.failing), [m for m in grab.seen if "price tick USDCHF" in m])
        levels = [lv for lv, m in grab.levels if "price tick GBPJPY" in m]

        grab.seen.clear()
        feed.broken = {"GBPJPY": "no history"}
        hist = asyncio.run(engine._history(trio, "H1", L.CTX_BARS + 60))
        hist_warned = [m for m in grab.seen if "GBPJPY left out" in m]
        feed.broken = {s: "terminal gone" for s in trio}
        try:
            asyncio.run(engine._history(trio, "H1", L.CTX_BARS + 60))
            raised = None
        except RuntimeError as e:
            raised = e

        # every pair failing on a price tick (the terminal closed): the first error is raised on each tick
        for s in trio:
            engine.next_due.pop(s, None)             # the next bar is due: every pair is read
        grab.levels.clear()
        down = []
        for _ in range(2):
            try:
                asyncio.run(engine.price_tick())
                down.append(None)
            except RuntimeError as e:
                down.append(str(e))
        down_seen = (down, dict(engine.failing), list(grab.levels))
        # ... which _loop reports as in v1.2.0: at ERROR with its traceback and a new status entry, on every tick
        grab.levels.clear()
        logging.getLogger("engine").propagate = False          # no tracebacks on the console

        async def a_while():
            try:
                await asyncio.wait_for(E.Engine._loop(engine, engine.price_tick, 0.01), 0.5)
            except asyncio.TimeoutError:
                pass
        asyncio.run(a_while())
        looped = ([m for lv, m in grab.levels if lv == logging.ERROR and m == "engine task failed"],
                  [e for e in engine.status["errors"] if e["message"] == "price_tick: RuntimeError: USDCHF: terminal gone"])
        # the feed back with one pair still failing: that pair is warned about once more
        feed.broken = {"GBPJPY": "terminal gone"}
        grab.levels.clear()
        asyncio.run(engine.price_tick())
        back = (dict(engine.failing), list(grab.levels))
    finally:
        logging.getLogger("engine").removeHandler(grab)
        logging.getLogger("engine").propagate = True
        with SessionLocal() as db:
            save_app_settings(db, {k: before[k] for k in ("timeframe", "symbols", "modes")})
    first = seen[0] if seen else None
    check(first and first[0] == ["USDCHF", "USDJPY"]
          and first[1] == {"GBPJPY": "price tick GBPJPY: RuntimeError: GBPJPY: symbol not found"}
          and first[2:] == (1, 1),
          "a pair failing on a price tick: one warning, one status error; the pairs before and after it still tick",
          (seen, grab.seen))
    check(seen[1:3] == [(1, 1), (2, 2)], "the same failure again: no new warning; another error: warned again",
          seen[1:3])
    check(seen[3:] == [(True, False, 2)], "the pair loads again: it ticks, nothing failing, no warning", seen[3:])
    check(bug_seen and isinstance(bug_seen[0], KeyError) and "USDCHF outcome" in str(bug_seen[0])
          and bug_seen[1] == {} and not bug_seen[2],
          "an error after the fetch is no failing pair: it ends the tick, for _loop to report with its traceback "
          "(as in v1.2.0)", bug_seen)
    check(sorted(hist) == ["USDCHF", "USDJPY"] and len(hist_warned) == 1,
          "self-learning history: a pair that fails to load is left out with a warning", (sorted(hist), hist_warned))
    check(raised is not None and "USDCHF: terminal gone" in str(raised),
          "every pair failing (the feed is down): the first error is raised", raised)
    check(levels == [logging.WARNING] * 2, "a failing pair's messages are warnings, not errors", levels)
    check(down_seen == (["USDCHF: terminal gone"] * 2, {}, []),
          "price tick, every pair failing (the feed is down): the first error is raised on every tick; no warning, "
          "no pair marked failing", down_seen)
    check(len(looped[0]) >= 2 and len(looped[1]) >= 2,
          "... reported by _loop on every tick, at ERROR with a new status entry (as in v1.2.0)",
          (len(looped[0]), len(looped[1])))
    gone = "price tick GBPJPY: RuntimeError: GBPJPY: terminal gone"
    check(back == ({"GBPJPY": gone}, [(logging.WARNING, gone)]),
          "the feed back, one pair still failing: no error raised, that pair warned about once", back)

def test_admin(client):
    client.post("/api/setup", headers=H, json={"token": main._setup_token, "email": "founder@test.local",
                                               "password": "founder-pass-1", "name": "Founder"})
    modes = lambda: client.get("/api/admin/engine").json()["settings"]["modes"]    # noqa: E731
    check(modes()["trend"]["timeframe"] == "D1", "GET /api/admin/engine: long-term on D1 by default")
    bad = [client.put("/api/admin/engine", headers=H, json={"modes": {"trend": {"timeframe": v}}}).json().get("detail")
           for v in ("W1", None, 5, ["D1"])]
    check(bad == ["invalid_timeframe"] * 4 and modes()["trend"]["timeframe"] == "D1",
          "PUT: an unknown timeframe -> 400 invalid_timeframe, nothing saved", bad)
    r1 = client.put("/api/admin/engine", headers=H, json={"modes": {"trend": {"timeframe": ""}, "custom": {"timeframe": "H4"},
                                                                    "news_ai": {"timeframe": "D1"}}})
    m = modes()
    check(r1.status_code == 200 and m["trend"]["timeframe"] == "" and m["custom"]["timeframe"] == "H4"
          and "timeframe" not in m["news_ai"],
          "PUT: \"\" (the engine's) and a known timeframe saved; news_ai has none", (r1.status_code, m))
    r2 = client.put("/api/admin/engine", headers=H, json={"modes": {"trend": {"timeframe": "D1"}, "custom": {"timeframe": ""}}})
    check(r2.status_code == 200 and modes()["trend"]["timeframe"] == "D1", "PUT: back to D1")
    # the cap the v1.3.0 migration stops at (db.MAX_SYMBOLS: the admin could not save a longer list again)
    before = client.get("/api/admin/engine").json()["settings"]["symbols"]
    many = SIX + ["EURGBP", "EURJPY", "EURCHF", "EURCAD", "EURAUD", "EURNZD", "GBPCHF", "GBPCAD", "GBPAUD", "GBPNZD",
                  "AUDJPY", "AUDCAD", "AUDNZD", "NZDJPY", "CADJPY"]
    put = lambda syms: client.put("/api/admin/engine", headers=H, json={"symbols": syms})      # noqa: E731
    got = [(r.status_code, r.json().get("detail")) for r in (put(many), put(many[:20]))]
    saved_list = client.get("/api/admin/engine").json()["settings"]["symbols"]
    put(before)
    check(len(many) == 21 == D.MAX_SYMBOLS + 1 and got == [(400, "invalid_symbols"), (200, None)]
          and saved_list == many[:20],
          "PUT symbols: 21 pairs -> 400 invalid_symbols, 20 (MAX_SYMBOLS) saved", (got, saved_list))


def test_flow():
    engine._loop = fake_loop                     # Engine.start creates its tasks, none of them runs
    main.SETUP_FILE = TMP / "setup_token.txt"    # not data/setup_token.txt
    with TestClient(main.app) as client:
        test_admin(client)
    real_utcnow, E.utcnow = E.utcnow, lambda: NOW[0]
    try:
        for t in (test_quiet_until, test_mode_timeframe, test_replay_reverse, test_history_defer, test_mode_bars,
                  test_run_strategies, test_late_exit, test_bars_held, test_leftover, test_failing_fetch,
                  test_engine_helpers, test_regret_bar_times, test_migration, test_demo_quote, test_migration_130,
                  test_rollback_130, test_failing_pair):
            t()
    finally:
        E.utcnow = real_utcnow


def main_():
    check(DATABASE_URL == os.environ["DATABASE_URL"] and str(TMP.as_posix()) in DATABASE_URL, "test database is a temp file")
    try:
        test_flow()
    finally:
        db_engine.dispose()
        after = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}
        check(after == _data_before, "nothing written to data/")
        check(not any(getattr(h, "baseFilename", "").endswith("server.log") and not isinstance(h, logging.NullHandler)
                      for h in logging.getLogger().handlers), "no handler writing logs/server.log")
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nall checks passed")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main_()
