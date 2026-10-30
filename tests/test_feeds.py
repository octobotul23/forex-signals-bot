"""Price feeds (server/feeds/prices.py): MT5 history converted to UTC across DST, Twelve Data history paged past its
5000-bar limit, live quotes on the Twelve Data and demo feeds, and every feed's max_history. Also the symbol contract
(bot/symbols.py: pip, digits, scale) and MT5Broker's symbol handling: gold's pip and freeze level, a hidden symbol
(GBPJPY) selected before its tick, once per connection, and a failing symbol that leaves the others alone.

    .venv\\Scripts\\python tests\\test_feeds.py

MT5Feed runs on the real bot/mt5_broker.py against a fake MetaTrader5 module (the real package is never imported)
whose server clock follows New York close, Europe/Athens or a fixed offset; the expected UTC times come from
hand-written DST rules, not from zoneinfo. Twelve Data runs against a fake requests.get and a fake clock (the 8 s
throttle and the 429 wait advance it instead of sleeping). No network, no MetaTrader 5 terminal, no files written.
Exit code 0 = every check passed.
"""
import logging
import os
import sys
import time
import traceback
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
for _k in ("MT5_SERVER_TZ", "MT5_PATH", "MT5_LOGIN"):     # this process only: the feed and connect() read them
    os.environ.pop(_k, None)

# bot/mt5_broker.py binds MetaTrader5 on import: give it this fake, so the real package is never loaded
FAKE = types.ModuleType("MetaTrader5")
sys.modules["MetaTrader5"] = FAKE

import bot.mt5_broker as MB              # noqa: E402
from server.feeds import prices as P     # noqa: E402

PASSED: list[str] = []
FAILED: list[str] = []
H = 3600


def check(ok, label: str, detail=""):
    d = repr(detail) if not isinstance(detail, str) else detail
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {d[:600]}" if not ok and detail != "" else ""))
    (PASSED if ok else FAILED).append(label)


class Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(record.getMessage())

    def count(self, text: str) -> int:
        return sum(text in m for m in self.lines)


LOG = Records()
_feed_log = logging.getLogger("feed")
_feed_log.addHandler(LOG)
_feed_log.setLevel(logging.INFO)
_feed_log.propagate = False


# ── hand-written DST rules (the expected answers) ───────────────
def ny_close(t: float) -> int:
    """New York close server clock: New York + 7h (bot/mt5_broker.py's own US DST rule)."""
    return (MB._ny_offset(t) + 7) * H


def athens(t: float) -> int:
    """Europe/Athens: UTC+3 from the last Sunday of March to the last Sunday of October (01:00 UTC), else UTC+2."""
    y = datetime.fromtimestamp(t, timezone.utc).year

    def last_sunday(m):
        d = datetime(y, m, 31, 1, tzinfo=timezone.utc)
        return (d - timedelta(days=(d.weekday() + 1) % 7)).timestamp()
    return 3 * H if last_sunday(3) <= t < last_sunday(10) else 2 * H


def fx_hours(n: int, forming: int) -> np.ndarray:
    """The latest n hour starts up to `forming` while FX trades: Sunday 17:00 to Friday 17:00 New York time
    (DST switches happen at weekends, when it is shut)."""
    t = np.arange(forming - int(n * 1.5) * H, forming + 1, H, dtype="int64")
    ny = t + np.array([MB._ny_offset(float(x)) for x in t], dtype="int64") * H
    day, hour = (ny // 86400 + 3) % 7, ny % 86400 // H          # 1970-01-01 was a Thursday: Monday = 0
    shut = ((day == 4) & (hour >= 17)) | (day == 5) | ((day == 6) & (hour < 17))
    return t[~shut][-n:]


def ts(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


# ── fake MetaTrader 5 terminal ──────────────────────────────────
class Terminal:
    """H1 bars of every FX hour up to the one forming now, stamped on a server clock: clock(utc epoch) -> offset."""

    def __init__(self, clock, n=30000):
        self.clock, self.ticking, self.looks, self.reads = clock, True, 0, 0
        self.utc = fx_hours(n, int(time.time() // H) * H)
        self.stamps = self.utc + np.array([clock(float(t)) for t in self.utc], dtype="int64")

    def tick(self):
        self.looks += 1
        now = time.time()
        msc = int((now + self.clock(now)) * 1000) + (self.looks if self.ticking else 0)
        return SimpleNamespace(time_msc=msc if self.ticking else 1_000_000, time=msc // 1000, bid=1.1, ask=1.10005)

    def rates(self, count):
        self.reads += 1
        dt = np.dtype([("time", "<i8"), ("open", "<f8"), ("high", "<f8"), ("low", "<f8"), ("close", "<f8"),
                       ("tick_volume", "<u8"), ("spread", "<i4"), ("real_volume", "<u8")])
        a = np.zeros(min(count, len(self.stamps)), dtype=dt)
        a["time"] = self.stamps[-len(a):]
        a["open"] = a["close"] = 1.1
        a["high"], a["low"], a["spread"] = 1.1005, 1.0995, 7
        return a


TERM: Terminal | None = None
INIT_CALLS: list[dict] = []
FAKE.TIMEFRAME_M1, FAKE.TIMEFRAME_M5, FAKE.TIMEFRAME_M15, FAKE.TIMEFRAME_M30 = 1, 5, 15, 30
FAKE.TIMEFRAME_H1, FAKE.TIMEFRAME_H4, FAKE.TIMEFRAME_D1 = 16385, 16388, 16408
FAKE.ACCOUNT_TRADE_MODE_DEMO = 0
FAKE.initialize = lambda **kw: INIT_CALLS.append(kw) or True
FAKE.account_info = lambda: SimpleNamespace(login=1, server="Fake-Demo", trade_mode=0)
FAKE.last_error = lambda: (1, "Success")
FAKE.symbol_select = lambda s, on: True
FAKE.symbol_info = lambda s: SimpleNamespace(point=0.00001, digits=5, trade_tick_value=1.0, trade_tick_size=0.00001,
                                             volume_min=0.01, volume_step=0.01, volume_max=30.0, trade_stops_level=0)
FAKE.symbol_info_tick = lambda s: TERM.tick()
FAKE.copy_rates_from_pos = lambda s, tf, pos, count: TERM.rates(count)

COLS = ["time", "open", "high", "low", "close", "spread_price"]
OLD_DTYPE = (pd.Series(pd.to_datetime(np.array([0], dtype="int64"), unit="s")) - pd.Timedelta(seconds=0)).dtype


def secs(s: pd.Series) -> np.ndarray:
    return s.values.astype("datetime64[s]").astype("int64")


def mt5_feed(clock, tz, n=30000, offset_hours=None):
    global TERM
    TERM = Terminal(clock, n)
    LOG.lines.clear()
    return P.MT5Feed("", offset_hours, tz)


def test_zone_helpers():
    check(P.server_zone("") == "auto" and P.server_zone(" AUTO ") == "auto" and P.server_zone("fixed") is None
          and P.server_zone("NYClose") is P.NY_CLOSE, "MT5_SERVER_TZ keywords")
    z = P.server_zone("Europe/Athens")
    check(z[0].key == "Europe/Athens" and z[1] == 0, f"IANA zone: {z}")
    for bad in ("Mars/Olympus", "../etc", "nyclose+1", "Europe", "x" * 300):
        try:
            P.server_zone(bad)
            check(False, f"{bad!r} must be refused")
        except RuntimeError as e:
            check("MT5_SERVER_TZ" in str(e) and "Europe/Athens" in str(e), f"{bad!r} refused with a hint", str(e))
    # MEXAtlantic-Demo: UTC+3 now, UTC+2 from 1 Nov 2026 (US DST end); EU and US DST differ in March and Oct/Nov
    for at, want in (("2026-10-28T12:00", 3), ("2026-11-02T12:00", 2), ("2026-03-10T12:00", 3),
                     ("2026-01-15T12:00", 2), ("2026-07-01T00:00", 3)):
        check(P.zone_offset(P.NY_CLOSE, ts(at)) == want * H == ny_close(ts(at)), f"New York close at {at}: UTC+{want}")
    check(P.zone_offset(z, ts("2026-10-28T12:00")) == 2 * H == athens(ts("2026-10-28T12:00")),
          "Europe/Athens is UTC+2 on 28 Oct 2026 (EU DST ended 25 Oct)")

    # finding #10's scenario: a bar seen live in September (server 13:00, UTC+3) and replayed after the switch
    sept = pd.Series(pd.to_datetime(["2026-09-15 13:00", "2026-11-03 13:00"]).astype("datetime64[s]"))
    out = P.to_utc(sept, P.NY_CLOSE)
    check(list(out) == [pd.Timestamp("2026-09-15 10:00"), pd.Timestamp("2026-11-03 11:00")],
          "each bar keeps the offset of its own DST season (one current offset would move September by an hour)",
          list(out))
    check(out.dtype == OLD_DTYPE, f"same dtype as the one-offset shift: {out.dtype}")
    for unit in ("s", "ms", "us", "ns"):
        o = P.to_utc(sept.astype(f"datetime64[{unit}]"), P.NY_CLOSE)
        check(list(o) == list(out) and o.dt.tz is None, f"datetime64[{unit}] in, naive UTC out ({o.dtype})")
    # stamps inside a switch hour (a 24/7 symbol over the weekend) are read on the clock from before the switch
    edge = pd.Series(pd.to_datetime(["2026-11-01 08:30", "2026-03-08 09:30", "2026-11-01 07:30",
                                     "2026-11-01 10:00"]).astype("datetime64[s]"))
    out = P.to_utc(edge, P.NY_CLOSE)
    check(not out.isna().any() and list(out) == [pd.Timestamp("2026-11-01 05:30"), pd.Timestamp("2026-03-08 07:30"),
                                                 pd.Timestamp("2026-11-01 04:30"), pd.Timestamp("2026-11-01 08:00")],
          "repeated and skipped hours: no NaT, no error", list(out))
    check(len(P.to_utc(pd.Series([], dtype="datetime64[s]"), P.NY_CLOSE)) == 0, "empty series")


def test_mt5_nyclose_auto():
    f = mt5_feed(ny_close, "auto")
    t0 = time.time()
    df = f.candles("EURUSD", "H1", 20000)
    check(time.time() - t0 < 10, f"first call learns the clock from ticks ({time.time() - t0:.1f} s)")
    check(f.offset == ny_close(time.time()), f"live offset learned: {f.offset}")
    check(list(df.columns) == COLS and df["time"].dtype == OLD_DTYPE and df["time"].dt.tz is None,
          f"columns and time dtype unchanged: {list(df.columns)} {df['time'].dtype}")
    want = TERM.utc[-20001:-1]
    check(len(df) == 20000 and (secs(df["time"]) == want).all(), "every bar is real UTC, across ~3 years of DST",
          f"{len(df)} bars, {(secs(df['time']) != want[:len(df)]).sum()} wrong")
    stamps = TERM.stamps[-20001:-1]
    one = stamps - f.offset                      # what one current offset gave
    other = one != want
    check(1000 < other.sum() < 19000, f"the history spans both seasons ({other.sum()} bars in the other one)")
    check((secs(df["time"])[~other] == one[~other]).all() and secs(df["time"])[-1] == one[-1],
          "bars of the current season (the live ones) convert exactly as before")
    check(np.allclose(df["spread_price"], 7 * 0.00001), "spread_price")
    check(f._scheme("EURUSD") is P.NY_CLOSE and f._us_dst is True and LOG.count("follows New York close") == 1
          and LOG.count("one offset") == 0, "auto picks New York close (history agrees), logged once", LOG.lines)
    reads = TERM.reads
    df2 = f.candles("EURUSD", "H1", 600)
    check(len(df2) == 600 and (secs(df2["time"]) == want[-600:]).all() and LOG.count("follows New York close") == 1,
          "live-size request: same times, no new log line")
    check(TERM.reads == reads + 1, "the history check is not repeated", TERM.reads - reads)
    # the season is judged at the instant the offset was learned: over the DST weekend it is still the old one
    f.offset, f._learned_at = 3 * H, ts("2026-10-30T20:00")
    check(f._scheme("EURUSD") is P.NY_CLOSE, "UTC+3 learned on Friday 30 Oct 2026 is New York close")
    f.offset, f._learned_at = 3 * H, ts("2026-12-01T12:00")
    check(f._scheme("EURUSD") is None and LOG.count("is not New York close") == 1, "UTC+3 learned in December is not")

    # a short history (one US season): nothing to tell New York close from one offset by, so New York close for now
    f = mt5_feed(ny_close, "auto", n=500)
    df = f.candles("EURUSD", "H1", 400)
    check(f._us_dst is None and (secs(df["time"]) == TERM.utc[-401:-1]).all() and LOG.count("all year") == 0
          and LOG.count("assumed until its history can be checked") == 1,
          "one season of history: undecided, bars real UTC, the choice said to be provisional", LOG.lines)

    # a 24/7 symbol read first (no weekend gaps) cannot tell: skipped for a day, the next symbol is checked at once
    f = mt5_feed(ny_close, "auto")
    real = FAKE.copy_rates_from_pos

    def rates(s, tf, pos, count):
        a = TERM.rates(count)
        if s == "BTCUSD":
            a["time"] = a["time"][-1] - np.arange(len(a))[::-1] * H
        return a
    FAKE.copy_rates_from_pos = rates
    try:
        f.candles("BTCUSD", "H1", 100)
        check(f._us_dst is None and "BTCUSD" in f._us_dst_skip and LOG.count("assumed until") == 1,
              "a 24/7 symbol: undecided, New York close assumed for now", LOG.lines)
        reads = TERM.reads
        f.candles("BTCUSD", "H1", 100)
        check(TERM.reads == reads + 1, "the 24/7 symbol's history is not read again", TERM.reads - reads)
        f.candles("EURUSD", "H1", 100)
        check(f._us_dst is True and TERM.reads == reads + 3 and LOG.count("follows New York close") == 1,
              "the next symbol is checked at once, the verdict logged", (TERM.reads - reads, LOG.lines))
    finally:
        FAKE.copy_rates_from_pos = real

    # a failed history read is tried again in 10 min, not a day
    f = mt5_feed(ny_close, "auto")

    def failing(s, tf, pos, count):
        if count == 7000:
            raise RuntimeError("copy_rates_from_pos failed: (-10005, 'IPC timeout')")
        return TERM.rates(count)
    FAKE.copy_rates_from_pos = failing
    try:
        f.candles("EURUSD", "H1", 100)
        f.candles("EURUSD", "H1", 100)
        check(f._us_dst is None and 590 < f._us_dst_retry - time.time() <= 600
              and LOG.count("tried again in 10 min") == 1, "failed read: retried in 10 min", LOG.lines)
    finally:
        FAKE.copy_rates_from_pos = real
    f._us_dst_retry = 0.0
    f.candles("EURUSD", "H1", 100)
    check(f._us_dst is True and LOG.count("follows New York close") == 1, "then decided", LOG.lines)


def test_mt5_other_clocks():
    # one offset all year that matches New York close today: the week opens in history give it away
    now = ny_close(time.time())
    f = mt5_feed(lambda t: now, "auto")
    df = f.candles("EURUSD", "H1", 20000)
    check(f._us_dst is False and (secs(df["time"]) == TERM.utc[-20001:-1]).all(),
          "a clock on one offset all year, matching New York close now: every bar real UTC")
    check(LOG.count("all year") == 1 and LOG.count("follows New York close") == 0, "said once", LOG.lines)
    f.candles("EURUSD", "H1", 100)
    check(LOG.count("all year") == 1, "not repeated", LOG.lines)

    # a server that is not New York close (here an hour ahead of it, all year): one offset, and a warning, once
    f = mt5_feed(lambda t: ny_close(time.time()) + H, "auto", n=3000)
    df = f.candles("EURUSD", "H1", 2000)
    f.candles("EURUSD", "H1", 2000)
    check((secs(df["time"]) == TERM.stamps[-2001:-1] - f.offset).all() and f.offset == ny_close(time.time()) + H,
          "auto, not New York close: every bar shifted by the live offset (as before)")
    check(LOG.count("is not New York close") == 1 and LOG.count("set MT5_SERVER_TZ") == 1,
          "warned once that history may be an hour off", LOG.lines)

    # a configured IANA zone converts without the live offset: no 3 s wait when no tick arrives
    f = mt5_feed(athens, "Europe/Athens")
    TERM.ticking = False
    t0 = time.time()
    df = f.candles("EURUSD", "H1", 20000)
    check(time.time() - t0 < 2 and f.offset is None, f"no wait for ticks ({time.time() - t0:.1f} s)")
    check(len(df) == 20000 and (secs(df["time"]) == TERM.utc[-20001:-1]).all(), "Europe/Athens server: real UTC")
    TERM.ticking = True
    f.candles("EURUSD", "H1", 10)
    f.candles("EURUSD", "H1", 10)
    check(f.offset == athens(time.time()) and LOG.count("check the setting") == 0,
          "the live clock agrees with Europe/Athens: no warning", LOG.lines)

    # nyclose configured on a server that is not: warned once
    f = mt5_feed(lambda t: ny_close(time.time()) + H, "nyclose", n=3000)
    for _ in range(3):
        f.candles("EURUSD", "H1", 100)
    check(LOG.count("check the setting") == 1, "MT5_SERVER_TZ disagreeing with the live clock: warned once",
          LOG.lines)

    # fixed: one learned offset, no warning (the admin chose it)
    f = mt5_feed(ny_close, "fixed", n=3000)
    df = f.candles("EURUSD", "H1", 2000)
    check(f.zone is None and (secs(df["time"]) == TERM.stamps[-2001:-1] - f.offset).all()
          and LOG.count("MT5_SERVER_TZ") == 0 and LOG.count("New York") == 0,
          "fixed: one offset for every bar, quietly", LOG.lines)

    # MT5_UTC_OFFSET_HOURS pins the offset: auto keeps it for every bar and says why; no tick is looked at
    f = mt5_feed(ny_close, "auto", n=3000, offset_hours=2.5)
    looks = TERM.looks
    df = f.candles("EURUSD", "H1", 100)
    check((secs(df["time"]) == TERM.stamps[-101:-1] - int(2.5 * H)).all() and TERM.looks == looks,
          "MT5_UTC_OFFSET_HOURS: that offset, no tick learning")
    check(LOG.count("set by MT5_UTC_OFFSET_HOURS") == 1, "and the log says the offset is pinned", LOG.lines)

    # an explicit zone with MT5_UTC_OFFSET_HOURS: the zone converts
    f = mt5_feed(ny_close, "nyclose", n=3000, offset_hours=3)
    df = f.candles("EURUSD", "H1", 2000)
    check((secs(df["time"]) == TERM.utc[-2001:-1]).all(), "nyclose wins over a pinned offset")

    # the setting from the environment (settings.py loads .env into it), and through make_feed
    os.environ["MT5_SERVER_TZ"] = "Europe/Athens"
    try:
        f = mt5_feed(athens, None, n=100)
        check(isinstance(f.zone, tuple) and f.zone[0].key == "Europe/Athens", "MT5_SERVER_TZ read from the env")
    finally:
        os.environ.pop("MT5_SERVER_TZ")
    f = P.make_feed("mt5", mt5_server_tz="nyclose")
    check(isinstance(f, P.MT5Feed) and f.zone is P.NY_CLOSE, "make_feed(mt5_server_tz=...)")
    check(P.make_feed("mt5").zone == "auto", "default: auto")
    calls = len(INIT_CALLS)
    try:
        P.MT5Feed("", None, "Mars/Olympus")
        check(False, "a bad MT5_SERVER_TZ must stop the feed")
    except RuntimeError as e:
        check(len(INIT_CALLS) == calls and "Mars/Olympus" in str(e), "bad zone refused before connecting to MT5")


# ── symbols beyond the FX majors (GBPJPY, XAUUSD) ───────────────
class Watch:
    """The fake terminal's Market Watch: `hidden` symbols have no tick until symbol_select shows them (GBPJPY at
    MEXAtlantic), `dead` ones have none even then, `missing` ones the broker does not offer. `calls`: (call, symbol)
    in order."""

    def __init__(self, hidden=(), dead=(), missing=()):
        self.hidden, self.dead, self.missing, self.calls = set(hidden), set(dead), set(missing), []

    def select(self, s, on):
        self.calls.append(("select", s))
        if s in self.missing:
            return False
        self.hidden.discard(s)
        return True

    def tick(self, s):
        self.calls.append(("tick", s))
        return None if s in self.hidden | self.dead | self.missing else TERM.tick()

    def selects(self, s) -> int:
        return self.calls.count(("select", s))


def spec_info(s):
    """symbol_info as MEXAtlantic reports it: gold with 2 digits, 100 oz a lot (a 0.01 move on 1 lot is $1) and a
    freeze level of 20 points; JPY pairs with 3 digits, the rest with 5, both without the freeze field (like the other
    fakes)."""
    if s.startswith("XAU"):
        return SimpleNamespace(point=0.01, digits=2, trade_tick_value=1.0, trade_tick_size=0.01, volume_min=0.01,
                               volume_step=0.01, volume_max=100.0, trade_stops_level=0, trade_freeze_level=20)
    jpy = "JPY" in s
    return SimpleNamespace(point=0.001 if jpy else 0.00001, digits=3 if jpy else 5,
                           trade_tick_value=0.67 if jpy else 1.0, trade_tick_size=0.001 if jpy else 0.00001,
                           volume_min=0.01, volume_step=0.01, volume_max=30.0, trade_stops_level=0)


def test_symbols():
    import yaml
    from bot import symbols as S
    for sym, want in (("EURUSD", (0.0001, 5, 1.0)), ("USDJPY", (0.01, 3, 1.0)), ("GBPJPY", (0.01, 3, 2.0)),
                      ("XAUUSD", (0.1, 2, 5.0)), ("XAUUSD.m", (0.1, 2, 5.0))):
        got = (S.pip_size(sym), S.price_digits(sym), S.symbol_scale(sym))
        check(got == want, f"{sym}: pip {want[0]}, {want[1]} digits, scale {want[2]:g}", got)
    check(P.pip_size is S.pip_size, "prices.pip_size is bot.symbols' (engine, main and learning import it from there)")
    check(P.BASE_PRICES["XAUUSD"] > 1000 and P.BASE_PRICES["GBPJPY"] > 100, "demo feed: gold and GBPJPY have a price")
    q = P.DemoFeed().quote("XAUUSD")
    check(abs(q - P.BASE_PRICES["XAUUSD"]) < 0.2 * P.BASE_PRICES["XAUUSD"], f"demo gold walks near its price: {q}")

    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))   # not load_config: it reads .env
    fx_default = {"symbol_specs": {"default": {"pip": 0.0001, "pip_value_per_lot": 10.0}}}   # v1.2.0's: no gold row
    for c, where in ((cfg, "config.yaml"), ({}, "no symbol_specs"), (fx_default, "only a default row, its FX pip")):
        g = S.spec_from_config("XAUUSD", c)
        check((g.pip, g.digits, g.scale, g.pip_value_per_lot) == (0.1, 2, 5.0, 10.0) and abs(g.point - 0.01) < 1e-12,
              f"spec_from_config XAUUSD ({where}): pip 0.1, 2 digits, point 0.01, scale 5, $10 a pip a lot", g)
    # as v1.2.0 gave them: the pip from the table (the default row for EURUSD), 5 or 3 digits, a point of pip / 10
    for sym, want in (("EURUSD", (0.0001, 10.0, 5)), ("USDJPY", (0.01, 6.7, 3))):
        g = S.spec_from_config(sym, cfg)
        check((g.pip, g.pip_value_per_lot, g.digits, g.scale) == (*want, 1.0) and abs(g.point - want[0] / 10) < 1e-12,
              f"spec_from_config {sym} unchanged", g)


def test_broker_symbols():
    global TERM
    TERM = Terminal(ny_close, 100)
    real = FAKE.symbol_select, FAKE.symbol_info, FAKE.symbol_info_tick
    w = Watch(hidden={"GBPJPY"}, missing={"XAGEUR"})
    FAKE.symbol_select, FAKE.symbol_info, FAKE.symbol_info_tick = w.select, spec_info, w.tick
    try:
        b = MB.MT5Broker()
        b.connect()
        g = b.symbol_spec("XAUUSD")
        check(g.pip == 0.1 and g.digits == 2 and g.point == 0.01 and abs(g.pip_value_per_lot - 10.0) < 1e-9
              and abs(g.freeze_distance - 0.20) < 1e-12 and g.scale == 5.0 and g.min_stop_distance == 0,
              "gold (2 digits): pip 0.1 (not 0.01), $10 a pip a lot, freeze level 20 points = 0.20, scale 5", g)
        for sym in ("EURUSD", "USDJPY", "EURUSD.m"):
            s, info = b.symbol_spec(sym), spec_info(sym)
            old = info.point * 10 if info.digits in (3, 5) else info.point          # v1.2.0's rule
            check(s.pip == old and s.pip_value_per_lot == info.trade_tick_value / info.trade_tick_size * old
                  and (s.digits, s.point, s.freeze_distance, s.scale) == (info.digits, info.point, 0.0, 1.0),
                  f"{sym}: as v1.2.0 (pip {old:g}); no freeze field in the fake: 0", s)

        # GBPJPY is hidden at MEXAtlantic: symbol_info_tick gives None until symbol_select shows it
        check(FAKE.symbol_info_tick("GBPJPY") is None, "the fake hides GBPJPY")
        w.calls.clear()
        check(b.tick("GBPJPY") is not None and w.calls[:2] == [("select", "GBPJPY"), ("tick", "GBPJPY")],
              "tick() on a hidden symbol selects it first", w.calls)
        for _ in range(3):
            b.tick("GBPJPY")
            b.symbol_spec("GBPJPY")
            b.rates("GBPJPY", "H1", 10)
        check(w.selects("GBPJPY") == 1, "symbol_select once across repeated tick / symbol_spec / rates", w.calls)
        b.connect()
        b.tick("GBPJPY")
        b.tick("GBPJPY")
        check(w.selects("GBPJPY") == 2, "selected again after a reconnect (a new terminal session), once", w.calls)
        # taken out of Market Watch by hand while running: that look fails, the next one selects it again
        w.hidden.add("GBPJPY")
        try:
            b.tick("GBPJPY")
            check(False, "a tick that is not there must raise")
        except MB.BrokerError as e:
            check("No tick for GBPJPY" in str(e), f"hidden meanwhile: no tick ({e})")
        check(b.tick("GBPJPY") is not None and w.selects("GBPJPY") == 3, "the next look selects it again", w.calls)
        # a symbol the broker does not offer: refused before its tick, every time (a failed select is not kept)
        for _ in range(2):
            try:
                b.tick("XAGEUR")
                check(False, "a symbol the broker does not offer must raise")
            except MB.BrokerError as e:
                check("not available" in str(e) and ("tick", "XAGEUR") not in w.calls, f"not offered: refused ({e})")
        check(w.selects("XAGEUR") == 2, "a failed select is asked again next time", w.calls)
    finally:
        FAKE.symbol_select, FAKE.symbol_info, FAKE.symbol_info_tick = real


def test_broker_reads_reselect():
    """Taken out of Market Watch after a read, with no tick() in between (the scalper's ticks_since loop, the server
    feed with a fixed MT5_UTC_OFFSET_HOURS): that read fails and the next one selects the symbol again, as v1.2.0,
    which selected it on every read."""
    global TERM
    TERM = Terminal(ny_close, 100)
    names = ("symbol_select", "symbol_info_tick", "copy_rates_from_pos", "copy_rates_range", "copy_ticks_from",
             "COPY_TICKS_INFO")
    real = {k: FAKE.__dict__[k] for k in names if k in FAKE.__dict__}
    w = Watch()

    def rates(s, tf, *_):
        w.calls.append(("rates", s))
        return None if s in w.hidden else TERM.rates(10)

    def ticks(s, frm, count, flags):
        w.calls.append(("ticks", s))
        if s in w.hidden:
            return None
        return np.array([(TERM.tick().time_msc, 1.1, 1.10005)],
                        dtype=[("time_msc", "<i8"), ("bid", "<f8"), ("ask", "<f8")])

    FAKE.symbol_select, FAKE.symbol_info_tick = w.select, w.tick
    FAKE.copy_rates_from_pos = FAKE.copy_rates_range = rates
    FAKE.copy_ticks_from, FAKE.COPY_TICKS_INFO = ticks, 2
    since = datetime.now(timezone.utc).replace(tzinfo=None)
    reads = {"rates": lambda b: len(b.rates("GBPJPY", "H1", 10)),
             "rates_range": lambda b: len(b.rates_range("GBPJPY", "H1", since - timedelta(hours=10), since)),
             "ticks_since": lambda b: len(b.ticks_since("GBPJPY", 1))}
    try:
        b = MB.MT5Broker()
        for name, read in reads.items():
            b.connect()                                  # a new session: nothing selected yet
            w.calls.clear()
            got = [read(b)]
            w.hidden.add("GBPJPY")                       # taken out of Market Watch by hand
            for _ in range(3):
                try:
                    got.append(read(b))
                except MB.BrokerError as e:
                    got.append(type(e).__name__)
            want = 1 if name == "ticks_since" else 10
            check(got == [want, "BrokerError", want, want] and w.selects("GBPJPY") == 2,
                  f"{name}: hidden after a read, that read fails, the next selects it again (once)", (got, w.calls))
    finally:
        for k in names:                                  # the fake as it was: what it did not have, it has not
            if k in real:
                setattr(FAKE, k, real[k])
            else:
                delattr(FAKE, k)


def test_mt5_feed_symbols():
    real = FAKE.symbol_select, FAKE.symbol_info_tick
    w = Watch(hidden={"GBPJPY", "XAUUSD"}, missing={"XAGEUR"})
    FAKE.symbol_select, FAKE.symbol_info_tick = w.select, w.tick
    try:
        f = mt5_feed(ny_close, "auto", n=3000)
        check(f.quote("GBPJPY") == 1.1 and w.calls[:2] == [("select", "GBPJPY"), ("tick", "GBPJPY")],
              "quote of a hidden symbol: selected, then read (v1.2.0 gave None)", w.calls)
        # the first symbol asked is not offered: it fails alone, the next one learns the clock
        try:
            f.candles("XAGEUR", "H1", 100)
            check(False, "a symbol the broker does not offer must raise")
        except MB.BrokerError as e:
            check("not available" in str(e) and f.offset is None, f"not offered: that symbol fails ({e})")
        df = f.candles("EURUSD", "H1", 100)
        check(len(df) == 100 and (secs(df["time"]) == TERM.utc[-101:-1]).all() and f.offset == ny_close(time.time()),
              "the next symbol learns the clock and serves its bars")
        df = f.candles("XAUUSD", "H1", 100)
        check(len(df) == 100 and (secs(df["time"]) == TERM.utc[-101:-1]).all() and "XAUUSD" not in w.hidden,
              "a hidden symbol's bars: selected first")
        # a symbol without ticks once the clock is known: its bars still convert, the others are untouched
        w.dead.add("XAUUSD")
        offset, looks = f.offset, TERM.looks
        df = f.candles("XAUUSD", "H1", 100)
        check(len(df) == 100 and (secs(df["time"]) == TERM.utc[-101:-1]).all() and f.quote("XAUUSD") is None,
              "no ticks for one symbol: its bars still served, its quote None")
        try:
            f.candles("XAGEUR", "H1", 100)
            check(False, "a symbol the broker does not offer must still raise")
        except MB.BrokerError:
            pass
        df = f.candles("EURUSD", "H1", 100)
        check(len(df) == 100 and (secs(df["time"]) == TERM.utc[-101:-1]).all() and f.offset == offset
              and f.quote("EURUSD") == 1.1 and TERM.looks > looks, "the others go on as before")
    finally:
        FAKE.symbol_select, FAKE.symbol_info_tick = real


# ── fake Twelve Data ────────────────────────────────────────────
class Clock:
    def __init__(self):
        self.t, self.sleeps = time.time(), []

    def time(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


class Resp:
    def __init__(self, data):
        self.data = data

    def json(self):
        return self.data


class TwelveData:
    """/time_series like the real one: outputsize 1..5000, newest bars first, optional end_date; /price."""

    def __init__(self, n, inclusive=True):
        forming = int(time.time() // H) * H
        self.t = np.arange(forming - (n - 1) * H, forming + 1, H, dtype="int64")      # every hour, the last forming
        self.close = 1.1 + np.cumsum(np.random.default_rng(7).normal(0, 0.0012, n))
        self.inclusive, self.calls, self.fail, self.raise_on = inclusive, [], {}, set()

    def get(self, url, timeout=None, params=None):
        endpoint = url.rsplit("/", 1)[1]
        self.calls.append((endpoint, dict(params), CLOCK.t))
        i = len(self.calls) - 1
        if i in self.raise_on:
            raise ConnectionError("network down")
        if i in self.fail:
            return Resp(self.fail[i])
        if endpoint == "price":
            return Resp({"price": "1.23456"})
        out = int(params["outputsize"])
        if not 1 <= out <= 5000:
            return Resp({"code": 400, "message": f"Invalid **outputsize** provided: {out}. Accepts values in the range "
                                                 "from 1 to 5000 inclusive.", "status": "error"})
        sel = np.arange(len(self.t))
        if "end_date" in params:
            e = datetime.strptime(params["end_date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            sel = sel[self.t <= e] if self.inclusive else sel[self.t < e]
        if not len(sel):
            return Resp({"code": 400, "message": "No data is available on the specified dates. Try setting different "
                                                 "start/end dates.", "status": "error"})
        values = [{"datetime": datetime.fromtimestamp(int(self.t[k]), timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                   "open": f"{self.close[k] - 0.0003:.5f}", "high": f"{self.close[k] + 0.001:.5f}",
                   "low": f"{self.close[k] - 0.001:.5f}", "close": f"{self.close[k]:.5f}"} for k in sel[-out:][::-1]]
        return Resp({"meta": {"symbol": params["symbol"], "interval": params["interval"]}, "values": values,
                     "status": "ok"})


CLOCK = Clock()


def td_feed(api):
    P.requests = SimpleNamespace(get=api.get)
    return P.TwelveDataFeed("test-key")


def test_twelvedata():
    global CLOCK
    real_requests, real_time = P.requests, P.time
    CLOCK = Clock()
    P.time = CLOCK
    try:
        for inclusive in (True, False):
            api = TwelveData(23000, inclusive)
            f = td_feed(api)
            df = f.candles("EURUSD", "H1", 12000)
            sizes = [c[1]["outputsize"] for c in api.calls]
            check(len(df) == 12000 and (secs(df["time"]) == api.t[-12001:-1]).all()
                  and np.allclose(df["close"], api.close[-12001:-1].round(5)),
                  f"12000 bars paged in requests of <= 5000 (end_date {'in' if inclusive else 'ex'}clusive)",
                  f"{len(df)} bars, outputsizes {sizes}")
            check(len(api.calls) == 3 and max(sizes) <= 5000 and "end_date" not in api.calls[0][1]
                  and all("end_date" in c[1] for c in api.calls[1:]), f"3 requests: {sizes}")
            check(all(c[1]["timezone"] == "UTC" and c[1]["apikey"] == "test-key" and c[1]["symbol"] == "EUR/USD"
                      for c in api.calls), "UTC times, key and symbol on every page")
            gaps = np.diff([c[2] for c in api.calls])
            check((gaps >= 8).all(), f"pages 8 s apart (free tier: 8 requests a minute): {gaps}")
            check(list(df.columns) == ["time", "open", "high", "low", "close"] and df["time"].is_monotonic_increasing
                  and df["time"].is_unique and df["time"].dt.tz is None and df["close"].dtype == float,
                  "ascending, unique, naive UTC, float prices")

        api = TwelveData(23000)
        df = td_feed(api).candles("EURUSD", "H1", 30000)       # finding #2: the learner's default history
        check(len(df) == P.TwelveDataFeed.max_history == 20000 and max(c[1]["outputsize"] for c in api.calls) <= 5000,
              f"30000 asked: clamped to max_history ({len(df)} bars, {len(api.calls)} requests)")

        api = TwelveData(23000)
        df = td_feed(api).candles("EURUSD", "H1", 600)
        check(len(api.calls) == 1 and api.calls[0][1]["outputsize"] == 601 and len(df) == 600
              and (secs(df["time"]) == api.t[-601:-1]).all(), "a live-size request is one call, as before")

        for inclusive in (True, False):
            api = TwelveData(7000, inclusive)
            LOG.lines.clear()
            df = td_feed(api).candles("EURUSD", "H1", 12000)
            check(len(df) == 6999 and (secs(df["time"]) == api.t[:-1]).all() and LOG.count("6999 of 12000") == 1
                  and len(api.calls) == 2, "history shorter than asked: what there is, a warning, no extra request",
                  (len(df), LOG.lines, len(api.calls)))
        api = TwelveData(10000, inclusive=False)
        LOG.lines.clear()
        df = td_feed(api).candles("EURUSD", "H1", 12000)       # exactly 2 full pages, then "No data" from the API
        check(len(df) == 9999 and LOG.count("9999 of 12000 EURUSD H1 bars (No data") == 1 and len(api.calls) == 3,
              "the API's 'No data' past the oldest bar: what there is", (len(df), LOG.lines, len(api.calls)))

        api = TwelveData(23000)
        api.fail[0] = {"code": 401, "message": "**apikey** parameter is incorrect", "status": "error"}
        try:
            td_feed(api).candles("EURUSD", "H1", 100)
            check(False, "an error on the first request must raise")
        except RuntimeError as e:
            check("apikey" in str(e) and "EURUSD" in str(e), f"first request refused: raises ({e})")

        api = TwelveData(23000)
        api.fail[1] = {"code": 429, "message": "You have run out of API credits for the current minute.",
                       "status": "error"}
        CLOCK.sleeps.clear()
        df = td_feed(api).candles("EURUSD", "H1", 8000)
        check(len(df) == 8000 and 60 in CLOCK.sleeps and len(api.calls) == 3
              and api.calls[2][1] == api.calls[1][1], "429 on a page: waits a minute and asks again",
              (len(df), CLOCK.sleeps, len(api.calls)))

        # a network error on an older page: the newer pages are kept; on the first page it raises
        api = TwelveData(23000)
        api.raise_on.add(1)
        LOG.lines.clear()
        df = td_feed(api).candles("EURUSD", "H1", 12000)
        check(len(df) == 4999 and (secs(df["time"]) == api.t[-5000:-1]).all() and len(api.calls) == 2
              and LOG.count("4999 of 12000 EURUSD H1 bars (ConnectionError: network down)") == 1,
              "a failed older page: the bars already fetched, and a warning", (len(df), LOG.lines))
        api = TwelveData(23000)
        api.raise_on.add(0)
        try:
            td_feed(api).candles("EURUSD", "H1", 12000)
            check(False, "a network error on the first page must raise")
        except ConnectionError:
            check(True, "a network error on the first page raises (the engine retries the symbol)")

        # requests puts the URL in its error text, the key included: never in a log line or an error
        class Leaky(TwelveData):
            def get(self, url, timeout=None, params=None):
                if len(self.calls) in self.raise_on:
                    self.calls.append((url.rsplit("/", 1)[1], dict(params), CLOCK.t))
                    raise ConnectionError(f"Max retries exceeded with url: {url}?apikey={params['apikey']}")
                return super().get(url, timeout, params)
        api = Leaky(23000)
        api.raise_on.add(1)
        LOG.lines.clear()
        td_feed(api).candles("EURUSD", "H1", 12000)
        check(LOG.count("apikey=***") == 1 and LOG.count("test-key") == 0, "the key masked in the warning", LOG.lines)
        api = Leaky(23000)
        api.raise_on.add(0)
        try:
            td_feed(api).candles("EURUSD", "H1", 100)
            check(False, "a network error on the first page must raise")
        except ConnectionError as e:
            check("test-key" not in str(e) and "apikey=***" in str(e) and e.__suppress_context__,
                  f"the key masked in the error, its chain dropped: {e}")

        # finding #16: a fresh reference price for test signals
        api = TwelveData(10)
        f = td_feed(api)
        check(f.quote("EURUSD") == 1.23456 and api.calls[-1][0] == "price"
              and api.calls[-1][1]["symbol"] == "EUR/USD", "quote from /price")
        n = len(api.calls)
        CLOCK.t += 10
        check(f.quote("EURUSD") == 1.23456 and len(api.calls) == n, "reused for a few seconds")
        CLOCK.t += 6
        f.quote("EURUSD")
        check(len(api.calls) == n + 1, "then asked again")
        api.fail[len(api.calls)] = {"code": 404, "message": "symbol not found", "status": "error"}
        check(f.quote("XAUXAG") is None, "API error: None")
        api.raise_on.add(len(api.calls))
        check(f.quote("GBPUSD") is None, "network error: None")
        api.fail[len(api.calls)] = {"price": "0"}
        check(f.quote("USDJPY") is None, "a zero price: None")
    finally:
        P.requests, P.time = real_requests, real_time


def test_demo():
    f = P.DemoFeed()
    a = f.candles("EURUSD", "H1", 600)
    b = f.candles("EURUSD", "H1", 5000)
    check(len(b) == 5000 and b.tail(600).reset_index(drop=True).equals(a), "longer history: older bars added before")
    check(np.allclose(b["open"].values[1:], b["close"].values[:-1])
          and (b["time"].diff().dropna() == pd.Timedelta(hours=1)).all(), "one continuous walk, hourly")
    check(f.candles("EURUSD", "H1", 600).equals(a), "the cache still serves the same recent bars")
    t0 = time.time()
    c = f.candles("EURUSD", "H1", 60000)
    check(len(c) == P.DemoFeed.max_history and c.tail(5000).reset_index(drop=True).equals(b),
          f"capped at max_history ({len(c)} bars, {time.time() - t0:.1f} s)")
    check(f.quote("EURUSD") == float(a["close"].iat[-1]), "quote: the latest synthetic close")
    g = P.DemoFeed()
    g.timeframe = "M15"                        # set by the engine at each price tick
    m15 = g.candles("GBPUSD", "M15", 600)
    d1 = g.candles("GBPUSD", "D1", 300)        # a mode on its own timeframe read after the engine's bars
    check(g.quote("GBPUSD") == float(m15["close"].iat[-1]) != float(d1["close"].iat[-1]),
          "quote of the series the engine reads (M15 here), not of a mode's D1 bars read after it")
    g.timeframe = "H1"                         # the admin moved the engine to H1: the M15 walk stays cached
    h1 = g.candles("GBPUSD", "H1", 600)
    check(g.quote("GBPUSD") == float(h1["close"].iat[-1]) != float(m15["close"].iat[-1]),
          "after a timeframe change: the new series' price")
    q = P.DemoFeed().quote("AUDUSD")
    check(isinstance(q, float) and q > 0, f"quote before any candles: {q}")
    for cls, want in ((P.PriceFeed, 44000), (P.DemoFeed, 44000), (P.MT5Feed, 44000), (P.TwelveDataFeed, 20000)):
        check(isinstance(cls.max_history, int) and cls.max_history == want, f"{cls.__name__}.max_history = {want}")


def main():
    started = time.time()
    for fn in (test_zone_helpers, test_mt5_nyclose_auto, test_mt5_other_clocks, test_symbols, test_broker_symbols,
               test_broker_reads_reselect, test_mt5_feed_symbols, test_twelvedata, test_demo):
        print(f"\n== {fn.__name__}")
        t0 = time.time()
        try:
            fn()
        except Exception:
            check(False, f"{fn.__name__}: raised", traceback.format_exc())
        print(f"     ({fn.__name__}: {time.time() - t0:.1f} s)")
    check(sys.modules["MetaTrader5"] is FAKE and MB.mt5 is FAKE, "the real MetaTrader5 package was never loaded")
    if FAILED:
        print("\nfailed:\n  " + "\n  ".join(FAILED))
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - started:.1f} s)")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
