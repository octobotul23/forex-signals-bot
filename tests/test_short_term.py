"""Short-term strategy (scalping), connector side, plus the helpers it shares with the server (bot/scalp.py) and the
dashboard's tile clamp (server/dashboard.py).

    .venv\\Scripts\\python tests\\test_short_term.py [-v] [scenario ...]

The real Connector and Scalper run on test_burst_connector's simulated broker, given a tick feed and a margin
calculator; the entry rule (and the stretch an open scalp is watched by) is replaced by what the test sets per
symbol. The event loop's clock can be pushed
forward, so hold times pass at once. No MetaTrader 5, no network, temporary files only. Exit code 0 = every check
passed.
"""
import asyncio
import itertools
import json
import logging
import math
import shutil
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import connector.connector as C                                               # noqa: E402
import test_burst_connector as T                                              # noqa: E402
from bot.scalp import (AGAINST_WIDEN, DEFAULTS, KEEP_SECONDS, MAX_HOLD_TOP, NEW_USER_MAX_HOLD,  # noqa: E402
                       RULE as RULE_PARAMS, UNIT_DEFAULTS, SecondBars, clamp_short, closes_soon, daily_limit_money,
                       decide, lots_for_money, ny_offset, risk_money, scalp_against, stretch)
from bot.symbols import SymbolSpec                                            # noqa: E402
from server.dashboard import DEFAULT as DASH_DEFAULT, MAX_TILES, clamp_dashboard  # noqa: E402

TMP = T.TMP
PIP = T.PIP
check, near, until, jump = T.check, T.near, T.until, T.jump
SHORT = {"enabled": True, "risk_unit": "pct", "risk_min": 0.5, "risk_max": 2.0, "hold_seconds": 30,
         "loss_unit": "pct", "daily_loss": 5.0, "max_open": 2, "max_spread_pips": 1.5, "symbols": []}
RULE: dict[str, dict] = {}          # symbol -> what the entry rule says for it every second (none: nothing)
ASKED: list[tuple] = []             # (symbol, second, bars) the rule was asked about
HOLDS: list[int] = []               # the hold time it was given each time
PIPS: dict[str, float] = {}         # symbol -> the pip it was given
STRETCH: dict[str, float] = {}      # symbol -> the stretch the rule shows now, for an open scalp (none: it does not look)
STRETCHED: list[str] = []           # the symbols the stretch was asked about
STRETCH_ASKED: list[tuple] = []     # (symbol, second, bars, pip) it was asked about


def fake_decide(symbol, bars, sec, pip, hold):
    ASKED.append((symbol, sec, len(bars)))
    HOLDS.append(hold)
    PIPS[symbol] = pip
    return RULE.get(symbol)


def fake_stretch(symbol, bars, sec, pip):
    STRETCHED.append(symbol)
    STRETCH_ASKED.append((symbol, sec, len(bars), pip))
    return STRETCH.get(symbol)


# the connector's clock (UTC): a Wednesday noon that runs on, so no scenario meets the daily or weekly close unless
# it sets one
WEDNESDAY = datetime(2026, 10, 28, 12, 0)
CLOCK = {"at": WEDNESDAY, "t0": time.monotonic()}


def set_clock(at):
    CLOCK.update(at=at, t0=time.monotonic())


C.utcnow = lambda: CLOCK["at"] + timedelta(seconds=time.monotonic() - CLOCK["t0"])
C.decide = fake_decide
C.stretch = fake_stretch
SERIAL = itertools.count(1)         # each Scalper's own files: by a number of its own, not by id(), which a later
                                    # Scalper can get from a collected one (and read that one's day)


def own_file(scalper, kind: str) -> Path:
    if "test_serial" not in scalper.__dict__:
        scalper.test_serial = next(SERIAL)
    return TMP / f"scalp-{kind}-{scalper.test_serial}.json"


C.Scalper._day_file = lambda self: own_file(self, "day")    # never scalp_day.json in the project
C.Scalper._open_file = lambda self: own_file(self, "open")  # nor scalp_open.json
C.Scalper.STEP = 0.01


class TickBroker(T.SimBroker):
    """The simulated broker with a tick feed (a fresh tick at every read while `feed`) and margin; deals stamped on
    the connector's clock; `slip`: pips an order's price moves against it on its way; gold, its pip 0.1."""
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.feed, self.margin_per_lot, self.tick_reads, self.slip, self.spec_reads = True, 200.0, 0, 0.0, {}

    def ticks_since(self, symbol, after_msc, count=5000, back_s=60):
        with self.lock:
            self.tick_reads += 1
            now = int(time.time() * 1000)
            if not self.feed or now <= after_msc:
                return []
            return [(now, self.bids[symbol], self.ask(symbol))]

    def margin_for(self, symbol, side, lots):
        return self.margin_per_lot * lots

    def _deal(self, *a, **kw):
        super()._deal(*a, **kw)
        self.deals[-1]["time"] = C.utcnow().isoformat(timespec="seconds")

    def open_market(self, symbol, side, lots, sl, tp, comment=""):
        with self.lock:
            if self.slip:
                self.bids[symbol] = round(self.bids[symbol] + (1 if side == "buy" else -1) * self.slip * PIP, 5)
            return super().open_market(symbol, side, lots, sl, tp, comment)

    def symbol_spec(self, symbol):
        with self.lock:
            self.spec_reads[symbol] = self.spec_reads.get(symbol, 0) + 1
        if symbol.startswith("XAU"):
            return SymbolSpec(symbol=symbol, pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, scale=5.0)
        return super().symbol_spec(symbol)


def harness(broker=None, short=None, **settings):
    b = broker or TickBroker(mode="netting")
    h = T.Harness(b, settings={**T.SETTINGS, **settings, "short_term": {**SHORT, **(short or {})}})
    h.c.scalper.symbols = ["EURUSD", "GBPUSD"]
    return b, h


def stop(h):
    if h.c.scalper.task:
        h.c.scalper.task.cancel()
    h.close()


def short_opens(h):
    return [m["trade"] for m in h.of("trade_opened") if m["trade"].get("strategy") == "short"]


def short_closes(h):
    return [m["trade"] for m in h.of("trade_closed") if m["trade"].get("strategy") == "short"]


def close_reasons(h):
    """(ticket, reason) of every close the scalper asks for from here on (manage's, a stop's)."""
    got, real = [], h.c.scalper.close

    async def close(ticket, why):
        got.append((ticket, why))
        return await real(ticket, why)
    h.c.scalper.close = close
    return got


def signal(b, sid, symbol="EURUSD", side="buy"):
    px = b.ask(symbol) if side == "buy" else b.bids[symbol]
    sign = 1 if side == "buy" else -1
    return {"id": sid, "symbol": symbol, "side": side, "price": px, "sl": px - sign * 20 * PIP,
            "tp": px + sign * 40 * PIP, "mode": "trend", "status": "open"}


TESTS = []


def scenario(fn):
    TESTS.append(fn)
    return fn


# ── shared helpers ─────────────────────────────────────────────
@scenario
def settings_clamp():
    d = clamp_short(None)
    check(d == DEFAULTS and d is not DEFAULTS, "no body, nothing stored: the defaults")
    got = clamp_short({"enabled": True, "risk_min": 50, "risk_max": "0.05", "hold_seconds": 9.6, "max_open": 99,
                       "max_spread_pips": 0, "daily_loss": 99, "bogus": 1})
    check(got == {**DEFAULTS, "enabled": True, "risk_min": 0.1, "risk_max": 10.0, "hold_seconds": 10, "max_open": 10,
                  "max_spread_pips": 0.1, "daily_loss": 50.0}, f"clamped, swapped when inverted, unknown dropped: {got}")
    cur = {**DEFAULTS, "risk_unit": "usd", "risk_min": 50.0, "risk_max": 80.0}
    got = clamp_short({"risk_unit": "pct"}, cur)
    check(got["risk_unit"] == "pct" and got["risk_min"] == UNIT_DEFAULTS["pct"]["risk_min"]
          and got["risk_max"] == UNIT_DEFAULTS["pct"]["risk_max"], f"50-80 USD never becomes 10 %: {got}")
    got = clamp_short({"risk_unit": "usd", "risk_max": 7}, None)
    check((got["risk_min"], got["risk_max"]) == (1.0, 7.0), f"a unit change keeps the value sent with it: {got}")
    got = clamp_short({"loss_unit": "usd"}, None)
    check(got["daily_loss"] == UNIT_DEFAULTS["usd"]["daily_loss"] and got["risk_unit"] == "pct", f"loss unit: {got}")
    check(clamp_short({"risk_unit": "pct"}, {**DEFAULTS, "risk_min": 3.0, "risk_max": 4.0})["risk_min"] == 3.0,
          "the same unit again changes nothing")
    for body, code in (({"risk_min": float("nan")}, "invalid_number"), ({"hold_seconds": "x"}, "invalid_number"),
                       ({"max_open": float("inf")}, "invalid_number"), ({"risk_unit": "eur"}, "invalid_unit"),
                       ({"hold_seconds": 10**400}, "invalid_number"), ({"max_open": -10**400}, "invalid_number"),
                       ({"daily_loss": 10**400}, "invalid_number"), ({"enabled": "false"}, "invalid_enabled"),
                       ({"enabled": 1}, "invalid_enabled"), ({"enabled": None}, "invalid_enabled"),
                       ({"loss_unit": ["pct"]}, "invalid_unit"), ({"symbols": "EURUSD"}, "invalid_symbols")):
        try:
            clamp_short(body)
            check(False, f"{body} must raise {code}")
        except ValueError as e:
            check(str(e) == code, f"{body} -> {code}, got {e}")
    got = clamp_short({"symbols": ["EURUSD", "EURUSD", 5, ["x"], {"a": 1}, "XAGUSD"]}, None, ["EURUSD", "GBPUSD"])
    check(got["symbols"] == ["EURUSD"], f"symbols: strings, once, only the engine's: {got['symbols']}")
    got = clamp_short(None, {**DEFAULTS, "symbols": ["EURUSD", 3], "risk_min": "junk", "hold_seconds": None,
                             "risk_unit": "btc"})
    check(got["symbols"] == ["EURUSD"] and got["risk_min"] == DEFAULTS["risk_min"]
          and got["hold_seconds"] == DEFAULTS["hold_seconds"] and got["risk_unit"] == "pct",
          f"bad stored values fall back to the defaults: {got}")
    check(clamp_short(None, {**DEFAULTS, "symbols": "EURUSD"})["symbols"] == [], "stored symbols not a list")
    check(clamp_short(None, {**DEFAULTS, "enabled": "yes"})["enabled"] is False, "a stored 'enabled' not a bool: off")
    h = T.Harness(TickBroker(mode="netting"), settings={**T.SETTINGS, "short_term": {**SHORT, "enabled": "true",
                                                                                      "hold_seconds": 10**400}})
    check(h.c.scalper.cfg == {**SHORT, "enabled": False, "hold_seconds": DEFAULTS["hold_seconds"]},
          f"the connector given bad values: off, the rest kept, no crash: {h.c.scalper.cfg}")
    h.close()


@scenario
def sizing_and_daily_limit():
    pct = {**DEFAULTS, "risk_min": 1.0, "risk_max": 3.0}
    check(near(risk_money(pct, 10_000, 0), 100) and near(risk_money(pct, 10_000, 1), 300)
          and near(risk_money(pct, 10_000, 0.5), 200), "pct: the confidence picks the point inside the range")
    check(near(risk_money(pct, 10_000, 7), 300) and near(risk_money(pct, 10_000, -1), 100), "strength clamped to 0..1")
    usd = {**DEFAULTS, "risk_unit": "usd", "risk_min": 1.0, "risk_max": 10.0}
    check(near(risk_money(usd, 10_000, 0.5), 5.5), "usd: money as it is")
    check(near(risk_money({**usd, "risk_max": 5000.0}, 1000, 1), 100), "usd: capped at 10 % of the balance")
    check(risk_money(pct, 0, 1) == 0 and risk_money(pct, -5, 1) == 0, "no balance: no risk")
    s = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, volume_min=0.01, volume_step=0.01, volume_max=50)
    check(lots_for_money(100, 5, s) == 2.0 and lots_for_money(99.9, 5, s) == 1.99, "lots round DOWN to the step")
    check(lots_for_money(0.4, 5, s) == 0.0, "under the minimum lot: 0 (skipped, never sized up)")
    check(lots_for_money(1e9, 5, s) == 50, "capped at volume_max")
    check(lots_for_money(100, 0, s) == 0 and lots_for_money(-1, 5, s) == 0, "no stop / no money: 0")
    check(near(daily_limit_money({**DEFAULTS, "daily_loss": 5.0}, 10_000), 500), "pct limit from the day's start")
    check(near(daily_limit_money({**DEFAULTS, "loss_unit": "usd", "daily_loss": 50.0}, 10_000), 50), "usd limit")


@scenario
def the_day_book():
    path, other = TMP / "daybook.json", TMP / "daybook-fresh.json"
    path.unlink(missing_ok=True)

    def limit(start):                                                   # 5 % of the day's start
        return start * 0.05

    empty = {"day": "", "start": 0.0, "pnl": 0.0, "warned": False, "guessed": {}}
    book = C.DayBook(path, limit)
    check(book.day == empty and not path.exists(), f"nothing kept yet: {book.day}")
    try:
        set_clock(WEDNESDAY)
        book.roll(10_000.0)
        check(book.day == {**empty, "day": "2026-10-28", "start": 10_000.0}, f"today starts: {book.day}")
        check(path.exists() and json.loads(path.read_text()) == book.day,
              "saved as it starts, before anything is booked (a restart keeps the day's start)")
        check(near(book.free(0), 500) and near(book.free(120), 380) and not book.reached(), "the limit: 500 free")
        today = book.day_of(None)
        check(today == "2026-10-28" and book.day_of("2026-10-27T23:59:00") == "2026-10-27"
              and book.day_of("2026-10-29T00:00:00") == today, "a result's day: never one after today")
        check(book.book(today, -100.0, 1) == 0 and near(book.today(), -100), f"booked: {book.day}")
        check(book.book(today, -50.0, 2, guess=True) == 0 and near(book.day["pnl"], -150)
              and book.day["guessed"] == {"2": 50.0}, f"a guess, kept by ticket: {book.day}")
        check(book.book(today, 20.0, 2) == 50.0 and near(book.day["pnl"], -80) and book.day["guessed"] == {},
              f"its real result replaces it: {book.day}")
        book.book(today, -420.0, 3)
        check(book.reached() and near(book.free(0), 0), f"the limit reached: {book.day}")
        book.day["warned"] = True                                       # as the scalper does when it tells
        book.book(today, -10.0, 4, guess=True)
        check(book.day["warned"], "a guess on top keeps it told")
        book.book(today, -10.0, 8, guess=True)
        check(book.book(today, 0.0, 8) == 10.0 and near(book.day["pnl"], -510) and book.reached()
              and book.day["warned"], f"a guess replaced, still over the limit: still told (no second warning): "
                                      f"{book.day}")
        check(book.book(today, 5.0, 4) == 10.0 and near(book.day["pnl"], -495) and not book.reached()
              and not book.day["warned"], f"back under the limit: told again when reached: {book.day}")
        check(book.book("2026-10-27", -30.0, 5) == 0 and near(book.day["pnl"], -495), "an earlier day: not booked")
        check(json.loads(path.read_text()) == book.day and C.DayBook(path, limit).day == book.day,
              "saved at every change, read back as it was")

        set_clock(WEDNESDAY + timedelta(days=1))
        check(book.today() == 0 and book.day["day"] == "2026-10-28", "a new day: nothing booked to it yet")
        check(book.book(book.day_of(None), -12.5, 6) == 0
              and book.day == {**empty, "day": "2026-10-29", "start": 9_505.0, "pnl": -12.5},
              f"its first result: it starts from the balance as the last day ended: {book.day}")
        book.roll(9_000.0)
        check(book.day["start"] == 9_505.0, "the same day again: not started over")
        check(C.DayBook(path, limit).day == book.day, "the new day read back")

        other.unlink(missing_ok=True)
        fresh = C.DayBook(other, limit)
        check(fresh.book("2026-10-29", -5.0, 7) == 0 and fresh.day == empty and not other.exists(),
              "nothing kept and no balance: a result has no day to go to")
        fresh.book("2026-10-29", -5.0, 7, balance=1_000.0)
        check(fresh.day == {**empty, "day": "2026-10-29", "start": 1_000.0, "pnl": -5.0},
              f"given the balance before it: {fresh.day}")
        check(C.DayBook(other, lambda start: 0.0).free(1e9) is None, "no limit: nothing to keep free")
        other.write_text("{not json")
        check(C.DayBook(other, limit).day == empty, "an unreadable file: starts over")
    finally:
        set_clock(WEDNESDAY)
        path.unlink(missing_ok=True)
        other.unlink(missing_ok=True)


@scenario
def second_bars():
    b = SecondBars(keep=10)
    b.add([(1000_100, 1.1, 1.1001), (1000_900, 1.2, 1.2001), (1001_000, 1.3, 1.3001), (1003_500, 1.4, 1.4001)])
    check([x[0] for x in b.bars] == [1000, 1001, 1003], "one bar per second that had ticks")
    check(b.bars[0] == [1000, 1.2, 1.2001, 2], "the last quote inside the second and its tick count")
    b.add([(1003_500, 9.9, 9.9), (1003_400, 9.9, 9.9)])
    check(b.bars[-1] == [1003, 1.4, 1.4001, 1], "ticks not newer than the last one are ignored")
    check([x[0] for x in b.upto(1001)] == [1000, 1001], "upto: the seconds up to and including it")
    b.add([(1013_000, 1.5, 1.5001)])
    check([x[0] for x in b.bars] == [1013], "older than `keep` seconds before the newest bar: dropped")


def walk(n=KEEP_SECONDS, start=1_800_000_000 - 1_800_000_000 % 86400 + 10 * 3600, step_pips=0.4, spread_pips=0.8,
         drift_last=0.0, last=60, seed=1):
    """One-second bars of a random walk (pips per second), the last `last` seconds drifting by `drift_last` pips a
    second; broker hour 10 at the start, so no rollover hour."""
    import random
    rnd, mid, out = random.Random(seed), 1.1, []
    for i in range(n):
        mid += (rnd.gauss(0, step_pips) + (drift_last if i >= n - last else 0)) * PIP
        out.append([start + i, mid - spread_pips * PIP / 2, mid + spread_pips * PIP / 2, 1])
    return out


@scenario
def rule_on_bars():
    calm = walk()
    sec = calm[-1][0]
    check(decide("EURUSD", calm, sec, PIP) is None, "a random walk with no stretch: nothing")
    down = walk(drift_last=-0.6)
    d = decide("EURUSD", down, sec, PIP)
    check(d and d["side"] == "buy" and d["stretch"] <= -RULE_PARAMS["z"] and 0 <= d["strength"] <= 1 and
          d["tp_pips"] is None, f"a fall well past its average: buy it back (the fade): {d}")
    up = [[t, 2.2 - a, 2.2 - b, n] for t, b, a, n in down]                     # the same walk upside down
    u = decide("EURUSD", up, sec, PIP)
    check(u and u["side"] == "sell" and near(u["stretch"], -d["stretch"], 0.01), f"a rise: sell ({u})")
    long_hold = decide("EURUSD", down, sec, PIP, hold=120)
    check(near(long_hold["sl_pips"] / d["sl_pips"], 2, 0.05), f"the stop grows with the root of the hold: "
                                                                 f"{d['sl_pips']} -> {long_hold['sl_pips']}")
    check(d["sl_pips"] >= RULE_PARAMS["min_sl_spreads"] * 0.8, f"never under {RULE_PARAMS['min_sl_spreads']} spreads")
    stronger = decide("EURUSD", walk(drift_last=-1.0), sec, PIP)
    check(stronger["strength"] > d["strength"], f"a bigger stretch is stronger: {d['strength']} < {stronger['strength']}")
    # when it does not look
    check(decide("EURUSD", down, sec + 1, PIP) is None, "a second with no tick of its own: nothing new to see")
    check(decide("EURUSD", down[-3000:], sec, PIP) is None, f"less than {RULE_PARAMS['warmup']} s of history: nothing")
    gapped = [b for b in down if not (sec - 2000 < b[0] < sec - 1600)]
    check(decide("EURUSD", gapped, sec, PIP) is None, "a gap (market shut, no feed) inside the last hour: nothing")
    wide = [*down[:-1], [sec, down[-1][1] - 0.5 * PIP, down[-1][2] + 0.5 * PIP, 1]]
    check(decide("EURUSD", wide, sec, PIP) is None, "the spread well over its usual: nothing")
    quiet = walk(step_pips=0.05, drift_last=-0.08)
    check(decide("EURUSD", quiet, sec, PIP) is None, "too quiet to beat the spread: nothing")
    late = [[t + 13 * 3600, b, a, n] for t, b, a, n in down]                  # broker hour 23: the rollover
    check(decide("EURUSD", late, sec + 13 * 3600, PIP) is None, "around the broker's rollover: nothing")
    check(decide("USDJPY", [[t, b * 100, a * 100, n] for t, b, a, n in down], sec, 0.01)["side"] == "buy",
          "a pip of 0.01 (JPY) works the same")


@scenario
def max_hold_clamp():
    # Y (max_hold_seconds, since v1.3.0): kept only when sent or stored; a block saved under v1.2.0 has none, and a
    # missing Y follows X
    check("max_hold_seconds" not in DEFAULTS, "not a default: a v1.2.0 user keeps following X")
    for got in (clamp_short(None), clamp_short(None, SHORT), clamp_short({"max_open": 3}, SHORT),
                clamp_short({"hold_seconds": 60, "risk_unit": "usd"}, SHORT)):
        check(list(got) == list(DEFAULTS), f"none sent or stored: none added (it follows X): {got}")
    check(clamp_short(None, SHORT) == SHORT, "a v1.2.0 block comes back as it was")
    cur = {**SHORT, "max_hold_seconds": 120}
    check(clamp_short({"max_hold_seconds": NEW_USER_MAX_HOLD}) == {**DEFAULTS, "max_hold_seconds": 120}
          and clamp_short(None, cur) == cur and clamp_short({"max_open": 3}, cur)["max_hold_seconds"] == 120,
          "sent or stored: kept")
    for body, stored, want in (({"hold_seconds": 60, "max_hold_seconds": 45}, None, (60, 60)),
                               ({"hold_seconds": 200}, cur, (200, 200)),
                               ({"hold_seconds": 10}, cur, (10, 120)),
                               ({"hold_seconds": 1000, "max_hold_seconds": 9}, None, (300, 300)),
                               (None, {**SHORT, "hold_seconds": 60, "max_hold_seconds": 30}, (60, 60)),
                               ({"max_hold_seconds": 5000}, cur, (30, MAX_HOLD_TOP)),
                               ({"max_hold_seconds": 150.9}, cur, (30, 150)),
                               ({"max_hold_seconds": -5}, cur, (30, 30)),
                               ({"max_hold_seconds": 90}, {**SHORT, "max_hold_seconds": "junk"}, (30, 90))):
        got = clamp_short(body, stored)
        check((got["hold_seconds"], got["max_hold_seconds"]) == want and type(got["max_hold_seconds"]) is int,
              f"X, then Y clamped to X..{MAX_HOLD_TOP} (a Y under X raised to it, never swapped): {body} {stored} "
              f"-> {got}")
    for y in (float("nan"), float("inf"), "x", "12.5", [120], {"s": 1}, 10**400):
        try:
            clamp_short({"max_hold_seconds": y}, cur)
            check(False, f"Y {y!r} must raise invalid_number")
        except ValueError as e:
            check(str(e) == "invalid_number", f"Y {y!r} -> invalid_number, got {e}")
    for bad in ("junk", None, float("nan"), 10**400, [1], {"a": 1}):
        got = clamp_short(None, {**SHORT, "max_hold_seconds": bad})
        check(got == SHORT, f"a stored Y gone bad ({bad!r}): left out, it follows X again: {got}")
    # null: the user's way back to "Y follows X" (a v1.2.0 block again); a body without the key keeps a stored Y
    for body, stored, want in (({"max_hold_seconds": None}, cur, SHORT),
                               ({"max_hold_seconds": None, "hold_seconds": 90}, cur, {**SHORT, "hold_seconds": 90}),
                               ({"max_hold_seconds": None}, None, DEFAULTS),
                               ({"max_hold_seconds": None}, SHORT, SHORT)):
        got = clamp_short(body, stored)
        check(got == want and clamp_short(None, got) == got, f"Y sent as null: removed: {body} {stored} -> {got}")
    check(clamp_short({"hold_seconds": 45}, cur)["max_hold_seconds"] == 120
          and clamp_short({}, cur)["max_hold_seconds"] == 120, "the key not sent: the stored Y stays")
    got = clamp_short({"max_hold_seconds": 200}, clamp_short({"max_hold_seconds": None}, cur))
    check(got == {**SHORT, "max_hold_seconds": 200}, f"set again after a null: {got}")


@scenario
def the_stretch():
    # stretch(): what decide() measures, given whether or not a trade opens, so an open scalp can be watched
    z, calm, down = RULE_PARAMS["z"], walk(), walk(drift_last=-0.6)
    sec = calm[-1][0]
    s = stretch("EURUSD", calm, sec, PIP)
    check(isinstance(s, float) and abs(s) < z and decide("EURUSD", calm, sec, PIP) is None,
          f"no trade: its stretch all the same, under z: {s}")
    s, d = stretch("EURUSD", down, sec, PIP), decide("EURUSD", down, sec, PIP)
    check(s <= -z and d["side"] == "buy" and d["stretch"] == round(s, 2) != s, f"a buy: decide()'s stretch, "
                                                                                 f"unrounded: {s} {d}")
    up = [[t, 2.2 - a, 2.2 - b, n] for t, b, a, n in down]
    check(near(stretch("EURUSD", up, sec, PIP), -s, 0.01), "a rise: the same stretch, over the average")
    gapped = [b for b in down if not (sec - 2000 < b[0] < sec - 1600)]
    wide = [*down[:-1], [sec, down[-1][1] - 0.5 * PIP, down[-1][2] + 0.5 * PIP, 1]]
    late = [[t + 13 * 3600, b, a, n] for t, b, a, n in down]
    for what, bars, at, pip in (("a second with no tick of its own", down, sec + 1, PIP),
                                ("too little history", down[-3000:], sec, PIP), ("a gap", gapped, sec, PIP),
                                ("the spread well over its usual", wide, sec, PIP),
                                ("too quiet to beat the spread", walk(step_pips=0.05, drift_last=-0.08), sec, PIP),
                                ("the broker's rollover", late, sec + 13 * 3600, PIP), ("no bars", [], sec, PIP),
                                ("no pip", down, sec, 0.0)):
        check(stretch("EURUSD", bars, at, pip) is None, f"{what}: decide() does not look, nothing to judge by")


def decide_v120(symbol, bars, sec, pip, hold=30):
    """decide() as v1.2.0 shipped it, word for word (bot/scalp.py, its RULE): v1.3.0 split it to share its measure with
    stretch(), and it must still answer exactly what this does."""
    r = RULE_PARAMS
    # it looks at a second only when that second had a tick, as it was tested (no tick: nothing new to see)
    if not bars or bars[-1][0] != sec or pip <= 0 or (sec // 3600) % 24 in r["rollover_hours"]:
        return None
    pt = pip / 10
    a = math.exp(-1.0 / r["tau"])
    ema = prev_t = prev_mid = None
    gap_end, var, spreads = bars[0][0], 0.0, []
    for t, bid, ask, _ in bars:
        mid = (bid + ask) * 0.5
        if prev_t is None:
            ema = mid
        else:
            # the average of the price held flat between two ticks: exact for any gap
            dt = t - prev_t
            k = math.exp(-dt / r["tau"])
            ema = k * ema + prev_mid * (a - k) + (1 - a) * mid
            if dt > r["gap"]:
                gap_end = t
            elif t > sec - r["vol_window"]:
                var += (mid - prev_mid) ** 2
        if t > sec - r["warmup"]:
            spreads.append(ask - bid)
        prev_t, prev_mid = t, mid
    if sec - gap_end < r["warmup"]:
        return None
    spread = bars[-1][2] - bars[-1][1]
    spreads.sort()
    if spread > r["spread_mult"] * spreads[len(spreads) // 2] + 0.1 * pt:
        return None
    per_s = max(var / r["vol_window"], (0.1 * pt) ** 2)                # variance per second
    if math.sqrt(per_s * r["vol_hold"]) < r["volcost"] * spread:
        return None
    dev = (prev_mid - ema) / math.sqrt(per_s * r["tau"])
    if abs(dev) < r["z"]:
        return None
    sl = max(r["ksl"] * math.sqrt(per_s * max(1, int(hold))), r["min_sl_spreads"] * spread)
    return {"side": "buy" if dev < 0 else "sell",
            "strength": min(1.0, (abs(dev) - r["z"]) / (r["z_full"] - r["z"])),
            "sl_pips": math.ceil(sl / pip * 10 - 1e-9) / 10, "tp_pips": None, "stretch": round(dev, 2)}


def rough_walk(rnd):
    """One-second bars of a random series and its pip, drawn from `rnd` to meet every case of the rule: a pair, a JPY
    pair or gold, any broker hour (the rollover too), seconds with no tick, now and then a silence (a gap, or one just
    short of it) or a spread spike, busy or too quiet, drifting at the end or not."""
    pip = rnd.choice((PIP, 0.01, 0.1))
    mid = {PIP: 1.1, 0.01: 150.0, 0.1: 2650.0}[pip]
    n, last = rnd.randint(3500, KEEP_SECONDS + 300), rnd.randint(5, 400)
    start = 1_800_000_000 - 1_800_000_000 % 86400 + rnd.randrange(86400)
    step, spread = rnd.choice((0.05, 0.4, 0.4, 1.0)), rnd.choice((0.2, 0.8, 0.8, 2.0))    # pips
    drift = rnd.choice((0.0, -1.0, 1.0)) * rnd.uniform(0.2, 1.2)
    no_tick = rnd.choice((0.0, 0.0, 0.3, 0.8))                       # the share of seconds with no tick
    hole = rnd.randrange(n) if rnd.random() < 0.1 else n             # a silence of 200-900 s from there
    hole_end = hole + rnd.randint(200, 900)
    out = []
    for i in range(n):
        mid += (rnd.gauss(0, step) + (drift if i >= n - last else 0.0)) * pip
        if hole <= i < hole_end or rnd.random() < no_tick:
            continue
        half = spread * pip * (rnd.choice((1.5, 3.0)) if rnd.random() < 0.03 else 1.0) / 2
        out.append([start + i, mid - half, mid + half, rnd.randint(1, 4)])
    return out, pip


def answer(fn, *args):
    """What fn("EURUSD", *args) returns, or the type of what it raises."""
    try:
        return fn("EURUSD", *args)
    except Exception as e:
        return type(e)


@scenario
def decide_as_in_v120():
    # decide() shares its measure with stretch() since v1.3.0: over many random series it answers as v1.2.0 did, and
    # the stretch is the one it trades on
    import random
    rnd, z, seen = random.Random(130), RULE_PARAMS["z"], {"buy": 0, "sell": 0, "under z": 0, "no look": 0}
    for _ in range(100):
        bars, pip = rough_walk(rnd)
        for cut in (0, 1, 5):
            done = bars[:len(bars) - cut]
            sec = done[-1][0] + (1 if rnd.random() < 0.1 else 0)     # now and then a second with no tick of its own
            hold = rnd.choice((30, 120, 600, 0, 45.7))
            old, new = decide_v120("EURUSD", done, sec, pip, hold), decide("EURUSD", done, sec, pip, hold)
            check(new == old and type(new) is type(old), f"as v1.2.0 answered: {old} -> {new}")
            s = stretch("EURUSD", done, sec, pip)
            if old:
                check(s is not None and abs(s) >= z and round(s, 2) == old["stretch"]
                      and old["side"] == ("buy" if s < 0 else "sell"), f"a trade: its stretch, unrounded: {s} {old}")
                seen[old["side"]] += 1
            else:
                check(s is None or abs(s) < z, f"no trade: no stretch, or one under z: {s}")
                seen["no look" if s is None else "under z"] += 1
    check(min(seen.values()) >= 20, f"every case met: {seen}")
    down = walk(drift_last=-0.6)
    sec = down[-1][0]
    for args in (([], sec, PIP), (down[:5], sec, PIP), (down, sec, 0.0), (down, sec, -PIP), (down, sec, PIP, None),
                 (down, sec, PIP, "45")):
        check(answer(decide, *args) == answer(decide_v120, *args), f"odd inputs, the same answer: {args[1:]}")


@scenario
def against_an_open_scalp():
    # the Scalper's "against" (bot/hold.py closes on it while a Y above X is set): the rule trading the other way now,
    # or the price stretched a whole unit further than at the entry
    check(AGAINST_WIDEN == 1.0, f"one unit: {AGAINST_WIDEN}")
    for side, entry, now, now_side, want, what in (
            ("buy", -2.4, -3.4, None, True, "a buy opened at -2.4, a whole unit further down"),
            ("buy", -2.4, -3.39, None, False, "not yet a whole unit"),
            ("buy", -2.4, -5.0, "buy", True, "further still, the rule buying again as well"),
            ("buy", -2.4, -2.9, "buy", False, "the rule buying again, half a unit further: not yet"),
            ("buy", -2.4, -1.0, None, False, "back toward its average: going its way"),
            ("buy", -2.4, 2.3, "sell", True, "the rule sells now"),
            ("sell", 2.4, 3.4, None, True, "a sell opened at +2.4, a whole unit further up"),
            ("sell", 2.4, 3.39, None, False, "not yet a whole unit"),
            ("sell", 2.4, 1.0, None, False, "back toward its average: going its way"),
            ("sell", 2.4, -2.2, "buy", True, "the rule buys now"),
            ("buy", -3.1, -4.1, None, True, "a whole unit, whatever the float rounding (-3.1 + 4.1 < 1)"),
            ("sell", 3.1, 4.1, None, True, "a whole unit, whatever the float rounding (4.1 - 3.1 < 1)"),
            ("buy", None, -9.0, "sell", False, "the entry's stretch not known: nothing decided"),
            ("buy", -2.4, None, "sell", False, "the rule does not look now: nothing decided"),
            ("sell", float("nan"), 9.0, "buy", False, "NaN is not a stretch"),
            ("buy", True, -9.0, None, False, "nor a bool"),
            ("buy", "junk", -9.0, None, False, "nor junk"),
            ("long", -2.4, -9.0, "sell", False, "a side neither buy nor sell"),
            (None, -2.4, -9.0, "sell", False, "no side")):
        got = scalp_against(side, entry, now, now_side)
        check(got is want, f"{what}: {side} at {entry}, now {now} ({now_side}) -> {got}")
    import random

    def more(bars, drift, n, seed):
        """`bars` and `n` seconds more of a walk, drifting `drift` pips a second."""
        rnd, out = random.Random(seed), [list(b) for b in bars]
        mid, t = (out[-1][1] + out[-1][2]) / 2, out[-1][0]
        for i in range(1, n + 1):
            mid += (rnd.gauss(0, 0.4) + drift) * PIP
            out.append([t + i, mid - 0.4 * PIP, mid + 0.4 * PIP, 1])
        return out

    fall = more(walk(), -0.6, 60, 2)
    d = decide("EURUSD", fall, fall[-1][0], PIP)
    check(d and d["side"] == "buy", f"a fall: the rule buys: {d}")
    for drift, n, want, what in ((0.0, 5, False, "a few seconds on, about where it was"),
                                 (0.6, 30, False, "coming back toward its average"),
                                 (-1.0, 30, True, "falling on: a whole unit further"),
                                 (1.0, 180, True, "risen well over its average: the rule sells")):
        bars = more(fall, drift, n, 3)
        sec = bars[-1][0]
        s, d_now = stretch("EURUSD", bars, sec, PIP), decide("EURUSD", bars, sec, PIP)
        side = d_now and d_now["side"]
        check(scalp_against("buy", d["stretch"], s, side) is want, f"{what}: {d['stretch']} -> {s} ({side})")


@scenario
def dashboard_clamp():
    check(clamp_dashboard(DASH_DEFAULT) == DASH_DEFAULT, "the default layout is valid as it is")
    got = clamp_dashboard({"tiles": [{"k": "pnl", "days": 400, "scope": "short"}, {"k": "nope"}, 5, {"k": ["pnl"]},
                                     {"k": "win_rate", "days": "7.4", "scope": "weird"}, {"k": "equity", "days": 9},
                                     {"k": "sig_pips", "scope": "short", "days": 0}]})
    check(got == {"tiles": [{"k": "pnl", "days": 365, "scope": "short"}, {"k": "win_rate", "days": 7, "scope": "all"},
                            {"k": "equity"}, {"k": "sig_pips", "days": 1}]},
          f"unknown kinds dropped, days 1..365, scope only where it applies: {got}")
    check(len(clamp_dashboard({"tiles": [{"k": "trades"}] * 40})["tiles"]) == MAX_TILES, f"at most {MAX_TILES} tiles")
    for body, code in ((None, "invalid_dashboard"), ({"tiles": "x"}, "invalid_dashboard"), ([], "invalid_dashboard"),
                       ({"tiles": [{"k": "pnl", "days": "x"}]}, "invalid_number"),
                       ({"tiles": [{"k": "pnl", "days": float("nan")}]}, "invalid_number")):
        try:
            clamp_dashboard(body)
            check(False, f"{body!r} must raise {code}")
        except ValueError as e:
            check(str(e) == code, f"{body!r} -> {code}, got {e}")


# ── the scalper in the connector ───────────────────────────────
@scenario
async def opens_sized_by_confidence_and_closes_after_hold():
    RULE.clear()
    HOLDS.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "tp_pips": None}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp opens")
    RULE.clear()
    t = short_opens(h)[0]
    entry = b.ask()
    check(t["signal_id"] is None and t["symbol"] == "EURUSD" and t["side"] == "buy" and near(t["entry_price"], entry),
          f"trade_opened: {t}")
    # 10 000 balance, 0.5..2 % at strength 0.5 -> 1.25 % = 125, at a 5 pip stop (10/pip/lot) -> 2.5 lots
    check(near(t["lots"], 2.5) and near(t["risk"], 125) and t["confidence"] == 50 and near(t["sl"], entry - 5 * PIP)
          and t["tp"] is None, f"sized from the confidence inside the range: {t}")
    p = next(iter(b.pos.values()))
    check(p["comment"].startswith("fxs:") and p["sl"] is not None, f"tagged fxs: with its stop: {p}")
    check(ASKED and all(s in ("EURUSD", "GBPUSD") and n >= 1 for s, _, n in ASKED), f"rule asked per symbol: {ASKED[:3]}")
    check(set(HOLDS) == {30}, f"the rule is told the hold time (its stop is sized to it): {set(HOLDS)}")
    await h.c.snapshot()                                # the scalp is in `known` now
    await jump(29)
    check(b.pos and not short_closes(h), "still open before its hold time")
    await jump(2)
    await until(lambda: short_closes(h), "the scalp closes at its hold time and is reported")
    c = short_closes(h)[0]
    check(c["ticket"] == t["ticket"] and near(c["pnl"], -12.5) and c["symbol"] == "EURUSD" and near(c["risk"], 125),
          f"trade_closed carries the result and what a server that lost its open needs: {c}")
    await h.c.snapshot()
    check(len(h.of("trade_closed")) == 1, f"the snapshot does not report it again: {h.of('trade_closed')}")
    await until(lambda: near(h.c.scalper.day["pnl"], -12.5), "the day's short-term result")
    check(not b.pos and not h.c.scalper.open, "nothing left open")
    stop(h)


@scenario
async def one_per_symbol_max_open_spread_and_margin():
    RULE.clear()
    b, h = harness(short={"max_open": 1})
    b.add_position("GBPUSD", "buy", 0.1, "by hand", magic=0)           # not this bot's: still blocks the symbol
    RULE.update(GBPUSD={"side": "sell", "strength": 1, "sl_pips": 5}, EURUSD={"side": "buy", "strength": 0.1,
                                                                            "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp on the free symbol")
    await asyncio.sleep(0.1)
    check([t["symbol"] for t in short_opens(h)] == ["EURUSD"], f"never next to another position: {short_opens(h)}")
    stop(h)

    RULE.clear()
    b, h = harness(short={"max_open": 1})
    RULE.update(GBPUSD={"side": "sell", "strength": 0.9, "sl_pips": 5}, EURUSD={"side": "buy", "strength": 0.2,
                                                                              "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    await asyncio.sleep(0.1)
    check([t["symbol"] for t in short_opens(h)] == ["GBPUSD"], f"max_open 1: the surest first: {short_opens(h)}")
    stop(h)

    RULE.clear()
    b, h = harness()
    b.spread = 2 * PIP                                                   # 2 pips > max 1.5
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: b.tick_reads > 20, "a few steps")
    check(not short_opens(h), "spread too wide: none")
    b.spread = 0.5 * PIP
    b.margin_per_lot = 1_000_000                                         # 0.01 lot would take 10 000 of 5 000 allowed
    await until(lambda: any("not enough free margin for 0.01 lots of EURUSD" in m for m in h.logs("warn")),
                "the user is told there is no room even for the smallest trade")
    check(not short_opens(h), "not enough free margin for the minimum lot: none")
    b.margin_per_lot = 10_000                                            # 4 lots would take 40 000 of 5 000 allowed
    await until(lambda: short_opens(h), "opens, smaller")
    t = short_opens(h)[0]
    check(near(t["lots"], 0.5) and near(t["risk"], 25), f"as many lots as half the free margin allows, its risk "
                                                        f"from them: {t}")
    await jump(31)
    await until(lambda: len(short_opens(h)) == 2, "closed at its hold time, the next one")
    smaller = [m for m in h.logs("warn") if "free margin allows 0.5 lots of EURUSD, not 4.0" in m]
    check(len(smaller) == 1 and sum("not enough free margin" in m for m in h.logs("warn")) == 1,
          f"told once an hour, not at every trade: {h.logs('warn')}")
    b.margin_per_lot = 200
    RULE.clear()
    await jump(31)
    await until(lambda: len(short_closes(h)) == 2, "both closed")
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    await until(lambda: len(short_opens(h)) == 3, "opens in full once the margin is there")
    check(near(short_opens(h)[-1]["lots"], 3.99), f"2 % of the 9 995 left at 5 pips: {short_opens(h)[-1]}")
    stop(h)


@scenario
async def long_term_signal_goes_first():
    RULE.clear()
    b, h = harness(max_open_trades=1)
    RULE.update(EURUSD={"side": "sell", "strength": 0.5, "sl_pips": 5}, GBPUSD={"side": "sell", "strength": 0.5,
                                                                              "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: len(short_opens(h)) == 2, "two scalps")
    RULE.clear()
    await h.c.on_signal(signal(b, 501, "EURUSD", "buy"), manual=False)
    check(not h.of("signal_skipped"), f"scalps do not count against max_open_trades: {h.of('signal_skipped')}")
    longs = [m["trade"] for m in h.of("trade_opened") if m["trade"].get("signal_id") == 501]
    check(longs and longs[0]["side"] == "buy", f"the signal's trade opened: {longs}")
    await until(lambda: any(c["symbol"] == "EURUSD" for c in short_closes(h)), "the EURUSD scalp reported closed")
    eur = [p for p in b.pos.values() if p["symbol"] == "EURUSD"]
    check(len(eur) == 1 and eur[0]["side"] == "buy" and eur[0]["comment"] == "fx:501",
          f"netting: the scalp was closed first, so the signal did not merge into it: {eur}")
    check(len(h.c.scalper.open) == 1, "the GBPUSD scalp is still followed")
    RULE["EURUSD"] = {"side": "sell", "strength": 1, "sl_pips": 5}
    await asyncio.sleep(0.1)
    check(len([t for t in short_opens(h) if t["symbol"] == "EURUSD"]) == 1, "no new scalp next to the long-term trade")
    stop(h)

    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "sell", "strength": 0.5, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    b.fail_close = 3                                                     # all three quick attempts fail
    await h.c.on_signal(signal(b, 502, "EURUSD", "buy"), manual=False)
    check(h.of("signal_skipped") and "short-term trade on EURUSD could not be closed" in
          h.of("signal_skipped")[-1]["reason"], f"a scalp that will not close: the signal is skipped, not merged: "
                                                f"{h.of('signal_skipped')}")
    stop(h)

    # hedging: once the scalp made way, what else holds the symbol is looked at again (the scalp just closed aside)
    for case in ("long-term too", "listed after its close"):
        RULE.clear()
        b, h = harness(TickBroker(mode="hedging"))
        RULE["EURUSD"] = {"side": "sell", "strength": 0.5, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp")
        RULE.clear()
        h.c.scalper.task.cancel()                   # no step of its own reads the positions meanwhile
        if case == "long-term too":
            b.add_position("EURUSD", "buy", 0.1, "fx:7")
            await h.c.on_signal(signal(b, 503, "EURUSD", "buy"), manual=False)
            eur = [p["comment"] for p in b.pos.values() if p["symbol"] == "EURUSD"]
            check(h.of("signal_skipped") and h.of("signal_skipped")[-1]["reason"] == "already have a position on EURUSD"
                  and eur == ["fx:7"] and not h.c.scalper.open,
                  f"a long-term trade there besides the scalp: the scalp is closed, the signal skipped: "
                  f"{h.of('signal_skipped')} {eur}")
        else:
            listed, closing, stale = b.positions, b.close_position, []

            def close_position(pos, *a, **kw):      # MT5's list lags behind a close: the next read still has it
                stale.append(next(p for p in listed() if p["ticket"] == pos["ticket"]))
                return closing(pos, *a, **kw)

            def positions(all_magic=False):
                return listed(all_magic) + [stale.pop()] if stale else listed(all_magic)
            b.close_position, b.positions = close_position, positions
            await h.c.on_signal(signal(b, 504, "EURUSD", "buy"), manual=False)
            longs = [m["trade"] for m in h.of("trade_opened") if m["trade"].get("signal_id") == 504]
            check(longs and not h.of("signal_skipped") and not stale,
                  f"the scalp it just closed, still listed, does not hold the symbol: the signal opens: "
                  f"{h.of('signal_skipped')}")
        await until(lambda: short_closes(h), "the scalp reported closed")
        stop(h)


@scenario
async def daily_limit_stops_short_term_only():
    RULE.clear()
    b, h = harness(short={"risk_unit": "usd", "risk_min": 10, "risk_max": 10, "loss_unit": "usd", "daily_loss": 20,
                          "max_open": 3})
    RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 1,
                                                                           "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: len(short_opens(h)) == 2, "two scalps")
    await asyncio.sleep(0.05)
    check([round(t["lots"], 2) for t in short_opens(h)] == [0.2, 0.2], f"10 USD at 5 pips = 0.2 lots: {short_opens(h)}")
    RULE.clear()
    b.move(b.bids["EURUSD"] - 6 * PIP, "EURUSD")                        # both stops hit: -10 each
    b.move(b.bids["GBPUSD"] - 6 * PIP, "GBPUSD")
    await until(lambda: len(short_closes(h)) == 2, "both stop-losses reported")
    await until(lambda: h.c.scalper.day["pnl"] <= -20 + 1e-9, "the day's loss counted")
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    await until(lambda: any("daily loss limit reached" in m for m in h.logs("warn")), "the limit reported")
    await asyncio.sleep(0.1)
    check(len(short_opens(h)) == 2, "no new scalp once the day's limit is reached")
    check(sum("daily loss limit reached" in m for m in h.logs("warn")) == 1, "reported once")
    await h.c.on_signal(signal(b, 601, "GBPUSD", "buy"), manual=False)
    check(any(m["trade"].get("signal_id") == 601 for m in h.of("trade_opened")), "long-term goes on")
    real = C.utcnow
    C.utcnow = lambda: real() + timedelta(days=1)
    try:
        await until(lambda: len(short_opens(h)) == 3, "the next day it trades again")
        check(h.c.scalper.day["pnl"] == 0 and h.c.scalper.day["day"] == (real() + timedelta(days=1)).date().isoformat(),
              f"a new day starts over: {h.c.scalper.day}")
    finally:
        C.utcnow = real
    stop(h)

    RULE.clear()                                                         # the free part of the limit caps the risk
    b, h = harness(short={"risk_unit": "usd", "risk_min": 15, "risk_max": 15, "loss_unit": "usd", "daily_loss": 20,
                          "max_open": 3})
    RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 1,
                                                                           "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: len(short_opens(h)) == 2, "two scalps")
    check(sorted(round(t["risk"], 2) for t in short_opens(h)) == [5.0, 15.0],
          f"the second one risks only what is left of the day's 20: {short_opens(h)}")
    stop(h)

    RULE.clear()                                                         # the open scalps fill it, nothing lost yet
    b, h = harness(short={"risk_unit": "usd", "risk_min": 20, "risk_max": 20, "loss_unit": "usd", "daily_loss": 20,
                          "max_open": 3})
    s = h.c.scalper
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    s.start()
    await until(lambda: short_opens(h), "a scalp risking the day's whole 20")
    del RULE["EURUSD"]
    n = len(ASKED)
    RULE["GBPUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    await until(lambda: any(sym == "GBPUSD" for sym, _, _ in ASKED[n:]), "the rule says buy GBPUSD")
    reads = b.tick_reads
    await until(lambda: b.tick_reads > reads + 2, "that step over")
    check(len(short_opens(h)) == 1 and s._free() <= 0 and s.day["pnl"] == 0,
          f"no room left for a second one: {short_opens(h)} {s.day}")
    check(not s.day["warned"] and not any("daily loss limit reached" in m for m in h.logs("warn")),
          f"nothing lost today: the limit is not told as reached: {h.logs('warn')}")
    await jump(31)
    await until(lambda: len(short_opens(h)) == 2, "the first one closed at its hold time: room for the next")
    check(short_opens(h)[-1]["symbol"] == "GBPUSD" and short_opens(h)[-1]["risk"] < 20 and not s.day["warned"]
          and not any("daily loss limit reached" in m for m in h.logs("warn")),
          f"it risks what is free of the 20: {short_opens(h)[-1]} {s.day}")
    stop(h)


@scenario
async def pnl_today_counts_today_only():
    """The short-term result the long-term daily limit leaves out (pnl_today): what is booked to today, plus what the
    open scalps show. A day kept from before, with no step of the scalper since to start the new one, is not today."""
    b, h = harness()
    s = h.c.scalper
    try:
        s.daybook.roll(10_000.0)
        s.daybook.book(s.daybook.day_of(None), -50.0, 1)
        check(near(s.pnl_today(), -50), f"booked today: {s.day}")
        s.open[9] = {"id": 9, "risk": 10.0, "info": {}, "close_at": 0.0, "profit": -7.5}
        check(near(s.pnl_today(), -57.5), "and what an open scalp shows")
        set_clock(WEDNESDAY + timedelta(days=1))
        check(near(s.pnl_today(), -7.5) and s.day["day"] == "2026-10-28",
              f"the next day, before any step: only what is open: {s.pnl_today()} {s.day}")
        s.open.clear()
        check(s.pnl_today() == 0, f"nothing open: nothing today: {s.pnl_today()}")
    finally:
        set_clock(WEDNESDAY)
        stop(h)


@scenario
async def leftovers_and_stop():
    RULE.clear()
    b, h = harness(execution="notify")                                  # off: still closes what it finds
    left = b.add_position("EURUSD", "buy", 0.1, "fxs:123")
    h.c.scalper.start()
    await until(lambda: short_closes(h), "a scalp left by the last run is closed and reported")
    c = short_closes(h)[0]
    check(c["ticket"] == left and c["side"] == "buy" and near(c["lots"], 0.1) and not b.pos,
          f"reported with what the server needs to book it: {c}")
    check(b.tick_reads == 0 and not short_opens(h), "execution not auto: no ticks read, no new scalp")
    stop(h)

    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    stray = b.add_position("GBPUSD", "sell", 0.1, "fxs:999")             # a scalp it is not following
    main = asyncio.ensure_future(asyncio.sleep(60))
    await h.c.shutdown(main, budget=8)
    await asyncio.sleep(0)
    check(main.cancelled(), "shutdown ends the main task")
    check(not b.pos, f"a stop closes every scalp: {list(b.pos.values())}")
    closed = {c["ticket"] for c in short_closes(h)}
    check(short_opens(h)[0]["ticket"] in closed and stray in closed, f"and reports them: {short_closes(h)}")
    check(h.c.scalper.task.done(), "the scalper stopped")
    h.close()


@scenario
async def off_paper_bursts_and_failures():
    RULE.clear()
    b, h = harness(short={"enabled": False})
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await asyncio.sleep(0.1)
    check(not short_opens(h) and b.tick_reads == 0, "switched off: nothing")
    h.c.apply_settings({**T.SETTINGS, "short_term": SHORT})
    await until(lambda: short_opens(h), "switched on from the web app: it trades")
    stop(h)

    h = T.Harness(C.PaperBroker(), settings={**T.SETTINGS, "short_term": SHORT})
    h.c.scalper.start()
    check(h.c.scalper.task is None and not h.c.scalper.active(), "paper mode: no scalper")
    h.close()

    RULE.clear()
    b, h = harness()
    loop = asyncio.get_running_loop()
    h.c.bursts[77] = {"symbol": "EURUSD", "status": "scheduled", "release": loop.time() + 80,
                      "p": {"lead_seconds": 60}}                         # checks in 20 s: a 30 s scalp would be in its way
    RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 0.5,
                                                                           "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    await asyncio.sleep(0.1)
    check([t["symbol"] for t in short_opens(h)] == ["GBPUSD"], f"not where a news burst checks soon: {short_opens(h)}")
    h.c.bursts.clear()
    stop(h)

    RULE.clear()
    b, h = harness()
    b.lost_reply = 1                                                     # filled, but the answer never came
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp found by its tag after a lost answer")
    RULE.clear()
    t = short_opens(h)[0]
    check(t["ticket"] in h.c.scalper.open and len(b.pos) == 1, "followed, and only one opened")
    b.fail_close = 3
    await jump(31)
    await until(lambda: t["ticket"] in h.c.closing, "a close that fails is queued")
    check(b.pos and any("Could not close" in m for m in h.logs("warn")), "and warned about")
    await jump(10)
    await h.c.retry_closes()
    await until(lambda: short_closes(h), "close_loop closes it and the scalper reports it")
    check(not b.pos and len(short_closes(h)) == 1, "once")
    stop(h)


@scenario
def new_york_close_gate():
    utc = (lambda *a: datetime(*a))
    check(ny_offset(utc(2026, 3, 8, 6, 59, 59).replace(tzinfo=timezone.utc).timestamp()) == -5
          and ny_offset(utc(2026, 3, 8, 7).replace(tzinfo=timezone.utc).timestamp()) == -4
          and ny_offset(utc(2026, 11, 1, 5, 59, 59).replace(tzinfo=timezone.utc).timestamp()) == -4
          and ny_offset(utc(2026, 11, 1, 6).replace(tzinfo=timezone.utc).timestamp()) == -5,
          "US DST 2026: 8 March 07:00 UTC to 1 November 06:00 UTC")
    try:
        from zoneinfo import ZoneInfo
        ny = ZoneInfo("America/New_York")
    except Exception:           # no time-zone database on this PC: the rule itself is checked above
        ny = None
    if ny:
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        bad = [t for t in (start + timedelta(minutes=30 * i) for i in range(4 * 365 * 48))
               if ny_offset(t.timestamp()) * 3600 != t.astimezone(ny).utcoffset().total_seconds()]
        check(not bad, f"the same as the time-zone database, 2025-2028: {bad[:3]}")
    cases = [  # (UTC, hold, closes soon)
        (utc(2026, 7, 15, 20, 54), 30, False),         # Wednesday, summer (UTC-4): 16:54:00 New York
        (utc(2026, 7, 15, 20, 54, 30), 30, True),      # 17:00 minus 30 s minus 5 min
        (utc(2026, 7, 15, 22, 4, 59), 30, True),
        (utc(2026, 7, 15, 22, 5), 30, False),          # 18:05: open again
        (utc(2026, 7, 15, 20, 50), 300, True),         # a 5 min hold stops 10 min before
        (utc(2026, 7, 15, 20, 49, 59), 300, False),
        (utc(2026, 1, 14, 20, 54, 30), 30, False),     # winter (UTC-5): an hour later in UTC
        (utc(2026, 1, 14, 21, 54, 30), 30, True),
        (utc(2026, 1, 14, 23, 5), 30, False),
        (utc(2026, 7, 17, 20, 54), 30, False),         # Friday, before its close
        (utc(2026, 7, 17, 20, 55), 30, True),
        (utc(2026, 7, 18, 2, 0), 30, True),            # Friday 22:00 New York
        (utc(2026, 7, 18, 12, 0), 30, True),           # Saturday
        (utc(2026, 7, 19, 21, 0), 30, True),           # Sunday 17:00 New York
        (utc(2026, 7, 19, 22, 5), 30, False),          # Sunday 18:05: open
        (utc(2026, 7, 20, 12, 0), 30, False),          # Monday
        (utc(2026, 12, 24, 20, 54), 30, False),        # Christmas Eve (a Thursday): shut from 21:00 UTC
        (utc(2026, 12, 24, 20, 54, 30), 30, True),     # ... minus 30 s minus 5 min
        (utc(2026, 12, 25, 12, 0), 30, True),          # Christmas Day
        (utc(2025, 12, 26, 0, 0), 30, False),          # the day after (a Friday): open again
        (utc(2026, 12, 31, 20, 59, 45), 30, True),     # New Year's Eve
        (utc(2027, 1, 1, 12, 0), 30, True),            # New Year's Day
    ]
    got = [(t.isoformat(), hold) for t, hold, want in cases if closes_soon(t, hold) != want]
    check(not got, f"the daily, weekly and holiday closes: wrong at {got}")


@scenario
async def no_new_scalp_before_the_close():
    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    set_clock(datetime(2026, 7, 17, 20, 55))                            # Friday 16:55 New York
    try:
        h.c.scalper.start()
        await until(lambda: sum(s == "EURUSD" for s, _, _ in ASKED) >= 2, "the rule says buy, twice")
        check(not short_opens(h) and not b.calls_of("open"), "no new scalp as the week closes")
        set_clock(WEDNESDAY)
        await until(lambda: short_opens(h), "it trades on a Wednesday noon")
    finally:
        set_clock(WEDNESDAY)
    stop(h)


@scenario
async def the_rule_measures_in_the_brokers_pip():
    RULE.clear()
    PIPS.clear()
    b, h = harness()
    b.bids["XAUUSD"] = 2650.0
    h.c.scalper.symbols = ["EURUSD", "XAUUSD"]
    h.c.scalper.start()
    await until(lambda: "XAUUSD" in PIPS and "EURUSD" in PIPS, "the rule asked about both")
    check(PIPS == {"EURUSD": PIP, "XAUUSD": 0.1}, f"gold in its own pip, a pair as before: {PIPS}")
    check(b.spec_reads == {"EURUSD": 1, "XAUUSD": 1}, f"read once: {b.spec_reads}")
    n = len(ASKED)
    await until(lambda: len(ASKED) >= n + 2, "asked again")
    check(b.spec_reads == {"EURUSD": 1, "XAUUSD": 1}, f"not read at every look: {b.spec_reads}")
    await jump(3601)
    n = len(ASKED)
    await until(lambda: len(ASKED) >= n + 2, "asked an hour later")
    check(b.spec_reads == {"EURUSD": 2, "XAUUSD": 2}, f"read again after an hour: {b.spec_reads}")
    stop(h)


@scenario
async def dry_run_prices_at_the_symbols_digits():
    RULE.clear()
    b, h = harness()
    s = h.c.scalper
    h.c.dry_run = True
    said = []
    s.say = lambda key, message, every, level="warning": said.append(message)    # each time, none held back
    b.bids["XAUUSD"] = 2650.123                                         # ask 2650.12305
    s.symbols = ["EURUSD", "XAUUSD"]
    RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, XAUUSD={"side": "buy", "strength": 1,
                                                                           "sl_pips": 5})
    s.start()
    await until(lambda: {m.split(" @ ")[0][-6:] for m in said if m.startswith("[dry-run]")} == {"EURUSD", "XAUUSD"},
                "both said what they would do")
    lines = sorted({m for m in said if m.startswith("[dry-run]")})
    check(lines == ["[dry-run] short-term would buy 4.0 EURUSD @ 1.10005 SL 1.09955",
                    "[dry-run] short-term would buy 4.0 XAUUSD @ 2650.12 SL 2649.62"] and not b.calls_of("open"),
          f"gold at 2 digits, a pair at 5, nothing opened: {lines}")
    stop(h)


@scenario
async def a_fill_far_from_its_stop():
    RULE.clear()
    b, h = harness()
    b.slip = 1                                                           # 6 pips to the stop, not 5
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t = short_opens(h)[0]
    check(near(t["entry_price"] - t["sl"], 6 * PIP) and near(t["risk"], 150) and t["ticket"] in b.pos,
          f"125 allowed, 150 at its stop: kept (under 1.25 times), its real risk reported: {t}")
    check(near(h.c.scalper.open[t["ticket"]]["risk"], 150) and near(h.c.scalper._free(), 500 - 150),
          f"and counted against the day's limit: {h.c.scalper._free()}")
    stop(h)

    RULE.clear()
    b, h = harness()
    b.slip = 2                                                           # 7 pips: 175 at its stop
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t = short_opens(h)[0]
    await until(lambda: short_closes(h), "closed at once and reported")
    check(near(t["risk"], 175) and short_closes(h)[0]["ticket"] == t["ticket"] and not b.pos,
          f"over 1.25 times its money: closed at once: {t}")
    check(any("too far from its stop" in m and "175.00 at risk, 125.00 allowed" in m for m in h.logs("warn")),
          f"the user is told: {h.logs('warn')}")
    await until(lambda: not h.c.scalper.booking, "booked")
    check(not h.c.scalper.open and near(h.c.scalper._free(), 500 + h.c.scalper.day["pnl"]), "no longer at risk")
    stop(h)


@scenario
async def hold_x_alone_as_in_v120():
    # no Y (a user saved under v1.2.0): closed at exactly X, in profit or not, whatever the rule says meanwhile; the
    # stretch is not even looked at
    RULE.clear()
    STRETCH.clear()
    STRETCHED.clear()
    HOLDS.clear()
    b, h = harness()
    s = h.c.scalper
    whys = close_reasons(h)
    check("max_hold_seconds" not in s.cfg and s.holds() == (30, 30), f"no Y: Y is X: {s.cfg}")
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4}
    s.start()
    await until(lambda: short_opens(h), "a scalp")
    t = short_opens(h)[0]["ticket"]
    # the rule sells now (with too large a stop to trade it) and the price stretched far on: v1.2.0 heeded neither
    RULE["EURUSD"] = {"side": "sell", "strength": 0.5, "sl_pips": 1e6}
    STRETCH["EURUSD"] = -9.0
    b.move(b.bids["EURUSD"] + 3 * PIP)                                  # in profit before X
    n = len(ASKED)
    await until(lambda: sum(x == "EURUSD" for x, _, _ in ASKED[n:]) >= 2, "the rule asked with the scalp open")
    await jump(s.open[t]["close_at"] - asyncio.get_running_loop().time() - 1)     # a second before X
    await asyncio.sleep(0.1)
    check(t in b.pos and not short_closes(h) and not whys, f"kept until X: {whys}")
    await jump(2)
    await until(lambda: short_closes(h), "closed at X")
    check(whys == [(t, "short-term hold time")] and short_closes(h)[0]["pnl"] > 0, f"as v1.2.0 closed it: {whys}")
    check(not STRETCHED and set(HOLDS) == {30}, f"the rule told X, no stretch read: {set(HOLDS)} {STRETCHED}")
    stop(h)


@scenario
async def held_past_x_until_in_profit_or_y():
    # Y above X: from X on a scalp is closed once in profit (by a point at least); one at a loss is kept, until Y at
    # the latest; never closed before X for a profit
    RULE.clear()
    STRETCH.clear()
    HOLDS.clear()
    b, h = harness(short={"max_hold_seconds": 120})
    s = h.c.scalper
    whys = close_reasons(h)
    check(s.holds() == (30, 120), f"X 30, Y 120: {s.holds()}")
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4}
    s.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t, entry = short_opens(h)[0]["ticket"], short_opens(h)[0]["entry_price"]
    check(set(HOLDS) == {120}, f"the rule sizes its stop to Y: {set(HOLDS)}")
    o = s.open[t]
    check((o["hold"], o["max_hold"], o["stretch"]) == (30, 120, -2.4) and near(o["min_profit"], 2.5),
          f"its hold and what counts as a profit (a point on its 2.5 lots) kept with it: {o}")
    saved = json.loads(s._open_file().read_text())
    check(set(saved[str(t)]) == {"id", "risk", "info"}, f"saved as v1.2.0 saved it (a rollback reads it): {saved}")
    b.move(entry + 3 * PIP)                                              # in profit, X not over
    await jump(20)
    await asyncio.sleep(0.1)
    check(t in b.pos and not whys, f"in profit before X: kept: {whys}")
    b.move(entry - 1 * PIP)                                              # at a loss at X
    await jump(11)
    await asyncio.sleep(0.1)
    check(t in b.pos and not whys, f"at a loss after X: kept: {whys}")
    b.move(entry)                                                        # even: not in profit by a point
    await asyncio.sleep(0.1)
    check(t in b.pos and not whys, f"even: kept: {whys}")
    b.move(entry + 1 * PIP)
    await until(lambda: short_closes(h), "closed once in profit")
    check(whys == [(t, "short-term: in profit after 30s")] and near(short_closes(h)[0]["pnl"], 25),
          f"in profit after X: {whys} {short_closes(h)}")
    stop(h)

    RULE.clear()
    b, h = harness(short={"max_hold_seconds": 120})
    whys = close_reasons(h)
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t = short_opens(h)[0]["ticket"]
    await jump(119)                                                      # at a loss all along (the spread)
    await asyncio.sleep(0.1)
    check(t in b.pos and not whys, f"at a loss: kept until Y: {whys}")
    await jump(2)
    await until(lambda: short_closes(h), "closed at Y")
    check(whys == [(t, "short-term: max hold 120s")] and near(short_closes(h)[0]["pnl"], -12.5),
          f"at Y whatever it shows: {whys} {short_closes(h)}")
    stop(h)


@scenario
async def held_past_x_unless_turned_against():
    # Y above X: a scalp the rule turned against is closed at any age: the price stretched a whole unit further from
    # its average than at its entry, or the rule trading the other way now
    RULE.clear()
    STRETCH.clear()
    STRETCHED.clear()
    b, h = harness(short={"max_hold_seconds": 120})
    whys = close_reasons(h)
    a = len(ASKED)
    RULE.update(EURUSD={"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4},
                GBPUSD={"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4})
    h.c.scalper.start()
    await until(lambda: len(short_opens(h)) == 2, "two scalps")
    RULE.clear()
    tick = {x["symbol"]: x["ticket"] for x in short_opens(h)}
    STRETCH.update(EURUSD=-3.3, GBPUSD=-1.0)                            # not a whole unit further; back toward it
    n, k = len(STRETCHED), len(STRETCH_ASKED)
    await until(lambda: {"EURUSD", "GBPUSD"} <= set(STRETCHED[n:]), "the stretch looked at")
    # on the same bars, second and pip as the rule on that tick (one ask per pair and second)
    seen = STRETCH_ASKED[k:]
    check(seen and {x[:3] for x in seen} <= set(ASKED[a:]) and all(x[3] == PIPS[x[0]] == PIP for x in seen)
          and all(x[2] > 0 for x in seen), f"the stretch read as the rule: {seen} {ASKED[a:][-4:]}")
    await jump(5)
    await asyncio.sleep(0.1)
    check(len(b.pos) == 2 and not whys, f"not against them: kept: {whys}")
    STRETCH["EURUSD"] = -3.4
    await until(lambda: whys, "EURUSD turned against")
    check(whys == [(tick["EURUSD"], "short-term: turned against")], f"a whole unit further, before X: {whys}")
    await until(lambda: len(short_closes(h)) == 1, "reported")
    RULE["GBPUSD"] = {"side": "sell", "strength": 0.5, "sl_pips": 1e6}  # the rule sells (too large a stop to trade)
    await until(lambda: len(whys) == 2, "GBPUSD turned against")
    check(whys[1] == (tick["GBPUSD"], "short-term: turned against"), f"the rule trading the other way: {whys}")
    await until(lambda: not b.pos, "both closed")
    stop(h)


@scenario
async def turned_against_while_none_may_open():
    # a scalp held past X is still watched once no new scalp may open (execution switched to manual) or its pair is
    # left out of the list: the rule turning against it closes it all the same; the other pairs are not read then,
    # and nothing is once it is closed
    for change in ({"execution": "manual"}, {"short_term": {**SHORT, "max_hold_seconds": 120, "symbols": ["GBPUSD"]}}):
        RULE.clear()
        STRETCH.clear()
        b, h = harness(short={"max_hold_seconds": 120})
        s = h.c.scalper
        whys = close_reasons(h)
        RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4}
        s.start()
        await until(lambda: short_opens(h), "a scalp")
        t = short_opens(h)[0]["ticket"]
        RULE.clear()
        STRETCH["EURUSD"] = -3.0                                         # not a whole unit further
        h.c.apply_settings({**h.c.settings, **change})
        manual = "execution" in change
        check(not s.active() if manual else s.active() and s.watch() == ["GBPUSD"],
              f"{change}: {s.active()} {s.watch()}")
        s.looked.clear()
        await until(s.looked.is_set, "the step under way over")
        if manual:
            RULE["GBPUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}   # a scalp would open there: none may
        a, n = len(ASKED), len(STRETCHED)
        await until(lambda: STRETCHED[n:].count("EURUSD") >= 2, f"{change}: the scalp's pair still read")
        asked = {x for x, _, _ in ASKED[a:]}
        check("EURUSD" in asked and len(short_opens(h)) == 1 and t in b.pos and not whys,
              f"{change}: kept, nothing new: {asked} {short_opens(h)} {whys}")
        check(asked == ({"EURUSD"} if manual else {"EURUSD", "GBPUSD"}), f"{change}: the pairs read: {asked}")
        STRETCH["EURUSD"] = -3.4
        await until(lambda: whys, f"{change}: turned against")
        check(whys == [(t, "short-term: turned against")], f"{change}: closed for it: {whys}")
        await until(lambda: short_closes(h) and not s.open, f"{change}: reported")
        if manual:
            s.looked.clear()
            await until(s.looked.is_set, "the step under way over")
            reads = b.tick_reads
            await asyncio.sleep(0.1)
            check(b.tick_reads == reads and not s.bars, f"none open: nothing read: {b.tick_reads - reads} {s.bars}")
        check(len(short_opens(h)) == 1, f"{change}: none opened meanwhile: {short_opens(h)}")
        RULE.clear()
        stop(h)


@scenario
async def a_close_decided_is_kept():
    # a close refused when the scalp was in profit after X is still done once MT5 takes it, though the profit is gone
    # by then and Y is far (close_loop gave up on it meanwhile)
    RULE.clear()
    STRETCH.clear()
    b, h = harness(short={"max_hold_seconds": 600})
    h.c.CLOSE_RETRY_FOR = 20
    whys = close_reasons(h)
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t, entry = short_opens(h)[0]["ticket"], short_opens(h)[0]["entry_price"]
    b.move(entry + 2 * PIP)
    b.fail_close = 1
    await jump(31)
    await until(lambda: t in h.c.closing, "in profit after X: its close refused and queued")
    check(any(f"Could not close #{t}" in m and "(short-term: in profit after 30s)" in m for m in h.logs("warn")),
          f"told with its reason: {h.logs('warn')}")
    b.move(entry - 2 * PIP)                                              # the profit is gone
    b.fail_close = 3                                                     # close_loop's three tries fail: it gives up
    await jump(25)
    await h.c.retry_closes()
    check(t not in h.c.closing and any(f"Could not close #{t}" in m and "Gave up" in m for m in h.logs("error")),
          f"given up: {h.logs('error')}")
    await until(lambda: short_closes(h), "closed all the same")
    check(whys == [(t, "short-term: in profit after 30s")] * 2 and short_closes(h)[0]["pnl"] < 0,
          f"the close decided is kept: {whys} {short_closes(h)}")
    stop(h)


@scenario
async def an_open_scalp_saved_by_v120():
    # scalp_open.json as v1.2.0 wrote it (no open time, hold or stretch): closed at once on the next start, as then,
    # whatever Y is set now
    RULE.clear()
    file = TMP / "v120-open.json"
    real = C.Scalper._open_file
    C.Scalper._open_file = lambda self: file
    try:
        b = TickBroker(mode="netting")
        left = b.add_position("EURUSD", "buy", 0.1, "fxs:1777000000000")
        info = {"strategy": "short", "symbol": "EURUSD", "side": "buy", "lots": 0.1, "entry_price": b.ask(),
                "sl": b.ask() - 5 * PIP, "tp": None, "demo": True, "opened_at": "2026-10-28T11:59:00", "risk": 5.0,
                "confidence": 40}
        file.write_text(json.dumps({str(left): {"id": 1777000000000, "risk": 5.0, "info": info}}))
        b, h = harness(broker=b, short={"max_hold_seconds": 120})
        whys = close_reasons(h)
        check(set(h.c.scalper.open) == {left}, f"the next start knows it: {h.c.scalper.open}")
        h.c.scalper.start()
        await h.c.scalper.first_look()
        check(left not in b.pos and whys == [(left, "short-term hold time")], f"closed at once: {whys}")
        await until(lambda: short_closes(h) and not h.c.scalper.booking, "reported and booked")
        c = short_closes(h)[0]
        check(c["ticket"] == left and c["confidence"] == 40 and near(c["risk"], 5.0),
              f"reported with what it was: {c}")
        check(json.loads(file.read_text()) == {}, "nothing left in the file")
        stop(h)
    finally:
        C.Scalper._open_file = real
    # one followed as v1.2.0 followed it (no open time) closes at its close_at, as then
    old = {"id": 1, "risk": 5.0, "info": info, "close_at": 100.0}
    check(C.Scalper._exit(old, 99.9) is None and C.Scalper._exit(old, 100.0) == "short-term hold time",
          "at its close_at")


@scenario
async def a_scalp_left_open_without_its_hold():
    # a scalp with no hold or Y of its own (scalp_open.json as v1.2.0 wrote it, or one found by its tag) whose close is
    # refused (the market shut after a weekend restart, frozen, MT5 busy) stays followed: the steps go on all the same,
    # a scalp opens on another pair
    for left_by in ("file", "tag"):
        RULE.clear()
        file = TMP / f"left-open-{left_by}.json"
        real = C.Scalper._open_file
        C.Scalper._open_file = lambda self: file
        try:
            b = TickBroker(mode="netting")
            left = b.add_position("EURUSD", "buy", 0.1, "fxs:1777000000000")
            if left_by == "file":
                info = {"strategy": "short", "symbol": "EURUSD", "side": "buy", "lots": 0.1, "entry_price": b.ask(),
                        "sl": b.ask() - 5 * PIP, "tp": None, "demo": True, "opened_at": "2026-10-28T11:59:00",
                        "risk": 5.0, "confidence": 40}
                file.write_text(json.dumps({str(left): {"id": 1777000000000, "risk": 5.0, "info": info}}))
            b.fail_close = 10 ** 6                                       # every close refused
            b, h = harness(broker=b, short={"max_hold_seconds": 120})
            s = h.c.scalper
            whys = close_reasons(h)
            with T.Caught() as lines:
                RULE["GBPUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -2.4}
                s.start()
                await until(lambda: short_opens(h), f"{left_by}: a scalp on GBPUSD")
                n = len(ASKED)
                await until(lambda: len(ASKED) > n + 2, f"{left_by}: steps go on")
            check([t["symbol"] for t in short_opens(h)] == ["GBPUSD"] and left in s.open and left in b.pos
                  and whys and set(whys) == {(left, "short-term hold time")},
                  f"{left_by}: the left scalp's close refused, a new one on GBPUSD: {short_opens(h)} {whys}")
            check(not any("KeyError" in x for x in lines),
                  f"{left_by}: no step failed: {[x for x in lines if 'Short-term' in x]}")
            b.fail_close = 0
            stop(h)
        finally:
            C.Scalper._open_file = real


@scenario
async def a_fill_far_from_its_stop_with_y():
    # Y above X: one filled too far from its stop still closes at once, X not over: its close refused, and given up
    # by close_loop, the next step closes it again
    RULE.clear()
    STRETCH.clear()
    b, h = harness(short={"max_hold_seconds": 120})
    h.c.CLOSE_RETRY_FOR = 0
    whys = close_reasons(h)
    b.slip, b.fail_close = 2, 4                                          # the close at once and close_loop's 3 tries
    RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t = short_opens(h)[0]["ticket"]
    await until(lambda: t in h.c.closing, "its close at once refused")
    await jump(6)
    await h.c.retry_closes()
    check(t not in h.c.closing and any("Gave up" in m for m in h.logs("error")), f"given up: {h.logs('error')}")
    await until(lambda: short_closes(h), "closed at once all the same")
    check(whys == [(t, "short-term hold time")], f"as v1.2.0 closed it: {whys}")
    stop(h)


@scenario
async def y_keeps_new_scalps_from_the_close_and_bursts():
    # a scalp may last until Y now: none opens where the daily close, or a news burst's check, would come first
    for y, opens in ((None, True), (120, False)):
        RULE.clear()
        set_clock(datetime(2026, 10, 28, 20, 54))                       # Wednesday 16:54 New York: 6 min to the close
        try:
            b, h = harness(short={} if y is None else {"max_hold_seconds": y})
            RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
            h.c.scalper.start()
            n = len(ASKED)
            await until(lambda: sum(s == "EURUSD" for s, _, _ in ASKED[n:]) >= 2, "the rule says buy, twice")
            await asyncio.sleep(0.05)
            check(bool(short_opens(h)) is opens, f"Y {y}: a scalp before the close: {short_opens(h)}")
            stop(h)
        finally:
            set_clock(WEDNESDAY)

    for y, symbols in ((None, ["EURUSD", "GBPUSD"]), (120, ["GBPUSD"])):
        RULE.clear()
        b, h = harness(short={} if y is None else {"max_hold_seconds": y})
        loop = asyncio.get_running_loop()
        h.c.bursts[77] = {"symbol": "EURUSD", "status": "scheduled", "release": loop.time() + 160,
                          "p": {"lead_seconds": 60}}                     # checks EURUSD in 100 s
        RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 0.5,
                                                                               "sl_pips": 5})
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp")
        await asyncio.sleep(0.1)
        check(sorted(t["symbol"] for t in short_opens(h)) == symbols,
              f"Y {y}: 45 s (X + 15) or 135 s (Y + 15) to the burst's check: {short_opens(h)}")
        h.c.bursts.clear()
        stop(h)


@scenario
async def pip_caps_follow_the_symbols_scale():
    # the spread cap is in a currency pair's pips: gold's is 5 times it (1.5 -> 7.5); a pair's as it was
    RULE.clear()
    b, h = harness()
    b.bids["XAUUSD"] = 2650.0
    h.c.scalper.symbols = ["EURUSD", "XAUUSD"]
    b.spread = 7.6 * PIP                                                 # the quote says 7.6 pips on both
    RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, XAUUSD={"side": "buy", "strength": 1,
                                                                           "sl_pips": 5})
    h.c.scalper.start()
    n = len(ASKED)
    await until(lambda: sum(s == "XAUUSD" for s, _, _ in ASKED[n:]) >= 2, "the rule says buy gold, twice")
    await asyncio.sleep(0.05)
    check(not short_opens(h), f"7.6 pips: over gold's 7.5 too: {short_opens(h)}")
    b.spread = 7.4 * PIP
    await until(lambda: short_opens(h), "a scalp")
    await asyncio.sleep(0.1)
    check([t["symbol"] for t in short_opens(h)] == ["XAUUSD"], f"7.4: under gold's 7.5, over 1.5: {short_opens(h)}")
    stop(h)

    # a long-term signal's price drift cap (5 pips): 25 of gold's
    b, h = harness()
    b.bids["XAUUSD"] = 2650.0

    def gold(sid, pips):
        px = round(b.ask("XAUUSD") - pips * 0.1, 2)
        return {"id": sid, "symbol": "XAUUSD", "side": "buy", "price": px, "sl": px - 10.0, "tp": px + 20.0,
                "mode": "trend", "status": "open"}

    def pair(sid, pips):
        sig = signal(b, sid)
        return {**sig, **{k: sig[k] - pips * PIP for k in ("price", "sl", "tp")}}

    for sig, opens, reason in ((gold(801, 26), False, "price moved 26.0 pips from the signal"), (gold(802, 24), True, ""),
                               (pair(803, 6), False, "price moved 6.0 pips from the signal"), (pair(804, 4), True, "")):
        await h.c.on_signal(sig, manual=False)
        skipped = [m["reason"] for m in h.of("signal_skipped") if m["signal_id"] == sig["id"]]
        opened = any(m["trade"].get("signal_id") == sig["id"] for m in h.of("trade_opened"))
        check(opened is opens and skipped == ([] if opens else [reason]),
              f"#{sig['id']} {sig['symbol']}: opened {opened}, skipped {skipped}")
    stop(h)


@scenario
async def a_burst_waking_or_waiting_keeps_the_symbol():
    RULE.clear()
    b, h = harness()
    loop = asyncio.get_running_loop()
    woke = []

    def margin_for(symbol, side, lots):     # (MT5 thread) meanwhile a news burst on the symbol starts its run
        if not woke:
            woke.append(symbol)
            loop.call_soon_threadsafe(h.c.bursts.__setitem__, 78, {"symbol": "EURUSD", "status": "armed",
                                                                    "release": loop.time() + 30,
                                                                    "p": {"lead_seconds": 60}})
        return 200.0 * lots

    b.margin_for = margin_for
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: woke, "a scalp about to be placed")
    await asyncio.sleep(0.1)
    check(not short_opens(h) and not b.calls_of("open"), "the burst that woke meanwhile keeps the symbol")
    h.c.bursts.clear()
    await until(lambda: short_opens(h), "it trades once the burst is gone")
    stop(h)

    RULE.clear()
    b, h = harness()
    order = b.add_order("EURUSD", "buy", 0.1, b.ask() + 20 * PIP, "fxb:9:b")    # left by a burst, not swept yet
    RULE.update(EURUSD={"side": "buy", "strength": 1, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 0.5,
                                                                           "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    check([t["symbol"] for t in short_opens(h)] == ["GBPUSD"], f"not where a burst order waits: {short_opens(h)}")
    del b.orders[order]
    await until(lambda: len(short_opens(h)) == 2, "it trades there once the order is gone")
    stop(h)


@scenario
async def long_term_limit_leaves_short_term_out():
    RULE.clear()
    b, h = harness()                                                     # long-term: 3 % a day
    h.c.risk.update_day(C.utcnow().date(), 10_000.0)
    RULE.update(EURUSD={"side": "buy", "strength": 0.5, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 0.5,
                                                                           "sl_pips": 5})
    h.c.scalper.start()
    await until(lambda: len(short_opens(h)) == 2, "two scalps")
    RULE.clear()
    b.move(b.bids["EURUSD"] - 2 * PIP, "EURUSD")
    await until(lambda: near(h.c.scalper.pnl_today(), -62.5 - 12.5), "what the open scalps show counts")
    b.move(b.bids["EURUSD"] - 3.5 * PIP, "EURUSD")                      # both stopped out: -150 each
    b.move(b.bids["GBPUSD"] - 5.5 * PIP, "GBPUSD")
    await until(lambda: near(h.c.scalper.day["pnl"], -300) and not h.c.scalper.booking, "both booked")
    check(near(b.account()["equity"], 9_700) and near(h.c.scalper.pnl_today(), -300), "3 % down, all of it short-term")
    await h.c.on_signal(signal(b, 701, "EURUSD", "buy"), manual=False)
    check(any(m["trade"].get("signal_id") == 701 for m in h.of("trade_opened")) and not h.c.risk.halted,
          f"long-term is not halted by short-term's losses: {h.of('signal_skipped')}")
    loop = asyncio.get_running_loop()
    st = {"id": 90, "symbol": "GBPUSD", "p": T.plan(90)["params"], "release": loop.time() + 100, "test": False,
          "status": "scheduled"}
    why, orders = await h.c._burst_plan(st)
    check(why is None and orders, f"nor are news bursts: {why}")
    b.balance -= 310                                                     # a long-term loss of 3 %
    await h.c.on_signal(signal(b, 702, "GBPUSD", "buy"), manual=False)
    sk = h.of("signal_skipped")
    check(sk and sk[-1]["signal_id"] == 702 and "daily loss limit" in sk[-1]["reason"] and h.c.risk.halted,
          f"long-term's own losses still halt it: {sk}")
    stop(h)


@scenario
async def open_scalps_survive_a_restart():
    RULE.clear()
    files = TMP / "restart-open.json", TMP / "restart-day.json"
    real = C.Scalper._open_file, C.Scalper._day_file
    C.Scalper._open_file, C.Scalper._day_file = (lambda self: files[0]), (lambda self: files[1])
    try:
        b, h = harness()
        RULE.update(EURUSD={"side": "buy", "strength": 0.5, "sl_pips": 5}, GBPUSD={"side": "buy", "strength": 0.5,
                                                                               "sl_pips": 5})
        h.c.scalper.start()
        await until(lambda: len(short_opens(h)) == 2, "two scalps")
        RULE.clear()
        opened = {t["ticket"]: t for t in short_opens(h)}
        saved = json.loads(files[0].read_text())
        check(sorted(map(int, saved)) == sorted(opened) and all(
            saved[str(t)]["risk"] == 125 and saved[str(t)]["info"]["symbol"] == opened[t]["symbol"]
            and saved[str(t)]["info"]["confidence"] == 50 for t in opened), f"saved as they open: {saved}")
        stop(h)                                                          # the connector dies: no stop, no closes
        eur = next(t for t, x in opened.items() if x["symbol"] == "EURUSD")
        b.move(b.bids["EURUSD"] - 5.5 * PIP, "EURUSD")                  # its stop-loss hit while it is away: -150
        check(eur not in b.pos and len(b.pos) == 1, "one closed, one still open")

        b, h = harness(broker=b)
        check(set(h.c.scalper.open) == set(opened), f"the next start knows both: {h.c.scalper.open}")
        h.c.scalper.start()
        await h.c.scalper.first_look()
        check(not b.pos, "the one still open is closed at once")
        await until(lambda: len(short_closes(h)) == 2 and not h.c.scalper.booking, "both reported and booked")
        got = {c["ticket"]: c for c in short_closes(h)}
        check(near(got[eur]["pnl"], -150) and got[eur]["symbol"] == "EURUSD" and near(got[eur]["risk"], 125)
              and got[eur]["confidence"] == 50, f"the one closed meanwhile: reported with what it was: {got[eur]}")
        day = h.c.scalper.day["pnl"]
        check(near(day, sum(c["pnl"] for c in got.values())) and near(day, -162.5), f"both booked: {h.c.scalper.day}")
        check(json.loads(files[0].read_text()) == {}, "nothing left in the file")
        stop(h)
    finally:
        C.Scalper._open_file, C.Scalper._day_file = real


@scenario
async def booked_to_the_day_it_closed():
    RULE.clear()
    set_clock(datetime(2026, 10, 28, 23, 59, 50))
    try:
        b, h = harness()
        RULE["EURUSD"] = {"side": "buy", "strength": 0.5, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp, just before midnight (UTC)")
        RULE.clear()
        check(h.c.scalper.day["day"] == "2026-10-28", f"its day: {h.c.scalper.day}")
        set_clock(datetime(2026, 10, 29, 0, 0, 30))
        await jump(31)
        await until(lambda: short_closes(h) and not h.c.scalper.booking, "closed after midnight and booked")
        check(h.c.scalper.day == {"day": "2026-10-29", "start": 10_000.0, "pnl": -12.5, "warned": False,
                                  "guessed": {}},
              f"to the new day, which starts from the balance before it: {h.c.scalper.day}")
        stop(h)
    finally:
        set_clock(WEDNESDAY)


@scenario
async def server_pauses_and_pairs():
    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    session = asyncio.ensure_future(h.c.session(T.Link(h, [{"type": "kicked", "reason": "subscription_inactive"}])))
    await until(lambda: h.c.scalper.paused, "the server refused the session")
    RULE["GBPUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    await asyncio.sleep(0.05)                                            # a read already under way
    reads = b.tick_reads
    await jump(31)
    await until(lambda: short_closes(h), "the open scalp still closes at its hold time")
    await asyncio.sleep(0.1)
    check(len(short_opens(h)) == 1 and b.tick_reads == reads and not h.c.scalper.active(), "but no new one opens")
    await jump(600)
    check(await session is False, "it retries 10 minutes later")
    short = {"settings": {**T.SETTINGS, "short_term": SHORT}}
    await h.c.session(T.Link(h, [{**T.WELCOME, **short, "symbols": ["GBPUSD"]}]))
    check(not h.c.scalper.paused and h.c.scalper.symbols == ["GBPUSD"], "a welcome: on again")
    await until(lambda: len(short_opens(h)) == 2, "it trades again")
    await h.c.session(T.Link(h, [{"type": "settings", **short, "symbols": ["EURUSD", "USDJPY"]}]))
    check(h.c.scalper.symbols == ["EURUSD", "USDJPY"], f"settings bring the engine's pairs too: {h.c.scalper.symbols}")
    await h.c.session(T.Link(h, [{"type": "settings", **short}]))
    check(h.c.scalper.symbols == ["EURUSD", "USDJPY"], "settings without them keep them")
    stop(h)


async def run_with(h, link):
    """The real run() on `link` (a server that ends the session for good), its background loops ended after."""
    real, C.connect = C.connect, lambda url, **kw: link
    try:
        await asyncio.wait_for(h.c.run(), 30)
    finally:
        C.connect = real
        for t in (h.c.sweeper, h.c.closer):
            t and t.cancel()


@scenario
async def the_end_of_run_closes_the_scalps():
    RULE.clear()
    b, h = harness(execution="notify")
    left = b.add_position("EURUSD", "buy", 0.1, "fxs:321")              # left open by the last run
    seen = []

    class Refused(T.Link):
        async def __aenter__(self):
            seen.append(left in b.pos)
            return self

    await run_with(h, Refused(h, [{"type": "error", "reason": "invalid_token"}]))
    check(seen == [False], "the last run's scalp is closed before the server is asked in")
    check([c["ticket"] for c in short_closes(h)] == [left], f"and reported: {short_closes(h)}")
    h.close()

    welcome = {**T.WELCOME, "settings": {**T.SETTINGS, "short_term": SHORT}, "symbols": ["EURUSD", "GBPUSD"]}
    for fails in (0, 99):
        RULE.clear()
        b, h = harness()
        RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}

        class Revoked(T.Link):
            async def __aiter__(self):
                yield json.dumps(welcome)
                await until(lambda: short_opens(h), "a scalp during the session")
                b.fail_close = fails
                yield json.dumps({"type": "kicked", "reason": "token_revoked"})

        await run_with(h, Revoked(h, []))
        t = short_opens(h)[0]["ticket"]
        if not fails:
            check(not b.pos and [c["ticket"] for c in short_closes(h)] == [t],
                  f"the server ended it for good: the scalp is closed and reported before run() ends: "
                  f"{short_closes(h)}")
        else:
            check(t in b.pos and any(f"Could not close #{t}: the connector stopped" in m for m in h.logs("error")),
                  f"one that will not close is reported: {h.logs('error')}")
            check(str(t) in json.loads(h.c.scalper._open_file().read_text()), "and kept for the next start")
        h.close()


@scenario
async def unsent_results_survive_a_restart():
    """A result queued with no link (a scalp closed as the server ended the session for good) is kept in
    outbox.json: the next start sends it first, after its welcome, and the file is empty again."""
    RULE.clear()
    b, h = harness()
    h.c.send = C.Connector.send.__get__(h.c)                            # the real send: the link, else the outbox
    welcome = {**T.WELCOME, "settings": {**T.SETTINGS, "short_term": SHORT}, "symbols": ["EURUSD", "GBPUSD"]}
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}

    class Revoked(T.Link):
        async def __aiter__(self):
            yield json.dumps(welcome)
            await until(lambda: short_opens(h), "a scalp during the session")
            RULE.clear()
            yield json.dumps({"type": "kicked", "reason": "token_revoked"})

    await run_with(h, Revoked(h, []))
    opened = sorted(x["ticket"] for x in short_opens(h))
    box = h.c._outbox_file()
    kept = sorted(m["trade"]["ticket"] for m in json.loads(box.read_text()) if m["type"] == "trade_closed")
    check(opened and not b.pos and not short_closes(h) and kept == opened,
          f"closed with no link: each trade_closed is kept in the file, not only in memory: {kept} "
          f"(sent: {[(m['type'], (m.get('trade') or {}).get('ticket')) for m in h.msgs]})")
    h.close()

    real = T.TConnector._outbox_file
    T.TConnector._outbox_file = lambda self: box                         # the next start, from the same folder
    try:
        b, h = harness(broker=b)
    finally:
        T.TConnector._outbox_file = real
    h.c._box = box
    check(sorted(m["trade"]["ticket"] for m in h.c.outbox if m["type"] == "trade_closed") == opened,
          "the next start loads it")
    h.c.send = C.Connector.send.__get__(h.c)
    loaded = list(h.c.outbox)
    await h.c.session(T.Link(h, [welcome]))
    check(sorted(c["ticket"] for c in short_closes(h)) == opened and h.msgs[:len(loaded)] == loaded,
          f"and sends it first once welcomed: {[m['type'] for m in h.msgs]}")
    check(not h.c.outbox and json.loads(box.read_text()) == [], "sent: the outbox and its file are empty")
    stop(h)


class NoClock(TickBroker):
    """MetaTrader 5's clock not proven by a live tick yet (MT5Broker._utc): a close's time is None `unproven` times."""
    unproven = 0

    def closed_result(self, ticket):
        r = super().closed_result(ticket)
        if r and self.unproven > 0:
            self.unproven -= 1
            return {**r, "exit_time": None}
        return r


@scenario
async def a_close_waits_for_the_clock():
    """A scalp's stop hit on Tuesday while the connector was away, the connector back on Wednesday before MT5's clock
    is proven: its report waits for the close time, so it is booked to Tuesday (not today's limit)."""
    RULE.clear()
    files = TMP / "clock-open.json", TMP / "clock-day.json"
    real = C.Scalper._open_file, C.Scalper._day_file, C.Scalper.CLOCK_STEP
    C.Scalper._open_file, C.Scalper._day_file = (lambda self: files[0]), (lambda self: files[1])
    C.Scalper.CLOCK_STEP = 0.05
    set_clock(datetime(2026, 10, 27, 15, 0))                             # Tuesday
    try:
        b, h = harness(broker=NoClock(mode="netting"))
        RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp")
        RULE.clear()
        stop(h)                                                          # the connector dies
        b.move(b.bids["EURUSD"] - 6 * PIP, "EURUSD")                     # its stop-loss hit on Tuesday
        set_clock(datetime(2026, 10, 28, 22, 30))                        # back on Wednesday evening
        b.unproven = 5                                                   # the clock known at the sixth history read
        b, h = harness(broker=b)
        h.c.scalper.start()
        await h.c.scalper.first_look()
        await until(lambda: short_closes(h) and not h.c.scalper.booking, "reported and booked")
        got = short_closes(h)[0]
        check(got["closed_at"].startswith("2026-10-27") and got["pnl"] < 0, f"its close time is Tuesday's: {got}")
        check(h.c.scalper.day["day"] == "2026-10-27" and near(h.c.scalper.day["pnl"], got["pnl"]),
              f"booked to Tuesday, Wednesday's limit untouched: {h.c.scalper.day}")
        stop(h)
    finally:
        C.Scalper._open_file, C.Scalper._day_file, C.Scalper.CLOCK_STEP = real
        set_clock(WEDNESDAY)


@scenario
async def the_daily_limit_holds_while_a_close_waits_for_the_clock():
    """The last run's scalp hit its stop while the connector was away and its report waits for MT5's clock: its risk
    still counts against today's limit meanwhile, so no new scalp opens on room its loss is about to take."""
    RULE.clear()
    files = TMP / "wait-open.json", TMP / "wait-day.json"
    real = C.Scalper._open_file, C.Scalper._day_file, C.Scalper.CLOCK_STEP
    C.Scalper._open_file, C.Scalper._day_file = (lambda self: files[0]), (lambda self: files[1])
    C.Scalper.CLOCK_STEP = 0.05
    short = {"risk_unit": "usd", "risk_min": 10, "risk_max": 10, "loss_unit": "usd", "daily_loss": 20, "max_open": 3}
    try:
        b, h = harness(broker=NoClock(mode="netting"), short=short)
        RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp risking 10")
        RULE.clear()
        t = short_opens(h)[0]["ticket"]
        stop(h)                                                          # the connector dies
        b.move(b.bids["EURUSD"] - 6 * PIP, "EURUSD")                     # its stop-loss hit while it is away
        files[1].write_text(json.dumps({"day": C.utcnow().date().isoformat(), "start": 10_000.0, "pnl": -12.0,
                                        "warned": False}))               # 12 of today's 20 already lost
        b.unproven = 10 ** 6                                             # MT5's clock not proven yet
        b, h = harness(broker=b, short=short)
        h.c.scalper.start()
        await h.c.scalper.first_look()
        ASKED.clear()
        RULE["GBPUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
        await until(lambda: sum(a[0] == "GBPUSD" for a in ASKED) >= 2, "the rule says buy GBPUSD")
        await asyncio.sleep(0.1)
        check(t in h.c.scalper.booking and near(h.c.scalper._free(), -2),
              f"its report waits for the clock, its risk (10) still counted: free {h.c.scalper._free()}")
        check(not short_opens(h), f"no scalp on the 8 that look free until its loss is booked: {short_opens(h)}")
        b.unproven = 0                                                   # the clock proven
        await until(lambda: short_closes(h) and not h.c.scalper.booking, "reported and booked")
        loss = short_closes(h)[0]["pnl"]                                 # past its stop (the gap, the spread)
        check(loss <= -10 and near(h.c.scalper.day["pnl"], -12 + loss), f"booked to today: {h.c.scalper.day}")
        await until(lambda: any("daily loss limit reached" in m for m in h.logs("warn")), "the limit reported")
        check(not short_opens(h), "and none once it is booked")
        stop(h)
    finally:
        C.Scalper._open_file, C.Scalper._day_file, C.Scalper.CLOCK_STEP = real
        RULE.clear()


@scenario
async def a_close_by_this_run_does_not_wait_for_the_clock():
    """A scalp the last run left open is closed by this run's first look: its close time is now, so its report does
    not wait for MT5's clock (only one that closed while the connector was away does)."""
    RULE.clear()
    files = TMP / "now-open.json", TMP / "now-day.json"
    real = C.Scalper._open_file, C.Scalper._day_file
    C.Scalper._open_file, C.Scalper._day_file = (lambda self: files[0]), (lambda self: files[1])
    try:
        b, h = harness(broker=NoClock(mode="netting"))
        RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp")
        RULE.clear()
        t = short_opens(h)[0]["ticket"]
        stop(h)                                                          # the connector dies, the scalp still open
        b.unproven = 10 ** 6                                             # MT5's clock not proven yet
        b, h = harness(broker=b)
        h.c.scalper.start()
        await h.c.scalper.first_look()
        check(t not in b.pos, "the first look closes it")
        await until(lambda: short_closes(h) and not h.c.scalper.booking,
                    f"reported and booked at once, not after CLOCK_WAIT ({C.Scalper.CLOCK_WAIT:.0f} s)")
        got, today = short_closes(h)[0], C.utcnow().date().isoformat()
        check(got["ticket"] == t and got["closed_at"].startswith(today) and h.c.scalper.day["day"] == today
              and near(h.c.scalper.day["pnl"], got["pnl"]), f"closed now, booked to today: {got} {h.c.scalper.day}")
        stop(h)
    finally:
        C.Scalper._open_file, C.Scalper._day_file = real


@scenario
async def a_close_asked_for_does_not_wait_for_the_clock():
    """The last run's scalp closed on a close request (Close in the web app) before MT5's clock is proven, at the
    request's second attempt or only at close_loop's retry: closed by this run, its report does not wait either."""
    RULE.clear()
    real = C.Scalper._open_file, C.Scalper._day_file
    try:
        for fails in (1, 3):            # the request's attempts: the first one refused / all three refused
            files = TMP / f"ask{fails}-open.json", TMP / f"ask{fails}-day.json"
            C.Scalper._open_file, C.Scalper._day_file = (lambda self, f=files[0]: f), (lambda self, f=files[1]: f)
            b, h = harness(broker=NoClock(mode="netting"))
            RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
            h.c.scalper.start()
            await until(lambda: short_opens(h), "a scalp")
            RULE.clear()
            t = short_opens(h)[0]["ticket"]
            stop(h)                                                      # the connector dies, the scalp still open
            b.unproven = 10 ** 6                                         # MT5's clock not proven yet
            b, h = harness(broker=b)                                     # back; its scalper not started yet
            check(h.c.scalper.open[t].get("away"), "the last run's scalp")
            b.fail_close = fails
            await h.c.close_ticket(t, "closed from the web app")
            if fails == 3:
                check(t in b.pos and t in h.c.closing, "all three attempts refused: left to close_loop")
                await jump(5.1)
                await h.c.retry_closes()
            check(t not in b.pos, f"closed ({fails} refused)")
            await until(lambda: short_closes(h) and not h.c.scalper.booking,
                        f"reported and booked at once, not after CLOCK_WAIT ({fails} refused)")
            got, today = short_closes(h)[0], C.utcnow().date().isoformat()
            check(got["ticket"] == t and got["closed_at"].startswith(today) and near(h.c.scalper.day["pnl"], got["pnl"]),
                  f"closed now, booked to today: {got} {h.c.scalper.day}")
            stop(h)
    finally:
        C.Scalper._open_file, C.Scalper._day_file = real


@scenario
async def a_stop_hit_in_this_run_does_not_wait_for_the_clock():
    """This run's scalp hits its stop-loss before MT5's clock is proven (just after a start): it was open in this
    run, so it closed just now; reported and booked at once, to today."""
    RULE.clear()
    b, h = harness(broker=NoClock(mode="netting"))
    b.unproven = 10 ** 6
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t = short_opens(h)[0]["ticket"]
    b.move(b.bids["EURUSD"] - 6 * PIP, "EURUSD")                         # its stop-loss hit
    await until(lambda: short_closes(h) and not h.c.scalper.booking,
                f"reported and booked at once, not after CLOCK_WAIT ({C.Scalper.CLOCK_WAIT:.0f} s)")
    got, today = short_closes(h)[0], C.utcnow().date().isoformat()
    check(got["ticket"] == t and got["pnl"] < 0 and got["closed_at"].startswith(today)
          and near(h.c.scalper.day["pnl"], got["pnl"]), f"booked to today: {got} {h.c.scalper.day}")
    stop(h)


class LagHistory(TickBroker):
    """The history behind or unreadable: a closed position's result is None `lag` times, its read fails `broken`
    times."""
    lag = broken = 0

    def closed_result(self, ticket):
        if self.broken > 0:
            self.broken -= 1
            raise RuntimeError("history_deals_get failed: (-10004, 'No IPC connection')")
        if self.lag > 0:
            self.lag -= 1
            return None
        return super().closed_result(ticket)


@scenario
async def a_result_late_or_never_in_the_history():
    """A scalp's result shows in the history only after a while: its report waits for it (one trade_closed, with its
    P&L) and the day books that. One never read (the history behind for good, or MT5 not answering) is booked as a
    loss of all it risked, so the daily limit does not miss it, and the web app's log says so."""
    RULE.clear()
    real = C.Scalper.REPORT_GAP
    C.Scalper.REPORT_GAP = 0.05
    try:
        for case, (lag, broken) in {"late": (6, 0), "never": (10 ** 6, 0), "unreadable": (0, 10 ** 6)}.items():
            b, h = harness(broker=LagHistory(mode="netting"))
            RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
            h.c.scalper.start()
            await until(lambda: short_opens(h), "a scalp")
            RULE.clear()
            t = short_opens(h)[0]["ticket"]
            risk = h.c.scalper.open[t]["risk"]
            b.lag, b.broken = lag, broken
            await jump(31)                                               # its hold time: the scalper closes it
            await until(lambda: t not in b.pos and t not in h.c.scalper.open and not h.c.scalper.booking,
                        f"closed and booked ({case})")
            sent = [x for x in short_closes(h) if x["ticket"] == t]
            warned = [m for m in h.logs("warn") if f"Short-term #{t}: its result could not be read" in m]
            day = h.c.scalper.day["pnl"]
            if case == "late":
                check(len(sent) == 1 and sent[0]["pnl"] is not None and near(day, sent[0]["pnl"]) and not warned,
                      f"reported once its result is in, with it, and booked: {sent} {day} {warned}")
            else:
                check(risk > 0 and abs(day + risk) < 0.006 and len(warned) == 1,
                      f"{case}: booked as a loss of what it risked ({risk}), and said: {day} {warned}")
                check(len(sent) == (1 if case == "never" else 0) and all(x["pnl"] is None for x in sent),
                      f"{case}: its last try reports it with no result (none if the history never answers): {sent}")
            b.lag = b.broken = 0
            stop(h)
    finally:
        C.Scalper.REPORT_GAP = real
        RULE.clear()


@scenario
async def a_scalp_missing_from_one_read_is_booked_once():
    """MT5 leaves an open scalp out of one read of the positions: taken for closed, its report waits for the result.
    Listed again, it is followed again at once (its report stands down), and its real close is reported and booked
    once."""
    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    t = short_opens(h)[0]["ticket"]
    entries, report = [], h.c.scalper._report

    def spy(ticket, s, wait=False):                                      # the entry each report checks it still owns
        entries.append(s)
        return report(ticket, s, wait)

    h.c.scalper._report = spy
    b.unlist(t, 0.05)
    await until(lambda: t in h.c.scalper.booking, "taken for closed while left out of the list")
    await until(lambda: t in h.c.scalper.open and t not in h.c.scalper.booking, "listed again: followed again at once")
    check(not short_closes(h), f"nothing reported meanwhile: {short_closes(h)}")
    await jump(31)                                                       # its hold time: closed, and the first
    await until(lambda: t not in b.pos and not h.c.scalper.booking and not h.c.scalper.reports,  # report wakes
                "closed at its hold time, every report done")
    # the first report, waking while the new one sends, would send too if it found its own entry booked again
    check(len(entries) == 2 and entries[1] is not entries[0],
          f"the new report has an entry of its own, so the first one stands down: {len(entries)} report(s)")
    sent = [x for x in short_closes(h) if x["ticket"] == t]
    day = h.c.scalper.day["pnl"]
    check(len(sent) == 1 and sent[0]["pnl"] is not None and near(day, sent[0]["pnl"]),
          f"reported and booked once: {sent} {day}")
    stop(h)


@scenario
async def a_scalp_out_of_the_list_past_its_report_is_booked_once():
    """MT5 leaves an open scalp out of its list for longer than the report waits: the report gives up and counts what
    it risked (kept for the day, across a restart too). Listed again, it is closed as a scalp not followed, and its real
    result replaces that loss instead of adding to it; the web app's log says so."""
    RULE.clear()
    real = C.Scalper.REPORT_GAP
    C.Scalper.REPORT_GAP = 0.05                                          # its report gives up after about 2 s
    try:
        b, h = harness()
        RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp")
        RULE.clear()
        t = short_opens(h)[0]["ticket"]
        risk = h.c.scalper.open[t]["risk"]
        b.move(round(b.bids["EURUSD"] + 2 * PIP, 5))                     # it shows a small profit
        b.unlist(t, 3.0)
        await until(lambda: t in h.c.scalper.booking, "taken for closed")
        await until(lambda: t not in h.c.scalper.booking, "its report gave up")
        saved = json.loads(h.c.scalper._day_file().read_text())
        check(near(h.c.scalper.day["pnl"], -risk) and h.c.scalper.day["guessed"] == {str(t): risk}
              and saved["guessed"] == {str(t): risk} and h.c.scalper._load_day()["guessed"] == {str(t): risk},
              f"counted as a loss of what it risked, kept for the day: {h.c.scalper.day} {saved}")
        h.c.scalper.day["warned"] = True                                 # as if that loss had reached the limit
        await until(lambda: any(p["ticket"] == t for p in b.positions()), "listed again", 5)
        await jump(C.Scalper.STRAYS_EVERY + 0.5)                         # the next look for scalps not followed
        await until(lambda: t not in b.pos and not h.c.scalper.booking and not h.c.scalper.reports,
                    "closed as a scalp not followed, and booked")
        got = round(sum(d["profit"] for d in b.deals if d["position"] == t), 2)
        sent = [x["pnl"] for x in short_closes(h) if x["ticket"] == t]
        back = [m for m in h.logs("info") if f"Short-term #{t}: it was still open after all" in m]
        check(got > 0 and near(h.c.scalper.day["pnl"], got) and not h.c.scalper.day["guessed"]
              and not h.c.scalper.day["warned"],
              f"its real result replaces the loss counted ({got}), the limit not reached any more: {h.c.scalper.day}")
        check(sent == [None, got] and len(back) == 1 and f"{risk:.2f}" in back[0],
              f"reported without a result, then with it; the log says the loss is taken back: {sent} {back}")
        stop(h)
    finally:
        C.Scalper.REPORT_GAP = real


@scenario
async def a_stray_never_read_books_its_stop():
    """A scalp it was not following (left by the last run: risk 0) whose result is never read is booked as a loss of
    what its stop-loss risks, not of nothing."""
    RULE.clear()
    real = C.Scalper.REPORT_GAP
    C.Scalper.REPORT_GAP = 0.05
    try:
        b, h = harness(broker=LagHistory(mode="netting"))
        stray = b.add_position("EURUSD", "buy", 0.1, "fxs:999")
        with b.lock:
            p = b.pos[stray]
            p["sl"] = round(p["entry_price"] - 7 * PIP, 5)
            spec = b.symbol_spec("EURUSD")
            want = round(abs(p["entry_price"] - p["sl"]) / spec.pip * spec.pip_value_per_lot * 0.1, 2)
        b.lag = 10 ** 6
        h.c.scalper.start()
        await until(lambda: stray not in b.pos and stray not in h.c.scalper.open and not h.c.scalper.booking,
                    "the stray closed at once, and booked")
        warned = [m for m in h.logs("warn") if f"Short-term #{stray}: its result could not be read" in m]
        day = h.c.scalper.day["pnl"]
        check(want > 1 and abs(day + want) < 0.006 and len(warned) == 1 and f"{want:.2f}" in warned[0],
              f"booked as a loss of what its stop risks ({want}), and said: {day} {warned}")
        b.lag = 0
        stop(h)
    finally:
        C.Scalper.REPORT_GAP = real


@scenario
async def a_stop_closes_the_scalps_in_its_own_time():
    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    RULE.clear()
    h.c.stopping = True                                                  # a stop began (its bursts end first)
    await jump(31)
    await until(lambda: short_closes(h), "stopping: the scalp still closes at its hold time")
    check(not b.pos and not h.c.scalper.task.done(), "and the scalper goes on until its stop")
    stop(h)

    RULE.clear()
    b, h = harness()
    RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
    h.c.scalper.start()
    await until(lambda: short_opens(h), "a scalp")
    seen = []

    async def slow_sweep(at, ended=True):          # the bursts' clean-up takes its time
        await asyncio.sleep(0.5)
        seen.append(dict(b.pos))

    h.c._sweep_at_stop = slow_sweep
    main = asyncio.ensure_future(asyncio.sleep(60))
    await h.c.shutdown(main, budget=8)
    check(seen == [{}] and short_closes(h), f"the scalps are closed meanwhile, not after it: {seen}")
    h.close()

    for case in ("refused", "stuck", "retried"):
        RULE.clear()
        b, h = harness()
        RULE["EURUSD"] = {"side": "buy", "strength": 1, "sl_pips": 5}
        h.c.scalper.start()
        await until(lambda: short_opens(h), "a scalp")
        t = short_opens(h)[0]["ticket"]
        if case == "stuck":
            close = b.close_position
            b.close_position = lambda *a, **kw: time.sleep(2.5) or close(*a, **kw)   # MT5 hangs on it
        else:
            b.fail_close = 99 if case == "refused" else 1
        main = asyncio.ensure_future(asyncio.sleep(60))
        await h.c.shutdown(main, budget=2 if case == "stuck" else 8)
        errors = [m for m in h.logs("error") if f"Could not close #{t}" in m]
        saved = json.loads(h.c.scalper._open_file().read_text())
        if case == "retried":
            await until(lambda: short_closes(h), "closed by the stop's last try and reported")
            check(not errors and not b.pos and str(t) not in json.loads(h.c.scalper._open_file().read_text()),
                  f"a scalp the last try closed is not reported as left open: {errors}")
        else:
            check(len(errors) == 1 and ("before closing this short-term trade" in errors[0]) == (case == "stuck"),
                  f"a scalp still open at the end is reported once ({case}): {errors}")
            check(str(t) in saved, "and kept for the next start")
        h.close()


# ── runner ─────────────────────────────────────────────────────
async def main() -> int:
    C.Connector.BURST_POLL, C.Connector.BURST_RESULT_WAIT, C.Connector.CLOSE_PAUSE = 0.01, 0.01, 0.01
    failed = 0
    for fn in TESTS:
        t0 = time.monotonic()
        ASKED.clear()
        try:
            res = fn()
            if asyncio.iscoroutine(res):
                await res
            print(f"PASS {fn.__name__} ({time.monotonic() - t0:.2f}s)")
        except Exception as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
            traceback.print_exc()
        # what a scenario left running (a report still waiting, a scalper not stopped on a failure) must not act
        # in the next one
        left = asyncio.all_tasks() - {asyncio.current_task()}
        for task in left:
            task.cancel()
        if left:
            await asyncio.wait(left, timeout=2)
    return failed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO if "-v" in sys.argv else logging.CRITICAL,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    picked = [fn for fn in TESTS if fn.__name__ in sys.argv[1:]]
    TESTS[:] = picked or TESTS
    try:
        with asyncio.Runner(loop_factory=T.SkewLoop) as runner:
            failures = runner.run(main())
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} scenarios passed")
    sys.exit(1 if failures else 0)
