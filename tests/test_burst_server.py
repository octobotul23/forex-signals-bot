"""News burst, server side (docs/NEWS_BURST.md §8): the helpers in server/burst.py, then the REST and
WebSocket flow through FastAPI's TestClient with a fake connector.

    .venv\\Scripts\\python tests\\test_burst_server.py

Runs on a throw-away SQLite file with the demo price feed; never writes to data/ or logs/, never uses the
network, never starts the engine's loops (burst_tick is called by hand). Exit code 0 = every check passed.
"""
import ast
import faulthandler
import importlib.util
import io
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-burst-server-"))
DB_FILE = TMP / "platform.db"
DATA_DIR = ROOT / "data"

# settings are read when server modules are imported, so configure first (.env never overrides these)
os.environ.update(DATABASE_URL=f"sqlite:///{DB_FILE.as_posix()}", PRICE_FEED="demo", COOKIE_SECURE="false",
                  SECRET_KEY="burst-test-" + "k" * 40, DEMO_SECONDS_PER_BAR="0", ANTHROPIC_API_KEY="",
                  ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="")
# server.main attaches logs/server.log unless the root logger already has a handler for that file
_guard = logging.NullHandler()
_guard.baseFilename = str((ROOT / "logs" / "server.log").resolve())
_console = logging.StreamHandler()
_console.setLevel(logging.ERROR)
logging.getLogger().handlers[:0] = [_guard, _console]

# an "old" database: user_trades as the first release created it (no burst_id), with one row in it
_c = sqlite3.connect(DB_FILE)
_c.execute("""CREATE TABLE user_trades (id INTEGER NOT NULL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE, signal_id INTEGER, ticket INTEGER NOT NULL,
    symbol VARCHAR(20) NOT NULL, side VARCHAR(4) NOT NULL, lots FLOAT NOT NULL, entry_price FLOAT NOT NULL,
    sl FLOAT, tp FLOAT, opened_at DATETIME NOT NULL, closed_at DATETIME, exit_price FLOAT, pnl FLOAT,
    status VARCHAR(10) NOT NULL, demo BOOLEAN NOT NULL)""")
_c.execute("INSERT INTO user_trades (id, user_id, ticket, symbol, side, lots, entry_price, opened_at, status, demo) "
           "VALUES (1, 999, 5, 'EURUSD', 'buy', 0.1, 1.1, '2026-01-01 00:00:00', 'closed', 1)")
_c.commit()
_c.close()
_data_before = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import inspect, select, text  # noqa: E402

import server.main as main                 # noqa: E402
from server import burst as B              # noqa: E402
from server.db import Burst, CalendarEvent, SessionLocal, UserTrade, utcnow  # noqa: E402
from server.db import engine as db_engine  # noqa: E402
from server.engine import engine           # noqa: E402
from server.settings import DATABASE_URL, DEFAULT_USER_SETTINGS  # noqa: E402

H = {"x-fx": "1"}                          # the CSRF header every state-changing API call needs
# a plan as plan_payload builds it; the engine, the test endpoint and the welcome add the pre-news `context`
PLAN_KEYS = {"id", "symbol", "event_title", "currency", "event_time", "seconds_until", "test", "params", "kind"}
SENT_PLAN_KEYS = PLAN_KEYS | {"context"}
PARAM_KEYS = set(B.LIMITS) | {"risk_pct", "max_spread_pips", "sl_unit", "sl_money", "analysis", "max_hold_seconds"}
# the burst block a user saved under v1.2.0 (its defaults), and what v1.2.0's params_for made of it
V120_BURST = {"enabled": False, "trades": 10, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
              "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "currencies": []}
V120_PARAMS = {"trades": 10, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0, "tp_start_pips": 4.0,
               "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "risk_pct": 1.0, "max_spread_pips": 3.0}
# the v1.3.0 keys as a v1.2.0 block reads them: the stop in pips, no analysis, Y = X (lots, sl_money: defaults)
LEGACY_EXTRA = {"lots": 0.1, "sl_unit": "pips", "sl_money": 2.0, "analysis": False, "max_hold_seconds": 30}
FAILED: list[str] = []


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


# ── server/burst.py ────────────────────────────────────────────
def test_helpers():
    good = ("Federal Funds Rate", "Non-Farm Employment Change", "German Prelim CPI m/m", "")
    bad = ("FOMC Press Conference", "Fed Chair Powell Speaks", "FOMC Meeting Minutes", "BOJ Gov Ueda Testifies",
           "Bank Holiday", "10-y Bond Auction", "Federal Budget Balance", "ECB President Lagarde Speech",
           "Parliamentary Elections", "G20 Summit", "Senate Hearing", "MPC Conference")
    check(all(B.event_ok(t) for t in good) and not any(B.event_ok(t) for t in bad),
          "event_ok: releases pass, speeches/minutes/auctions/holidays... do not")

    t1, t2 = datetime(2026, 10, 28, 12, 30), datetime(2026, 10, 28, 14, 0)
    evs = [{"id": "h", "time": t2, "currency": "CAD", "title": "Overnight Rate", "impact": "High"},
           {"id": "a", "time": t1, "currency": "USD", "title": "Non-Farm Employment Change", "impact": "High"},
           {"id": "b", "time": t1, "currency": "usd", "title": "Unemployment Rate", "impact": "High"},
           {"id": "c", "time": t1, "currency": "EUR", "title": "Main Refinancing Rate", "impact": "High"},
           {"id": "d", "time": t1, "currency": "GBP", "title": "GDP m/m", "impact": "Medium"},
           {"id": "e", "time": t1, "currency": "JPY", "title": "BOJ Press Conference", "impact": "High"},
           {"id": "f", "time": t1, "currency": "CNY", "title": "GDP q/y", "impact": "High"},
           {"id": "g", "time": t1, "currency": "NZD", "title": "Official Cash Rate", "impact": "High"},
           {"id": "i", "time": t1, "currency": "USD", "title": "Unemployment Rate", "impact": "High"}]
    app = {"symbols": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"]}     # no NZDUSD, XAUUSD, GBPJPY
    cands = B.candidates(evs, app)
    check([(c["symbol"], c["time"], c["event_id"], c["currency"]) for c in cands]
          == [("EURUSD", t1, "a", "USD"), ("GBPUSD", t1, "a", "USD"), ("USDJPY", t1, "a", "USD"),
              ("USDCAD", t2, "h", "CAD")],
          "candidates: High only, title filter, every pair of the currency's list in app symbols, sorted, one per "
          "(pair, time)", cands)
    check(cands and cands[0]["title"] == "Non-Farm Employment Change + Unemployment Rate + Main Refinancing Rate"
          and cands[0]["currencies"] == ["USD", "EUR"], "candidates: collapsed events join their titles", cands[:1])
    check([c["ranks"] for c in cands] == [{"USD": 0, "EUR": 0}, {"USD": 2}, {"USD": 3}, {"CAD": 0}],
          "candidates: each pair's place in the list of every currency behind it", [c["ranks"] for c in cands])
    check(B.candidates([SimpleNamespace(**evs[0])], app)[0]["event_id"] == "h", "candidates accepts CalendarEvent rows")
    check(all(isinstance(v, list) and v for v in B.BURST_PAIRS.values()) and B.BURST_PAIRS["USD"][0] == "EURUSD"
          and B.BURST_PAIRS["GBP"][0] == "GBPUSD" and B.BURST_PAIRS["JPY"][0] == "USDJPY"
          and B.BURST_SYMBOLS == ["EURUSD", "XAUUSD", "GBPUSD", "USDJPY", "GBPJPY", "AUDUSD", "NZDUSD", "USDCAD",
                                  "USDCHF"], "BURST_PAIRS: ordered lists, each currency's own pair first", B.BURST_SYMBOLS)

    eu, ca = cands[0], cands[-1]
    us = lambda symbols=(), **b: {"symbols": list(symbols), "burst": b}    # noqa: E731
    syms = app["symbols"]
    check(B.allowed_for(us(), eu, syms) and B.allowed_for(us(currencies=["EUR"]), eu, syms)
          and not B.allowed_for(us(currencies=["EUR"]), ca, syms) and not B.allowed_for(us(["GBPUSD"]), eu, syms)
          and B.allowed_for(us(["USDCAD"], currencies=["CAD", "JPY"]), ca, syms) and not B.allowed_for(us(), eu, ["GBPUSD"]),
          "allowed_for: app symbols, the user's pairs, the user's currencies (any of a collapsed event)")

    # ── which pair a release is traded on (plan_for / pick_pairs) ──
    ev3 = lambda i, cur, title, t=t1: {"id": i, "time": t, "currency": cur, "title": title, "impact": "High"}  # noqa: E731
    rel = [ev3("u", "USD", "CPI m/m"), ev3("e", "EUR", "Main Refinancing Rate"), ev3("g", "GBP", "GDP m/m"),
           ev3("j", "JPY", "BOJ Policy Rate"), ev3("c", "CAD", "Overnight Rate", t2)]
    all8 = app["symbols"] + ["GBPJPY", "XAUUSD"]
    c8 = B.candidates(rel, {"symbols": all8})
    pu = lambda own=(), **b: {"symbols": list(own), "burst": b}     # noqa: E731   the user's pairs, burst settings
    plan = lambda u, cs=c8, syms=all8, **kw: [(c["symbol"], c["currency"], c["title"])  # noqa: E731
                                              for c in B.plan_for(u, cs, syms, **kw)]
    v120 = [("EURUSD", "USD", "CPI m/m + Main Refinancing Rate"), ("GBPUSD", "GBP", "GDP m/m"),
            ("USDJPY", "JPY", "BOJ Policy Rate"), ("USDCAD", "CAD", "Overnight Rate")]
    check(plan(pu()) == v120, "plan_for, no pairs picked: each release on its currency's own pair, titles as in v1.2.0",
          plan(pu()))
    check(plan(pu()) == plan(pu(), B.candidates(rel, app), app["symbols"]) == plan({"symbols": [], "burst": {}}),
          "... the same with v1.2.0's pairs only, or no burst `symbols` key at all")
    check(plan(pu(["GBPUSD"])) == [("GBPUSD", "GBP", "GDP m/m")] and plan(pu(["USDCHF", "AUDUSD"])) == [],
          "... a user's own pairs without the currency's own pair: that release is not traded (as in v1.2.0)",
          plan(pu(["GBPUSD"])))
    check(plan(pu(symbols=["xauusd"])) == [("XAUUSD", "USD", "CPI m/m")],
          "picks [XAUUSD]: the USD release on gold, nothing else (EUR, GBP, JPY, CAD have no gold)",
          plan(pu(symbols=["xauusd"])))
    check(plan(pu(symbols=["USDJPY", "GBPUSD", "XAUUSD"])) == [("XAUUSD", "USD", "CPI m/m"), ("GBPUSD", "GBP", "GDP m/m"),
                                                                ("USDJPY", "JPY", "BOJ Policy Rate")],
          "picks: the first picked pair in the currency's list order (USD: XAUUSD before GBPUSD), one run a release",
          plan(pu(symbols=["USDJPY", "GBPUSD", "XAUUSD"])))
    check(plan(pu(symbols=["GBPUSD"])) == [("GBPUSD", "USD", "CPI m/m + GDP m/m")],
          "two releases picking one pair in one minute: one burst, named after both",
          plan(pu(symbols=["GBPUSD"])))
    check(plan(pu(symbols=["GBPJPY"], currencies=["JPY"])) == [("GBPJPY", "GBP", "GDP m/m + BOJ Policy Rate")],
          "picks [GBPJPY], currencies [JPY]: the JPY release on GBPJPY (named, as in v1.2.0, with every release that "
          "chose it)", plan(pu(symbols=["GBPJPY"], currencies=["JPY"])))
    check(plan(pu(currencies=["EUR"])) == [v120[0]] and plan(pu(["EURUSD"], symbols=["XAUUSD"])) == [],
          "the user's currencies and own pairs still apply", plan(pu(currencies=["EUR"])))
    check(plan(pu(), have={("EURUSD", t1)}) == v120[1:] and plan(pu(), have={("XAUUSD", t1)}) == v120,
          "a release with a burst on its own pair (a cancelled one too) gets no other; one on another pair is not it")
    check(plan(pu(symbols=["XAUUSD"]), have={("EURUSD", t1)}) == []
          and plan(pu(symbols=["XAUUSD", "USDJPY"]), have={("USDJPY", t1)}) == [],
          "picks: a release already traded on its own pair or a picked one gets no second run (one run a release)")
    check(plan(pu(symbols=["XAUUSD", "GBPUSD"]), taken={"XAUUSD"})[:1] == [("GBPUSD", "USD", "CPI m/m + GDP m/m")]
          and plan(pu(), taken={"EURUSD"}) == [("XAUUSD", "USD", "CPI m/m")] + v120[1:],
          "taken (another run's pairs): the next picked pair; no picks and the currency's own pair taken: the next pair "
          "of its list that none took (USD on XAUUSD; EUR, whose list is EURUSD alone: none)",
          plan(pu(), taken={"EURUSD"}))
    check(plan(pu(), taken={"EURUSD", "XAUUSD"})[:1] == [("GBPUSD", "USD", "CPI m/m + GDP m/m")]
          and plan(pu(), taken={"EURUSD", "XAUUSD", "GBPUSD", "USDJPY", "GBPJPY"}) == [v120[3]]
          and plan(pu(), taken={"GBPUSD", "USDJPY"}) == [v120[0], ("GBPJPY", "GBP", "GDP m/m + BOJ Policy Rate"),
                                                         v120[3]],
          "... the one after when that is taken too (GBPUSD: with the GBP release, one burst); every pair taken: none; "
          "GBP and JPY both on GBPJPY, the pair their lists share: one burst, named after both",
          (plan(pu(), taken={"EURUSD", "XAUUSD"}), plan(pu(), taken={"GBPUSD", "USDJPY"})))
    check(plan(pu(), B.candidates(rel, app), app["symbols"], taken={"EURUSD"})[:1] == [("GBPUSD", "USD",
                                                                                         "CPI m/m + GDP m/m")]
          and plan(pu(["EURUSD", "USDJPY"]), taken={"EURUSD"})[:1] == [("USDJPY", "USD", "CPI m/m + BOJ Policy Rate")]
          and plan(pu(["GBPUSD"]), taken={"EURUSD"}) == [("GBPUSD", "GBP", "GDP m/m")]
          and plan(pu(currencies=["EUR"]), taken={"EURUSD"}) == [],
          "... only pairs the user and the engine trade (no gold: USD on GBPUSD; the user's own pairs); a user who does "
          "not trade the own pair has no release of it traded, as before; the user's currencies still apply",
          plan(pu(["EURUSD", "USDJPY"]), taken={"EURUSD"}))
    check(plan(pu(), have={("XAUUSD", t1, "USD")}, taken={"EURUSD"}) == v120[1:]
          and plan(pu(), have={("XAUUSD", t1)}, taken={"EURUSD"}) == v120[1:]
          and plan(pu(), have={("GBPUSD", t1, "GBP")}, taken={"EURUSD"})[:1] == [("XAUUSD", "USD", "CPI m/m")],
          "... planned once: a release with a burst on the pair it falls back to gets no other; a burst on another pair "
          "of its list (GBP's on GBPUSD) is not it", plan(pu(), have={("GBPUSD", t1, "GBP")}, taken={"EURUSD"}))
    check(plan({"symbols": [], "burst": {"symbols": "XAUUSD"}}) == v120,
          "a corrupt stored `symbols`: each currency's own pair")
    check(B.pick_pairs([{"symbol": s, "rank": r} for r, s in enumerate(B.BURST_PAIRS["USD"])], ["USDJPY", "XAUUSD"],
                       taken=("XAUUSD",), n=4) == [{"symbol": "USDJPY", "rank": 3}]
          and B.pick_pairs([{"symbol": "GBPUSD", "rank": 2}], []) == []
          and B.pick_pairs([{"symbol": "GBPUSD", "rank": 2}], [], taken=("EURUSD",)) == [],
          "pick_pairs: picks in list order minus taken, up to n; no picks: rank 0 only (none among the pairs: none, "
          "whatever is taken)")
    usd = [{"symbol": s, "rank": r} for r, s in enumerate(B.BURST_PAIRS["USD"])]
    check(B.pick_pairs(usd, ["GBPUSD", "XAUUSD"]) == [{"symbol": "XAUUSD", "rank": 1}]
          and B.pick_pairs(usd, ["GBPUSD", "XAUUSD"], n=2) == [{"symbol": "XAUUSD", "rank": 1},
                                                               {"symbol": "GBPUSD", "rank": 2}],
          "pick_pairs: one pair by default (the first picked in list order), n of them when asked",
          B.pick_pairs(usd, ["GBPUSD", "XAUUSD"]))
    check(B.pick_pairs(usd, [], taken=("EURUSD",)) == [usd[1]] and B.pick_pairs(usd, [], ("EURUSD", "XAUUSD")) == [usd[2]]
          and B.pick_pairs(usd, [], ("EURUSD",), n=4) == usd[1:] and B.pick_pairs(usd, [], ("XAUUSD",)) == [usd[0]]
          and B.pick_pairs(usd, [], tuple(B.BURST_PAIRS["USD"])) == []
          and B.pick_pairs(usd, ["USDJPY"], ("EURUSD",)) == [usd[3]] and B.pick_pairs(usd, ["EURUSD"], ("EURUSD",)) == [],
          "pick_pairs, no picks and the own pair taken: the next ones of the list none took, in its order (another taken "
          "pair: the own pair still); picks: the picks alone, as before", B.pick_pairs(usd, [], taken=("EURUSD",)))
    check(B.pick_pairs(usd[1:], [], taken=("XAUUSD",)) == [] and B.pick_pairs(usd[1:], [], ("XAUUSD", "GBPUSD")) == []
          and B.pick_pairs(usd[1:], [], ("XAUUSD",), n=3) == [],
          "pick_pairs, no picks and the own pair not among the pairs (the user does not trade it): none, also when "
          "another run took one of the others (no fallback: only a taken own pair moves the burst)",
          B.pick_pairs(usd[1:], [], taken=("XAUUSD",)))
    check(plan(pu(symbols=["EURUSD", "XAUUSD"]), have={("EURUSD", t1)}, taken={"EURUSD"})
          == [("XAUUSD", "USD", "CPI m/m")],
          "a run of another kind took the currency's own pair (its row in `have`): the classic burst takes the next "
          "picked pair", plan(pu(symbols=["EURUSD", "XAUUSD"]), have={("EURUSD", t1)}, taken={"EURUSD"}))
    check(plan(pu(symbols=["EURUSD", "XAUUSD"]), have={("EURUSD", t1, "USD")}, taken={"EURUSD"})
          == [("XAUUSD", "USD", "CPI m/m")],
          "... also when that run was planned for the same release (its currency): it is not the classic burst's")
    gbp_first = B.candidates([ev3("g", "GBP", "GDP m/m"), ev3("u", "USD", "CPI m/m")], {"symbols": all8})
    check(plan(pu(symbols=["XAUUSD", "GBPUSD"]), gbp_first)
          == [("GBPUSD", "GBP", "GDP m/m"), ("XAUUSD", "USD", "CPI m/m")],
          "the GBP release listed first in that minute: USD still takes its first pick in list order (XAUUSD), GBP "
          "its own GBPUSD", plan(pu(symbols=["XAUUSD", "GBPUSD"]), gbp_first))
    check(plan(pu(symbols=["XAUUSD", "GBPUSD"], currencies=["USD"])) == [("XAUUSD", "USD", "CPI m/m")],
          "currencies [USD], picks [XAUUSD, GBPUSD]: the GBP release in that minute is not traded on GBPUSD",
          plan(pu(symbols=["XAUUSD", "GBPUSD"], currencies=["USD"])))
    # the engine's `have` carries each burst's currency: a release keeps its one run when the picks change
    rows = lambda u, have=frozenset(), cs=c8: {(c["symbol"], c["time"], c["currency"])  # noqa: E731
                                               for c in B.plan_for(u, cs, all8, have)}
    usd1 = B.candidates([ev3("u", "USD", "CPI m/m")], {"symbols": all8})
    gold = rows(pu(symbols=["XAUUSD"]), cs=usd1)
    later = [rows(pu(symbols=s), gold, usd1) for s in (["GBPUSD"], [], ["EURUSD", "USDJPY"])]
    check(gold == {("XAUUSD", t1, "USD")} and later == [set()] * 3,
          "a USD release planned on XAUUSD: picks changed to [GBPUSD], [] or others, no second run for it",
          (gold, later))
    gold = rows(pu(symbols=["XAUUSD"]))
    check(gold == {("XAUUSD", t1, "USD")} and plan(pu(symbols=["GBPUSD"]), have=gold) == [("GBPUSD", "GBP", "GDP m/m")]
          and plan(pu(), have=gold) == [("EURUSD", "EUR", "Main Refinancing Rate")] + v120[1:]
          and plan(pu(symbols=["XAUUSD"]), have=gold | rows(pu(), gold)) == [],
          "... the other releases of that minute are traded as the new picks say, named after their own releases only",
          (plan(pu(symbols=["GBPUSD"]), have=gold), plan(pu(), have=gold)))

    # ── which releases keep a planned burst (stands; `live` from Engine.live_releases) ──
    own = {cur: pairs[0] for cur, pairs in B.BURST_PAIRS.items()}
    curs = list(B.BURST_PAIRS)
    diff = [(cur, live) for cur in curs for n in range(1 << len(curs))
            for live in [{c for i, c in enumerate(curs) if n >> i & 1}]
            if B.stands(own[cur], cur, live, []) != (own[cur] in {own[c] for c in live})]
    check(not diff, "stands, no picks: v1.2.0's rule (a burst stands while a release whose own pair it is stays)",
          diff[:3])
    check(not B.stands("GBPUSD", "GBP", {"USD"}, []) and not B.stands("USDJPY", "JPY", {"USD", "EUR"}, [])
          and not B.stands("GBPJPY", "GBP", {"JPY"}, []) and B.stands("GBPUSD", "GBP", {"GBP", "USD"}, []),
          "stands: a USD release that merely lists GBPUSD / USDJPY keeps neither once their own release left")
    check(B.stands("XAUUSD", "USD", {"USD"}, []) and not B.stands("XAUUSD", "USD", {"GBP", "EUR"}, ["XAUUSD"])
          and B.stands("GBPJPY", "GBP", {"JPY"}, ["GBPJPY"]) and B.stands("GBPUSD", "GBP", {"USD"}, ["GBPUSD"])
          and not B.stands("XAUUSD", "USD", set(), ["XAUUSD"]),
          "stands with picks: its own currency's release (picks changed since), or one whose list has a picked pair")
    check(B.burst_picks({"symbols": ["xauusd", "EURGBP"]}) == ["XAUUSD"] and B.burst_picks({"symbols": "XAUUSD"}) == []
          and B.burst_picks({}) == [], "burst_picks: clamped; a corrupt value or none: no picks")

    got = B.clamp_burst({"enabled": 1, "trades": 99, "lead_seconds": 1, "offset_pips": "0.2", "sl_pips": 500,
                         "tp_start_pips": 4, "tp_step_pips": -1, "hold_seconds": 9999.9, "expire_seconds": 5,
                         "currencies": ["usd", "XXX", "USD", "jpy"], "bogus": 3}, max_trades=25)
    check(got == {"enabled": True, "trades": 25, "lead_seconds": 10, "offset_pips": 1.0, "sl_pips": 200.0,
                  "tp_start_pips": 4.0, "tp_step_pips": 0.0, "hold_seconds": 600, "expire_seconds": 10,
                  "currencies": ["USD", "JPY"]}, "clamp_burst: every range, the admin cap, currencies, unknown keys", got)
    check(B.clamp_burst({"trades": 99}, 100)["trades"] == 40 and B.clamp_burst({"trades": 0}, 40)["trades"] == 1
          and isinstance(B.clamp_burst({"lead_seconds": 30.0}, 40)["lead_seconds"], int), "clamp_burst: trades 1..40, ints stay ints")
    for bad_body in ({"trades": "many"}, {"offset_pips": None}, {"sl_pips": float("nan")}, {"hold_seconds": float("inf")},
                     {"currencies": "USD"}, {"symbols": "XAUUSD"}):
        try:
            B.clamp_burst(bad_body, 40)
            check(False, f"clamp_burst rejects {bad_body}")
        except ValueError:
            pass
    got = B.clamp_burst({"symbols": ["xauusd", "EURGBP", "XAUUSD", "gbpjpy", 5, "EURUSD"]}, 40)
    check(got == {"symbols": ["XAUUSD", "GBPJPY", "EURUSD"]} and B.clamp_burst({"symbols": []}, 40) == {"symbols": []},
          "clamp_burst symbols: pairs of BURST_PAIRS only, upper case, once, in the user's order", got)

    p = B.params_for({**DEFAULT_USER_SETTINGS, "risk_per_trade_pct": 0.5, "max_spread_pips": 2.0}, 5, lead_seconds=15)
    check(set(p) == PARAM_KEYS and p["trades"] == 5 and p["lead_seconds"] == 15 and p["risk_pct"] == 0.5
          and p["max_spread_pips"] == 2.0 and p["offset_pips"] == 7.0, "params_for: snapshot, trades capped, override", p)
    check(set(B.params_for({**DEFAULT_USER_SETTINGS, "burst": {"symbols": ["XAUUSD"]}}, 40)) == PARAM_KEYS,
          "params_for: the burst's pairs stay on the server (the connector gets the LIMITS and v1.3.0 keys only)")
    corrupt = {**DEFAULT_USER_SETTINGS, "burst": {"trades": "x"}}
    check(B.params_for(corrupt, 40)["trades"] == 10, "params_for falls back to defaults on a corrupt stored value")

    check([B.version_ok(v) for v in ("1.1.0", "1.2", "1.10.0", "2.0.0", "1.0.0", "1.0.9", "?", None, "")]
          == [True, True, True, True, False, False, False, False, False], "version_ok: >= 1.1.0")
    row = SimpleNamespace(id=12, symbol="EURUSD", event_title="Federal Funds Rate", currency="USD", test=False,
                          event_time=datetime(2026, 10, 28, 18, 0), params={"trades": 10})
    plan = B.plan_payload(row, row.event_time - timedelta(seconds=734.2))
    check(set(plan) == PLAN_KEYS and plan["event_time"] == "2026-10-28T18:00:00Z" and plan["seconds_until"] == 734.2
          and plan["params"] == {"trades": 10} and plan["test"] is False and plan["kind"] == "burst", "plan_payload", plan)
    check(B.FINAL == {"closed", "expired", "skipped", "cancelled", "error"}, "FINAL statuses")


def test_v130_helpers():
    """v1.3.0's burst keys (stop unit, lots, hold X/Y, analysis), read the v1.2.0 way for a block saved without them."""
    from server.db import User
    legacy_user = {**DEFAULT_USER_SETTINGS, "burst": dict(V120_BURST)}
    p = B.params_for(legacy_user, 40)
    check({k: p[k] for k in V120_PARAMS} == V120_PARAMS and {k: p[k] for k in p if k not in V120_PARAMS} == LEGACY_EXTRA
          and B.needs_version(p, "burst", "EURUSD") == B.MIN_CONNECTOR,
          "a v1.2.0 user: v1.2.0's params, the new keys read the v1.2.0 way, any connector from 1.1.0 runs it", p)
    check(B.params_for({k: v for k, v in legacy_user.items() if k != "burst"}, 40) == p
          and B.params_for({**legacy_user, "burst": "junk"}, 40) == p,
          "no stored burst block (or a junk one): the same v1.2.0 params")
    new = B.params_for(DEFAULT_USER_SETTINGS, 40)
    check({k: new[k] for k in LEGACY_EXTRA} == {"lots": 0.1, "sl_unit": "pct", "sl_money": 2.0, "analysis": True,
                                                "max_hold_seconds": 120}
          and {k: new[k] for k in V120_PARAMS} == V120_PARAMS and B.needs_version(new) == B.V130 == (1, 3, 0),
          "a new user: the stop as 2 % of balance, 0.10 lots, hold 30..120 s, analysis on; needs connector 1.3.0", new)
    corrupt = B.params_for({**DEFAULT_USER_SETTINGS, "burst": {**DEFAULT_USER_SETTINGS["burst"], "analysis": "yes"}}, 40)
    check(corrupt == p, "a corrupt stored value: v1.2.0's defaults, as before (never a guess at the new keys)", corrupt)
    over = B.params_for({**DEFAULT_USER_SETTINGS, "burst": {**DEFAULT_USER_SETTINGS["burst"], "max_hold_seconds": 40}},
                        40, hold_seconds=90)
    check(over["hold_seconds"] == 90 and over["max_hold_seconds"] == 90, "params_for: Y raised to an override's X", over)

    # db.User.merged_settings: the gap is filled at read time, nothing is migrated
    old = User(settings={**{k: v for k, v in DEFAULT_USER_SETTINGS.items() if k not in ("burst", "short_term")},
                         "burst": {**V120_BURST, "enabled": True}})
    s = old.merged_settings()
    check(s["burst"] == {**V120_BURST, "enabled": True, "symbols": [], "sl_unit": "pips", "sl_money": 2.0, "lots": 0.1,
                         "analysis": False} and "max_hold_seconds" not in s["short_term"],
          "merged_settings of a v1.2.0 user: stop in pips, no analysis, no Y (burst nor short-term)", s["burst"])
    for stored in ({k: v for k, v in DEFAULT_USER_SETTINGS.items() if k != "burst"}, None):
        s = User(settings=stored).merged_settings()["burst"]
        check(s == B.legacy_burst(None) and (s["sl_unit"], s["analysis"]) == ("pips", False)
              and "max_hold_seconds" not in s,
              f"merged_settings with no stored burst block ({'other keys' if stored else 'nothing'} stored: an "
              "account from before the burst): read as v1.2.0, never as a new user", s)
    s = User(settings=dict(DEFAULT_USER_SETTINGS)).merged_settings()
    check(s["burst"] == DEFAULT_USER_SETTINGS["burst"] and s["short_term"]["max_hold_seconds"] == 120,
          "merged_settings of a new user: the v1.3.0 defaults", (s["burst"], s["short_term"]))
    check(B.legacy_burst({"sl_unit": "usd"})["sl_money"] == 100.0 and B.legacy_burst(None)["sl_unit"] == "pips",
          "legacy_burst: a missing sl_money is its unit's default")

    # clamp_burst: units, money, lots, Y, analysis
    cb = lambda body, cur=None: B.clamp_burst(body, 40, cur)    # noqa: E731
    pct, usd, pips = ({**DEFAULT_USER_SETTINGS["burst"], "sl_unit": u, "sl_money": m}
                      for u, m in (("pct", 3.0), ("usd", 250.0), ("pips", 2.0)))
    check(cb({"sl_unit": "usd"}, pct) == {"sl_unit": "usd", "sl_money": 100.0}
          and cb({"sl_unit": "pct"}, usd) == {"sl_unit": "pct", "sl_money": 2.0}
          and cb({"sl_unit": "usd"}, B.legacy_burst(V120_BURST)) == {"sl_unit": "usd", "sl_money": 100.0}
          and cb({"sl_unit": "pips"}, usd) == {"sl_unit": "pips", "sl_money": 2.0}
          and cb({"sl_unit": "pct"}, {"sl_unit": "lots"}) == {"sl_unit": "pct", "sl_money": 2.0},
          "a unit changed without sl_money resets it to the new unit's default (pips: the pct one)")
    check(cb({"sl_unit": "usd", "sl_money": 40}, pct) == {"sl_unit": "usd", "sl_money": 40.0}
          and cb({"sl_unit": "pct"}, pct) == {"sl_unit": "pct"} and cb({"sl_unit": "usd"}) == {"sl_unit": "usd"},
          "a new sl_money comes with it; the same unit, or no cur: nothing reset")
    check(cb({"sl_money": 50}, pct) == {"sl_money": 10.0} and cb({"sl_money": 0.5}, usd) == {"sl_money": 1.0}
          and cb({"sl_money": 1e9}, usd) == {"sl_money": 100_000.0} and cb({"sl_money": 50}, pips) == {"sl_money": 10.0}
          and cb({"sl_money": 0.01}) == {"sl_money": 0.1} and cb({"sl_unit": "usd", "sl_money": 5000}, pct)["sl_money"]
          == 5000.0, "sl_money: its unit's range (body's unit, else cur's; pips and none: the pct range)")
    check(cb({"lots": 0.001}) == {"lots": 0.01} and cb({"lots": 500}) == {"lots": 100.0} and cb({"lots": "0.25"})
          == {"lots": 0.25}, "lots: 0.01..100")
    check(cb({"max_hold_seconds": 10, "hold_seconds": 30}) == {"hold_seconds": 30, "max_hold_seconds": 30}
          and cb({"max_hold_seconds": 9999}, pct) == {"max_hold_seconds": 1800}
          and cb({"max_hold_seconds": 10}, {"hold_seconds": 45}) == {"max_hold_seconds": 45}
          and cb({"max_hold_seconds": 3}, {"hold_seconds": "junk"}) == {"max_hold_seconds": 5}
          and cb({"max_hold_seconds": 200.7}, pct) == {"max_hold_seconds": 200},
          "max_hold_seconds: X..1800, under X raised to X (body's, else cur's), never swapped")
    check(cb({"analysis": False}) == {"analysis": False} and cb({"analysis": True}) == {"analysis": True},
          "analysis: a bool")
    for body, code in (({"sl_unit": "lots"}, "invalid_unit"), ({"sl_unit": None}, "invalid_unit"),
                       ({"analysis": "false"}, "invalid_analysis"), ({"analysis": 1}, "invalid_analysis"),
                       ({"analysis": None}, "invalid_analysis"), ({"sl_money": "x"}, "invalid_number"),
                       ({"sl_money": float("nan")}, "invalid_number"), ({"lots": None}, "invalid_number"),
                       ({"max_hold_seconds": "x"}, "invalid_number"), ({"max_hold_seconds": float("inf")}, "invalid_number"),
                       ({"max_hold_seconds": float("nan")}, "invalid_number")):
        try:
            got = cb(body, pct)
            check(False, f"clamp_burst {body} -> {code}", got)
        except ValueError as e:
            check(str(e) == code, f"clamp_burst {body} -> {code}", e)
    check(B.raise_max_hold({"hold_seconds": 60, "max_hold_seconds": 30}) == {"hold_seconds": 60, "max_hold_seconds": 60}
          and B.raise_max_hold({"hold_seconds": 60}) == {"hold_seconds": 60}
          and B.raise_max_hold({"hold_seconds": 60, "max_hold_seconds": "x"}) == {"hold_seconds": 60},
          "raise_max_hold: Y >= X; no Y stays none; an unreadable one is dropped (follows X)")
    got = [B.raise_max_hold({"hold_seconds": x, "max_hold_seconds": 5000})["max_hold_seconds"] for x in ("x", None, 60)]
    check(got == [B.MAX_HOLD] * 3 and B.MAX_HOLD == 1800, "raise_max_hold: Y over MAX_HOLD is capped, X read or not",
          got)
    # a Y sent as null: the user's way back to "Y follows X" (v1.2.0's hold); the caller merges it and drops the key
    check(cb({"max_hold_seconds": None}) == {"max_hold_seconds": None} == cb({"max_hold_seconds": None}, pct)
          and cb({"max_hold_seconds": None, "hold_seconds": 45}, pct) == {"hold_seconds": 45, "max_hold_seconds": None},
          "clamp_burst: a Y sent as null comes back as null (removed), X as sent", cb({"max_hold_seconds": None}, pct))
    stored = {**DEFAULT_USER_SETTINGS["burst"], "hold_seconds": 40, "max_hold_seconds": 200}
    merged = B.raise_max_hold({**stored, **cb({"max_hold_seconds": None}, stored)})
    check("max_hold_seconds" not in merged and merged == {k: v for k, v in stored.items() if k != "max_hold_seconds"}
          and B.raise_max_hold({**stored, **cb({"hold_seconds": 60}, stored)})["max_hold_seconds"] == 200,
          "raise_max_hold drops a null Y (nothing else changed); a body without Y keeps the stored one", merged)
    for burst in ({**stored, "max_hold_seconds": None}, merged):
        got = B.params_for({**DEFAULT_USER_SETTINGS, "burst": burst}, 40)
        check(got["max_hold_seconds"] == got["hold_seconds"] == 40,
              f"params_for: a Y {'stored as null' if 'max_hold_seconds' in burst else 'removed'} follows X", got)
    got = B.params_for({**DEFAULT_USER_SETTINGS, "burst": merged}, 40, hold_seconds=90)
    check(got["max_hold_seconds"] == got["hold_seconds"] == 90, "... an override's X too", got)
    legacy = B.params_for({**DEFAULT_USER_SETTINGS, "burst": {**merged, "sl_unit": "pips", "analysis": False}}, 40)
    check(B.needs_version(legacy, "burst", "EURUSD") == B.MIN_CONNECTOR,
          "a Y removed (with the stop in pips, no analysis): a plan v1.2.0 runs as it is", legacy)

    # needs_version: what a connector before 1.3.0 cannot run as planned
    base = {**V120_PARAMS, **LEGACY_EXTRA}
    check(B.needs_version(V120_PARAMS) == B.needs_version(base) == B.needs_version(base, None, "USDJPY")
          == B.needs_version({**base, "max_hold_seconds": 30.0}) == B.needs_version(None) == B.MIN_CONNECTOR,
          "needs_version: a v1.2.0 plan (with or without the new keys at their v1.2.0 values): MIN_CONNECTOR")
    newer = {"sl_unit pct": ({**base, "sl_unit": "pct"}, "burst", None),
             "sl_unit usd": ({**base, "sl_unit": "usd"}, "burst", None),
             "Y > X": ({**base, "max_hold_seconds": 31}, "burst", None),
             "Y junk": ({**base, "max_hold_seconds": "x"}, "burst", None),
             "analysis on": ({**base, "analysis": True}, "burst", None),
             "analysis junk": ({**base, "analysis": "false"}, "burst", None),
             "analysis None": ({**base, "analysis": None}, "burst", None),
             "analysis 0": ({**base, "analysis": 0}, "burst", None),
             "Y nan": ({**base, "max_hold_seconds": float("nan")}, "burst", None),
             "X nan": ({**base, "hold_seconds": float("nan"), "max_hold_seconds": 30}, "burst", None),
             "kind rapid": (base, "rapid", None), "XAUUSD": (base, "burst", "XAUUSD"),
             "GBPJPY": (base, "burst", "GBPJPY"), "XAUUSD.m": (base, "burst", "XAUUSD.m")}
    got = {k: B.needs_version(*a) for k, a in newer.items()}
    check(all(v == B.V130 for v in got.values()), "needs_version: each v1.3.0 key, Rapid and the new pairs need 1.3.0",
          got)
    check([B.new_pair(s) for s in ("XAUUSD", "xauusd.m", "GBPJPY", "EURUSD", "USDJPY", "", None)]
          == [True, True, True, False, False, False, False]
          and B.pairs_for(["EURUSD", "XAUUSD", "GBPJPY", "USDJPY"], "1.2.0") == ["EURUSD", "USDJPY"]
          and B.pairs_for(["EURUSD", "XAUUSD"], None) == ["EURUSD"]
          and B.pairs_for(["EURUSD", "XAUUSD", "GBPJPY"], "1.3.0") == ["EURUSD", "XAUUSD", "GBPJPY"],
          "new_pair / pairs_for: gold and the scaled pairs only for connectors from 1.3.0")
    # gold under any name: one SYMBOL_SCALE does not know (gold against another currency, a broker prefix) is not
    # scaled, but its pip was 0.0001 before 1.3.0 all the same
    check([B.new_pair(s) for s in ("XAUEUR", "mXAUUSD", "xaueur.pro")] == [True, True, True]
          and B.pairs_for(["EURUSD", "XAUEUR"], "1.2.0") == ["EURUSD"]
          and B.pairs_for(["mXAUUSD", "GBPUSD"], "1.2.0") == ["GBPUSD"],
          "new_pair: any gold symbol, scaled or not, is new")
    check(B.run_seconds(V120_PARAMS) == 120 and B.run_seconds(new) == 210 and B.run_seconds({}) == 120
          and B.run_seconds({"expire_seconds": "x", "hold_seconds": None, "max_hold_seconds": float("inf")}) == 120
          and B.run_seconds({**V120_PARAMS, "max_hold_seconds": 10}) == 120,
          "run_seconds: expire + max(X, Y); unreadable values are their defaults")
    rapid = SimpleNamespace(id=3, symbol="EURUSD", event_title="CPI", currency="USD", test=False, kind="rapid",
                            event_time=datetime(2026, 10, 28, 18, 0), params={})
    ctx = {"expect": 0.4, "ai": None, "title": "CPI"}
    got = B.plan_payload(rapid, rapid.event_time, ctx)
    check(got["kind"] == "rapid" and got["context"] == ctx and set(got) == SENT_PLAN_KEYS
          and "context" not in B.plan_payload(rapid, rapid.event_time, None), "plan_payload: kind, and context when given",
          got)


def test_rapid_helpers():
    """Rapid's news runs (v1.3.0, bot/rapid.py): Burst rows of kind "rapid", planned before the classic burst over a
    release's first max_runs pairs; the classic burst then takes a pair Rapid left, never one Rapid holds. Rapid off:
    the classic burst's plans exactly as before."""
    from bot.rapid import DEFAULTS as RAPID_DEFAULTS
    from bot.scalp import DEFAULTS as SHORT_DEFAULTS
    from server.db import User

    t1, t2 = datetime(2026, 10, 28, 12, 30), datetime(2026, 10, 28, 14, 0)
    ev3 = lambda i, cur, title, t=t1: {"id": i, "time": t, "currency": cur, "title": title, "impact": "High"}  # noqa: E731
    rel = [ev3("u", "USD", "CPI m/m"), ev3("e", "EUR", "Main Refinancing Rate"), ev3("g", "GBP", "GDP m/m"),
           ev3("j", "JPY", "BOJ Policy Rate"), ev3("c", "CAD", "Overnight Rate", t2)]
    six = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"]      # v1.2.0's pairs
    all8 = six + ["GBPJPY", "XAUUSD"]
    c8, c6 = B.candidates(rel, {"symbols": all8}), B.candidates(rel, {"symbols": six})
    rp = lambda **r: {**DEFAULT_USER_SETTINGS["rapid"], "enabled": True, **r}     # noqa: E731   Rapid on

    def us(own=(), rapid=None, **burst):        # the user's pairs, the rapid block (None: none stored), the burst's
        return {"symbols": list(own), "burst": burst, **({} if rapid is None else {"rapid": rapid})}

    def runs(u, cs=c8, syms=all8, have=frozenset(), classic=True):
        return [(c["symbol"], c["currency"], c["title"], kind) for c, kind in B.plan_runs(u, cs, syms, have, classic)]

    check(DEFAULT_USER_SETTINGS["rapid"] == {**RAPID_DEFAULTS, "max_hold_seconds": 120}
          and DEFAULT_USER_SETTINGS["rapid"]["enabled"] is False, "a new user's rapid block: bot/rapid.py's, off, Y 120 s")
    check(B.wants(us(enabled=True)) and B.wants(us(enabled=True), None) and not B.wants(us()) and not B.wants({})
          and B.wants(us(rapid=rp()), "rapid") and not B.wants(us(rapid=rp())) and not B.wants(us(rapid=rp()), "scalp")
          and not any(B.wants(us(rapid=r), "rapid") for r in (
              None, "on", RAPID_DEFAULTS, rp(enabled="true"), rp(triggers={"signals": True, "news": False}),
              rp(triggers="news"), rp(triggers={"news": 1}))),
          "wants: burst = the classic switch as up to v1.2.0; rapid = Rapid on with its news trigger (true only)")

    # ── Rapid off: plan_runs is plan_for, call for call (the engine's call up to M3) ──
    offs = (None, "junk", DEFAULT_USER_SETTINGS["rapid"], rp(triggers={"signals": True, "news": False}),
            {"enabled": "true"})
    bursts = ({}, {"enabled": True}, {"symbols": ["XAUUSD"]}, {"symbols": ["GBPUSD"]}, {"currencies": ["EUR"]},
              {"symbols": ["GBPJPY"], "currencies": ["JPY"]}, {"symbols": ["USDJPY", "GBPUSD", "XAUUSD"]})
    haves = (frozenset(), {("EURUSD", t1, "USD", "burst")},
             {("XAUUSD", t1, "USD", "burst"), ("USDCAD", t2, "CAD", "burst")})
    diff = [(off, b, own, have) for off in offs for b in bursts for own in ((), ["GBPUSD", "XAUUSD"]) for have in haves
            for cs, syms in ((c8, all8), (c6, six))
            if B.plan_runs(us(own, off, **b), cs, syms, have)
            != [(c, "burst") for c in B.plan_for(us(own, off, **b), cs, syms, {h[:3] for h in have})]]
    check(not diff, "Rapid off (no block, a corrupt one, switched off, its news trigger off): plan_runs plans what "
          "plan_for did, the classic burst's runs alone", diff[:2])
    check(runs(us(rapid=rp()), classic=False) and not runs(us(rapid=rp(enabled=False)), classic=False)
          and runs(us(rapid=rp(enabled=False))) == [(c["symbol"], c["currency"], c["title"], "burst")
                                                    for c in B.plan_for(us(), c8, all8)],
          "plan_runs: the classic burst's only when asked (its switch), Rapid's only when on")

    # ── Rapid on: a release's first max_runs pairs, in the currency's list order ──
    r2 = [("EURUSD", "USD", "CPI m/m + Main Refinancing Rate", "rapid"), ("XAUUSD", "USD", "CPI m/m", "rapid"),
          ("GBPUSD", "GBP", "GDP m/m", "rapid"), ("USDJPY", "JPY", "BOJ Policy Rate", "rapid"),
          ("GBPJPY", "GBP", "GDP m/m + BOJ Policy Rate", "rapid"), ("USDCAD", "CAD", "Overnight Rate", "rapid")]
    check(runs(us(rapid=rp()), classic=False) == r2,
          "Rapid (max_runs 2): USD on EURUSD and XAUUSD, GBP on GBPUSD and GBPJPY, JPY on USDJPY and GBPJPY; a pair two "
          "releases take is one run, named after both", runs(us(rapid=rp()), classic=False))
    usd4 = [r2[0], r2[1], ("GBPUSD", "USD", "CPI m/m + GDP m/m", "rapid"),
            ("USDJPY", "USD", "CPI m/m + BOJ Policy Rate", "rapid")]
    check(runs(us(rapid=rp(max_runs=1)), classic=False) == [x for x in r2 if x[0] not in ("XAUUSD", "GBPJPY")]
          and runs(us(rapid=rp(max_runs=9)), classic=False)[:4] == usd4
          and runs(us(rapid=rp(max_runs="x")), classic=False) == r2,
          "max_runs: 1 = each currency's own pair; 9 is clamped to 5 (USD on its four pairs); a corrupt one is 2",
          runs(us(rapid=rp(max_runs=9)), classic=False))
    gu = B.candidates([ev3("g", "GBP", "GDP m/m"), ev3("u", "USD", "CPI m/m")], {"symbols": all8})   # GBP listed first
    check(runs(us(rapid=rp(max_runs=1)), gu, classic=False)
          == [("GBPUSD", "GBP", "GDP m/m", "rapid"), ("EURUSD", "USD", "CPI m/m", "rapid")],
          "a release's pairs by their place in its currency's list, not the calendar's order: GBP listed first (its "
          "GBPUSD comes first) and USD still on EURUSD", runs(us(rapid=rp(max_runs=1)), gu, classic=False))
    check(runs(us(rapid=rp(symbols=["GBPUSD", "USDJPY"])), classic=False)
          == [("GBPUSD", "USD", "CPI m/m + GDP m/m", "rapid"), ("USDJPY", "USD", "CPI m/m + BOJ Policy Rate", "rapid")],
          "rapid.symbols: only those pairs, still in each list's order (USD on both, GBP and JPY on their own)",
          runs(us(rapid=rp(symbols=["GBPUSD", "USDJPY"])), classic=False))
    check(runs(us(["XAUUSD", "USDCAD"], rapid=rp()), classic=False)
          == [("XAUUSD", "USD", "CPI m/m", "rapid"), ("USDCAD", "CAD", "Overnight Rate", "rapid")]
          and runs(us(rapid=rp()), c6, six, classic=False)
          == [r2[0], ("GBPUSD", "USD", "CPI m/m + GDP m/m", "rapid"), r2[3], r2[5]],
          "the pairs the user and the engine trade only (no XAUUSD, GBPJPY: USD on EURUSD and GBPUSD)",
          runs(us(rapid=rp()), c6, six, classic=False))

    # ── Rapid first, then the classic burst on a pair Rapid left: never both on one pair ──
    both = us(rapid=rp(max_runs=1), enabled=True, symbols=["EURUSD", "XAUUSD", "GBPUSD"])
    check(runs(both) == [r2[0], r2[2], r2[3], r2[5], ("XAUUSD", "USD", "CPI m/m", "burst")],
          "Rapid on each own pair, the classic burst (picks EURUSD, XAUUSD, GBPUSD) on XAUUSD: its first pick Rapid "
          "left", runs(both))
    check(runs(us(rapid=rp(), enabled=True)) == r2,
          "the classic burst without picks where Rapid (max_runs 2) holds every pair of each list: no classic run (as "
          "with any other run on that pair)", runs(us(rapid=rp(), enabled=True)))
    # the classic burst without picks where Rapid holds a release's own pair: the next pair of its list Rapid left
    alone = us(rapid=rp(max_runs=1), enabled=True)
    check(runs(alone) == [r2[0], r2[2], r2[3], r2[5], ("XAUUSD", "USD", "CPI m/m", "burst"),
                          ("GBPJPY", "GBP", "GDP m/m + BOJ Policy Rate", "burst")],
          "the classic burst without picks, Rapid (max_runs 1) on each own pair: USD's on XAUUSD, GBP's and JPY's on "
          "GBPJPY (one run, named after both), none for EUR and CAD (no other pair in their lists)", runs(alone))
    check(runs(alone, c6, six) == [r2[0], r2[2], r2[3], r2[5]]
          and runs(us(["EURUSD", "USDCAD"], rp(max_runs=1), enabled=True)) == [r2[0], r2[5]],
          "... only on a pair the user and the engine trade: v1.2.0's pairs or the user's own, all Rapid's: no classic "
          "run", runs(alone, c6, six))
    have = {(c["symbol"], c["time"], c["currency"], k) for c, k in B.plan_runs(alone, c8, all8)}
    check(B.plan_runs(alone, c8, all8, have) == [], "... planned once: with its runs in `have`, nothing more",
          B.plan_runs(alone, c8, all8, have))
    check(runs(us(rapid=rp(max_runs=1, symbols=["XAUUSD"]), enabled=True))
          == [("XAUUSD", "USD", "CPI m/m", "rapid")] + [(*x, "burst") for x in (
              ("EURUSD", "USD", "CPI m/m + Main Refinancing Rate"), ("GBPUSD", "GBP", "GDP m/m"),
              ("USDJPY", "JPY", "BOJ Policy Rate"), ("USDCAD", "CAD", "Overnight Rate"))],
          "Rapid on another pair than the own one (rapid.symbols XAUUSD): the classic burst on each own pair, as before",
          runs(us(rapid=rp(max_runs=1, symbols=["XAUUSD"]), enabled=True)))
    no_eur = us(["GBPUSD", "XAUUSD", "USDJPY"], rp(max_runs=1, symbols=["XAUUSD"]), enabled=True)   # no EURUSD traded
    check(runs(no_eur) == [("XAUUSD", "USD", "CPI m/m", "rapid"), ("GBPUSD", "GBP", "GDP m/m", "burst"),
                           ("USDJPY", "JPY", "BOJ Policy Rate", "burst")]
          and runs(no_eur, B.candidates([rel[0]], {"symbols": all8})) == [("XAUUSD", "USD", "CPI m/m", "rapid")],
          "the user does not trade USD's own pair (EURUSD) and Rapid holds XAUUSD: no classic burst for USD's release "
          "(as up to v1.2.0: no own pair, no burst), GBP's and JPY's on their own pairs", runs(no_eur))
    check(runs(us(rapid=rp(max_runs=1), enabled=True, symbols=["EURUSD", "GBPUSD"]))
          == [r2[0], r2[2], r2[3], r2[5]],
          "with picks the classic burst keeps to them: picks EURUSD, GBPUSD both Rapid's, no classic run (no fallback)",
          runs(us(rapid=rp(max_runs=1), enabled=True, symbols=["EURUSD", "GBPUSD"])))
    have = {(c["symbol"], c["time"], c["currency"], k) for c, k in B.plan_runs(both, c8, all8)}
    check(B.plan_runs(both, c8, all8, have) == [], "planned once: with its runs in `have`, nothing more")
    grid = [B.plan_runs(us(own, rp(max_runs=n, symbols=rs), enabled=True, symbols=bp), cs, syms)
            for n in (1, 2, 5) for rs in ([], ["XAUUSD", "GBPJPY"]) for own in ((), ["EURUSD", "GBPUSD", "GBPJPY"])
            for bp in ([], ["EURUSD", "XAUUSD", "GBPUSD"], ["GBPJPY", "USDJPY"]) for cs, syms in ((c8, all8), (c6, six))]
    kinds = [[k for _, k in g] for g in grid]
    check(all(len({(c["symbol"], c["time"]) for c, _ in g}) == len(g) for g in grid)
          and all(k == sorted(k, key=lambda x: x != "rapid") for k in kinds)
          and sum("burst" in k and "rapid" in k for k in kinds) >= 10,
          "never two runs on one pair at one time, Rapid's listed first, whatever the picks and limits",
          [g for g in grid if len({(c["symbol"], c["time"]) for c, _ in g}) < len(g)][:1])
    cancelled = {("EURUSD", t1, "USD", "rapid")}
    check(runs(us(rapid=rp()), have=cancelled, classic=False) == [x for x in r2 if x[0] not in ("EURUSD", "XAUUSD")],
          "a release with a Rapid run (a cancelled one too) gets no other; nor one whose pairs include a Rapid run's",
          runs(us(rapid=rp()), have=cancelled, classic=False))
    held = {("EURUSD", t1, "USD", "burst")}
    check(runs(us(rapid=rp()), have=held, classic=False)
          == [r2[1], ("GBPUSD", "USD", "CPI m/m + GDP m/m", "rapid"), r2[3], r2[4], r2[5]]
          and runs(us(rapid=rp(), enabled=True), have=held) == runs(us(rapid=rp()), have=held, classic=False),
          "a classic run planned before Rapid was switched on holds its pair: Rapid takes the next ones, no classic "
          "run is added", runs(us(rapid=rp()), have=held, classic=False))

    # ── a Rapid run's params, version and life ──
    keys = {"orders", "sl_unit", "risk_unit", "risk_min", "risk_max", "sl_pips", "lots", "tp_min_usd", "tp_max_usd",
            "limit_pips", "offset_pips", "expire_seconds", "hold_seconds", "analysis", "max_hold_seconds",
            "max_spread_pips"}             # written out: not B.RAPID_PARAMS, which would be checked against itself
    plain = keys - {"orders", "sl_unit", "max_hold_seconds", "max_spread_pips"}
    s = {**DEFAULT_USER_SETTINGS, "max_spread_pips": 2.0, "rapid": rp(orders=30, sl_unit="money")}
    p = B.params_for(s, 6, "rapid")
    check(set(p) == keys and p["orders"] == 6
          and p["sl_unit"] == "money" and p["max_hold_seconds"] == 120 and p["max_spread_pips"] == 2.0
          and {k: p[k] for k in plain} == {k: RAPID_DEFAULTS[k] for k in plain},
          "params_for rapid: Rapid's keys, orders capped by the admin's max_trades, Y, the user's spread cap", p)
    # every key set to a value of its own (none its default): each reaches the plan as set, none dropped or renamed
    mine = {"orders": 30, "sl_unit": "money", "risk_unit": "usd", "risk_min": 25.0, "risk_max": 75.0, "sl_pips": 12.0,
            "lots": 0.5, "tp_min_usd": 20.0, "tp_max_usd": 40.0, "limit_pips": 1.5, "offset_pips": 3.0,
            "expire_seconds": 45, "hold_seconds": 40, "analysis": False, "max_hold_seconds": 300}
    usual = {**RAPID_DEFAULTS, **DEFAULT_USER_SETTINGS["rapid"]}
    full = B.params_for({**DEFAULT_USER_SETTINGS, "max_spread_pips": 2.5, "rapid": rp(**mine)}, 6, "rapid")
    check(all(mine[k] != usual[k] for k in mine) and set(full) == keys
          and all(full[k] == mine[k] for k in mine if k != "orders") and full["orders"] == 6
          and full["max_spread_pips"] == 2.5,
          "params_for rapid: each key as the user set it, orders capped by the admin's max_trades", full)
    check(B.params_for(s, 6) == B.params_for(s, 6, "burst") and set(B.params_for(s, 6)) == PARAM_KEYS,
          "params_for burst: as before")
    old = B.params_for({**DEFAULT_USER_SETTINGS, "rapid": {**RAPID_DEFAULTS, "hold_seconds": 45}}, 40, "rapid")
    junk = B.params_for({**DEFAULT_USER_SETTINGS, "rapid": "junk"}, 40, "rapid")
    bad = B.params_for({**DEFAULT_USER_SETTINGS, "rapid": {"orders": "x", "hold_seconds": 3}}, 40, "rapid")
    check(old["max_hold_seconds"] == 45 and junk == B.params_for(DEFAULT_USER_SETTINGS, 40, "rapid")
          and (junk["orders"], junk["max_hold_seconds"]) == (10, 120)
          and (bad["orders"], bad["hold_seconds"], bad["max_hold_seconds"]) == (10, 5, 5),
          "params_for rapid: Y missing = X; a corrupt block reads as a new user's; a bad value its default",
          (old, junk, bad))
    check(B.needs_version(p, "rapid") == B.V130 and B.needs_version(V120_PARAMS, "rapid", "EURUSD") == B.V130
          and B.needs_version(V120_PARAMS, "burst", "EURUSD") == B.MIN_CONNECTOR
          and not B.version_ok("1.2.0", B.needs_version(p, "rapid")) and B.version_ok("1.3.0", B.needs_version(p, "rapid")),
          "needs_version: a Rapid run needs connector 1.3.0, whatever its params")
    check(B.run_seconds(p, "rapid") == 30 + 120 and B.run_seconds({}, "rapid") == 60 and B.run_seconds({}) == 120
          and B.run_seconds({"expire_seconds": "x", "hold_seconds": 45}, "rapid") == 75,
          "run_seconds rapid: Rapid's expire + max(X, Y); an unreadable value is Rapid's default (30 s), not the "
          "burst's (90 s)")

    # ── the stored block (db.User.merged_settings) ──
    ms = lambda settings: User(settings=settings).merged_settings()     # noqa: E731
    st_on = {**SHORT_DEFAULTS, "enabled": True}
    check(ms(None)["rapid"] == ms({"rapid": "junk"})["rapid"] == DEFAULT_USER_SETTINGS["rapid"]
          and ms({"rapid": {"enabled": True, "orders": "x"}})["rapid"] == {**RAPID_DEFAULTS, "enabled": True},
          "merged_settings: no rapid block stored (a v1.2.0 user) or a corrupt one: the defaults, off; a bad value its "
          "default, no Y when none was stored")
    got = ms({"rapid": rp(), "short_term": st_on})
    check(got["rapid"]["enabled"] is False and got["short_term"]["enabled"] is True
          and ms({"rapid": rp(), "short_term": SHORT_DEFAULTS})["rapid"]["enabled"] is True,
          "merged_settings: Rapid and short-term both stored on (written some other way): short-term keeps it",
          (got["rapid"]["enabled"], got["short_term"]["enabled"]))


# ── API / WebSocket flow ───────────────────────────────────────
def session_of(client) -> str:
    tok = client.cookies.get("fx_session")
    client.cookies.clear()
    return tok


def use(client, tok: str):
    client.cookies.clear()
    client.cookies.set("fx_session", tok)


OPEN: list = []


def ws_open(client, path: str):
    ws = client.websocket_connect(path).__enter__()
    OPEN.append(ws)
    return ws


def connect(client, token: str, version="1.3.0", demo=True):
    """A fake connector: the hello the real one sends, then the server's welcome."""
    ws = ws_open(client, "/ws/connector")
    ws.send_json({"type": "auth", "token": token, "version": version, "paper": False,
                  "account": {"login": 7, "server": "Test-Demo", "currency": "USD", "balance": 10000.0,
                              "equity": 10000.0, "demo": demo, "trade_allowed": True}})
    return ws, ws.receive_json()


def close(ws):
    OPEN.remove(ws)
    try:
        ws.__exit__(None, None, None)
    except Exception:
        pass


def sync(ws) -> list[dict]:
    """Everything the server sent so far, once it has handled everything we sent (ping -> pong)."""
    ws.send_json({"type": "ping"})
    out = []
    while (m := ws.receive_json())["type"] != "pong":
        out.append(m)
    return out


def plans_in(msgs) -> dict[int, dict]:
    return {m["burst"]["id"]: m["burst"] for m in msgs if m["type"] == "burst"}


def cancel_msg(bid: int) -> dict:
    return {"type": "burst_cancel", "id": bid}


def row(bid: int) -> Burst:
    with SessionLocal() as db:
        return db.get(Burst, bid)


def add(*objs):
    with SessionLocal() as db:
        db.add_all(objs)
        db.commit()
    return objs[0]


def bursts_where(*conds) -> list[Burst]:
    with SessionLocal() as db:
        return db.scalars(select(Burst).where(*conds).order_by(Burst.id)).all()


def err(r) -> str:
    return f"{r.status_code} {r.json().get('detail')}"


def test_flow():
    loops = []

    async def fake_loop(fn, every, first_delay=0):
        loops.append((fn.__name__, every, first_delay))

    engine._loop = fake_loop                     # Engine.start creates its tasks, none of them runs
    main.SETUP_FILE = TMP / "setup_token.txt"    # not data/setup_token.txt
    with TestClient(main.app) as client:
        try:
            _flow(client, loops)
        finally:
            for ws in list(OPEN):             # an open socket keeps the TestClient from shutting down
                close(ws)


def _flow(client, loops):
    from server.db import User, get_app_settings

    tick = lambda: client.portal.call(engine.burst_tick)     # noqa: E731
    check(("burst_tick", 2, 5) in loops, "Engine.start runs burst_tick every 2 s, first after 5 s", loops)

    insp = inspect(db_engine)
    check("burst_id" in {c["name"] for c in insp.get_columns("user_trades")} and "bursts" in insp.get_table_names(),
          "init_db: bursts table created, burst_id added to an old user_trades table")
    with SessionLocal() as db:
        old = db.get(UserTrade, 1)
    check(old is not None and old.ticket == 5 and old.burst_id is None, "init_db keeps the existing trade rows")
    try:
        main.init_db()
        check(True, "init_db runs again on a migrated database")
    except Exception as e:
        check(False, "init_db runs again on a migrated database", e)

    # accounts, the way the app creates them
    r = client.post("/api/setup", headers=H, json={"token": main._setup_token, "email": "founder@test.local",
                                                   "password": "founder-pass-1", "name": "Founder"})
    founder = session_of(client)
    ids = {}
    for name in ("alice", "bob"):
        r = client.post("/api/auth/register", headers=H, json={"email": f"{name}@test.local", "accept_terms": True,
                                                               "password": f"{name}-pass-1"})
        ids[name] = (session_of(client), r.json()["id"])
    (alice, alice_id), (bob, bob_id) = ids["alice"], ids["bob"]
    tokens = {}
    for name, tok in (("alice", alice), ("bob", bob)):
        use(client, tok)
        tokens[name] = client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"]
    check(founder and alice and bob and all(tokens.values()), "founder, two clients and their connector tokens")

    # ── settings ──
    use(client, alice)
    r = client.get("/api/me/settings").json()
    check(r["settings"]["burst"] == DEFAULT_USER_SETTINGS["burst"] and r["burst_app"] == {"enabled": True, "max_trades": 40},
          "GET /api/me/settings: burst defaults + burst_app", r.get("burst_app"))
    r = client.put("/api/me/settings", headers=H, json={"burst": {
        "enabled": True, "trades": 99, "lead_seconds": 5, "offset_pips": 0.5, "sl_pips": 1000, "tp_step_pips": -3,
        "hold_seconds": "45", "currencies": ["usd", "XYZ", "USD"], "bogus": 1}})
    got = r.json()["settings"]["burst"]
    check(got == {**DEFAULT_USER_SETTINGS["burst"], "enabled": True, "trades": 40, "lead_seconds": 10,
                  "offset_pips": 1.0, "sl_pips": 200.0, "tp_step_pips": 0.0, "hold_seconds": 45, "currencies": ["USD"]},
          "PUT /api/me/settings: burst values clamped, unknown keys dropped", got)
    for body, code in (({"burst": {"trades": "many"}}, "invalid_number"), ({"burst": True}, "invalid_burst"),
                       ({"burst": {"currencies": "USD"}}, "invalid_currencies"),
                       ({"burst": {"symbols": "XAUUSD"}}, "invalid_symbols")):
        r = client.put("/api/me/settings", headers=H, json=body)
        check(r.status_code == 400 and r.json()["detail"] == code, f"PUT burst {body['burst']!r} -> 400 {code}", err(r))
    r = client.put("/api/me/settings", headers=H, json={"burst": {"trades": 10, "lead_seconds": 60, "offset_pips": 7,
                                                                  "sl_pips": 10, "tp_step_pips": 1.5,
                                                                  "hold_seconds": 30, "currencies": []}})
    check(r.json()["settings"]["burst"] == {**DEFAULT_USER_SETTINGS["burst"], "enabled": True}
          and r.json()["settings"]["risk_per_trade_pct"] == 1.0, "a partial burst PUT merges into the saved settings")

    # ── admin: kill switch + cap ──
    use(client, founder)
    r = client.put("/api/admin/engine", headers=H, json={"burst": {"enabled": False, "max_trades": 99}})
    check(r.json()["settings"]["burst"] == {"enabled": False, "max_trades": 40}, "admin engine: burst saved, max_trades <= 40",
          r.json()["settings"].get("burst"))
    r = client.put("/api/admin/engine", headers=H, json={"burst": {"max_trades": 5}})
    check(r.json()["settings"]["burst"] == {"enabled": False, "max_trades": 5}, "admin engine: partial burst update")
    audit = client.get("/api/admin/audit").json()
    check(audit[0]["action"] == "engine_settings" and "burst" in audit[0]["details"], "admin burst change is audited")
    r = client.put("/api/admin/engine", headers={**H, "content-type": "application/json"},
                   content='{"burst": {"max_trades": 1e999}}')                      # Infinity: int() overflows
    check(r.status_code == 400 and r.json()["detail"] == "invalid_number", "admin max_trades Infinity: 400", err(r))
    use(client, alice)
    check(client.get("/api/me/settings").json()["burst_app"] == {"enabled": False, "max_trades": 5},
          "burst_app follows the admin settings")
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "burst_disabled", "test burst: 400 burst_disabled", err(r))
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"burst": {"enabled": True}})

    # ── calendar ──
    now = utcnow().replace(microsecond=0)
    t1 = now + timedelta(minutes=10)
    ev = lambda i, t, cur, title, impact="High": CalendarEvent(id=i, time=t, currency=cur, title=title,  # noqa: E731
                                                                 impact=impact, source="test")
    add(ev("ev-fed", t1, "USD", "Federal Funds Rate"), ev("ev-presser", t1, "USD", "FOMC Press Conference"),
        ev("ev-ecb", t1, "EUR", "Main Refinancing Rate"), ev("ev-boj", now + timedelta(minutes=20), "JPY", "BOJ Policy Rate"),
        ev("ev-uk", now + timedelta(days=2), "GBP", "CPI y/y"),
        ev("ev-med", now + timedelta(minutes=5), "CAD", "Building Permits m/m", "Medium"),
        ev("ev-past", now - timedelta(hours=1), "USD", "Retail Sales m/m"),
        ev("ev-far", now + timedelta(days=8), "USD", "CPI m/m"))
    use(client, alice)
    r = client.get("/api/bursts").json()
    check(r["bursts"] == [] and r["app"] == {"enabled": True, "max_trades": 5}
          and r["connector"] == {"online": False, "demo": None, "version": None}, "GET /api/bursts: empty, app, connector",
          {k: r[k] for k in ("app", "connector")})
    up = r["upcoming"]
    check([u["symbol"] for u in up] == ["EURUSD", "USDJPY", "GBPUSD"] and all(set(u) == {"event_id", "time", "currency",
          "title", "symbol", "kind"} and u["kind"] == "burst" for u in up),
          "upcoming: qualifying events within 7 days, one per pair and minute", up)
    check("Federal Funds Rate" in up[0]["title"] and "Main Refinancing Rate" in up[0]["title"]
          and "Press" not in up[0]["title"] and up[0]["time"] == t1.isoformat() + "Z", "upcoming: collapsed title", up[:1])
    for body, want in (({"burst": {"currencies": ["GBP"]}}, ["GBPUSD"]), ({"burst": {"currencies": ["EUR"]}}, ["EURUSD"]),
                       ({"burst": {"currencies": []}, "symbols": ["USDJPY"]}, ["USDJPY"])):
        client.put("/api/me/settings", headers=H, json=body)
        got = [u["symbol"] for u in client.get("/api/bursts").json()["upcoming"]]
        check(got == want, f"upcoming follows the user's picks {body}", got)
    client.put("/api/me/settings", headers=H, json={"symbols": []})
    # the classic burst switched off and Rapid's news runs off (no rapid block stored, as for a v1.2.0 user; or one
    # switched off): upcoming still lists what the classic burst would trade (plan_for), as up to v1.2.0
    on = client.get("/api/bursts").json()["upcoming"]
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": False}})
    for case in ("no rapid block", "rapid off"):
        if case == "rapid off":
            client.put("/api/me/settings", headers=H, json={"rapid": {"enabled": False}})
        with SessionLocal() as db:
            u = db.get(User, alice_id)
            if case == "no rapid block":
                u.settings = {k: v for k, v in u.settings.items() if k != "rapid"}
                db.commit()
            check(("rapid" in u.settings) == (case == "rapid off"), f"{case}: alice's stored settings")
            s, app_s = u.merged_settings(), get_app_settings(db)
            evs = db.scalars(select(CalendarEvent).where(
                CalendarEvent.impact == "High", CalendarEvent.time > utcnow(),
                CalendarEvent.time <= utcnow() + timedelta(days=7)).order_by(CalendarEvent.time)).all()
        want = [{"event_id": c["event_id"], "time": c["time"].isoformat() + "Z", "currency": c["currency"],
                 "title": c["title"], "symbol": c["symbol"], "kind": "burst"}       # alice has no runs yet: none done
                for c in B.plan_for(s, B.candidates(evs, app_s), app_s["symbols"])][:10]
        got = client.get("/api/bursts").json()["upcoming"]
        check(s["burst"]["enabled"] is False and s["rapid"]["enabled"] is False and want and got == want == on,
              f"upcoming, the classic burst off and {case}: plan_for's runs, kind burst, as with it on", got)
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": True}})
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "connector_offline", "test burst: 400 connector_offline", err(r))

    # ── bob: real account, then an old connector ──
    use(client, bob)
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": True}})
    ws, welcome = connect(client, tokens["bob"], version="1.0.0", demo=False)
    check(welcome["type"] == "welcome" and welcome["bursts"] == [], "welcome carries a bursts list", welcome.get("bursts"))
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "demo_only", "test burst: 400 demo_only", err(r))
    close(ws)
    ws, _ = connect(client, tokens["bob"], version="1.0.0", demo=True)
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "connector_outdated", "test burst: 400 connector_outdated", err(r))
    tick()
    rows = bursts_where(Burst.user_id == bob_id)
    check(len(rows) == 1 and rows[0].status == "skipped" and rows[0].reason == B.OUTDATED and rows[0].symbol == "EURUSD",
          "plan: a connector older than 1.1.0 gets its burst skipped", [b.to_dict() for b in rows])
    check(not plans_in(sync(ws)), "an outdated connector is sent no plan")
    bob_burst = rows[0].id
    close(ws)

    # ── alice: test burst ──
    use(client, alice)
    ws, welcome = connect(client, tokens["alice"])
    bw = ws_open(client, "/ws")                                # alice's browser tab
    check(bw.receive_json()["type"] == "hello", "browser socket connected")
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURGBP"})            # not one the engine trades
    check(r.status_code == 400 and r.json()["detail"] == "invalid_symbol", "test burst: 400 invalid_symbol", err(r))
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "eurusd", "offset_pips": 0.2})
    t = r.json()
    check(r.status_code == 200 and t["status"] == "planned" and t["test"] is True and t["event_title"] == "Test burst"
          and t["symbol"] == "EURUSD" and t["event_id"] is None, "POST /api/bursts/test creates a planned test burst", r.text)
    check(t.get("params") == {"trades": 5, "lead_seconds": 15, "offset_pips": 1.0, "sl_pips": 10.0, "tp_start_pips": 4.0,
                              "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "risk_pct": 1.0,
                              "max_spread_pips": 3.0, "lots": 0.1, "sl_unit": "pct", "sl_money": 2.0, "analysis": True,
                              "max_hold_seconds": 120},
          "test params: user's settings, lead 15 s, offset clamped to 1, trades capped by the admin", t.get("params"))
    plans = plans_in(sync(ws))
    tp = plans.get(t["id"], {})
    check(list(plans) == [t["id"]] and set(tp) == SENT_PLAN_KEYS and tp["test"] is True and 12 < tp["seconds_until"] <= 15
          and tp["params"] == t["params"] and tp["kind"] == "burst"
          and tp["context"] == {"expect": None, "ai": None, "title": ""},
          "the test plan goes to the connector at once (no event, no AI analysis: a context saying nothing)", plans)
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "GBPUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "test_running", "test burst: 400 test_running", err(r))

    # ── plan + deliver ──
    tick()
    rows = bursts_where(Burst.user_id == alice_id, Burst.test.is_(False))
    fed = rows[0] if rows else None
    check(len(rows) == 1 and fed.status == "planned" and fed.symbol == "EURUSD" and fed.event_time == t1
          and fed.currency in ("USD", "EUR") and fed.event_id in ("ev-fed", "ev-ecb") and "Federal Funds Rate" in fed.event_title
          and "Main Refinancing Rate" in fed.event_title and "Press" not in fed.event_title,
          "burst_tick plans one burst per pair and release minute (USDJPY in 20 min not yet)", [b.to_dict() for b in rows])
    check(fed and fed.params["trades"] == 5 and fed.params["lead_seconds"] == 60 and set(fed.params) == PARAM_KEYS,
          "plan params: the user's settings, trades capped by the admin", fed and fed.params)
    plans = plans_in(sync(ws))
    check(set(plans) == {fed.id, t["id"]} and 590 < plans[fed.id]["seconds_until"] <= 600
          and plans[fed.id]["event_time"] == t1.isoformat() + "Z", "deliver: planned bursts are resent each tick", plans)
    pushed = plans_in(sync(bw))
    check(fed.id in pushed, "new bursts are pushed to the user's browsers")
    check(pushed.get(fed.id, {}).get("kind") == "burst" and "analysis" in pushed.get(fed.id, {})
          and pushed[fed.id]["analysis"] is None, "a planned burst is a classic one (kind burst), no analysis yet",
          pushed.get(fed.id))
    tick()
    msgs = sync(ws)
    check(row(fed.id).status == "planned" and row(fed.id).reason is None and cancel_msg(fed.id) not in msgs
          and fed.id in plans_in(msgs), "the planned EURUSD burst of a USD release survives the next burst_tick "
          "(live_releases knows every pair of the lists)", (row(fed.id).to_dict(), msgs))

    # ── connector reports ──
    tid = t["id"]
    opened = {"symbol": "EURUSD", "side": "buy", "lots": 0.5, "entry_price": 1.1, "sl": 1.099, "tp": None, "demo": True,
              "opened_at": utcnow().isoformat()}
    for m in ({"type": "burst_update", "id": fed.id, "status": "scheduled"},
              {"type": "burst_update", "id": tid, "status": "scheduled"},
              {"type": "burst_update", "id": tid, "status": "armed", "orders": 10, "side": None},
              {"type": "trade_opened", "trade": {"ticket": 7001, "burst_id": tid, **opened}},
              {"type": "trade_opened", "trade": {"ticket": 7002, "burst_id": bob_burst, **opened}},
              {"type": "burst_update", "id": tid, "status": "triggered", "side": "buy", "fills": 5},
              {"type": "trade_closed", "trade": {"ticket": 7001, "exit_price": 1.1005, "pnl": 3.456,
                                                 "closed_at": utcnow().isoformat()}},
              # handed to the sweeper, which reported it while the burst still ran: not in the runner's own sum
              {"type": "trade_opened", "trade": {"ticket": 7003, "burst_id": tid, **opened}},
              {"type": "trade_closed", "trade": {"ticket": 7003, "exit_price": 1.1002, "pnl": 1.004,
                                                 "closed_at": utcnow().isoformat()}},
              {"type": "burst_update", "id": tid, "status": "closed", "side": "buy", "orders": 10, "fills": 5,
               "pnl": 3.456, "reason": None}):
        ws.send_json(m)
    sync(ws)
    tb = row(tid)
    check((tb.status, tb.side, tb.orders, tb.fills, tb.pnl, tb.reason) == ("closed", "buy", 10, 5, 4.46, None),
          "burst_update: status, side, orders, fills applied; pnl that of its closed trades", tb.to_dict())
    check(row(fed.id).status == "scheduled", "burst_update scheduled confirms a plan")
    with SessionLocal() as db:
        trades = {x.ticket: x for x in db.scalars(select(UserTrade).where(UserTrade.user_id == alice_id))}
    check(trades[7001].burst_id == tid and trades[7001].signal_id is None and trades[7001].status == "closed",
          "trade_opened stores burst_id", trades[7001].to_dict())
    check(trades[7002].burst_id is None, "a burst_id of another user's burst is not stored")
    msgs = sync(bw)
    seq = [m["burst"]["status"] for m in msgs if m["type"] == "burst" and m["burst"]["id"] == tid]
    logs = [m["message"] for m in msgs if m["type"] == "connector_log"]
    check(seq == ["scheduled", "armed", "triggered", "closed"], "every burst_update is pushed to the browsers", seq)
    check(len(logs) == 3 and "armed" in logs[0] and "triggered" in logs[1] and "closed" in logs[2],
          "connector_log lines for armed, triggered and the final state", logs)
    close(bw)

    ws.send_json({"type": "burst_update", "id": tid, "status": "armed", "orders": 99, "pnl": 0})
    msgs = sync(ws)
    tb = row(tid)
    check(tb.status == "closed" and tb.orders == 10 and tb.pnl == 4.46, "a final burst never changes again")
    check(cancel_msg(tid) in msgs, "a runner still reporting on a finished burst is told to cancel", msgs)
    for m in ({"type": "trade_opened", "trade": {"ticket": 7004, "burst_id": tid, **opened}},
              {"type": "trade_closed", "trade": {"ticket": 7004, "exit_price": 1.0988, "pnl": -1.2,
                                                 "closed_at": utcnow().isoformat()}}):
        ws.send_json(m)
    sync(ws)
    check(row(tid).pnl == 3.26 and row(tid).status == "closed",
          "a burst trade closed after its burst ended (by the sweeper) counts in the burst's P&L", row(tid).to_dict())
    for m in ({"id": fed.id, "status": "exploded"}, {"id": bob_burst, "status": "error", "reason": "x"},
              {"id": "junk", "status": "armed"}, {"status": "armed"}):
        ws.send_json({"type": "burst_update", **m})
    sync(ws)
    check(row(fed.id).status == "scheduled" and row(bob_burst).status == "skipped" and row(bob_burst).reason == B.OUTDATED,
          "unknown status, another user's burst and bad ids are ignored")
    inf, nan = float("inf"), float("nan")              # JSON allows them; a failing commit used to end the socket
    for m in ({"type": "burst_update", "id": fed.id, "orders": 1e30, "fills": inf, "pnl": nan},
              {"type": "burst_update", "id": inf, "status": "armed"},
              {"type": "trade_opened", "trade": {"ticket": 1e30, **opened}},
              {"type": "trade_opened", "trade": {**opened, "ticket": 7003, "entry_price": nan, "lots": inf}},
              {"type": "trade_closed", "trade": {"ticket": -inf}}):
        ws.send_json(m)
    sync(ws)
    with SessionLocal() as db:
        t7003 = db.scalar(select(UserTrade).where(UserTrade.ticket == 7003))
    f = row(fed.id)
    check((f.status, f.orders, f.fills, f.pnl) == ("scheduled", 0, 0, None) and t7003 and t7003.entry_price == 0.0
          and t7003.lots == 0.0, "numbers the database cannot store are dropped and the socket stays open")

    # ── reconcile: event rescheduled ──
    # ForexFactory ids include the time: a moved release comes back as a new row, the old one is deleted
    moved_to = t1 + timedelta(minutes=3)
    with SessionLocal() as db:
        db.delete(db.get(CalendarEvent, "ev-fed"))
        db.add(ev("ev-fed-2", moved_to, "USD", "Federal Funds Rate"))
        db.commit()
    tick()
    msgs = sync(ws)
    f = row(fed.id)
    check(f.status == "scheduled" and f.reason is None and cancel_msg(fed.id) not in msgs,
          "a collapsed burst stands while one of its releases (ECB) keeps that time", (f.to_dict(), msgs))
    moved = bursts_where(Burst.user_id == alice_id, Burst.event_time == moved_to)
    check(len(moved) == 1 and moved[0].status == "planned" and moved[0].event_id == "ev-fed-2"
          and moved[0].id in plans_in(msgs), "the moved event gets a new burst at its new time")
    with SessionLocal() as db:
        db.get(CalendarEvent, "ev-ecb").time = moved_to
        db.commit()
    tick()
    msgs = sync(ws)
    f = row(fed.id)
    check(f.status == "scheduled" and f.reason == "event rescheduled" and msgs.count(cancel_msg(fed.id)) == 1,
          "every release gone from that time: burst_cancel sent, the connector reports the end", (f.to_dict(), msgs))
    moved = moved[0]
    tick()
    check(cancel_msg(fed.id) not in sync(ws), "burst_cancel is sent only once")
    ws.send_json({"type": "burst_update", "id": fed.id, "status": "cancelled", "reason": None})
    sync(ws)
    f = row(fed.id)
    check(f.status == "cancelled" and f.reason == "event rescheduled" and fed.id not in engine.burst_cancel_sent,
          "the connector's final report closes it; the server's reason is kept", f.to_dict())

    # ── cancel endpoint ──
    r = client.post(f"/api/bursts/{moved.id}/cancel", headers=H)
    check(r.status_code == 200 and r.json()["status"] == "cancelled" and r.json()["reason"] == "cancelled by you"
          and cancel_msg(moved.id) in sync(ws), "cancel a planned burst: cancelled at once (and the connector told)", r.text)
    r = client.post(f"/api/bursts/{moved.id}/cancel", headers=H)
    check(r.status_code == 400 and r.json()["detail"] == "burst_finished", "cancel a finished burst: 400 burst_finished", err(r))
    use(client, founder)
    r = client.post(f"/api/bursts/{moved.id}/cancel", headers=H)
    check(r.status_code == 404, "cancel someone else's burst: 404", err(r))
    use(client, alice)
    add(ev("ev-rba", now + timedelta(minutes=12), "AUD", "Cash Rate"))
    tick()
    rba = bursts_where(Burst.user_id == alice_id, Burst.symbol == "AUDUSD")[0]
    ws.send_json({"type": "burst_update", "id": rba.id, "status": "scheduled"})
    sync(ws)
    r = client.post(f"/api/bursts/{rba.id}/cancel", headers=H)
    check(r.status_code == 200 and r.json()["status"] == "scheduled" and r.json()["reason"] == "cancelled by you"
          and cancel_msg(rba.id) in sync(ws), "cancel a scheduled burst: burst_cancel to the connector", r.text)
    ws.send_json({"type": "burst_update", "id": rba.id, "status": "cancelled"})
    sync(ws)
    check(row(rba.id).status == "cancelled", "the connector confirms the cancel")

    # ── reconcile: the user's switch ──
    add(ev("ev-snb", now + timedelta(minutes=11), "CHF", "SNB Policy Rate"),
        ev("ev-boc", now + timedelta(minutes=13), "CAD", "Overnight Rate"))
    tick()
    snb = bursts_where(Burst.user_id == alice_id, Burst.symbol == "USDCHF")[0]
    boc = bursts_where(Burst.user_id == alice_id, Burst.symbol == "USDCAD")[0]
    ws.send_json({"type": "burst_update", "id": snb.id, "status": "scheduled"})
    sync(ws)
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": False}})
    tick()
    msgs = sync(ws)
    snb, boc = row(snb.id), row(boc.id)
    check(boc.status == "cancelled" and boc.reason == "news burst switched off", "switch off: a planned burst is cancelled",
          boc.to_dict())
    check(snb.status == "scheduled" and snb.reason == "news burst switched off" and cancel_msg(snb.id) in msgs,
          "switch off: a scheduled burst gets burst_cancel", (snb.to_dict(), msgs))
    tick()
    check(cancel_msg(snb.id) not in sync(ws), "switch off: burst_cancel only once")
    ws.send_json({"type": "burst_update", "id": snb.id, "status": "cancelled"})
    sync(ws)
    add(ev("ev-nfp", now + timedelta(minutes=9), "USD", "Non-Farm Employment Change"))
    tick()
    check(row(snb.id).status == "cancelled" and not bursts_where(Burst.user_id == alice_id, Burst.event_time == now + timedelta(minutes=9)),
          "switch off: no new bursts are planned")
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": True}})
    up = {(u["symbol"], u["time"]) for u in client.get("/api/bursts").json()["upcoming"]}
    at = lambda minutes: (now + timedelta(minutes=minutes)).isoformat() + "Z"     # noqa: E731
    check(("USDCHF", at(11)) not in up and ("USDCAD", at(13)) not in up and ("EURUSD", at(9)) in up,
          "upcoming leaves out releases whose burst already ended (switched off, then on again)", up)

    # ── reconcile: the admin kill switch ──
    tick()
    nfp = bursts_where(Burst.user_id == alice_id, Burst.event_time == now + timedelta(minutes=9))
    check(len(nfp) == 1 and nfp[0].status == "planned", "switch on again: the next release is planned")
    ws.send_json({"type": "burst_update", "id": nfp[0].id, "status": "scheduled"})
    sync(ws)
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"burst": {"enabled": False}})
    add(ev("ev-gb", now + timedelta(minutes=14), "GBP", "Retail Sales m/m"))
    tick()
    msgs = sync(ws)
    n = row(nfp[0].id)
    check(n.status == "scheduled" and n.reason == "news burst disabled by admin" and cancel_msg(n.id) in msgs
          and not bursts_where(Burst.symbol == "GBPUSD", Burst.user_id == alice_id),
          "admin kill switch: burst_cancel sent, nothing new planned", (n.to_dict(), msgs))
    ws.send_json({"type": "burst_update", "id": n.id, "status": "cancelled"})
    sync(ws)
    client.put("/api/admin/engine", headers=H, json={"burst": {"enabled": True, "max_trades": 40}})
    use(client, alice)

    # ── the user's pair picks: a release on another pair of its currency's list ──
    r = client.put("/api/me/settings", headers=H, json={"burst": {"symbols": ["xauusd", "GBPJPY", "EURGBP"],
                                                                  "currencies": ["USD", "JPY"]}})
    check(r.json()["settings"]["burst"]["symbols"] == ["XAUUSD", "GBPJPY"],
          "PUT burst symbols: upper case, only pairs of the currency lists", r.json()["settings"]["burst"])
    add(ev("ev-ism", now + timedelta(minutes=7), "USD", "ISM Services PMI"),
        ev("ev-tankan", now + timedelta(minutes=8), "JPY", "Tankan Manufacturing Index"))
    up = [(u["symbol"], u["time"]) for u in client.get("/api/bursts").json()["upcoming"]]
    check(up == [("XAUUSD", at(7)), ("GBPJPY", at(8)), ("GBPJPY", at(20))],
          "upcoming follows the pair picks (the USD releases at +9 and +13 keep their cancelled EURUSD bursts)", up)
    tick()
    xau = bursts_where(Burst.user_id == alice_id, Burst.event_time == now + timedelta(minutes=7))
    gj = bursts_where(Burst.user_id == alice_id, Burst.event_time == now + timedelta(minutes=8))
    check([(b.symbol, b.status, b.currency, b.event_id, b.event_title) for b in xau]
          == [("XAUUSD", "planned", "USD", "ev-ism", "ISM Services PMI")],
          "an XAUUSD pick: the USD release is planned on XAUUSD, not EURUSD", [b.to_dict() for b in xau])
    check([(b.symbol, b.status, b.currency, b.event_id) for b in gj] == [("GBPJPY", "planned", "JPY", "ev-tankan")],
          "a JPY release with the pick GBPJPY is planned on GBPJPY, not USDJPY", [b.to_dict() for b in gj])
    picked = [b.id for b in xau + gj]
    plans = plans_in(sync(ws))
    check(set(picked) <= set(plans) and [plans[i]["symbol"] for i in picked] == ["XAUUSD", "GBPJPY"],
          "both plans go to the connector", plans)
    tick()
    msgs = sync(ws)
    check(all(row(i).status == "planned" and row(i).reason is None and cancel_msg(i) not in msgs for i in picked)
          and len(bursts_where(Burst.user_id == alice_id, Burst.event_time.in_([now + timedelta(minutes=m) for m in (7, 8)])))
          == 2, "the XAUUSD and GBPJPY bursts survive the next burst_tick, no second run", [row(i).to_dict() for i in picked])
    got = {b["id"]: b for b in client.get("/api/bursts").json()["bursts"]}
    check(all(got[i]["kind"] == "burst" and got[i]["analysis"] is None for i in picked)
          and all("kind" in b and "analysis" in b for b in got.values()),
          "GET /api/bursts: every burst carries kind (burst) and analysis", [got.get(i) for i in picked])
    # the picks cleared while these bursts wait: each stands on its own currency's release, which gets no second run
    client.put("/api/me/settings", headers=H, json={"burst": {"symbols": []}})
    tick()
    msgs = sync(ws)
    at78 = bursts_where(Burst.user_id == alice_id, Burst.event_time.in_([now + timedelta(minutes=m) for m in (7, 8)]))
    check(all(row(i).status == "planned" and row(i).reason is None and cancel_msg(i) not in msgs for i in picked)
          and len(at78) == 2, "picks cleared: the XAUUSD (USD) and GBPJPY (JPY) bursts stand, their releases get no "
          "second run on EURUSD or USDJPY", [b.to_dict() for b in at78])
    client.put("/api/me/settings", headers=H, json={"burst": {"symbols": ["XAUUSD", "GBPJPY"]}})
    for i in picked:
        r = client.post(f"/api/bursts/{i}/cancel", headers=H)
        check(r.status_code == 200 and r.json()["status"] == "cancelled", f"cancel the picked burst {i}", err(r))
    msgs = sync(ws)
    tick()
    check(all(cancel_msg(i) in msgs for i in picked) and len(bursts_where(
          Burst.user_id == alice_id, Burst.event_time.in_([now + timedelta(minutes=m) for m in (7, 8)]))) == 2,
          "a cancelled pick stands: no other pair is planned at that release", msgs)
    # the picks move to another pair of the USD list: the release keeps its cancelled XAUUSD burst, in the list as in
    # the engine (a burst that ended is known with its currency, not only by its pair)
    client.put("/api/me/settings", headers=H, json={"burst": {"symbols": ["GBPUSD"]}})
    up = [(u["symbol"], u["time"]) for u in client.get("/api/bursts").json()["upcoming"]]
    check(at(7) not in [t for _, t in up], "picks changed to GBPUSD: upcoming has nothing at the release whose "
          "XAUUSD pick was cancelled", up)
    tick()
    msgs = sync(ws)
    check([b.symbol for b in bursts_where(Burst.user_id == alice_id, Burst.event_time == now + timedelta(minutes=7))]
          == ["XAUUSD"] and "GBPUSD" not in [p["symbol"] for p in plans_in(msgs).values()],
          "... nor does burst_tick plan one there: the list and the engine agree", msgs)
    with SessionLocal() as db:                         # these two releases leave the calendar, the picks are reset
        for i in ("ev-ism", "ev-tankan"):
            db.delete(db.get(CalendarEvent, i))
        db.commit()
    r = client.put("/api/me/settings", headers=H, json={"burst": {"symbols": [], "currencies": []}})
    check(r.json()["settings"]["burst"] == {**DEFAULT_USER_SETTINGS["burst"], "enabled": True},
          "picks reset: the burst settings are the defaults again", r.json()["settings"]["burst"])
    sync(ws)

    # ── reconcile: a release leaves while another currency's release stays at that minute ──
    t14 = now + timedelta(minutes=14)
    add(ev("ev-us14", t14, "USD", "Core Retail Sales m/m"), ev("ev-jp14", t14, "JPY", "Tertiary Industry Activity m/m"))
    with SessionLocal() as db:
        live14 = engine.live_releases(db, {t14})
    check(live14 == {t14: {"USD", "GBP", "JPY"}},
          "live_releases: every currency released at that minute, also those sharing a pair (GBPUSD, USDJPY, GBPJPY)",
          live14)
    tick()
    at14 = {b.symbol: b for b in bursts_where(Burst.user_id == alice_id, Burst.event_time == t14)}
    check({s: (b.status, b.currency) for s, b in at14.items()} == {"EURUSD": ("planned", "USD"),
          "GBPUSD": ("planned", "GBP"), "USDJPY": ("planned", "JPY")},
          "USD, GBP (ev-gb) and JPY in one minute, no picks: each on its own pair", [b.to_dict() for b in at14.values()])
    gu, uj, eu = (at14.get(s) for s in ("GBPUSD", "USDJPY", "EURUSD"))
    ws.send_json({"type": "burst_update", "id": gu.id, "status": "scheduled"})
    sync(ws)
    with SessionLocal() as db:                         # the GBP and JPY releases move away, USD keeps 14 min
        for i in ("ev-gb", "ev-jp14"):
            db.delete(db.get(CalendarEvent, i))
        db.commit()
    tick()
    msgs = sync(ws)
    gu, uj, eu = row(gu.id), row(uj.id), row(eu.id)
    check((gu.status, gu.reason) == ("scheduled", "event rescheduled") and cancel_msg(gu.id) in msgs
          and (uj.status, uj.reason) == ("cancelled", "event rescheduled"),
          "GBPUSD (GBP) and USDJPY (JPY) bursts whose releases left are cancelled: the USD release that stays at that "
          "minute does not keep them (as in v1.2.0)", (gu.to_dict(), uj.to_dict(), msgs))
    check((eu.status, eu.reason) == ("planned", None) and cancel_msg(eu.id) not in msgs,
          "... the EURUSD burst of the USD release stands", eu.to_dict())
    ws.send_json({"type": "burst_update", "id": gu.id, "status": "cancelled"})
    client.post(f"/api/bursts/{eu.id}/cancel", headers=H)
    sync(ws)
    check(row(gu.id).status == row(eu.id).status == "cancelled", "both ended (the connector's report, the user's cancel)")

    # ── reconcile with picks: a release leaves, another currency's release whose list has the picked pair stays ──
    t6 = now + timedelta(minutes=6)
    client.put("/api/me/settings", headers=H, json={"burst": {"symbols": ["GBPJPY"]}})
    add(ev("ev-gb6", t6, "GBP", "GDP m/m"))
    tick()
    add(ev("ev-jp6", t6, "JPY", "Household Spending y/y"))
    tick()
    at6 = bursts_where(Burst.user_id == alice_id, Burst.event_time == t6)
    check([(b.symbol, b.currency, b.status) for b in at6] == [("GBPJPY", "GBP", "planned")],
          "picks [GBPJPY]: the GBP release on GBPJPY; a JPY release joining that minute shares it",
          [b.to_dict() for b in at6])
    with SessionLocal() as db:                         # the GBP release moves away, the JPY one stays
        db.delete(db.get(CalendarEvent, "ev-gb6"))
        db.commit()
    tick()
    tick()
    msgs = sync(ws)
    got = [(row(b.id).status, row(b.id).reason, cancel_msg(b.id) in msgs) for b in at6]
    check(got == [("planned", None, False)] and len(bursts_where(Burst.user_id == alice_id, Burst.event_time == t6)) == 1,
          "... the GBPJPY burst stands on the JPY release (its pair picked), no other run", got)
    for b in at6:
        client.post(f"/api/bursts/{b.id}/cancel", headers=H)
    with SessionLocal() as db:
        db.delete(db.get(CalendarEvent, "ev-jp6"))
        db.commit()
    client.put("/api/me/settings", headers=H, json={"burst": {"symbols": []}})
    sync(ws)

    # ── reconcile: missed release, no final report, no stepping back ──
    params = B.params_for({**DEFAULT_USER_SETTINGS, "burst": V120_BURST}, 40)     # a v1.2.0 user's: Y = X
    mk = lambda sym, secs, status="planned": Burst(user_id=alice_id, event_title="Test burst", symbol=sym, test=True,  # noqa: E731
                                                   event_time=utcnow() + timedelta(seconds=secs), status=status,
                                                   params=params)
    past = add(mk("USDCHF", -1))
    stuck = add(mk("USDCAD", -(90 + 30 + 301), "armed"))
    stuck_open = add(mk("AUDUSD", -(90 + 30 + 301), "triggered"))
    alive = add(mk("USDJPY", -(90 + 30 + 200), "triggered"))
    trade = lambda ticket, status, pnl, bid: UserTrade(user_id=alice_id, ticket=ticket, symbol="USDCAD", side="buy",  # noqa: E731
                                                       lots=0.2, entry_price=1.38, status=status, pnl=pnl, burst_id=bid)
    add(trade(7101, "closed", 12.5, stuck.id), trade(7102, "closed", -3.25, stuck.id), trade(7103, "open", None, stuck.id),
        trade(7104, "open", None, stuck_open.id))
    tick()
    check((row(stuck_open.id).status, row(stuck_open.id).pnl) == ("error", None),
          "... none of its trades closed: no P&L yet (not 0)", row(stuck_open.id).to_dict())
    check((row(past.id).status, row(past.id).reason) == ("skipped", "connector was offline"),
          "planned and the release passed: skipped", row(past.id).to_dict())
    check((row(stuck.id).status, row(stuck.id).reason, row(stuck.id).pnl)
          == ("error", "no final report from the connector", 9.25),
          "no final report long after the release: error, with the P&L of its closed trades", row(stuck.id).to_dict())
    check(row(alive.id).status == "triggered", "a running burst within expire + hold + 300 s is left alone")
    ws.send_json({"type": "burst_update", "id": alive.id, "status": "armed"})
    ws.send_json({"type": "burst_update", "id": alive.id, "status": "scheduled"})
    sync(ws)
    check(row(alive.id).status == "triggered", "a late lower status never moves a burst back")
    ws.send_json({"type": "burst_update", "id": alive.id, "status": "expired"})
    sync(ws)
    check(row(alive.id).status == "expired", "expired is a final report")

    # ── after the timeout: late closes, trades booked from their close, the runner's final report wins ──
    def trades_of(bid):
        with SessionLocal() as db:
            return {t.ticket: t for t in db.scalars(select(UserTrade).where(UserTrade.burst_id == bid))}
    closed = lambda ticket, pnl, **kw: {"type": "trade_closed", "trade": {  # noqa: E731
        "ticket": ticket, "exit_price": 1.381, "pnl": pnl, "closed_at": "2026-10-28T18:05:00", **kw}}
    ws.send_json(closed(7103, 2.0))
    ws.send_json(closed(7105, -1.0, burst_id=stuck.id, symbol="USDCAD", side="buy", lots=0.2, entry_price=1.38,
                        demo=True, opened_at="2026-10-28T18:00:01"))
    ws.send_json(closed(7106, 5.0, symbol="USDCAD", side="buy", lots=0.2))                    # no burst: not ours
    sync(ws)
    got = trades_of(stuck.id)
    t = got.get(7105)
    check(t is not None and (t.status, t.side, t.lots, t.entry_price, t.pnl, t.symbol) == ("closed", "buy", 0.2, 1.38,
                                                                                        -1.0, "USDCAD"),
          "trade_closed for a ticket it never saw opened, with its burst: booked from what the close carries",
          t and t.to_dict())
    check((row(stuck.id).status, row(stuck.id).pnl) == ("error", 10.25),
          "late closes counted in the timed-out burst's P&L (12.5 - 3.25 + 2.0 - 1.0)", row(stuck.id).to_dict())
    with SessionLocal() as db:
        stray = db.scalar(select(UserTrade).where(UserTrade.ticket == 7106))
    check(stray is None, "trade_closed for an unknown ticket without a burst: ignored")
    bobs = add(Burst(user_id=bob_id, event_title="Bob's burst", symbol="EURUSD", test=True, event_time=utcnow(),
                     status="closed", params=params))
    ws.send_json(closed(7108, 5.0, burst_id=bobs.id, symbol="EURUSD", side="buy", lots=0.2))
    sync(ws)
    with SessionLocal() as db:
        stray = db.scalar(select(UserTrade).where(UserTrade.ticket == 7108))
    check(stray is None and not trades_of(bobs.id), "... or with another user's burst: ignored")
    ws.send_json({"type": "burst_update", "id": stuck.id, "status": "closed", "side": "buy", "orders": 2, "fills": 5,
                  "pnl": 10.25})
    msgs = sync(ws)
    s = row(stuck.id)
    check((s.status, s.reason, s.side, s.fills, s.orders, s.pnl) == ("closed", None, "buy", 5, 2, 10.25)
          and cancel_msg(stuck.id) not in msgs,
          "the runner's final report after the server's timeout replaces it (side, fills, P&L)", s.to_dict())
    ws.send_json({"type": "burst_update", "id": stuck.id, "status": "error", "reason": "late duplicate"})
    sync(ws)
    check((row(stuck.id).status, row(stuck.id).reason) == ("closed", None), "a final report is never replaced again")
    ws.send_json({"type": "burst_update", "id": stuck_open.id, "status": "triggered", "side": "sell"})
    msgs = sync(ws)
    check((row(stuck_open.id).status, row(stuck_open.id).side, row(stuck_open.id).reason) == ("error", None, B.TIMED_OUT)
          and cancel_msg(stuck_open.id) in msgs,
          "after the timeout a report that is not final changes nothing: the runner is told to stop",
          row(stuck_open.id).to_dict())
    own_error = add(mk("NZDUSD", -600, "error"))
    with SessionLocal() as db:
        db.get(Burst, own_error.id).reason = "RuntimeError: terminal disconnected"
        db.commit()
    ws.send_json({"type": "burst_update", "id": own_error.id, "status": "closed", "fills": 5})
    sync(ws)
    check((row(own_error.id).status, row(own_error.id).fills) == ("error", 0),
          "an error the runner reported itself is not replaced by a later report", row(own_error.id).to_dict())

    # ── welcome after a reconnect ──
    close(ws)
    soon = add(mk("GBPUSD", 2))
    later = add(mk("AUDUSD", 60, "scheduled"))
    r = client.post(f"/api/bursts/{later.id}/cancel", headers=H)
    check(r.status_code == 409 and r.json()["detail"] == "connector_offline",
          "cancel a scheduled burst while the connector is offline: 409", err(r))
    ws, welcome = connect(client, tokens["alice"])
    got = {p["id"]: p for p in welcome["bursts"]}
    check(later.id in got and soon.id not in got
          and all(set(p) == SENT_PLAN_KEYS and p["seconds_until"] > 3 for p in got.values()),
          "welcome: this user's planned/scheduled bursts more than 3 s ahead", list(got))
    check({"XAUUSD", "GBPJPY"} <= set(welcome["symbols"]), "welcome of a 1.3.0 connector: every pair the engine trades",
          welcome["symbols"])

    # ── a cancel survives a dead link ──
    r = client.post(f"/api/bursts/{later.id}/cancel", headers=H)
    check(r.status_code == 200 and cancel_msg(later.id) in sync(ws), "cancel a scheduled burst", err(r))
    close(ws)                                          # the cancel went into a link that died before the connector reported
    ws, welcome = connect(client, tokens["alice"])
    check(later.id not in {p["id"] for p in welcome["bursts"]}, "welcome does not replay a cancelled plan", welcome["bursts"])
    tick()
    msgs = sync(ws)
    tick()
    msgs += sync(ws)
    check(msgs.count(cancel_msg(later.id)) == 1, "after the reconnect burst_tick sends the cancel again, once", msgs)
    ws.send_json({"type": "burst_update", "id": later.id, "status": "cancelled"})
    sync(ws)
    check((row(later.id).status, row(later.id).reason) == ("cancelled", "cancelled by you"), "the connector confirms it")

    r = client.get("/api/bursts").json()
    times = [datetime.fromisoformat(b["event_time"][:-1]) for b in r["bursts"]]
    check(r["connector"] == {"online": True, "demo": True, "version": "1.3.0"}, "GET /api/bursts: connector state", r["connector"])
    check(times == sorted(times, reverse=True) and {b["user_id"] for b in r["bursts"]} == {alice_id}
          and len(r["bursts"]) == len(bursts_where(Burst.user_id == alice_id)), "GET /api/bursts: own bursts, newest first")
    tr = {x["ticket"]: x for x in client.get("/api/trades").json()["trades"]}
    check(tr.get(7001, {}).get("burst_id") == tid, "GET /api/trades shows burst_id")
    close(ws)

    _v130_flow(client, tick, founder, alice, alice_id, tokens["alice"])
    _rapid_flow(client, tick, founder)

    # ── calendar refresh: a release the feed no longer has is deleted ──
    d3 = now + timedelta(days=3)
    ff = lambda i, t: {"id": i, "time": t, "currency": "USD", "title": i, "impact": "Low", "forecast": "",  # noqa: E731
                       "previous": "", "actual": "", "source": "forexfactory"}
    week = [ff("ff-old", now - timedelta(hours=5)), ff("ff-a", d3), ff("ff-b", d3 + timedelta(hours=1)),
            ff("ff-c", d3 + timedelta(hours=2))]
    engine.save_calendar(week)
    add(ev("ev-manual", d3 + timedelta(hours=1), "USD", "Manual entry", "Low"))
    engine.save_calendar([week[1], ff("ff-b2", d3 + timedelta(hours=3)), week[3]])     # b moved: new id
    engine.save_calendar([])
    with SessionLocal() as db:
        ids = set(db.scalars(select(CalendarEvent.id).where(CalendarEvent.id.in_(
            ["ff-old", "ff-a", "ff-b", "ff-b2", "ff-c", "ev-manual"]))))
    check(ids == {"ff-old", "ff-a", "ff-b2", "ff-c", "ev-manual"},
          "save_calendar: a future release missing from the feed is deleted; past and other sources kept", ids)

    # ── deleting a user deletes their bursts ──
    use(client, founder)
    r = client.delete(f"/api/admin/users/{bob_id}", headers=H)
    check(r.status_code == 200 and not bursts_where(Burst.user_id == bob_id), "deleting a user deletes their bursts", err(r))

    # ── connector download bundles every bot module connector.py needs ──
    use(client, alice)
    r = client.get("/api/connector/download")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    names, missing, todo, seen = set(z.namelist()), [], ["fx-connector/connector.py"], set()
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        if name not in names:
            missing.append(name)
            continue
        for node in ast.walk(ast.parse(z.read(name))):
            mods = []
            if isinstance(node, ast.ImportFrom) and node.level and name.startswith("fx-connector/bot/"):
                mods = [f"bot.{node.module}"] if node.module else [f"bot.{a.name}" for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module == "bot":
                mods = [f"bot.{a.name}" for a in node.names]
            elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith("bot."):
                mods = [node.module]
            elif isinstance(node, ast.Import):
                mods = [a.name for a in node.names if a.name.startswith("bot.")]
            todo += [f"fx-connector/{m.replace('.', '/')}.py" for m in mods]
    check(r.status_code == 200 and not missing and {"fx-connector/bot/__init__.py", "fx-connector/connector.ini"} <= names,
          "connector zip bundles every bot module connector.py imports (followed through their imports)", missing)
    listed = {"fx-connector/" + f.removeprefix("connector/") for f in main.CONNECTOR_FILES}
    check(listed <= names and names - listed == {"fx-connector/connector.ini"},
          "connector zip: every file of CONNECTOR_FILES, plus the generated connector.ini", sorted(names ^ listed))
    check({"fx-connector/bot/hold.py", "fx-connector/bot/news_view.py"} <= names,
          "connector zip: v1.3.0's bot/hold.py and bot/news_view.py", sorted(names))
    check("bot/rapid.py" in main.CONNECTOR_FILES and "fx-connector/bot/rapid.py" in names,
          "connector zip: v1.3.0's bot/rapid.py (Rapid), listed in CONNECTOR_FILES", sorted(names))
    # unzipped as on the client's PC, the connector and every bundled bot module import from that folder alone
    # (-I: no PYTHONPATH, no current folder; MetaTrader5 stubbed; connector.ini is only read by main())
    unz = TMP / "fx-connector-unzipped"
    z.extractall(unz, [n for n in names if not n.endswith(".ini")])
    top = unz / "fx-connector"
    mods = sorted(n.removeprefix("fx-connector/").removesuffix(".py").replace("/", ".").removesuffix(".__init__")
                  for n in names if n.startswith("fx-connector/bot/") and n.endswith(".py"))
    code = ("import importlib, sys, types\n"
            "sys.modules['MetaTrader5'] = types.ModuleType('MetaTrader5')\n"
            f"sys.path.insert(0, {str(top)!r})\n"
            f"for m in ['connector'] + {mods!r}:\n"
            "    print(m, importlib.import_module(m).__file__)\n")
    p = subprocess.run([sys.executable, "-I", "-B", "-X", "utf8", "-c", code], cwd=unz, capture_output=True,
                       text=True, timeout=60)
    files = dict(line.split(" ", 1) for line in p.stdout.splitlines() if " " in line)
    check(p.returncode == 0 and set(files) == {"connector", *mods}
          and all(Path(f).resolve().is_relative_to(top.resolve()) for f in files.values()),
          f"the unzipped connector imports with its bundled {len(mods)} bot modules only", p.stderr[-2000:] or files)
    main.CONNECTOR_FILES.append("bot/not_there.py")
    try:
        r = client.get("/api/connector/download")
    finally:
        main.CONNECTOR_FILES.remove("bot/not_there.py")
    check(r.status_code == 500 and r.json()["detail"] == "connector_incomplete",
          "a listed connector file missing on the server: 500, not a zip that fails on the client's PC", err(r))


def _v130_flow(client, tick, founder, alice, alice_id, alice_token):
    """v1.3.0 on the server: a user saved by v1.2.0 on a 1.2.0 connector runs exactly as before; the new keys and the
    new pairs only ever reach a connector from 1.3.0; the pre-news context and analysis; the hold up to Y."""
    import server.engine as E
    from bot.scalp import DEFAULTS as SHORT_DEFAULTS
    from server.db import AIAnalysis, Signal, User
    from server.settings import DEFAULT_APP_SETTINGS

    # carol: an account saved by v1.2.0 (burst and short-term blocks without the v1.3.0 keys), on connector 1.2.0
    client.cookies.clear()
    r = client.post("/api/auth/register", headers=H, json={"email": "carol@test.local", "accept_terms": True,
                                                           "password": "carol-pass-1"})
    carol, carol_id = session_of(client), r.json()["id"]
    with SessionLocal() as db:
        db.get(User, carol_id).settings = {
            **{k: v for k, v in DEFAULT_USER_SETTINGS.items() if k not in ("burst", "short_term")},
            "burst": {**V120_BURST, "enabled": True}, "short_term": {**SHORT_DEFAULTS, "symbols": ["EURUSD", "XAUUSD"]}}
        db.commit()
    use(client, carol)
    carol_token = client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"]
    cw, welcome = connect(client, carol_token, version="1.2.0")
    check(not {"XAUUSD", "GBPJPY"} & set(welcome["symbols"]) and "EURUSD" in welcome["symbols"]
          and welcome["settings"]["short_term"]["symbols"] == ["EURUSD"]
          and "max_hold_seconds" not in welcome["settings"]["short_term"],
          "welcome of a 1.2.0 connector: no XAUUSD / GBPJPY, in the engine's pairs nor in its short-term pairs",
          (welcome["symbols"], welcome["settings"]["short_term"]))
    aw, _ = connect(client, alice_token)

    # ── signals on the new pairs ──
    gold = Signal(mode="trend", symbol="XAUUSD", side="buy", timeframe="H1", price=2000.0, sl=1990.0, tp=2020.0)
    eu = Signal(mode="trend", symbol="EURUSD", side="sell", timeframe="H1", price=1.1, sl=1.101, tp=1.098)
    client.portal.call(engine._publish, gold)
    client.portal.call(engine._publish, eu)
    got = ([m["signal"]["symbol"] for m in sync(cw) if m["type"] == "signal"],
           [m["signal"]["symbol"] for m in sync(aw) if m["type"] == "signal"])
    check(got == (["EURUSD"], ["XAUUSD", "EURUSD"]), "a signal on XAUUSD goes to connectors from 1.3.0 only", got)
    r = client.post(f"/api/signals/{gold.id}/execute", headers=H)
    check(r.status_code == 409 and r.json()["detail"] == "connector_outdated",
          "execute an XAUUSD signal on connector 1.2.0: 409 connector_outdated", err(r))
    use(client, alice)
    r = client.post(f"/api/signals/{gold.id}/execute", headers=H)
    check(r.status_code == 200 and any(m["type"] == "execute" for m in sync(aw)), "... on 1.3.0 it is sent", err(r))

    # ── a release: the v1.2.0 user as before, the new user with the pre-news context ──
    # half a minute off the whole minutes the calendar checks above used: their `now` can be this very second, and a
    # release here must never land on one of theirs (t + 2 min on the GBPJPY run of t6)
    t = utcnow().replace(microsecond=0) + timedelta(minutes=4, seconds=30)
    pce = lambda i, at, title: CalendarEvent(id=i, time=at, currency="USD", title=title, impact="High",  # noqa: E731
                                             source="test", forecast="0.3%", previous="0.2%")
    # the newest AI analysis counts, wherever its row is: an older one is saved before it and another after it
    ai = lambda at, eu, uj: AIAnalysis(kind="news", model="test", created_at=at, result={"biases": [  # noqa: E731
        {"symbol": "EURUSD", "direction": eu[0], "confidence": eu[1]},
        {"symbol": "USDJPY", "direction": uj[0], "confidence": uj[1]}]})
    add(pce("ev-pce", t, "Core PCE Price Index m/m"), ai(utcnow() - timedelta(hours=1), ("buy", 70), ("sell", 80)))
    add(ai(utcnow(), ("sell", 60), ("buy", 50)))
    add(ai(utcnow() - timedelta(minutes=30), ("buy", 90), ("sell", 40)))
    # dave: a new user whose burst picks USDJPY, a second pair at the same release (each pair its own context)
    client.cookies.clear()
    client.post("/api/auth/register", headers=H, json={"email": "dave@test.local", "accept_terms": True,
                                                       "password": "dave-pass-1"})
    use(client, session_of(client))
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": True, "symbols": ["USDJPY"]}})
    dw, _ = connect(client, client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"])
    tick()
    cb = bursts_where(Burst.user_id == carol_id, Burst.event_time == t)
    ab = bursts_where(Burst.user_id == alice_id, Burst.event_time == t)
    check(len(cb) == 1 and cb[0].status == "planned" and cb[0].symbol == "EURUSD"
          and {k: cb[0].params[k] for k in V120_PARAMS} == V120_PARAMS
          and {k: v for k, v in cb[0].params.items() if k not in V120_PARAMS} == LEGACY_EXTRA,
          "a v1.2.0 user on connector 1.2.0: planned as in v1.2.0, with v1.2.0's params", [b.to_dict() for b in cb])
    check(bursts_where(Burst.user_id == carol_id) and not bursts_where(Burst.user_id == carol_id,
                                                                       Burst.reason == B.OUTDATED),
          "... none of its bursts is skipped as outdated")
    cplans, aplans = plans_in(sync(cw)), plans_in(sync(aw))
    check(cb and cb[0].id in cplans and cplans[cb[0].id]["params"] == cb[0].params and cplans[cb[0].id]["kind"] == "burst",
          "... and its plan goes to its connector", cplans.get(cb[0].id if cb else None))
    ctx = aplans.get(ab[0].id, {}).get("context") if ab else None
    check(len(ab) == 1 and ab[0].params["sl_unit"] == "pct" and isinstance(ctx, dict)
          and ctx["title"] == "Core PCE Price Index m/m" and ctx["expect"] < 0 and ctx["ai"] == -0.6,
          "a new user on 1.3.0: the plan carries the release's context (a higher US inflation forecast: EURUSD down; "
          "the AI's sell 60 %)", ctx)
    dp = [p for p in plans_in(sync(dw)).values() if p["event_time"] == t.isoformat() + "Z"]
    dctx = dp[0].get("context") if dp else None
    check(len(dp) == 1 and dp[0]["symbol"] == "USDJPY" and isinstance(dctx, dict) and dctx["expect"] > 0
          and dctx["ai"] == 0.5, "twin runs at one release (alice on EURUSD, dave on USDJPY): each pair its own "
          "context (USDJPY up; the newest analysis' buy 50 %)", dp)
    close(dw)
    real, engine_log = E.context_for, logging.getLogger("engine")
    E.context_for = lambda *a, **k: 1 / 0                     # noqa: E731
    engine_log.disabled = True
    try:
        tick()
    finally:
        E.context_for, engine_log.disabled = real, False
    aplans = plans_in(sync(aw))
    check(ab and ab[0].id in aplans and "context" not in aplans[ab[0].id] and row(ab[0].id).status == "planned",
          "context_for failing: the plan still goes, without a context", aplans.get(ab[0].id if ab else None))
    # the query of the newest AI analysis failing (a locked database): burst_contexts gives no run a context and does
    # not raise; the tick still sends its plans (without one) and its cancels
    real_select = E.select

    def no_ai(*a, **k):
        if a and a[0] is AIAnalysis:
            raise RuntimeError("database is locked")
        return real_select(*a, **k)
    dropped = add(Burst(user_id=alice_id, event_title="Test burst", symbol="AUDUSD", test=True, status="scheduled",
                        event_time=utcnow() + timedelta(minutes=8), reason=B.USER_CANCEL,
                        params=B.params_for(DEFAULT_USER_SETTINGS, 40)))
    E.select, engine_log.disabled = no_ai, True
    try:
        try:
            alone = engine.burst_contexts([SimpleNamespace(id=-5, test=False, event_id=None, event_time=t,
                                                           symbol="EURUSD")], utcnow())
        except Exception as e:
            alone = e
        try:
            ticked = tick()
        except Exception as e:
            ticked = e
    finally:
        E.select, engine_log.disabled = real_select, False
    msgs = sync(aw)
    aplans = plans_in(msgs)
    check(alone == {}, "burst_contexts: the newest AI analysis unreadable: no context for any run, no exception", alone)
    check(ticked is None and ab and ab[0].id in aplans and "context" not in aplans[ab[0].id]
          and cancel_msg(dropped.id) in msgs, "... and burst_tick still sends the plan (without a context) and the "
          "cancel of a run cancelled by its user", (ticked, aplans.get(ab[0].id if ab else None), msgs))
    aw.send_json({"type": "burst_update", "id": dropped.id, "status": "cancelled"})
    sync(aw)
    check(row(dropped.id).status == "cancelled", "... which its connector then confirms", row(dropped.id).to_dict())

    # ── a v1.3.0 key on connector 1.2.0 ──
    use(client, carol)
    r = client.put("/api/me/settings", headers=H, json={"burst": {"sl_unit": "pct"}})
    pushed = [m for m in sync(cw) if m["type"] == "settings"]
    check((r.json()["settings"]["burst"]["sl_unit"], r.json()["settings"]["burst"]["sl_money"]) == ("pct", 2.0)
          and pushed and not {"XAUUSD", "GBPJPY"} & set(pushed[-1]["symbols"])
          and pushed[-1]["settings"]["short_term"]["symbols"] == ["EURUSD"],
          "PUT settings: the push to a 1.2.0 connector leaves out XAUUSD / GBPJPY",
          pushed and (pushed[-1]["symbols"], pushed[-1]["settings"]["short_term"]["symbols"]))
    add(pce("ev-pce2", t + timedelta(minutes=1), "Personal Spending m/m"))
    tick()
    c2 = bursts_where(Burst.user_id == carol_id, Burst.event_time == t + timedelta(minutes=1))
    check(len(c2) == 1 and (c2[0].status, c2[0].reason) == ("skipped", B.OUTDATED) and c2[0].id not in plans_in(sync(cw)),
          "the stop in % on connector 1.2.0: the burst is skipped as outdated, never sent", [b.to_dict() for b in c2])
    r = client.put("/api/me/settings", headers=H, json={"burst": {"sl_unit": "pips"}})
    check(r.json()["settings"]["burst"]["sl_unit"] == "pips", "back to the stop in pips")
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "XAUUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "connector_outdated",
          "test burst on XAUUSD with connector 1.2.0: 400 connector_outdated", err(r))
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD"})
    tp = plans_in(sync(cw)).get(r.json().get("id"), {})
    check(r.status_code == 200 and tp.get("params") == {**V120_PARAMS, "lead_seconds": 15, **LEGACY_EXTRA},
          "a v1.2.0 user's test burst on connector 1.2.0: v1.2.0's params", (r.text, tp))
    client.post(f"/api/bursts/{r.json().get('id')}/cancel", headers=H)
    client.put("/api/me/settings", headers=H, json={"burst": {"hold_seconds": 45}, "short_term": {"hold_seconds": 45}})
    client.put("/api/me/settings", headers=H, json={"burst": {"hold_seconds": 30}, "notify_browser": True})
    with SessionLocal() as db:
        stored = db.get(User, carol_id).settings
    check("max_hold_seconds" not in stored["burst"] and "max_hold_seconds" not in stored["short_term"]
          and stored["burst"]["hold_seconds"] == 30, "PUTs never store a Y the user did not have (burst, short-term)",
          stored["burst"])
    sync(cw)

    # ── the new user's keys through PUT /api/me/settings ──
    use(client, alice)
    put = lambda body: client.put("/api/me/settings", headers=H, json={"burst": body})    # noqa: E731
    got = [put(body).json()["settings"]["burst"]["max_hold_seconds"] for body in
           ({"hold_seconds": 200}, {"max_hold_seconds": 100}, {"hold_seconds": 30, "max_hold_seconds": 120})]
    check(got == [200, 200, 120], "PUT burst: Y stays >= X (an X above Y raises it, a Y under X is raised to X)", got)
    got = [put({"sl_unit": u}).json()["settings"]["burst"] for u in ("usd", "pct")]
    check([(g["sl_unit"], g["sl_money"]) for g in got] == [("usd", 100.0), ("pct", 2.0)],
          "PUT burst: a new unit without sl_money starts from its default (100 $, 2 %)", got)
    for body, code in (({"sl_unit": "lots"}, "invalid_unit"), ({"analysis": "false"}, "invalid_analysis"),
                       ({"max_hold_seconds": "x"}, "invalid_number"), ({"lots": None}, "invalid_number")):
        r = put(body)
        check(r.status_code == 400 and r.json()["detail"] == code, f"PUT burst {body} -> 400 {code}", err(r))

    def kept():
        with SessionLocal() as db:
            return db.get(User, alice_id).settings["burst"]
    r = put({"hold_seconds": 40})
    check(r.status_code == 200 and (kept()["hold_seconds"], kept()["max_hold_seconds"]) == (40, 120),
          "PUT burst without Y: the stored Y stays", kept())
    r = put({"max_hold_seconds": None})
    web = r.json()["settings"]["burst"]
    check(r.status_code == 200 and "max_hold_seconds" not in kept() and "max_hold_seconds" not in web
          and kept()["hold_seconds"] == 40 and B.params_for({**DEFAULT_USER_SETTINGS, "burst": kept()}, 40)
          ["max_hold_seconds"] == 40,
          "PUT burst with Y null: removed from the stored block and the web app's, the plans' Y follows X (v1.2.0's "
          "hold)", (kept(), web))
    r = put({"hold_seconds": 50})
    check(r.status_code == 200 and "max_hold_seconds" not in kept() and kept()["hold_seconds"] == 50,
          "... a PUT that sends none adds none back", kept())
    r = put({"hold_seconds": 30, "max_hold_seconds": 120})
    check(r.status_code == 200 and (kept()["hold_seconds"], kept()["max_hold_seconds"]) == (30, 120), "... set again",
          kept())
    sync(aw)

    # ── the engine's timeout counts the hold up to Y ──
    newp = B.params_for(DEFAULT_USER_SETTINGS, 40)
    mk = lambda sym, secs, status: Burst(user_id=alice_id, event_title="Test burst", symbol=sym, test=True,  # noqa: E731
                                         event_time=utcnow() + timedelta(seconds=secs), status=status, params=newp)
    gone, held = add(mk("NZDUSD", -(90 + 120 + 301), "armed")), add(mk("USDCHF", -(90 + 30 + 301), "triggered"))
    tick()
    check((row(gone.id).status, row(gone.id).reason) == ("error", B.TIMED_OUT) and row(held.id).status == "triggered",
          "timeout = expire + max(X, Y) + 300 s: a run holding up to Y = 120 s is not given up at expire + X + 300",
          (row(gone.id).to_dict(), row(held.id).to_dict()))
    aw.send_json({"type": "burst_update", "id": held.id, "status": "expired"})

    # ── the pre-news analysis a connector reports: kept once ──
    pid, look = ab[0].id, {"side": "sell", "score": -0.62, "parts": {"h1": -0.4, "expect": -0.3}}
    for m in ({"id": pid, "status": "scheduled", "analysis": look}, {"id": pid, "status": "armed", "analysis": {"x": 1}},
              {"id": held.id, "analysis": {"note": "x" * 2100}}, {"id": held.id, "analysis": {"score": float("nan")}},
              {"id": held.id, "analysis": ["side", "buy"]}):
        aw.send_json({"type": "burst_update", **m})
    cw.send_json({"type": "burst_update", "id": held.id, "analysis": {"side": "buy"}})     # not carol's burst
    sync(aw)
    sync(cw)
    check(row(pid).analysis == look and row(pid).status == "armed", "burst_update: the first analysis is kept, not a later "
          "one", row(pid).to_dict())
    check(row(held.id).analysis is None, "an analysis over 2000 characters, with NaN, not an object, or from another "
          "user's connector: not kept", row(held.id).analysis)
    bw = ws_open(client, "/ws")                                # alice's browser tab
    check(bw.receive_json()["type"] == "hello", "browser socket connected (alice, for the late analysis)")
    aw.send_json({"type": "burst_update", "id": held.id, "analysis": {"side": "both", "why": "unclear"}})
    told = sync(aw)
    check(row(held.id).analysis == {"side": "both", "why": "unclear"} and row(held.id).status == "expired",
          "... one that fits is kept, also on a run that already ended", row(held.id).to_dict())
    msgs = sync(bw)
    seen = [m["burst"] for m in msgs if m["type"] == "burst" and m["burst"]["id"] == held.id]
    check([(b["status"], b["analysis"]) for b in seen] == [("expired", {"side": "both", "why": "unclear"})]
          and not [m for m in msgs if m["type"] == "connector_log"] and cancel_msg(held.id) not in told,
          "... and the browsers get the ended run with it: no status change, no log line, no cancel", (msgs, told))
    close(bw)

    # ── a test burst started for a calendar event reads its forecast (carol: alice's `soon` test may still wait) ──
    use(client, carol)
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD", "event_id": "nope"})
    check(r.status_code == 400 and r.json()["detail"] == "invalid_event", "test burst for an unknown event: 400", err(r))
    r = client.post("/api/bursts/test", headers=H, json={"symbol": "EURUSD", "event_id": "ev-pce"})
    tp = plans_in(sync(cw)).get(r.json().get("id"), {})
    check(r.status_code == 200 and r.json()["event_id"] == "ev-pce" and tp.get("context") == ctx,
          "a test burst for an event: the event's context, as its release's plan has it", (r.text, tp.get("context")))
    client.post(f"/api/bursts/{r.json().get('id')}/cancel", headers=H)
    sync(cw)

    # ── the admin changes the pairs: each connector gets those its version trades ──
    use(client, founder)
    full = list(DEFAULT_APP_SETTINGS["symbols"])
    client.put("/api/admin/engine", headers=H, json={"symbols": [s for s in full if s != "USDCHF"]})
    pc, pa = ([m["symbols"] for m in sync(x) if m["type"] == "settings"] for x in (cw, aw))
    check(pc == [[s for s in full if s not in ("USDCHF", "XAUUSD", "GBPJPY")]] and pa == [[s for s in full if s != "USDCHF"]],
          "admin pair change: connector 1.2.0 gets no XAUUSD / GBPJPY, 1.3.0 every pair", (pc, pa))
    client.put("/api/admin/engine", headers=H, json={"symbols": full})
    sync(cw)
    sync(aw)

    # ── a test signal on a pair added in v1.3.0: never to an older connector ──
    use(client, carol)
    client.put("/api/me/settings", headers=H, json={"execution": "auto"})
    candles = {s: engine.candles.get(s) for s in ("EURUSD", "XAUUSD")}
    engine.candles.update({s: engine.feed.candles(s, "H1", 200) for s in candles})   # a refusal: for the pair alone
    sent = lambda ws: [m["signal"]["symbol"] for m in sync(ws) if m["type"] == "signal"]    # noqa: E731
    sent(cw)
    r = client.post("/api/test-signal", headers=H, json={"symbol": "XAUUSD"})
    check(r.status_code == 400 and r.json()["detail"] == "connector_outdated" and not sent(cw),
          "test signal on XAUUSD with connector 1.2.0: 400 connector_outdated, nothing sent", err(r))
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"symbols": ["XAUUSD", *[s for s in full if s != "XAUUSD"]]})
    use(client, carol)
    r = client.post("/api/test-signal", headers=H, json={})
    check(r.status_code == 200 and r.json()["signal"]["symbol"] == "EURUSD" and sent(cw) == ["EURUSD"],
          "... one without a pair takes the first the connector trades (the admin's list starts with XAUUSD): EURUSD",
          r.text)
    client.post("/api/test-signal/close", headers=H)
    close(cw)
    cw, _ = connect(client, carol_token)
    r = client.post("/api/test-signal", headers=H, json={"symbol": "XAUUSD"})
    check(r.status_code == 200 and sent(cw) == ["XAUUSD"], "... on connector 1.3.0 the XAUUSD test signal is sent", err(r))
    client.post("/api/test-signal/close", headers=H)
    for s, df in candles.items():
        if df is None:
            engine.candles.pop(s, None)
        else:
            engine.candles[s] = df
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"symbols": full})
    sync(cw)
    sync(aw)

    # ── a connector downgraded with a plan it cannot run waiting ──
    # carol (on 1.3.0 since the test signal) picks XAUUSD: a plan with v1.2.0's params that 1.2.0 cannot run either
    use(client, carol)
    client.put("/api/me/settings", headers=H, json={"burst": {"symbols": ["XAUUSD"]}})
    t3 = t + timedelta(minutes=2)
    add(pce("ev-pce3", t3, "PCE Price Index m/m"))
    tick()
    a3 = bursts_where(Burst.user_id == alice_id, Burst.event_time == t3)
    c3 = bursts_where(Burst.user_id == carol_id, Burst.event_time == t3)
    check(len(c3) == 1 and c3[0].symbol == "XAUUSD" and c3[0].status == "planned"
          and B.needs_version(c3[0].params) == B.MIN_CONNECTOR and c3[0].id in plans_in(sync(cw)),
          "carol's burst on XAUUSD (connector 1.3.0): planned with v1.2.0's params, and sent", [b.to_dict() for b in c3])
    close(aw)
    aw, welcome = connect(client, alice_token, version="1.2.0")
    sigs = {s["symbol"] for s in welcome["open_signals"]}
    check(len(a3) == 1 and a3[0].id not in {p["id"] for p in welcome["bursts"]}
          and not {"XAUUSD", "GBPJPY"} & set(welcome["symbols"]) and "EURUSD" in sigs and "XAUUSD" not in sigs,
          "welcome of a downgraded connector (1.2.0): no plan it cannot run, no new pairs, no XAUUSD signal",
          (welcome["bursts"], welcome["symbols"], sigs))
    close(cw)
    cw, welcome = connect(client, carol_token, version="1.2.0")
    check(c3 and c3[0].id not in {p["id"] for p in welcome["bursts"]},
          "... nor, for carol, her XAUUSD plan: v1.2.0's params, but a pair 1.2.0 cannot trade", welcome["bursts"])
    tick()
    check(a3 and (row(a3[0].id).status, row(a3[0].id).reason) == ("skipped", B.OUTDATED)
          and a3[0].id not in plans_in(sync(aw)), "... and burst_tick skips that plan as outdated",
          a3 and row(a3[0].id).to_dict())
    check(c3 and (row(c3[0].id).status, row(c3[0].id).reason) == ("skipped", B.OUTDATED)
          and c3[0].id not in plans_in(sync(cw)), "... carol's XAUUSD plan too", c3 and row(c3[0].id).to_dict())
    # a release under 3 s away when its run is planned: the delivery loop leaves it alone that tick, so the run is
    # marked outdated as it is made (else it would wait as planned and end as "connector was offline")
    t5 = utcnow() + timedelta(seconds=2.5)
    add(pce("ev-pce5", t5, "Core PCE Price Index m/m"))
    try:
        tick()
        a5 = bursts_where(Burst.user_id == alice_id, Burst.event_time == t5)
        c5 = bursts_where(Burst.user_id == carol_id, Burst.event_time == t5)
    finally:
        with SessionLocal() as db:                       # never planned by a later burst_tick
            db.delete(db.get(CalendarEvent, "ev-pce5"))
            db.commit()
    sent5 = {**plans_in(sync(aw)), **plans_in(sync(cw))}
    check(len(a5) == 1 and len(c5) == 1 and c5[0].symbol == "XAUUSD"
          and [(b.status, b.reason) for b in a5 + c5] == [("skipped", B.OUTDATED)] * 2
          and not {a5[0].id, c5[0].id} & set(sent5),
          "a run planned under 3 s before its release on connector 1.2.0 (alice: the stop in %; carol: XAUUSD with "
          "v1.2.0's params): skipped as outdated in the tick that makes it", [b.to_dict() for b in a5 + c5])
    close(aw)
    aw, welcome = connect(client, alice_token)
    check("XAUUSD" in {s["symbol"] for s in welcome["open_signals"]}, "welcome of 1.3.0: the XAUUSD signal too")
    close(aw)
    close(cw)

    # ── carol's account as one from before the burst (no burst block stored) switches it on: saved as v1.2.0 ──
    with SessionLocal() as db:
        u = db.get(User, carol_id)
        u.settings = {k: v for k, v in u.settings.items() if k != "burst"}
        db.commit()
    use(client, carol)
    r = client.put("/api/me/settings", headers=H, json={"burst": {"enabled": True}})
    with SessionLocal() as db:
        stored = db.get(User, carol_id).settings["burst"]
    check(r.status_code == 200 and stored == {**B.legacy_burst(None), "enabled": True}
          and (stored["sl_unit"], stored["analysis"]) == ("pips", False) and "max_hold_seconds" not in stored,
          "PUT burst enabled with no stored block: the block saved is v1.2.0's (stop in pips, no analysis, no Y)",
          stored)
    client.put("/api/me/settings", headers=H, json={"burst": {"enabled": False}})

    # ── a run's context: only the releases of its minute that land on its pair ──
    # a CAD and a USD CPI in one minute, and a BOC speech: USD's list has no USDCAD, and a speech is no release a burst
    # trades, so a run on USDCAD reads the CAD CPI alone (and one on EURUSD the USD CPI alone); the two forecasts
    # pull USDCAD opposite ways, so either one leaking in shows
    t4 = t + timedelta(minutes=3)
    ev = lambda i, cur, title, fc, prev: CalendarEvent(id=i, time=t4, currency=cur, title=title,  # noqa: E731
                                                       impact="High", source="test", forecast=fc, previous=prev)
    evs = [ev("ev-ctx-cad", "CAD", "CPI m/m", "0.1%", "0.3%"), ev("ev-ctx-usd", "USD", "Core CPI m/m", "0.1%", "0.3%"),
           ev("ev-ctx-boc", "CAD", "BOC Gov Macklem Speaks", "", "")]
    add(*evs)
    try:
        runs = [SimpleNamespace(id=i, test=False, event_id=None, event_time=t4, symbol=s)
                for i, s in ((-1, "USDCAD"), (-2, "EURUSD"))]
        got = engine.burst_contexts(runs, utcnow())
        # one run whose context fails (the first, so a failure that ended the whole call would show) leaves the
        # others theirs
        whole = E.context_for
        E.context_for = lambda evs, sym, *a: 1 / 0 if sym == "USDCAD" else whole(evs, sym, *a)     # noqa: E731
        engine_log.disabled = True
        try:
            alone = engine.burst_contexts(runs, utcnow())
        finally:
            E.context_for, engine_log.disabled = whole, False
        # test runs started for one event on two pairs: each pair its own context, never the first one's
        twins = engine.burst_contexts([SimpleNamespace(id=i, test=True, event_id="ev-ctx-usd", event_time=t4, symbol=s)
                                       for i, s in ((-3, "EURUSD"), (-4, "USDJPY"))], utcnow())
    finally:
        with SessionLocal() as db:                       # never planned by a later burst_tick
            for e in evs:
                db.delete(db.get(CalendarEvent, e.id))
            db.commit()
    cad, usd = got.get(-1) or {}, got.get(-2) or {}
    want = {s: E.context_for([e], s)["expect"] for s, e in (("USDCAD", evs[0]), ("EURUSD", evs[1]))}
    check(cad.get("title") == "CPI m/m" and cad.get("expect") == want["USDCAD"] and want["USDCAD"] > 0
          and usd.get("title") == "Core CPI m/m" and usd.get("expect") == want["EURUSD"] and want["EURUSD"] > 0,
          "burst_contexts: a run reads only the releases that land on its pair (USDCAD: the CAD CPI, a lower "
          "forecast, up; EURUSD: the USD one), never a speech nor another currency's release of that minute",
          (got, want))
    check(set(alone) == {-2} and alone[-2] == usd, "burst_contexts: the USDCAD run's context failing, the EURUSD run "
          "still gets its own (the failure is that run's alone)", alone)
    pair = {s: E.context_for([evs[1]], s)["expect"] for s in ("EURUSD", "USDJPY")}
    check(set(twins) == {-3, -4} and twins[-3]["expect"] == pair["EURUSD"] > 0 > twins[-4]["expect"] == pair["USDJPY"],
          "burst_contexts: two test runs for one event, on EURUSD and USDJPY: each its pair's context (opposite signs)",
          (twins, pair))


def _rapid_flow(client, tick, founder):
    """Rapid (v1.3.0) on the server: its block through PUT /api/me/settings (clamped, exclusive with short-term, its
    pairs as short-term's), never sent to a connector before 1.3.0; its news runs planned before the classic burst,
    resent, stopped and timed out per kind; its trades booked as rapid; the rollback kit switching it off."""
    from server import auth
    from server.db import User
    from server.settings import DEFAULT_APP_SETTINGS

    put = lambda body: client.put("/api/me/settings", headers=H, json=body)       # noqa: E731
    trade = lambda ticket, **kw: {"ticket": ticket, "symbol": "EURUSD", "side": "buy", "lots": 0.1,  # noqa: E731
                                  "entry_price": 1.1, "sl": 1.099, "tp": None, "demo": True,
                                  "opened_at": utcnow().isoformat(), **kw}

    def stored(uid):
        with SessionLocal() as db:
            return db.get(User, uid).settings

    def pushed(ws):
        return [m["settings"] for m in sync(ws) if m["type"] == "settings"]

    # erin: a new user on connector 1.3.0; frank: one on connector 1.2.0
    for key in [k for k in auth._hits if k.startswith("register:")]:      # 5 sign-ups an hour from one address
        auth._hits.pop(key)
    acc = {}
    for name in ("erin", "frank"):
        client.cookies.clear()
        r = client.post("/api/auth/register", headers=H, json={"email": f"{name}@test.local", "accept_terms": True,
                                                               "password": f"{name}-pass-1"})
        tok = session_of(client)
        use(client, tok)
        acc[name] = (tok, r.json()["id"],
                     client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"])
    (erin, erin_id, erin_token), (frank, frank_id, frank_token) = acc["erin"], acc["frank"]
    use(client, erin)
    ew, welcome = connect(client, erin_token)
    web = client.get("/api/me/settings").json()["settings"]
    check(web["rapid"] == welcome["settings"]["rapid"] == DEFAULT_USER_SETTINGS["rapid"] and "rapid_note" not in web,
          "a new user: Rapid at its defaults (off), on the web and in the welcome of connector 1.3.0",
          (web.get("rapid"), welcome["settings"].get("rapid")))

    # ── PUT rapid: clamped; the codes of what cannot be used ──
    r = put({"rapid": {"enabled": True, "orders": 99, "max_runs": 9, "hold_seconds": 3, "max_hold_seconds": 1,
                       "risk_min": 9, "risk_max": 0.1, "tp_min_usd": 20, "tp_max_usd": 5,
                       "triggers": {"signals": False}, "symbols": ["EURUSD", "XXXYYY", "EURUSD"], "bogus": 1}})
    got = r.json()["settings"]["rapid"]
    want = {**DEFAULT_USER_SETTINGS["rapid"], "enabled": True, "orders": 40, "max_runs": 5, "hold_seconds": 5,
            "max_hold_seconds": 5, "risk_min": 5.0, "risk_max": 5.0, "tp_min_usd": 20.0, "tp_max_usd": 20.0,
            "triggers": {"signals": False, "news": True}, "symbols": ["EURUSD"]}
    p = pushed(ew)
    check(r.status_code == 200 and got == want and stored(erin_id)["rapid"] == want,
          "PUT rapid: every range, risk_max and tp_max raised to their minimum, Y to X, the triggers it leaves out "
          "kept, the pairs the engine trades only, unknown keys dropped", got)
    check(len(p) == 1 and p[0]["rapid"] == want, "a Rapid change reaches connector 1.3.0", p)
    r = put({"rapid": {"max_hold_seconds": None}})
    p = pushed(ew)
    check(r.status_code == 200 and "max_hold_seconds" not in stored(erin_id)["rapid"]
          and "max_hold_seconds" not in r.json()["settings"]["rapid"]
          and len(p) == 1 and "max_hold_seconds" not in p[0]["rapid"]
          and B.params_for(stored(erin_id), 40, "rapid")["max_hold_seconds"] == 5,
          "PUT rapid with Y null: removed (it follows X), on the web app and on connector 1.3.0", (stored(erin_id), p))
    put({"rapid": {"hold_seconds": 5}})
    check("max_hold_seconds" not in stored(erin_id)["rapid"], "... a PUT that sends none adds none back")
    put({"rapid": {"max_hold_seconds": 5}})
    sync(ew)
    check(stored(erin_id)["rapid"] == want, "... set again")
    for body, code in (({"rapid": "on"}, "invalid_rapid"), ({"rapid": {"orders": "many"}}, "invalid_number"),
                       ({"rapid": {"orders": 10**400}}, "invalid_number"),
                       ({"rapid": {"risk_unit": "eur"}}, "invalid_unit"), ({"rapid": {"sl_unit": "lots"}}, "invalid_unit"),
                       ({"rapid": {"symbols": "EURUSD"}}, "invalid_symbols"),
                       ({"rapid": {"triggers": "news"}}, "invalid_triggers"),
                       ({"rapid": {"triggers": {"news": "yes"}}}, "invalid_triggers"),
                       ({"rapid": {"enabled": "false"}}, "invalid_enabled"),
                       ({"rapid": {"analysis": 1}}, "invalid_analysis"),
                       ({"rapid": {"symbols": ["XXXYYY"]}}, "pairs_not_traded")):
        r = put(body)
        check(r.status_code == 400 and r.json().get("detail") == code, f"PUT {str(body)[:60]} -> 400 {code}", err(r))
    check(stored(erin_id)["rapid"] == want and not pushed(ew), "a refused Rapid PUT stores nothing and sends nothing")

    # ── Rapid and short-term exclude each other ──
    flags = lambda r: (r.json()["settings"]["short_term"]["enabled"],  # noqa: E731
                       r.json()["settings"]["rapid"]["enabled"])
    got = [flags(put(b)) for b in ({"short_term": {"enabled": True}}, {"rapid": {"enabled": True}},
                                   {"short_term": {"enabled": True}, "rapid": {"enabled": False}},
                                   {"rapid": {"enabled": True}, "short_term": {"hold_seconds": 20}})]
    p = [(s["short_term"]["enabled"], s["rapid"]["enabled"]) for s in pushed(ew)]
    check(got == [(True, False), (False, True), (True, False), (False, True)] and p == got,
          "switching short-term on switches Rapid off and the other way round; the connector gets each", (got, p))
    put({"rapid": {"enabled": False}})
    sync(ew)
    r = put({"rapid": {"enabled": True}, "short_term": {"enabled": True}})
    st = stored(erin_id)
    check(r.status_code == 400 and r.json().get("detail") == "rapid_excludes_short"
          and (st["short_term"]["enabled"], st["rapid"]["enabled"]) == (False, False) and not pushed(ew),
          "both switched on at once: 400 rapid_excludes_short, nothing stored or sent", err(r))
    put({"rapid": {"enabled": True}})
    sync(ew)

    # ── a connector before 1.3.0 never gets the rapid block ──
    use(client, frank)
    fw, fwel = connect(client, frank_token, version="1.2.0")
    r = put({"rapid": {"enabled": True, "max_runs": 1},
             "burst": {"enabled": True, "sl_unit": "pips", "analysis": False, "hold_seconds": 30,
                       "max_hold_seconds": 30, "symbols": ["EURUSD", "GBPUSD"]}})
    p = pushed(fw)
    check(not {"rapid", "rapid_note"} & set(fwel["settings"]) and r.json()["settings"]["rapid"]["enabled"] is True
          and len(p) == 1 and not {"rapid", "rapid_note"} & set(p[0]),
          "connector 1.2.0: no rapid block in its welcome nor in a push (the web app gets it)",
          (sorted(fwel["settings"]), p))

    # ── Rapid's pairs the engine no longer trades: as short-term's ──
    use(client, erin)
    put({"rapid": {"symbols": ["GBPUSD"]}})
    sync(ew)
    use(client, founder)
    full = list(DEFAULT_APP_SETTINGS["symbols"])
    client.put("/api/admin/engine", headers=H, json={"symbols": [s for s in full if s != "GBPUSD"]})
    p = pushed(ew)
    sync(fw)
    check(p and p[-1]["rapid"]["enabled"] is False and p[-1]["rapid"]["symbols"] == []
          and p[-1].get("rapid_note") == "pairs_gone",
          "admin removes Rapid's only pair: the connector gets Rapid off, with the note pairs_gone", p[-1:])
    use(client, erin)
    web = client.get("/api/me/settings").json()["settings"]
    check(web["rapid"]["enabled"] is True and web["rapid"]["symbols"] == [] and web.get("rapid_note") == "pairs_gone",
          "the web app gets the stored switch, no pairs and the note", (web.get("rapid"), web.get("rapid_note")))
    r = put({"rapid": {**web["rapid"], "hold_seconds": 40, "symbols_picked": False}})
    st = stored(erin_id)["rapid"]
    check(r.status_code == 200 and st["symbols"] == ["GBPUSD"] and st["enabled"] is True and st["hold_seconds"] == 40,
          "the web app's save (its empty pairs, none picked) keeps the stored pairs and switch", st)
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"symbols": full})
    p = pushed(ew)
    sync(fw)
    check(p and p[-1]["rapid"]["enabled"] is True and p[-1]["rapid"]["symbols"] == ["GBPUSD"]
          and "rapid_note" not in p[-1], "the pair back: the connector gets Rapid on that pair again", p[-1:])

    # ── a release: Rapid's runs first, then one classic run on a pair Rapid left ──
    use(client, erin)
    put({"rapid": {"max_runs": 2, "symbols": [], "triggers": {"news": True}},
         "burst": {"enabled": True, "symbols": ["EURUSD", "XAUUSD", "GBPUSD"]}})
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"burst": {"max_trades": 6}})
    with SessionLocal() as db:
        busy = set(db.scalars(select(CalendarEvent.time)))
    base = utcnow().replace(microsecond=0) + timedelta(minutes=13)
    tr = next(t for t in (base + timedelta(seconds=k) for k in range(7, 60)) if t not in busy)  # no other release then
    add(CalendarEvent(id="ev-rapid", time=tr, currency="USD", title="Retail Sales m/m", impact="High", source="test",
                      forecast="0.5%", previous="0.2%"))
    sync(ew)
    sync(fw)
    tick()
    at_tr = lambda uid: bursts_where(Burst.user_id == uid, Burst.event_time == tr)     # noqa: E731
    er, fr = at_tr(erin_id), at_tr(frank_id)
    with SessionLocal() as db:
        es = db.get(User, erin_id).merged_settings()
    eplans, fplans = plans_in(sync(ew)), plans_in(sync(fw))
    check([(b.symbol, b.kind, b.status) for b in er]
          == [("EURUSD", "rapid", "planned"), ("XAUUSD", "rapid", "planned"), ("GBPUSD", "burst", "planned")],
          "erin (Rapid max_runs 2; the burst's picks EURUSD, XAUUSD, GBPUSD): Rapid on EURUSD and XAUUSD, then the "
          "classic burst on GBPUSD", [b.to_dict() for b in er])
    check(len(er) == 3 and er[0].params == er[1].params == B.params_for(es, 6, "rapid") and er[0].params["orders"] == 6
          and er[2].params == B.params_for(es, 6) and er[2].params["trades"] == 6,
          "Rapid's params with its orders capped by the admin's max_trades; the classic burst's its own",
          er and er[0].params)
    check(len(er) == 3 and all(b.id in eplans and eplans[b.id]["kind"] == b.kind and eplans[b.id]["params"] == b.params
                               for b in er), "every run's plan goes to connector 1.3.0, with its kind",
          {i: p.get("kind") for i, p in eplans.items()})
    check([(b.symbol, b.kind, b.status, b.reason) for b in fr]
          == [("EURUSD", "rapid", "skipped", B.OUTDATED), ("GBPUSD", "burst", "planned", None)]
          and fr[1].id in fplans and fr[0].id not in fplans,
          "frank on connector 1.2.0: his Rapid run skipped as outdated, never sent; his classic burst (v1.2.0's "
          "params) on his next pick, sent", [b.to_dict() for b in fr])
    tick()
    again = plans_in(sync(ew))
    sync(fw)
    check(len(at_tr(erin_id)) == 3 and len(at_tr(frank_id)) == 2 and {b.id for b in er} <= set(again),
          "the next tick adds no run; the planned ones (Rapid's too) are sent again until the connector confirms")

    # ── Rapid's news trigger switched off: its runs stop, the classic burst goes on ──
    use(client, erin)
    put({"rapid": {"triggers": {"news": False}}})
    sync(ew)
    tick()
    msgs = sync(ew)
    check(len(er) == 3 and [(row(b.id).status, row(b.id).reason) for b in er]
          == [("cancelled", "Rapid news runs switched off")] * 2 + [("planned", None)]
          and cancel_msg(er[0].id) in msgs and cancel_msg(er[1].id) in msgs and cancel_msg(er[2].id) not in msgs,
          "Rapid's news trigger off: its planned runs are cancelled (the cancel sent), the classic burst's stays",
          [row(b.id).to_dict() for b in er])
    put({"rapid": {"triggers": {"news": True}}})
    tick()
    sync(ew)
    check(len(at_tr(erin_id)) == 3, "on again: no new Rapid run for that release (a cancel stands)", at_tr(erin_id))

    # ── the admin's kill switch stops both kinds ──
    sched = add(Burst(user_id=erin_id, event_id="ev-rapid", event_title="Retail Sales m/m", currency="USD",
                      symbol="USDJPY", event_time=tr, kind="rapid", status="scheduled", params=er[0].params))
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"burst": {"enabled": False}})
    tick()
    msgs = sync(ew)
    sync(fw)
    check((row(sched.id).status, row(sched.id).reason) == ("scheduled", "news burst disabled by admin")
          and cancel_msg(sched.id) in msgs
          and (row(er[2].id).status, row(er[2].id).reason) == ("cancelled", "news burst disabled by admin")
          and row(fr[1].id).status == "cancelled",
          "the admin's kill switch: a Rapid run its connector holds gets the cancel, the classic runs are cancelled",
          (row(sched.id).to_dict(), row(er[2].id).to_dict()))
    client.put("/api/admin/engine", headers=H, json={"burst": {"enabled": True, "max_trades": 40}})
    tick()
    ew.send_json({"type": "burst_update", "id": sched.id, "status": "cancelled"})
    sync(ew)
    sync(fw)
    check(len(at_tr(erin_id)) == 4 and len(at_tr(frank_id)) == 2 and row(sched.id).status == "cancelled",
          "switched on again: nothing planned again at that release, of either kind")

    # ── the timeout counts each kind's own life ──
    rp = B.params_for(DEFAULT_USER_SETTINGS, 40, "rapid")         # a new user's: expire 30 s, X 30 s, Y 120 s
    mk = lambda sym, secs, status, kind, params: Burst(  # noqa: E731
        user_id=erin_id, event_title="Retail Sales m/m", currency="USD", symbol=sym, kind=kind, status=status,
        event_time=utcnow() + timedelta(seconds=secs), params=params)
    gone = add(mk("AUDUSD", -(30 + 120 + 301), "armed", "rapid", rp))
    held = add(mk("USDCAD", -(30 + 30 + 301), "triggered", "rapid", rp))
    bare = add(mk("USDCHF", -(30 + 30 + 301), "armed", "rapid", {}))
    classic = add(mk("NZDUSD", -(30 + 30 + 301), "armed", "burst", {}))
    tick()
    got = [(row(b.id).status, row(b.id).reason) for b in (gone, held, bare, classic)]
    check(got == [("error", B.TIMED_OUT), ("triggered", None), ("error", B.TIMED_OUT), ("armed", None)],
          "timeout per kind: a Rapid run's is Rapid's expire + max(X, Y) + 300 s (30 + 120 + 300; unreadable params: "
          "30 + 30 + 300), a classic run's the burst's (90 + 30 + 300)", got)
    for b in (held, classic):
        ew.send_json({"type": "burst_update", "id": b.id, "status": "expired"})
    sync(ew)

    # ── Rapid's trades: booked as rapid ──
    rid, cid = er[0].id, er[2].id
    for t in (trade(9001, burst_id=rid), trade(9002, burst_id=cid), trade(9003, strategy="rapid"),
              trade(9006, burst_id=rid)):
        ew.send_json({"type": "trade_opened", "trade": t})
    # a Rapid trade whose open never reached the server, and a close that says nothing of itself
    ew.send_json({"type": "trade_closed", "trade": trade(9004, strategy="rapid", exit_price=1.1012, pnl=3.0,
                                                         closed_at=utcnow().isoformat())})
    ew.send_json({"type": "trade_closed", "trade": {"ticket": 9005, "pnl": 9.0, "closed_at": utcnow().isoformat()}})
    sync(ew)

    def booked():
        with SessionLocal() as db:
            return {t.ticket: t for t in db.scalars(select(UserTrade).where(UserTrade.user_id == erin_id))}
    got = {k: (t.strategy, t.status) for k, t in booked().items()}
    check(got == {9001: ("rapid", "open"), 9002: ("burst", "open"), 9003: ("rapid", "open"), 9004: ("rapid", "closed"),
                  9006: ("rapid", "open")},
          "a Rapid news run's trade (its burst_id) and a Rapid signal run's (strategy rapid) are rapid, a close whose "
          "open never came is kept; the classic burst's stays burst; an unknown close saying nothing is ignored", got)
    use(client, erin)
    r = client.get("/api/trades", params={"strategy": "rapid"}).json()
    b = client.get("/api/trades", params={"strategy": "burst"}).json()
    check(sorted(t["ticket"] for t in r["trades"]) == [9001, 9003, 9004, 9006]
          and [t["ticket"] for t in b["trades"]] == [9002],
          "/api/trades?strategy=rapid: Rapid's trades only; burst: the classic burst's",
          ([t["ticket"] for t in r["trades"]], [t["ticket"] for t in b["trades"]]))
    s = client.get("/api/trades/stats", params={"days": "7"}).json()
    check(s["open"]["rapid"] == 3 and s["open"]["burst"] == 1 and s["periods"]["7"]["rapid"]["closed"] == 1
          and s["periods"]["7"]["rapid"]["pnl"] == 3.0 and s["periods"]["7"]["burst"]["closed"] == 0,
          "/api/trades/stats: Rapid's open and closed trades, apart from the classic burst's",
          (s.get("open"), s.get("periods", {}).get("7", {}).get("rapid")))
    r = put({"dashboard": {"tiles": [{"k": "trades", "scope": "rapid"}, {"k": "win_rate", "days": 7, "scope": "rapid"}]}})
    check(r.json()["settings"]["dashboard"] == {"tiles": [{"k": "trades", "days": 30, "scope": "rapid"},
                                                          {"k": "win_rate", "days": 7, "scope": "rapid"}]},
          "PUT dashboard: a tile's scope rapid is kept", r.json()["settings"].get("dashboard"))

    # ── trades booked before their strategy existed (db.TRADE_STRATEGY at init_db): a Rapid run's are rapid ──
    with SessionLocal() as db:
        db.execute(text("UPDATE user_trades SET strategy = NULL WHERE user_id = :u AND ticket IN (9001, 9002)"),
                   {"u": erin_id})
        db.execute(text("UPDATE user_trades SET strategy = 'long' WHERE user_id = :u AND ticket = 9006"), {"u": erin_id})
        db.add_all([UserTrade(user_id=erin_id, ticket=t, symbol="EURUSD", side="buy", lots=0.1, entry_price=1.1,
                              opened_at=utcnow(), status="closed", demo=True, burst_id=b, strategy="burst")
                    for t, b in ((9007, cid), (9008, rid))])
        db.commit()
    main.init_db()
    got = {k: t.strategy for k, t in booked().items()}
    check(got == {9001: "rapid", 9002: "burst", 9003: "rapid", 9004: "rapid", 9006: "rapid", 9007: "burst",
                  9008: "burst"},
          "init_db with both kinds of run: a trade without a strategy (or booked long) of a Rapid run is rapid, of a "
          "classic one burst; a trade booked burst is never relabelled", got)

    # ── a local Rapid run's trades (its snapshot and orphan reports carry its run id, ms, as burst_id): no Burst row
    # has that id, so they are booked rapid with no burst ──
    local = 1730000000123
    ew.send_json({"type": "trade_opened", "trade": trade(9010, burst_id=local, strategy="rapid")})
    ew.send_json({"type": "trade_closed", "trade": trade(9011, burst_id=local + 1, strategy="rapid", exit_price=1.1,
                                                         pnl=-2.5, closed_at=utcnow().isoformat())})
    sync(ew)
    got = {k: (t.strategy, t.status, t.burst_id) for k, t in booked().items() if k in (9010, 9011)}
    check(got == {9010: ("rapid", "open", None), 9011: ("rapid", "closed", None)},
          "a local Rapid run's open and close (open never sent) with its run id as burst_id: booked rapid, no burst", got)

    # ── a Rapid-only user (the classic burst off, Rapid's news trigger on): Rapid's runs only; GET /api/bursts lists
    # the upcoming runs as burst_tick plans them (Rapid's first, then the classic burst on a pair Rapid left) ──
    with SessionLocal() as db:
        busy = set(db.scalars(select(CalendarEvent.time)))
    soon = utcnow().replace(microsecond=0) + timedelta(minutes=2)
    ro = next(t for t in (soon + timedelta(seconds=k) for k in range(60)) if t not in busy)       # no other release
    rb = next(t for t in (soon + timedelta(seconds=k) for k in range(60, 120)) if t not in busy)
    usd = lambda i, t, title: CalendarEvent(id=i, time=t, currency="USD", title=title, impact="High",  # noqa: E731
                                            source="test", forecast="0.5%", previous="0.2%")
    listed = lambda t: [(u["symbol"], u["kind"]) for u in client.get("/api/bursts").json()["upcoming"]  # noqa: E731
                        if u["time"] == t.isoformat() + "Z"]
    r = put({"burst": {"enabled": False}, "rapid": {"enabled": True, "max_runs": 2, "symbols": [],
                                                     "triggers": {"news": True}}})
    add(usd("ev-rapid-only", ro, "ISM Services PMI"))
    sync(ew)
    up = listed(ro)
    tick()
    only = bursts_where(Burst.user_id == erin_id, Burst.event_time == ro)
    with SessionLocal() as db:
        es = db.get(User, erin_id).merged_settings()
    plans = plans_in(sync(ew))
    check(r.status_code == 200 and len(only) == 2
          and all((b.kind, b.status, b.reason) == ("rapid", "planned", None) and b.params == B.params_for(es, 40, "rapid")
                  and b.id in plans and plans[b.id]["kind"] == "rapid" for b in only),
          "a Rapid-only user (the classic burst off): Rapid's runs planned with Rapid's params and sent with their "
          "kind, no classic run", [b.to_dict() for b in only])
    check(up == [(b.symbol, b.kind) for b in only] == [("EURUSD", "rapid"), ("XAUUSD", "rapid")],
          "GET /api/bursts upcoming for a Rapid-only user: Rapid's runs with their kind, as burst_tick then plans them",
          up)
    put({"burst": {"enabled": True}})
    add(usd("ev-rapid-both", rb, "Durable Goods Orders m/m"))
    sync(ew)
    up = listed(rb)
    tick()
    both = bursts_where(Burst.user_id == erin_id, Burst.event_time == rb)
    sync(ew)
    check(up == [(b.symbol, b.kind) for b in both] == [("EURUSD", "rapid"), ("XAUUSD", "rapid"), ("GBPUSD", "burst")],
          "GET /api/bursts upcoming with both on: Rapid's runs first, the classic burst on the pick Rapid left (not on "
          "the pairs Rapid takes), as burst_tick then plans them", (up, [b.to_dict() for b in both]))
    # in time order across releases (plan_runs gives every Rapid run before the classic burst's): a classic run at a
    # release earlier than a Rapid one is listed first, so the Dashboard's next release and the first ten are right
    put({"rapid": {"symbols": ["EURUSD"]}})              # none of GBP's pairs: no Rapid run at its release
    tg1 = next(t for t in (soon + timedelta(seconds=k) for k in range(120, 180)) if t not in busy)
    tu1 = next(t for t in (soon + timedelta(seconds=k) for k in range(180, 240)) if t not in busy)
    add(CalendarEvent(id="ev-order-gbp", time=tg1, currency="GBP", title="GDP m/m", impact="High", source="test",
                      forecast="0.5%", previous="0.2%"), usd("ev-order-usd", tu1, "ISM Manufacturing PMI"))
    sync(ew)
    up = [(u["time"], u["symbol"], u["kind"]) for u in client.get("/api/bursts").json()["upcoming"]]
    early, late = (tg1.isoformat() + "Z", "GBPUSD", "burst"), (tu1.isoformat() + "Z", "EURUSD", "rapid")
    when = [datetime.fromisoformat(u[0].rstrip("Z")) for u in up]
    check(early in up and late in up and up.index(early) < up.index(late) and when == sorted(when),
          "GET /api/bursts upcoming with both on: in time order, a classic run at a GBP release before a Rapid one at "
          "a later USD release (not every Rapid run first)", up)
    with SessionLocal() as db:                           # nothing of them for burst_tick to plan below
        for i in ("ev-order-gbp", "ev-order-usd"):
            db.delete(db.get(CalendarEvent, i))
        db.commit()
    put({"rapid": {"symbols": []}})
    sync(ew)

    # ── burst_stop_reason for Rapid's runs: a release moved; a pair another currency's release lists but does not
    # own stands on rapid.symbols (any pair when none is picked), never on the classic burst's picks ──
    with SessionLocal() as db:
        db.get(CalendarEvent, "ev-rapid-only").time = ro + timedelta(hours=1)
        db.commit()
    tick()
    msgs = sync(ew)
    check(all((row(b.id).status, row(b.id).reason) == ("cancelled", "event rescheduled") and cancel_msg(b.id) in msgs
              for b in only), "a Rapid run's release moved: its planned runs are cancelled (event rescheduled), the "
          "cancel sent", [row(b.id).to_dict() for b in only])
    tg = next(t for t in (utcnow().replace(microsecond=0) + timedelta(minutes=30, seconds=k) for k in range(60))
              if t not in busy)
    add(usd("ev-rapid-usd", tg, "Pending Home Sales m/m"))
    gb = add(Burst(user_id=erin_id, event_id="ev-rapid-gbp", event_title="GDP m/m", currency="GBP", symbol="GBPUSD",
                   event_time=tg, kind="rapid", status="planned", params=only[0].params))
    stood = []
    for body in ({"rapid": {"symbols": ["GBPUSD"]}, "burst": {"symbols": ["EURUSD"]}}, {"rapid": {"symbols": []}}):
        put(body)
        tick()
        sync(ew)
        stood.append((row(gb.id).status, row(gb.id).reason))
    check(stood == [("planned", None)] * 2, "a Rapid run on GBPUSD whose GBP release is gone, at a USD release (which "
          "lists GBPUSD, its own pair EURUSD): it stands with GBPUSD in rapid.symbols (the burst's picks EURUSD), and "
          "with rapid.symbols empty (any pair)", stood)
    put({"rapid": {"symbols": ["EURUSD"]}, "burst": {"symbols": ["GBPUSD"]}})
    tick()
    msgs = sync(ew)
    check((row(gb.id).status, row(gb.id).reason) == ("cancelled", "event rescheduled") and cancel_msg(gb.id) in msgs,
          "... GBPUSD in the classic burst's picks only, not in rapid.symbols: event rescheduled, the cancel sent",
          row(gb.id).to_dict())
    line = main._burst_line(row(gb.id).to_dict())
    check(line == f"Rapid run #{gb.id} GBPUSD cancelled (event rescheduled)"
          and main._burst_line(row(both[2].id).to_dict()).startswith(f"News burst #{both[2].id} GBPUSD"),
          "the connector log line names a Rapid run 'Rapid run', a classic one 'News burst'", line)

    def free(minutes):                                   # a time that minute with no other release
        with SessionLocal() as db:
            taken = set(db.scalars(select(CalendarEvent.time)))
        start = utcnow().replace(microsecond=0) + timedelta(minutes=minutes)
        return next(t for t in (start + timedelta(seconds=k) for k in range(60)) if t not in taken)

    # ── a user's cancel of a Rapid run stands when rapid.symbols changes after it: its release is planned once,
    # even where the new pairs hold no run of it ──
    tc = free(6)
    put({"burst": {"enabled": False}, "rapid": {"max_runs": 1, "symbols": []}})
    add(usd("ev-rapid-cancel", tc, "Initial Jobless Claims"))
    sync(ew)
    tick()
    at_tc = lambda: bursts_where(Burst.user_id == erin_id, Burst.event_time == tc)     # noqa: E731
    mine = at_tc()
    sync(ew)
    sync(fw)
    check([(b.symbol, b.kind, b.status) for b in mine] == [("EURUSD", "rapid", "planned")],
          "erin (Rapid max_runs 1, any pair): one Rapid run on EURUSD at a USD release", [b.to_dict() for b in mine])
    r = client.post(f"/api/bursts/{mine[0].id}/cancel", headers=H)
    check(r.status_code == 200 and (row(mine[0].id).status, row(mine[0].id).reason) == ("cancelled", B.USER_CANCEL),
          "erin cancels it", err(r))
    put({"rapid": {"symbols": ["GBPUSD"]}})                # USD's list has GBPUSD: no run of that release on it
    sync(ew)
    up = listed(tc)
    tick()
    sync(ew)
    sync(fw)
    check([b.id for b in at_tc()] == [mine[0].id] and up == [] and stored(erin_id)["rapid"]["enabled"] is True,
          "rapid.symbols changed to GBPUSD after the cancel: no Rapid run planned again for that release, none "
          "listed upcoming (the cancel stands)", ([b.to_dict() for b in at_tc()], up))

    # ── connector 1.2.0 never gets a Rapid run, not even one whose params v1.2.0 reads (the stop in pips, no
    # analysis, Y = X): its kind alone needs 1.3.0, as it is planned, resent and replayed in the welcome ──
    use(client, frank)
    put({"burst": {"enabled": False}, "rapid": {"analysis": False, "sl_unit": "pips", "hold_seconds": 30,
                                                 "max_hold_seconds": 30, "max_runs": 1, "symbols": [],
                                                 "triggers": {"news": True}}})
    sync(fw)
    eur = lambda i, t: CalendarEvent(id=i, time=t, currency="EUR", title="Main Refinancing Rate",  # noqa: E731
                                     impact="High", source="test", forecast="2.15%", previous="2.15%")
    ta = free(7)
    add(eur("ev-rapid-old", ta))
    tick()
    fa = bursts_where(Burst.user_id == frank_id, Burst.event_time == ta)
    sync(ew)
    check([(b.symbol, b.kind, b.status, b.reason) for b in fa] == [("EURUSD", "rapid", "skipped", B.OUTDATED)]
          and fa[0].id not in plans_in(sync(fw)) and fa[0].params["analysis"] is False
          and B.needs_version(fa[0].params, "burst", "EURUSD") == B.MIN_CONNECTOR,
          "frank on connector 1.2.0, Rapid with the stop in pips, no analysis, Y = X (params a classic run of "
          "v1.2.0's would pass with): his Rapid run skipped as outdated, never sent", [b.to_dict() for b in fa])
    t5 = utcnow() + timedelta(seconds=2.5)               # under 3 s away: the delivery loop leaves it alone that tick
    add(eur("ev-rapid-old5", t5))
    try:
        tick()
        f5 = bursts_where(Burst.user_id == frank_id, Burst.event_time == t5)
    finally:
        with SessionLocal() as db:                       # never planned by a later burst_tick
            db.delete(db.get(CalendarEvent, "ev-rapid-old5"))
            db.commit()
    sync(ew)
    check([(b.symbol, b.kind, b.status, b.reason) for b in f5] == [("EURUSD", "rapid", "skipped", B.OUTDATED)]
          and f5[0].id not in plans_in(sync(fw)),
          "... one planned under 3 s before its release: skipped as outdated in the tick that makes it",
          [b.to_dict() for b in f5])
    close(fw)
    fw, _ = connect(client, frank_token)                 # connector 1.3.0: both runs planned and sent
    ts, tp = free(8), free(9)
    add(eur("ev-rapid-sched", ts), eur("ev-rapid-plan", tp))
    tick()
    sp = [bursts_where(Burst.user_id == frank_id, Burst.event_time == t) for t in (ts, tp)]
    plans = plans_in(sync(fw))
    sync(ew)
    check([[(b.symbol, b.kind, b.status) for b in x] for x in sp] == [[("EURUSD", "rapid", "planned")]] * 2
          and all(x[0].id in plans for x in sp), "frank on connector 1.3.0: the same Rapid runs planned and sent",
          [[b.to_dict() for b in x] for x in sp])
    sid, pid = sp[0][0].id, sp[1][0].id
    fw.send_json({"type": "burst_update", "id": sid, "status": "scheduled"})
    sync(fw)
    close(fw)
    fw, welcome = connect(client, frank_token, version="1.2.0")         # the connector downgraded
    check(row(sid).status == "scheduled" and not {sid, pid} & {p["id"] for p in welcome["bursts"]},
          "back on connector 1.2.0: its welcome replays neither (the one it scheduled, the one still planned)",
          welcome["bursts"])
    tick()
    check((row(pid).status, row(pid).reason) == ("skipped", B.OUTDATED) and pid not in plans_in(sync(fw))
          and row(sid).status == "scheduled",
          "... and burst_tick skips the planned one as outdated, never resent",
          (row(pid).to_dict(), row(sid).to_dict()))
    sync(ew)
    check(stored(frank_id)["rapid"]["enabled"] is True, "frank's Rapid stays on")

    # ── the classic burst without picks where Rapid holds a release's own pair: the next pair of its list Rapid left,
    # planned once; on connector 1.2.0 a new pair is skipped as outdated, an old one runs with v1.2.0's params; Rapid's
    # news runs off: the own pair, as up to v1.2.0 ──
    put({"burst": {"enabled": True, "symbols": []}, "rapid": {"max_runs": 2}})          # frank, on connector 1.2.0
    use(client, erin)
    put({"burst": {"enabled": True, "symbols": []}, "rapid": {"max_runs": 1, "symbols": [], "triggers": {"news": True}}})
    sync(ew)
    sync(fw)
    tf = free(10)
    add(usd("ev-rapid-fallback", tf, "Core PCE Price Index m/m"))
    tick()
    at_tf = lambda uid, t=tf: bursts_where(Burst.user_id == uid, Burst.event_time == t)    # noqa: E731
    ef, ff = at_tf(erin_id), at_tf(frank_id)
    eplans, fplans = plans_in(sync(ew)), plans_in(sync(fw))
    with SessionLocal() as db:
        fs = db.get(User, frank_id).merged_settings()
    check([(b.symbol, b.kind, b.status) for b in ef] == [("EURUSD", "rapid", "planned"), ("XAUUSD", "burst", "planned")]
          and all(b.id in eplans for b in ef),
          "erin (Rapid max_runs 1, the classic burst without picks): Rapid on EURUSD, the classic burst on XAUUSD, the "
          "next pair of USD's list, both sent", [b.to_dict() for b in ef])
    check([(b.symbol, b.kind, b.status, b.reason) for b in ff]
          == [("EURUSD", "rapid", "skipped", B.OUTDATED), ("XAUUSD", "rapid", "skipped", B.OUTDATED),
              ("GBPUSD", "burst", "planned", None)]
          and ff[2].id in fplans and not {ff[0].id, ff[1].id} & set(fplans) and ff[2].params == B.params_for(fs, 40)
          and B.needs_version(ff[2].params, "burst", "GBPUSD") == B.MIN_CONNECTOR,
          "frank on connector 1.2.0 (Rapid max_runs 2): his Rapid runs skipped as outdated still hold their pairs; his "
          "classic burst on GBPUSD, the next pair they left, with v1.2.0's params, sent", [b.to_dict() for b in ff])
    tick()
    sync(ew)
    sync(fw)
    check(len(at_tf(erin_id)) == 2 and len(at_tf(frank_id)) == 3, "the next tick plans nothing more at that release")
    use(client, frank)
    put({"rapid": {"max_runs": 1}})
    sync(fw)
    tn = free(11)
    add(usd("ev-rapid-fallback-new", tn, "Core PCE Price Index y/y"))
    tick()
    fn = at_tf(frank_id, tn)
    fplans = plans_in(sync(fw))
    sync(ew)
    check([(b.symbol, b.kind, b.status, b.reason) for b in fn]
          == [("EURUSD", "rapid", "skipped", B.OUTDATED), ("XAUUSD", "burst", "skipped", B.OUTDATED)]
          and not {b.id for b in fn} & set(fplans),
          "frank (Rapid max_runs 1) on connector 1.2.0: the classic burst's next pair is XAUUSD, which v1.2.0 does not "
          "trade: skipped as outdated, never sent", [b.to_dict() for b in fn])
    put({"rapid": {"triggers": {"news": False}}})
    use(client, erin)
    put({"rapid": {"triggers": {"news": False}}})
    sync(ew)
    sync(fw)
    to = free(12)
    add(usd("ev-rapid-fallback-off", to, "Personal Spending m/m"))
    tick()
    eo, fo = at_tf(erin_id, to), at_tf(frank_id, to)
    eplans, fplans = plans_in(sync(ew)), plans_in(sync(fw))
    check([(b.symbol, b.kind, b.status) for b in eo] == [("EURUSD", "burst", "planned")]
          and [(b.symbol, b.kind, b.status) for b in fo] == [("EURUSD", "burst", "planned")]
          and eo[0].id in eplans and fo[0].id in fplans and B.needs_version(fo[0].params, "burst", "EURUSD")
          == B.MIN_CONNECTOR, "Rapid's news runs off: the classic burst on USD's own pair EURUSD, as up to v1.2.0 "
          "(connector 1.2.0 too, with v1.2.0's params)", ([b.to_dict() for b in eo], [b.to_dict() for b in fo]))
    close(ew)
    close(fw)
    _rapid_rollback({erin_id, frank_id})


def _rapid_rollback(rapid_users: set):
    """updates/v1.3.0 deploy_130.py rollback_db on a copy of the flow's database: refused while a run (a Rapid one
    too) is under way or a Rapid trade is open, nothing changed; else Rapid is switched off in every stored user that
    had it on, nothing else of the settings changed, in the transaction that does the rest."""
    spec = importlib.util.spec_from_file_location("deploy_130", ROOT / "updates" / "v1.3.0" / "deploy_130.py")
    kit = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode, was = True, sys.dont_write_bytecode      # no __pycache__ in updates/
    try:
        spec.loader.exec_module(kit)
    finally:
        sys.dont_write_bytecode = was
    check([kit.rapid_off(s) for s in (None, "", "junk", "[1]", "{}", '{"rapid": "on"}', '{"rapid": {"enabled": false}}')]
          == [None] * 7 and json.loads(kit.rapid_off('{"x": 1, "rapid": {"enabled": true, "orders": 5}}'))
          == {"x": 1, "rapid": {"enabled": False, "orders": 5}},
          "rapid_off: a stored rapid block switched off; nothing to change otherwise (none, off, not JSON)")
    path = TMP / "rollback-rapid.db"
    src, dst = sqlite3.connect(DB_FILE), sqlite3.connect(path)
    try:
        src.backup(dst)
    finally:
        dst.close(), src.close()

    def sql(*stmts):
        c = sqlite3.connect(path)
        try:
            for s in stmts:
                c.execute(s)
            c.commit()
        finally:
            c.close()

    def users() -> dict:
        c = sqlite3.connect(path)
        try:
            return dict(c.execute("SELECT id, settings FROM users").fetchall())
        finally:
            c.close()

    def rolled(out=None):
        try:
            return kit.rollback_db(path, out)
        except SystemExit as e:
            return e

    before = users()
    parsed = lambda d: {uid: json.loads(s or "null") for uid, s in d.items()}       # noqa: E731
    on = {uid for uid, s in parsed(before).items() if ((s or {}).get("rapid") or {}).get("enabled") is True}
    final = ", ".join(f"'{s}'" for s in sorted(B.FINAL))
    sql(f"UPDATE bursts SET status = 'closed' WHERE status NOT IN ({final})",
        "UPDATE user_trades SET status = 'closed' WHERE status = 'open'",
        "UPDATE bursts SET status = 'armed' WHERE id = (SELECT max(id) FROM bursts WHERE kind = 'rapid')")
    got = rolled()
    check(on == rapid_users and isinstance(got, SystemExit) and "1 burst(s) under way, 0 burst trade(s) open" in str(got)
          and users() == before, "rollback: refused while a Rapid run is under way, nothing changed", (on, got))
    sql("UPDATE bursts SET status = 'closed' WHERE status = 'armed'",
        "UPDATE user_trades SET status = 'open' WHERE ticket = 9003")
    got = rolled()
    check(isinstance(got, SystemExit) and "0 burst(s) under way, 1 burst trade(s) open" in str(got) and users() == before,
          "... and while a Rapid signal run's trade (strategy rapid, no burst) is open", got)
    sql("UPDATE user_trades SET status = 'closed' WHERE ticket = 9003")
    counts = {}
    got = rolled(counts)
    after, want = users(), parsed(before)
    for uid in on:
        want[uid]["rapid"]["enabled"] = False
    check(not isinstance(got, SystemExit) and counts == {"rapid_users": 2, "rapid_runs": 0} and parsed(after) == want
          and {u: s for u, s in after.items() if u not in on} == {u: s for u, s in before.items() if u not in on},
          "rollback: Rapid switched off in the stored settings of both users who had it on, nothing else changed; no "
          "Rapid run left to cancel (the refusal saw to that)", (got, counts))
    c = sqlite3.connect(path)
    try:
        kinds = dict(c.execute("SELECT kind, count(*) FROM bursts GROUP BY kind").fetchall())
    finally:
        c.close()
    check(kinds.get("rapid", 0) >= 5 and kinds.get("burst", 0) > 0, "bursts.kind stays, the Rapid runs with it", kinds)
    counts = {}
    got = rolled(counts)
    check(not isinstance(got, SystemExit) and counts == {"rapid_users": 0, "rapid_runs": 0} and users() == after,
          "a second rollback: nothing more to switch off", counts)


def _timeout():
    print("FAIL timeout: a WebSocket wait never returned", flush=True)
    faulthandler.dump_traceback(all_threads=True)
    os._exit(3)


def main_():
    watchdog = threading.Timer(120, _timeout)
    watchdog.daemon = True
    watchdog.start()
    check(DATABASE_URL == os.environ["DATABASE_URL"] and str(TMP.as_posix()) in DATABASE_URL, "test database is a temp file")
    try:
        test_helpers()
        test_v130_helpers()
        test_rapid_helpers()
        test_flow()
    finally:
        watchdog.cancel()
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
