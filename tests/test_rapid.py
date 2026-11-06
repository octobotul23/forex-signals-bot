"""Rapid (v1.3.0): the pure helpers in bot/rapid.py: its settings (clamp_rapid), the confidence, the per-order sizing
(size_order), how many orders fit (fit_orders), the stop-limit prices, the netting split and the local runs' comments;
then the connector's local runs (a short-term signal's): the real Connector and Scalper on test_short_term's simulated
broker, its rule, stretch and clock stand-ins.

    .venv\\Scripts\\python tests\\test_rapid.py [scenario ...]

The pure part: no broker, no network, no files (nothing of MT5, asyncio, the server or websockets is pulled in: the
connector part is loaded by its first scenario). The connector part: no MetaTrader 5, no network, temporary files only.
Exit code 0 = every check passed.
"""
import contextlib
import json
import logging
import math
import random
import re
import shutil
import sys
import tempfile
import time
import traceback
import types
from dataclasses import replace
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot import rapid                                                         # noqa: E402
from bot.rapid import (COMMENT_MAX, DEFAULTS, LIMITS, MAX_HOLD_TOP, NEW_USER_MAX_HOLD, RAPID_MARGIN_USE,  # noqa: E402
                       RUN_KIND, SL_CAP_PIPS, TP_MAX_SL, UNIT_DEFAULTS, clamp_rapid, confidence_from_score,
                       confidence_from_stretch, fit_orders, local_comment, risk_at, size_order, split_volume,
                       stop_limit_prices, tp_usd_at)
from bot.risk import money_for, stop_floor_pips                               # noqa: E402
from bot.symbols import SymbolSpec                                            # noqa: E402

PURE = not {"MetaTrader5", "asyncio", "server", "websockets"} & set(sys.modules)   # nothing of those pulled in
# the broker probe's specs: 5-digit EURUSD and 3-digit USDJPY (10 points a pip), gold with 2 digits, contract 100
# (a 0.1 pip is 10 $ a lot), a 20 point freeze level and a 5 point stops level, scale 5
EURUSD = SymbolSpec("EURUSD", pip=0.0001, pip_value_per_lot=10.0, digits=5, point=0.00001, volume_min=0.01,
                    volume_step=0.01, volume_max=100.0)
USDJPY = SymbolSpec("USDJPY", pip=0.01, pip_value_per_lot=6.7, digits=3, point=0.001, volume_min=0.01,
                    volume_step=0.01, volume_max=100.0)
XAUUSD = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, volume_min=0.01,
                    volume_step=0.01, volume_max=100.0, min_stop_distance=0.05, freeze_distance=0.2, scale=5.0)
GBPJPY = SymbolSpec("GBPJPY", pip=0.01, pip_value_per_lot=6.7, digits=3, point=0.001, volume_min=0.01,
                    volume_step=0.01, volume_max=50.0, min_stop_distance=0.003, freeze_distance=0.002, scale=2.0)
NAN, INF = float("nan"), float("inf")
RUN_TAG = re.compile(r"fx([brs]):(\d+)")       # connector/connector.py RUN_TAG: how a run's orders are told apart

TESTS = []


def scenario(fn):
    TESTS.append(fn)
    return fn


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def near(a, b, eps=1e-6):
    return a is not None and b is not None and abs(a - b) < eps


def raises(fn, code, what):
    try:
        got = fn()
    except ValueError as e:
        check(str(e) == code, f"{what}: must raise {code}, raised {e}")
        return
    check(False, f"{what}: must raise {code}, gave {got!r}")


def whole_points(pips, spec):
    p = pips * spec.pip / spec.point
    return abs(p - round(p)) < 1e-6


def on_step(lots, spec):
    s = lots / spec.volume_step
    return abs(s - round(s)) < 1e-6


def floor_of(spec, spread):
    return max(stop_floor_pips(spec, spread, None), spec.freeze_distance / spec.pip + (spread or 0.0))


# ── clamp_rapid ────────────────────────────────────────────────
@scenario
def pure_module():
    check(PURE, "bot/rapid.py pulls in no MT5, no asyncio, no server, no websockets")
    check(RUN_KIND == "rapid", f"RUN_KIND: {RUN_KIND}")
    check(RAPID_MARGIN_USE == 0.5, "a side takes at most half the free margin (the Scalper's MARGIN_USE)")
    check(TP_MAX_SL == 3.0, f"a take-profit is at most 3 stops: {TP_MAX_SL}")


@scenario
def clamp_defaults():
    d = clamp_rapid(None)
    check(d == DEFAULTS and d is not DEFAULTS, f"no body, nothing stored: the defaults: {d}")
    check(d["triggers"] is not DEFAULTS["triggers"] and d["symbols"] is not DEFAULTS["symbols"],
          "a fresh triggers dict and symbols list, never the shared defaults")
    d["triggers"]["news"] = False
    d["symbols"].append("EURUSD")
    check(DEFAULTS["triggers"] == {"signals": True, "news": True} and DEFAULTS["symbols"] == [],
          "changing a result never changes the defaults")
    check(list(clamp_rapid({})) == list(DEFAULTS) and "max_hold_seconds" not in DEFAULTS,
          "the whole block, every key; no Y unless sent or stored (it follows X)")
    want = {"enabled": False, "orders": 10, "sl_unit": "pips", "risk_unit": "pct", "risk_min": 0.2, "risk_max": 1.0,
            "sl_pips": 8.0, "lots": 0.10, "tp_min_usd": 10.0, "tp_max_usd": 15.0, "limit_pips": 0.0,
            "offset_pips": 2.0, "expire_seconds": 30, "hold_seconds": 30, "triggers": {"signals": True, "news": True},
            "analysis": True, "loss_unit": "pct", "daily_loss": 5.0, "max_runs": 2, "cooldown_seconds": 120,
            "symbols": []}
    check(clamp_rapid(None) == want, "the design's defaults")
    check(clamp_rapid(None, "junk") == want and clamp_rapid("junk") == want, "a body or cur that is no dict: ignored")
    check(NEW_USER_MAX_HOLD == 120 and MAX_HOLD_TOP == 1800, "Y: 120 for a new user's block, at most 1800")
    for key, rng in LIMITS.items():
        rngs = rng.values() if isinstance(rng, dict) else (rng,)
        check(all(lo <= hi for lo, hi in rngs), f"{key}: a range")
    for k in ("orders", "expire_seconds", "hold_seconds", "max_runs", "cooldown_seconds"):
        check(type(want[k]) is int and type(clamp_rapid({k: 7.9})[k]) is int, f"{k} is an int")
    for k in ("risk_min", "risk_max", "sl_pips", "lots", "tp_min_usd", "tp_max_usd", "limit_pips", "offset_pips",
              "daily_loss"):
        check(type(clamp_rapid({k: 3})[k]) is float, f"{k} is a float")


@scenario
def clamp_numbers():
    got = clamp_rapid({"orders": 99, "sl_pips": 0.2, "lots": 500, "limit_pips": -1, "offset_pips": 0.1,
                       "expire_seconds": 1, "hold_seconds": 9999, "max_runs": 9, "cooldown_seconds": -5,
                       "tp_min_usd": 0, "tp_max_usd": 20000, "risk_min": 0.01, "risk_max": 9, "daily_loss": 99,
                       "bogus": 1})
    check(got == {**DEFAULTS, "orders": 40, "sl_pips": 1.0, "lots": 100.0, "limit_pips": 0.0, "offset_pips": 0.5,
                  "expire_seconds": 5, "hold_seconds": 600, "max_runs": 5, "cooldown_seconds": 0, "tp_min_usd": 1.0,
                  "tp_max_usd": 10_000.0, "risk_min": 0.05, "risk_max": 5.0, "daily_loss": 50.0},
          f"clamped to the ranges, unknown keys dropped: {got}")
    got = clamp_rapid({"orders": 0, "max_runs": 0, "cooldown_seconds": 99999, "limit_pips": 9, "offset_pips": 60,
                       "sl_pips": 500, "lots": 0.001, "expire_seconds": 5000, "hold_seconds": 1, "daily_loss": 0.1})
    check((got["orders"], got["max_runs"], got["cooldown_seconds"], got["limit_pips"], got["offset_pips"],
           got["sl_pips"], got["lots"], got["expire_seconds"], got["hold_seconds"], got["daily_loss"])
          == (1, 1, 3600, 5.0, 50.0, 200.0, 0.01, 600, 5, 0.5), f"the other ends: {got}")
    got = clamp_rapid({"orders": "7", "hold_seconds": 45.9, "sl_pips": "12.5", "lots": "0.3"})
    check((got["orders"], got["hold_seconds"], got["sl_pips"], got["lots"]) == (7, 45, 12.5, 0.3),
          f"numbers as text, an int key cut to its whole part: {got}")
    keys = [k for k in LIMITS] + ["max_hold_seconds"]
    for k in keys:
        for bad in (NAN, INF, -INF, "x", None, [1], {"a": 1}, True, False, 10**400):
            if bad is None and k == "max_hold_seconds":     # null: Y removed (clamp_max_hold)
                continue
            raises(lambda: clamp_rapid({k: bad}), "invalid_number", f"{k} = {bad!r}")
    raises(lambda: clamp_rapid({"orders": "7.5"}), "invalid_number", "an int key as decimal text")


@scenario
def clamp_units():
    for body in ({"risk_unit": "eur"}, {"risk_unit": None}, {"risk_unit": ["pct"]}, {"loss_unit": 5},
                 {"loss_unit": "PCT"}, {"sl_unit": "pct"}, {"sl_unit": "usd"}, {"sl_unit": True}, {"sl_unit": None}):
        raises(lambda: clamp_rapid(body), "invalid_unit", f"{body}")
    check(clamp_rapid({"sl_unit": "money"})["sl_unit"] == "money", "sl_unit money")
    check(clamp_rapid({"sl_unit": "pips"}, {**DEFAULTS, "sl_unit": "money"})["sl_unit"] == "pips", "back to pips")
    check(clamp_rapid(None, {**DEFAULTS, "sl_unit": "pct"})["sl_unit"] == "pips", "a stored sl_unit gone bad: pips")
    pct = {**DEFAULTS, "risk_min": 0.5, "risk_max": 2.0}
    got = clamp_rapid({"risk_unit": "usd"}, pct)
    check((got["risk_unit"], got["risk_min"], got["risk_max"]) == ("usd", 10.0, 50.0),
          f"a unit change with no values: that unit's defaults (2 % never becomes 2 $): {got}")
    check((UNIT_DEFAULTS["usd"]["risk_min"], UNIT_DEFAULTS["usd"]["risk_max"]) == (10.0, 50.0)
          and (UNIT_DEFAULTS["pct"]["risk_min"], UNIT_DEFAULTS["pct"]["risk_max"]) == (0.2, 1.0)
          and UNIT_DEFAULTS["pct"]["daily_loss"] == 5.0 and UNIT_DEFAULTS["usd"]["daily_loss"] == 100.0,
          "the unit defaults")
    usd = {**DEFAULTS, "risk_unit": "usd", "risk_min": 300.0, "risk_max": 800.0}
    got = clamp_rapid({"risk_unit": "pct"}, usd)
    check((got["risk_min"], got["risk_max"]) == (0.2, 1.0), f"300-800 $ never becomes 5 %: {got}")
    got = clamp_rapid({"risk_unit": "usd", "risk_max": 30}, pct)
    check((got["risk_min"], got["risk_max"]) == (10.0, 30.0), f"a unit change keeps the value sent with it: {got}")
    got = clamp_rapid({"risk_unit": "usd", "risk_max": 7}, pct)
    check((got["risk_min"], got["risk_max"]) == (10.0, 10.0),
          f"... a max under the new unit's min default is raised to it: {got}")
    got = clamp_rapid({"risk_unit": "usd", "risk_min": 0.5, "risk_max": 20000}, None)
    check((got["risk_min"], got["risk_max"]) == (1.0, 10_000.0), f"the usd range is 1..10000: {got}")
    got = clamp_rapid({"risk_unit": "pct"}, pct)
    check((got["risk_min"], got["risk_max"]) == (0.5, 2.0), f"the same unit again changes nothing: {got}")
    got = clamp_rapid({"risk_min": 30}, {**DEFAULTS, "risk_unit": "usd", "risk_min": 10.0, "risk_max": 50.0})
    check((got["risk_min"], got["risk_max"]) == (30.0, 50.0), f"a stored unit's range: {got}")
    got = clamp_rapid({"loss_unit": "usd"}, None)
    check(got["daily_loss"] == 100.0 and got["risk_unit"] == "pct" and got["risk_min"] == 0.2,
          f"the loss unit resets the daily loss only: {got}")
    got = clamp_rapid({"loss_unit": "usd", "daily_loss": 2e6}, None)
    check(got["daily_loss"] == 1_000_000.0, f"the usd daily loss is at most 1 000 000: {got}")
    got = clamp_rapid({"loss_unit": "pct"}, {**DEFAULTS, "loss_unit": "usd", "daily_loss": 250.0})
    check(got["daily_loss"] == 5.0, f"250 $ never becomes 50 %: {got}")
    got = clamp_rapid({"loss_unit": "usd", "daily_loss": 250}, None)
    check(got["daily_loss"] == 250.0, f"a daily loss sent with its unit: {got}")
    got = clamp_rapid(None, {**DEFAULTS, "risk_unit": "btc", "loss_unit": 3, "risk_min": 0.4, "daily_loss": 7})
    check((got["risk_unit"], got["loss_unit"], got["risk_min"], got["daily_loss"]) == ("pct", "pct", 0.4, 7.0),
          f"a stored unit gone bad: pct, the values clamped to its range: {got}")
    got = clamp_rapid(None, {**DEFAULTS, "risk_unit": "usd", "risk_min": "junk", "risk_max": None,
                             "loss_unit": "usd", "daily_loss": NAN})
    check((got["risk_min"], got["risk_max"], got["daily_loss"]) == (10.0, 50.0, 100.0),
          f"a stored money value gone bad: its unit's default: {got}")


@scenario
def clamp_order_of_pairs():
    got = clamp_rapid({"risk_min": 0.8, "risk_max": 0.3})
    check((got["risk_min"], got["risk_max"]) == (0.8, 0.8), f"risk_max under risk_min: raised, not swapped: {got}")
    got = clamp_rapid({"risk_min": 0.8}, {**DEFAULTS, "risk_max": 0.5})
    check((got["risk_min"], got["risk_max"]) == (0.8, 0.8), f"... against a stored max too: {got}")
    got = clamp_rapid({"tp_min_usd": 20, "tp_max_usd": 12})
    check((got["tp_min_usd"], got["tp_max_usd"]) == (20.0, 20.0), f"tp_max under tp_min: raised: {got}")
    got = clamp_rapid({"tp_min_usd": 20})
    check((got["tp_min_usd"], got["tp_max_usd"]) == (20.0, 20.0), f"a min over the stored max: {got}")
    got = clamp_rapid({"tp_min_usd": 12, "tp_max_usd": 40})
    check((got["tp_min_usd"], got["tp_max_usd"]) == (12.0, 40.0), f"in order: kept: {got}")
    got = clamp_rapid({"risk_min": 5, "risk_max": 5})
    check((got["risk_min"], got["risk_max"]) == (5.0, 5.0), "equal: kept")


@scenario
def clamp_max_hold():
    check("max_hold_seconds" not in clamp_rapid({"hold_seconds": 60}), "Y not sent, none stored: not stored")
    got = clamp_rapid({"max_hold_seconds": NEW_USER_MAX_HOLD})
    check(got == {**DEFAULTS, "max_hold_seconds": 120} and type(got["max_hold_seconds"]) is int, f"Y sent: {got}")
    cur = {**DEFAULTS, "max_hold_seconds": 120}
    check(clamp_rapid(None, cur) == cur and clamp_rapid({"orders": 3}, cur)["max_hold_seconds"] == 120,
          "stored: kept")
    for body, stored, want in (({"hold_seconds": 60, "max_hold_seconds": 20}, None, (60, 60)),
                               ({"hold_seconds": 200}, cur, (200, 200)),
                               ({"hold_seconds": 10}, cur, (10, 120)),
                               ({"hold_seconds": 1000, "max_hold_seconds": 9}, None, (600, 600)),
                               (None, {**DEFAULTS, "hold_seconds": 60, "max_hold_seconds": 30}, (60, 60)),
                               ({"max_hold_seconds": 5000}, cur, (30, MAX_HOLD_TOP)),
                               ({"max_hold_seconds": 1800}, cur, (30, 1800)),
                               ({"max_hold_seconds": 1801}, cur, (30, 1800)),
                               ({"max_hold_seconds": 150.9}, cur, (30, 150)),
                               ({"max_hold_seconds": -5}, cur, (30, 30)),
                               ({"max_hold_seconds": 90}, {**DEFAULTS, "max_hold_seconds": "junk"}, (30, 90))):
        got = clamp_rapid(body, stored)
        check((got["hold_seconds"], got["max_hold_seconds"]) == want and type(got["max_hold_seconds"]) is int,
              f"X, then Y clamped to X..{MAX_HOLD_TOP} (a Y under X raised to it): {body} {stored} -> {got}")
    for bad in ("junk", None, NAN, 10**400, [1], {"a": 1}, True):
        got = clamp_rapid(None, {**DEFAULTS, "max_hold_seconds": bad})
        check("max_hold_seconds" not in got, f"a stored Y gone bad ({bad!r}): left out, it follows X: {got}")
    # null: the user's way back to "Y follows X" (as up to v1.2.0); a body without the key keeps a stored Y
    for body, stored in (({"max_hold_seconds": None}, cur), ({"max_hold_seconds": None, "hold_seconds": 90}, cur),
                         ({"max_hold_seconds": None}, None),
                         ({"max_hold_seconds": None}, {**DEFAULTS, "max_hold_seconds": "junk"})):
        got = clamp_rapid(body, stored)
        check("max_hold_seconds" not in got and got["hold_seconds"] == body.get("hold_seconds", 30)
              and clamp_rapid(None, got) == got, f"Y sent as null: removed, X as sent or stored: {body} -> {got}")
    check(clamp_rapid({"hold_seconds": 45}, cur)["max_hold_seconds"] == 120
          and clamp_rapid({}, cur)["max_hold_seconds"] == 120, "the key not sent: the stored Y stays")
    got = clamp_rapid({"max_hold_seconds": 300}, clamp_rapid({"max_hold_seconds": None}, cur))
    check(got["max_hold_seconds"] == 300, f"set again after a null: {got}")


@scenario
def clamp_switches():
    for key, code in (("enabled", "invalid_enabled"), ("analysis", "invalid_analysis")):
        for bad in ("false", "true", 1, 0, None, [True]):
            raises(lambda: clamp_rapid({key: bad}), code, f"{key} = {bad!r} (\"false\" must never switch it on)")
        check(clamp_rapid({key: True})[key] is True and clamp_rapid({key: False})[key] is False, f"{key}: a bool")
    check(clamp_rapid(None, {**DEFAULTS, "enabled": "yes"})["enabled"] is False, "a stored 'enabled' not a bool: off")
    check(clamp_rapid(None, {**DEFAULTS, "analysis": "no"})["analysis"] is True,
          "a stored 'analysis' not a bool: its default")
    check(clamp_rapid({"enabled": True}, {**DEFAULTS, "analysis": False}) ==
          {**DEFAULTS, "enabled": True, "analysis": False}, "one switch changes, the other is kept")


@scenario
def clamp_triggers():
    got = clamp_rapid({"triggers": {"signals": False}})
    check(got["triggers"] == {"signals": False, "news": True}, f"one trigger sent: the other kept: {got['triggers']}")
    cur = {**DEFAULTS, "triggers": {"signals": False, "news": True}}
    got = clamp_rapid({"triggers": {"news": False}}, cur)
    check(got["triggers"] == {"signals": False, "news": False}, f"... the stored one: {got['triggers']}")
    got = clamp_rapid({"triggers": {"news": True, "signals": True, "other": 5}}, cur)
    check(got["triggers"] == {"signals": True, "news": True}, f"unknown trigger keys dropped: {got['triggers']}")
    check(clamp_rapid({"triggers": {}}, cur)["triggers"] == cur["triggers"], "an empty dict changes nothing")
    for bad in ("news", None, ["news"], True, {"news": "false"}, {"signals": 1}, {"news": None}):
        raises(lambda: clamp_rapid({"triggers": bad}), "invalid_triggers", f"triggers = {bad!r}")
    for stored, want in (("x", (True, True)), (None, (True, True)), ([1], (True, True)),
                         ({"signals": "yes", "news": False}, (True, False)), ({"news": 0}, (True, True))):
        got = clamp_rapid(None, {**DEFAULTS, "triggers": stored})
        check(got["triggers"] == dict(zip(("signals", "news"), want)),
              f"a stored trigger gone bad: its default, the good one kept ({stored!r}): {got['triggers']}")
    stored = {"signals": False, "news": False}
    got = clamp_rapid(None, {**DEFAULTS, "triggers": stored})
    check(got["triggers"] == stored and got["triggers"] is not stored, "a stored dict: copied, never shared")


@scenario
def clamp_symbols():
    app = ["EURUSD", "GBPUSD", "XAUUSD"]
    got = clamp_rapid({"symbols": ["EURUSD", "EURUSD", 5, ["x"], {"a": 1}, "XAGUSD", "XAUUSD"]}, None, app)
    check(got["symbols"] == ["EURUSD", "XAUUSD"], f"strings, once, only the app's pairs: {got['symbols']}")
    got = clamp_rapid({"symbols": ["EURUSD", "XAGUSD"]})
    check(got["symbols"] == ["EURUSD", "XAGUSD"], "no pair list given: kept as sent")
    check(clamp_rapid({"symbols": []}, {**DEFAULTS, "symbols": ["EURUSD"]})["symbols"] == [], "empty: no restriction")
    for bad in ("EURUSD", None, 5, {"EURUSD": 1}):
        raises(lambda: clamp_rapid({"symbols": bad}), "invalid_symbols", f"symbols = {bad!r}")
    check(clamp_rapid(None, {**DEFAULTS, "symbols": "EURUSD"})["symbols"] == [], "stored symbols not a list: none")
    got = clamp_rapid(None, {**DEFAULTS, "symbols": ["EURUSD", 3, "GBPJPY"]}, app)
    check(got["symbols"] == ["EURUSD"], f"stored: strings, the app's pairs: {got['symbols']}")
    got = clamp_rapid({"orders": 4}, {**DEFAULTS, "symbols": ["GBPJPY"]})
    check(got["symbols"] == ["GBPJPY"], "None keeps the stored list as it is")
    stored = ["EURUSD"]
    check(clamp_rapid(None, {**DEFAULTS, "symbols": stored})["symbols"] is not stored, "a fresh list")


@scenario
def clamp_idempotent():
    rnd = random.Random(7)
    pool = {"orders": [0, 3, 41, 12.7], "sl_unit": ["pips", "money"], "risk_unit": ["pct", "usd"],
            "risk_min": [0.01, 0.3, 4, 30, 99999], "risk_max": [0.02, 0.9, 6, 70], "sl_pips": [0.5, 9, 250],
            "lots": [0.001, 0.37, 300], "tp_min_usd": [0, 11, 5000, 20000], "tp_max_usd": [0.5, 14, 9000],
            "limit_pips": [-1, 0.7, 9], "offset_pips": [0.1, 3, 99], "expire_seconds": [1, 45, 900],
            "hold_seconds": [1, 40, 900], "max_hold_seconds": [3, 100, 4000], "loss_unit": ["pct", "usd"],
            "daily_loss": [0.1, 6, 2e6], "max_runs": [0, 3, 8], "cooldown_seconds": [-1, 60, 9999],
            "enabled": [True, False], "analysis": [True, False], "triggers": [{"news": False}, {"signals": False}],
            "symbols": [["EURUSD"], [], ["XAUUSD", "AAA"]]}
    for i in range(400):
        body = {k: rnd.choice(v) for k, v in pool.items() if rnd.random() < 0.4}
        cur = clamp_rapid({k: rnd.choice(v) for k, v in pool.items() if rnd.random() < 0.4}) \
            if rnd.random() < 0.5 else None
        got = clamp_rapid(body, cur, ["EURUSD", "XAUUSD"])
        what = f"#{i} {body} over {cur} -> {got}"
        check(clamp_rapid(None, got) == got and clamp_rapid(got, got) == got, f"clamping again changes nothing: {what}")
        check(got["risk_min"] <= got["risk_max"] and got["tp_min_usd"] <= got["tp_max_usd"], f"in order: {what}")
        if "max_hold_seconds" in got:
            check(got["hold_seconds"] <= got["max_hold_seconds"] <= MAX_HOLD_TOP, f"X <= Y <= top: {what}")
        else:
            check("max_hold_seconds" not in body and (cur is None or "max_hold_seconds" not in cur),
                  f"Y only when sent or stored: {what}")
        for key, rng in LIMITS.items():
            if isinstance(rng, dict):
                rng = rng[got["loss_unit" if key == "daily_loss" else "risk_unit"]]
            check(rng[0] <= got[key] <= rng[1], f"{key} in its range: {what}")


# ── confidence, risk and take-profit ───────────────────────────
@scenario
def confidence():
    for score, want in ((0.62, 0.62), (-0.8, 0.8), (1.7, 1.0), (-3, 1.0), (0, 0.0), (-0.0, 0.0), ("0.5", 0.5),
                        (None, 0.0), (NAN, 0.0), (INF, 0.0), ("x", 0.0), (True, 0.0), ([1], 0.0)):
        got = confidence_from_score(score)
        check(near(got, want) and 0 <= got <= 1, f"confidence_from_score({score!r}) = {got}, want {want}")
    for stretch, z, want in ((2.0, 2.0, 0.0), (-2.0, 2.0, 0.0), (-3.0, 2.0, 0.5), (3.0, 2.0, 0.5), (4.0, 2.0, 1.0),
                             (-5.0, 2.0, 1.0), (1.0, 2.0, 0.0), (0.0, 2.0, 0.0), (2.6, 2.0, 0.3),
                             (None, 2.0, 0.0), (NAN, 2.0, 0.0), (3.0, None, 0.0), (3.0, 0.0, 0.0), (3.0, -2.0, 0.0),
                             (3.0, NAN, 0.0), (True, 2.0, 0.0), (3.0, 1.5, 1.0), (2.25, 1.5, 0.5)):
        got = confidence_from_stretch(stretch, z)
        check(near(got, want) and 0 <= got <= 1, f"confidence_from_stretch({stretch!r}, {z!r}) = {got}, want {want}")


@scenario
def risk_and_tp_at():
    c = clamp_rapid(None)
    for conf, risk, tp in ((0, 0.2, 10.0), (1, 1.0, 15.0), (0.5, 0.6, 12.5), (0.25, 0.4, 11.25), (-1, 0.2, 10.0),
                           (2, 1.0, 15.0), (None, 0.2, 10.0), (NAN, 0.2, 10.0), ("x", 0.2, 10.0), (True, 0.2, 10.0)):
        check(near(risk_at(c, conf), risk) and near(tp_usd_at(c, conf), tp),
              f"c {conf!r}: risk {risk_at(c, conf)} want {risk}, tp {tp_usd_at(c, conf)} want {tp}")
    u = clamp_rapid({"risk_unit": "usd", "tp_min_usd": 20, "tp_max_usd": 20})
    check(near(risk_at(u, 0.5), 30.0) and near(tp_usd_at(u, 0.9), 20.0), "usd risk 10..50; a flat TP range")


# ── size_order ─────────────────────────────────────────────────
@scenario
def size_pips_mode():
    c = clamp_rapid(None)                      # pips: SL 8, risk 0.2..1.0 % per order, TP 10..15 $
    got = size_order(c, 0.5, 10_000, EURUSD, 0.2)
    # 0.6 % of 10 000 = 60 at an 8 pip stop: 0.75 lots; TP 12.5 $ = 1.667 pips on 0.75 lots: 17 points
    check(not got.get("skip") and near(got["money"], 60.0) and near(got["sl_pips"], 8.0) and near(got["lots"], 0.75)
          and near(got["tp_usd"], 12.5) and near(got["tp_pips"], 1.7) and got["tp_clamped"] is False,
          f"EURUSD at c 0.5: {got}")
    check(set(got) == {"lots", "sl_pips", "tp_pips", "tp_usd", "money", "tp_clamped"}, f"the keys: {sorted(got)}")
    got = size_order(c, 1, 10_000, EURUSD, 0.2)
    check(near(got["money"], 100.0) and near(got["lots"], 1.25) and near(got["tp_usd"], 15.0)
          and near(got["tp_pips"], 1.2), f"c 1: 1 % = 100 at 8 pips = 1.25 lots, 15 $ = 1.2 pips: {got}")
    got = size_order(c, 0, 10_000, EURUSD, 0.2)
    check(near(got["money"], 20.0) and near(got["lots"], 0.25) and near(got["tp_pips"], 4.0),
          f"c 0: 0.2 % = 20 at 8 pips = 0.25 lots, 10 $ = 4 pips: {got}")
    got = size_order(c, 0.33, 7_777, EURUSD, 0.3)
    check(got["lots"] * got["sl_pips"] * 10 <= got["money"] + 1e-9 and on_step(got["lots"], EURUSD),
          f"the money is never exceeded, lots DOWN to the step: {got}")
    got = size_order(c, 0, 100, EURUSD, 0.2)
    check(set(got) == {"skip"} and "too little" in got["skip"],
          f"0.2 % of 100 at 8 pips is under 0.01 lots: skipped: {got}")
    got = size_order(c, 0, 1000, EURUSD, 4.0)
    # the spread 4 pips: the floor is 3 spreads = 12 pips, the 8 pip stop raised to it (2 / (12 × 10) = 0.016)
    check(near(got["sl_pips"], 12.0) and near(got["lots"], 0.01) and got["lots"] * 12 * 10 <= got["money"] + 1e-9,
          f"a stop under the floor is raised to it, the lots from it: {got}")
    got = size_order(clamp_rapid({"sl_pips": 8.25}), 1, 10_000, EURUSD, 0.2)
    check(near(got["sl_pips"], 8.3) and whole_points(got["sl_pips"], EURUSD), f"the stop UP to the point: {got}")
    got = size_order(c, 1, 10_000, USDJPY, 0.3)
    # 100 at 8 pips of 6.7 $: 1.865 -> 1.86 lots; 15 / (1.86 × 6.7) = 1.2036 pips -> 12 points
    check(near(got["lots"], 1.86) and near(got["sl_pips"], 8.0) and near(got["tp_pips"], 1.2),
          f"USDJPY (6.7 $ a pip): {got}")
    for bal in (0, -5, None, NAN, "x"):
        got = size_order(c, 1, bal, EURUSD, 0.2)
        check(set(got) == {"skip"} and "balance" in got["skip"], f"balance {bal!r}: skipped: {got}")
    got = size_order(c, 1, 10_000, replace(EURUSD, pip_value_per_lot=0), 0.2)
    check(set(got) == {"skip"}, f"no pip value: skipped: {got}")
    for what, spec in (("no pip", replace(EURUSD, pip=0.0)), ("no point", replace(EURUSD, point=0.0))):
        got = size_order(DEFAULTS, 0.5, 10_000, spec, 1.0)
        check(got == {"skip": "no contract data for EURUSD (pip, point, pip value or lot step)"},
              f"{what}: skipped, never a division by it: {got}")
    got = size_order(c, 1, 10_000_000, EURUSD, 0.2)
    check(near(got["lots"], 100.0), f"at most volume_max per order: {got}")


@scenario
def size_money_mode():
    c = clamp_rapid({"sl_unit": "money"})      # the lot 0.10 at confidence 1, the stop from the money
    got = size_order(c, 1, 10_000, EURUSD, 0.2)
    check(near(got["lots"], 0.10) and near(got["money"], 100.0) and near(got["sl_pips"], 100.0)
          and near(got["tp_pips"], 15.0) and got["tp_clamped"] is False,
          f"c 1: 0.10 lots, 100 at risk: a 100 pip stop, 15 $ = 15 pips: {got}")
    got = size_order(c, 0, 10_000, EURUSD, 0.2)
    check(near(got["lots"], 0.02) and near(got["money"], 20.0) and near(got["sl_pips"], 100.0)
          and near(got["tp_pips"], 50.0),
          f"c 0: the lot × 0.2 / 1.0 = 0.02, 20 at risk: the same 100 pip stop; 10 $ = 50 pips: {got}")
    got = size_order(c, 0.5, 10_000, EURUSD, 0.2)
    check(near(got["lots"], 0.06) and near(got["sl_pips"], 100.0), f"c 0.5: 0.10 × 0.6 = 0.06 lots: {got}")
    got = size_order(clamp_rapid({"sl_unit": "money", "lots": 0.01}), 0, 10_000, EURUSD, 0.2)
    check(set(got) == {"skip"} and "minimum lot" in got["skip"], f"0.01 × 0.2 = 0.002 lots: skipped: {got}")
    got = size_order(c, 1, 100_000, EURUSD, 0.2)
    check(near(got["sl_pips"], SL_CAP_PIPS) and near(got["lots"], 0.10) and near(got["money"], 1000.0),
          f"1000 at 0.10 lots would be 1000 pips: capped at {SL_CAP_PIPS} (it risks less): {got}")
    got = size_order(c, 0, 1000, EURUSD, 5.0)
    # 2 at risk on 0.02 lots = 10 pips, the floor 15 (3 spreads): the stop 15, the lots cut to 2 / 150 = 0.0133
    check(near(got["sl_pips"], 15.0) and near(got["lots"], 0.01) and 0.01 * 15 * 10 <= got["money"] + 1e-9,
          f"under the floor: the floor, the lots cut to fit the money: {got}")
    got = size_order(c, 0, 1000, EURUSD, 8.0)
    check(set(got) == {"skip"} and "too little" in got["skip"], f"0.01 lots at a 24 pip floor lose 2.4 > 2: {got}")
    got = size_order(clamp_rapid({"sl_unit": "money", "lots": 300}), 1, 10_000_000, EURUSD, 0.2)
    check(near(got["lots"], 100.0), f"at most volume_max per order: {got}")
    for what, spec in (("no pip", replace(EURUSD, pip=0.0)), ("no point", replace(EURUSD, point=0.0))):
        got = size_order(c, 0.5, 10_000, spec, 1.0)
        check(got == {"skip": "no contract data for EURUSD (pip, point, pip value or lot step)"},
              f"{what}: skipped, never a division by it: {got}")
    gotm = size_order(clamp_rapid({"sl_unit": "money", "risk_unit": "usd"}), 1, 10_000, EURUSD, 0.2)
    check(near(gotm["money"], 50.0) and near(gotm["lots"], 0.10) and near(gotm["sl_pips"], 50.0),
          f"usd: 50 $ on 0.10 lots = 50 pips: {gotm}")
    got = size_order(clamp_rapid({"sl_unit": "money", "risk_unit": "usd", "risk_min": 5000, "risk_max": 5000}), 1,
                     1000, EURUSD, 0.2)
    check(near(got["money"], 100.0), f"$ capped at 10 % of the balance: {got}")


@scenario
def size_tp_clamp():
    c = clamp_rapid(None)
    got = size_order(c, 0, 1_000_000, EURUSD, 0.2)
    # 2000 at 8 pips = 25 lots; 10 $ on 25 lots = 0.04 pips, under the floor (0.6 pips: 3 spreads)
    check(near(got["lots"], 25.0) and near(got["tp_pips"], 0.6) and got["tp_clamped"] is True,
          f"a TP under the floor: raised to it, flagged: {got}")
    got = size_order(clamp_rapid({"sl_pips": 2, "tp_min_usd": 100, "tp_max_usd": 100}), 1, 1000, EURUSD, 0.2)
    # 10 at 2 pips = 0.5 lots; 100 $ on 0.5 lots = 20 pips, over 3 × the 2 pip stop
    check(near(got["lots"], 0.5) and near(got["tp_pips"], 6.0) and got["tp_clamped"] is True,
          f"a TP over 3 stops: cut to it, flagged: {got}")
    got = size_order(clamp_rapid({"sl_pips": 2, "tp_min_usd": 35, "tp_max_usd": 35}), 1, 1000, EURUSD, 0.2)
    # 35 $ on 0.5 lots = 7 pips, 3.5 stops
    check(near(got["lots"], 0.5) and near(got["tp_pips"], 6.0) and got["tp_clamped"] is True,
          f"a TP of 3.5 stops: cut to 3: {got}")
    got = size_order(clamp_rapid({"sl_pips": 2, "tp_min_usd": 30, "tp_max_usd": 30}), 1, 1000, EURUSD, 0.2)
    check(near(got["tp_pips"], 6.0) and got["tp_clamped"] is False, f"exactly 3 stops: not clamped: {got}")
    got = size_order(c, 0, 1_000_000, EURUSD, None)
    check(near(got["tp_pips"], 0.1) and got["tp_clamped"] is True, f"no spread known: the floor is one point: {got}")
    got = size_order(c, 0, 1_000_000, replace(EURUSD, freeze_distance=0.0002), 0.1)
    check(near(got["tp_pips"], 2.1) and got["tp_clamped"] is True,
          f"the freeze level plus the spread (2.1) over 3 spreads (0.3): {got}")


@scenario
def size_gold_scale():
    c = clamp_rapid(None)
    got = size_order(c, 1, 10_000, XAUUSD, 1.5)
    # 8 pips × 5 = 40 pips (4.00 $ of gold); 100 at 40 pips of 10 $ = 0.25 lots; 15 $ = 6 pips; the floor 4.5
    check(near(got["sl_pips"], 40.0) and near(got["lots"], 0.25) and near(got["tp_pips"], 6.0)
          and whole_points(got["sl_pips"], XAUUSD) and got["tp_clamped"] is False, f"XAUUSD pips: {got}")
    got = size_order(c, 0, 1_000_000, XAUUSD, 1.5)
    check(near(got["tp_pips"], floor_of(XAUUSD, 1.5)) and near(got["tp_pips"], 4.5) and got["tp_clamped"],
          f"gold's floor: 3 spreads (4.5) over the freeze + spread (3.5): {got}")
    got = size_order(c, 0, 1_000_000, XAUUSD, 0.2)
    check(near(got["tp_pips"], 2.2) and got["tp_clamped"], f"... the freeze + spread (2.2) over 3 spreads: {got}")
    m = clamp_rapid({"sl_unit": "money"})
    got = size_order(m, 1, 10_000, XAUUSD, 1.5)
    check(near(got["sl_pips"], 100.0) and near(got["lots"], 0.10), f"XAUUSD money: 100 on 0.10 lots: {got}")
    got = size_order(clamp_rapid({"sl_unit": "money", "risk_max": 5, "risk_min": 5}), 1, 50_000, XAUUSD, 1.5)
    check(near(got["sl_pips"], SL_CAP_PIPS * 5) and near(got["money"], 2500.0),
          f"the cap is 200 × 5 = 1000 pips on gold: {got}")
    got = size_order(clamp_rapid({"sl_pips": 3}), 1, 10_000, GBPJPY, 0.8)
    check(near(got["sl_pips"], 6.0) and whole_points(got["sl_pips"], GBPJPY), f"GBPJPY: 3 pips × 2: {got}")


@scenario
def size_random():
    rnd = random.Random(11)
    seen = {"skip": 0, "clamp_lo": 0, "clamp_hi": 0, "plain": 0, "raised": 0}
    for i in range(3000):
        spec = rnd.choice((EURUSD, USDJPY, XAUUSD, GBPJPY, replace(EURUSD, volume_step=0.1, volume_min=0.1),
                           replace(XAUUSD, min_stop_distance=1.0)))
        body = {"sl_unit": rnd.choice(("pips", "money")), "risk_unit": rnd.choice(("pct", "usd")),
                "sl_pips": rnd.choice((1, 1.5, rnd.uniform(1, 5), rnd.uniform(1, 200))),
                "lots": rnd.choice((0.01, 0.05, 0.1, 0.37, 2, 150)),
                "tp_min_usd": rnd.uniform(1, 60), "tp_max_usd": rnd.uniform(1, 200)}
        body["risk_min"] = rnd.uniform(0.05, 5) if body["risk_unit"] == "pct" else rnd.uniform(1, 2000)
        body["risk_max"] = rnd.uniform(0.05, 5) if body["risk_unit"] == "pct" else rnd.uniform(1, 2000)
        c = clamp_rapid(body)
        conf, bal = rnd.random(), rnd.choice((50, 300, 1000, 10_000, 250_000))
        spread = rnd.choice((None, 0.0, rnd.uniform(0, 6)))
        got = size_order(c, conf, bal, spec, spread)
        what = f"#{i} {spec.symbol} {c} c {conf} bal {bal} spread {spread}: {got}"
        if "skip" in got:
            check(set(got) == {"skip"} and isinstance(got["skip"], str) and got["skip"], f"a reason: {what}")
            seen["skip"] += 1
            continue
        money = money_for(c["risk_unit"], risk_at(c, conf), bal)
        fl = floor_of(spec, spread)
        check(near(got["money"], money), f"the money of risk_at(c): {what}")
        check(got["lots"] >= spec.volume_min - 1e-12 and got["lots"] <= spec.volume_max + 1e-12
              and on_step(got["lots"], spec), f"lots on the step, volume_min..volume_max: {what}")
        check(got["lots"] * got["sl_pips"] * spec.pip_value_per_lot <= money * (1 + 1e-9),
              f"NEVER above the money at the stop: {what}")
        check(whole_points(got["sl_pips"], spec) and whole_points(got["tp_pips"], spec), f"whole points: {what}")
        check(got["sl_pips"] >= fl - 1e-9 and got["tp_pips"] >= fl - 1e-9, f"not under the floor: {what}")
        check(got["tp_pips"] <= TP_MAX_SL * got["sl_pips"] + 1e-9, f"TP at most 3 stops: {what}")
        if c["sl_unit"] == "pips":
            check(got["sl_pips"] >= c["sl_pips"] * spec.scale - 1e-9, f"never under the set stop: {what}")
            if got["sl_pips"] > c["sl_pips"] * spec.scale + spec.point / spec.pip + 1e-9:
                seen["raised"] += 1
        else:
            check(got["sl_pips"] <= max(SL_CAP_PIPS * spec.scale, fl + spec.point / spec.pip) + 1e-9,
                  f"at most the cap: {what}")
        want_tp = c["tp_min_usd"] + conf * (c["tp_max_usd"] - c["tp_min_usd"])
        check(near(got["tp_usd"], want_tp), f"tp_usd_at(c): {what}")
        raw = want_tp / (got["lots"] * spec.pip_value_per_lot)
        if got["tp_clamped"]:
            seen["clamp_lo" if raw < got["tp_pips"] else "clamp_hi"] += 1
        else:
            check(abs(raw - got["tp_pips"]) <= spec.point / spec.pip / 2 + 1e-9, f"the TP of tp_usd: {what}")
            seen["plain"] += 1
    check(all(v >= 30 for v in seen.values()), f"every branch exercised: {seen}")


# ── fit_orders ─────────────────────────────────────────────────
@scenario
def fit_orders_limits():
    check(fit_orders(10, 10.0, 1000.0, 50, 20.0, 10_000.0) == (10, None), "all fit")
    check(fit_orders(10, 10.0, None, None, None, None) == (10, None), "nothing known: no cap")
    check(fit_orders(10, 10.0, 35.0, None, None, None) == (3, "daily limit"), "35 of daily room at 10 each: 3")
    check(fit_orders(10, 10.0, 30.0, None, None, None) == (3, "daily limit"), "exactly 3")
    check(fit_orders(10, 0.1, 0.3, None, None, None) == (3, "daily limit"), "0.3 / 0.1 is 3, whatever the float")
    check(fit_orders(10, 10.0, 100.0, None, None, None) == (10, None), "exactly all")
    for room in (5.0, 0.0, -20.0, NAN, "x"):
        check(fit_orders(10, 10.0, room, None, None, None) == (0, "daily limit"), f"room {room!r}: skipped")
    check(fit_orders(10, 10.0, None, 4, None, None) == (4, "pending-order limit"), "4 pending slots left")
    for room in (0, -3, NAN):
        check(fit_orders(10, 10.0, None, room, None, None) == (0, "pending-order limit"), f"pending {room!r}")
    check(fit_orders(10, 10.0, None, 10, None, None) == (10, None), "exactly the pending room")
    check(fit_orders(10, 10.0, None, None, 100.0, 1000.0) == (5, "free margin"),
          f"half of 1000 free margin at 100 an order: 5 ({RAPID_MARGIN_USE})")
    check(fit_orders(10, 10.0, None, None, 100.0, 2000.0) == (10, None), "exactly the margin")
    for need in (None, 0, -1, NAN, "x"):
        check(fit_orders(10, 10.0, None, None, need, 10.0) == (10, None), f"margin need {need!r} unknown: no cap")
    for free in (None, 0, -50, NAN):
        check(fit_orders(10, 10.0, None, None, 100.0, free) == (0, "free margin"), f"free margin {free!r}: none")
    check(fit_orders(10, 10.0, 30.0, 5, 100.0, 800.0) == (3, "daily limit"), "the tightest: daily 3, pending 5, m 4")
    check(fit_orders(10, 10.0, 60.0, 5, 100.0, 800.0) == (4, "free margin"), "the tightest: margin 4")
    check(fit_orders(10, 10.0, 60.0, 2, 100.0, 800.0) == (2, "pending-order limit"), "the tightest: pending 2")
    check(fit_orders(10, 10.0, 40.0, 4, 100.0, 800.0) == (4, "daily limit"), "a tie: the first of them")
    check(fit_orders(10, 10.0, 0.0, 0, 100.0, 0.0) == (0, "daily limit"), "all none: skipped, the daily limit")
    for n in (0, -2, None, "x", True):
        check(fit_orders(n, 10.0, 100.0, 5, 1.0, 100.0) == (0, "no orders"), f"n {n!r}: no orders")
    for m in (0, -1, None, NAN):
        check(fit_orders(10, m, 100.0, 5, 1.0, 100.0) == (0, "no money to risk"), f"money {m!r}")
    check(fit_orders(3, 10.0, 1e9, 1000, 1.0, 1e9) == (3, None), "never more than asked")


# ── stop-limit prices ──────────────────────────────────────────
@scenario
def stop_limit_eurusd():
    bid, ask = 1.08000, 1.08002
    check(stop_limit_prices("buy", bid, ask, EURUSD, 2, 0) == (1.08022, 1.08021),
          "buy: trigger 2 pips over the ask, the limit one point under it (no stops level: never AT the trigger)")
    check(stop_limit_prices("sell", bid, ask, EURUSD, 2, 0) == (1.0798, 1.07981), "sell: under the bid, limit over")
    sl5 = replace(EURUSD, min_stop_distance=0.00005)
    check(stop_limit_prices("buy", bid, ask, sl5, 2, 0) == (1.08022, 1.08017), "limit_pips 0 = the stops level")
    check(stop_limit_prices("sell", bid, ask, sl5, 2, 0) == (1.0798, 1.07985), "... sell")
    check(stop_limit_prices("buy", bid, ask, sl5, 2, 0.3) == (1.08022, 1.08017), "a limit closer than the stops level")
    check(stop_limit_prices("buy", bid, ask, sl5, 2, 1) == (1.08022, 1.08012), "limit_pips 1: 10 points behind")
    check(stop_limit_prices("sell", bid, ask, sl5, 2, 1.5) == (1.0798, 1.07995), "... sell, 15 points over")
    check(stop_limit_prices("buy", bid, ask, sl5, 2.37, 0) == (1.08026, 1.08021),
          "an offset of 23.7 points: 24, away from the market")
    check(stop_limit_prices("sell", bid, ask, sl5, 2.37, 0) == (1.07976, 1.07981), "... sell: 24 under the bid")
    fz = replace(EURUSD, freeze_distance=0.00018)                       # 18 + the 2 point spread = 20 = the offset
    check(stop_limit_prices("buy", bid, ask, fz, 2, 0) == (1.08022, 1.08021), "the offset exactly freeze + spread")
    fz = replace(EURUSD, freeze_distance=0.00019)
    for side in ("buy", "sell"):
        got = stop_limit_prices(side, bid, ask, fz, 2, 0)
        check(isinstance(got, str) and "freeze" in got, f"{side}: 20 points < 19 + 2: a reason, not prices: {got}")
    got = stop_limit_prices("buy", bid, 1.00004, replace(EURUSD, freeze_distance=0.00017), 2, 0)
    check(isinstance(got, str), f"ask under the bid: no price: {got}")
    got = stop_limit_prices("buy", 1.0, 1.00004, replace(EURUSD, freeze_distance=0.00017), 2, 0)
    check(isinstance(got, str), f"a wide spread (4) + the freeze (17) over 20 points: {got}")
    got = stop_limit_prices("buy", bid, ask, replace(EURUSD, min_stop_distance=0.00025), 2, 0)
    check(isinstance(got, str), f"a trigger inside the stops level: the broker would refuse it: {got}")
    for side, b, a in (("up", bid, ask), (None, bid, ask), ("buy", None, ask), ("sell", bid, None),
                       ("buy", 0, ask), ("buy", NAN, ask), ("sell", bid, "x"), ("buy", True, ask)):
        check(isinstance(stop_limit_prices(side, b, a, EURUSD, 2, 0), str), f"{side} {b!r} {a!r}: a reason")
    check(isinstance(stop_limit_prices("buy", bid, ask, EURUSD, None, 0), str), "no offset: a reason")
    check(isinstance(stop_limit_prices("buy", bid, ask, EURUSD, 0, 0), str), "offset 0: a reason")


@scenario
def stop_limit_other_symbols():
    # gold: 2 digits, 5 points stops level, 20 points freeze, scale 5: 2 pips × 5 = 10 pips = 1.00
    got = stop_limit_prices("buy", 2650.00, 2650.15, XAUUSD, 2, 0)
    check(got == (2651.15, 2651.10), f"gold buy: 1.00 over the ask, the limit 5 points under: {got}")
    got = stop_limit_prices("sell", 2650.00, 2650.15, XAUUSD, 2, 0)
    check(got == (2649.0, 2649.05), f"gold sell: {got}")
    got = stop_limit_prices("buy", 2650.00, 2650.15, XAUUSD, 2, 0.5)
    check(got == (2651.15, 2650.9), f"gold limit_pips 0.5 × 5 = 25 points: {got}")
    got = stop_limit_prices("buy", 2650.00, 2650.15, XAUUSD, 0.5, 0)
    check(isinstance(got, str), f"gold 0.5 × 5 = 25 points < freeze 20 + spread 15: {got}")
    got = stop_limit_prices("buy", 2650.00, 2650.15, XAUUSD, 0.7, 0)
    check(got == (2650.5, 2650.45), f"gold 0.7 × 5 = 35 points = freeze + spread: placed: {got}")
    for side in ("buy", "sell"):
        t, lim = stop_limit_prices(side, 2650.03, 2650.18, XAUUSD, 3.3, 0.21)
        check(round(t, 2) == t and round(lim, 2) == lim, f"gold {side}: 2 digits: {t} {lim}")
        check((lim < t) if side == "buy" else (lim > t), f"{side}: the limit on the pullback side")
        check(abs(t - lim) >= XAUUSD.min_stop_distance - 1e-9, f"{side}: at least the stops level apart")
    got = stop_limit_prices("buy", 150.000, 150.010, USDJPY, 2, 0)
    check(got == (150.03, 150.029), f"USDJPY: 3 digits: {got}")
    got = stop_limit_prices("sell", 190.000, 190.020, GBPJPY, 2, 0)
    check(got == (189.96, 189.963), f"GBPJPY: 2 pips × 2, the limit 3 points (its stops level) over: {got}")


@scenario
def stop_limit_random():
    rnd = random.Random(5)
    for i in range(3000):
        spec = rnd.choice((EURUSD, USDJPY, XAUUSD, GBPJPY))
        spec = replace(spec, min_stop_distance=rnd.choice((0, 3, 5, 12)) * spec.point,
                       freeze_distance=rnd.choice((0, 5, 20, 40)) * spec.point)
        mid = {"EURUSD": 1.08, "USDJPY": 150.0, "XAUUSD": 2650.0, "GBPJPY": 190.0}[spec.symbol]
        bid = round(mid + rnd.randint(-5000, 5000) * spec.point, spec.digits)
        ask = round(bid + rnd.randint(0, 40) * spec.point, spec.digits)
        side, off, lim = rnd.choice(("buy", "sell")), rnd.uniform(0.5, 50), rnd.uniform(0, 5)
        got = stop_limit_prices(side, bid, ask, spec, off, lim)
        what = f"#{i} {side} {bid} {ask} {spec.symbol} stops {spec.min_stop_distance} freeze " \
               f"{spec.freeze_distance} off {off} lim {lim}: {got}"
        k, pt = spec.scale, spec.point
        need = spec.freeze_distance + (ask - bid)
        if isinstance(got, str):
            check(off * k * spec.pip < max(need, spec.min_stop_distance) + pt, f"a reason only when too close: {what}")
            continue
        t, li = got
        check(round(t, spec.digits) == t and round(li, spec.digits) == li, f"the symbol's digits: {what}")
        dist = (t - ask) if side == "buy" else (bid - t)
        check(dist >= off * k * spec.pip - 1e-9 and dist < off * k * spec.pip + pt + 1e-9,
              f"the trigger the offset away, rounded away from the market: {what}")
        check(dist >= need - 1e-9 and dist >= spec.min_stop_distance - 1e-9, f"outside freeze + spread: {what}")
        gap = (t - li) if side == "buy" else (li - t)
        check(gap >= max(lim * k * spec.pip, spec.min_stop_distance, pt) - 1e-9
              and gap < max(lim * k * spec.pip, spec.min_stop_distance, pt) + pt + 1e-9,
              f"the limit behind the trigger: limit_pips, at least the stops level and a point: {what}")


# ── split_volume, local comments ───────────────────────────────
@scenario
def split_volume_netting():
    check(split_volume(1.0, EURUSD) == [1.0], "one order under volume_max")
    check(split_volume(10 * 0.07, EURUSD) == [0.7], "10 × 0.07 (0.7000000000000001): one order of 0.7")
    check(split_volume(100.0, EURUSD) == [100.0], "exactly volume_max: one")
    check(split_volume(200.0, EURUSD) == [100.0, 100.0], "twice volume_max: two")
    got = split_volume(250.0, EURUSD)
    check(got == [83.34, 83.33, 83.33] and near(sum(got), 250.0), f"over volume_max: evenly, on the step: {got}")
    got = split_volume(100.01, EURUSD)
    check(got == [50.01, 50.0], f"just over: two halves: {got}")
    got = split_volume(1.239, EURUSD)
    check(got == [1.23], f"DOWN to the step: {got}")
    for bad in (0, -1, 0.005, None, NAN, INF, "x"):
        check(split_volume(bad, EURUSD) == [], f"{bad!r}: no order")
    check(split_volume(0.05, replace(EURUSD, volume_step=0.1, volume_min=0.1)) == [], "under volume_min: none")
    fine = replace(EURUSD, volume_min=0.1)                              # a step finer than the minimum lot
    check(split_volume(0.05, fine) == [] and split_volume(0.099, fine) == [],
          "whole steps, but under volume_min: none (the broker would refuse it)")
    check(split_volume(0.1, fine) == [0.1] and split_volume(0.15, fine) == [0.15], "volume_min and over: one")
    got = split_volume(4000.0, GBPJPY)
    check(got == [50.0] * 80, f"40 orders × 100 lots on a 50 lot max: 80 orders: {len(got)}")
    check(split_volume(500.0, replace(EURUSD, volume_max=INF)) == [500.0], "no volume_max: one")
    rnd = random.Random(3)
    for i in range(500):
        spec = replace(EURUSD, volume_max=rnd.choice((1.0, 5.0, 50.0, 100.0)),
                       volume_step=rnd.choice((0.01, 0.1)), volume_min=0.1)
        total = rnd.uniform(0.1, 400)
        got = split_volume(total, spec)
        want = math.floor(total / spec.volume_step + 1e-9) * spec.volume_step
        check(near(sum(got), want, 1e-6) and all(on_step(v, spec) and spec.volume_min <= v <= spec.volume_max
                                                 for v in got)
              and len(got) == math.ceil(want / spec.volume_max - 1e-9) and max(got) - min(got) <= spec.volume_step
              + 1e-9, f"#{i} {total} {spec.volume_max} {spec.volume_step}: {got}")


@scenario
def local_comments():
    rid = 1_798_000_000_123                    # a 13-digit ms id (until the year 2286)
    for i in range(100):
        c = local_comment(rid, i)
        m = RUN_TAG.match(c)
        check(len(c) <= COMMENT_MAX == 20 and c == f"fxr:{rid}:{i}", f"{c}: at most 20 chars")
        check(m and m.group(1) == "r" and int(m.group(2)) == rid, f"{c}: read back as Rapid run {rid}")
    check(len(local_comment(9_999_999_999_999, 99)) == 20, "the longest: 20")
    check(local_comment(int(1.73e12), 0) == "fxr:1730000000000:0", "an int id")
    raises(lambda: local_comment(rid, 100), "comment_too_long", "i 100 on a 13-digit id: 21 chars")
    check(len(local_comment(10**13, 9)) == 20, "a 14-digit id with a one-digit i still fits: 20")
    raises(lambda: local_comment(10**13, 10), "comment_too_long", "a 14-digit id with i 10: 21 chars")
    for bad in ((-1, 0), (rid, -1), (True, 0), (rid, False)):
        raises(lambda: local_comment(*bad), "invalid_comment", f"{bad}")


@scenario
def module_surface():
    for name in ("DEFAULTS", "LIMITS", "UNIT_DEFAULTS", "clamp_rapid", "confidence_from_score",
                 "confidence_from_stretch", "risk_at", "tp_usd_at", "size_order", "fit_orders", "stop_limit_prices",
                 "split_volume", "local_comment", "RUN_KIND", "RAPID_MARGIN_USE"):
        check(hasattr(rapid, name), f"bot/rapid.py has {name}")


# ── the connector's local runs ─────────────────────────────────
# loaded by the first of these scenarios (connector_kit): test_short_term's harness, the real Connector and Scalper on
# test_burst_connector's simulated broker with a tick feed, the rule (S.RULE) and the stretch (S.STRETCH) as each
# scenario sets them per symbol, the connector's clock on a Wednesday noon, every file in a temporary folder
asyncio = C = S = T = None
RUNNER = []                     # the event loop the connector scenarios share (its clock can be pushed forward)
PIP = 0.0001
RAPID = {"enabled": True, "triggers": {"signals": True, "news": False}, "orders": 3}
# the rule's signals, 3 of its units from the average (z 2): confidence 0.5 -> 0.6 % of 10 000 = 60 per order, at the
# 8 pip stop 0.75 lots, the take-profit 12.5 $ = 1.7 pips; EURUSD at 1.10000 / 1.10005: a buy's trigger 1.10025, its
# limit 1.10020 (the 0.5 pip stops level behind it), SL 1.09940, TP 1.10037
BUY = {"side": "buy", "strength": 0.5, "sl_pips": 5, "stretch": -3.0}
SELL = {"side": "sell", "strength": 0.5, "sl_pips": 5, "stretch": 3.0}


def connector_kit():
    global asyncio, C, S, T
    import asyncio
    sys.path.insert(0, str(ROOT / "tests"))
    import test_short_term as S
    C, T = S.C, S.T
    C.Connector.BURST_POLL, C.Connector.BURST_RESULT_WAIT, C.Connector.CLOSE_PAUSE = 0.01, 0.01, 0.01
    logging.basicConfig(level=logging.CRITICAL)


def in_loop(coro):
    """Runs a connector scenario; what it left running (a report still waiting, a scalper not stopped on a failure)
    is cancelled after it, so it never acts in the next one."""
    if not RUNNER:
        try:
            connector_kit()
        except BaseException:
            coro.close()
            raise
        RUNNER.append(asyncio.Runner(loop_factory=T.SkewLoop))

    async def settled():
        try:
            await coro
        finally:
            left = asyncio.all_tasks() - {asyncio.current_task()}
            for task in left:
                task.cancel()
            if left:
                await asyncio.wait(left, timeout=2)
    RUNNER[0].run(settled())


def harness(broker=None, short=None, rapid=None, **settings):
    """test_short_term's harness, on a hedging account (one ticket per order) by default, short-term off and Rapid's
    signal runs on (RAPID, then `rapid`)."""
    return S.harness(broker or S.TickBroker(mode="hedging"), {"enabled": False, **(short or {})},
                     rapid={**RAPID, **(rapid or {})}, **settings)


def runs(h):
    return [st for st in h.c.bursts.values() if st.get("local")]


def rapid_opens(h):
    return [m["trade"] for m in h.of("trade_opened") if m["trade"].get("strategy") == RUN_KIND]


def rapid_closes(h):
    return [m["trade"] for m in h.of("trade_closed") if m["trade"].get("strategy") == RUN_KIND]


def rapid_file(h):
    return h.c._rapid_file(h.c.scalper._open_file())


def today():
    return C.utcnow().date().isoformat()


def starts(h):
    """The symbols of the signals the Scalper hands to start_local from here on."""
    got, real = [], h.c.start_local

    async def start_local(sym, d):
        got.append(sym)
        return await real(sym, d)
    h.c.start_local = start_local
    return got


def fill(b, symbol="EURUSD", side="buy"):
    """The price through a run's triggers (2 pips out) and back to their limits (half a pip behind them): each order
    fills at its limit, half a pip in loss (-3.75 on 0.75 lots)."""
    bid, k = b.bids[symbol], 1 if side == "buy" else -1
    b.move(bid + k * 2 * PIP, symbol)
    b.move(bid + k * 1.5 * PIP, symbol)


@contextlib.contextmanager
def logged():
    """The connector's log lines (INFO and up) while inside, in a list; none printed."""
    lines, log = [], C.log
    keep = log.level, log.propagate
    handler = logging.Handler(logging.INFO)
    handler.emit = lambda record: lines.append(record.getMessage())
    log.setLevel(logging.INFO)
    log.propagate = False
    log.addHandler(handler)
    try:
        yield lines
    finally:
        log.removeHandler(handler)
        log.setLevel(keep[0])
        log.propagate = keep[1]


@contextlib.contextmanager
def one_folder():
    """Every connector made inside keeps its files (scalp_*.json, rapid_*.json) in one temporary folder, as the next
    start of a connector finds the last one's: that folder."""
    folder = Path(tempfile.mkdtemp(dir=T.TMP))
    real = C.Scalper._open_file, C.Scalper._day_file
    C.Scalper._open_file = lambda self: folder / "scalp_open.json"
    C.Scalper._day_file = lambda self: folder / "scalp_day.json"
    try:
        yield folder
    finally:
        C.Scalper._open_file, C.Scalper._day_file = real


@scenario
async def signal_starts_a_local_run_on_demo_only():
    S.RULE.clear()
    b, h = harness()

    async def no_sweep():
        return 0
    sweep, h.c.sweep_orphans = h.c.sweep_orphans, no_sweep      # no sweep writes rapid_open.json meanwhile
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a signal's run places its orders")
    S.RULE.clear()
    check(len(b.expire_ins) == 3 and all(140 < e <= 150.5 for e in b.expire_ins),
          f"broker-side expiry = its start (a local run's release) + Rapid's expire (30 s) + 120 s from now: "
          f"{b.expire_ins}")
    st = runs(h)[0]
    rid = st["id"]
    await S.until(lambda: st["status"] == "armed", "the run armed")
    saved = json.loads(rapid_file(h).read_text())[str(rid)]
    check(sorted(saved["orders"]) == sorted(str(t) for t in st["placed"]) and len(saved["orders"]) == 3
          and not saved["trades"], f"its 3 orders kept in rapid_open.json once placed, before any fill: {saved}")
    check(not h.of("burst_update"), f"armed: no burst_update for a local run: {h.of('burst_update')}")
    check(list(h.c.bursts) == [rid] and st["kind"] == RUN_KIND and st["sides"] == ("buy",) and near(st["c"], 0.5)
          and abs(rid - time.time() * 1000) < 60_000, f"a local run, its id its start in ms: {rid} {st['sides']} "
                                                       f"{st['c']}")
    got = b.calls_of("place")
    check([c[6] for c in got] == [local_comment(rid, i) for i in range(3)] and {c[1] for c in got} == {"buy"},
          f"the signal's side only, each order fxr:<id>:<i>: {got}")
    check(all(near(c[2], 0.75) and near(c[3], 1.10025) and near(c[7], 1.10020) and near(c[4], 1.09940)
              and near(c[5], 1.10037) for c in got), f"stop-limits sized at confidence 0.5, the trigger 2 pips out, "
                                                     f"TP 12.5 $ from the limit: {got}")
    check(not h.of("burst_update") and not b.calls_of("open") and not S.short_opens(h),
          f"no Burst row (no burst_update ever), no scalp: {h.of('burst_update')}")
    fill(b)
    await S.until(lambda: len(rapid_opens(h)) == 3, "its fills reported")
    check(len(json.loads(rapid_file(h).read_text())[str(rid)]["trades"]) == 3,
          f"each fill kept in rapid_open.json as it is reported: {rapid_file(h).read_text()}")
    h.c.sweep_orphans = sweep
    opens = rapid_opens(h)
    check(all("burst_id" not in t and near(t["entry_price"], 1.10020) and near(t["lots"], 0.75) for t in opens),
          f"trade_opened: strategy rapid, no burst: {opens}")
    await h.c.push_snapshot()
    snap = h.of("snapshot")[-1]["positions"]
    check(len(snap) == 3 and all(p["strategy"] == RUN_KIND and p["burst_id"] is None for p in snap),
          f"the snapshot says rapid, with no burst_id (its id, a start in ms, is no Burst row's): {snap}")
    await S.jump(31)
    await S.until(lambda: len(rapid_closes(h)) == 3 and not h.c.bursts, "closed at its hold time, the run over")
    check(not h.of("burst_update"), f"over: no burst_update ever for a local run: {h.of('burst_update')}")
    closes = rapid_closes(h)
    total = round(sum(t["pnl"] for t in closes), 2)
    check(near(total, -11.25) and all("burst_id" not in t for t in closes), f"trade_closed: strategy rapid: {closes}")
    check(near(h.c.rapid_day.day["pnl"], total) and near(h.c.rapid_day.free(0.0), 500 + total),
          f"booked to Rapid's own day (5 % of 10 000): {h.c.rapid_day.day}")
    check(near(h.c.rapid_pnl_today(), total), f"the snapshot's positions, closed and booked, are not counted again: "
                                              f"{h.c.rapid_pnl_today()}")
    check(h.c.rapid_cool["EURUSD"] > asyncio.get_running_loop().time() + 100, "its symbol rests for its cooldown")
    check(json.loads(rapid_file(h).read_text()) == {}, "rapid_open.json forgets a run that is over")
    S.stop(h)

    # netting: one order carries them all
    S.RULE.clear()
    b, h = harness(S.TickBroker(mode="netting"))
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run on netting")
    got, rid = b.calls_of("place"), runs(h)[0]["id"]
    check(len(got) == 1 and near(got[0][2], 2.25) and got[0][6] == local_comment(rid, 0), f"netting: {got}")
    S.stop(h)

    for case in ("a real account", "started on a real account"):
        S.RULE.clear()
        b, h = harness()
        if case == "a real account":
            b.demo = False              # read before each run
        else:
            h.c.demo = False            # as run() read it at the start
        got = starts(h)
        S.RULE["EURUSD"] = dict(BUY)
        with logged() as lines:
            h.c.scalper.start()
            await S.until(lambda: len(got) >= 2, "signals")
        said = [m for m in lines if m == "Rapid: signal runs need a demo account: none starts on this one"]
        check(len(said) == 1 and not b.calls_of("place") and not h.c.bursts,
              f"{case}: no run, told once a day: {said}")
        S.stop(h)


@scenario
async def one_run_per_symbol_then_a_cooldown():
    S.RULE.clear()
    b, h = harness()
    got = starts(h)
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a run")
    first, n = runs(h)[0]["id"], len(got)
    await S.until(lambda: len(got) >= n + 2, "more signals while it runs")
    check(len(runs(h)) == 1 and len(b.calls_of("place")) == 3, "one run per symbol at a time")
    await S.jump(31)
    await S.until(lambda: not h.c.bursts, "its orders expired, the run over")
    check(sum(c[2] == "ok" for c in b.calls_of("cancel")) == 3 and not b.orders, "its orders cancelled at expiry")
    n = len(got)
    await S.until(lambda: len(got) >= n + 2, "signals in its cooldown")
    check(not h.c.bursts and len(b.calls_of("place")) == 3, "none for cooldown_seconds (120) after it")
    await S.jump(121)
    await S.until(lambda: len(b.calls_of("place")) == 6, "a new run once the cooldown is over")
    check(runs(h)[0]["id"] > first, f"a new id: {runs(h)[0]['id']} after {first}")
    S.stop(h)

    # Rapid's own pairs: the rule is asked about those only
    S.RULE.clear()
    S.ASKED.clear()
    b, h = harness(rapid={"symbols": ["GBPUSD"]})
    S.RULE.update(EURUSD=dict(BUY), GBPUSD=dict(SELL))
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a run on GBPUSD")
    check([st["symbol"] for st in runs(h)] == ["GBPUSD"] and {s for s, _, _ in S.ASKED} == {"GBPUSD"}
          and {c[1] for c in b.calls_of("place")} == {"sell"}, f"rapid.symbols: {runs(h)} {set(S.ASKED)}")
    S.stop(h)

    # none of its own: short-term's pairs (short-term off or not); its own before short-term's
    for short, own, want in ((["GBPUSD"], [], "GBPUSD"), (["GBPUSD"], ["EURUSD"], "EURUSD")):
        S.RULE.clear()
        S.ASKED.clear()
        b, h = harness(short={"symbols": short}, rapid={"symbols": own})
        S.RULE.update(EURUSD=dict(BUY), GBPUSD=dict(SELL))
        h.c.scalper.start()
        await S.until(lambda: len(b.calls_of("place")) == 3, f"a run on {want}")
        n = len(S.ASKED)
        await S.until(lambda: len(S.ASKED) >= n + 4, "more steps")
        check([st["symbol"] for st in runs(h)] == [want] and {s for s, _, _ in S.ASKED} == {want}
              and len(b.calls_of("place")) == 3, f"rapid {own}, short-term {short}: runs on {want} only: "
                                                  f"{[st['symbol'] for st in runs(h)]} {set(S.ASKED)}")
        S.stop(h)


@scenario
async def two_signals_in_one_millisecond_two_run_ids():
    """A run's id is its start in ms, and one more than the last when that is not later: two signals of one step (the
    clock not moving between them) start two runs with ids of their own, each run's orders fxr:<its id>:<i>."""
    S.RULE.clear()
    b, h = harness()
    fixed, real = time.time(), time.time
    time.time = lambda: fixed
    try:
        await h.c.start_local("EURUSD", dict(BUY))
        await h.c.start_local("GBPUSD", dict(SELL))
    finally:
        time.time = real
    rid = int(fixed * 1000)
    check(sorted(h.c.bursts) == [rid, rid + 1] and h.c.bursts[rid]["symbol"] == "EURUSD"
          and h.c.bursts[rid + 1]["symbol"] == "GBPUSD", f"two runs, the second's id one more: {sorted(h.c.bursts)}")
    await S.until(lambda: len(b.calls_of("place")) == 6, "both runs place their orders")
    got = b.calls_of("place")
    check([c[6] for c in got if c[1] == "buy"] == [local_comment(rid, i) for i in range(3)]
          and [c[6] for c in got if c[1] == "sell"] == [local_comment(rid + 1, i) for i in range(3)],
          f"each run's orders under its own id: {got}")
    check(set(json.loads(rapid_file(h).read_text())) == {str(rid), str(rid + 1)}, "both kept in rapid_open.json")
    S.stop(h)


@scenario
async def no_local_run_before_a_news_run_or_where_a_trade_is():
    # a news burst on EURUSD checks it 60 s before its release; a run started now lasts its orders' expiry (30 s), then
    # Y at most (none: X, 30 s), and must be over BURST_GAP (15 s) before that check
    for until_s, y, one in ((100, None, False), (200, None, True), (200, 120, False)):
        S.RULE.clear()
        b, h = harness(rapid=None if y is None else {"max_hold_seconds": y})
        await h.c.on_burst(T.plan(31, seconds_until=until_s))
        got = starts(h)
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        what = f"the news run's check {until_s - 60} s away, Y {y or 30}"
        if one:
            await S.until(lambda: len(b.calls_of("place")) == 3, f"a run: {what}")
        else:
            await S.until(lambda: len(got) >= 2, "signals")
            check(not runs(h) and not b.calls_of("place"), f"no run: {what}")
        check(h.c.bursts[31]["status"] == "scheduled", "the news run still waits")
        S.stop(h)

    S.RULE.clear()
    b, h = harness()
    b.add_position("GBPUSD", "buy", 0.1, "fx:9")                         # a long-term trade
    b.add_position("EURUSD", "sell", 0.1, "by hand", magic=0)            # not this bot's
    got = starts(h)
    S.RULE.update(EURUSD=dict(BUY), GBPUSD=dict(SELL))
    h.c.scalper.start()
    await S.until(lambda: len(got) >= 4, "signals on both")
    check(not runs(h) and not b.calls_of("place"), "never next to another position, this bot's or not")
    S.stop(h)

    # the other way round: a news burst and a Rapid news run planned on EURUSD while a local run holds it are skipped
    # at their check, and the local run's orders are left as they are
    S.RULE.clear()
    b, h = harness()
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a local run on EURUSD")
    S.RULE.clear()
    with b.lock:
        local = {t: dict(o) for t, o in b.orders.items()}
    check(len(local) == 3 and all(C.run_tag(o["comment"])[0] == RUN_KIND for o in local.values()),
          f"its 3 fxr: orders: {local}")
    await h.c.on_burst(T.plan(31, seconds_until=61))
    await h.c.on_burst(T.rapid(32, seconds_until=61))
    await S.jump(1.5)                                                    # their check, 60 s before the release
    for bid in (31, 32):
        fin = await T.finished(h, bid)
        check(fin["status"] == "skipped" and fin["reason"] == "another news burst is running on EURUSD",
              f"#{bid} skipped: {fin}")
    got = b.calls_of("place")
    check(len(got) == 3 and not any(c[6].startswith(("fxb:31", "fxb:32")) for c in got), f"nothing placed: {got}")
    with b.lock:
        check(b.orders == local and not b.calls_of("cancel"), f"the local run's orders untouched: {b.orders}")
    check(len(runs(h)) == 1 and runs(h)[0]["status"] == "armed", f"and it goes on: {runs(h)}")
    S.stop(h)


@scenario
async def a_long_term_signal_makes_way():
    S.RULE.clear()
    b, h = harness()
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a run")
    S.RULE.clear()
    rid = runs(h)[0]["id"]
    fill(b)
    await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
    with logged() as lines:
        await h.c.on_signal(S.signal(b, 601, "EURUSD", "buy"), manual=False)
    longs = [m["trade"] for m in h.of("trade_opened") if m["trade"].get("signal_id") == 601]
    check(longs and not h.of("signal_skipped"), f"the signal trades: {h.of('signal_skipped')}")
    check(len(rapid_closes(h)) == 3 and not h.c.bursts and [p["comment"] for p in b.pos.values()] == ["fx:601"],
          f"the run's positions closed first, reported as Rapid's: {[p['comment'] for p in b.pos.values()]}")
    check(f"Rapid run #{rid} on EURUSD: a long-term signal on the same symbol goes first, ending it" in lines
          and sum(m.endswith("(a long-term signal on the same symbol)") for m in lines) == 3, f"logged: {lines}")
    check(near(h.c.rapid_day.day["pnl"], -11.25), f"and booked: {h.c.rapid_day.day}")
    S.stop(h)

    S.RULE.clear()
    b, h = harness()
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run, nothing filled")
    S.RULE.clear()
    await h.c.on_signal(S.signal(b, 602, "EURUSD", "buy"), manual=False)
    check(not b.orders and sum(c[2] == "ok" for c in b.calls_of("cancel")) == 3 and not h.c.bursts
          and [p["comment"] for p in b.pos.values()] == ["fx:602"], f"its orders cancelled, the signal trades: "
                                                                     f"{b.calls_of('cancel')} {h.of('signal_skipped')}")
    S.stop(h)

    # the long-term count leaves the runs' positions out
    S.RULE.clear()
    b, h = harness(max_open_trades=1)
    S.RULE["GBPUSD"] = dict(SELL)
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a run on GBPUSD")
    S.RULE.clear()
    fill(b, "GBPUSD", "sell")
    await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
    await h.c.on_signal(S.signal(b, 603, "EURUSD", "buy"), manual=False)
    longs = [m["trade"] for m in h.of("trade_opened") if m["trade"].get("signal_id") == 603]
    check(longs and not h.of("signal_skipped") and len(runs(h)) == 1,
          f"max_open_trades 1: three Rapid positions do not count, and the run goes on: {h.of('signal_skipped')}")
    S.stop(h)


@scenario
async def the_reservation_cuts_the_count():
    # 150 a day: 2 orders of 60 fit; the run holds those 120 of it until it ends
    S.RULE.clear()
    b, h = harness(rapid={"loss_unit": "usd", "daily_loss": 150, "cooldown_seconds": 0})
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run")
    S.RULE.clear()
    eur = runs(h)[0]
    check(len(b.calls_of("place")) == 2 and near(eur["reserved"], 120)
          and f"Rapid run #{eur['id']}: 2 of 3 orders per side (daily limit)" in h.logs("warn"),
          f"cut to what fits, reported: {eur['reserved']} {h.logs('warn')}")
    S.RULE["GBPUSD"] = dict(SELL)
    skipped = "on GBPUSD skipped: Rapid's daily loss limit leaves no room for an order risking 60.00"
    await S.until(lambda: any(skipped in m for m in h.logs("warn")), "a GBPUSD run skipped")
    check(len(b.calls_of("place")) == 2, "nothing placed on GBPUSD: 30 is left beside the EURUSD run's 120")
    S.RULE.clear()
    await S.jump(31)
    await S.until(lambda: not h.c.bursts, "the EURUSD run expired")
    S.RULE["GBPUSD"] = dict(SELL)
    await S.until(lambda: len(b.calls_of("place")) == 4, "GBPUSD gets 2 once that reservation is gone")
    S.stop(h)

    # 120 a day: the EURUSD run's 2 × 60 take all of it. That is no limit reached (nothing is lost yet), so it is not
    # told as one; a GBPUSD signal finds no room (start_local starts no run where rapid_room is 0, and a run's plan
    # says why: no room for an order). Once 120 is lost, reaching it is told, once.
    S.RULE.clear()
    b, h = harness(rapid={"loss_unit": "usd", "daily_loss": 120})
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run")
    S.RULE.clear()
    eur = runs(h)[0]
    check(len(b.calls_of("place")) == 2 and near(eur["reserved"], 120), f"2 orders of 60 fit: {eur['reserved']}")
    got = starts(h)
    S.RULE["GBPUSD"] = dict(SELL)
    await S.until(lambda: got.count("GBPUSD") >= 2, "GBPUSD signals")
    S.RULE.clear()
    acct = await h.c.call(b.account)
    check(near(await h.c.rapid_room(None, acct), 0.0) and len(runs(h)) == 1 and len(b.calls_of("place")) == 2,
          f"no room left, no GBPUSD run: {runs(h)}")
    why, orders = await h.c._rapid_plan(h.c._local_run(eur["id"] + 1, "GBPUSD", "sell", 0.5, 3.0), acct)
    check(why == "Rapid's daily loss limit leaves no room for an order risking 60.00" and not orders,
          f"a GBPUSD run's plan is skipped for it: {why}")
    check(not any(m.startswith("Rapid daily loss limit reached") for m in h.logs("warn"))
          and h.c.rapid_day.day["warned"] is False, f"not told as the limit reached: {h.logs('warn')}")
    h.c.rapid_day.book(today(), -120.0, 4242)
    n = got.count("GBPUSD")
    S.RULE["GBPUSD"] = dict(SELL)
    await S.until(lambda: got.count("GBPUSD") >= n + 2, "more GBPUSD signals")
    S.RULE.clear()
    told = [m for m in h.logs("warn") if m.startswith("Rapid daily loss limit reached")]
    check(len(told) == 1 and told[0].startswith("Rapid daily loss limit reached (120.00 lost today)")
          and h.c.rapid_day.day["warned"] is True and len(b.calls_of("place")) == 2,
          f"120 lost: told once: {told}")
    S.stop(h)

    S.RULE.clear()
    b, h = harness()
    b.limit_orders = 2                                                   # the account's cap on pending orders
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run")
    rid = runs(h)[0]["id"]
    check(len(b.calls_of("place")) == 2
          and f"Rapid run #{rid}: 2 of 3 orders per side (pending-order limit)" in h.logs("warn"),
          f"cut to the room for pending orders: {h.logs('warn')}")
    S.stop(h)

    # the margin cannot be read: no cap (the broker refuses an order it cannot cover anyway), none reserved
    S.RULE.clear()
    b, h = harness()

    def no_margin(symbol, side, lots):
        raise RuntimeError("order_calc_margin failed: (-10005, 'IPC timeout')")
    b.margin_for = no_margin
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run")
    S.RULE.clear()
    st = runs(h)[0]
    check(len(b.calls_of("place")) == 3 and st["margin"] == 0.0 and near(st["reserved"], 180.0)
          and not any("free margin" in m for m in h.logs("warn")),
          f"no margin known: every order placed, no margin held: {st['margin']} {h.logs('warn')}")
    S.stop(h)


@scenario
async def the_daily_limit_stops_rapid_only():
    S.RULE.clear()
    b, h = harness(rapid={"loss_unit": "usd", "daily_loss": 150})
    h.c.rapid_day.roll(10_000.0)
    h.c.rapid_day.book(today(), -150.0, 4242)
    got = starts(h)
    S.RULE.update(EURUSD=dict(BUY), GBPUSD=dict(SELL))
    h.c.scalper.start()
    await S.until(lambda: got.count("EURUSD") >= 3 and got.count("GBPUSD") >= 3, "signals on both, step after step",
                  timeout=8)
    told = [m for m in h.logs("warn") if m.startswith("Rapid daily loss limit reached (150.00 lost today)")]
    # not even a run that is skipped at once: no run state, no cooldown
    check(len(told) == 1 and not b.calls_of("place") and not h.c.bursts and not h.c.burst_done and not h.c.rapid_cool,
          f"no run, told once that day: {told} {h.c.burst_done} {h.c.rapid_cool}")
    S.RULE.clear()
    await h.c.on_signal(S.signal(b, 701, "EURUSD", "buy"), manual=False)
    check(any(m["trade"].get("signal_id") == 701 for m in h.of("trade_opened")), "the long-term signal still trades")
    S.stop(h)

    # the long-term limit (3 % of the day's start: 300) leaves Rapid's result out: 250 booked today and the 150 an
    # open Rapid position loses now are no long-term loss, and that position does not count against max_open_trades
    S.RULE.clear()
    b, h = harness(max_open_trades=2)
    b.bids["USDCHF"] = 0.9
    await h.c.on_signal(S.signal(b, 702, "GBPUSD", "buy"), manual=False)     # the long-term day starts at 10 000
    b.balance -= 250.0
    h.c.rapid_day.roll(10_000.0)
    h.c.rapid_day.book(today(), -250.0, 4343)
    b.add_position("USDCHF", "buy", 1.0, local_comment(1_798_000_000_000, 0))
    b.move(0.89855, "USDCHF")                                            # 15 pips under its entry: -150
    await h.c.on_signal(S.signal(b, 703, "EURUSD", "buy"), manual=False)
    longs = [m["trade"]["signal_id"] for m in h.of("trade_opened") if m["trade"].get("signal_id")]
    check(longs == [702, 703] and not h.of("signal_skipped")
          and not any("Daily loss limit reached" in m for m in h.logs("warn")),
          f"402.50 down today, 2.50 of it long-term's: {longs} {h.of('signal_skipped')}")
    S.stop(h)

    # nothing booked, an open Rapid position alone losing 300 (the long-term limit 2 %: 200): no long-term loss either
    S.RULE.clear()
    b, h = harness(daily_loss_limit_pct=2.0)
    b.bids["USDCHF"] = 0.9
    await h.c.on_signal(S.signal(b, 704, "GBPUSD", "buy"), manual=False)     # the long-term day starts at 10 000
    b.add_position("USDCHF", "buy", 1.0, local_comment(1_798_000_000_000, 0))
    b.move(0.89705, "USDCHF")                                            # 30 pips under its entry: -300
    held = await h.c.call(b.positions)
    check(near(h.c.rapid_pnl_today(held), -300.0), f"Rapid's result today is what it loses now: "
                                                   f"{h.c.rapid_pnl_today(held)}")
    await h.c.on_signal(S.signal(b, 705, "EURUSD", "buy"), manual=False)
    longs = [m["trade"]["signal_id"] for m in h.of("trade_opened") if m["trade"].get("signal_id")]
    check(longs == [704, 705] and not h.of("signal_skipped") and not h.c.risk.halted,
          f"302.50 down today, 2.50 of it long-term's: {longs} {h.of('signal_skipped')}")
    S.stop(h)


@scenario
async def closes_booked_from_the_sweeper_once():
    S.RULE.clear()
    b, h = harness()
    rid = 1_798_000_000_000                                              # a run that is gone
    t = b.add_position("EURUSD", "buy", 0.75, local_comment(rid, 0))
    other = b.add_position("GBPUSD", "buy", 0.1, "fxb:5:b")              # a news burst's
    check(h.c.symbol_owner("EURUSD", await h.c.call(b.positions)) == RUN_KIND, "what it left holds its symbol")
    deals, result = b.position_deals, b.closed_result
    b.position_deals = lambda position: [] if position == t else deals(position)    # history behind on the fxr: one
    b.closed_result = lambda ticket: None if ticket == t else result(ticket)
    left = await h.c.sweep_orphans()
    closed = {m["trade"]["ticket"]: m["trade"] for m in h.of("trade_closed")}
    check(left == 0 and not b.pos and set(closed) == {other} and t in h.c.unreported,
          f"both swept, the fxr: one's close not read yet: {closed}")
    owed = h.c.rapid_booking.get(t) or {}
    check(near(owed.get("profit"), -3.75) and owed.get("id") == rid and near(h.c.rapid_day.day["pnl"], 0.0),
          f"counted as it showed last, not booked: {h.c.rapid_booking}")
    check(near(h.c.rapid_pnl_today(), -3.75) and near(h.c.rapid_pnl_today([]), -3.75),
          f"Rapid's result today counts it: {h.c.rapid_pnl_today()}")
    del b.position_deals, b.closed_result                                # history caught up
    await h.c.sweep_orphans()
    closed = {m["trade"]["ticket"]: m["trade"] for m in h.of("trade_closed")}
    check(set(closed) == {t, other} and len(h.of("trade_closed")) == 2 and not h.c.unreported
          and not h.c.rapid_booking and near(h.c.rapid_pnl_today(), -3.75),
          f"the next sweep reports it, once; booked, no longer as seen: {closed}")
    check(closed[t]["strategy"] == RUN_KIND and closed[t]["burst_id"] == rid and near(closed[t]["pnl"], -3.75)
          and "strategy" not in closed[other], f"the fxr: one reported as Rapid's: {closed}")
    check(near(h.c.rapid_day.day["pnl"], -3.75) and h.c.rapid_booked == {t: True},
          f"booked to Rapid's day, the news burst's not: {h.c.rapid_day.day}")
    await h.c.report_closed(t, 0.01, {"strategy": RUN_KIND})
    check(near(h.c.rapid_day.day["pnl"], -3.75), "a ticket is booked once")
    # a result that cannot be read: the loss it showed last, until it is read
    h.c.known[777] = {"ticket": 777, "symbol": "EURUSD", "profit": -5.0, "comment": local_comment(rid, 1)}
    await h.c._rapid_closed(777, None)
    check(near(h.c.rapid_day.day["pnl"], -8.75) and h.c.rapid_booked[777] is False
          and any(m.startswith("Rapid #777: its result could not be read") for m in h.logs("warn")),
          f"a guess, the user told: {h.c.rapid_day.day}")
    check(near(h.c.rapid_pnl_today(), -8.75), "its loss is not counted again as open")
    await h.c._rapid_closed(777, None)
    check(near(h.c.rapid_day.day["pnl"], -8.75), "not guessed twice")
    res = {"pnl": -4.0, "exit_price": 1.1, "exit_time": C.utcnow().isoformat(timespec="seconds")}
    await h.c._rapid_closed(777, res)
    check(near(h.c.rapid_day.day["pnl"], -7.75) and h.c.rapid_booked[777] is True, "the result read replaces it")
    await h.c._rapid_closed(777, res)
    check(near(h.c.rapid_day.day["pnl"], -7.75), "and is never booked again")
    S.stop(h)


@scenario
async def the_sweeper_names_what_it_cleaned_up():
    """Its message names whose orders / positions it removed: a news burst's in v1.2.0's words, a Rapid run's (a local
    run's fxr:, a news run of a Rapid runner: rapid_news) as Rapid's, both when it was both; what it could not remove
    is not named (it is in the log, the next sweep tries again)."""
    S.RULE.clear()
    rid = 1_798_000_000_000                                              # runs that are gone
    classic = lambda b: (b.add_order("GBPUSD", "buy", 0.1, 1.3, "fxb:5:0"), b.add_position("GBPUSD", "sell", 0.1,
                                                                                             "fxb:5:1"))
    local = lambda b: (b.add_position("EURUSD", "buy", 0.75, local_comment(rid, 0)),)
    news = lambda b: (b.add_order("EURUSD", "buy", 0.75, 1.102, "fxb:7:0"),)       # its id in rapid_news
    for case, made, want in (
            ("a news burst's", (classic,), "an earlier news burst: removed 1 pending order(s), closed 1 position(s)"),
            ("a local Rapid run's", (local,), "an earlier Rapid run: removed 0 pending order(s), closed 1 position(s)"),
            ("a Rapid news run's", (news,), "an earlier Rapid run: removed 1 pending order(s), closed 0 position(s)"),
            ("both", (classic, local, news),
             "an earlier news burst and Rapid run: removed 2 pending order(s), closed 2 position(s)")):
        b, h = harness()
        h.c.rapid_news.add(7)
        for make in made:
            make(b)
        check(await h.c.sweep_orphans() == 0 and not b.pos and not b.orders, f"{case}: all swept")
        said = [m for m in h.logs() if m.startswith("Cleaned up")]
        check(said == [f"Cleaned up after {want}"] and said[0] in h.logs("warn"), f"{case}: {said}")
        S.stop(h)
    b, h = harness()            # the news burst's position cannot be closed, its order is removed: both still named
    h.c.rapid_news.add(7)
    order, stuck = classic(b)
    local(b)
    real = b.close_position

    def close_position(x, comment):
        if x["ticket"] == stuck:
            raise RuntimeError("market closed")
        return real(x, comment)
    b.close_position = close_position
    check(await h.c.sweep_orphans() == 1 and stuck in b.pos and order not in b.orders, "the news burst's left")
    said = [m for m in h.logs() if m.startswith("Cleaned up")]
    check(said == ["Cleaned up after an earlier news burst and Rapid run: removed 1 pending order(s), "
                   "closed 1 position(s)"], f"its order removed, a Rapid position closed: {said}")
    del b.close_position
    h.msgs.clear()
    h.c.rapid_news.add(7)                                # forgotten by the last sweep: nothing of it was left then
    news(b)
    await h.c.sweep_orphans()
    said = [m for m in h.logs() if m.startswith("Cleaned up")]
    check(said == ["Cleaned up after an earlier news burst and Rapid run: removed 1 pending order(s), "
                   "closed 1 position(s)"] and not b.pos and not b.orders, f"the next sweep: {said}")
    S.stop(h)
    b, h = harness()            # nothing of the news burst's can be removed (its order, its position): Rapid's alone
    order, stuck = classic(b)
    local(b)
    real = b.close_position

    def close_position(x, comment):
        if x["ticket"] == stuck:
            raise RuntimeError("market closed")
        return real(x, comment)

    def cancel_order(ticket):
        raise RuntimeError("market closed")
    b.close_position, b.cancel_order = close_position, cancel_order
    check(await h.c.sweep_orphans() == 2 and list(b.pos) == [stuck] and order in b.orders,
          "the news burst's order and position left, the Rapid position closed")
    said = [m for m in h.logs() if m.startswith("Cleaned up")]
    check(said == ["Cleaned up after an earlier Rapid run: removed 0 pending order(s), closed 1 position(s)"],
          f"what it could not remove is not named (only the Rapid run's): {said}")
    del b.close_position, b.cancel_order
    S.stop(h)
    for case, made, want in (("a news burst's", (classic,), "an earlier news burst"),
                             ("a local Rapid run's", (local,), "an earlier Rapid run"),
                             ("a Rapid news run's order alone", (news,), "an earlier Rapid run"),
                             ("both", (classic, local), "an earlier news burst and Rapid run")):
        b, h = harness()
        h.c.dry_run = True
        h.c.rapid_news.add(7)                            # the news run's id: its pending order alone names it
        for make in made:
            make(b)
        n = len(b.orders), len(b.pos)
        await h.c.sweep_orphans()
        said = [m for m in h.logs() if m.startswith("[dry-run]")]
        check(said == [f"[dry-run] would remove {n[0]} order(s) and close {n[1]} position(s) left by {want}"]
              and (len(b.orders), len(b.pos)) == n, f"dry-run, {case}: {said}")
        S.stop(h)


@scenario
async def a_restart_takes_up_what_the_last_run_left():
    S.RULE.clear()
    folder = Path(tempfile.mkdtemp(dir=T.TMP))
    real = C.Scalper._open_file, C.Scalper._day_file
    C.Scalper._open_file = lambda self: folder / "scalp_open.json"
    C.Scalper._day_file = lambda self: folder / "scalp_day.json"
    try:
        b, h = harness()
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await S.until(lambda: len(b.calls_of("place")) == 3, "a run on EURUSD")
        S.RULE.clear()
        fill(b)
        await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
        S.RULE["GBPUSD"] = dict(SELL)
        await S.until(lambda: any(st["symbol"] == "GBPUSD" and st["status"] == "armed" for st in runs(h)),
                      "a GBPUSD run")
        S.RULE.clear()
        eur, gbp = (next(st["id"] for st in runs(h) if st["symbol"] == s) for s in ("EURUSD", "GBPUSD"))
        # a GBPUSD order out of the book unexplained (the sweeper's to look up: unchecked): it filled, and its
        # position closed at its stop, with none of its deals in history yet
        gst = next(st for st in runs(h) if st["id"] == gbp)
        with b.lock:
            u = min(gst["pending"])
            rec = gst["pending"].pop(u)
            o = b.orders.pop(u)
            b.done[u] = ("filled", o)
            b._fill(o, o["price_stoplimit"])
            out = b._ticket()
            b._close(b.pos[u], o["lots"], o["sl"], order=out)
        b.hide(u, 60)
        b.hide(out, 60)
        h.c.unchecked[u] = [gst, rec, 0]
        h.c._save_rapid_open()
        saved = json.loads((folder / "rapid_open.json").read_text())
        check(set(saved) == {str(eur), str(gbp)} and len(saved[str(eur)]["trades"]) == 3
              and len(saved[str(gbp)]["orders"]) == 3 and str(u) in saved[str(gbp)]["orders"]
              and not saved[str(eur)]["orders"], f"kept, the order in unchecked too: {saved}")
        # the connector dies: nothing more of it reaches MT5 or its files
        h.c._save_rapid_open = lambda: None

        async def hang(*a, **kw):
            await asyncio.Event().wait()
        h.c.call = hang
        tasks = [st["task"] for st in runs(h)]
        S.stop(h)
        await asyncio.sleep(0.05)
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks, timeout=2)
        trades = sorted(t for t, p in b.pos.items() if p["symbol"] == "EURUSD")
        with b.lock:                                    # one meets its stop or target while no connector runs
            b._close(b.pos[trades[0]], 0.75, b.bids["EURUSD"])
            # and a GBPUSD order fills at its limit, its position closed at its stop (8 pips on 0.75 lots: -60)
            g = min(t for t, o in b.orders.items() if o["symbol"] == "GBPUSD")
            o = b.orders.pop(g)
            b.done[g] = ("filled", o)
            b._fill(o, o["price_stoplimit"])
            b._close(b.pos[g], o["lots"], o["sl"])
        check(len(b.orders) == 1 and len(b.pos) == 2, f"left behind: {b.orders} {b.pos}")
        x = min(b.orders)                               # and the broker expires another GBPUSD order meanwhile
        b.expire(x)
        b.hidden.clear()                                # the unchecked order's deals in history by now

        b, h = harness(b)
        check(set(h.c.rapid_left) == {eur, gbp}, f"the next start reads them: {h.c.rapid_left}")
        await h.c._rapid_adopt()
        check(set(h.c.unchecked) == {g, u} and x not in h.c.unchecked,
              f"the fills looked up (the one unchecked before too), the order its history shows gone unfilled "
              f"dropped at once: {list(h.c.unchecked)}")
        await h.c.sweep_orphans()
        await S.until(lambda: len(rapid_closes(h)) == 5, "the five trades reported")
        closes = rapid_closes(h)
        total = round(sum(t["pnl"] for t in closes), 2)
        check(not b.orders and not b.pos and sorted(t["ticket"] for t in closes) == sorted(trades + [g, u])
              and near(total, -131.25), f"orders removed, positions closed at once, each reported: {closes}")
        check(near(next(t["pnl"] for t in closes if t["ticket"] == g), -60.0)
              and near(next(t["pnl"] for t in closes if t["ticket"] == u), -60.0),
              f"the fills that came and went while it was away too, with their result: {closes}")
        check(near(h.c.rapid_day.day["pnl"], total), f"and booked to Rapid's day: {h.c.rapid_day.day}")
        check(json.loads((folder / "rapid_open.json").read_text()) == {} and not h.c.unreported
              and not h.c.unchecked, "nothing left to take up")
        S.stop(h)
    finally:
        C.Scalper._open_file, C.Scalper._day_file = real


@scenario
async def a_trade_booked_before_a_crash_is_not_booked_again():
    """The last run reported and booked a trade, then died before rapid_open.json followed (it still lists the
    trade): rapid_day.json keeps the tickets booked with the day, so the next start neither reports nor books it
    again; the run's other trades are taken up as before."""
    S.RULE.clear()
    with one_folder() as folder:
        b, h = harness()
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await S.until(lambda: len(b.calls_of("place")) == 3, "a run on EURUSD")
        S.RULE.clear()
        rid = runs(h)[0]["id"]
        fill(b)
        await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
        trades = sorted(b.pos)
        with b.lock:                                    # one at its take-profit (1.7 pips on 0.75 lots: +12.75)
            b._close(b.pos[trades[0]], 0.75, 1.10037)
        await S.until(lambda: len(rapid_closes(h)) == 1, "its close reported")
        check(rapid_closes(h)[0]["ticket"] == trades[0] and near(h.c.rapid_day.day["pnl"], 12.75)
              and json.loads((folder / "rapid_day.json").read_text())["booked"] == [trades[0]],
              f"booked, its ticket kept with the day: {(folder / 'rapid_day.json').read_text()}")
        check(len(json.loads((folder / "rapid_open.json").read_text())[str(rid)]["trades"]) == 3,
              "rapid_open.json not written since: it still lists the trade")
        # the connector dies: nothing more of it reaches MT5 or its files
        h.c._save_rapid_open = lambda: None

        async def hang(*a, **kw):
            await asyncio.Event().wait()
        h.c.call = hang
        tasks = [st["task"] for st in runs(h)]
        first = h
        S.stop(h)
        await asyncio.sleep(0.05)
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks, timeout=2)

        b, h = harness(b)
        check(set(h.c.rapid_left) == {rid} and h.c.rapid_booked == {trades[0]: True},
              f"the next start reads the run and what was booked of it: {h.c.rapid_booked}")
        await h.c._rapid_adopt()
        check(set(h.c.unreported) == set(trades[1:]) and not h.c.rapid_booking.get(trades[0]),
              f"the booked trade is not taken up again: {list(h.c.unreported)}")
        await h.c.sweep_orphans()
        await S.until(lambda: len(rapid_closes(h)) == 2, "the other two reported")
        closes = rapid_closes(h)
        check(not b.pos and sorted(t["ticket"] for t in closes) == trades[1:],
              f"the other two closed and reported: {closes}")
        check(sum(t["ticket"] == trades[0] for t in rapid_closes(first) + closes) == 1,
              "one trade_closed for the trade booked before the crash")
        check(near(h.c.rapid_day.day["pnl"], 12.75 - 7.5) and sorted(h.c.rapid_day.day["booked"]) == trades,
              f"booked once: {h.c.rapid_day.day}")
        check(json.loads((folder / "rapid_open.json").read_text()) == {} and not h.c.unreported,
              "nothing left to take up")
        S.stop(h)


@scenario
async def a_close_not_reported_yet_is_kept_for_the_next_start():
    """A run's trade closed at its stop while its deals are not in history yet (handed to the sweeper: unreported):
    rapid_open.json keeps it among the run's trades, also once the run is over, so the next start after a crash
    reports it and books it to Rapid's day, once."""
    S.RULE.clear()
    with one_folder() as folder:
        b, h = harness()
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await S.until(lambda: len(b.calls_of("place")) == 3, "a run on EURUSD")
        S.RULE.clear()
        rid = runs(h)[0]["id"]
        fill(b)
        await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
        trades = sorted(b.pos)
        t0 = trades[0]
        with b.lock:                                    # at its stop (8 pips on 0.75 lots: -60), history behind
            out = b._ticket()
            b._close(b.pos[t0], 0.75, 1.09940, order=out)
        b.hide(out, 600)
        await S.until(lambda: t0 in h.c.unreported, "handed to the sweeper, unread")
        await h.c.sweep_orphans()
        saved = json.loads((folder / "rapid_open.json").read_text())
        check(set(saved[str(rid)]["trades"]) == {str(t) for t in trades} and not rapid_closes(h),
              f"closed, not reported: still kept with the run's other trades: {saved}")
        await S.jump(31)
        await S.until(lambda: len(rapid_closes(h)) == 2 and not h.c.bursts, "the other two closed at the hold time")
        saved = json.loads((folder / "rapid_open.json").read_text())
        check(list(saved) == [str(rid)] and list(saved[str(rid)]["trades"]) == [str(t0)]
              and t0 in h.c.unreported, f"the run over: kept for that trade alone: {saved}")
        # the connector dies: nothing more of it reaches MT5 or its files
        h.c._save_rapid_open = lambda: None

        async def hang(*a, **kw):
            await asyncio.Event().wait()
        h.c.call = hang
        first = h
        S.stop(h)
        await asyncio.sleep(0.05)
        b.hidden.clear()                                # its deals in history by now

        b, h = harness(b)
        check(set(h.c.rapid_left) == {rid}, f"the next start reads the run: {h.c.rapid_left}")
        await h.c._rapid_adopt()
        await h.c.sweep_orphans()
        await S.until(lambda: rapid_closes(h), "reported")
        closes = rapid_closes(first) + rapid_closes(h)
        mine = [t for t in closes if t["ticket"] == t0]
        check(len(mine) == 1 and mine[0]["strategy"] == RUN_KIND and near(mine[0]["pnl"], -60.0),
              f"reported once, as Rapid's, with its result: {mine}")
        check(near(h.c.rapid_day.day["pnl"], -60.0 - 7.5) and h.c.rapid_day.day["booked"].count(t0) == 1
              and sorted(h.c.rapid_day.day["booked"]) == trades and h.c.rapid_booked.get(t0) is True,
              f"booked to Rapid's day once: {h.c.rapid_day.day}")
        check(json.loads((folder / "rapid_open.json").read_text()) == {} and not h.c.unreported,
              "nothing left to take up")
        S.stop(h)


@scenario
async def closed_and_not_booked_yet_still_counts_as_rapids():
    """Rapid trades that closed at their stop while their result cannot be read yet: until it is booked, each counts
    to Rapid's day as it showed last (rapid_booking), so the long-term daily limit (1 %: 100) does not take that loss
    for its own and stop, and Rapid's own limit does not miss it."""
    S.RULE.clear()
    b, h = harness(daily_loss_limit_pct=1.0)
    h.c.risk.update_day(C.utcnow().date(), 10_000.0)                  # the long-term day starts at 10 000
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a run on EURUSD")
    S.RULE.clear()
    st = runs(h)[0]
    fill(b)
    await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
    b.move(1.09950)                                                      # 7 pips under the fills: -52.50 each
    await S.until(lambda: len(st["trades"]) == 3 and all(near(tr.get("profit"), -52.5)
                                                         for tr in st["trades"].values()), "seen at -52.50")
    b.closed_result = lambda ticket: None                                # history behind: nothing of the closes
    b.position_deals = lambda position: []
    b.move(1.09935)                                                      # through the stops: -63.75 each
    await S.until(lambda: len(h.c.unreported) == 3, "handed to the sweeper, unread")
    check(not rapid_closes(h) and not b.pos and near(h.c.rapid_day.day["pnl"], 0.0),
          f"none reported or booked yet: {h.c.rapid_day.day}")
    check(len(h.c.rapid_booking) == 3 and all(near(x["profit"], -52.5) for x in h.c.rapid_booking.values()),
          f"each as it showed last: {h.c.rapid_booking}")
    check(near(h.c.rapid_pnl_today(), -157.5) and near(h.c.rapid_pnl_today([]), -157.5),
          f"Rapid's result today counts them: {h.c.rapid_pnl_today()}")
    acct = await h.c.call(b.account)
    check(near(await h.c.rapid_room(None, acct), 500.0 - 157.5), "and so does what is free of its limit (500)")
    await h.c.on_signal(S.signal(b, 801, "EURUSD", "buy"), manual=False)
    check(any(m["trade"].get("signal_id") == 801 for m in h.of("trade_opened")) and not h.of("signal_skipped")
          and not h.c.risk.halted and not any("Daily loss limit reached" in m for m in h.logs("warn")),
          f"191.25 down, 33.75 of it long-term's: the signal trades: {h.of('signal_skipped')}")
    del b.closed_result, b.position_deals                                # history caught up
    await h.c.sweep_orphans()
    await S.until(lambda: len(rapid_closes(h)) == 3, "the three reported")
    check(not h.c.rapid_booking and near(h.c.rapid_day.day["pnl"], -191.25)
          and near(h.c.rapid_pnl_today(), -191.25), f"booked by their result, no longer as seen: {h.c.rapid_day.day}")
    S.stop(h)


@scenario
async def a_trade_listed_again_counts_once():
    """A Rapid trade out of the list of positions for a moment counts as it showed last (rapid_booking); listed again,
    it counts as the position it is, and not a second time from rapid_booking."""
    S.RULE.clear()
    b, h = harness()
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: len(b.calls_of("place")) == 3, "a run on EURUSD")
    S.RULE.clear()
    st = runs(h)[0]
    fill(b)
    await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
    b.move(1.09950)                                                      # 7 pips under the fills: -52.50 each
    await S.until(lambda: len(st["trades"]) == 3 and all(near(tr.get("profit"), -52.5)
                                                         for tr in st["trades"].values()), "seen at -52.50")
    pid = min(st["trades"])
    b.unlist(pid, 0.1)                                                   # not listed for a moment
    await S.until(lambda: pid in h.c.rapid_booking and st["trades"][pid]["gone"] > 0,
                  "counted as it showed last while not listed")
    check(near(h.c.rapid_booking[pid]["profit"], -52.5) and not st["trades"][pid]["closed"],
          f"as it showed last, not taken as closed: {h.c.rapid_booking}")
    await S.until(lambda: st["trades"][pid]["gone"] == 0, "listed again")
    held = await h.c.call(b.positions)
    check(pid in {x["ticket"] for x in held} and pid in h.c.rapid_booking, f"listed: {held}")
    check(near(h.c.rapid_pnl_today(held), -157.5), f"counted once, as a position: {h.c.rapid_pnl_today(held)}")
    await h.c.push_snapshot()
    check(near(h.c.rapid_pnl_today(), -157.5), f"as last seen too: {h.c.rapid_pnl_today()}")
    acct = await h.c.call(b.account)
    check(near(await h.c.rapid_room(None, acct), 500.0 - 180.0),
          "what is free of its limit (500): its run holds its reservation (180), more than the 157.50 it loses")
    S.stop(h)


@scenario
async def a_signal_run_past_the_long_term_daily_limit():
    """The long-term daily limit (1 %: 100) is passed and not checked yet: a local run is Rapid's (its own limit),
    so it places its orders and the long-term limit is not checked, nor halted, by it."""
    S.RULE.clear()
    b, h = harness(daily_loss_limit_pct=1.0)
    h.c.risk.update_day(C.utcnow().date(), 10_000.0)                  # the long-term day starts at 10 000
    b.balance -= 250.0                                                   # 250 down: over the limit
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h) and runs(h)[0]["status"] == "armed", "a run on EURUSD")
    S.RULE.clear()
    check(len(b.calls_of("place")) == 3 and not h.c.risk.halted
          and not any("daily loss limit (1.0%) reached" in m for m in h.logs()),
          f"its 3 stop-limits placed, the long-term limit not halted: {h.c.risk.halted} {h.logs()}")
    S.stop(h)


@scenario
async def the_stop_sweeps_mixed_runs():
    S.RULE.clear()
    b, h = harness(short={"enabled": True})
    S.RULE["GBPUSD"] = dict(SELL)
    h.c.scalper.start()
    await S.until(lambda: S.short_opens(h), "a scalp on GBPUSD")
    S.RULE.clear()
    check(not h.c.bursts, "short-term on: its signals are scalps")
    h.c.apply_settings({**h.c.settings, "short_term": {**S.SHORT, "enabled": False}})
    S.RULE["EURUSD"] = dict(BUY)
    await S.until(lambda: len(b.calls_of("place")) == 3, "short-term off: a Rapid run on EURUSD")
    S.RULE.clear()
    fill(b)
    await S.until(lambda: len(rapid_opens(h)) == 3, "filled")
    b.bids["USDCHF"] = 0.9
    await h.c.on_burst({**T.plan(77), "symbol": "USDCHF"})              # a news burst, v1.2.0's
    await T.armed(h, 77)
    b.move(0.9007, "USDCHF")                                             # its buy stops (7 pips out) fill
    await S.until(lambda: any(m["trade"].get("burst_id") == 77 for m in h.of("trade_opened")), "the burst filled")
    long_term = b.add_position("GBPUSD", "buy", 0.1, "fx:9")
    by_hand = b.add_position("EURUSD", "sell", 0.2, "by hand", magic=0)
    main = asyncio.ensure_future(asyncio.sleep(60))
    await h.c.shutdown(main, budget=15)
    left = [x["comment"] for x in [*b.orders.values(), *b.pos.values()] if C.run_tag(x["comment"])]
    check(not left and sorted(b.pos) == sorted([long_term, by_hand]), f"nothing of a run left, the rest kept: {left}")
    closes = rapid_closes(h)
    check(len(closes) == 3 and near(h.c.rapid_day.day["pnl"], round(sum(t["pnl"] for t in closes), 2)),
          f"the Rapid run's closes reported and booked: {closes} {h.c.rapid_day.day}")
    check(S.short_closes(h) and any(m["trade"].get("burst_id") == 77 for m in h.of("trade_closed")),
          "the scalp and the news burst closed too")
    await S.until(lambda: main.done(), "run() cancelled")
    errors = [m for m in h.logs("error") if "clean-up was over" in m or m.startswith("Could not close")]
    check(not errors and main.cancelled(), f"all of it within the stop: {errors}")
    S.stop(h)


@scenario
async def a_signal_run_held_past_x_until_it_turns_against():
    # its pair is still read once no new run may start there (the signal trigger switched off, the pair out of
    # Rapid's list since): the rule turning against the run still ends it before Y
    for case, change in (("as it started", {}), ("its signal trigger off since", {"triggers": {"signals": False}}),
                         ("Rapid's pairs GBPUSD only since", {"symbols": ["GBPUSD"]})):
        S.RULE.clear()
        S.STRETCH.clear()
        b, h = harness(rapid={"max_hold_seconds": 120})
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await S.until(lambda: len(b.calls_of("place")) == 3, f"{case}: a run")
        S.RULE.clear()
        fill(b)
        await S.until(lambda: len(rapid_opens(h)) == 3, f"{case}: filled")
        if change:
            h.c.apply_settings({**h.c.settings, "rapid": {**h.c.settings["rapid"], **change}})
        await S.jump(31)
        await asyncio.sleep(0.3)
        check(len(b.pos) == 3 and not rapid_closes(h), f"{case}: past X, in loss and not turned against: held (Y 120)")
        with logged() as lines:
            S.STRETCH["EURUSD"] = -4.0          # a whole unit further from its average than at the signal (-3)
            await S.until(lambda: len(rapid_closes(h)) == 3, f"{case}: closed once the rule turned against it")
        check(not b.pos and sum(m.endswith("(turned against the signal)") for m in lines) == 3,
              f"{case}: logged: {lines}")
        S.STRETCH.clear()
        S.stop(h)


@scenario
async def rapid_off_leaves_the_scalper_as_it_was():
    for case, more in (("no Rapid block", {}), ("Rapid on too", {"rapid": RAPID})):
        S.RULE.clear()
        b, h = S.harness(**more)
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await S.until(lambda: S.short_opens(h), f"{case}: a scalp")
        S.RULE.clear()
        await asyncio.sleep(0.05)
        check(len(S.short_opens(h)) == 1 and not b.calls_of("place") and not h.c.bursts
              and not rapid_file(h).exists() and not h.c.rapid_day.path.exists(),
              f"{case}: short-term trades as before, nothing of Rapid")
        S.stop(h)
    for case, block in (("Rapid off", {**RAPID, "enabled": False}),
                        ("its signal trigger off", {**RAPID, "triggers": {"signals": False, "news": True}})):
        S.RULE.clear()
        b, h = S.harness(short={"enabled": False}, rapid=block)
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await asyncio.sleep(0.2)
        check(b.tick_reads == 0 and not b.calls and not h.c.bursts, f"{case}, short-term off: nothing read or placed")
        S.stop(h)
    # short-term on beside Rapid and its signal trigger: the signals are scalps, and no local run starts even when
    # start_local is asked directly
    S.RULE.clear()
    b, h = harness(short={"enabled": True})
    check(h.c.rapid["enabled"] and h.c.rapid["triggers"]["signals"] and not h.c.scalper.rapid_on(),
          f"short-term on: Rapid's signal runs off: {h.c.rapid}")
    await h.c.start_local("EURUSD", dict(BUY))
    check(not h.c.bursts and not b.calls_of("place") and not rapid_file(h).exists(), f"no run: {h.c.bursts}")
    S.stop(h)


@scenario
async def no_signal_run_unless_execution_is_auto():
    # execution notify or confirm: the rule is not even read; switched to auto, a signal starts a run
    for mode in ("notify", "confirm"):
        S.RULE.clear()
        b, h = harness(execution=mode)
        got = starts(h)
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await asyncio.sleep(0.2)
        check(not got and b.tick_reads == 0 and not b.calls and not h.c.bursts,
              f"execution {mode}: nothing read, no run: {got} {b.calls}")
        h.c.apply_settings({**h.c.settings, "execution": "auto"})
        await S.until(lambda: len(b.calls_of("place")) == 3, f"{mode}, then auto: a run")
        S.RULE.clear()
        check(len(runs(h)) == 1, f"one run: {runs(h)}")
        S.stop(h)


@scenario
async def no_signal_run_while_paused_or_stopping():
    """As for a scalp: the session refused by the server (the Scalper paused until its next welcome) or the connector
    stopping, nothing is read and no run starts; paused no more, a signal starts one."""
    for case in ("paused", "stopping"):
        S.RULE.clear()
        b, h = harness()
        on = h.c.scalper if case == "paused" else h.c
        setattr(on, case, True)
        S.RULE["EURUSD"] = dict(BUY)
        h.c.scalper.start()
        await asyncio.sleep(0.2)
        check(b.tick_reads == 0 and not b.calls and not h.c.bursts, f"{case}: nothing read or placed")
        setattr(on, case, False)
        await S.until(lambda: len(b.calls_of("place")) == 3, f"{case} no more: a run")
        S.RULE.clear()
        check(len(runs(h)) == 1, f"one run: {runs(h)}")
        S.stop(h)


@scenario
async def no_signal_run_around_the_new_york_close():
    """No local run starts where its orders' expiry and Y (60 s here) would meet the daily rollover (17:00 New York:
    from 5 min before it less those 60 s until 18:05) or the weekly close (from Friday's until Sunday 18:05): told once
    in the log. Past 18:05 New York a signal starts a run."""
    said = "Rapid: no new run around the daily and weekly close (17:00 New York)"
    # UTC: New York is 4 hours behind until Sunday 1 November (06:00 UTC), 5 from then on
    for case, at, later in (("Wednesday 16:59", datetime(2026, 10, 28, 20, 59), datetime(2026, 10, 28, 22, 10)),
                            ("Friday 16:58", datetime(2026, 10, 30, 20, 58), datetime(2026, 11, 1, 23, 10))):
        S.RULE.clear()
        b, h = harness()
        S.set_clock(at)
        try:
            got = starts(h)
            S.RULE["EURUSD"] = dict(BUY)
            with logged() as lines:
                h.c.scalper.start()
                await S.until(lambda: len(got) >= 2, f"{case}: signals")
            check(lines.count(said) == 1 and not b.calls_of("place") and not h.c.bursts,
                  f"{case} New York: no run, told once: {[m for m in lines if 'close' in m]}")
            S.set_clock(later)                          # 18:10 New York: that day's, or Sunday's
            await S.until(lambda: len(b.calls_of("place")) == 3, f"{case} New York, then 18:10: a run")
        finally:
            S.set_clock(S.WEDNESDAY)
        S.RULE.clear()
        check(len(runs(h)) == 1, f"one run: {runs(h)}")
        S.stop(h)


@scenario
async def an_account_turned_real_skips_the_run_it_plans():
    """The account is read again as a local run plans its orders: one that turned real since the run started (an MT5
    login switched; allow_real_account on, else the burst's own check refuses it first) skips the run there."""
    S.RULE.clear()
    b, h = harness()
    h.c.allow_real = True
    real = h.c.start_local

    async def start_local(sym, d):
        await real(sym, d)
        b.demo = False                  # after start_local's own checks, before the run reads the account
    h.c.start_local = start_local
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    skipped = "on EURUSD skipped: Rapid signal runs trade demo accounts only"
    await S.until(lambda: any(skipped in m for m in h.logs("warn")), "the run skipped as it plans")
    S.RULE.clear()
    check(len(h.c.burst_done) == 1 and not h.c.bursts and not b.calls_of("place")
          and not b.calls_of("stop_limit_allowed"), f"nothing placed, nothing asked of the symbol: {b.calls}")
    S.stop(h)


@scenario
async def a_run_checking_or_an_order_left_keeps_its_symbol():
    """A run holds its symbol from its check on, before it placed anything (symbol_owner): signals while the first
    run still checks (its stop-limit check slow here) start no second one. An order a run that is gone left in the
    book (a news run's fxb:, a local run's fxr:; the sweeper removes it) keeps a new run off its symbol too."""
    S.RULE.clear()
    b, h = harness()
    gate, real = asyncio.Event(), h.c.call

    async def call(fn, *a, **kw):
        if fn == b.stop_limit_allowed:          # the first run's check takes its time
            await gate.wait()
        return await real(fn, *a, **kw)
    h.c.call = call
    got = starts(h)
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    await S.until(lambda: runs(h), "a run")
    n = len(got)
    await S.until(lambda: len(got) >= n + 2, "more signals while it checks")
    check([st["status"] for st in runs(h)] == ["checking"] and not h.c.burst_done and not b.calls_of("place"),
          f"one run, checking, none beside it: {[st['status'] for st in runs(h)]} {h.c.burst_done}")
    gate.set()
    await S.until(lambda: len(b.calls_of("place")) == 3, "its orders placed")
    S.RULE.clear()
    await asyncio.sleep(0.05)
    check(len(runs(h)) == 1 and not h.c.burst_done and len(b.calls_of("place")) == 3, "placed once")
    S.stop(h)

    S.RULE.clear()
    b, h = harness()
    b.add_order("GBPUSD", "sell", 0.1, 1.29900, "fxb:999:0")                        # a news run's, gone
    b.add_order("EURUSD", "buy", 0.1, 1.10100, local_comment(1_798_000_000_000, 0))   # a local run's, gone
    got = starts(h)
    S.RULE.update(EURUSD=dict(BUY), GBPUSD=dict(SELL))
    h.c.scalper.start()
    await S.until(lambda: got.count("EURUSD") >= 2 and got.count("GBPUSD") >= 2, "signals on both")
    check(not h.c.bursts and not h.c.burst_done and not b.calls_of("place"), f"no run next to them: {h.c.burst_done}")
    S.stop(h)


@scenario
async def open_rapid_losses_count_against_its_room():
    """rapid_room counts what Rapid's positions lose now: those of a run that is gone (left to the sweeper) on top of
    today's result; a live run's in place of its reservation when they lose more (never both)."""
    S.RULE.clear()
    b, h = harness(rapid={"loss_unit": "usd", "daily_loss": 50, "risk_unit": "usd", "risk_min": 20, "risk_max": 20})
    b.bids["USDCHF"] = 0.9
    b.add_position("USDCHF", "buy", 1.0, local_comment(1_798_000_000_000, 0))     # a run that is gone left it
    b.move(0.89965, "USDCHF")                                            # 4 pips under its entry: -40
    free = await h.c.rapid_room(None, await h.c.call(b.account))
    check(near(free, 10.0), f"50 a day, nothing booked, 40 lost by that position now: {free} left")
    await h.c.push_snapshot()                                           # the positions as last seen
    b.pos_errors = 1                                                    # the next read of them fails
    free = await h.c.rapid_room(None, await h.c.call(b.account))
    check(near(free, 10.0) and not b.pos_errors, f"unreadable: the position as last seen counts: {free} left")
    S.RULE["EURUSD"] = dict(BUY)
    h.c.scalper.start()
    skipped = "on EURUSD skipped: Rapid's daily loss limit leaves no room for an order risking 20.00"
    await S.until(lambda: any(skipped in m for m in h.logs("warn")), "a run of orders risking 20 skipped")
    S.RULE.clear()
    check(not b.calls_of("place"), f"nothing placed: {b.calls_of('place')}")
    S.stop(h)

    S.RULE.clear()
    b, h = harness(rapid={"loss_unit": "usd", "daily_loss": 50})
    rid = 1_798_000_000_000
    st = h.c.bursts[rid] = h.c._local_run(rid, "USDCHF", "buy", 0.5, -3.0)    # a live run, filled
    st.update(status="armed", side="buy")
    b.bids["USDCHF"] = 0.9
    b.add_position("USDCHF", "buy", 1.0, local_comment(rid, 0))
    b.move(0.89975, "USDCHF")                                            # -30
    acct = await h.c.call(b.account)
    for reserved, room in ((10.0, 20.0), (40.0, 10.0)):
        st["reserved"] = reserved
        free = await h.c.rapid_room(None, acct)
        check(near(free, room), f"its positions losing 30, {reserved:.0f} reserved: {free} left of 50")
    h.c.bursts.clear()
    S.stop(h)


@scenario
async def a_skipped_signal_run_is_told_once_a_while():
    """A local run skipped (no stop-limits on the symbol here) reaches the web app's log once per RAPID_SKIP_EVERY
    and symbol: a short-term rule may fire every second, so the skips in between are only logged."""
    S.RULE.clear()
    b, h = harness(rapid={"cooldown_seconds": 0})
    b.stop_limit = False

    def skips(msgs, sym):
        return [m for m in msgs if re.fullmatch(rf"Rapid run #\d+ on {sym} skipped: stop-limit orders are not "
                                                rf"allowed on {sym}", m)]
    with logged() as lines:
        S.RULE.update(EURUSD=dict(BUY), GBPUSD=dict(SELL))
        h.c.scalper.start()
        await S.until(lambda: len(skips(lines, "EURUSD")) >= 3 and len(skips(lines, "GBPUSD")) >= 3,
                      "runs skipped one after the other on both", timeout=8)
        check(len(skips(h.logs("warn"), "EURUSD")) == 1 and len(skips(h.logs("warn"), "GBPUSD")) == 1,
              f"told once per symbol: {h.logs('warn')}")
        h.c.scalper.said["rapid-skip:EURUSD"] -= h.c.RAPID_SKIP_EVERY      # EURUSD's time is up
        n = len(skips(lines, "EURUSD"))
        await S.until(lambda: len(skips(lines, "EURUSD")) >= n + 2, "more EURUSD runs skipped")
    S.RULE.clear()
    check(len(skips(h.logs("warn"), "EURUSD")) == 2 and len(skips(h.logs("warn"), "GBPUSD")) == 1,
          f"once more for EURUSD, then only logged again: {h.logs('warn')}")
    S.stop(h)


@scenario
async def a_bad_rapid_value_is_its_default():
    """A Rapid block with one value the connector cannot use: that value is its default, the rest kept, a warning
    logged (Rapid stays on); "false" never switches it on."""
    b, h = harness()
    on = {**RAPID, "sl_pips": 12, "cooldown_seconds": 60, "symbols": ["GBPUSD"]}
    with logged() as lines:
        h.c.apply_rapid({**on, "orders": "x"})
    r = h.c.rapid
    check(r["enabled"] is True and r["orders"] == DEFAULTS["orders"] != RAPID["orders"] and r["sl_pips"] == 12
          and r["cooldown_seconds"] == 60 and r["symbols"] == ["GBPUSD"]
          and r["triggers"] == {"signals": True, "news": False}, f"orders its default, the rest kept: {r}")
    check("Rapid settings: invalid_number (the bad value is left out)" in lines, f"logged: {lines}")
    h.c.apply_rapid({"enabled": "false"})
    check(h.c.rapid["enabled"] is False, f"\"false\" leaves Rapid off: {h.c.rapid}")
    S.stop(h)


@scenario
async def a_news_runs_leftovers_are_rapids_after_a_restart():
    """A Rapid news run's orders carry fxb:<id>, as every news run's: its id, kept in rapid_open.json, says whose they
    are once its runner is gone. Killed with a position open, the next start counts that position as Rapid's (the
    snapshot, rapid_room, rapid_pnl_today: no long-term loss); the sweeper's close of it is reported as Rapid's and
    booked to Rapid's day, and the id is forgotten once nothing of the run is left."""
    S.RULE.clear()
    news = {"triggers": {"signals": False, "news": True}}
    with one_folder() as folder:
        b, h = harness(rapid=news)
        T.view(b, "up")
        await h.c.on_burst(T.rapid(41, orders=1))
        await T.armed(h, 41)
        check(json.loads((folder / "rapid_open.json").read_text()) == {"41": {"news": True}},
              "its id kept before its first order")
        fill(b)
        await S.until(lambda: h.of("trade_opened"), "its buy filled")
        # the connector dies: nothing more of it reaches MT5 or its files
        h.c._save_rapid_open = lambda: None

        async def hang(*a, **kw):
            await asyncio.Event().wait()
        h.c.call = hang
        task = h.c.bursts[41]["task"]
        S.stop(h)
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.wait([task], timeout=2)
        b.move(1.09960)                                 # 6 pips under its limit (1.10020) on 0.25 lots: -15

        # the long-term limit 0.1 % (10): the 15 that position loses are Rapid's, not long-term's
        b, h = harness(b, rapid=news, daily_loss_limit_pct=0.1)
        check(h.c.rapid_news == {41} and not h.c.rapid_left, f"the next start reads its id: {h.c.rapid_news}")
        held = await h.c.call(b.positions)
        check(len(held) == 1 and h.c.strategy_of(held[0]) == RUN_KIND and h.c.snapshot_burst_id(held[0]) == 41
              and near(h.c.rapid_pnl_today(held), -15.0), f"its position is Rapid's, its loss Rapid's today: {held}")
        free = await h.c.rapid_room(None, await h.c.call(b.account))
        check(near(free, 485.0), f"and comes off Rapid's room (5 % of 10 000): {free}")
        h.c.risk.update_day(C.utcnow().date(), 10_000.0)                 # the long-term day began at 10 000
        await h.c.on_signal(S.signal(b, 801, "GBPUSD", "buy"), manual=False)
        check(any(m["trade"].get("signal_id") == 801 for m in h.of("trade_opened")) and not h.c.risk.halted,
              f"a long-term signal still trades: {h.of('signal_skipped')}")
        await h.c.push_snapshot()
        snap = {(p["symbol"], p["burst_id"], p["strategy"]) for p in h.of("snapshot")[-1]["positions"]}
        check(snap == {("EURUSD", 41, RUN_KIND), ("GBPUSD", None, "long")}, f"the snapshot: {snap}")
        await h.c.sweep_orphans()
        closes, pid = [m["trade"] for m in h.of("trade_closed")], held[0]["ticket"]
        check(len(closes) == 1 and closes[0]["ticket"] == pid and closes[0]["burst_id"] == 41
              and closes[0]["strategy"] == RUN_KIND and near(closes[0]["pnl"], -15.0)
              and [p["comment"] for p in b.pos.values()] == ["fx:801"], f"swept, reported as Rapid's: {closes}")
        check(near(h.c.rapid_day.day["pnl"], -15.0) and h.c.rapid_booked == {pid: True}
              and near(h.c.rapid_pnl_today(await h.c.call(b.positions)), -15.0),
              f"booked to Rapid's day, counted once: {h.c.rapid_day.day}")
        await h.c.sweep_orphans()
        check(not h.c.rapid_news and json.loads((folder / "rapid_open.json").read_text()) == {},
              f"nothing of the run left: its id forgotten: {h.c.rapid_news}")
        S.stop(h)


@scenario
async def run_takes_up_what_the_last_run_left_before_its_first_sweep():
    """run() hands the local runs the last run left (rapid_open.json) to the sweeper before its first sweep: a trade
    of theirs that closed at its stop while no connector ran is reported and booked to Rapid's day at the start."""
    S.RULE.clear()
    with one_folder() as folder:
        b, rid = S.TickBroker(mode="hedging"), 1_798_000_000_000
        t = b.add_position("EURUSD", "buy", 0.75, local_comment(rid, 0))
        entry = b.pos[t]["entry_price"]
        with b.lock:                                    # closed at its stop while no connector ran: -48.75
            b._close(b.pos[t], 0.75, 1.09940)
        (folder / "rapid_open.json").write_text(json.dumps({str(rid): {
            "symbol": "EURUSD", "side": "buy", "demo": True, "digits": 5, "pip": PIP, "orders": {},
            "trades": {str(t): {"side": "buy", "lots": 0.75, "entry": entry, "opened_at": "2026-10-28T11:59:00"}}}}))
        b, h = harness(b)
        check(set(h.c.rapid_left) == {rid}, f"the start reads it: {h.c.rapid_left}")
        order, adopt, sweep = [], h.c._rapid_adopt, h.c.sweep_orphans

        async def rapid_adopt():
            order.append("adopt")
            return await adopt()

        async def sweep_orphans():
            order.append("sweep")
            return await sweep()
        h.c._rapid_adopt, h.c.sweep_orphans = rapid_adopt, sweep_orphans
        await S.run_with(h, T.Link(h, [{"type": "error", "reason": "invalid_token"}]))
        closes = rapid_closes(h)
        check(order == ["adopt", "sweep"], f"taken up, then swept: {order}")
        check([c["ticket"] for c in closes] == [t] and near(closes[0]["pnl"], -48.75)
              and near(h.c.rapid_day.day["pnl"], -48.75), f"its close reported and booked: {closes}")
        check(json.loads((folder / "rapid_open.json").read_text()) == {}, "nothing left to take up")
        S.stop(h)


@scenario
async def nothing_written_to_the_project():
    now = {p.name: p.stat().st_mtime_ns for p in (ROOT / "connector").glob("*.json")}
    check(now == T.KEPT, f"nothing written to connector/: {sorted(set(now) ^ set(T.KEPT))}")


# ── runner ─────────────────────────────────────────────────────
def main() -> int:
    failed = 0
    for fn in TESTS:
        t0 = time.monotonic()
        try:
            res = fn()
            if isinstance(res, types.CoroutineType):     # a connector scenario
                in_loop(res)
            print(f"PASS {fn.__name__} ({time.monotonic() - t0:.2f}s)")
        except Exception as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
            traceback.print_exc()
    return failed


if __name__ == "__main__":
    picked = [fn for fn in TESTS if fn.__name__ in sys.argv[1:]]
    TESTS[:] = picked or TESTS
    try:
        failures = main()
    finally:
        if RUNNER:
            RUNNER[0].close()
            shutil.rmtree(T.TMP, ignore_errors=True)
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} scenarios passed")
    sys.exit(1 if failures else 0)
