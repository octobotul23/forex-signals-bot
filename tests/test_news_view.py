"""Pre-news analysis: the server's half (server/news_view.py: the calendar value parser, event polarity, the forecast's
expectation, base / quote side, the AI bias and the context sent to the connector) and the connector's half
(bot/news_view.py: H1 trend, M1 momentum, blow-out, the decision and its history text).

    .venv\\Scripts\\python tests\\test_news_view.py

Pure functions on synthetic bars and hand-made events / analyses: no database, no settings, no MetaTrader 5, no
network, no files. Exit code 0 = every check passed.
"""
import ast
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import bot.news_view as V                                       # noqa: E402
import server.news_view as S                                    # noqa: E402

FAILED: list[str] = []
PASSED: list[str] = []
NOW = datetime(2026, 10, 30, 12, 30)                            # naive UTC, as the database keeps times


def check(ok, label: str, detail=""):
    got = f"\n       got: {detail!r}"[:700] if not ok and detail != "" else ""
    print(f"{'ok  ' if ok else 'FAIL'} {label}{got}")
    (PASSED if ok else FAILED).append(label)


def near(a, b, eps=1e-9):
    return a is not None and b is not None and abs(a - b) <= eps * max(1.0, abs(b))


def frame(closes, width, freq="h", start="2026-10-20"):
    """Bars as MT5Broker.rates returns them: open = the previous close, high / low `width` / 2 outside the body."""
    c = np.asarray(closes, dtype=float)
    o = np.r_[c[0], c[:-1]]
    return pd.DataFrame({"time": pd.date_range(start, periods=len(c), freq=freq), "open": o,
                         "high": np.maximum(o, c) + width / 2, "low": np.minimum(o, c) - width / 2, "close": c,
                         "tick_volume": 100, "spread": 8})


def bars(closes, half, freq="h", start="2026-10-20"):
    """Bars with open = close and high / low exactly `half` outside it: every row's range is 2 x half, and its true
    range too as long as the close moves less than `half` from row to row."""
    c = np.asarray(closes, dtype=float)
    return pd.DataFrame({"time": pd.date_range(start, periods=len(c), freq=freq), "open": c, "high": c + half,
                         "low": c - half, "close": c, "tick_volume": 100, "spread": 8})


def wilder(df, n):
    """Wilder's ATR worked out by hand: the true range of each row (high - low, or the gap to the previous close when
    larger; the first row has no previous close), smoothed a += (tr - a) / n from the first row."""
    high, low, close = (df[k].astype(float).tolist() for k in ("high", "low", "close"))
    a = None
    for i in range(len(high)):
        tr = high[i] - low[i] if i == 0 else max(high[i] - low[i], abs(high[i] - close[i - 1]),
                                                 abs(low[i] - close[i - 1]))
        a = tr if a is None else a + (tr - a) / n
    return a


# ── server: the value parser ───────────────────────────────────
def test_parse():
    # units: "%" a percentage, "#" a count (K/M/B/T), "" a plain number (an index such as a PMI)
    table = [
        ("0.7%", (0.7, "%")), ("<1.25%", (1.25, "%")), (">50.0", (50.0, "")), ("-12.5K", (-12500.0, "#")),
        ("1.2M", (1.2e6, "#")), ("3B", (3e9, "#")), ("1.1T", (1.1e12, "#")), ("256K", (256000.0, "#")),
        ("-0.1%", (-0.1, "%")), ("+0.3%", (0.3, "%")), ("  0.3% ", (0.3, "%")), ("12k", (12000.0, "#")),
        ("1.2 M", (1.2e6, "#")), ("1,234.5K", (1234500.0, "#")), ("−0.4%", (-0.4, "%")), ("< -5.0B", (-5e9, "#")),
        (".5%", (0.5, "%")), ("50.0", (50.0, "")), (5, (5.0, "")), (-2.5, (-2.5, "")),
        ("≤1.25%", (1.25, "%")), ("≥ 50", (50.0, "")), ("0.0K", (0.0, "#")),
        ("", None), (None, None), ("n/a", None), ("N/A", None), ("-", None), ("abc", None), ("1.2.3", None),
        ("%", None), ("K", None), ("1.2X", None), ("--1", None), ("1.2%%", None), ("1.2KM", None), ("1,23", None),
        ("0.5% y/y", None), (True, None), (float("nan"), None), (float("inf"), None), ([], None), ({}, None),
        ("9" * 400, None), ("9" * 400 + "T", None), ("9" * 400 + "%", None),     # read as inf: unreadable
        (10 ** 400, None), (-10 ** 400, None),             # an int too big for a float: unreadable, no OverflowError
    ]
    for text, want in table:
        got = S.parse_value(text)
        ok = got == want if want is None or got is None else (near(got[0], want[0]) and got[1] == want[1])
        check(ok, f"parse_value({text!r}) -> {want}", got)


# ── server: polarity by ForexFactory title ─────────────────────
def test_polarity():
    table = {
        -1: ["Unemployment Rate", "Unemployment Claims", "Claimant Count Change", "Unemployment Change",
             "Continuing Jobless Claims", "Initial Jobless Claims", "Challenger Job Cuts y/y", "unemployment rate",
             "Continuing Claims", "Initial Claims"],
        0: ["Trade Balance", "Goods Trade Balance", "Current Account", "Federal Budget Balance", "Goods Balance"],
        1: ["CPI m/m", "Core CPI y/y", "Advance GDP q/q", "Flash Manufacturing PMI", "ISM Services PMI",
            "Retail Sales m/m", "Core Retail Sales m/m", "Federal Funds Rate", "Official Bank Rate",
            "Non-Farm Employment Change", "ADP Non-Farm Employment Change", "Employment Change",
            "Average Hourly Earnings m/m", "Something New", "", None],
    }
    for want, titles in table.items():
        for title in titles:
            got = S.polarity(title)
            check(got == want, f"polarity({title!r}) = {want:+d}", got)


# ── server: what the forecast expects ──────────────────────────
def test_expect():
    up = S.expect_score("CPI m/m", "0.3%", "0.2%")
    check(up is not None and up > 0, "CPI expected up: + for its currency", up)
    check(S.EXPECT_PP == 0.25 and up == 0.38, "a %: tanh(the change in percentage points / 0.25) = tanh(0.4)", up)
    down = S.expect_score("CPI m/m", "0.1%", "0.2%")
    check(down is not None and down < 0, "CPI expected down: -", down)
    unemp = S.expect_score("Unemployment Rate", "4.2%", "4.1%")
    check(unemp is not None and unemp < 0, "unemployment expected up: - (inverted)", unemp)
    check((S.expect_score("Unemployment Rate", "4.0%", "4.1%") or 0) > 0, "unemployment expected down: +")
    check((S.expect_score("Unemployment Claims", "230K", "220K") or 0) < 0, "more claims expected: -")
    check((S.expect_score("Claimant Count Change", "-10.2K", "5.0K") or 0) > 0, "claimant count turning down: +")
    tb = S.expect_score("Trade Balance", "-45.0B", "-50.0B")
    check(tb is not None and tb > 0, "trade balance less negative (a smaller deficit): +", tb)
    check((S.expect_score("Trade Balance", "-55.0B", "-50.0B") or 0) < 0, "trade balance more negative: -")
    check((S.expect_score("Current Account", "12.0B", "10.0B") or 0) > 0, "a bigger surplus: +")
    flip = S.expect_score("Trade Balance", "2.0B", "-1.0B")
    check(flip is not None and flip > 0.9, "a deficit turning into a surplus: strongly +", flip)
    check((S.expect_score("Advance GDP q/q", "-0.2%", "-0.5%") or 0) > 0, "GDP less negative: + (|previous| below)")
    check((S.expect_score("Non-Farm Employment Change", "180K", "150K") or 0) > 0, "more jobs expected: +")
    check(S.expect_score("CPI m/m", "0.3%", "200K") is None, "mixed units: None")
    check(S.expect_score("CPI m/m", "0.3", "0.2%") is None, "a count against a %: None")
    for f, p in (("", "0.2%"), ("0.3%", ""), (None, "0.2%"), ("0.3%", None), ("n/a", "0.2%"), ("", "")):
        check(S.expect_score("CPI m/m", f, p) is None, f"forecast {f!r}, previous {p!r}: None")
    check(S.expect_score("CPI m/m", "0.2%", "0.2%") == 0.0, "no change expected: 0")
    check(S.expect_score("CPI m/m", "0.0%", "0.0%") == 0.0, "0 after 0: 0, no division by zero")
    z = S.expect_score("CPI m/m", "0.2%", "0.0%")
    check(z == 0.664, "a % after 0 %: in points (0.2 pp -> tanh(0.8)), no division by zero", z)
    small, big = S.expect_score("CPI y/y", "3.1%", "3.0%"), S.expect_score("CPI y/y", "4.0%", "3.0%")
    check(0 < small < big <= 1, "a bigger expected change scores more, never over 1", (small, big))
    check(S.expect_score("Unemployment Rate", "-0.0%", "0.0%") == 0.0 and str(
        S.expect_score("Unemployment Rate", "0.0%", "0.0%")) == "0.0", "never a -0.0")
    check(str(S.expect_score("Unemployment Rate", "4.1%", "4.1%")) == "0.0", "inverted, no change: 0.0, not -0.0",
          S.expect_score("Unemployment Rate", "4.1%", "4.1%"))
    check(str(S.expect_score("Unemployment Claims", "220K", "220K")) == "0.0", "an inverted count, no change: 0.0")
    extreme = S.expect_score("Retail Sales m/m", "900%", "0.01%")
    check(extreme is not None and -1 <= extreme <= 1, "an extreme change stays in [-1, 1]", extreme)
    # the step by the kind of value. A %: percentage points over 0.25, so tiny m/m and q/q releases do not hit the cap
    # and rates, unemployment rates and y/y figures are not crushed (relative to |previous| they were 0.995 and 0.1)
    table = [
        ("Advance GDP q/q", "0.1%", "0.0%", 0.38, "0.0 -> 0.1 % GDP: 0.1 pp, not the cap"),
        ("GDP q/q", "0.2%", "0.1%", 0.38, "0.1 -> 0.2 % GDP: the same 0.1 pp"),
        ("Core CPI m/m", "0.3%", "0.2%", 0.38, "0.2 -> 0.3 % core CPI: +0.38"),
        ("Core CPI m/m", "0.2%", "0.3%", -0.38, "0.3 -> 0.2 %: the same size the other way"),
        ("CPI y/y", "3.1%", "3.0%", 0.38, "a y/y 0.1 pp: like an m/m 0.1 pp"),
        ("Retail Sales m/m", "-0.1%", "0.1%", -0.664, "-0.2 pp: tanh(-0.8), not -1"),
        ("Official Bank Rate", "4.00%", "4.25%", -0.762, "a 25 bp cut: tanh(-1), not -0.175"),
        ("Federal Funds Rate", "4.50%", "4.25%", 0.762, "a 25 bp hike: +0.762"),
        ("Unemployment Rate", "4.3%", "4.1%", -0.664, "unemployment +0.2 pp (inverted): -0.664, not -0.145"),
        ("Unemployment Rate", "4.1%", "4.2%", 0.38, "unemployment -0.1 pp: +0.38"),
        # a plain number (an index): points over 2
        ("Flash Manufacturing PMI", "50.5", "50.0", 0.245, "PMI +0.5: tanh(0.25), over QUIET (was 0.03)"),
        ("ISM Services PMI", "52.0", "54.0", -0.762, "PMI -2 points: tanh(-1)"),
        ("ZEW Economic Sentiment", "12.5", "-5.0", 1.0, "a sentiment index +17.5 points: the cap"),
        ("Flash Manufacturing PMI", "50.0", "50.0", 0.0, "an index, no change: 0"),
        # a count (K/M/B/T): relative to |previous| x 3, as before
        ("Non-Farm Employment Change", "180K", "150K", 0.537, "NFP +20 %: tanh(0.6)"),
        ("Unemployment Claims", "230K", "220K", -0.136, "claims +4.5 % (inverted): -tanh(0.136)"),
        ("Existing Home Sales", "950K", "1.2M", -0.555, "K against M: the same unit, scaled"),
        ("Claimant Count Change", "5.0K", "0.0K", -0.995, "a count after 0: on |forecast| (inverted)"),
        ("Unemployment Claims", "0.0K", "0.0K", 0.0, "a count, 0 after 0: 0"),
        # a balance: relative in any unit but %, a plain number too
        ("Trade Balance", "-45.0", "-50.0", 0.291, "a plain-number balance: relative, like -45B after -50B"),
        ("Trade Balance", "-45.0B", "-50.0B", 0.291, "a smaller deficit: tanh(0.3)"),
        ("Current Account", "-1.5%", "-2.0%", 0.964, "a balance in %: percentage points (tanh(0.5 / 0.25)), not relative"),
    ]
    for title, f, p, want, what in table:
        got = S.expect_score(title, f, p)
        check(got == want, f"{title} {f} after {p}: {want} ({what})", got)
    check(S.expect_score("Non-Farm Employment Change", "180K", "150") is None, "a count against a plain number: None")
    check(S.expect_score("Flash Manufacturing PMI", "50.5", "50.0%") is None, "a plain number against a %: None")
    check(S.EXPECT_POINTS == 2.0 and S.EXPECT_GAIN == 3.0, "steps: 2 index points, 3 x the relative change")
    check(S.expect_score(5, "0.3%", "0.2%") == 0.38 and S.expect_score(None, "0.3%", "0.2%") == 0.38,
          "a junk title: polarity +1, no error")


# ── server: base / quote side ──────────────────────────────────
def test_pair_side():
    table = [("EURUSD", "USD", -1), ("EURUSD", "EUR", 1), ("XAUUSD", "USD", -1), ("GBPJPY", "JPY", -1),
             ("GBPJPY", "GBP", 1), ("EURUSD", "JPY", 0), ("USDJPY", "USD", 1), ("XAUUSD", "XAU", 1),
             ("EUR/USD", "USD", -1), ("EURUSD.m", "EUR", 1), ("eurusd", "usd", -1), ("", "USD", 0),
             (None, "USD", 0), ("EURUSD", None, 0), ("EURUSD", "", 0), ("EURUSD", "US", 0),
             ("USD", "USD", 0), ("EURUS", "EUR", 0), (5, "USD", 0), ("EURUSD", 7, 0)]
    for pair, cur, want in table:
        got = S.pair_side(pair, cur)
        check(got == want, f"pair_side({pair!r}, {cur!r}) = {want:+d}", got)


# ── server: the AI bias ────────────────────────────────────────
def analysis_row(age_min, biases=None):
    """An AIAnalysis row as the engine stores it: created_at + result = MarketAnalysis.model_dump()."""
    biases = biases if biases is not None else [
        {"symbol": "EURUSD", "direction": "buy", "confidence": 80, "horizon_hours": 12},
        {"symbol": "USD/JPY", "direction": "sell", "confidence": 60, "horizon_hours": 12},
        {"symbol": "GBPUSD", "direction": "neutral", "confidence": 40, "horizon_hours": 12},
        {"symbol": "GBPJPY", "direction": "buy", "confidence": 90, "horizon_hours": 1},
    ]
    return SimpleNamespace(created_at=NOW - timedelta(minutes=age_min), kind="news",
                           result={"headlines": [], "biases": biases, "summary_en": "", "summary_ro": ""})


def test_ai_part():
    row = analysis_row(30)
    check(S.ai_part(row, "EURUSD", NOW) == 0.8, "buy at 80 -> +0.8", S.ai_part(row, "EURUSD", NOW))
    check(S.ai_part(row, "USDJPY", NOW) == -0.6, "sell at 60 -> -0.6 (a slash in the AI's symbol is ignored)",
          S.ai_part(row, "USDJPY", NOW))
    check(S.ai_part(row, "GBPUSD", NOW) == 0.0, "neutral -> 0 (a bias, not a missing one)")
    check(S.ai_part(row, "XAUUSD", NOW) is None, "no bias for the pair -> None")
    check(S.ai_part(None, "EURUSD", NOW) is None, "no analysis -> None")
    check(S.ai_part(analysis_row(119), "EURUSD", NOW) == 0.8, "1 h 59 min old: still used")
    check(S.ai_part(analysis_row(121), "EURUSD", NOW) is None, "over 2 h old -> None")
    check(S.ai_part(analysis_row(180), "EURUSD", NOW, max_age_h=6) == 0.8, "max_age_h is honoured")
    check(S.ai_part(analysis_row(30), "GBPJPY", NOW) == 0.9, "inside the bias's own horizon: used")
    check(S.ai_part(analysis_row(90), "GBPJPY", NOW, max_age_h=6) is None, "past the bias's horizon_hours -> None")
    aware = NOW.replace(tzinfo=timezone.utc)
    check(S.ai_part(row, "EURUSD", aware) == 0.8, "an aware `now` works with naive stored times")
    api = {"created_at": (NOW - timedelta(minutes=10)).isoformat() + "Z", "biases": row.result["biases"]}
    check(S.ai_part(api, "EURUSD", NOW) == 0.8, "a /api/market/analysis dict (ISO time with Z) works")
    padded = {"created_at": " 2026-10-30T12:00:00Z ", "biases": row.result["biases"]}
    check(S.ai_part(padded, "EURUSD", NOW) == 0.8, "an ISO time with spaces around it", S.ai_part(padded, "EURUSD", NOW))
    check(S.ai_part(row, "EUR/USD", NOW) == 0.8, "a slash in the pair is ignored too", S.ai_part(row, "EUR/USD", NOW))
    objs = SimpleNamespace(biases=[SimpleNamespace(symbol="EURUSD", direction="sell", confidence=70, horizon_hours=4)])
    dumped = {"created_at": NOW - timedelta(minutes=5), "result": objs}
    check(S.ai_part(dumped, "EURUSD", NOW) == -0.7, "biases as objects (PairBias) work")
    check(S.ai_part(objs, "EURUSD", NOW) is None, "no time on the analysis -> None (its age cannot be checked)")
    check(S.ai_part(SimpleNamespace(created_at="junk", result=row.result), "EURUSD", NOW) is None,
          "a junk time -> None")
    check(S.ai_part(SimpleNamespace(created_at=NOW, result={"biases": None}), "EURUSD", NOW) is None,
          "no bias list -> None")
    for bad in ({"symbol": "EURUSD", "direction": "up", "confidence": 80},
                {"symbol": "EURUSD", "direction": "buy", "confidence": "high"},
                {"symbol": "EURUSD", "direction": "buy", "confidence": float("nan")},
                {"symbol": "EURUSD", "direction": "buy", "confidence": True},
                {"symbol": "EURUSD", "direction": "buy", "confidence": 10 ** 400},       # too big for a float
                {"symbol": "EURUSD", "direction": "sell", "confidence": -10 ** 400}):
        try:
            got = S.ai_part(analysis_row(5, [bad]), "EURUSD", NOW)
        except Exception as exc:                                # noqa: BLE001 - the error is the finding
            got = exc
        check(got is None, f"a junk bias -> None: {bad}", got)
    loud = analysis_row(5, [{"symbol": "EURUSD", "direction": "BUY", "confidence": 150}])
    check(S.ai_part(loud, "EURUSD", NOW) == 1.0, "confidence is capped at 100, direction read in any case")
    fresh = SimpleNamespace(created_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10),
                            result=row.result)
    check(S.ai_part(fresh, "EURUSD", None) == 0.8, "now None: the clock")
    check(S.ai_part(analysis_row(30), "EURUSD", NOW - timedelta(minutes=40)) == 0.8,
          "an analysis stamped a little after `now` (clock skew) is still used")
    # the 2 h edge: the plan says "less than 2 h old", so exactly 2 h is too old (and the same for a bias's horizon)
    check(S.ai_part(analysis_row(120), "EURUSD", NOW) is None, "exactly 2 h old -> None",
          S.ai_part(analysis_row(120), "EURUSD", NOW))
    check(S.ai_part(analysis_row(119.99), "EURUSD", NOW) == 0.8, "a moment under 2 h: used")
    check(S.ai_part(analysis_row(59), "GBPJPY", NOW) == 0.9 and S.ai_part(analysis_row(60), "GBPJPY", NOW) is None,
          "a 1 h horizon: used at 59 min, not at exactly 60")
    # horizon 0 is no horizon of its own (only the 2 h rule), and the AI's symbol is read in any case
    api = {"created_at": NOW - timedelta(minutes=30),
           "biases": [{"symbol": "eur/usd", "direction": "buy", "confidence": 80, "horizon_hours": 0}]}
    check(S.ai_part(api, "EURUSD", NOW) == 0.8, "horizon 0 and a lowercase 'eur/usd': used", S.ai_part(api, "EURUSD", NOW))
    neg = analysis_row(5, [{"symbol": "EURUSD", "direction": "buy", "confidence": -20, "horizon_hours": 12}])
    check(S.ai_part(neg, "EURUSD", NOW) == 0.0, "a negative confidence is 0, it never flips the bias",
          S.ai_part(neg, "EURUSD", NOW))
    # a sell at 0 (or at a negative confidence clamped to 0) is 0.0, never a -0.0 (stored as "-0.0" in the analysis)
    for conf in (0, -20):
        v = S.ai_part(analysis_row(5, [{"symbol": "EURUSD", "direction": "sell", "confidence": conf,
                                        "horizon_hours": 12}]), "EURUSD", NOW)
        check(v == 0.0 and str(v) == "0.0", f"sell at confidence {conf}: 0.0, not -0.0", v)
    v = S.ai_part(analysis_row(5, [{"symbol": "EURUSD", "direction": "buy", "confidence": 66.6667, "horizon_hours": 12}]),
                  "EURUSD", NOW)
    check(v == 0.667, "a fractional confidence: rounded to 3 decimals (66.6667 -> 0.667)", v)
    # a time with another zone is turned into UTC: 15:00 at +02:00 is 13:00 UTC, 2 h 30 min before 15:30 UTC
    zoned = {"created_at": "2026-10-30T15:00:00+02:00", "biases": row.result["biases"]}
    check(S.ai_part(zoned, "EURUSD", datetime(2026, 10, 30, 15, 30)) is None,
          "an ISO time at +02:00 is read in UTC (2 h 30 min old -> None)",
          S.ai_part(zoned, "EURUSD", datetime(2026, 10, 30, 15, 30)))
    check(S.ai_part(zoned, "EURUSD", datetime(2026, 10, 30, 13, 30)) == 0.8, "... and 30 min old in UTC terms: used")
    # horizon_hours is the LLM's number, unbounded: any size is a (long) horizon, never an OverflowError
    for h, what in ((10 ** 12, "10**12 h"), (10 ** 400, "an int too big for a float"), (float("inf"), "inf")):
        row = analysis_row(30, [{"symbol": "EURUSD", "direction": "buy", "confidence": 80, "horizon_hours": h}])
        try:
            got = S.ai_part(row, "EURUSD", NOW)
        except Exception as exc:                                # noqa: BLE001 - the error is the finding
            got = exc
        check(got == 0.8, f"a horizon of {what}: a long horizon, no exception", got)
    # junk is no horizon of its own (only the 2 h rule): 90 minutes old is still used. A bool is not 1 h.
    for h in (float("nan"), True, "12", None, -5, [3]):
        row = analysis_row(90, [{"symbol": "EURUSD", "direction": "buy", "confidence": 80, "horizon_hours": h}])
        check(S.ai_part(row, "EURUSD", NOW) == 0.8, f"a junk horizon {h!r}: no horizon of its own",
              S.ai_part(row, "EURUSD", NOW))
    check(S.ai_part(analysis_row(90, [{"symbol": "EURUSD", "direction": "buy", "confidence": 80, "horizon_hours": 1.5}]),
                    "EURUSD", NOW) is None, "a 1.5 h horizon at exactly 90 minutes: run its course")


# ── server: the context sent to the connector ──────────────────
def test_context_for():
    cpi = {"title": "CPI m/m", "currency": "USD", "forecast": "0.4%", "previous": "0.2%", "impact": "High"}
    e = S.expect_score("CPI m/m", "0.4%", "0.2%")
    check(e is not None and e > 0, "a positive USD surprise expected", e)
    ctx = S.context_for(cpi, "EURUSD")
    check(set(ctx) == {"expect", "ai", "title"}, "keys: expect, ai, title", ctx)
    check(ctx["expect"] == -e and ctx["title"] == "CPI m/m" and ctx["ai"] is None,
          "EURUSD: USD is the quote, so the expect is negative; no analysis -> ai None", ctx)
    check(S.context_for(cpi, "XAUUSD")["expect"] == -e, "XAUUSD: negative too")
    check(S.context_for(cpi, "USDJPY")["expect"] == e, "USDJPY: USD is the base, positive")
    check(S.context_for(cpi, "GBPJPY")["expect"] is None, "GBPJPY: USD is not in the pair -> None")
    unemp = SimpleNamespace(id="x", time=NOW, title="Unemployment Rate", currency="USD", impact="High",
                            forecast="4.3%", previous="4.1%", actual="", source="forexfactory")
    got = S.context_for(unemp, "EURUSD")
    check(got["expect"] is not None and got["expect"] > 0,
          "a CalendarEvent row: USD unemployment up is bad for USD, so EURUSD should rise", got)
    gbp = S.context_for({"title": "Claimant Count Change", "currency": "GBP", "forecast": "20.5K", "previous": "10.1K"},
                        "GBPJPY")
    check(gbp["expect"] is not None and gbp["expect"] < 0, "GBP claimant count up: GBPJPY should fall", gbp)
    got = S.context_for(cpi, "EURUSD", analysis_row(30), NOW)
    check(got["ai"] == 0.8 and got["expect"] == -e, "with a fresh analysis: its bias for the pair", got)
    check(S.context_for(cpi, "EURUSD", analysis_row(200), NOW)["ai"] is None, "a stale analysis: ai None")
    check(S.context_for({}, "EURUSD") == {"expect": None, "ai": None, "title": ""}, "an empty event: all empty")
    check(S.context_for([], "EURUSD") == {"expect": None, "ai": None, "title": ""}
          and S.context_for(None, "EURUSD") == {"expect": None, "ai": None, "title": ""}, "no events: all empty")
    flat = S.context_for({"title": "CPI m/m", "currency": "USD", "forecast": "0.2%", "previous": "0.2%"}, "EURUSD")
    check(flat["expect"] == 0.0 and str(flat["expect"]) == "0.0", "no change expected: 0.0, not -0.0", flat)
    # ... and an average that rounds to 0 from below: (0.001 - 0.001 - 0.001) / 3 = -0.000333 is 0.0, not -0.0 (the
    # check above cannot see it: sum([-0.0]) starts from 0 and is +0.0)
    ev = [{"title": t, "currency": "USD", "forecast": f, "previous": "3000K"}
          for t, f in (("A", "3001K"), ("B", "2999K"), ("C", "2999K"))]
    got = S.context_for(ev, "USDJPY")["expect"]
    check(got == 0.0 and str(got) == "0.0", "a tiny negative average is 0.0, not -0.0", got)
    huge = analysis_row(30, [{"symbol": "EURUSD", "direction": "buy", "confidence": 80, "horizon_hours": 10 ** 12}])
    check(S.context_for(cpi, "EURUSD", huge, NOW)["ai"] == 0.8, "an unbounded AI horizon never breaks the context")
    # ints too big for a float (json.loads makes one from a long digit string) in the AI's confidence or in an event's
    # forecast / previous: that part is junk, the rest of the context stays, and context_for never raises
    loud = analysis_row(30, [{"symbol": "EURUSD", "direction": "buy", "confidence": 10 ** 400}])
    big = [{"title": "GDP", "currency": "USD", "forecast": 10 ** 400, "previous": 5},
           {"title": "PMI", "currency": "USD", "forecast": 50, "previous": -10 ** 400}, cpi]
    try:
        got = S.context_for(big, "EURUSD", loud, NOW)
    except Exception as exc:                                    # noqa: BLE001 - the error is the finding
        got = exc
    check(got == {"expect": -e, "ai": None, "title": "GDP + PMI + CPI m/m"},
          "a confidence and a forecast / previous of 10**400: left out, the CPI forecast still read", got)


def test_context_release():
    """A release is every event landing on the pair in one minute (burst.candidates collapses them into one run): the
    forecast part averages them, each with its own polarity and its own side of the pair."""
    nfp = [{"id": "n1", "title": "Non-Farm Employment Change", "currency": "USD", "forecast": "180K", "previous": "150K"},
           {"id": "n2", "title": "Unemployment Rate", "currency": "USD", "forecast": "4.1%", "previous": "4.2%"},
           {"id": "n3", "title": "Average Hourly Earnings m/m", "currency": "USD", "forecast": "0.3%", "previous": "0.2%"}]
    each = [S.expect_score(r["title"], r["forecast"], r["previous"]) for r in nfp]
    check(each == [0.537, 0.38, 0.38], "more jobs, less unemployment (inverted), higher earnings: all good for USD", each)
    title = "Non-Farm Employment Change + Unemployment Rate + Average Hourly Earnings m/m"
    got = S.context_for(nfp, "EURUSD")
    check(got == {"expect": -0.432, "ai": None, "title": title},
          "NFP + UR + AHE on EURUSD: the average of the three (-1.297 / 3), USD the quote", got)
    check(S.context_for(nfp, "USDJPY")["expect"] == 0.432, "... on USDJPY: USD the base, +",
          S.context_for(nfp, "USDJPY"))
    check(S.context_for(nfp[::-1], "EURUSD")["expect"] == -0.432, "the order of the rows does not matter")
    # the candidate carries (currency, id, title) only: the engine passes the rows looked up by those ids
    cand = {"event_id": "n1", "time": NOW, "currency": "USD", "symbol": "EURUSD", "currencies": ["USD"],
            "title": title, "events": [("USD", "n1", nfp[0]["title"]), ("USD", "n2", nfp[1]["title"]),
                                       ("USD", "n3", nfp[2]["title"])]}
    by_id = {r["id"]: r for r in nfp}
    got = S.context_for([by_id[i] for _, i, _ in cand["events"]], cand["symbol"])
    check(got["expect"] == -0.432 and got["title"] == cand["title"],
          "the candidate's rows, looked up by its event ids: the forecast part is alive, the title the candidate's", got)
    rows = [SimpleNamespace(time=NOW, impact="High", actual="", source="forexfactory", **r) for r in nfp]
    check(S.context_for(rows, "EURUSD")["expect"] == -0.432, "CalendarEvent-like rows work")
    check(S.context_for(iter(rows), "EURUSD")["expect"] == -0.432, "any iterable of rows (a ScalarResult) works")
    # an inverted event pulling the other way: unemployment UP in the same minute as more jobs
    mixed = [nfp[0], {**nfp[1], "forecast": "4.3%", "previous": "4.1%"}, nfp[2]]
    got = S.context_for(mixed, "EURUSD")
    check(got["expect"] == -0.084, "unemployment up (-0.664 for USD) against NFP and AHE: (0.537 - 0.664 + 0.38) / 3",
          got)
    # an event that cannot be read is left out, not counted as 0
    blank = nfp + [{"title": "Unemployment Rate", "currency": "USD", "forecast": "", "previous": "4.1%"}]
    check(S.context_for(blank, "EURUSD")["expect"] == -0.432, "an event without a forecast is left out of the average")
    check(S.context_for([{"title": "FOMC Statement", "currency": "USD", "forecast": "", "previous": ""}],
                        "EURUSD")["expect"] is None, "no event can be read: expect None")
    # GBPJPY: a GBP and a JPY release in one minute sit on opposite sides of the pair
    gj = [{"title": "CPI y/y", "currency": "GBP", "forecast": "3.1%", "previous": "3.0%"},
          {"title": "BOJ Policy Rate", "currency": "JPY", "forecast": "0.75%", "previous": "0.50%"}]
    got = S.context_for(gj, "GBPJPY")
    check(got["expect"] == -0.191 and got["title"] == "CPI y/y + BOJ Policy Rate",
          "GBPJPY: GBP CPI up (+0.38, the base) and a BOJ hike (+0.762 for JPY, the quote): (0.38 - 0.762) / 2", got)
    check(S.context_for(gj, "GBPUSD")["expect"] == 0.38, "the same minute on GBPUSD: only the GBP event is in the pair")
    check(S.context_for(gj, "USDJPY")["expect"] == -0.762, "... on USDJPY: only the JPY event, JPY the quote")
    check(S.context_for(gj, "EURUSD")["expect"] is None, "... on EURUSD: neither currency is in the pair")
    # one title each, junk rows read as no event
    cpi = {"title": "CPI m/m", "currency": "USD", "forecast": "0.4%", "previous": "0.2%"}
    got = S.context_for([cpi, cpi], "EURUSD")
    check(got["title"] == "CPI m/m" and got["expect"] == -0.664, "the same event twice: one title", got)
    got = S.context_for([{"title": "", "currency": "USD"}, cpi], "EURUSD")
    check(got["title"] == "CPI m/m" and got["expect"] == -0.664, "an untitled row adds no empty title", got)
    got = S.context_for(["x", None, 5, ("USD", "n1", "Non-Farm Employment Change")], "EURUSD")
    check(got == {"expect": None, "ai": None, "title": ""},
          "junk rows (text, None, a number, a candidate's (currency, id, title)): no fields, no error", got)
    check(S.context_for("CPI m/m", "EURUSD") == {"expect": None, "ai": None, "title": ""},
          "a bare text is not an event (str.title is a method, not a title)")
    got = S.context_for(b"CPI m/m", "EURUSD")
    check(got == {"expect": None, "ai": None, "title": ""}, "bytes are not an event either (bytes.title is a method)",
          got)


# ── connector: H1 trend ────────────────────────────────────────
def test_trend():
    i = np.arange(120)
    rising, falling = frame(1.10 + 0.0005 * i, 0.002), frame(1.20 - 0.0005 * i, 0.002)
    up, down = V.trend_h1(rising), V.trend_h1(falling)
    check(up is not None and up > 0.5, "a steady H1 rise: clearly +", up)
    check(down is not None and down < -0.5, "a steady H1 fall: clearly -", down)
    check(near(up, -down, 1e-3), "rise and fall are mirror images", (up, down))
    check(V.trend_h1(frame(np.full(120, 1.1), 0.002)) == 0.0, "flat: 0")
    check(V.trend_h1(frame(np.full(120, 1.1), 0.0)) == 0.0, "no movement at all (ATR 0): 0, no division by zero")
    check(V.trend_h1(frame(1.10 + 0.0005 * np.arange(60), 0.002)) is None, "59 closed bars: None")
    check(V.trend_h1(frame(1.10 + 0.0005 * np.arange(61), 0.002)) is not None, "60 closed bars: a value")
    gold = frame(2400 + 0.5 * i, 2.0)
    check(near(V.trend_h1(gold), up, 1e-3), "measured in ATRs: gold reads like EURUSD", (V.trend_h1(gold), up))
    spiked = rising.copy()
    spiked.loc[spiked.index[-1], ["high", "low", "close"]] = [0.5, 0.4, 0.45]
    check(V.trend_h1(spiked) == up, "the forming bar is left out")
    before = rising.copy()
    V.trend_h1(rising)
    check(rising.equals(before), "the bars are not changed")
    nan = rising.copy()
    nan.loc[nan.index[-2], "close"] = float("nan")
    check(V.trend_h1(nan) is None, "a NaN close: None")
    junk = rising.copy().astype({"close": object})
    junk.loc[junk.index[5], "close"] = "x"
    check(V.trend_h1(junk) is None, "a junk value: None, no exception")
    check(V.trend_h1(rising.drop(columns=["high"])) is None, "a missing column: None")
    check(V.trend_h1(None) is None and V.trend_h1(pd.DataFrame()) is None and V.trend_h1("bars") is None,
          "no bars: None")
    noise = np.random.default_rng(7)
    vals = [abs(V.trend_h1(frame(1.1 + np.cumsum(noise.normal(0, 0.001, 120)), 0.001))) for _ in range(200)]
    check(float(np.median(vals)) < 0.5, "a random walk's trend is mostly weak (median |trend| < 0.5)",
          float(np.median(vals)))
    # the size pinned: EMA20 - EMA50 of the closed closes over 2 x ATR(14). Every bar spans 0.2 around its close and
    # the close climbs 0.01 a bar, so the true range (and the ATR) is 0.2 exactly
    c = 1.1 + 0.01 * np.arange(120)
    steady = bars(c, 0.1)
    closed = pd.Series(c[:-1])
    gap = closed.ewm(span=20, adjust=False).mean().iat[-1] - closed.ewm(span=50, adjust=False).mean().iat[-1]
    want = round(float(np.tanh(gap / (2 * 0.2))), 3)
    got = V.trend_h1(steady)
    check(got == want and got == 0.354, "trend = tanh((EMA20 - EMA50) / (2 x H1 ATR)) of the closed bars = 0.354",
          (got, want))
    # ... and at exactly TREND["min_bars"] closed bars, where the EMAs' start-up still shows: adjust=False (the
    # recursive EMA) gives 0.308, adjust=True would give 0.310 (on 119 bars the two agree to 3 decimals)
    got = V.trend_h1(bars(1.1 + 0.01 * np.arange(61), 0.1))
    check(got == 0.308, "trend on exactly 60 closed bars (EMAs with adjust=False) = 0.308", got)
    check(V.TREND["fast"] == 20 and V.TREND["slow"] == 50 and V.TREND["unit"] == 2.0 and V.TREND["atr"] == 14,
          "H1 EMA20 / EMA50, ATR(14), unit 2 ATR", V.TREND)
    # the ATR period pinned: the same closes, the last 15 closed bars 4x wider, so ATR(14) and ATR(7) differ
    half = np.full(120, 0.1)
    half[-16:] = 0.4
    widening = bars(c, half)
    want14 = round(float(np.tanh(gap / (2 * wilder(widening.iloc[:-1], 14)))), 3)
    want7 = round(float(np.tanh(gap / (2 * wilder(widening.iloc[:-1], 7)))), 3)
    got = V.trend_h1(widening)
    check(got == want14 == 0.122 and want7 != want14, "widening H1 bars: the trend is measured in ATR(14) (0.122), "
                                                      "not ATR(7)", (got, want14, want7))
    # the connector fetches H1_BARS rows: enough for the trend (its closed bars and the forming one)
    check(V.H1_BARS >= V.TREND["min_bars"] + 1, "H1_BARS covers the trend (closed bars + the forming one)", V.H1_BARS)
    check(V.trend_h1(rising.tail(V.H1_BARS)) == up and V.trend_h1(widening.tail(V.H1_BARS)) == got,
          "the trend on the H1_BARS rows the connector fetches is the same")


def test_atr():
    # Wilder smoothing, not a plain 14-bar average: a wide bar 20 rows back still weighs a little
    flat = bars(np.full(60, 1.1), 0.001)
    flat.loc[flat.index[-20], "high"] += 0.05
    got, want = V._atr(flat, 14), wilder(flat, 14)
    check(near(got, want, 1e-12) and near(want, 0.00287, 1e-2), "ATR is Wilder's smoothing (0.00287, not 0.0020)",
          (got, want))
    # the TRUE range: closes that jump 0.02 between bars 0.002 wide make a true range of 0.021, not 0.002
    zigzag = bars(np.where(np.arange(60) % 2, 1.12, 1.10), 0.001)
    got, want = V._atr(zigzag, 14), wilder(zigzag, 14)
    check(near(got, want, 1e-12) and got > 0.02, "ATR uses the true range (gaps to the previous close count)",
          (got, want))


# ── connector: M1 momentum ─────────────────────────────────────
def test_momentum():
    base = np.full(140, 1.1)
    rise = base.copy()
    rise[-30:] += 0.0001 * np.arange(1, 31)
    fall = base.copy()
    fall[-30:] -= 0.0001 * np.arange(1, 31)
    up, down = V.momentum_m1(frame(rise, 0.0003, "min")), V.momentum_m1(frame(fall, 0.0003, "min"))
    check(up is not None and up > 0.5, "the last half hour rising: clearly +", up)
    check(down is not None and down < -0.5, "falling: clearly -", down)
    check(near(up, -down, 1e-3), "mirror images", (up, down))
    check(V.momentum_m1(frame(base, 0.0003, "min")) == 0.0, "flat: 0")
    check(V.momentum_m1(frame(base, 0.0, "min")) == 0.0, "no movement at all: 0, no division by zero")
    check(V.momentum_m1(frame(rise[-44:], 0.0003, "min")) is None, "44 rows: None")
    check(V.momentum_m1(frame(rise[-45:], 0.0003, "min")) is not None, "45 rows: a value")
    late = base.copy()
    late[-1] += 0.003
    check((V.momentum_m1(frame(late, 0.0003, "min")) or 0) > 0.5, "the forming bar's close is the price now: it counts")
    old = base.copy()
    old[:-31] -= 0.01                                    # a fall more than 30 minutes back says nothing
    check(V.momentum_m1(frame(old, 0.0003, "min")) == 0.0, "moves before the last 30 minutes are not counted")
    check(V.momentum_m1(None) is None and V.momentum_m1(frame(rise, 0.0003, "min").drop(columns=["close"])) is None,
          "no bars / no close column: None")
    nan = frame(rise, 0.0003, "min")
    nan.loc[nan.index[-1], "close"] = float("nan")
    check(V.momentum_m1(nan) is None, "a NaN price now: None")
    junk = frame(rise, 0.0003, "min").astype({"close": object})
    junk.loc[junk.index[-5], "close"] = "x"
    try:
        got = V.momentum_m1(junk)
    except Exception as exc:                                    # noqa: BLE001 - the error is the finding
        got = exc
    check(got is None, "a junk M1 value: None, no exception", got)
    # the ATR is of the CLOSED bars: a forming bar that is wide just before the release (same close) changes nothing
    rising = frame(rise, 0.0003, "min")
    wide = rising.copy()
    wide.loc[wide.index[-1], "high"] += 0.003
    wide.loc[wide.index[-1], "low"] -= 0.003
    check(V.momentum_m1(rising) == V.momentum_m1(wide), "a wide forming bar does not damp the momentum",
          (V.momentum_m1(rising), V.momentum_m1(wide)))
    # the legs and the size pinned: every bar spans 0.2 (ATR 0.2), so momentum = tanh(move / (5 x 0.2)) = tanh(move),
    # move = the average of the 15- and 30-minute changes up to the price now. One +0.01 step from row k on:
    for k, move, what in ((-13, 0.01, "both legs"), (-15, 0.01, "both legs"),
                          (-16, 0.005, "the 30-minute leg only"), (-25, 0.005, "the 30-minute leg only"),
                          (-30, 0.005, "the 30-minute leg only"), (-31, 0.0, "neither leg")):
        c = np.full(140, 1.1)
        c[k:] += 0.01
        got, want = V.momentum_m1(bars(c, 0.1, "min")), round(float(np.tanh(move)), 3) + 0.0
        check(got == want, f"a +0.01 step on the last {-k} rows: {what}, {want}", got)
    check(V.MOMENTUM["short"] == 15 and V.MOMENTUM["long"] == 30 and V.MOMENTUM["unit"] == 5.0
          and V.MOMENTUM["atr"] == 14, "15 / 30 minutes, ATR(14), unit 5 ATR", V.MOMENTUM)
    # the ATR period pinned: the last 15 closed bars 4x wider, so ATR(14) and ATR(7) differ
    c = np.full(140, 1.1)
    c[-30:] += 0.01 * np.arange(1, 31)
    half = np.full(140, 0.1)
    half[-16:] = 0.4
    widening = bars(c, half, "min")
    move = (c[-1] - c[-16] + c[-1] - c[-31]) / 2
    want14 = round(float(np.tanh(move / (5 * wilder(widening.iloc[:-1], 14)))), 3)
    want7 = round(float(np.tanh(move / (5 * wilder(widening.iloc[:-1], 7)))), 3)
    got = V.momentum_m1(widening)
    check(got == want14 == 0.075 and want7 != want14, "widening M1 bars: the momentum is measured in ATR(14) "
                                                      "(0.075), not ATR(7)", (got, want14, want7))
    # a push rounding to 0 from below is 0.0, never a -0.0 (it would be stored as "-0.0" in the analysis)
    c = np.full(140, 1.1)
    c[-1] -= 0.00001
    got = V.momentum_m1(bars(c, 0.1, "min"))
    check(got == 0.0 and str(got) == "0.0", "a tiny fall rounding to 0: 0.0, not -0.0", got)
    # the connector fetches M1_BARS rows: enough for the momentum
    check(V.M1_BARS >= V.MOMENTUM["min_rows"], "M1_BARS covers the momentum", V.M1_BARS)
    check(V.momentum_m1(frame(rise, 0.0003, "min").tail(V.M1_BARS)) == up and
          V.momentum_m1(widening.tail(V.M1_BARS)) == want14, "the momentum on the M1_BARS rows fetched is the same")


# ── connector: blow-out ────────────────────────────────────────
def calm(n=140, width=0.0002):
    return frame(np.full(n, 1.1), width, "min")


def spike(df, row, up):
    out = df.copy()
    out.loc[out.index[row], "high"] = out["close"].iat[row] + up
    return out


def test_blow_out():
    w = 0.0002
    check(V.blow_out(calm()) is False, "a calm hour: False (a real bool)")
    check(V.blow_out(spike(calm(), -3, 3.5 * w)) is True, "the last 10 minutes span 4x the usual range: True")
    check(V.blow_out(spike(calm(), -1, 3.5 * w)) is True, "a jump in the forming bar counts")
    check(V.blow_out(spike(calm(), -3, 2.0 * w)) is False, "2.5x the usual range: not a blow-out")
    check(V.blow_out(spike(calm(), -10, 3.5 * w)) is True, "the oldest of the last 10 rows is inside the window")
    check(V.blow_out(spike(calm(), -11, 3.5 * w)) is False, "the row before the window is not")
    check(V.blow_out(spike(calm(), -50, 20 * w)) is False, "an old spike in the 2 hours before: False")
    check(V.blow_out(spike(calm(129), -3, 3.5 * w)) is False, "129 rows: too few, False")
    check(V.blow_out(spike(calm(130), -3, 3.5 * w)) is True, "130 rows: enough")
    flat = calm(width=0.0)
    flat.loc[flat.index[-3], "high"] = 1.1005
    check(V.blow_out(flat) is False, "no usual range to compare with (median 0): False")
    check(V.blow_out(None) is False and V.blow_out(pd.DataFrame()) is False, "no bars: False")
    junk = calm().astype({"high": object})
    junk.loc[junk.index[-2], "high"] = "x"
    check(V.blow_out(junk) is False, "a junk value: False, no exception")
    # the threshold pinned on exact binary numbers: closes 100.0, every row 99.75..100.25, so every 10-row range is
    # 0.5 and so is their median
    even = bars(np.full(140, 100.0), 0.25, "min")
    at3 = even.copy()
    at3.loc[at3.index[-3], "high"] = 101.25                  # the last range 1.5 = exactly 3 x the median
    check(V.blow_out(at3) is False, "exactly 3x the usual range: not yet a blow-out (over 3x is)")
    over3 = even.copy()
    over3.loc[over3.index[-3], "high"] = 101.3               # 1.55 = 3.1x
    check(V.blow_out(over3) is True, "3.1x the usual range: a blow-out")
    old = even.copy()
    old.loc[old.index[-50], "high"] = 110.0                   # one wild range in the 2 hours before ...
    old.loc[old.index[-3], "high"] = 101.75                   # ... and 4x the usual one now
    check(V.blow_out(old) is True, "the usual range is the MEDIAN: one old spike does not raise it")
    check(V.BLOW_OUT == {"window": 10, "history": 120, "mult": 3.0}, "10-minute ranges, 2 hours, 3x", V.BLOW_OUT)
    # the history pinned: the median of ALL twelve 10-row ranges before the window, not of the last hour only, not
    # with the window's own range in it, not eleven. The first hour's six ranges are 1.0, the second hour's 0.5:
    # the median of the twelve is 0.75, so the line is 3 x 0.75 = 2.25
    c = np.full(140, 100.0)
    half = np.full(140, 0.25)
    half[10:70] = 0.5                                         # rows 10..69: the first hour of the 2 hours (1.0 ranges)
    two = bars(c, half, "min")
    under = two.copy()
    under.loc[under.index[-3], "high"] = 101.75              # the last range 2.0: under 2.25 (over 3 x the last hour's)
    check(V.blow_out(under) is False, "the usual range is the median of all twelve blocks (0.75): 2.0 is not a blow-out")
    over = two.copy()
    over.loc[over.index[-3], "high"] = 102.25                # 2.5: over 2.25 (under 3 x a median that took it in)
    check(V.blow_out(over) is True, "... and 2.5 is (the window's own range is not in the median)")
    # the connector fetches M1_BARS rows: enough for every M1 measure
    check(V.M1_BARS >= V.BLOW_OUT["window"] + V.BLOW_OUT["history"] and V.M1_BARS >= V.MOMENTUM["min_rows"],
          "M1_BARS covers every M1 measure", V.M1_BARS)
    check(V.blow_out(spike(calm(), -3, 3.5 * w).tail(V.M1_BARS)) is True and V.blow_out(over.tail(V.M1_BARS)) is True,
          "a blow-out on the M1_BARS rows fetched is still seen")


# ── connector: the decision ────────────────────────────────────
def test_decide():
    check(near(sum(V.WEIGHTS.values()), 1.0) and V.WEIGHTS["expect"] <= 0.2 and V.EXPECT_MAX <= 0.2,
          "weights add up to 1 and the forecast's weight is capped at 0.2", V.WEIGHTS)
    check(V.WEIGHTS["expect"] < V.CLEAR and V.WEIGHTS["expect"] + V.WEIGHTS["ai"] < V.CLEAR,
          "the forecast (even with the AI) can never reach the clear score on its own")
    d = V.decide(None, None, 1.0, None, True, False)
    check(d["action"] == "both" and d["score"] == V.WEIGHTS["expect"] and d["agree"] == 1,
          "expect alone at its strongest: both, never one side", d)
    check(V.decide(None, None, -1.0, None, True, False)["action"] == "both", "nor the other side")
    check(V.decide(None, None, 1.0, None, True, False, clear=0.1)["action"] == "both",
          "even with a low threshold, expect alone is one part: both")
    check(V.decide(None, None, 1.0, 1.0, True, False)["action"] == "both", "expect + AI at their strongest: under 0.5")
    d = V.decide(0.8, 0.7, None, None, True, False)
    check(d["action"] == "buy" and d["reason"] == "clear" and d["agree"] == 2 and near(d["score"], 0.525),
          "trend + momentum agreeing over the threshold: buy", d)
    d = V.decide(-0.8, -0.7, None, None, True, False)
    check(d["action"] == "sell" and near(d["score"], -0.525), "the same down: sell", d)
    d = V.decide(0.7, 0.6, 0.4, None, True, False)
    check(d["action"] == "buy" and d["agree"] == 3, "the forecast tips a close call over the threshold", d)
    d = V.decide(0.7, 0.6, None, None, True, False)
    check(d["action"] == "both" and d["reason"] == "weak", "... which without it stays both", d)
    d = V.decide(1.0, None, None, None, True, False)
    check(d["action"] == "both" and d["reason"] == "weak" and d["agree"] == 1, "one strong part alone: both", d)
    d = V.decide(1.0, None, None, None, True, False, clear=0.3)
    check(d["action"] == "both" and d["reason"] == "alone",
          "one strong part over a low threshold: still both (two parts must agree)", d)
    d = V.decide(1.0, 0.02, 0.02, 0.02, True, False, clear=0.3)
    check(d["action"] == "both" and d["agree"] == 1, "parts close to 0 do not count as agreeing", d)
    d = V.decide(None, None, None, None, True, False)
    check(d["action"] == "both" and d["score"] == 0.0 and d["agree"] == 0
          and d["parts"] == {"trend": None, "momentum": None, "expect": None, "ai": None},
          "every part missing: both, never skip", d)
    check(V.decide(None, None, None, None, None, False)["action"] == "both", "spread unknown (None): no skip")
    d = V.decide(1.0, 1.0, 1.0, 1.0, False, False)
    check(d["action"] == "skip" and d["reason"] == "spread", "spread over its cap: skip, however clear", d)
    d = V.decide(1.0, 1.0, 1.0, 1.0, True, True)
    check(d["action"] == "skip" and d["reason"] == "blow_out", "blow-out: skip", d)
    d = V.decide(1.0, 1.0, 1.0, 1.0, None, True)
    check(d["action"] == "skip" and d["reason"] == "blow_out", "blow-out skips even with the spread unknown (None)", d)
    check(V.decide(None, None, None, None, False, True)["reason"] == "spread", "both bad: the spread is named")
    d = V.decide(1.0, 1.0, 1.0, 1.0, True, False)
    check(d["action"] == "buy" and d["score"] == 1.0 and d["agree"] == 4, "everything up: buy at 1.0", d)
    check(V.decide(-1, -1, -1, -1, True, False)["score"] == -1.0, "everything down: -1.0")
    d = V.decide(1.0, 1.0, -1.0, -1.0, True, False)
    check(d["action"] == "both" and near(d["score"], 0.4), "forecast and AI against the chart: back to both", d)
    d = V.decide(1.0, -1.0, 1.0, 1.0, True, False)
    check(d["action"] == "both" and near(d["score"], 0.3), "trend and momentum disagree: both", d)
    d = V.decide(5, -7, float("nan"), "x", True, False)
    check(d["parts"] == {"trend": 1.0, "momentum": -1.0, "expect": None, "ai": None},
          "parts clamped to [-1, 1]; NaN and junk are missing", d)
    check(V.decide(True, True, None, None, True, False)["parts"]["trend"] is None, "a bool is not a part")
    d = V.decide(np.float32(0.8), np.float64(0.7), -0.0001, None, np.bool_(True), np.bool_(False))
    check(d["action"] == "buy" and d["parts"]["trend"] == 0.8 and str(d["parts"]["expect"]) == "0.0",
          "numpy numbers and bools are read; a part rounding to 0 is 0.0, not -0.0", d)
    d = V.decide(0.5, -0.5, None, None, True, False)
    check(d["score"] == 0.0 and str(d["score"]) == "0.0" and d["agree"] == 0, "a zero score is 0.0, nobody agrees", d)
    d = V.decide(-0.001, None, None, None, True, False)
    check(str(d["score"]) == "0.0" and d["agree"] == 0, "a score rounding to 0 from below is 0.0, not -0.0", d)
    check(V.decide(None, None, None, None, True, False, clear=0)["reason"] == "weak",
          "a zero score is weak even at threshold 0 (never 'alone')")
    check(V.decide(0.8, 0.7, None, None, True, False, clear="x")["action"] == "buy", "a junk threshold: the default")
    check(V.decide(0.8, 0.7, None, None, True, False, clear=0.6)["action"] == "both", "a higher threshold is honoured")
    # the junk threshold is the default 0.5: not 0 (a score of 0.455 with two parts behind it stays both) and not 1
    # (0.525 with two parts trades one side; a NaN clamped by min / max would read as 1)
    for junk in ("x", None, float("nan"), [], 10 ** 400):
        d = V.decide(0.7, 0.6, None, None, True, False, clear=junk)
        check(d["action"] == "both" and d["score"] == 0.455, f"a junk threshold {junk!r} is CLEAR: 0.455 is both", d)
        d = V.decide(0.8, 0.7, None, None, True, False, clear=junk)
        check(d["action"] == "buy", f"a junk threshold {junk!r} is CLEAR, not 1: 0.525 trades one side", d)
    # numpy bools (a spread check worked out on numpy / pandas values) in the skip direction too
    d = V.decide(0.8, 0.7, None, None, np.bool_(False), False)
    check(d["action"] == "skip" and d["reason"] == "spread", "a numpy False spread: skip", d)
    d = V.decide(0.8, 0.7, None, None, True, np.bool_(True))
    check(d["action"] == "skip" and d["reason"] == "blow_out", "a numpy True blow-out: skip", d)
    check(V.decide(0.8, 0.7, None, None, 0, False)["reason"] == "spread", "a falsy spread_ok (0): skip")
    check(V.decide(1, 1, 1, 1, True, False, clear=2.0)["action"] == "buy",
          "a threshold over 1 is clamped to 1: a full score still trades one side")
    check(V.decide(0.1, 0.1, None, None, True, False, clear=-1)["action"] == "buy", "a negative threshold is 0")
    # exactly on the threshold counts (plan: |s| >= 0.5)
    d = V.decide(1.0, None, 1.0, None, True, False)
    check(d["score"] == 0.5 and d["agree"] == 2 and d["action"] == "buy" and d["reason"] == "clear",
          "|s| exactly 0.5 with two parts agreeing: one side", d)
    d = V.decide(-1.0, None, -1.0, None, True, False)
    check(d["score"] == -0.5 and d["action"] == "sell", "... and -0.5: sell", d)
    # QUIET pinned: a part of exactly 0.05 agrees, 0.049 says nothing
    d = V.decide(1.0, 0.05, None, None, True, False, clear=0.3)
    check(d["agree"] == 2 and d["action"] == "buy", "a second part at exactly QUIET (0.05) agrees: one side", d)
    d = V.decide(1.0, 0.049, None, None, True, False, clear=0.3)
    check(d["agree"] == 1 and d["action"] == "both" and d["reason"] == "alone",
          "a second part just under QUIET does not: both", d)
    check(V.QUIET == 0.05 and V.CLEAR == 0.5 and V.AGREE == 2, "QUIET 0.05, CLEAR 0.5, AGREE 2")
    d = V.decide(0.8, 0.7, None, None, True, False)
    check(set(d) == {"action", "score", "parts", "agree", "reason"} and d["reason"] in V.REASONS,
          "the answer's keys", d)
    # REASONS is what the connector and the UI match decide()'s reason against: exactly the reasons it gives
    reasons = {V.decide(*a)["reason"] for a in ((1, 1, 1, 1, False, False), (1, 1, 1, 1, True, True),
                                                 (0.8, 0.7, None, None, True, False),
                                                 (1.0, None, None, None, True, False, 0.3),
                                                 (None, None, None, None, True, False))}
    check(reasons == set(V.REASONS) and len(V.REASONS) == len(set(V.REASONS)),
          "every reason decide() gives is in REASONS, and every listed reason occurs", (reasons, V.REASONS))


# ── connector: the whole look ──────────────────────────────────
def test_analyse():
    i = np.arange(120)
    h1 = frame(1.10 + 0.0005 * i, 0.002)
    # a random-walk minute chart (seeded) that rises steadily over its last half hour: momentum, not a blow-out
    m = 1.1 + np.cumsum(np.random.default_rng(0).normal(0, 0.0001, 140))
    m[-30:] += 0.00007 * np.arange(1, 31)
    m1 = frame(m, 0.0001, "min")
    check(V.momentum_m1(m1) > 0.5 and V.blow_out(m1) is False, "a steady push is momentum, not a blow-out",
          (V.momentum_m1(m1), V.blow_out(m1)))
    d = V.analyse(h1, m1, {"expect": 0.5, "ai": None, "title": "CPI m/m"})
    check(d["action"] == "buy" and d["parts"]["expect"] == 0.5, "a rising chart and a good forecast: buy", d)
    check(V.analyse(h1, m1, None)["action"] == "buy", "no context: the chart alone can still be clear")
    d = V.analyse(h1, m1, None, clear=0.99)
    check(d["action"] == "both" and d["reason"] == "weak", "analyse passes `clear` on: a 0.99 threshold is not met", d)
    blown = spike(m1, -2, 0.01)
    check(V.analyse(h1, blown, {"expect": 0.5})["action"] == "skip", "a blow-out on M1: skip")
    d = V.analyse(h1, blown, None, spread_ok=None)
    check(d["action"] == "skip" and d["reason"] == "blow_out", "a blow-out on M1 with the spread unknown: skip", d)
    check(V.analyse(h1, m1, {"expect": 0.5}, spread_ok=False)["action"] == "skip", "spread over its cap: skip")
    d = V.analyse(None, None, "junk")
    check(d["action"] == "both" and d["agree"] == 0, "no bars, junk context: the straddle", d)
    d = V.analyse(None, None, {"expect": None, "ai": 0.7})
    check(d["parts"]["ai"] == 0.7 and d["parts"]["expect"] is None and d["score"] == round(0.15 * 0.7, 3),
          "analyse passes the AI bias on as its own part", d)
    junk = m1.astype({"close": object})
    junk.loc[junk.index[-5], "close"] = "x"
    try:
        d = V.analyse(h1, junk)
    except Exception as exc:                                    # noqa: BLE001 - the error is the finding
        d = {"action": exc}
    check(d["action"] == "both" and d["parts"]["momentum"] is None,
          "junk M1 bars: the straddle (momentum missing), no exception", d)


# ── connector: the history line ────────────────────────────────
def test_describe():
    buy = {"action": "buy", "score": 0.62, "parts": {"trend": 0.8, "momentum": 0.9, "expect": None, "ai": 0.0},
           "agree": 2, "reason": "clear"}
    check(V.describe(buy, "ro") == "Analiză: BUY clar (0.62): trend H1 +, momentum +", "RO one side",
          V.describe(buy, "ro"))
    check(V.describe(buy, "en") == "Analysis: BUY clear (0.62): H1 trend +, momentum +", "EN one side",
          V.describe(buy, "en"))
    sell = V.decide(-0.8, -0.9, -0.5, None, True, False)
    check(V.describe(sell, "en") == f"Analysis: SELL clear ({sell['score']:.2f}): H1 trend -, momentum -, forecast -",
          "EN sell, with the forecast", V.describe(sell, "en"))
    both = V.decide(0.6, -0.3, 0.2, 0.7, True, False)
    t = V.describe(both, "en")
    check(t.startswith(f"Analysis: unclear ({both['score']:.2f}), both sides at reduced risk: ")
          and t.endswith("H1 trend +, momentum -, forecast +, AI +"), "EN both", t)
    t = V.describe(both, "ro")
    check(t.startswith("Analiză: neclar (") and "ambele direcții cu risc redus" in t
          and t.endswith("așteptări +, AI +"), "RO both", t)
    check(V.describe(V.decide(1, 1, 1, 1, False, False), "ro") == "Analiză: fără tranzacție, spread peste limită",
          "RO skip: spread")
    check(V.describe(V.decide(1, 1, 1, 1, True, True), "en").startswith("Analysis: no trade, the price already jumped"),
          "EN skip: blow-out")
    check(V.describe(V.decide(None, None, None, None, True, False), "en")
          == "Analysis: unclear (0.00), both sides at reduced risk: no signals", "nothing known: no signals")
    t = V.describe(buy, "ro", shadow=True)
    check(t == "Analiză (doar observă), ar fi ales: BUY clar (0.62): trend H1 +, momentum +", "RO watch-only", t)
    t = V.describe(buy, "en", shadow=True)
    check(t.startswith("Analysis (watch only), would have: BUY clear (0.62)"), "EN watch-only", t)
    check(V.describe(buy, "ro-RO").startswith("Analiză:") and V.describe(buy, None).startswith("Analysis:")
          and V.describe(buy, "fr").startswith("Analysis:"), "lang: ro... is Romanian, anything else English")
    check(V.describe(buy, "RO").startswith("Analiză:") and V.describe(buy, "Ro-ro").startswith("Analiză:"),
          "lang is read in any case", V.describe(buy, "RO"))
    check(V.describe(None, "en") == "Analysis: unavailable"
          and V.describe({"action": "?"}, "ro") == "Analiză: indisponibilă", "no decision: unavailable")
    check(V.describe({"action": "buy", "score": "x", "parts": None}, "en") == "Analysis: BUY clear (0.00): no signals",
          "a damaged decision never raises")
    try:
        t = V.describe({"action": "buy", "score": 0.6, "parts": ["x"]}, "en")
    except Exception as exc:                                    # noqa: BLE001 - the error is the finding
        t = exc
    check(t == "Analysis: BUY clear (0.60): no signals", "parts not a dict (a list): no signals, no exception", t)
    d = {"action": "buy", "score": 0.62, "parts": {"trend": 0.8, "momentum": 0.05, "expect": 0.02, "ai": None}}
    check(V.describe(d, "en") == "Analysis: BUY clear (0.62): H1 trend +, momentum +",
          "a part under QUIET is not listed, one at QUIET is", V.describe(d, "en"))


# ── both modules stay pure ─────────────────────────────────────
def imports_of(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            out.add("." * node.level + (node.module or "").split(".")[0])
    return out


def test_pure():
    got = imports_of(ROOT / "bot" / "news_view.py")
    check(got <= {"functools", "math", "numbers", "statistics", "numpy", "pandas"},
          "bot/news_view.py needs only the standard library, numpy and pandas (it ships with the connector)", got)
    got = imports_of(ROOT / "server" / "news_view.py")
    check(got <= {"math", "re", "datetime"}, "server/news_view.py has no database, settings or network", got)
    parts = (0.4, None, 0.1, None)
    V.decide(*parts, True, False)
    weights = {"trend": 0.35, "momentum": 0.35, "expect": 0.15, "ai": 0.15}
    check(parts == (0.4, None, 0.1, None) and V.WEIGHTS == weights, "decide changes neither its inputs nor the weights")


if __name__ == "__main__":
    t0 = time.time()
    for fn in (test_parse, test_polarity, test_expect, test_pair_side, test_ai_part, test_context_for,
               test_context_release, test_trend, test_atr, test_momentum, test_blow_out, test_decide, test_analyse,
               test_describe, test_pure):
        print(f"── {fn.__name__}")
        try:
            fn()
        except Exception as exc:                                # a crash is a failure, the next test still runs
            import traceback
            traceback.print_exc()
            check(False, f"{fn.__name__} crashed", exc)
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - t0:.1f} s)")
    sys.exit(1 if FAILED else 0)
