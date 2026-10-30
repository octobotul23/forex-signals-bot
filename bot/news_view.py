"""Pre-news analysis, connector side: just before a news run (classic burst or Rapid) places its orders, a quick look
at the chart decides whether it trades one side, both sides, or nothing.

The look has four parts, each a number in [-1, 1] (+ = up, - = down) or None when it cannot be worked out:
- trend:    H1 EMA20 vs EMA50 (trend_h1);
- momentum: the last 15 and 30 minutes on M1 (momentum_m1);
- expect:   the release's forecast vs its previous value, already turned into the pair's direction by the server
            (server/news_view.py context_for);
- ai:       the server's newest AI bias for the pair, if fresh (same place).
decide() weighs them into one score s in [-1, 1]:
- one side ("buy" / "sell") at full risk only when |s| >= clear AND at least two parts point the same way as s;
- "skip" when the spread is over its cap or the price already blew out (blow_out) before the release;
- otherwise "both": the usual straddle, at reduced risk.
A part that is missing or fails counts 0: it can only lead to the straddle, never to a skip.

Pure functions. The bars are DataFrames as MT5Broker.rates returns them (time, open, high, low, close, tick_volume,
spread; the LAST row is the bar still forming). Every measure is in ATR units, so it reads the same on EURUSD, GBPJPY
or gold. bot/indicators.py is not used: it is not part of the connector's download.
"""
import functools
import math
import numbers
import statistics

import numpy as np
import pandas as pd

H1_BARS = 120          # bars the connector asks for (rates(symbol, "H1", H1_BARS)) ...
M1_BARS = 140          # ... and rates(symbol, "M1", M1_BARS): enough for every measure below
TREND = {
    "fast": 20, "slow": 50,    # the two H1 EMAs
    "atr": 14,
    "min_bars": 60,            # closed H1 bars needed: fewer, no trend
    "unit": 2.0,               # an EMA gap of this many H1 ATRs reads as tanh(1) = 0.76; on a random walk the gap is
                               # about 1 ATR (1 sd), so plain noise stays near 0.5
}
MOMENTUM = {
    "short": 15, "long": 30,   # minutes the price change is measured over (averaged)
    "atr": 14,
    "min_rows": 45,            # M1 rows needed (the forming one included): 30 back plus a warmed-up ATR
    "unit": 5.0,               # an average change of this many M1 ATRs reads as 0.76; noise (1 sd) is about 2.7 ATRs
}
BLOW_OUT = {
    "window": 10,              # minutes in one range (the last one ends with the forming bar)
    "history": 120,            # minutes before it the usual range is taken from (12 ranges, their median)
    "mult": 3.0,               # the last range over this many medians: blown out
}
# How much each part weighs in the score; they add up to 1, so s stays in [-1, 1]. The chart (trend, momentum) carries
# most of it. The forecast is what the market already expects and has priced in: it may tip a close call but must never
# decide alone, so its weight stays <= EXPECT_MAX (with the default clear 0.5 it adds at most 0.15). The AI bias is
# hours old and broad: the same small weight.
WEIGHTS = {"trend": 0.35, "momentum": 0.35, "expect": 0.15, "ai": 0.15}
EXPECT_MAX = 0.2
CLEAR = 0.5            # |s| needed for one side
AGREE = 2              # parts that must point the way of s for one side
QUIET = 0.05           # a part closer to 0 than this says nothing: it is not counted as agreeing
PARTS = ("trend", "momentum", "expect", "ai")
REASONS = ("spread", "blow_out", "clear", "alone", "weak")       # decide()'s "reason", see there
_COLS = {"high", "low", "close"}

assert abs(sum(WEIGHTS.values()) - 1) < 1e-9 and WEIGHTS["expect"] <= EXPECT_MAX


def _never_raises(default):
    """A measure that fails on odd bars (junk values, NaN, a missing column) returns `default` instead of raising:
    the run then falls back to the straddle, it is never lost to an exception."""
    def wrap(fn):
        @functools.wraps(fn)
        def safe(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception:
                return default
        return safe
    return wrap


def _prices(df, rows: int, closed: bool = False) -> pd.DataFrame | None:
    """high, low and close of the bars as floats (the forming last row left out when `closed`), or None when `df` is
    not bars, has fewer than `rows` of them, or holds a value that is not a finite number (NaN, inf, junk): pandas
    would quietly step over a NaN, and a measure built around a hole is not one."""
    if not isinstance(df, pd.DataFrame) or not _COLS.issubset(df.columns):
        return None
    px = df[["high", "low", "close"]].iloc[:-1 if closed else None]
    if len(px) < rows:
        return None
    px = px.astype(float)                                  # junk text raises: _never_raises turns it into None
    return px if np.isfinite(px.to_numpy()).all() else None


def _atr(px: pd.DataFrame, n: int) -> float:
    """Wilder's average true range of the bars `px` (_prices) at their last row (NaN with fewer than n rows)."""
    high, low, close = px["high"], px["low"], px["close"]
    prev = close.shift()
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean().iat[-1])


def _squash(x: float) -> float:
    return round(math.tanh(x), 3) + 0.0                    # + 0.0: never a -0.0


@_never_raises(None)
def trend_h1(h1: pd.DataFrame) -> float | None:
    """The H1 trend in [-1, 1]: EMA20 minus EMA50 of the closed bars' closes, in H1 ATR(14) units, squashed (tanh).
    + = up. None with fewer than TREND["min_bars"] closed bars or a bad value in them; 0 when the price never moved."""
    t = TREND
    px = _prices(h1, t["min_bars"], closed=True)           # the forming bar is left out
    if px is None:
        return None
    close = px["close"]
    gap = float(close.ewm(span=t["fast"], adjust=False).mean().iat[-1]
                - close.ewm(span=t["slow"], adjust=False).mean().iat[-1])
    atr = _atr(px, t["atr"])
    if not (math.isfinite(gap) and math.isfinite(atr)):
        return None
    if atr <= 0:
        return 0.0
    return _squash(gap / (t["unit"] * atr))


@_never_raises(None)
def momentum_m1(m1: pd.DataFrame) -> float | None:
    """The last half hour's push in [-1, 1]: the average of the 15- and 30-minute close changes up to the price now
    (the forming bar's close), in M1 ATR(14) units of the closed bars, squashed (tanh). + = up. None with fewer than
    MOMENTUM["min_rows"] rows or a bad value in them; 0 when the price never moved."""
    m = MOMENTUM
    px = _prices(m1, m["min_rows"])
    if px is None:
        return None
    close = px["close"]
    now = float(close.iat[-1])
    move = (now - float(close.iat[-1 - m["short"]]) + now - float(close.iat[-1 - m["long"]])) / 2
    atr = _atr(px.iloc[:-1], m["atr"])
    if not (math.isfinite(move) and math.isfinite(atr)):
        return None
    if atr <= 0:
        return 0.0
    return _squash(move / (m["unit"] * atr))


@_never_raises(False)
def blow_out(m1: pd.DataFrame) -> bool:
    """True when the price is already running wild: the high-low range of the last 10 M1 rows (the forming one
    included) is over 3 x the median range of the twelve 10-row blocks of the 2 hours before them. A straddle placed
    into that is filled at random. False with too few rows, a bad value in them, or no usual range to compare with
    (median 0)."""
    b = BLOW_OUT
    w, need = b["window"], b["window"] + b["history"]
    px = _prices(m1.tail(need) if isinstance(m1, pd.DataFrame) else None, need)
    if px is None:
        return False
    high, low = px["high"].to_numpy(), px["low"].to_numpy()
    ranges = [float(high[i:i + w].max() - low[i:i + w].min()) for i in range(0, need, w)]
    last, usual = ranges[-1], statistics.median(ranges[:-1])
    if usual <= 0:
        return False
    return bool(last > b["mult"] * usual)


def _part(v) -> float | None:
    """A part as decide() uses it: a finite number clamped to [-1, 1], else None (missing)."""
    if isinstance(v, bool) or not isinstance(v, numbers.Real) or not math.isfinite(v):
        return None
    return round(max(-1.0, min(1.0, float(v))), 3) + 0.0


def decide(trend: float | None, momentum: float | None, expect: float | None, ai: float | None,
           spread_ok: bool | None, blown: bool, clear: float = CLEAR) -> dict:
    """What a news run does, from the four parts (each in [-1, 1] or None = missing, which counts 0):
    {"action": "buy" | "sell" | "both" | "skip",
     "score": s = the WEIGHTS-weighted sum of the parts, in [-1, 1],
     "parts": {"trend", "momentum", "expect", "ai"} as used (clamped, None when missing),
     "agree": how many parts point the way of s (|part| >= QUIET, same sign; 0 when s is 0),
     "reason": "spread" (spread over its cap: skip) | "blow_out" (skip) | "clear" (one side) |
               "alone" (|s| >= clear but fewer than AGREE parts behind it: both) | "weak" (|s| < clear: both)}.
    spread_ok: False = the spread is over its cap (None = not known: no skip). blown: blow_out() of the M1 bars.
    clear: the |s| one side needs (CLEAR by default; clamped to [0, 1]; junk or NaN = CLEAR)."""
    parts = {k: _part(v) for k, v in zip(PARTS, (trend, momentum, expect, ai))}
    s = max(-1.0, min(1.0, sum(WEIGHTS[k] * (v or 0.0) for k, v in parts.items())))
    s = round(s, 3) + 0.0                                  # + 0.0: never a -0.0
    agree = 0 if s == 0 else sum(1 for v in parts.values() if v is not None and abs(v) >= QUIET and (v > 0) == (s > 0))
    out = {"action": "both", "score": s, "parts": parts, "agree": agree}
    try:
        clear = float(clear)
    except (TypeError, ValueError, OverflowError):
        clear = CLEAR
    clear = CLEAR if math.isnan(clear) else max(0.0, min(1.0, clear))    # a NaN would clamp to 1: junk too
    if spread_ok is not None and not spread_ok:
        return {**out, "action": "skip", "reason": "spread"}
    if blown:
        return {**out, "action": "skip", "reason": "blow_out"}
    if abs(s) >= clear and s != 0:
        if agree >= AGREE:
            return {**out, "action": "buy" if s > 0 else "sell", "reason": "clear"}
        return {**out, "reason": "alone"}
    return {**out, "reason": "weak"}


def analyse(h1: pd.DataFrame | None, m1: pd.DataFrame | None, context: dict | None = None,
            spread_ok: bool | None = True, clear: float = CLEAR) -> dict:
    """The whole look in one call: decide() on trend_h1(h1), momentum_m1(m1), blow_out(m1) and the server's context
    ({"expect", "ai", "title"}, server/news_view.py context_for; None or junk = no context)."""
    ctx = context if isinstance(context, dict) else {}
    return decide(trend_h1(h1), momentum_m1(m1), ctx.get("expect"), ctx.get("ai"), spread_ok, blow_out(m1), clear)


_TEXT = {
    "ro": {"head": "Analiză", "shadow": "Analiză (doar observă), ar fi ales", "clear": "clar",
           "both": "neclar ({s}), ambele direcții cu risc redus", "spread": "fără tranzacție, spread peste limită",
           "blow_out": "fără tranzacție, prețul a sărit deja (ultimele 10 min > 3× normal)",
           "none": "fără semnale", "na": "indisponibilă",
           "parts": {"trend": "trend H1", "momentum": "momentum", "expect": "așteptări", "ai": "AI"}},
    "en": {"head": "Analysis", "shadow": "Analysis (watch only), would have", "clear": "clear",
           "both": "unclear ({s}), both sides at reduced risk", "spread": "no trade, spread over its cap",
           "blow_out": "no trade, the price already jumped (last 10 min > 3x usual)",
           "none": "no signals", "na": "unavailable",
           "parts": {"trend": "H1 trend", "momentum": "momentum", "expect": "forecast", "ai": "AI"}},
}


def describe(decision: dict | None, lang: str = "en", shadow: bool = False) -> str:
    """decide()'s answer as one short line for the burst history, in Romanian (lang "ro...") or English, e.g.
    "Analiză: BUY clar (0.62): trend H1 +, momentum +" / "Analysis: unclear (0.21), both sides at reduced risk: H1
    trend +, momentum -" / "Analysis: no trade, spread over its cap". The parts listed are those that say something
    (|part| >= QUIET), with their direction. shadow: the analysis is only watched (its switch is off), so the line
    says what it would have done."""
    t = _TEXT["ro" if str(lang or "").lower().startswith("ro") else "en"]
    head = t["shadow"] if shadow else t["head"]
    if not isinstance(decision, dict) or decision.get("action") not in ("buy", "sell", "both", "skip"):
        return f"{head}: {t['na']}"
    action = decision["action"]
    if action == "skip":
        return f"{head}: {t['blow_out'] if decision.get('reason') == 'blow_out' else t['spread']}"
    s = _part(decision.get("score")) or 0.0
    parts = decision.get("parts") if isinstance(decision.get("parts"), dict) else {}
    said = [f"{t['parts'][k]} {'+' if v > 0 else '-'}" for k in PARTS
            if (v := _part(parts.get(k))) is not None and abs(v) >= QUIET]
    why = ", ".join(said) or t["none"]
    if action == "both":
        return f"{head}: {t['both'].format(s=f'{s:.2f}')}: {why}"
    return f"{head}: {action.upper()} {t['clear']} ({s:.2f}): {why}"
