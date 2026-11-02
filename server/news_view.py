"""Pre-news analysis, server side: the two parts of the look the connector cannot work out from its chart, computed
once per (release, pair) and sent to it as the run's top-level `context` (bot/news_view.py weighs them with its own).

- expect: each event's forecast against its previous value (the change the market expects), read with the event's
  polarity (a higher unemployment rate is bad for its currency), turned into the pair's direction (good for USD is
  down for EURUSD, up for USDJPY) and averaged over the events of the release (NFP + Unemployment Rate + Average
  Hourly Earnings land in the same minute). The free ForexFactory feed never carries the actual number, so this is all
  the calendar says before the release. Weighed lightly by the connector: the consensus is already in the price.
- ai: the newest AI bias for the pair (server/ai.py PairBias), when one is fresh.

Pure functions: no database, no settings. The engine passes the release's calendar rows and the newest AIAnalysis row.
"""
import math
import re
from datetime import datetime, timedelta, timezone

# How big an expected change reads, by the kind of value (expect = tanh(change / its step), signed by polarity):
EXPECT_PP = 0.25       # a % (m/m, y/y, a rate, an unemployment rate): in percentage points, +0.1 pp -> 0.38, a 25 bp
                       # rate move -> 0.76. Relative to |previous| a 0.0 -> 0.1 % GDP read as +1 and a rate cut as noise
EXPECT_POINTS = 2.0    # a plain number (PMI, sentiment indices): in points, 50.0 -> 50.5 reads 0.24, +2 points 0.76
EXPECT_GAIN = 3.0      # a count (K/M/B/T: payrolls, claims) or a balance: tanh(EXPECT_GAIN x the change relative to
                       # |previous|), +10 % -> 0.29, +33 % -> 0.76
AI_MAX_AGE_H = 2.0     # an AI analysis older than this says nothing about a release now
_SCALE = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
# "<1.25%", "> 50.0", "-12.5K", "+0.3%", "1,234.5": an optional bound sign, a number, an optional K/M/B/T or %
_VALUE = re.compile(r"^[<>≤≥]?\s*([+-]?)\s*(\d+(?:,\d{3})*(?:\.\d+)?|\.\d+)\s*([KMBT%]?)$", re.I)
# ForexFactory titles where a higher number is bad for the currency (more people out of work, more job cuts)
_INVERTED = re.compile(r"unemployment|jobless|claimant count|job cuts|continuing claims|initial claims", re.I)
# balances: positive is a surplus, negative a deficit; higher (a bigger surplus, a smaller deficit) is good
_BALANCE = re.compile(r"trade balance|current account|budget balance|goods balance", re.I)


def _field(e, key):
    """A field of a calendar event, an analysis or a bias, from a dict or an object (a CalendarEvent row, a PairBias);
    None when it has none. Text, numbers and sequences have no fields (str.title is a method, not a title)."""
    if isinstance(e, dict):
        return e.get(key)
    if e is None or isinstance(e, (str, bytes, int, float, list, tuple)):
        return None
    return getattr(e, key, None)


def parse_value(text) -> tuple[float, str] | None:
    """A ForexFactory number as (value, unit): "0.7%" -> (0.7, "%"), "-12.5K" -> (-12500.0, "#"), "<1.25%" ->
    (1.25, "%") (a bound is read as its number), "1.1T" -> (1.1e12, "#"), "50.5" -> (50.5, ""). Units: "%" a
    percentage, "#" a count (K/M/B/T scale the number, so "950K" and "1.2M" are the same unit), "" a plain number (an
    index such as a PMI; an int or float is one too). None for an empty, "n/a" or otherwise unreadable value."""
    if isinstance(text, bool):
        return None
    if isinstance(text, (int, float)):
        try:
            value = float(text)
        except OverflowError:                                # an int of 400 digits (json.loads makes one): unreadable
            return None
        return (value, "") if math.isfinite(value) else None
    if not isinstance(text, str):
        return None
    m = _VALUE.match(text.strip().replace("−", "-"))       # a typographic minus is a minus
    if not m:
        return None
    sign, digits, suffix = m.groups()
    suffix = suffix.upper()
    value = float(digits.replace(",", "")) * (-1 if sign == "-" else 1)
    if suffix == "%":
        unit = "%"
    else:
        value, unit = value * _SCALE[suffix], "#" if suffix else ""
    return (value, unit) if math.isfinite(value) else None    # 400 digits read as inf: unreadable


def polarity(title: str | None) -> int:
    """How a higher number moves the event's currency, by its ForexFactory title: -1 inverted (Unemployment Rate,
    Unemployment Claims, Claimant Count Change and close variants: more unemployment is bad), 0 a balance read by its
    sign (Trade Balance, Current Account: a surplus or a smaller deficit is good), +1 everything else (CPI, GDP,
    PMI, retail sales, rates, Non-Farm Employment Change...)."""
    t = title if isinstance(title, str) else ""
    if _INVERTED.search(t):
        return -1
    if _BALANCE.search(t):
        return 0
    return 1


def expect_score(title: str | None, forecast, previous) -> float | None:
    """What the forecast expects for the event's currency, in [-1, 1] (+ = good for it): the forecast's change from
    the previous value, over a step set by the kind of value, squashed (tanh) and signed by polarity():
    - a % (CPI m/m and y/y, GDP q/q, rates, unemployment rates): the change in percentage points over EXPECT_PP, so the
      same 0.1 pp reads the same up or down and near 0 % (0.0 -> 0.1 % GDP) as at 4 % (a 25 bp cut is -0.76);
    - a count (K/M/B/T) or a balance (polarity 0, any unit but %): the change relative to |previous| (to |forecast|
      when previous is 0) x EXPECT_GAIN, so a deficit that shrinks (-50B -> -45B) is good;
    - a plain number (PMI, sentiment indices): the change in points over EXPECT_POINTS.
    None unless both values parse with the same unit (% with %, a count with a count, a number with a number)."""
    f, p = parse_value(forecast), parse_value(previous)
    if f is None or p is None or f[1] != p[1]:
        return None
    (fv, unit), (pv, _) = f, p
    pol = polarity(title)
    if unit == "%":
        x = (fv - pv) / EXPECT_PP
    elif unit == "#" or pol == 0:
        base = abs(pv) or abs(fv)
        if base == 0:
            return 0.0                                     # 0 expected after 0: no change
        x = EXPECT_GAIN * (fv - pv) / base
    else:
        x = (fv - pv) / EXPECT_POINTS
    if pol < 0:
        x = -x
    return round(math.tanh(x), 3) + 0.0                    # + 0.0: an inverted "no change" is 0.0, never a -0.0


def pair_side(pair: str | None, currency: str | None) -> int:
    """+1 when `currency` is the pair's base (EUR in EURUSD, GBP in GBPJPY, XAU in XAUUSD), -1 when it is the quote
    (USD in EURUSD and XAUUSD, JPY in GBPJPY), 0 otherwise. A broker suffix or a slash in the pair is ignored."""
    p, c = str(pair or "").upper().replace("/", ""), str(currency or "").upper().strip()
    if len(p) < 6 or len(c) != 3:
        return 0
    if c == p[:3]:
        return 1
    if c == p[3:6]:
        return -1
    return 0


def _naive_utc(t) -> datetime | None:
    """A datetime (naive = UTC, as the database keeps them) or an ISO text ("...Z" too) as a naive UTC datetime."""
    if isinstance(t, str):
        try:
            t = datetime.fromisoformat(t.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(t, datetime):
        return None
    return t.astimezone(timezone.utc).replace(tzinfo=None) if t.tzinfo else t


def ai_part(analysis, pair: str, now: datetime | None, max_age_h: float = AI_MAX_AGE_H) -> float | None:
    """The newest AI bias for `pair` in [-1, 1]: +confidence/100 for "buy", -confidence/100 for "sell", 0 for
    "neutral". `analysis`: an AIAnalysis row (created_at, result = MarketAnalysis.model_dump()), or a dict with
    "created_at" (a datetime or ISO text) and either "result" (a dump or a MarketAnalysis) or "biases" (as
    /api/market/analysis returns it). A bare MarketAnalysis has no time, so it gives None.
    None when there is no analysis, its time is unknown, it is not younger than max_age_h hours (plan: "less than 2 h
    old"; exactly 2 h is too old) or than the bias's own horizon_hours (0, none or junk = no horizon of its own; the
    LLM's number is not bounded, so any size is compared, never turned into a timedelta), or it has no bias for the
    pair. now: naive UTC (None = the clock)."""
    if analysis is None:
        return None
    result = _field(analysis, "result")
    biases = _field(analysis if result is None else result, "biases")
    created = _naive_utc(_field(analysis, "created_at"))
    now = _naive_utc(now) or datetime.now(timezone.utc).replace(tzinfo=None)
    if created is None or not isinstance(biases, (list, tuple)):
        return None
    age_h = (now - created) / timedelta(hours=1)           # in hours: a horizon of 10**12 h would overflow a timedelta
    if age_h >= max_age_h:                                 # used while LESS than max_age_h hours old
        return None
    want = str(pair or "").upper().replace("/", "")
    for b in biases:
        if str(_field(b, "symbol") or "").upper().replace("/", "") != want:
            continue
        horizon = _field(b, "horizon_hours")
        if isinstance(horizon, (int, float)) and not isinstance(horizon, bool) and horizon > 0 and age_h >= horizon:
            return None                                    # the bias has run its course (same rule as max_age_h)
        sign = {"buy": 1, "sell": -1, "neutral": 0}.get(str(_field(b, "direction") or "").lower())
        conf = _field(b, "confidence")
        if sign is None or isinstance(conf, bool) or not isinstance(conf, (int, float)):
            return None
        try:
            conf = float(conf)
        except OverflowError:                              # an int of 400 digits (json.loads makes one): junk
            return None
        if not math.isfinite(conf):
            return None
        return round(sign * max(0.0, min(100.0, conf)) / 100, 3) + 0.0
    return None


def context_for(events, pair: str, analysis=None, now: datetime | None = None) -> dict:
    """The run's `context` for the connector: {"expect", "ai", "title"}.
    `events`: the release's calendar rows, every event that lands on the pair in that minute (burst.candidates
    collapses them into one run: NFP + Unemployment Rate + Average Hourly Earnings, CPI m/m + Core CPI m/m, a GBP and
    a JPY release on GBPJPY). CalendarEvent rows or dicts with title, currency, forecast, previous (a missing field
    counts as empty), looked up by the candidate's event ids: the candidate itself carries no forecast or previous. A
    single row (a dict or a row object) is read as a list of one.
    - expect: the average, over the events that can be read, of expect_score() turned into the pair's direction
      (x pair_side: + = the pair should rise), so an inverted Unemployment Rate and a GBP / JPY pair's two sides are
      each read the right way. An event whose values do not parse, or whose currency is not in the pair, is left out;
      None when none is left.
    - ai: ai_part() (None without a fresh bias).
    - title: the events' titles, each once, joined by " + " (as the candidate's)."""
    if events is None:
        rows = []
    elif isinstance(events, (dict, str, bytes)) or not hasattr(events, "__iter__"):
        rows = [events]
    else:
        rows = list(events)
    titles, seen = [], []
    for ev in rows:
        title = str(_field(ev, "title") or "")
        if title and title not in titles:
            titles.append(title)
        e = expect_score(title, _field(ev, "forecast"), _field(ev, "previous"))
        side = pair_side(pair, _field(ev, "currency"))
        if e is not None and side != 0:
            seen.append(e * side)
    return {"expect": round(sum(seen) / len(seen), 3) + 0.0 if seen else None,
            "ai": ai_part(analysis, pair, now),
            "title": " + ".join(titles)}
