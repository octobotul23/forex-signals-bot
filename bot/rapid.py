"""Rapid (v1.3.0): the short-term variant that places many stop-limit orders at once, at a high-impact release (a news
run the server plans: a Burst row of kind "rapid") or on a short-term signal (a local run, demo accounts only). Its
settings, how a run is sized, cut to fit and priced. Shared by the server (it checks what users save) and the
connector (it places the runs), so both clamp the same way. Pure: no broker, no clock, no files.

A run's confidence c (0..1: confidence_from_score at news, confidence_from_stretch on a signal) picks the money each
order may lose between risk_min and risk_max, and the take-profit in money between tp_min_usd and tp_max_usd, the same
for every order of the run. Its orders are stop-limits: the trigger offset_pips beyond the price, the entry at a limit
a pullback behind it (stop_limit_prices); the stop and the take-profit are measured from that limit."""
import math

from .risk import money_for, money_stop, stop_floor_pips
from .scalp import UNITS, lots_for_money
from .symbols import SymbolSpec

RUN_KIND = "rapid"                 # a news run's Burst.kind, a local run's st["kind"], its trades' strategy
DEFAULTS = {
    "enabled": False,              # excludes short_term.enabled: the server turns the other one off
    "orders": 10,                  # orders per side of a run (netting: one order per side carrying them all)
    "sl_unit": "pips",             # pips = the stop is sl_pips, the lots come from the risk money | money = the lot is
                                   # `lots` scaled by the confidence, the stop as far as the risk money allows
    "risk_unit": "pct",            # pct = % of balance | usd = money in the account currency
    "risk_min": 0.2,               # money EACH order may lose at its stop, at confidence 0 ...
    "risk_max": 1.0,               # ... and at confidence 1
    "sl_pips": 8.0,                # sl_unit pips: the stop (× the symbol's scale)
    "lots": 0.10,                  # sl_unit money: each order's lot at confidence 1 (risk_max)
    "tp_min_usd": 10.0,            # each order's take-profit in money, at confidence 0 ...
    "tp_max_usd": 15.0,            # ... and at confidence 1
    "limit_pips": 0.0,             # the entry's pullback behind the trigger (× scale); 0 = the broker's stops level
    "offset_pips": 2.0,            # the trigger's distance from the price (× scale)
    "expire_seconds": 30,          # an order not filled by then is cancelled
    "hold_seconds": 30,            # X: from this on a fill in profit is closed; at max_hold_seconds (Y, missing = X)
                                   # it is closed whatever it shows (bot/hold.py)
    "triggers": {"signals": True, "news": True},    # what starts a run: a short-term signal, a high-impact release
    "analysis": True,              # news runs: the pre-news analysis picks the sides (off: computed and logged only)
    "loss_unit": "pct",
    "daily_loss": 5.0,             # its own daily loss limit (rapid_day.json): when hit, only Rapid stops for the day
    "max_runs": 2,                 # news runs per release, over the release's pairs in their list order
    "cooldown_seconds": 120,       # a symbol's pause after a signal run on it ends
    "symbols": [],                 # empty = no restriction
}
SL_UNITS = ("pips", "money")
TRIGGERS = ("signals", "news")
# what a money key starts from when its unit changes and no new value came with it (50 $ must not become 50 %)
UNIT_DEFAULTS = {"pct": {"risk_min": 0.2, "risk_max": 1.0, "daily_loss": 5.0},
                 "usd": {"risk_min": 10.0, "risk_max": 50.0, "daily_loss": 100.0}}
# key -> {unit: (lo, hi)} for the money keys, (lo, hi) for the rest
LIMITS = {
    "orders": (1, 40),
    "risk_min": {"pct": (0.05, 5.0), "usd": (1.0, 10_000.0)},
    "risk_max": {"pct": (0.05, 5.0), "usd": (1.0, 10_000.0)},
    "sl_pips": (1.0, 200.0),
    "lots": (0.01, 100.0),
    "tp_min_usd": (1.0, 10_000.0),
    "tp_max_usd": (1.0, 10_000.0),
    "limit_pips": (0.0, 5.0),
    "offset_pips": (0.5, 50.0),
    "expire_seconds": (5, 600),
    "hold_seconds": (5, 600),
    "daily_loss": {"pct": (0.5, 50.0), "usd": (1.0, 1_000_000.0)},
    "max_runs": (1, 5),
    "cooldown_seconds": (0, 3600),
}
INTS = {"orders", "expire_seconds", "hold_seconds", "max_runs", "cooldown_seconds"}
# max_hold_seconds (Y): the latest a fill is closed, X..MAX_HOLD_TOP. Not in DEFAULTS, as in bot/scalp.py: a block
# without it follows hold_seconds (X) wherever it is used; a user's block starts with NEW_USER_MAX_HOLD (the server's
# DEFAULT_USER_SETTINGS)
MAX_HOLD_TOP = 1800
NEW_USER_MAX_HOLD = 120
SL_CAP_PIPS = LIMITS["sl_pips"][1]  # sl_unit money: the stop is never further than this (× scale); the run risks less
# one side of a run may take at most this share of the free margin left by the other open runs (the Scalper's
# MARGIN_USE, which lives in connector/connector.py)
RAPID_MARGIN_USE = 0.5
TP_MAX_SL = 3.0                    # the take-profit is at most this many stops away
COMMENT_MAX = 20                   # a local run's order comment: fxr:<13-digit ms id>:<i under 100>


def _num(v, cast):
    if isinstance(v, bool):                            # true is no number: it must not become 1 % or 1 order
        raise ValueError("invalid_number")
    try:
        n = cast(v)
        if not math.isfinite(n):                       # JSON allows NaN and Infinity
            raise ValueError
    except (TypeError, ValueError, OverflowError):    # int(Infinity), junk, an int too large for a float
        raise ValueError("invalid_number")
    return n


def clamp_rapid(body: dict | None, cur: dict | None = None, symbols: list[str] | None = None) -> dict:
    """`cur` (or the defaults) updated with the keys found in `body`, every value clamped to its range; the whole
    block comes back (DEFAULTS filled in), unknown keys dropped. As bot/scalp.py clamp_short:
    - the money ranges follow their unit; a unit changed without a new value resets that unit's keys to its
      UNIT_DEFAULTS (risk_min and risk_max for risk_unit, daily_loss for loss_unit);
    - risk_max under risk_min is raised to it, and tp_max_usd under tp_min_usd too (never swapped);
    - max_hold_seconds (Y) is in the result only when `body` or `cur` has it, clamped to hold_seconds..MAX_HOLD_TOP
      once hold_seconds is clamped (a Y under X is raised to X); a stored one gone bad is left out (it follows X),
      and so is one `body` sends as None (null: the user's way back to "Y follows X");
    - triggers: a dict of "signals" / "news" bools; the keys `body` leaves out keep their value, others are dropped;
    - symbols: the pairs the engine trades (unknown ones are dropped); None keeps the list as it is.
    A value stored in `cur` that went bad gets its default. Raises ValueError("invalid_number" | "invalid_unit" |
    "invalid_symbols" | "invalid_triggers" | "invalid_enabled" | "invalid_analysis") for a value in `body` that cannot
    be used (the switches as clamp_short's and the burst's: "false" must never switch one on)."""
    body = body if isinstance(body, dict) else {}
    out = {**DEFAULTS, **(cur if isinstance(cur, dict) else {})}
    for key, code in (("enabled", "invalid_enabled"), ("analysis", "invalid_analysis")):
        if key in body:
            if not isinstance(body[key], bool):
                raise ValueError(code)
            out[key] = body[key]
        if not isinstance(out[key], bool):             # a stored value that went bad: the default
            out[key] = DEFAULTS[key]
    if out.get("sl_unit") not in SL_UNITS:
        out["sl_unit"] = DEFAULTS["sl_unit"]
    if "sl_unit" in body:
        if body["sl_unit"] not in SL_UNITS:
            raise ValueError("invalid_unit")
        out["sl_unit"] = body["sl_unit"]
    for unit_key, keys in (("risk_unit", ("risk_min", "risk_max")), ("loss_unit", ("daily_loss",))):
        if out.get(unit_key) not in UNITS:
            out[unit_key] = DEFAULTS[unit_key]
        if unit_key in body:
            if body[unit_key] not in UNITS:
                raise ValueError("invalid_unit")
            if body[unit_key] != out[unit_key]:
                out.update({k: UNIT_DEFAULTS[body[unit_key]][k] for k in keys})
            out[unit_key] = body[unit_key]
    for key, rng in LIMITS.items():
        cast = int if key in INTS else float
        v = _num(body[key], cast) if key in body else out.get(key, DEFAULTS[key])
        try:
            v = _num(v, cast)
        except ValueError:                             # a stored value that went bad: back to the default
            v = DEFAULTS[key]
            if isinstance(rng, dict):                  # ... of its unit
                v = UNIT_DEFAULTS[out["loss_unit" if key == "daily_loss" else "risk_unit"]][key]
        if isinstance(rng, dict):
            rng = rng[out["loss_unit" if key == "daily_loss" else "risk_unit"]]
        out[key] = cast(max(rng[0], min(rng[1], v)))
    out["risk_max"] = max(out["risk_min"], out["risk_max"])
    out["tp_max_usd"] = max(out["tp_min_usd"], out["tp_max_usd"])
    y = out.get("max_hold_seconds")                    # none stored: none, it follows X
    if "max_hold_seconds" in body:                     # null: removed, it follows X again
        y = None if body["max_hold_seconds"] is None else _num(body["max_hold_seconds"], int)
    elif y is not None:
        try:
            y = _num(y, int)
        except ValueError:                             # a stored value that went bad: none, it follows X
            y = None
    stored = out.get("triggers") if isinstance(out.get("triggers"), dict) else {}
    trig = {k: stored[k] if isinstance(stored.get(k), bool) else DEFAULTS["triggers"][k] for k in TRIGGERS}
    if "triggers" in body:
        if not isinstance(body["triggers"], dict):
            raise ValueError("invalid_triggers")
        for k in TRIGGERS:
            if k in body["triggers"]:
                if not isinstance(body["triggers"][k], bool):
                    raise ValueError("invalid_triggers")
                trig[k] = body["triggers"][k]
    out["triggers"] = trig
    if "symbols" in body:
        if not isinstance(body["symbols"], list):
            raise ValueError("invalid_symbols")
        out["symbols"] = list(dict.fromkeys(s for s in body["symbols"] if isinstance(s, str)))
    if not isinstance(out.get("symbols"), list):
        out["symbols"] = []
    out["symbols"] = list(dict.fromkeys(s for s in out["symbols"] if isinstance(s, str)))
    if symbols is not None:
        out["symbols"] = [s for s in out["symbols"] if s in symbols]
    res = {k: out[k] for k in DEFAULTS}
    if y is not None:                                  # never under X: raised to it, not swapped
        res["max_hold_seconds"] = max(res["hold_seconds"], min(MAX_HOLD_TOP, y))
    return res


# ── confidence, risk and take-profit ───────────────────────────
def _finite(v) -> float | None:
    """`v` as a float, or None when it is no finite number (missing, a bool, NaN, infinity, junk)."""
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def _unit(c) -> float:
    """`c` held to 0..1; 0 when it is no finite number."""
    f = _finite(c)
    return 0.0 if f is None else max(0.0, min(1.0, f))


def confidence_from_score(score) -> float:
    """A news run's confidence: how sure the pre-news analysis was (bot/news_view.py analyse's score, -1..1), either
    way: |score| held to 0..1. 0 when the analysis did not run (None) or gave no number."""
    f = _finite(score)
    return 0.0 if f is None else min(1.0, abs(f))


def confidence_from_stretch(stretch, z) -> float:
    """A signal run's confidence: how far past the Scalper's threshold `z` the price stretched (bot/scalp.py
    stretch), in units of z: (|stretch| - z) / z held to 0..1 (0 at the threshold, 1 at twice it). 0 when either is
    unknown or z is not above 0."""
    s, t = _finite(stretch), _finite(z)
    if s is None or t is None or t <= 0:
        return 0.0
    return max(0.0, min(1.0, (abs(s) - t) / t))


def risk_at(cfg: dict, c) -> float:
    """The risk setting at confidence `c` (0..1): risk_min + c × (risk_max - risk_min), in risk_unit (% or $), per
    order."""
    return cfg["risk_min"] + _unit(c) * (cfg["risk_max"] - cfg["risk_min"])


def tp_usd_at(cfg: dict, c) -> float:
    """Each order's take-profit in money at confidence `c` (0..1): tp_min_usd + c × (tp_max_usd - tp_min_usd)."""
    return cfg["tp_min_usd"] + _unit(c) * (cfg["tp_max_usd"] - cfg["tp_min_usd"])


def _ppp(spec: SymbolSpec) -> float:
    """Points in a pip (10 on 5-digit FX, 3-digit JPY and 2-digit gold); 0 for a spec without pip or point."""
    if not (spec.pip > 0 and spec.point > 0):
        return 0.0
    ppp = spec.pip / spec.point
    return round(ppp) if abs(ppp - round(ppp)) < 1e-6 else ppp    # whole: pts / ppp is the decimal itself


def size_order(cfg: dict, c, balance, spec: SymbolSpec, spread_pips) -> dict:
    """One order of a run at confidence `c` on `spec`'s symbol, with the spread quoted now (pips): {"lots", "sl_pips",
    "tp_pips", "tp_usd", "money", "tp_clamped"}, or {"skip": the reason}.
    money: what the order may lose at its stop, money_for(risk_unit, risk_at(c), balance) (capped at MAX_RISK_PCT of
    the balance). The stop and the take-profit are whole points, measured from the order's limit price, never closer
    than the floor: the broker's stops and freeze levels, three spreads (stop_floor_pips) and the freeze level plus
    the spread (a stop that close could be neither placed nor changed).
    - sl_unit pips: the stop is sl_pips × the symbol's scale (rounded UP to the point; under the floor, the floor), the
      lots what loses the money there (bot/scalp.py lots_for_money: DOWN to the volume step, at most volume_max).
    - sl_unit money: the lots are `lots` × risk_at(c) / risk_max, DOWN to the step; the stop as far as the money
      allows (bot/risk.py money_stop: at least the floor, the lots cut to fit there; at most SL_CAP_PIPS × scale, the
      order then risks less), the lots then at most volume_max.
    Skipped when the lots come under volume_min. The take-profit is tp_usd_at(c) in pips on those lots, rounded to the
    point and held to [floor, TP_MAX_SL × the stop] (tp_clamped: it was moved)."""
    sym, k = spec.symbol, spec.scale if spec.scale > 0 else 1.0
    pv, ppp, step = spec.pip_value_per_lot, _ppp(spec), spec.volume_step
    if not (pv > 0 and ppp > 0 and step > 0):
        return {"skip": f"no contract data for {sym} (pip, point, pip value or lot step)"}
    risk = risk_at(cfg, c)
    money = money_for(cfg["risk_unit"], risk, balance)
    if not money:
        return {"skip": "no money to risk: the balance is not above 0"}
    spread = max(0.0, _finite(spread_pips) or 0.0)
    floor = max(stop_floor_pips(spec, spread, None), max(0.0, spec.freeze_distance) / spec.pip + spread)
    lo = max(1, math.ceil(floor * ppp - 1e-9))         # the floor in points, rounded UP
    if cfg["sl_unit"] == "money":
        want = cfg["lots"] * risk / cfg["risk_max"] if cfg["risk_max"] > 0 else 0.0
        want = round(math.floor(want / step + 1e-9) * step, 8)
        if want < spec.volume_min:
            return {"skip": f"{want} lots per order is under the minimum lot size of {sym} ({spec.volume_min})"}
        sl, lots = money_stop(money, want, 1, spec, floor, SL_CAP_PIPS * k)
        if not lots:
            return {"skip": f"{money:.2f} at risk is too little for {spec.volume_min} lots with the closest stop "
                            f"allowed on {sym} ({floor:.1f} pips)"}
        lots = min(lots, spec.volume_max)              # one order is one ticket (netting splits the side instead)
        sl_pts = max(lo, round(sl * ppp))
    else:
        sl_pts = max(lo, math.ceil(cfg["sl_pips"] * k * ppp - 1e-6))
        lots = lots_for_money(money, sl_pts / ppp, spec)
        if not lots:
            return {"skip": f"{money:.2f} at risk is too little for {spec.volume_min} lots with a "
                            f"{sl_pts / ppp:g} pip stop on {sym}"}
    tp_usd = tp_usd_at(cfg, c)
    tp_pts = round(min(tp_usd / (lots * pv) * ppp, 1e12))
    hi, clamped = math.floor(TP_MAX_SL * sl_pts + 1e-9), False
    if tp_pts < lo:
        tp_pts, clamped = lo, True
    elif tp_pts > hi:
        tp_pts, clamped = hi, True
    return {"lots": lots, "sl_pips": sl_pts / ppp, "tp_pips": tp_pts / ppp, "tp_usd": tp_usd, "money": money,
            "tp_clamped": clamped}


# ── how many orders fit ────────────────────────────────────────
def fit_orders(n, money, free_room, pending_room, margin_per_order, free_margin) -> tuple[int, str | None]:
    """How many of a run's `n` orders per side may be placed, each risking `money`: (count, None) when all fit, else
    (count, what cut it: "daily limit" | "pending-order limit" | "free margin"; the tightest, the first of those on a
    tie); count 0 = the run is skipped.
    - free_room: what is still free of Rapid's daily loss limit (its limit - today's loss - the open loss - what the
      other open runs reserved): count × money must fit in it. None = not known: no cap.
    - pending_room: the orders the account may still place (its limit_orders - the orders open now), counted as `n`
      counts them (the caller turns tickets into orders: hedging places one per order and side, netting one per side
      and volume_max chunk). None = not known: no cap.
    - margin_per_order: the margin one order of one side needs; free_margin: the free margin minus what the other open
      runs reserved. count × margin_per_order must fit in RAPID_MARGIN_USE × free_margin. A margin not known (None, 0,
      junk) is no cap: the broker refuses an order it cannot cover anyway.
    (0, "no orders") for n under 1, (0, "no money to risk") for a money that is no number above 0."""
    try:
        count = int(n) if not isinstance(n, bool) else 0
    except (TypeError, ValueError, OverflowError):
        count = 0
    if count < 1:
        return 0, "no orders"
    m = _finite(money)
    if m is None or m <= 0:
        return 0, "no money to risk"
    reason = None
    fits = []
    if free_room is not None:
        fits.append((math.floor(max(0.0, _finite(free_room) or 0.0) / m + 1e-9), "daily limit"))
    if pending_room is not None:
        p = _finite(pending_room)
        fits.append((max(0, math.floor(p + 1e-9)) if p is not None else 0, "pending-order limit"))
    need = _finite(margin_per_order)
    if need is not None and need > 0:
        cap = RAPID_MARGIN_USE * max(0.0, _finite(free_margin) or 0.0)
        fits.append((math.floor(cap / need + 1e-9), "free margin"))
    for fit, why in fits:
        if fit < count:
            count, reason = max(0, fit), why
    return count, reason


# ── stop-limit prices, volumes, comments ───────────────────────
def stop_limit_prices(side: str, bid, ask, spec: SymbolSpec, offset_pips, limit_pips) -> tuple[float, float] | str:
    """A stop-limit order's (trigger, limit) prices on `side`, rounded to the symbol's digits, or the reason it cannot
    be placed (a str). Both distances are × the symbol's scale.
    - buy: the trigger offset_pips over the ask, the limit under it; sell: the trigger offset_pips under the bid, the
      limit over it. The limit sits limit_pips behind the trigger on the pullback side, and never closer than the
      broker's stops level (nor than one point: the broker refuses a limit at the trigger); limit_pips 0 = the stops
      level.
    - The trigger is rounded away from the market and the limit away from the trigger, so neither distance shrinks.
    The reason when the trigger would sit within the freeze level plus the spread from the price (an order that close
    could not be cancelled), or within the stops level (the broker refuses it); and for no price or an unknown side."""
    if side not in ("buy", "sell"):
        return f"unknown side {side!r}"
    b, a, ppp, point = _finite(bid), _finite(ask), _ppp(spec), spec.point
    if b is None or a is None or b <= 0 or a < b or not ppp:
        return f"no price for {spec.symbol}"
    k = spec.scale if spec.scale > 0 else 1.0
    off, lim = max(0.0, _finite(offset_pips) or 0.0), max(0.0, _finite(limit_pips) or 0.0)
    bid_pt, ask_pt = round(b / point), round(a / point)                   # quotes sit on the point grid
    off_pt = math.ceil(off * k * ppp - 1e-6)
    stops_pt = max(0, math.ceil(spec.min_stop_distance / point - 1e-6))
    need = max(max(0, math.ceil(spec.freeze_distance / point - 1e-6)) + ask_pt - bid_pt, stops_pt, 1)
    if off_pt < need:
        return (f"the trigger {off_pt / ppp:g} pips from the price is within the freeze level and spread of "
                f"{spec.symbol} ({need / ppp:g} pips): an order that close could not be cancelled")
    gap = max(math.ceil(lim * k * ppp - 1e-6), stops_pt, 1)
    if side == "buy":
        trigger = ask_pt + off_pt
        limit = trigger - gap
    else:
        trigger = bid_pt - off_pt
        limit = trigger + gap
    if trigger <= 0 or limit <= 0:
        return f"no price for {spec.symbol}"
    return round(trigger * point, spec.digits), round(limit * point, spec.digits)


def split_volume(total_lots, spec: SymbolSpec) -> list[float]:
    """A netting run's side as orders: one of total_lots (DOWN to the volume step), split evenly into as few orders of
    at most volume_max as it takes, each a whole number of steps. [] when that is under volume_min (or no number)."""
    t, step = _finite(total_lots), spec.volume_step
    if t is None or t <= 0 or not step > 0:
        return []
    steps = math.floor(t / step + 1e-9)
    if steps < 1 or steps * step < spec.volume_min - 1e-9:
        return []
    vmax = spec.volume_max
    per = max(1, math.floor(vmax / step + 1e-9)) if math.isfinite(vmax) and vmax > 0 else steps
    parts = -(-steps // per)
    return [round((steps // parts + (i < steps % parts)) * step, 8) for i in range(parts)]


def local_comment(run_id: int, i: int) -> str:
    """The comment on order `i` of local run `run_id` (its start in ms): "fxr:<id>:<i>", which connector.py run_tag
    reads back as ("rapid", id). Raises ValueError for one over COMMENT_MAX chars (MT5 keeps 31, brokers may cut
    shorter) or a negative id or index."""
    if isinstance(run_id, bool) or isinstance(i, bool) or int(run_id) < 0 or int(i) < 0:
        raise ValueError("invalid_comment")
    out = f"fxr:{int(run_id)}:{int(i)}"
    if len(out) > COMMENT_MAX:
        raise ValueError("comment_too_long")
    return out
