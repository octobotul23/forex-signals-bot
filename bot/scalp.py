"""Short-term strategy (scalping): its settings, sizing, daily limit and entry rule. Shared by the server (it checks
what users save) and the connector (it trades them, connector/connector.py Scalper), so both clamp the same way."""
import math
from collections import deque
from datetime import datetime, timedelta, timezone

from .symbols import SymbolSpec

DEFAULTS = {
    "enabled": False,
    "risk_unit": "pct",            # pct = % of balance | usd = money in the account currency
    "risk_min": 0.5,               # risk of the weakest trade it takes ...
    "risk_max": 2.0,               # ... and of the strongest: the confidence picks the point in between
    "hold_seconds": 30,            # X: from this on a scalp in profit is closed; at max_hold_seconds (Y, missing = X)
                                   # it is closed at market whatever it shows (bot/hold.py)
    "loss_unit": "pct",
    "daily_loss": 5.0,             # its own daily loss limit: when hit, only short-term stops until the next day
    "max_open": 2,                 # scalps open at once (one per symbol)
    "max_spread_pips": 1.5,
    "symbols": [],                 # empty = every pair the engine trades
}
UNITS = ("pct", "usd")
# what a money key starts from when its unit changes and no new value came with it (50 $ must not become 50 %)
UNIT_DEFAULTS = {"pct": {"risk_min": 0.5, "risk_max": 2.0, "daily_loss": 5.0},
                 "usd": {"risk_min": 1.0, "risk_max": 10.0, "daily_loss": 50.0}}
MAX_RISK_PCT = 10.0                # a $ risk is capped at this % of balance too
# key -> {unit: (lo, hi)} for the money keys, (lo, hi) for the rest
RANGES = {
    "risk_min": {"pct": (0.1, MAX_RISK_PCT), "usd": (0.1, 100_000.0)},
    "risk_max": {"pct": (0.1, MAX_RISK_PCT), "usd": (0.1, 100_000.0)},
    "daily_loss": {"pct": (0.5, 50.0), "usd": (1.0, 1_000_000.0)},
    "hold_seconds": (10, 300),
    "max_open": (1, 10),
    "max_spread_pips": (0.1, 10.0),
}
INTS = {"hold_seconds", "max_open"}
# max_hold_seconds (Y, since v1.3.0): the latest a scalp is closed, X..MAX_HOLD_TOP. Not in DEFAULTS: a user saved
# under v1.2.0 has none, and a missing Y follows hold_seconds (X) wherever it is used, as v1.2.0 held a trade. A user
# created from v1.3.0 on starts with NEW_USER_MAX_HOLD (server/settings.py DEFAULT_USER_SETTINGS)
MAX_HOLD_TOP = 600
NEW_USER_MAX_HOLD = 120


def _num(v, cast):
    try:
        n = cast(v)
        if not math.isfinite(n):                       # JSON allows NaN and Infinity
            raise ValueError
    except (TypeError, ValueError, OverflowError):    # int(Infinity), junk, an int too large for a float
        raise ValueError("invalid_number")
    return n


def clamp_short(body: dict | None, cur: dict | None = None, symbols: list[str] | None = None) -> dict:
    """`cur` (or the defaults) updated with the keys found in `body`, every value clamped to its range.
    Ranges follow the unit, so the values are clamped again when only the unit changed.
    symbols: the pairs the engine trades (unknown ones are dropped); None keeps the list as it is.
    max_hold_seconds (Y) is in the result only when `body` or `cur` has it, clamped to hold_seconds..MAX_HOLD_TOP
    once hold_seconds is clamped (a Y under X is raised to X); a stored one gone bad is left out (it follows X again),
    and so is one `body` sends as None (null: the user's way back to "Y follows X", as v1.2.0 held a trade).
    Raises ValueError("invalid_enabled" | "invalid_number" | "invalid_unit" | "invalid_symbols")."""
    body = body if isinstance(body, dict) else {}
    out = {**DEFAULTS, **(cur or {})}
    if "enabled" in body:
        if not isinstance(body["enabled"], bool):     # "false" must never switch it on
            raise ValueError("invalid_enabled")
        out["enabled"] = body["enabled"]
    if not isinstance(out["enabled"], bool):           # a stored value that went bad: off
        out["enabled"] = False
    for unit_key, keys in (("risk_unit", ("risk_min", "risk_max")), ("loss_unit", ("daily_loss",))):
        if out.get(unit_key) not in UNITS:
            out[unit_key] = DEFAULTS[unit_key]
        if unit_key in body:
            if body[unit_key] not in UNITS:
                raise ValueError("invalid_unit")
            if body[unit_key] != out[unit_key]:
                out.update({k: UNIT_DEFAULTS[body[unit_key]][k] for k in keys})
            out[unit_key] = body[unit_key]
    for key, rng in RANGES.items():
        cast = int if key in INTS else float
        v = _num(body[key], cast) if key in body else out.get(key, DEFAULTS[key])
        try:
            v = _num(v, cast)
        except ValueError:                             # a stored value that went bad: back to the default
            v = DEFAULTS[key]
        if isinstance(rng, dict):
            rng = rng[out["loss_unit" if key == "daily_loss" else "risk_unit"]]
        out[key] = cast(max(rng[0], min(rng[1], v)))
    if out["risk_min"] > out["risk_max"]:
        out["risk_min"], out["risk_max"] = out["risk_max"], out["risk_min"]
    y = out.get("max_hold_seconds")                    # none stored (a v1.2.0 user): none, it follows X
    if "max_hold_seconds" in body:                     # null: removed, it follows X again
        y = None if body["max_hold_seconds"] is None else _num(body["max_hold_seconds"], int)
    elif y is not None:
        try:
            y = _num(y, int)
        except ValueError:                             # a stored value that went bad: none, it follows X
            y = None
    if "symbols" in body:
        if not isinstance(body["symbols"], list):
            raise ValueError("invalid_symbols")
        out["symbols"] = list(dict.fromkeys(s for s in body["symbols"] if isinstance(s, str)))
    if not isinstance(out.get("symbols"), list):
        out["symbols"] = []
    out["symbols"] = [s for s in out["symbols"] if isinstance(s, str)]
    if symbols is not None:
        out["symbols"] = [s for s in out["symbols"] if s in symbols]
    res = {k: out[k] for k in DEFAULTS}
    if y is not None:                                  # never under X: raised to it, not swapped
        res["max_hold_seconds"] = max(res["hold_seconds"], min(MAX_HOLD_TOP, y))
    return res


def risk_money(st: dict, balance: float, strength: float) -> float:
    """Money one trade may lose at its stop: risk_min for the weakest setup it takes (strength 0), risk_max for
    the strongest (1). A $ amount is capped at MAX_RISK_PCT of balance as well."""
    if balance <= 0:
        return 0.0
    s = max(0.0, min(1.0, float(strength)))
    r = st["risk_min"] + (st["risk_max"] - st["risk_min"]) * s
    money = balance * r / 100 if st["risk_unit"] == "pct" else r
    return min(money, balance * MAX_RISK_PCT / 100)


def lots_for_money(money: float, sl_pips: float, spec: SymbolSpec) -> float:
    """Lots that lose about `money` at a stop `sl_pips` away. Rounds DOWN to the volume step; 0 when even the
    minimum lot would lose more (the trade is skipped, never sized up)."""
    if money <= 0 or sl_pips <= 0 or spec.pip_value_per_lot <= 0:
        return 0.0
    raw = money / (sl_pips * spec.pip_value_per_lot)
    lots = round(math.floor(raw / spec.volume_step + 1e-9) * spec.volume_step, 8)
    return min(lots, spec.volume_max) if lots >= spec.volume_min else 0.0


def daily_limit_money(st: dict, day_start_balance: float) -> float:
    """The day's short-term loss that stops short-term until the next day."""
    if st["loss_unit"] == "pct":
        return max(0.0, day_start_balance) * st["daily_loss"] / 100
    return st["daily_loss"]


def ny_offset(at: float) -> int:
    """New York's UTC offset in hours at epoch `at` (US DST: 2nd Sunday of March to 1st Sunday of November, 02:00),
    worked out here: the connector's PC may have no time-zone database (bot/mt5_broker.py has the same rule)."""
    y = datetime.fromtimestamp(at, timezone.utc).year
    mar, nov = datetime(y, 3, 1, tzinfo=timezone.utc), datetime(y, 11, 1, tzinfo=timezone.utc)
    start = mar + timedelta(days=(6 - mar.weekday()) % 7 + 7, hours=7)
    end = nov + timedelta(days=(6 - nov.weekday()) % 7, hours=6)
    return -4 if start.timestamp() <= at < end.timestamp() else -5


def closes_soon(t: datetime, hold: float) -> bool:
    """No new scalp at naive-UTC `t`: the daily rollover (17:00 New York, wide spreads) or the weekly close would come
    before it is over and closed. Every day from 17:00 New York minus `hold` + 5 min until 18:05, and from Friday's
    until Sunday 18:05 (a scalp the market shuts on carries only its stop-loss across the gap). Christmas and New
    Year's Day too, from 21:00 UTC on their eves minus `hold` + 5 min (the hours bot/mt5_broker.py _fx_closed takes FX
    as shut). `hold`: the longest a scalp may stay open (max_hold_seconds, Y, when set). Kept out of decide(), whose
    hours are those the rule was tested with."""
    md, u = (t.month, t.day), (t - t.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    if md in ((12, 25), (1, 1)) or (md in ((12, 24), (12, 31)) and u >= 21 * 3600 - hold - 300):
        return True
    ny = t + timedelta(hours=ny_offset(t.replace(tzinfo=timezone.utc).timestamp()))
    s = (ny - ny.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    start, end, day = 17 * 3600 - hold - 300, 18 * 3600 + 300, ny.weekday()
    return start <= s < end or (day == 4 and s >= start) or day == 5 or (day == 6 and s < end)


class SecondBars:
    """One symbol's last `keep` seconds as one-second bars, built from quote ticks the way the rule's history was
    built: the last bid and ask inside each second and how many ticks it had. A second with no tick has no bar
    (its quote is the one before it)."""

    def __init__(self, keep: int = 3900):
        self.keep = keep
        self.bars: deque[list] = deque()           # [second (broker clock), bid, ask, ticks], oldest first
        self.last_msc = 0                          # the newest tick taken in

    def add(self, ticks):
        """(time_msc, bid, ask) ticks, oldest first; ones not newer than the last taken in are ignored."""
        for msc, bid, ask in ticks:
            if msc <= self.last_msc:
                continue
            self.last_msc, sec = msc, msc // 1000
            if self.bars and self.bars[-1][0] == sec:
                b = self.bars[-1]
                b[1], b[2], b[3] = bid, ask, b[3] + 1
            else:
                self.bars.append([sec, bid, ask, 1])
        while self.bars and self.bars[0][0] <= self.bars[-1][0] - self.keep:
            self.bars.popleft()

    def upto(self, sec: int) -> list[list]:
        """The bars of seconds up to and including `sec`, oldest first."""
        out = list(self.bars)
        while out and out[-1][0] > sec:
            out.pop()
        return out


# The entry rule: fade a stretch away from the 10-minute average. When the price is more than `z` volatility units
# from its exponential average it trades back toward it, with a stop `ksl` volatility units (of the hold time)
# away and no target. Of every rule tested on one-second bars of the six pairs and gold (Sep 2025 - Oct 2026;
# chosen on the data before June 2026, checked on June - October) none made money after the spread: this one lost
# the least while trading often (out of sample about -0.05 R a trade held 120 s, -0.12 R held 30 s; the same
# trades with a random side lost about as much), and how far the price had stretched (the strength) did not pick
# out the better trades. It is here because the strategy was asked for: keep it on a demo account.
RULE = {
    "tau": 600,            # the average's time constant (s)
    "z": 2.0,              # the stretch that opens a trade, in volatility units of `tau` seconds ...
    "z_full": 4.0,         # ... and the stretch that counts as the strongest (strength 1)
    "vol_window": 1800,    # volatility: the mid's second-to-second moves over this long (s)
    "warmup": 3600,        # history needed with no gap in it (s)
    "gap": 300,            # a silence longer than this (s) is a gap
    "ksl": 2.5,            # stop distance, in volatility units of the hold time
    "min_sl_spreads": 3.0, # ... and at least this many spreads
    "volcost": 3.0,        # the price must move at least this many spreads in `vol_hold` s (else the spread eats it)
    "vol_hold": 120,
    "spread_mult": 1.25,   # the spread may be at most this times its median of the last hour
    "rollover_hours": (23, 0),   # broker hours skipped: the spread jumps around the daily rollover
}
KEEP_SECONDS = RULE["warmup"] + RULE["gap"]       # the bars the rule needs (the connector keeps this many)


def _measure(bars: list[list], sec: int, pip: float) -> tuple[float, float, float] | None:
    """What the rule sees right after second `sec` (decide()): (the stretch, the mid's variance per second, the
    spread now), or None when it does not look at all (no tick in `sec`, the rollover hours, a gap or too little
    history, the spread well over its usual, too quiet to beat the spread)."""
    r = RULE
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
    return (prev_mid - ema) / math.sqrt(per_s * r["tau"]), per_s, spread


def stretch(symbol: str, bars: list[list], sec: int, pip: float) -> float | None:
    """How far the price is from its 10-minute average right after second `sec`, in volatility units: decide()'s
    "stretch", unrounded, below 0 under the average and above it over it (a trade opens from RULE["z"] out). Given
    whether or not a trade would open, so an open scalp can be watched (scalp_against); None when decide() would not
    look at all (no tick in `sec`, the rollover hours, a gap, the spread, too quiet): nothing to judge by."""
    m = _measure(bars, sec, pip)
    return None if m is None else m[0]


def decide(symbol: str, bars: list[list], sec: int, pip: float, hold: int = 30) -> dict | None:
    """The entry rule for `symbol` (no broker suffix), asked once per second right after second `sec` is over,
    with the bars up to it (oldest first): None, or
    {"side": "buy" | "sell", "strength": 0..1 (how sure: picks the risk inside the user's range),
     "sl_pips": the protective stop, "tp_pips": the target or None}.
    `hold`: the longest the trade may be held, in seconds (its stop is sized to them): max_hold_seconds (Y) when
    set, else hold_seconds."""
    m = _measure(bars, sec, pip)
    if m is None:
        return None
    r, (dev, per_s, spread) = RULE, m
    if abs(dev) < r["z"]:
        return None
    sl = max(r["ksl"] * math.sqrt(per_s * max(1, int(hold))), r["min_sl_spreads"] * spread)
    return {"side": "buy" if dev < 0 else "sell",
            "strength": min(1.0, (abs(dev) - r["z"]) / (r["z_full"] - r["z"])),
            "sl_pips": math.ceil(sl / pip * 10 - 1e-9) / 10, "tp_pips": None, "stretch": round(dev, 2)}


# An open scalp has turned against itself once the price stretched this many volatility units further from its
# average than when the trade opened (not part of the tested rule: the hold-until-profit exit, bot/hold.py)
AGAINST_WIDEN = 1.0


def _dev(v) -> float | None:
    """`v` as a stretch: a finite number, else None (missing, a bool, NaN, junk)."""
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def scalp_against(side: str, entry_stretch: float | None, now_stretch: float | None, now_side: str | None) -> bool:
    """The Scalper's "against" for an open scalp (bot/hold.py hold_action): the rule now opens a trade the other way
    (`now_side`: decide()'s side this second, None for no trade), or the price has stretched AGAINST_WIDEN units
    further from its average than when the trade opened (`entry_stretch`: decide()'s "stretch" then; `now_stretch`:
    stretch() now). A buy opened at -2.4 is against from -3.4 down, a sell opened at +2.4 from +3.4 up.
    False when either stretch is unknown (None: the rule's filters would not look now, or it was not kept), or `side`
    is neither "buy" nor "sell": nothing is decided on that."""
    entry, now = _dev(entry_stretch), _dev(now_stretch)
    if entry is None or now is None or side not in ("buy", "sell"):
        return False
    if now_side == ("sell" if side == "buy" else "buy"):
        return True
    further = entry - now if side == "buy" else now - entry
    return further >= AGAINST_WIDEN - 1e-9         # a whole unit, whatever the float rounding of the two stretches
