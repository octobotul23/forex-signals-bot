"""Position sizing and trading guards. Shared by the backtester and the live trader."""
import math
from dataclasses import replace
from datetime import date

from .scalp import MAX_RISK_PCT, UNITS, lots_for_money
from .symbols import SymbolSpec


class RiskManager:
    def __init__(self, risk_cfg: dict):
        self.risk_pct = float(risk_cfg.get("risk_per_trade_pct", 1.0))
        self.max_open = int(risk_cfg.get("max_open_trades", 3))
        self.daily_loss_pct = float(risk_cfg.get("daily_loss_limit_pct", 0) or 0)
        self.max_spread_pips = float(risk_cfg.get("max_spread_pips", 0) or 0)
        self.day: date | None = None
        self.day_start_equity: float | None = None
        self.halted = False

    # ── daily loss limit ───────────────────────────────────────
    def update_day(self, day: date, equity: float) -> bool:
        """Call before each decision. Returns True when a new trading day started."""
        if day != self.day:
            self.day, self.day_start_equity, self.halted = day, equity, False
            return True
        return False

    def check_daily_loss(self, equity: float) -> bool:
        """Returns True only at the moment the limit is first breached today."""
        if self.halted or not self.daily_loss_pct or not self.day_start_equity:
            return False
        if equity <= self.day_start_equity * (1 - self.daily_loss_pct / 100):
            self.halted = True
            return True
        return False

    def day_pnl(self, equity: float) -> float:
        return equity - (self.day_start_equity or equity)

    # ── entry guards ───────────────────────────────────────────
    def can_open(self, open_total: int, spread_pips: float, scale: float = 1.0) -> tuple[bool, str]:
        """Checked only when flat on the symbol (the bot holds at most one trade per symbol). `scale`: the symbol's
        (SymbolSpec.scale): the spread cap is in a currency pair's pips, so gold's is 5 × it."""
        if self.halted:
            return False, f"daily loss limit ({self.daily_loss_pct}%) reached"
        if open_total >= self.max_open:
            return False, f"max open trades ({self.max_open})"
        cap = self.max_spread_pips * scale
        if self.max_spread_pips and spread_pips > cap:    # said as set when not scaled (v1.2.0's wording)
            return False, f"spread {spread_pips:.1f} pips > {self.max_spread_pips if scale == 1 else round(cap, 2)}"
        return True, ""

    # ── sizing ─────────────────────────────────────────────────
    def lot_size(self, balance: float, entry: float, sl: float, spec: SymbolSpec) -> float:
        """Lots such that hitting the stop-loss loses `risk_pct` of balance.

        Rounds DOWN to the broker's volume step. If even the minimum lot would risk more
        than allowed, returns 0 (skip the trade) instead of rounding up.
        """
        sl_pips = abs(entry - sl) / spec.pip
        if sl_pips <= 0 or balance <= 0:
            return 0.0
        risk_money = balance * self.risk_pct / 100
        raw = risk_money / (sl_pips * spec.pip_value_per_lot)
        steps = math.floor(raw / spec.volume_step + 1e-9)
        lots = round(steps * spec.volume_step, 8)
        if lots < spec.volume_min:
            return 0.0
        return min(lots, spec.volume_max)


def burst_lots(balance: float, risk_pct: float, sl_pips: float, trades: int, spec: SymbolSpec) -> tuple[int, float]:
    """One risk budget for the whole group: if every ticket of ONE side hits its stop the loss is
    ≈ risk_pct of balance. Returns (n, lots_per_ticket). Lots round DOWN to volume_step. If that is under
    volume_min, n shrinks so each ticket can use volume_min; (0, 0.0) if even one minimum lot is too much."""
    if balance <= 0 or risk_pct <= 0 or sl_pips <= 0 or trades < 1 or spec.pip_value_per_lot <= 0:
        return 0, 0.0
    total = balance * risk_pct / 100 / (sl_pips * spec.pip_value_per_lot)     # lots of one whole side
    lots = round(math.floor(total / trades / spec.volume_step + 1e-9) * spec.volume_step, 8)
    if lots >= spec.volume_min:
        return trades, min(lots, spec.volume_max)
    n = min(trades, math.floor(total / spec.volume_min + 1e-9))
    return (n, spec.volume_min) if n >= 1 else (0, 0.0)


# ── stop loss in money (v1.3.0: SL in pips / % of balance / $) ──
def _finite(v) -> float | None:
    """`v` as a float, or None when it is no finite number (junk, a bool, NaN, infinity)."""
    if isinstance(v, bool):                            # True must not become a 1 % risk
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def money_for(unit: str, value, balance) -> float:
    """Money a stop set in `unit` may lose: "pct" = `value` % of `balance`, "usd" = `value` in the account currency.
    Either is capped at MAX_RISK_PCT % of the balance (bot/scalp.py: a short-term trade's cap too). 0 for an unknown
    unit, or a value or balance that is not a finite number above 0."""
    v, bal = _finite(value), _finite(balance)
    if unit not in UNITS or v is None or bal is None or v <= 0 or bal <= 0:
        return 0.0
    money = bal * v / 100 if unit == "pct" else v
    return min(money, bal * MAX_RISK_PCT / 100)


def money_stop(money: float, lots: float, n: int, spec: SymbolSpec, floor_pips: float,
               cap_pips: float | None) -> tuple[float, float]:
    """The stop at which `n` orders of `lots` each (the user's lot) lose at most `money` together. Returns
    (sl_pips, lots): sl_pips = money / (n × lots × pip value), rounded DOWN to the point (sl_pips × pip is a whole
    number of points, a price the broker can take).
    - Under `floor_pips` (stop_floor_pips) the stop is the floor, rounded UP to the point, and the lots are cut to fit
      the money (lots_for_money: DOWN to the volume step), so the money is never exceeded. A cut under volume_min gives
      lots 0: the caller skips the run.
    - Over `cap_pips` (the largest stop the setting allows; None or 0 = no cap) the stop is the cap, rounded DOWN to
      the point, and the run risks less than the money. A cap under the floor gives way to the floor.
    `lots` is rounded DOWN to the volume step first; above volume_max it is kept (the caller splits the order).
    (0.0, 0.0) for a lot under volume_min, no money, no order, or a spec without pip, point, pip value or step."""
    m, lt, k = _finite(money), _finite(lots), _finite(n)
    pv, pip, point, step = spec.pip_value_per_lot, spec.pip, spec.point, spec.volume_step
    if m is None or lt is None or k is None or m <= 0 or lt <= 0 or k < 1 or \
            not (pv > 0 and pip > 0 and point > 0 and step > 0):
        return 0.0, 0.0
    lt = round(math.floor(lt / step + 1e-9) * step, 8)
    if lt <= 0 or lt < spec.volume_min:
        return 0.0, 0.0
    ppp = pip / point                          # points in a pip: 10 on 5-digit FX, 3-digit JPY and 2-digit gold
    if abs(ppp - round(ppp)) < 1e-6:
        ppp = round(ppp)                       # whole: points / ppp is then the decimal itself (20.3, not 20.2999...)
    raw = min(m / (k * lt * pv) * ppp, 1e12)   # the stop, in points, that loses exactly the money (kept finite)
    pts = math.floor(raw * (1 + 1e-12))        # DOWN to the point; the factor only stops 199.99999999999997 being 199
    lo = max(1, math.ceil((_finite(floor_pips) or 0.0) * ppp - 1e-9))
    cap = _finite(cap_pips)
    if cap is not None and cap > 0:
        pts = min(pts, max(1, math.floor(cap * ppp + 1e-9)))
    if pts < lo:
        pts = lo
        lt = min(lt, lots_for_money(m / k, pts / ppp, replace(spec, volume_max=math.inf)))
    return pts / ppp, lt


def stop_floor_pips(spec: SymbolSpec, spread_pips: float | None, spread_cap_pips: float | None) -> float:
    """The closest a money stop may sit, in pips: the broker's stops level and freeze level (an order closer than
    these can be neither placed nor changed) and three spreads, the spread being the larger of the one quoted now and
    its cap (spread_cap_pips, already multiplied by the symbol's scale): a stop inside the spread's noise is taken out
    by the release's widening alone. An unknown spread or cap (None, junk) counts 0."""
    if not spec.pip > 0:
        return 0.0
    spread = max(_finite(spread_pips) or 0.0, _finite(spread_cap_pips) or 0.0)
    return max(spec.min_stop_distance / spec.pip, spec.freeze_distance / spec.pip, 3 * spread, 0.0)
