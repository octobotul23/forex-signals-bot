"""Hold time X / Y ("hold until profit"): when a quick trade (news burst, short-term, Rapid) is closed. Shared by the
connector's burst runner and its Scalper, so every strategy closes by the same rule.

Up to v1.2.0 a trade was closed at market once it had been open `hold_seconds` (X), win or lose. Now X is the moment
it MAY close in profit and `max_hold_seconds` (Y, never under X) the moment it MUST close:
- from X on, it is closed as soon as it is in profit (its P/L clears min_profit: one point on its lots);
- at any age, it is closed when the signal turns against it (`against`: the caller's verdict, e.g. news_against),
  but only while a real Y above X is set;
- at Y, it is closed whatever it shows.
Its stop-loss and take-profit stay at the broker all along. A missing Y follows X (a user stored before v1.3.0, a
plan from an older server), and a Y at or under X counts as X: the trade then closes at exactly X, in profit or not,
and `against` is not heeded, as in v1.2.0.

hold_action only answers "close it now, and why". The caller latches the first answer that is not None and retries
that close until it is done: a refused close, or a profit that fades a second later, must not undo the decision.
"""
import math

from .symbols import SymbolSpec


def hold_action(age: float, hold: float, max_hold: float | None, profit: float | None, min_profit: float,
                against: bool) -> str | None:
    """Whether a trade open `age` seconds is closed now, and why: "profit" (X is over and it is in profit), "against"
    (the signal turned against it, at any age), "max" (Y is over) or None (keep it). Checked in that order, so a
    trade in profit after X closes as "profit" even when the signal turned as well.
    hold = X and max_hold = Y in seconds (a Y under X, or None, counts as X). profit: its P/L now, in money (None =
    unknown: not in profit). min_profit: the P/L that counts as a profit (min_profit() of its lots).
    against counts only when Y is above X: with Y = X (None: a missing Y follows X) the trade is held to exactly X as
    in v1.2.0, whatever the signal does."""
    y = hold if max_hold is None else max(hold, max_hold)
    if age >= hold and profit is not None and profit >= min_profit:     # NaN compares False: not in profit
        return "profit"
    if against and y > hold:                       # Y = X is the v1.2.0 setup: no early close on the signal
        return "against"
    if age >= y:
        return "max"
    return None


def min_profit(spec: SymbolSpec, lots: float) -> float:
    """Money of one point (the smallest price step) on `lots`: the buffer a close must clear to count as in profit, so
    a trade a rounding cent above zero is not taken for a winner. 0 for no lots or a spec without pip or point."""
    if spec.pip <= 0 or spec.point <= 0 or not lots > 0:
        return 0.0
    return max(0.0, spec.pip_value_per_lot * lots * spec.point / spec.pip)


def _price(v) -> float | None:
    """`v` as a price: a finite number above 0, else None (a missing quote, a bool, NaN, junk)."""
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) and f > 0 else None


def news_against(side: str, bid: float | None, ask: float | None, pre_mid: float | None) -> bool:
    """A news trade has turned against itself when the market is back through the mid quoted just before the release
    (the move it was opened for is gone): for a buy the mid now ((bid + ask) / 2) at or under the pre-release mid, for
    a sell at or over it. Mid against mid: right after a release the spread widens, and a buy's bid (a sell's ask)
    can cross the pre-release mid on the widening alone while the market is still moving the trade's way.
    False when the pre-release mid, the bid or the ask is unknown (None, 0, junk), or the side is neither "buy" nor
    "sell"."""
    b, a, pre = _price(bid), _price(ask), _price(pre_mid)
    if b is None or a is None or pre is None:
        return False
    mid = (b + a) / 2
    if side == "buy":
        return mid <= pre
    if side == "sell":
        return mid >= pre
    return False
