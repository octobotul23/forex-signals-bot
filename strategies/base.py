"""The contract every strategy follows. Backtester and live trader both use it.

How it is called, on every newly CLOSED bar:
  1. prepare(df)            -> once per data refresh: add indicator columns
  2. exit_signal(df, i, p)  -> for an open position p: return True to close it at market
  3. entry_signal(df, i)    -> when flat: return a Signal to open a trade, or None

Rule: only look at rows 0..i. Row i is the bar that just closed; there is no row i+1 yet in
live trading, so peeking ahead makes a backtest look better than reality.
"""
from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class Signal:
    side: str                 # "buy" or "sell"
    sl: float                 # stop-loss price (required — risk sizing depends on it)
    tp: float | None = None   # take-profit price (optional)
    comment: str = ""


@dataclass
class PositionView:
    """Read-only view of an open position, passed to exit_signal()."""
    symbol: str
    side: str
    entry_price: float
    sl: float
    tp: float | None
    open_time: datetime
    bars_held: int = 0


class Strategy:
    name = "base"
    warmup_bars = 200            # bars needed before indicators are valid
    default_params: dict = {}
    # False when exit_signal() ignores the position's entry/stop/target (self-learning then reuses its answers)
    exit_uses_levels = True

    def __init__(self, params: dict | None = None):
        self.params = {**self.default_params, **(params or {})}

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def entry_signal(self, df: pd.DataFrame, i: int) -> Signal | None:
        return None

    def exit_signal(self, df: pd.DataFrame, i: int, position: PositionView) -> bool:
        return False
