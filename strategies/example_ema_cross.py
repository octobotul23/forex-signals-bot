"""EXAMPLE strategy — EMA crossover with ATR stops. Replace with your own rules in my_strategy.py.

Buy  when the fast EMA crosses above the slow EMA.
Sell when the fast EMA crosses below the slow EMA.
Stop-loss = sl_atr x ATR, take-profit = tp_atr x ATR. Exit early on the opposite cross.
"""
from bot.indicators import atr, crossed_above, crossed_below, ema

from .base import PositionView, Signal, Strategy


class EmaCross(Strategy):
    name = "EMA cross (example)"
    default_params = {"fast": 20, "slow": 50, "atr_period": 14, "sl_atr": 2.0, "tp_atr": 3.0}
    exit_uses_levels = False     # the exit is the opposite cross, whatever the position

    def __init__(self, params=None):
        super().__init__(params)
        self.warmup_bars = int(self.params["slow"]) * 3

    def prepare(self, df):
        p = self.params
        df["ema_fast"] = ema(df["close"], int(p["fast"]))
        df["ema_slow"] = ema(df["close"], int(p["slow"]))
        df["atr"] = atr(df, int(p["atr_period"]))
        return df

    def entry_signal(self, df, i):
        close, a = df["close"].iat[i], df["atr"].iat[i]
        p = self.params
        if crossed_above(df["ema_fast"], df["ema_slow"], i):
            return Signal("buy", sl=close - p["sl_atr"] * a, tp=close + p["tp_atr"] * a, comment="ema up")
        if crossed_below(df["ema_fast"], df["ema_slow"], i):
            return Signal("sell", sl=close + p["sl_atr"] * a, tp=close - p["tp_atr"] * a, comment="ema down")
        return None

    def exit_signal(self, df, i, position: PositionView):
        if position.side == "buy":
            return crossed_below(df["ema_fast"], df["ema_slow"], i)
        return crossed_above(df["ema_fast"], df["ema_slow"], i)
