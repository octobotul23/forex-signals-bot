"""YOUR strategy. Fill in the three methods, then set `strategy: my_strategy` in config.yaml.

Available columns in df: time, open, high, low, close, tick_volume (+ whatever you add in prepare).
Helpers in bot.indicators: sma, ema, rsi, atr, bollinger, macd, highest, lowest,
crossed_above, crossed_below.

Values you put in config.yaml under `strategy_params` arrive in self.params.
"""
from bot.indicators import atr, ema, rsi  # noqa: F401  (import what you need)

from .base import PositionView, Signal, Strategy


class MyStrategy(Strategy):
    name = "My strategy"
    warmup_bars = 200
    default_params = {}

    def prepare(self, df):
        # TODO: add indicator columns, e.g.
        # df["rsi"] = rsi(df["close"], 14)
        # df["atr"] = atr(df, 14)
        return df

    def entry_signal(self, df, i):
        # TODO: your entry rules. Return Signal("buy"/"sell", sl=..., tp=...) or None.
        return None

    def exit_signal(self, df, i, position: PositionView):
        # TODO: optional early-exit rule. SL/TP are handled automatically.
        return False
