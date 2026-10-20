"""Event-driven, multi-symbol backtester.

Execution model (deliberately pessimistic):
  * Signals are evaluated on the CLOSE of bar i, orders fill at the OPEN of bar i+1.
  * Prices in the data are BID. Buys enter at ask (bid + spread) and exit at bid;
    sells enter at bid and exit at ask.
  * If one bar touches both stop-loss and take-profit, the stop-loss is assumed hit first.
  * If price gaps through a stop, the fill is at the (worse) open price.
  * One shared balance and one RiskManager across all symbols, same as live trading.
"""
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from itertools import groupby

import numpy as np
import pandas as pd

from strategies import PositionView, Strategy

from .config import REPORTS_DIR
from .metrics import compute_metrics
from .risk import RiskManager
from .symbols import SymbolSpec

log = logging.getLogger("backtest")


@dataclass
class Trade:
    symbol: str
    side: str
    lots: float
    entry_time: datetime
    entry_price: float
    sl: float
    tp: float | None
    comment: str = ""
    entry_index: int = 0
    exit_time: datetime | None = None
    exit_price: float | None = None
    pips: float = 0.0
    pnl: float = 0.0
    reason: str = ""


@dataclass
class _SymbolState:
    df: pd.DataFrame
    spec: SymbolSpec
    o: np.ndarray = field(init=False)
    h: np.ndarray = field(init=False)
    l: np.ndarray = field(init=False)
    c: np.ndarray = field(init=False)
    t: np.ndarray = field(init=False)
    spread: np.ndarray = field(init=False)   # in price units
    trade: Trade | None = None
    pending_entry: object = None             # Signal waiting for next open
    pending_exit: bool = False
    last_i: int | None = None                # last bar seen (for marking equity)


class Backtester:
    def __init__(self, cfg: dict, strategy: Strategy, data: dict[str, pd.DataFrame], specs: dict[str, SymbolSpec]):
        self.cfg, self.strategy, self.specs = cfg, strategy, specs
        bt = cfg["backtest"]
        self.initial = float(bt.get("initial_balance", 10000))
        self.commission = float(bt.get("commission_per_lot", 0) or 0)
        self.default_spread_pips = float(bt.get("default_spread_pips", 1.0))
        self.risk = RiskManager(cfg["risk"])
        self.balance = self.initial
        self.closed: list[Trade] = []
        self.equity_curve: list[tuple[datetime, float]] = []
        self.skipped: dict[str, int] = {}
        self.state: dict[str, _SymbolState] = {}
        for sym, raw in data.items():
            df = strategy.prepare(raw.copy().reset_index(drop=True))
            st = _SymbolState(df=df, spec=specs[sym])
            st.o, st.h, st.l, st.c = (df[k].to_numpy(float) for k in ("open", "high", "low", "close"))
            st.t = df["time"].to_numpy()
            default = self.default_spread_pips * st.spec.pip
            if "spread" in df.columns:
                sp = df["spread"].to_numpy(float) * st.spec.point
                st.spread = np.where(sp > 0, sp, default)
            else:
                st.spread = np.full(len(df), default)
            self.state[sym] = st

    # ── money helpers ──────────────────────────────────────────
    def _pnl(self, tr: Trade, exit_price: float, spec: SymbolSpec) -> tuple[float, float]:
        pips = (exit_price - tr.entry_price) / spec.pip
        if tr.side == "sell":
            pips = -pips
        return pips, pips * spec.pip_value_per_lot * tr.lots

    def _close(self, sym: str, i: int, price: float, reason: str):
        st = self.state[sym]
        tr = st.trade
        tr.pips, gross = self._pnl(tr, price, st.spec)
        tr.pnl = gross - self.commission * tr.lots
        tr.exit_time, tr.exit_price, tr.reason = pd.Timestamp(st.t[i]).to_pydatetime(), price, reason
        self.balance += tr.pnl
        self.closed.append(tr)
        st.trade, st.pending_exit = None, False

    def _equity(self) -> float:
        eq = self.balance
        for st in self.state.values():
            if st.trade and st.last_i is not None:
                i = st.last_i
                px = st.c[i] if st.trade.side == "buy" else st.c[i] + st.spread[i]
                eq += self._pnl(st.trade, px, st.spec)[1]
        return eq

    def _skip(self, reason: str):
        key = "spread too wide" if reason.startswith("spread") else reason
        self.skipped[key] = self.skipped.get(key, 0) + 1

    # ── per-bar processing ─────────────────────────────────────
    def _on_open(self, sym: str, i: int):
        """Fills pending orders at this bar's open, then checks SL/TP within the bar."""
        st = self.state[sym]
        o, h, l, s = st.o[i], st.h[i], st.l[i], st.spread[i]

        if st.trade and st.pending_exit:
            self._close(sym, i, o if st.trade.side == "buy" else o + s, "exit rule")

        if st.pending_entry is not None and st.trade is None:
            sig, st.pending_entry = st.pending_entry, None
            fill = o + s if sig.side == "buy" else o
            valid = (sig.sl < fill and (sig.tp is None or sig.tp > fill)) if sig.side == "buy" \
                else (sig.sl > fill and (sig.tp is None or sig.tp < fill))
            if not valid:
                self._skip("gap past stop/target")
            else:
                lots = self.risk.lot_size(self.balance, fill, sig.sl, st.spec)
                if lots <= 0:
                    self._skip("lot size below minimum")
                else:
                    st.trade = Trade(sym, sig.side, lots, pd.Timestamp(st.t[i]).to_pydatetime(), fill,
                                     sig.sl, sig.tp, sig.comment, entry_index=i)

        tr = st.trade
        if tr is None:
            return
        if tr.side == "buy":
            if o <= tr.sl:
                self._close(sym, i, o, "stop-loss (gap)")
            elif tr.tp is not None and o >= tr.tp:
                self._close(sym, i, o, "take-profit (gap)")
            elif l <= tr.sl:
                self._close(sym, i, tr.sl, "stop-loss")
            elif tr.tp is not None and h >= tr.tp:
                self._close(sym, i, tr.tp, "take-profit")
        else:
            ao, ah, al = o + s, h + s, l + s
            if ao >= tr.sl:
                self._close(sym, i, ao, "stop-loss (gap)")
            elif tr.tp is not None and ao <= tr.tp:
                self._close(sym, i, ao, "take-profit (gap)")
            elif ah >= tr.sl:
                self._close(sym, i, tr.sl, "stop-loss")
            elif tr.tp is not None and al <= tr.tp:
                self._close(sym, i, tr.tp, "take-profit")

    def _on_close(self, sym: str, i: int):
        """Bar i just closed: ask the strategy for exits/entries (filled next bar)."""
        st = self.state[sym]
        if i < self.strategy.warmup_bars or i >= len(st.df) - 1:
            return
        if st.trade:
            tr = st.trade
            view = PositionView(sym, tr.side, tr.entry_price, tr.sl, tr.tp, tr.entry_time, i - tr.entry_index)
            if self.strategy.exit_signal(st.df, i, view):
                st.pending_exit = True
            return
        sig = self.strategy.entry_signal(st.df, i)
        if sig is None:
            return
        open_total = sum(1 for x in self.state.values() if x.trade or x.pending_entry is not None)
        ok, why = self.risk.can_open(open_total, st.spread[i] / st.spec.pip)
        if ok:
            st.pending_entry = sig
        else:
            self._skip(why)

    # ── main loop ──────────────────────────────────────────────
    def run(self) -> dict:
        events = sorted(
            ((t, sym, i) for sym, st in self.state.items() for i, t in enumerate(st.t)),
            key=lambda e: (e[0], e[1]),
        )
        for t, group in groupby(events, key=lambda e: e[0]):
            bars = [(sym, i) for _, sym, i in group]
            day = pd.Timestamp(t).date()
            self.risk.update_day(day, self._equity())
            for sym, i in bars:
                self._on_open(sym, i)
                self.state[sym].last_i = i
            eq = self._equity()
            self.risk.check_daily_loss(eq)
            self.equity_curve.append((pd.Timestamp(t).to_pydatetime(), eq))
            for sym, i in bars:
                self._on_close(sym, i)

        for sym, st in self.state.items():   # close anything still open at the last price
            if st.trade:
                i = len(st.df) - 1
                self._close(sym, i, st.c[i] if st.trade.side == "buy" else st.c[i] + st.spread[i], "end of data")
        if self.equity_curve:
            self.equity_curve[-1] = (self.equity_curve[-1][0], self.balance)
        return self.report()

    def report(self) -> dict:
        trades = [asdict(t) for t in self.closed]
        metrics = compute_metrics(trades, self.equity_curve, self.initial)
        step = max(1, len(self.equity_curve) // 1500)   # keep the chart light
        curve = self.equity_curve[::step]
        if self.equity_curve and curve[-1] is not self.equity_curve[-1]:
            curve.append(self.equity_curve[-1])
        return {
            "generated": datetime.now().isoformat(timespec="seconds"),
            "strategy": self.strategy.name,
            "params": self.strategy.params,
            "symbols": list(self.state),
            "timeframe": self.cfg.get("timeframe"),
            "period": [str(self.equity_curve[0][0]) if self.equity_curve else None,
                       str(self.equity_curve[-1][0]) if self.equity_curve else None],
            "risk": self.cfg["risk"],
            "metrics": metrics,
            "skipped_signals": self.skipped,
            "equity": [[t.isoformat(), round(e, 2)] for t, e in curve],
            "trades": trades,
        }


def save_report(report: dict) -> tuple:
    REPORTS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    body = json.dumps(report, default=str, indent=1)
    path = REPORTS_DIR / f"backtest_{stamp}.json"
    path.write_text(body, encoding="utf-8")
    (REPORTS_DIR / "latest_backtest.json").write_text(body, encoding="utf-8")
    csv = REPORTS_DIR / f"backtest_{stamp}_trades.csv"
    pd.DataFrame(report["trades"]).to_csv(csv, index=False)
    return path, csv
