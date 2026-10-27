"""Live / demo trading loop on MetaTrader 5.

Every `poll_seconds`:
  1. read account + this bot's open positions, record positions that closed (SL/TP/manual)
  2. for each symbol, when a NEW bar has closed: run exit rules, then entry rules
  3. publish status for the dashboard

Safety: refuses real-money accounts unless live.allow_real_account is true; in dry_run it
only reports signals. The dashboard's Pause button stops new entries (exits still run).
"""
import logging
import time
from datetime import datetime, timezone

import pandas as pd

from strategies import PositionView, Strategy

from .mt5_broker import BrokerError, MT5Broker
from .notifier import Notifier
from .risk import RiskManager
from .store import Store

log = logging.getLogger("live")


class LiveTrader:
    def __init__(self, cfg: dict, strategy: Strategy):
        self.cfg, self.strategy = cfg, strategy
        live = cfg["live"]
        self.symbols: list[str] = cfg["symbols"]
        self.timeframe: str = cfg["timeframe"]
        self.dry_run = bool(live.get("dry_run", True))
        raw = int(live.get("poll_seconds", 10))
        self.poll = min(300, max(2, raw))
        if raw > 300:   # the broker's clock offset is proven from looks under 10 min apart (MT5Broker._learn_offset)
            log.warning("live.poll_seconds %s capped at 300 s", raw)
        self.broker = MT5Broker(magic=int(live.get("magic", 20261020)), deviation=int(live.get("deviation_points", 20)))
        self.risk = RiskManager(cfg["risk"])
        self.store = Store()
        self.notify = Notifier()
        self.specs = {}
        self.last_bar: dict[str, pd.Timestamp] = {}
        self.last_equity_save = 0.0
        self.account: dict = {}

    def _event(self, level: str, msg: str, telegram: bool = False):
        getattr(log, level if level in ("info", "warning", "error") else "info")(msg)
        self.store.event(level, msg)
        if telegram:
            self.notify.send(msg)

    # ── startup ────────────────────────────────────────────────
    def start(self):
        self.broker.connect()
        self.account = self.broker.account()
        if not self.account["demo"] and not self.cfg["live"].get("allow_real_account", False):
            raise BrokerError("This is a REAL-money account. Test on a demo account first, or set "
                              "live.allow_real_account: true in config.yaml if you really mean it.")
        if not self.dry_run and not self.broker.terminal_trading_enabled():
            raise BrokerError("'Algo Trading' is disabled in the MT5 terminal. Click the Algo Trading button and retry.")
        self.specs = {s: self.broker.symbol_spec(s) for s in self.symbols}
        mode = "DRY RUN" if self.dry_run else ("DEMO" if self.account["demo"] else "REAL")
        self._event("info", f"🤖 Bot started [{mode}] {self.strategy.name} on {', '.join(self.symbols)} {self.timeframe} "
                            f"— balance {self.account['balance']:.2f} {self.account['currency']}", telegram=True)

    def run(self):
        self.start()
        try:
            while True:
                try:
                    self.tick()
                except BrokerError as e:
                    self._event("error", f"Broker error: {e}")
                    self.notify.send(f"⚠️ {e}", throttle_key="broker_error")
                except Exception as e:   # keep running; report and continue
                    log.exception("Unexpected error")
                    self.store.event("error", f"{type(e).__name__}: {e}")
                    self.notify.send(f"⚠️ Bot error: {type(e).__name__}: {e}", throttle_key="error")
                time.sleep(self.poll)
        except KeyboardInterrupt:
            self._event("info", "🛑 Bot stopped by user", telegram=True)
        finally:
            self._publish(running=False)
            self.broker.shutdown()

    # ── one iteration ──────────────────────────────────────────
    def tick(self):
        self.account = self.broker.account()
        equity = self.account["equity"]
        if self.risk.update_day(datetime.now(timezone.utc).date(), equity):
            self.store.event("info", f"New trading day, start equity {equity:.2f}")
        if self.risk.check_daily_loss(equity):
            self._event("warning", f"⛔ Daily loss limit hit ({self.risk.daily_loss_pct}%). "
                                   "No new trades until tomorrow (UTC).", telegram=True)

        positions = self.broker.positions()
        self._sync_closed(positions)
        control, _ = self.store.get_state("control", {"paused": False})
        paused = bool(control and control.get("paused"))

        for symbol in self.symbols:
            df = self.broker.rates(symbol, self.timeframe, self.strategy.warmup_bars + 50)
            closed = df.iloc[:-1].reset_index(drop=True)       # drop the bar still forming
            bar_time = closed["time"].iat[-1]
            first_look = symbol not in self.last_bar
            if self.last_bar.get(symbol) == bar_time:
                continue
            self.last_bar[symbol] = bar_time
            if first_look:
                continue   # don't act on a bar that closed before we started
            self._on_new_bar(symbol, closed, positions, paused)
            positions = self.broker.positions()

        if time.time() - self.last_equity_save >= 60:
            self.store.add_equity(self.account["balance"], equity)
            self.last_equity_save = time.time()
        self._publish(running=True, paused=paused, positions=positions)

    def _on_new_bar(self, symbol: str, closed: pd.DataFrame, positions: list[dict], paused: bool):
        df = self.strategy.prepare(closed.copy())
        i = len(df) - 1
        mine = [p for p in positions if p["symbol"] == symbol]

        for p in mine:
            # bars and the raw position stamp are both on the broker's clock, as in the backtester
            opened = pd.Timestamp(p["broker_time"], unit="s")
            view = PositionView(symbol, p["side"], p["entry_price"], p["sl"], p["tp"], opened.to_pydatetime(),
                                int((df["time"] > opened).sum()))
            if self.strategy.exit_signal(df, i, view):
                if self.dry_run:
                    self._event("info", f"[DRY RUN] would close {p['side']} {symbol} #{p['ticket']}", telegram=True)
                else:
                    self.broker.close_position(p)
                    self._event("info", f"Exit rule: closing {p['side']} {symbol} #{p['ticket']}")
        if mine or paused:
            return

        sig = self.strategy.entry_signal(df, i)
        if sig is None:
            return
        tick = self.broker.tick(symbol)
        spec = self.specs[symbol]
        spread_pips = (tick.ask - tick.bid) / spec.pip
        ok, why = self.risk.can_open(len(positions), spread_pips)
        if not ok:
            self._event("info", f"Signal {sig.side} {symbol} skipped: {why}")
            return

        entry = tick.ask if sig.side == "buy" else tick.bid
        wrong_side = (sig.sl >= entry or (sig.tp is not None and sig.tp <= entry)) if sig.side == "buy" \
            else (sig.sl <= entry or (sig.tp is not None and sig.tp >= entry))
        if wrong_side:
            self._event("warning", f"Signal {sig.side} {symbol} skipped: SL/TP on the wrong side of price {entry}")
            return
        if spec.min_stop_distance and abs(entry - sig.sl) < spec.min_stop_distance:
            self._event("warning", f"Signal {sig.side} {symbol} skipped: stop closer than broker minimum")
            return
        lots = self.risk.lot_size(self.account["balance"], entry, sig.sl, spec)
        if lots <= 0:
            self._event("warning", f"Signal {sig.side} {symbol} skipped: risk too small for minimum lot")
            return

        sl_pips = abs(entry - sig.sl) / spec.pip
        tp_txt = f"{sig.tp:.{spec.digits}f}" if sig.tp else "—"
        summary = (f"{sig.side.upper()} {symbol} {lots} lots @ {entry:.{spec.digits}f}\n"
                   f"SL {sig.sl:.{spec.digits}f} ({sl_pips:.1f} pips) · TP {tp_txt}"
                   + (f"\n{sig.comment}" if sig.comment else ""))
        if self.dry_run:
            self._event("info", f"[DRY RUN] signal: {summary}", telegram=True)
            return
        res = self.broker.open_market(symbol, sig.side, lots, sig.sl, sig.tp, sig.comment or self.strategy.name)
        self.store.open_trade(ticket=res["ticket"], symbol=symbol, side=sig.side, lots=res["lots"],
                              entry_time=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                              entry_price=res["price"], sl=sig.sl, tp=sig.tp, comment=sig.comment)
        self._event("info", f"✅ Opened {summary}", telegram=True)

    def _sync_closed(self, positions: list[dict]):
        live_tickets = {p["ticket"] for p in positions}
        known = self.store.open_trade_tickets()
        for p in positions:                     # adopt positions we don't know yet (e.g. after a DB reset)
            if p["ticket"] not in known and p["open_time"] is not None:     # None: broker clock not learned yet
                self.store.open_trade(ticket=p["ticket"], symbol=p["symbol"], side=p["side"], lots=p["lots"],
                                      entry_time=p["open_time"], entry_price=p["entry_price"], sl=p["sl"],
                                      tp=p["tp"], comment=p["comment"])
        for ticket in known - live_tickets:
            res = self.broker.closed_result(ticket)
            if res is None or res["exit_time"] is None:
                continue                        # history or broker clock not synced yet; retry next tick
            self.store.close_trade(ticket, res["exit_time"], res["exit_price"], res["pnl"])
            icon = "💰" if res["pnl"] > 0 else "🔻"
            self._event("info", f"{icon} Closed #{ticket} @ {res['exit_price']} — P&L {res['pnl']:+.2f} "
                                f"{self.account.get('currency', '')} ({res['comment'] or 'closed'})", telegram=True)

    def _publish(self, running: bool, paused: bool = False, positions: list | None = None):
        eq = self.account.get("equity", 0)
        self.store.set_state("status", {
            "running": running, "paused": paused, "dry_run": self.dry_run, "account": self.account,
            "positions": positions or [], "day_pnl": round(self.risk.day_pnl(eq), 2) if eq else 0,
            "halted": self.risk.halted, "strategy": self.strategy.name, "symbols": self.symbols,
            "timeframe": self.timeframe, "poll_seconds": self.poll,
            "last_bars": {s: str(t) for s, t in self.last_bar.items()},
        })
