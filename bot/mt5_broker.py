"""Thin wrapper around the official MetaTrader5 Python package.

The MT5 terminal must be installed and running on this Windows PC; the Python package
talks to it locally. "Algo Trading" must be enabled in the terminal for orders to go through.
"""
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from .symbols import SymbolSpec, pip_size, symbol_scale

log = logging.getLogger("mt5")

try:
    import MetaTrader5 as mt5
except ImportError:   # lets backtests on CSV run without the package
    mt5 = None

TIMEFRAMES = {"M1": "TIMEFRAME_M1", "M5": "TIMEFRAME_M5", "M15": "TIMEFRAME_M15", "M30": "TIMEFRAME_M30",
              "H1": "TIMEFRAME_H1", "H4": "TIMEFRAME_H4", "D1": "TIMEFRAME_D1"}


# answers that leave a deal's outcome open: placed (10008), processing error (10011), timeout (10012), locked (10028)
UNSURE_RETCODES = (10008, 10011, 10012, 10028)

# place_pending's kinds: (buy type, sell type). A stop-limit turns into a plain limit on the same ticket once its
# trigger is reached (BUY_STOP_LIMIT -> BUY_LIMIT, SELL_STOP_LIMIT -> SELL_LIMIT)
PENDING_TYPES = {"stop": ("ORDER_TYPE_BUY_STOP", "ORDER_TYPE_SELL_STOP"),
                 "stop_limit": ("ORDER_TYPE_BUY_STOP_LIMIT", "ORDER_TYPE_SELL_STOP_LIMIT")}
ORDER_KINDS = {"ORDER_TYPE_BUY_STOP": "stop", "ORDER_TYPE_SELL_STOP": "stop",
               "ORDER_TYPE_BUY_STOP_LIMIT": "stop_limit", "ORDER_TYPE_SELL_STOP_LIMIT": "stop_limit",
               "ORDER_TYPE_BUY_LIMIT": "limit", "ORDER_TYPE_SELL_LIMIT": "limit"}
# order_state: MT5's order states. One with a request on its way is taken as it stands until that is done: being
# added = started, being modified or removed = still placed
ORDER_STATES = {"ORDER_STATE_STARTED": "started", "ORDER_STATE_REQUEST_ADD": "started",
                "ORDER_STATE_PLACED": "placed", "ORDER_STATE_REQUEST_MODIFY": "placed",
                "ORDER_STATE_REQUEST_CANCEL": "placed", "ORDER_STATE_PARTIAL": "partial",
                "ORDER_STATE_FILLED": "filled", "ORDER_STATE_CANCELED": "canceled",
                "ORDER_STATE_EXPIRED": "expired", "ORDER_STATE_REJECTED": "rejected"}
SYMBOL_ORDER_STOP_LIMIT = 8     # symbol_info().order_mode flag (the package has no name for it)


class BrokerError(RuntimeError):
    def __init__(self, message: str, retcode: int | None = None):
        super().__init__(message)
        self.retcode = retcode
        self.done = self.unsure = 0.0       # close_position: closed before the failure / sent with no answer


def _ts(seconds: float) -> str:
    """Epoch seconds -> naive ISO. MT5's own stamps are on the broker's clock: convert them with MT5Broker._utc."""
    return datetime.fromtimestamp(seconds, timezone.utc).replace(tzinfo=None).isoformat()


def _ny_offset(at: float) -> int:
    """New York's UTC offset in hours at epoch `at` (US DST: 2nd Sunday of March to 1st Sunday of November, 02:00)."""
    y = datetime.fromtimestamp(at, timezone.utc).year
    mar, nov = datetime(y, 3, 1, tzinfo=timezone.utc), datetime(y, 11, 1, tzinfo=timezone.utc)
    start = mar + timedelta(days=(6 - mar.weekday()) % 7 + 7, hours=7)
    end = nov + timedelta(days=(6 - nov.weekday()) % 7, hours=6)
    return -4 if start.timestamp() <= at < end.timestamp() else -5


def _fx_closed(now: float | None = None) -> bool:
    """FX is shut: Friday 17:00 to Sunday 17:00 New York time (21:00 or 22:00 UTC, with US DST), Christmas and New
    Year's Day, and their eves from 21:00 UTC. The latest tick can then be hours old, even one that only just reached
    the terminal after a reconnect."""
    now = time.time() if now is None else now
    d, ny = datetime.fromtimestamp(now, timezone.utc), datetime.fromtimestamp(now + _ny_offset(now) * 3600, timezone.utc)
    h, md = ny.weekday() * 24 + ny.hour, (d.month, d.day)
    return (4 * 24 + 17 <= h < 6 * 24 + 17 or md in ((12, 25), (1, 1))
            or (md in ((12, 24), (12, 31)) and d.hour >= 21))


def _fx_last_close(now: float | None = None) -> float:
    """Epoch of the latest Friday 17:00 New York at or before `now`: the start of the weekend in which brokers move
    their clocks for DST."""
    now = time.time() if now is None else now
    ny = datetime.fromtimestamp(now + _ny_offset(now) * 3600, timezone.utc)
    fri = (ny - timedelta(days=(ny.weekday() - 4) % 7)).replace(hour=17, minute=0, second=0, microsecond=0)
    if fri > ny:
        fri -= timedelta(days=7)
    wall = fri.timestamp()                       # New York wall time read as UTC; a Friday is far from any DST switch
    return wall - _ny_offset(wall + 5 * 3600) * 3600


class MT5Broker:
    def __init__(self, magic: int = 0, deviation: int = 20):
        if mt5 is None:
            raise BrokerError("MetaTrader5 package not installed (pip install MetaTrader5)")
        self.magic, self.deviation = magic, deviation
        self._pending_fill: dict[tuple[str, str], int] = {}  # (symbol, kind) -> filling mode the broker accepted
        self._gtc_only: set[str] = set()            # symbols whose broker refused a specified expiry
        self._offset: int | None = None             # broker clock minus UTC, seconds (only ever from a live tick)
        self._offset_at = 0.0                       # when a live tick last confirmed _offset (0: never)
        self._offsets: list[tuple[float, int]] = []     # (broker time from, offset): one entry per DST regime seen
        self._seen: dict[str, tuple[int, float, bool]] = {}  # symbol -> (time_msc, local time, moved) of the last look
        self._selected: set[str] = set()            # symbols already put in Market Watch on this connection

    # ── connection ─────────────────────────────────────────────
    def connect(self):
        self._selected.clear()                      # a new terminal session: its Market Watch may differ
        kwargs = {}
        if os.getenv("MT5_PATH"):
            kwargs["path"] = os.getenv("MT5_PATH")
        if os.getenv("MT5_LOGIN"):
            kwargs.update(login=int(os.getenv("MT5_LOGIN")), password=os.getenv("MT5_PASSWORD", ""),
                          server=os.getenv("MT5_SERVER", ""))
        if not mt5.initialize(**kwargs):
            raise BrokerError(f"MT5 initialize failed: {mt5.last_error()}. Is the MT5 terminal installed and running?")
        info = mt5.account_info()
        if info is None:
            raise BrokerError("Connected to MT5 but no account is logged in.")
        log.info("Connected to MT5: account %s on %s (%s)", info.login, info.server, "DEMO" if self.is_demo() else "REAL")

    def shutdown(self):
        mt5.shutdown()

    def account(self) -> dict:
        a = mt5.account_info()
        if a is None:
            raise BrokerError(f"account_info failed: {mt5.last_error()}")
        modes = {mt5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING: "hedging", mt5.ACCOUNT_MARGIN_MODE_RETAIL_NETTING: "netting",
                 mt5.ACCOUNT_MARGIN_MODE_EXCHANGE: "exchange"}
        return {"login": a.login, "server": a.server, "currency": a.currency, "balance": a.balance,
                "equity": a.equity, "margin": a.margin, "free_margin": a.margin_free, "leverage": a.leverage,
                "demo": a.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO, "trade_allowed": bool(a.trade_allowed),
                "margin_mode": modes.get(a.margin_mode, "netting")}

    def is_demo(self) -> bool:
        return mt5.account_info().trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO

    def terminal_trading_enabled(self) -> bool:
        t = mt5.terminal_info()
        return bool(t and t.trade_allowed)

    # ── market data ────────────────────────────────────────────
    def _select(self, symbol: str):
        """Puts the symbol in Market Watch: a hidden one (GBPJPY at some brokers) has no ticks until then. Once per
        symbol and connection, and again after a read of it failed (tick, rates, ticks_since)."""
        if symbol in self._selected:
            return
        if not mt5.symbol_select(symbol, True):
            raise BrokerError(f"Symbol {symbol} not available at this broker (check the exact name, e.g. EURUSD.m)")
        self._selected.add(symbol)

    def symbol_spec(self, symbol: str) -> SymbolSpec:
        self._select(symbol)
        s = mt5.symbol_info(symbol)
        pip = pip_size(symbol)              # by name, not digits: gold quoted with 2 digits still has a 0.1 pip
        tick_value = s.trade_tick_value or 0
        pip_value = tick_value / s.trade_tick_size * pip if s.trade_tick_size else 10.0
        return SymbolSpec(symbol=symbol, pip=pip, pip_value_per_lot=pip_value, digits=s.digits, point=s.point,
                          volume_min=s.volume_min, volume_step=s.volume_step, volume_max=s.volume_max,
                          min_stop_distance=s.trade_stops_level * s.point,
                          freeze_distance=getattr(s, "trade_freeze_level", 0) * s.point, scale=symbol_scale(symbol))

    @staticmethod
    def _tf(timeframe: str):
        if timeframe not in TIMEFRAMES:
            raise BrokerError(f"Unknown timeframe {timeframe}; use one of {list(TIMEFRAMES)}")
        return getattr(mt5, TIMEFRAMES[timeframe])

    @staticmethod
    def _to_df(rates) -> pd.DataFrame:
        if rates is None or len(rates) == 0:
            raise BrokerError(f"No price data returned: {mt5.last_error()}")
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s")
        return df[["time", "open", "high", "low", "close", "tick_volume", "spread"]]

    def _bars(self, symbol: str, rates) -> pd.DataFrame:
        try:
            return self._to_df(rates)
        except BrokerError:
            self._selected.discard(symbol)      # taken out of Market Watch meanwhile? selected again on the next read
            raise

    def rates(self, symbol: str, timeframe: str, count: int) -> pd.DataFrame:
        """Latest `count` bars. The LAST row is the bar still forming."""
        self._select(symbol)
        return self._bars(symbol, mt5.copy_rates_from_pos(symbol, self._tf(timeframe), 0, count))

    def rates_range(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> pd.DataFrame:
        self._select(symbol)
        return self._bars(symbol, mt5.copy_rates_range(symbol, self._tf(timeframe), start, end))

    def tick(self, symbol: str):
        self._select(symbol)
        t = mt5.symbol_info_tick(symbol)
        if t is None:
            self._selected.discard(symbol)      # taken out of Market Watch meanwhile? selected again on the next look
            raise BrokerError(f"No tick for {symbol}: {mt5.last_error()}")
        self._learn_offset(symbol, getattr(t, "time_msc", 0))
        return t

    def ticks_since(self, symbol: str, after_msc: int, count: int = 5000,
                    back_s: int = 60) -> list[tuple[int, float, float]]:
        """Quote ticks (time_msc on the broker's clock, bid, ask) stamped after `after_msc`, oldest first; at most
        `count`. after_msc 0: the last `back_s` seconds before the symbol's last tick. The short-term strategy builds
        its one-second bars from these, as the history it was tested on was built (COPY_TICKS_INFO, bid and ask
        both set)."""
        self._select(symbol)
        if not after_msc:
            t = self.tick(symbol)
            after_msc = int(getattr(t, "time_msc", 0) or t.time * 1000) - back_s * 1000
        ticks = mt5.copy_ticks_from(symbol, max(0, after_msc // 1000), count, mt5.COPY_TICKS_INFO)
        if ticks is None:
            self._selected.discard(symbol)      # as in rates(): selected again on the next read
            raise BrokerError(f"copy_ticks_from failed for {symbol}: {mt5.last_error()}")
        out = [(int(x["time_msc"]), float(x["bid"]), float(x["ask"])) for x in ticks
               if x["time_msc"] > after_msc and x["bid"] > 0 and x["ask"] > 0]
        if out:
            self._learn_offset(symbol, out[-1][0])
        return out

    def margin_for(self, symbol: str, side: str, lots: float) -> float | None:
        """Margin a market order of `lots` would take now, in the account currency (None: MT5 could not say)."""
        t = self.tick(symbol)
        buy = side == "buy"
        m = mt5.order_calc_margin(mt5.ORDER_TYPE_BUY if buy else mt5.ORDER_TYPE_SELL, symbol, float(lots),
                                  t.ask if buy else t.bid)
        return float(m) if m is not None else None

    def _learn_offset(self, symbol: str, msc: int):
        """The server's offset is a live tick's stamp minus UTC, rounded to the half hour. A tick is proven live when
        the symbol's stamp moved forward on two looks in a row, each within 10 min of the one before: a single look
        proves nothing (Friday's last tick read on Saturday morning looks like UTC-6), and a single move can be the
        terminal catching up after a reconnect with a tick from before the symbol's own break (gold, indices).
        While FX is shut nothing is learned. A new offset proven after a weekend that followed the old one's last
        proof is a DST switch: it covers every stamp since that weekend's close (the first deals after the reopen
        predate the proof)."""
        if not msc:
            return
        now = time.time()
        prev = self._seen.get(symbol)
        if prev is not None and msc == prev[0] and now - prev[1] < 2:
            return                                          # the same look again (positions() then close_position()), not a stall
        moved = prev is not None and msc > prev[0] and now - prev[1] < 600
        self._seen[symbol] = (msc, now, moved)
        if not (moved and prev[2]) or _fx_closed():
            return
        offset = round((msc / 1000 - now) / 1800) * 1800
        if not -12 * 3600 <= offset <= 14 * 3600:
            return
        if offset != self._offset:
            since = prev[0] / 1000
            if self._offset is not None:
                log.info("MT5 server clock is now UTC%+.1fh", offset / 3600)
                close = _fx_last_close(now)
                if self._offset_at <= close:
                    since = close + self._offset          # the close, on the old clock
            self._offsets.append((since, offset))
            self._offset = offset
        self._offset_at = now

    def _utc(self, broker_seconds: int, symbol: str = "") -> str | None:
        """MT5 stamps positions and deals on the broker's clock (UTC+2/+3 for most): real UTC, converted with the
        offset in force when the stamp was made (a DST switch while running keeps older stamps right). None until a
        live tick has proven the offset (e.g. a restart over the weekend): callers wait rather than store a time
        that may be hours off; also for a stamp made since the last weekend's close until a live tick proves the
        offset again (DST may have switched that weekend). Looks at a tick again when the proof is 5 min old."""
        if symbol and time.time() - self._offset_at > 300:
            try:
                self.tick(symbol)
            except BrokerError:
                pass
        if not self._offsets:
            return None
        close = _fx_last_close()
        if self._offset_at < close and broker_seconds > close + self._offset:
            return None
        offset = self._offsets[0][1]
        for since, o in self._offsets[1:]:
            if broker_seconds > since:
                offset = o
        return _ts(broker_seconds - offset)

    # ── positions & orders ─────────────────────────────────────
    def positions(self, all_magic: bool = False) -> list[dict]:
        """Open positions opened by THIS bot (matching magic number); `all_magic` adds manual/other EAs' ones.
        A failed read raises: callers must never take it for a flat account (false closes, a second entry)."""
        ps = mt5.positions_get()
        if ps is None and mt5.last_error()[0] < 0:          # None also means "no positions" on some builds
            raise BrokerError(f"positions_get failed: {mt5.last_error()}")
        out = []
        for p in ps or ():
            if p.magic != self.magic and not all_magic:
                continue
            out.append({"ticket": p.ticket, "identifier": p.identifier, "symbol": p.symbol,
                        "side": "buy" if p.type == mt5.POSITION_TYPE_BUY else "sell",
                        "lots": p.volume, "entry_price": p.price_open, "sl": p.sl or None, "tp": p.tp or None,
                        "price": p.price_current, "profit": p.profit + p.swap,
                        "open_time": self._utc(p.time, p.symbol), "broker_time": p.time, "comment": p.comment})
        return out

    @staticmethod
    def _filling(symbol: str):
        mode = mt5.symbol_info(symbol).filling_mode
        if mode & 1:
            return mt5.ORDER_FILLING_FOK
        if mode & 2:
            return mt5.ORDER_FILLING_IOC
        return mt5.ORDER_FILLING_RETURN

    def _send(self, request: dict, ok: tuple = ()):
        res = mt5.order_send(request)
        if res is None:
            raise BrokerError(f"order_send failed: {mt5.last_error()}")
        if res.retcode not in (ok or (mt5.TRADE_RETCODE_DONE,)):
            raise BrokerError(f"order rejected ({res.retcode}): {res.comment}", res.retcode)
        return res

    def open_market(self, symbol: str, side: str, lots: float, sl: float, tp: float | None, comment: str = ""):
        """A partial fill (10010, IOC) is a position too: `lots` is what was really filled."""
        spec = self.symbol_spec(symbol)
        t = self.tick(symbol)
        price = t.ask if side == "buy" else t.bid
        res = self._send({
            "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": float(lots),
            "type": mt5.ORDER_TYPE_BUY if side == "buy" else mt5.ORDER_TYPE_SELL, "price": price,
            "sl": round(sl, spec.digits), "tp": round(tp, spec.digits) if tp else 0.0,
            "deviation": self.deviation, "magic": self.magic, "comment": comment[:31],
            "type_time": mt5.ORDER_TIME_GTC, "type_filling": self._filling(symbol),
        }, (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL))
        return {"ticket": res.order, "price": res.price or price, "lots": res.volume}

    def close_position(self, pos: dict, comment: str = "exit rule", volume: float | None = None,
                       orders: list | None = None) -> float:
        """`volume` closes only that part of the position (netting news burst: one slice). Sent as deals of at most
        the symbol's volume_max: MT5 refuses a bigger one (10014), and a merged netting position can be bigger. A
        partial fill (10010, IOC) is followed by a deal for the rest. Returns the volume closed; a failure raises,
        possibly after some deals went through: callers size their next deal from the position as it is then. The
        error's `done` is what those deals closed, its `unsure` the volume of a deal MT5 gave no answer for, or one
        that leaves the outcome open (UNSURE_RETCODES): it may have gone through. `orders` gets the ticket of each
        order that closed something: its deals carry it."""
        spec, buy = self.symbol_spec(pos["symbol"]), pos["side"] == "buy"
        total = left = round(float(volume or pos["lots"]), 8)
        sending = 0.0
        try:
            while left > 1e-9:
                lots = min(left, spec.volume_max) if spec.volume_max > 0 else left
                t = self.tick(pos["symbol"])
                sending = lots
                res = self._send({
                    "action": mt5.TRADE_ACTION_DEAL, "symbol": pos["symbol"], "volume": float(lots),
                    "type": mt5.ORDER_TYPE_SELL if buy else mt5.ORDER_TYPE_BUY, "position": pos["ticket"],
                    "price": t.bid if buy else t.ask, "deviation": self.deviation, "magic": self.magic,
                    "comment": comment[:31], "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": self._filling(pos["symbol"]),
                }, (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL))
                sending = 0.0
                done = res.volume if res.retcode == mt5.TRADE_RETCODE_DONE_PARTIAL else lots
                if (done or 0) <= 0:
                    raise BrokerError(f"order rejected ({res.retcode}): nothing filled", res.retcode)
                if orders is not None and res.order:
                    orders.append(res.order)
                left = round(left - done, 8)
        except BrokerError as e:
            e.done = round(total - left, 8)
            e.unsure = sending if e.retcode is None or e.retcode in UNSURE_RETCODES else 0.0
            raise
        return total

    def closed_result(self, ticket: int) -> dict | None:
        """Final result of a closed position: exit price/time and net P&L (incl. commission & swap)."""
        deals = mt5.history_deals_get(position=ticket)
        if not deals:
            return None
        exits = [d for d in deals if d.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY)]
        if not exits:
            return None
        last = exits[-1]
        pnl = sum(d.profit + d.commission + d.swap + getattr(d, "fee", 0.0) for d in deals)
        return {"exit_price": last.price, "exit_time": self._utc(last.time, getattr(last, "symbol", "")),
                "pnl": round(pnl, 2), "comment": last.comment}

    # ── pending orders (news burst, Rapid) ─────────────────────
    def stop_limit_allowed(self, symbol: str) -> bool:
        """The symbol takes stop-limit orders (order_mode flag 8). A symbol_info without the field does not."""
        self._select(symbol)
        return bool((getattr(mt5.symbol_info(symbol), "order_mode", 0) or 0) & SYMBOL_ORDER_STOP_LIMIT)

    def place_pending(self, symbol: str, side: str, lots: float, price: float, sl: float | None, tp: float | None,
                      comment: str = "", expire_in: float | None = None, kind: str = "stop",
                      limit_price: float | None = None) -> int:
        """Buy-stop / sell-stop. Returns the order ticket (= the identifier of the position it opens).
        With `expire_in` seconds the broker itself removes the order then: a backstop for a PC that goes away
        mid-burst (only where the symbol allows a specified expiry; otherwise good-till-cancelled).
        kind "stop_limit": a buy / sell stop-limit. `price` is the trigger, `limit_price` the limit the order turns
        into on the same ticket once the trigger is reached, on the pullback side: below the trigger for a buy, above
        it for a sell (the broker refuses it at the trigger or closer than its stops level). SL and TP as given (Rapid
        measures them from the limit). A symbol that takes no stop-limit orders raises before anything is sent: there
        is no plain-stop fallback. The filling mode is learnt per symbol and kind."""
        if kind not in PENDING_TYPES:
            raise ValueError(f"unknown pending order kind {kind!r}")
        if kind == "stop_limit" and limit_price is None:
            raise ValueError("a stop-limit order needs its limit price")
        spec = self.symbol_spec(symbol)
        if kind == "stop_limit" and not self.stop_limit_allowed(symbol):
            raise BrokerError(f"stop-limit orders are not allowed on {symbol}")
        fill = self._pending_fill.get((symbol, kind))
        req = {"action": mt5.TRADE_ACTION_PENDING, "symbol": symbol, "volume": float(lots),
               "type": getattr(mt5, PENDING_TYPES[kind][0 if side == "buy" else 1]),
               "price": round(price, spec.digits), "sl": round(sl, spec.digits) if sl else 0.0,
               "tp": round(tp, spec.digits) if tp else 0.0, "magic": self.magic, "comment": comment[:31],
               "type_time": mt5.ORDER_TIME_GTC, "type_filling": self._filling(symbol) if fill is None else fill}
        if kind == "stop_limit":
            req["stoplimit"] = round(limit_price, spec.digits)
            if not (req["stoplimit"] < req["price"] if side == "buy" else req["stoplimit"] > req["price"]):
                raise ValueError(f"{symbol}: a {side} stop-limit's limit {req['stoplimit']} must be "
                                 f"{'below' if side == 'buy' else 'above'} its trigger {req['price']}")
        if expire_in and symbol not in self._gtc_only and mt5.symbol_info(symbol).expiration_mode & 4:  # SPECIFIED
            req["type_time"], req["expiration"] = mt5.ORDER_TIME_SPECIFIED, int(self._server_now(symbol) + expire_in)
        ok = (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED)
        while True:
            try:
                res = self._send(req, ok)
                break
            except BrokerError as e:   # each retry changes one field for good, so this ends after two at most
                if e.retcode == mt5.TRADE_RETCODE_INVALID_FILL and req["type_filling"] != mt5.ORDER_FILLING_RETURN:
                    req["type_filling"] = mt5.ORDER_FILLING_RETURN   # some brokers take stop orders only with RETURN
                elif e.retcode == mt5.TRADE_RETCODE_INVALID_EXPIRATION and "expiration" in req:
                    log.warning("%s: broker refused expiry %s (%s); its stop orders are good-till-cancelled from now",
                                symbol, req["expiration"], e)
                    req["type_time"] = mt5.ORDER_TIME_GTC
                    del req["expiration"]
                    self._gtc_only.add(symbol)
                else:
                    raise
        self._pending_fill[symbol, kind] = req["type_filling"]
        return res.order

    def _server_now(self, symbol: str) -> float:
        """Now on the broker's clock, which MT5 uses for every time (expiry included). Before news the symbol's tick
        is seconds old, so its own stamp is the broker's now, whatever the PC clock says. Local time plus the offset
        is trusted over it only when a live tick confirmed the offset in the last 5 min (it then also covers a slow
        tick), or when it is less than 15 min ahead: an hour ahead is a DST switch the offset has not seen yet."""
        t = self.tick(symbol)
        stamp, local = getattr(t, "time_msc", 0) / 1000, time.time() + (self._offset or 0)
        if not stamp:
            return local
        if self._offset is not None and time.time() - self._offset_at <= 300:
            return max(stamp, local)
        return local if 0 < local - stamp < 900 else stamp

    @staticmethod
    def _order_kind(order_type: int) -> str:
        """An order type's kind: "stop", "stop_limit", "limit" (a stop-limit whose trigger was reached), else
        "market" (a deal on its way)."""
        return next((k for name, k in ORDER_KINDS.items() if getattr(mt5, name, None) == order_type), "market")

    def pending_orders(self, symbol: str | None = None) -> list[dict]:
        """Pending orders placed by THIS bot (matching magic number). `type` is MT5's own, `kind` as _order_kind. A
        stop-limit waits with `price` = its trigger and `price_stoplimit` = its limit; once triggered it is a limit
        on the same ticket: `price` = the limit, `price_stoplimit` 0.0 (as for every other kind)."""
        orders = mt5.orders_get(symbol=symbol) if symbol else mt5.orders_get()
        if orders is None and mt5.last_error()[0] < 0:     # None also means "no orders" on some builds
            raise BrokerError(f"orders_get failed: {mt5.last_error()}")
        buys = (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT)
        return [{"ticket": o.ticket, "symbol": o.symbol, "side": "buy" if o.type in buys else "sell",
                 "price": o.price_open, "sl": o.sl or None, "tp": o.tp or None, "lots": o.volume_current,
                 "comment": o.comment, "type": int(o.type), "kind": self._order_kind(o.type),
                 "price_stoplimit": float(getattr(o, "price_stoplimit", 0) or 0.0)}
                for o in orders or () if o.magic == self.magic]

    def order_state(self, ticket: int) -> dict | None:
        """Where order `ticket` stands: in the book (orders_get) or done (history_orders_get). `state`: "started"
        (being registered), "placed" (waiting), "partial", "filled", "canceled", "expired" or "rejected". A stop-limit
        whose trigger was reached is a limit on the same ticket: its `type` changed (6 -> 2, 7 -> 3, `kind` "limit"),
        `price_open` is the limit and `price_stoplimit` 0.0. None when unknown: no such order in either (history can
        lag a moment behind the book) or a read that failed; never proof the order is gone."""
        ticket, states = int(ticket), {getattr(mt5, name, None): s for name, s in ORDER_STATES.items()}
        for live in (True, False):        # the book first: an order that leaves it meanwhile is then in the history
            found = mt5.orders_get(ticket=ticket) if live else mt5.history_orders_get(ticket=ticket)
            o = next((x for x in found or () if x.ticket == ticket), None)
            if o is None:
                continue
            state = states.get(o.state, "placed" if live else None)   # a state this table lacks: in the book, waiting
            if state is None:
                return None
            return {"state": state, "type": int(o.type), "kind": self._order_kind(o.type),
                    "volume_initial": o.volume_initial, "volume_current": o.volume_current, "price_open": o.price_open,
                    "price_stoplimit": float(getattr(o, "price_stoplimit", 0) or 0.0)}
        return None

    def pending_room(self) -> int | None:
        """How many more pending orders the account may hold: its cap (account limit_orders) minus the orders open
        now, this bot's or not. None: the broker sets no cap (0 or no field). A failed read raises."""
        a = mt5.account_info()
        if a is None:
            raise BrokerError(f"account_info failed: {mt5.last_error()}")
        cap = int(getattr(a, "limit_orders", 0) or 0)
        if cap <= 0:
            return None
        n = mt5.orders_total()
        if n is None:
            raise BrokerError(f"orders_total failed: {mt5.last_error()}")
        return max(0, cap - int(n))

    def cancel_order(self, ticket: int) -> bool:
        """True when removed; False when the order no longer exists (filled or removed meanwhile): not an error."""
        try:
            self._send({"action": mt5.TRADE_ACTION_REMOVE, "order": int(ticket)})
            return True
        except BrokerError:
            if not mt5.orders_get(ticket=int(ticket)):
                return False
            raise                    # e.g. 10029 frozen: the price is inside the freeze level

    def deal_entry(self, ticket: int) -> dict | None:
        """How pending order `ticket` filled; None when it never did. Looked up by order first because on a
        netting account the fill may have merged into a position another order opened. `entry`: "in" (it opened or
        added to the position), "out" (it reduced or closed it), "inout" (it reversed it: the identifier stays)."""
        deals = list(mt5.history_deals_get(ticket=int(ticket)) or ())
        if not deals:
            deals = [d for d in mt5.history_deals_get(position=int(ticket)) or () if d.entry == mt5.DEAL_ENTRY_IN][:1]
        if not deals:
            return None
        lots, e = sum(d.volume for d in deals), deals[0].entry
        return {"price": sum(d.price * d.volume for d in deals) / lots, "time": self._utc(deals[0].time, getattr(deals[0], "symbol", "")), "lots": lots,
                "side": "buy" if deals[0].type == mt5.DEAL_TYPE_BUY else "sell", "position": deals[0].position_id,
                "entry": "in" if e == mt5.DEAL_ENTRY_IN else "inout" if e == mt5.DEAL_ENTRY_INOUT else "out"}

    def position_deals(self, position: int) -> list[dict]:
        """The buys and sells booked on position `position`, oldest first: what it holds adds up from them (a
        netting reversal included, whether booked as one in/out deal or as an out and an in). `mine`: the deal
        carries this bot's magic number (a close by hand has 0, another EA's its own)."""
        deals = mt5.history_deals_get(position=int(position))
        if deals is None and mt5.last_error()[0] < 0:
            raise BrokerError(f"history_deals_get failed: {mt5.last_error()}")
        trades = sorted((d for d in deals or () if d.type in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL)),
                        key=lambda d: (getattr(d, "time_msc", 0) or d.time * 1000, d.ticket))
        return [{"side": "buy" if d.type == mt5.DEAL_TYPE_BUY else "sell", "lots": d.volume, "price": d.price,
                 "order": d.order, "mine": d.magic == self.magic} for d in trades]
