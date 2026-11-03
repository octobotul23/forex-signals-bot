"""News burst, connector side (docs/NEWS_BURST.md §6, §8): a simulated broker with the MT5Adapter API drives the
real Connector runner; a fake send() collects what would go to the server.

    .venv\\Scripts\\python tests\\test_burst_connector.py [-v]

SimBroker: hedging or netting account (netting merges fills into one position per symbol and applies partial
closes), stop orders that trigger when the price crosses them, stop-limits that turn into a limit on the same ticket
there and fill at it once the price comes back (Rapid), TP/SL hits on price moves, deal and order history, closes in
deals of at most volume_max, and failures on demand (reads, opens, closes, partial fills).
The event loop's clock (the connector's monotonic clock) can be pushed forward, so hold and expiry times pass
at once. No MetaTrader 5, no network, temporary files only. Exit code 0 = every check passed.
"""
import asyncio
import itertools
import json
import logging
import shutil
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-burst-connector-"))
BOXES = itertools.count()
KEPT = {p.name: p.stat().st_mtime_ns for p in (ROOT / "connector").glob("*.json")}   # the connector's own files

from bot.mt5_broker import BrokerError                                   # noqa: E402
from bot.news_view import H1_BARS, M1_BARS, describe                     # noqa: E402
from bot.rapid import size_order, stop_limit_prices                      # noqa: E402
from bot.risk import burst_lots                                          # noqa: E402
from bot.symbols import SymbolSpec                                       # noqa: E402
from connector.connector import (MAGIC, Connector, MT5Adapter, PaperBroker, Scalper, clamp_burst,  # noqa: E402
                                 position_net, run_tag, split_volume)

SERIAL = itertools.count(1)         # each Scalper's own files: by a number of its own, not by id(), which a later
                                    # Scalper can get from a collected one (and read that one's Rapid news runs)


def own_file(scalper, kind: str) -> Path:
    if "test_serial" not in scalper.__dict__:
        scalper.test_serial = next(SERIAL)
    return TMP / f"scalp-{kind}-{scalper.test_serial}.json"


Scalper._day_file = lambda self: own_file(self, "day")      # never scalp_day.json in the project
Scalper._open_file = lambda self: own_file(self, "open")    # nor scalp_open.json

PIP = 0.0001
SETTINGS = {"execution": "auto", "modes": {"trend": True}, "symbols": [], "risk_per_trade_pct": 1.0,
            "max_open_trades": 3, "daily_loss_limit_pct": 3.0, "max_spread_pips": 3.0, "max_price_drift_pips": 5,
            "follow_exits": True}


# ── simulated broker ───────────────────────────────────────────
class SimBroker:
    """The MT5Adapter API on a simulated account. Prices move only when the test calls move()."""
    def __init__(self, mode="hedging", demo=True, balance=10_000.0, spread_pips=0.5, volume_max=30.0, stops_pips=0.5):
        self.lock = threading.RLock()          # the connector calls from its MT5 thread, the test from the loop
        self.mode, self.demo, self.balance, self.algo, self.broken = mode, demo, balance, True, False
        self.bids = {"EURUSD": 1.10000, "GBPUSD": 1.30000}
        self.spread, self.volume_max, self.stops = spread_pips * PIP, volume_max, stops_pips * PIP
        self.orders, self.pos, self.deals, self.calls, self.expire_ins = {}, {}, [], [], []
        self.frozen, self.fail_close, self.next = {}, 0, 5000     # frozen: order ticket -> cancels that still fail
        self.blank = 0                                            # pending_orders() calls that wrongly return []
        self.pos_errors = self.acct_errors = 0                    # positions() / account() calls that fail (IPC)
        self.fail_open = self.lost_reply = 0     # open_market calls refused / filled but answered None (IPC)
        self.partial = None                      # open_market fills at most this many lots (IOC, 10010)
        self.hidden, self.unlisted = {}, {}      # order -> until when history lacks its deals; position -> ... the list
        self.bars = {}                           # timeframe -> DataFrame, or a function giving one (rates())
        self.margin = None                       # margin of one lot (margin_for); None: not known
        self.frozen_close = 0                    # close_position calls refused with 10029 (frozen)
        self.stop_limit, self.freeze = True, 0.0     # stop-limits taken (order_mode & 8); the freeze level (price)
        self.limit_orders, self.room_errors = None, 0    # the account's pending-order cap (None: none); failed reads
        self.done = {}                           # order ticket -> (its state, the order) once it left the book

    def hide(self, order, seconds):
        """The deals of `order` show in history only `seconds` from now (MT5's history behind its positions)."""
        with self.lock:
            self.hidden[order] = time.monotonic() + seconds

    def unlist(self, position, seconds):
        """Position `position` shows in the list of positions only `seconds` from now (its deal already in history)."""
        with self.lock:
            self.unlisted[position] = time.monotonic() + seconds

    def _history(self):
        now = time.monotonic()
        return [d for d in self.deals if self.hidden.get(d["order"], 0) <= now]

    def _ticket(self):
        self.next += 1
        return self.next

    def ask(self, symbol="EURUSD"):
        return round(self.bids[symbol] + self.spread, 5)

    def _px(self, p):
        return self.bids[p["symbol"]] if p["side"] == "buy" else self.ask(p["symbol"])

    def _profit(self, p, vol=None, px=None):
        sign = 1 if p["side"] == "buy" else -1
        return round(((self._px(p) if px is None else px) - p["entry_price"]) * sign / PIP * 10.0
                     * (p["lots"] if vol is None else vol), 2)

    def _deal(self, order, position, entry, side, lots, price, profit, magic=MAGIC):
        self.deals.append({"ticket": self._ticket(), "order": order, "position": position, "entry": entry,
                           "side": side, "lots": lots, "price": price, "profit": profit, "time": "2026-10-28T18:00:00",
                           "magic": magic})

    def _fill(self, o, px):
        cur = next((p for p in self.pos.values() if p["symbol"] == o["symbol"]), None) \
            if self.mode == "netting" else None
        if cur and cur["side"] == o["side"]:          # netting: the fill adds to the one position, SL/TP overwritten
            cur["entry_price"] = round((cur["entry_price"] * cur["lots"] + px * o["lots"]) / (cur["lots"] + o["lots"]), 6)
            cur["lots"] = round(cur["lots"] + o["lots"], 8)
            cur["sl"], cur["tp"] = o["sl"], o["tp"]
            return self._deal(o["ticket"], cur["identifier"], "in", o["side"], o["lots"], px, 0.0, o.get("magic", MAGIC))
        if cur and o["lots"] > cur["lots"] + 1e-9:     # netting reversal (one in/out deal): MT5 keeps the identifier,
            pnl = self._profit(cur, cur["lots"], px)   # the ticket becomes the reversing order's
            self.balance += pnl
            self._deal(o["ticket"], cur["identifier"], "inout", o["side"], o["lots"], px, pnl, o.get("magic", MAGIC))
            del self.pos[cur["ticket"]]
            cur.update(ticket=o["ticket"], side=o["side"], lots=round(o["lots"] - cur["lots"], 8), entry_price=px,
                       sl=o["sl"], tp=o["tp"])
            self.pos[cur["ticket"]] = cur
            return
        if cur:                                        # netting: an opposite fill reduces or closes it
            self._close(cur, o["lots"], px, order=o["ticket"], magic=o.get("magic", MAGIC))
            return
        t = o["ticket"]
        self.pos[t] = {"ticket": t, "identifier": t, "symbol": o["symbol"], "side": o["side"], "lots": o["lots"],
                       "entry_price": px, "sl": o["sl"], "tp": o["tp"], "open_time": "2026-10-28T18:00:00",
                       "comment": o["comment"], "magic": o.get("magic", MAGIC)}
        self._deal(t, t, "in", o["side"], o["lots"], px, 0.0, o.get("magic", MAGIC))

    def _close(self, p, vol, px, order=None, magic=None):
        """`magic`: of the order that closes (default the position's, as for its SL/TP; 0 for a close by hand)."""
        pnl = self._profit(p, vol, px)
        self.balance += pnl
        self._deal(order or self._ticket(), p["identifier"], "out", "sell" if p["side"] == "buy" else "buy", vol, px, pnl,
                   p["magic"] if magic is None else magic)
        p["lots"] = round(p["lots"] - vol, 8)
        if p["lots"] <= 1e-9:
            del self.pos[p["ticket"]]

    def move(self, bid, symbol="EURUSD"):
        """New price: stop orders that it crosses fill at the market price; a stop-limit it crosses becomes a limit on
        the same ticket (as MT5: type 2 / 3, its price the limit, its comment, SL and TP kept), which fills at its
        limit once the price is back at it (never in the move that triggered it); then TP/SL are checked."""
        with self.lock:
            self.bids[symbol] = round(bid, 5)
            bid, ask = self.bids[symbol], self.ask(symbol)
            for o in sorted(self.orders.values(), key=lambda o: o["ticket"]):
                buy = o["side"] == "buy"
                if o["symbol"] != symbol:
                    continue
                if o.get("kind") == "stop_limit":
                    if ask >= o["price"] if buy else bid <= o["price"]:
                        o.update(type=2 if buy else 3, kind="limit", price=o["price_stoplimit"], price_stoplimit=0.0)
                elif o.get("kind") == "limit":
                    if ask <= o["price"] if buy else bid >= o["price"]:
                        del self.orders[o["ticket"]]
                        self.done[o["ticket"]] = ("filled", o)
                        self._fill(o, o["price"])
                elif ask >= o["price"] if buy else bid <= o["price"]:
                    del self.orders[o["ticket"]]
                    self.done[o["ticket"]] = ("filled", o)
                    self._fill(o, ask if buy else bid)
            for p in [p for p in self.pos.values() if p["symbol"] == symbol]:
                px, long = self._px(p), p["side"] == "buy"
                if (p["tp"] and (px >= p["tp"] if long else px <= p["tp"])) or \
                        (p["sl"] and (px <= p["sl"] if long else px >= p["sl"])):
                    self._close(p, p["lots"], px)

    def add_position(self, symbol, side, lots, comment, magic=MAGIC):
        with self.lock:
            t = self._ticket()
            self._fill({"ticket": t, "symbol": symbol, "side": side, "lots": lots, "sl": None, "tp": None,
                        "comment": comment, "magic": magic}, self.ask(symbol) if side == "buy" else self.bids[symbol])
            return t

    def add_order(self, symbol, side, lots, price, comment, magic=MAGIC):
        with self.lock:
            t = self._ticket()
            self.orders[t] = {"ticket": t, "symbol": symbol, "side": side, "lots": lots, "price": price, "sl": None,
                              "tp": None, "comment": comment, "magic": magic, "type": 4 if side == "buy" else 5,
                              "kind": "stop", "price_stoplimit": 0.0}
            return t

    def expire(self, ticket):
        """The broker removes order `ticket` itself (its own expiry): gone from the book, "expired" in its history."""
        with self.lock:
            self.done[ticket] = ("expired", self.orders.pop(ticket))

    def calls_of(self, kind):
        with self.lock:
            return [c for c in self.calls if c[0] == kind]

    # adapter API
    def connect(self):
        pass

    def account(self):
        with self.lock:
            if self.acct_errors:
                self.acct_errors -= 1
                raise RuntimeError("account_info failed: (-10005, 'IPC timeout')")
            equity = self.balance + sum(self._profit(p) for p in self.pos.values())
            return {"login": 1, "server": "sim", "currency": "USD", "balance": round(self.balance, 2),
                    "equity": round(equity, 2), "free_margin": round(equity, 2), "leverage": 100,
                    "demo": self.demo, "trade_allowed": True, "margin_mode": self.mode}

    def terminal_trading_enabled(self):
        return self.algo

    def symbol_spec(self, symbol):
        return SymbolSpec(symbol=symbol, pip=PIP, pip_value_per_lot=10.0, digits=5, point=0.00001, volume_min=0.01,
                          volume_step=0.01, volume_max=self.volume_max, min_stop_distance=self.stops,
                          freeze_distance=self.freeze)

    def quote(self, symbol):
        with self.lock:
            return self.bids[symbol], self.ask(symbol), round(self.spread / PIP, 2)

    def rates(self, symbol, timeframe, count):
        """The bars the test gave for `timeframe` (a function is called outside the lock: it may block or raise);
        none given: BrokerError, as MT5Broker on a symbol with no history."""
        with self.lock:
            self.calls.append(("rates", timeframe, count))
            got = self.bars.get(timeframe)
        if got is None:
            raise BrokerError(f"copy_rates failed for {symbol} {timeframe}")
        return got() if callable(got) else got

    def margin_for(self, symbol, side, lots):
        with self.lock:
            self.calls.append(("margin", side, lots))
            return None if self.margin is None else round(self.margin * lots, 2)

    def positions(self, all_magic=False):
        with self.lock:
            if self.pos_errors:                # MT5Broker.positions raises on a failed positions_get
                self.pos_errors -= 1
                raise RuntimeError("positions_get failed: (-10005, 'IPC timeout')")
            now = time.monotonic()
            return [{**p, "price": self._px(p), "profit": self._profit(p)} for p in self.pos.values()
                    if (all_magic or p["magic"] == MAGIC) and self.unlisted.get(p["identifier"], 0) <= now]

    def open_market(self, symbol, side, lots, sl, tp, comment=""):
        with self.lock:
            self.calls.append(("open", side, lots))
            if self.fail_open:
                self.fail_open -= 1
                raise RuntimeError("order rejected (10004): requote")
            t, px = self._ticket(), self.ask(symbol) if side == "buy" else self.bids[symbol]
            lots = min(lots, self.partial) if self.partial else lots
            self._fill({"ticket": t, "symbol": symbol, "side": side, "lots": lots, "sl": sl, "tp": tp,
                        "comment": comment}, px)
            if self.lost_reply:
                self.lost_reply -= 1
                raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
            return {"ticket": t, "price": px, "lots": lots}

    def close_position(self, pos, comment="", price=None, volume=None):
        """Like MT5Adapter.close_position: `volume` or the caller's pos["lots"], in deals of at most volume_max; the
        tickets of their orders back. A refusal raises BrokerError with its retcode."""
        with self.lock:
            self.calls.append(("close", pos["ticket"], volume))
            if self.fail_close:
                self.fail_close -= 1
                raise BrokerError("order rejected (10004): requote", 10004)
            if self.frozen_close:
                self.frozen_close -= 1
                raise BrokerError("order rejected (10029): frozen", 10029)
            p = self.pos.get(pos["ticket"])
            if p is None:
                raise BrokerError("order rejected (10036): position closed", 10036)
            left = round(volume or pos["lots"], 8)
            if left > p["lots"] + 1e-9:
                raise BrokerError("order rejected (10038): close volume exceeds the position", 10038)
            orders = []
            while left > 1e-9:
                lots = min(left, self.volume_max)
                self.calls.append(("deal", p["ticket"], lots))
                orders.append(self._ticket())
                self._close(p, lots, self._px(p), order=orders[-1], magic=MAGIC)
                left = round(left - lots, 8)
            return orders

    def closed_result(self, ticket):
        with self.lock:
            deals = [d for d in self._history() if d["position"] == ticket]
            exits = [d for d in deals if d["entry"] == "out"]
            if not exits:
                return None
            return {"exit_price": exits[-1]["price"], "exit_time": exits[-1]["time"],
                    "pnl": round(sum(d["profit"] for d in deals), 2)}

    def place_pending(self, symbol, side, lots, price, sl, tp, comment="", expire_in=None, kind="stop",
                      limit_price=None):
        """A stop, or (kind "stop_limit") a stop-limit as MT5Broker places one: `price` its trigger, `limit_price` its
        limit, refused on a symbol without stop-limits (nothing sent), at its trigger or closer to it than the stops
        level, or on the trigger's market side. Then as a stop: a trigger closer to the price than the stops level, a
        volume over volume_max, or an account at its limit_orders is refused."""
        with self.lock:
            self.expire_ins.append(expire_in)
            price, sl, tp = round(price, 5), sl and round(sl, 5), tp and round(tp, 5)
            limit = None if limit_price is None else round(limit_price, 5)
            extra = (limit,) if kind == "stop_limit" else ()          # a stop's call as before: 7 fields
            self.calls.append(("place", side, lots, price, sl, tp, comment) + extra)
            if kind == "stop_limit":
                if limit is None:
                    raise ValueError("a stop-limit order needs its limit price")
                if not self.stop_limit:
                    raise BrokerError(f"stop-limit orders are not allowed on {symbol}")
                gap = price - limit if side == "buy" else limit - price
                if gap <= 0:
                    raise ValueError(f"{symbol}: a {side} stop-limit's limit {limit} is on the wrong side of {price}")
                if gap < self.stops - 1e-9:
                    raise BrokerError("order rejected (10015): invalid price", 10015)
            if lots > self.volume_max + 1e-9:
                raise RuntimeError("order rejected (10014): invalid volume")
            if not (price >= self.ask(symbol) + self.stops if side == "buy" else price <= self.bids[symbol] - self.stops):
                raise RuntimeError("order rejected (10015): invalid price")
            if self.limit_orders is not None and len(self.orders) >= self.limit_orders:
                raise BrokerError("order rejected (10033): the pending orders limit is reached", 10033)
            t, buy = self._ticket(), side == "buy"
            self.orders[t] = {"ticket": t, "symbol": symbol, "side": side, "lots": lots, "price": price,
                              "sl": sl or None, "tp": tp or None, "comment": comment[:31], "magic": MAGIC,
                              "type": (6 if buy else 7) if kind == "stop_limit" else (4 if buy else 5), "kind": kind,
                              "price_stoplimit": limit if kind == "stop_limit" else 0.0}
            return t

    def stop_limit_allowed(self, symbol):
        with self.lock:
            self.calls.append(("stop_limit_allowed", symbol))
            return self.stop_limit

    def pending_room(self):
        """As MT5Broker.pending_room: limit_orders minus the orders in the book (anyone's); None: no cap."""
        with self.lock:
            if self.room_errors:
                self.room_errors -= 1
                raise BrokerError("account_info failed: (-10005, 'IPC timeout')")
            return None if self.limit_orders is None else max(0, self.limit_orders - len(self.orders))

    def order_state(self, ticket):
        """As MT5Broker.order_state: one in the book is "placed" (a triggered stop-limit: kind "limit"), one gone what
        became of it (filled, canceled, expired); None when there is no such order. The order's volume_current (what
        is left unfilled after a partial fill) when the test set one, else all of it until it fills."""
        with self.lock:
            self.calls.append(("state", ticket))
            state, o = ("placed", self.orders[ticket]) if ticket in self.orders else self.done.get(ticket, (None, None))
            if o is None:
                return None
            return {"state": state, "type": o.get("type", 4 if o["side"] == "buy" else 5),
                    "kind": o.get("kind", "stop"), "volume_initial": o["lots"],
                    "volume_current": 0.0 if state == "filled" else o.get("volume_current", o["lots"]),
                    "price_open": o["price"], "price_stoplimit": o.get("price_stoplimit", 0.0)}

    def pending_orders(self, symbol=None):
        with self.lock:
            if self.broken:
                raise RuntimeError("terminal disconnected")
            if self.blank:                     # some builds answer orders_get() with None for a moment
                self.blank -= 1
                return []
            return [dict(o) for o in self.orders.values() if o["magic"] == MAGIC and symbol in (None, o["symbol"])]

    def cancel_order(self, ticket):
        with self.lock:
            if ticket not in self.orders:
                self.calls.append(("cancel", ticket, "gone"))
                return False
            if self.frozen.get(ticket, 0) > 0:
                self.frozen[ticket] -= 1
                self.calls.append(("cancel", ticket, "frozen"))
                raise RuntimeError("order rejected (10029): frozen")
            self.done[ticket] = ("canceled", self.orders.pop(ticket))
            self.calls.append(("cancel", ticket, "ok"))
            return True

    def deal_entry(self, ticket):
        with self.lock:
            deals = self._history()
            d = next((d for d in deals if d["order"] == ticket), None) or \
                next((d for d in deals if d["position"] == ticket and d["entry"] == "in"), None)
            return d and {"price": d["price"], "time": d["time"], "lots": d["lots"], "side": d["side"],
                          "position": d["position"], "entry": d["entry"]}

    def position_deals(self, position):
        with self.lock:
            return [{"side": d["side"], "lots": d["lots"], "price": d["price"], "order": d["order"],
                     "mine": d["magic"] == MAGIC} for d in self._history() if d["position"] == position]


# ── harness ────────────────────────────────────────────────────
class SkewLoop(asyncio.SelectorEventLoop):
    """loop.time() + skew: pushing skew forward makes hold / expiry times pass at once."""
    skew = 0.0

    def time(self):
        return super().time() + self.skew


class TConnector(Connector):
    def _map_file(self):                          # never write tickets.json into the project; one per connector, as
        return TMP / f"tickets-{id(self)}.json"   # every SimBroker numbers its tickets from 5001 again

    def _outbox_file(self):                       # nor outbox.json; a new file per connector (an id can be reused)
        if "_box" not in self.__dict__:
            self._box = TMP / f"outbox-{next(BOXES)}.json"
        return self._box


class Harness:
    def __init__(self, broker, allow_real=False, dry_run=False, settings=None):
        self.b, self.msgs, self.at_trigger = broker, [], []
        self.c = TConnector("http://127.0.0.1:9", "tok", broker, allow_real, dry_run)
        self.c.send = self.send
        self.c.apply_settings(dict(settings or SETTINGS))

    async def send(self, msg, keep=False):
        self.msgs.append(json.loads(json.dumps(msg, default=str)))
        if msg.get("status") == "triggered" and isinstance(self.b, SimBroker):   # what the book held at that moment
            with self.b.lock:
                self.at_trigger.append(sorted(o["side"] for o in self.b.orders.values()))

    def of(self, kind):
        return [m for m in self.msgs if m["type"] == kind]

    def updates(self, bid):           # the status reports (an analysis comes in a burst_update with no status)
        return [m for m in self.of("burst_update") if m["id"] == bid and "status" in m]

    def analyses(self, bid):
        return [m["analysis"] for m in self.of("burst_update") if m["id"] == bid and "analysis" in m]

    def statuses(self, bid):
        return [m["status"] for m in self.updates(bid)]

    def final(self, bid):
        ups = [m for m in self.updates(bid) if m["status"] in ("closed", "expired", "skipped", "cancelled", "error")]
        return ups[-1] if ups else None

    def logs(self, level=None):
        return [m["message"] for m in self.of("log") if level in (None, m["level"])]

    def close(self):
        for st in list(self.c.bursts.values()):
            st.get("task") and st["task"].cancel()
        self.c.mt5.shutdown(wait=False)


def plan(bid, test=False, seconds_until=0.5, **params):
    p = {"trades": 5, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0, "tp_start_pips": 4.0,
         "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "risk_pct": 1.0, "max_spread_pips": 3.0}
    p.update(params)
    return {"id": bid, "symbol": "EURUSD", "event_title": "Test event", "currency": "USD",
            "event_time": "2026-10-28T18:00:00Z", "seconds_until": seconds_until, "test": test, "params": p}


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def near(a, b, eps=1e-6):
    return a is not None and b is not None and abs(a - b) < eps


async def until(cond, what, timeout=5.0):
    end = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > end:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.005)


async def jump(seconds):
    asyncio.get_running_loop().skew += seconds
    await asyncio.sleep(0.03)


async def armed(h, bid):
    await until(lambda: "armed" in h.statuses(bid) or h.final(bid), f"burst #{bid} armed")
    check("armed" in h.statuses(bid), f"burst #{bid} not armed: {h.final(bid)}")


async def finished(h, bid, timeout=5.0):
    await until(lambda: h.final(bid) is not None, f"burst #{bid} final report", timeout)
    return h.final(bid)


TESTS = []


def scenario(fn):
    TESTS.append(fn)
    return fn


# ── pure helpers ───────────────────────────────────────────────
@scenario
def burst_lots_edges():
    s = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0)               # min 0.01, step 0.01, max 100
    check(burst_lots(10_000, 1, 10, 10, s) == (10, 0.1), "1% of 10k over 10 tickets at 10 pips = 0.10 each")
    check(burst_lots(10_000, 1, 10, 40, s) == (40, 0.02), "0.025 rounds DOWN to 0.02")
    check(burst_lots(1_000, 1, 10, 20, s) == (10, 0.01), "under the minimum lot: n shrinks to 10 × 0.01")
    check(burst_lots(500, 1, 10, 20, s) == (5, 0.01), "n shrinks to 5")
    check(burst_lots(50, 1, 10, 5, s) == (0, 0.0), "even one minimum lot risks too much")
    check(burst_lots(1e7, 5, 2, 1, s) == (1, 100.0), "capped at volume_max")
    check(burst_lots(0, 1, 10, 5, s) == (0, 0.0) and burst_lots(10_000, 1, 10, 0, s) == (0, 0.0), "no balance / 0 trades")
    B, S = (lambda lots: {"side": "buy", "lots": lots}), (lambda lots: {"side": "sell", "lots": lots})
    nets = [(position_net(d)["side"], position_net(d)["lots"], position_net(d)["entered"], position_net(d)["flips"],
             position_net(d)["first_entered"]) for d in
            ([], [B(0.5), B(0.5), S(0.2)], [B(0.4), S(1.0)], [B(0.4), S(0.4), S(0.6)], [B(0.4), S(1.0), S(0.5), B(2.0)],
             [B(0.5), B(0.5), S(0.5), S(0.5)])]
    check(nets == [(None, 0.0, 0.0, 0, 0.0), ("buy", 0.8, 1.0, 0, 1.0), ("sell", 0.6, 0.6, 1, 0.4),
                   ("sell", 0.6, 0.6, 1, 0.4), ("buy", 0.9, 0.9, 2, 0.4), (None, 0.0, 1.0, 0, 1.0)],
          f"position_net: adds, reduces, reverses (one in/out deal or out + in), twice, comes and goes: {nets}")
    P = (lambda side, lots, px: {"side": side, "lots": lots, "price": px})
    lasts = [(n["side"], n["last_side"], n["entered"], n["entry"], n["first_side"], n["first_entry"])
             for n in map(position_net, (
                 [P("buy", 0.5, 1.1), P("buy", 0.5, 1.2), P("sell", 0.2, 1.3)],
                 [P("buy", 0.4, 1.1), P("sell", 1.0, 1.2)],
                 [P("buy", 0.4, 1.1), P("sell", 1.0, 1.2), P("buy", 0.6, 1.3)],
                 [P("buy", 0.5, 1.1), P("sell", 0.5, 1.2), P("sell", 0.3, 1.3), P("buy", 0.3, 1.0)]))]
    check(lasts == [("buy", "buy", 1.0, 1.15, "buy", 1.15), ("sell", "sell", 0.6, 1.2, "buy", 1.1),
                    (None, "sell", 0.6, 1.2, "buy", 1.1), (None, "sell", 0.3, 1.3, "buy", 1.1)],
          f"position_net: the side and average entry of the last entry, and of the first: {lasts}")
    coarse = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, volume_min=0.1, volume_step=0.1)
    check(burst_lots(10_000, 1, 10, 3, coarse) == (3, 0.3), "rounds down to a 0.1 step")
    for bal in (137.0, 999.0, 2_345.0, 51_000.0):               # never more than the budget for one side
        for trades in (1, 7, 13, 40):
            n, lots = burst_lots(bal, 1.3, 12.5, trades, s)
            check(n * lots * 12.5 * 10.0 <= bal * 0.013 + 1e-6, f"over budget: {bal} {trades} -> {n} x {lots}")
    c = clamp_burst({"trades": 999, "lead_seconds": 1, "offset_pips": "x", "hold_seconds": float("nan"),
                     "tp_step_pips": -3})
    check((c["trades"], c["lead_seconds"], c["offset_pips"], c["hold_seconds"], c["tp_step_pips"]) == (40, 10, 7.0, 30, 0),
          f"clamp_burst: {c}")
    parts = split_volume(1.0, SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0, volume_max=0.3))
    check(parts == [0.25, 0.25, 0.25, 0.25], f"split_volume even chunks <= volume_max: {parts}")


@scenario
def run_tags_and_paper_specs():
    made = {("burst", 60): "fxb:60:3", ("burst", 7): "fxb:7:b", ("rapid", 1730000000123): "fxr:1730000000123:0",
            ("scalp", 1730000000456): "fxs:1730000000456"}      # as each runner writes them
    got = {c: run_tag(c) for c in made.values()}
    check(all(run_tag(c) == k for k, c in made.items()), f"each run's comment read back as its kind and id: {got}")
    junk = ["fx:5", "fxb exit", "fxq:1", "fxb:", "fxr:x:1", " fxb:1", "", None]
    check([run_tag(c) for c in junk] == [None] * len(junk), "a signal's tag, another EA's, none at all: not a run's")
    p = PaperBroker()
    gold, eur, jpy = p.symbol_spec("XAUUSD"), p.symbol_spec("EURUSD"), p.symbol_spec("USDJPY")
    check((gold.pip, gold.digits, gold.point, gold.pip_value_per_lot, gold.scale) == (0.1, 2, 0.01, 10.0, 5.0),
          f"paper gold: its pip 0.1, 2 digits, its settings scaled: {gold}")
    check((eur.pip, eur.digits, eur.point, eur.pip_value_per_lot, eur.scale) == (PIP, 5, 0.00001, 10.0, 1.0)
          and (jpy.pip, jpy.digits, jpy.point, jpy.pip_value_per_lot, jpy.scale) == (0.01, 3, 0.001, 6.7, 1.0)
          and p.symbol_spec("GBPJPY.m").scale == 2.0, f"the pairs as before: {eur} {jpy}")
    try:
        p.rates("EURUSD", "M1", 10)
        check(False, "paper mode has no price history")
    except BrokerError as e:
        check(str(e) == "no history in paper mode", f"paper rates: {e}")
    a = MT5Adapter.__new__(MT5Adapter)                  # no MT5Broker (no terminal): one that echoes its call
    a.b = type("Echo", (), {"rates": lambda self, *args: args})()
    check(a.rates("XAUUSD", "M5", 300) == ("XAUUSD", "M5", 300), "MT5Adapter.rates is MT5Broker.rates")


@scenario
def mt5_broker_with_fake_terminal():
    """bot/mt5_broker.py against a fake MetaTrader5 module (real constants, fake calls): no terminal, no orders."""
    import bot.mt5_broker as mb
    real, fx_closed = mb.mt5, mb._fx_closed
    if real is None:
        return print("  (MetaTrader5 package not installed: skipped)")
    sent = []
    fake = SimpleNamespace(**{k: getattr(real, k) for k in dir(real) if k.isupper()})

    def order_send(req):
        sent.append(dict(req))
        if getattr(order_send, "refuse_expiry", False) and req.get("type_time") == real.ORDER_TIME_SPECIFIED:
            return SimpleNamespace(retcode=real.TRADE_RETCODE_INVALID_EXPIRATION, comment="Invalid expiration")
        if req["action"] == real.TRADE_ACTION_PENDING and req["type_filling"] != real.ORDER_FILLING_RETURN:
            return SimpleNamespace(retcode=real.TRADE_RETCODE_INVALID_FILL, comment="Unsupported filling mode")
        if req["action"] == real.TRADE_ACTION_REMOVE:
            code = {7: real.TRADE_RETCODE_DONE, 9: real.TRADE_RETCODE_FROZEN}.get(req["order"], 10013)
            return SimpleNamespace(retcode=code, comment="")
        return SimpleNamespace(retcode=real.TRADE_RETCODE_PLACED if req["action"] == real.TRADE_ACTION_PENDING
                               else real.TRADE_RETCODE_DONE, comment="", order=777)

    order = SimpleNamespace(ticket=9, symbol="EURUSD", type=real.ORDER_TYPE_SELL_STOP, price_open=1.0993, sl=1.1003,
                            tp=0.0, volume_current=1.0, comment="fxb:1:s", magic=MAGIC)
    deal = SimpleNamespace(order=5, position_id=4, entry=real.DEAL_ENTRY_IN, type=real.DEAL_TYPE_BUY, volume=0.5,
                           price=1.1008, time=1_800_000_000)
    fake.order_send = order_send
    fake.last_error = lambda: (1, "Success")
    fake.symbol_select = lambda s, on: True
    fake.symbol_info = lambda s: SimpleNamespace(point=0.00001, digits=5, trade_tick_value=1.0, trade_tick_size=0.00001,
                                                 volume_min=0.01, volume_step=0.01, volume_max=30.0,
                                                 trade_stops_level=5, filling_mode=1)    # FOK only, like MEXAtlantic
    fake.symbol_info_tick = lambda s: SimpleNamespace(bid=1.1, ask=1.10005)
    fake.orders_get = lambda **kw: (order,) if kw.get("ticket") in (None, 9) else ()
    fake.history_deals_get = lambda **kw: (deal,) if kw.get("ticket") == 5 else ()
    fake.account_info = lambda: SimpleNamespace(login=1, server="s", currency="USD", balance=1e4, equity=1e4, margin=0,
                                                margin_free=1e4, leverage=100, trade_mode=real.ACCOUNT_TRADE_MODE_DEMO,
                                                trade_allowed=True, margin_mode=real.ACCOUNT_MARGIN_MODE_RETAIL_NETTING)
    fake.positions_get = lambda: (SimpleNamespace(ticket=4, identifier=4, symbol="EURUSD", type=real.POSITION_TYPE_BUY,
                                                  volume=1.0, price_open=1.1008, sl=1.0998, tp=0.0, price_current=1.1,
                                                  profit=0.0, swap=0.0, time=1_800_000_000, comment="fxb:1:b", magic=MAGIC),
                                  SimpleNamespace(ticket=3, identifier=3, symbol="EURUSD", type=real.POSITION_TYPE_SELL,
                                                  volume=0.1, price_open=1.1, sl=0.0, tp=0.0, price_current=1.1,
                                                  profit=0.0, swap=0.0, time=1_800_000_000, comment="", magic=0))
    mb.mt5 = fake
    try:
        b = mb.MT5Broker(magic=MAGIC)
        check(b.place_pending("EURUSD", "buy", 1.0, 1.1007549, 1.0997549, None, "fxb:1:b") == 777, "returns res.order")
        check([r["type_filling"] for r in sent] == [real.ORDER_FILLING_FOK, real.ORDER_FILLING_RETURN], "10030 retry")
        r = sent[-1]
        check((r["action"], r["type"], r["type_time"], r["price"], r["sl"], r["tp"], r["magic"]) ==
              (real.TRADE_ACTION_PENDING, real.ORDER_TYPE_BUY_STOP, real.ORDER_TIME_GTC, 1.10075, 1.09975, 0.0, MAGIC),
              f"pending request {r}")
        sent.clear()
        b.place_pending("EURUSD", "sell", 1.0, 1.0993, 1.1003, 1.0989, "fxb:1:s")
        check(len(sent) == 1 and sent[0]["type_filling"] == real.ORDER_FILLING_RETURN and
              sent[0]["type"] == real.ORDER_TYPE_SELL_STOP, "working filling mode remembered per symbol")
        check(b.cancel_order(7) is True and b.cancel_order(8) is False, "removed / no longer exists")
        check(sent[-1] == {"action": real.TRADE_ACTION_REMOVE, "order": 8}, "TRADE_ACTION_REMOVE carries 'order'")
        try:
            b.cancel_order(9)
            check(False, "a frozen cancel must raise")
        except mb.BrokerError as e:
            check(e.retcode == real.TRADE_RETCODE_FROZEN, f"retcode on the error: {e.retcode}")
        check(b.pending_orders("EURUSD") == [{"ticket": 9, "symbol": "EURUSD", "side": "sell", "price": 1.0993,
                                              "sl": 1.1003, "tp": None, "lots": 1.0, "comment": "fxb:1:s",
                                              "type": real.ORDER_TYPE_SELL_STOP, "kind": "stop",
                                              "price_stoplimit": 0.0}], "orders")
        d = b.deal_entry(5)
        check(d and d["side"] == "buy" and d["lots"] == 0.5 and d["position"] == 4 and d["entry"] == "in"
              and b.deal_entry(6) is None,
              f"deal_entry {d}")
        history = fake.history_deals_get
        on_4 = [SimpleNamespace(ticket=12, order=8, type=real.DEAL_TYPE_SELL, volume=1.5, price=1.1, time=3, time_msc=3000,
                                magic=0),
                SimpleNamespace(ticket=10, order=5, type=real.DEAL_TYPE_BUY, volume=0.5, price=1.1008, time=1, time_msc=1000,
                                magic=MAGIC),
                SimpleNamespace(ticket=13, order=0, type=real.DEAL_TYPE_BALANCE, volume=0.0, price=0.0, time=4, time_msc=4000,
                                magic=0),
                SimpleNamespace(ticket=11, order=6, type=real.DEAL_TYPE_BUY, volume=0.5, price=1.1009, time=1, time_msc=1500,
                                magic=MAGIC)]
        fake.history_deals_get = lambda **kw: tuple(on_4) if kw.get("position") == 4 else ()
        deals = b.position_deals(4)
        check([(x["side"], x["lots"], x["order"], x["mine"]) for x in deals]
              == [("buy", 0.5, 5, True), ("buy", 0.5, 6, True), ("sell", 1.5, 8, False)]
              and position_net(deals)["side"] == "sell" and b.position_deals(7) == [],
              f"position_deals: trades only, oldest first {deals}")
        fake.history_deals_get = lambda **kw: None
        try:
            fake.last_error = lambda: (-10005, "IPC timeout")
            b.position_deals(4)
            check(False, "a failed history read raises")
        except mb.BrokerError:
            pass
        finally:
            fake.history_deals_get, fake.last_error = history, lambda: (1, "Success")
        check(b.account()["margin_mode"] == "netting", "margin_mode")
        check([p["identifier"] for p in b.positions()] == [4] and len(b.positions(all_magic=True)) == 2, "positions")
        sent.clear()
        b.close_position(b.positions()[0], "fxb exit", volume=0.2)
        check(sent[0]["volume"] == 0.2 and sent[0]["position"] == 4 and sent[0]["type"] == real.ORDER_TYPE_SELL,
              f"partial close {sent[0]}")

        # broker clock: a controllable local clock and a tick stamped on a UTC+tz server (frozen while the market is shut)
        clock, tz, live, stamp, shut = [1_800_010_000.0], [3], [True], [0.0], [False]

        def tick(s):
            if live[0]:
                stamp[0] = clock[0] + tz[0] * 3600 - 2          # a 2 s old tick
            return SimpleNamespace(bid=1.1, ask=1.10005, time_msc=int(stamp[0] * 1000))
        fake.symbol_info_tick = tick
        mb.time, mb._fx_closed = SimpleNamespace(time=lambda: clock[0]), lambda: shut[0]
        utc = lambda h, at=1_800_000_000: mb._ts(at - h * 3600)    # a broker stamp (default: the positions'), in UTC
        b2 = mb.MT5Broker(magic=MAGIC)
        p = b2.positions()[0]
        check(p["open_time"] is None and p["broker_time"] == 1_800_000_000 and b2._offset is None,
              "one look proves nothing: no time until a live tick proves the offset")
        clock[0] += 5
        check(b2.positions()[0]["open_time"] is None and b2._offset is None,
              "one move proves nothing either (it can be a terminal catching up after a reconnect)")
        clock[0] += 5
        check(b2.positions()[0]["open_time"] == utc(3) and b2._offset == 10800 and b2._offset_at == clock[0],
              "a second move in a row, each within 10 min, proves the offset")
        b2._learn_offset("US500", int((clock[0] - 3 * 3600) * 1000))          # first look at a 6 h old tick
        check(b2._offset == 10800, "a single stale look changes nothing")
        live[0], shut[0] = False, True
        clock[0] += 9 * 3600
        check(b2.positions()[0]["open_time"] == utc(3) and b2._offset == 10800, "a frozen weekend tick changes nothing")
        for _ in range(2):
            stamp[0] += 3600
            clock[0] += 5
            b2.tick("EURUSD")
        check(b2.positions()[0]["open_time"] == utc(3) and b2._offset == 10800,
              "while FX is shut, newer but hours-old ticks (terminal reconnect) prove nothing")
        b3 = mb.MT5Broker(magic=MAGIC)
        check(b3.positions()[0]["open_time"] is None and b3._offset is None and b3.closed_result(4) is None,
              "restart on the weekend: no guess from a stale tick")
        shut[0] = False
        b4 = mb.MT5Broker(magic=MAGIC)
        b4._learn_offset("EURUSD", int((clock[0] + 10800) * 1000))
        clock[0] += 19 * 3600
        b4._learn_offset("EURUSD", int((clock[0] + 10800 - 7 * 3600) * 1000))  # newer, but the last look was 19 h ago
        for _ in range(3):
            b4._learn_offset("XAUUSD", int((clock[0] - 13 * 3600) * 1000))     # live, but UTC-13 is no time zone
            clock[0] += 5
        check(b4._offset is None, "no proof after a 19 h gap; an offset outside -12h..+14h is ignored")
        live[0], tz[0] = True, 2
        b2.tick("EURUSD")                                      # first look after 19 h, then one move: no proof
        clock[0] += 5
        b2.tick("EURUSD")
        check(b2._offset == 10800, "the first looks after a long gap prove nothing")
        clock[0] += 5
        check(b2.positions()[0]["open_time"] == utc(3) and b2._offset == 7200 and b2.deal_entry(5)["time"] == utc(3)
              and b2._utc(int(stamp[0])) == utc(2, int(stamp[0])),
              "DST switch: picked up from the next live tick; stamps made before it keep their old offset")

        # broker-side expiry: SPECIFIED on the broker's clock, GTC fallback once it is refused
        fake.symbol_info = lambda s: SimpleNamespace(point=0.00001, digits=5, trade_tick_value=1.0, trade_tick_size=0.00001,
                                                     volume_min=0.01, volume_step=0.01, volume_max=30.0,
                                                     trade_stops_level=5, filling_mode=1, expiration_mode=15)
        sent.clear()
        b2.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:2:b", expire_in=210)
        check(sent[-1]["type_time"] == real.ORDER_TIME_SPECIFIED and sent[-1]["expiration"] == int(clock[0] + 7200 + 210),
              f"expiration on the server clock {sent[-1]}")
        stamp[0], live[0] = clock[0] + 7200 + 240, False      # the PC clock runs 4 min slow
        sent.clear()
        b2.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:2:b", expire_in=210)
        check(b2._offset == 7200 and sent[-1]["expiration"] == int(stamp[0] + 210),
              f"a slow PC clock does not pull the expiry earlier: {sent[-1]}")
        b5, tz[0], live[0] = mb.MT5Broker(magic=MAGIC), 3, True
        for _ in range(3):
            b5.tick("EURUSD")                                  # proven UTC+3 ...
            clock[0] += 5
        clock[0] += 60 * 3600
        tz[0] = 2
        b5.tick("EURUSD")                                      # ... then the weekend's DST switch; one look (the quote)
        live[0] = False
        sent.clear()
        b5.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:3:b", expire_in=270)
        check(b5._offset == 10800 and sent[-1]["expiration"] == int(stamp[0] + 270),
              f"an offset an hour out of date (DST) does not delay the expiry: {sent[-1]}")
        order_send.refuse_expiry = True
        sent.clear()
        b2.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:2:b", expire_in=210)
        check([x["type_time"] for x in sent] == [real.ORDER_TIME_SPECIFIED, real.ORDER_TIME_GTC]
              and "expiration" not in sent[-1], f"10022 -> GTC {sent}")
        sent.clear()
        b2.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:2:b", expire_in=210)
        check(len(sent) == 1 and sent[0]["type_time"] == real.ORDER_TIME_GTC, "GTC remembered per symbol")

        # the real calendar: the FX week follows New York (Fri 17:00 - Sun 17:00), brokers switch DST on that weekend
        at = lambda *a: datetime(*a, tzinfo=timezone.utc).timestamp()
        check([fx_closed(at(*a)) for a in ((2026, 10, 30, 21, 30), (2026, 11, 6, 21, 30), (2026, 11, 8, 21, 30),
                                            (2027, 3, 14, 21, 30), (2026, 12, 25, 12), (2026, 10, 28, 0, 30))]
              == [True, False, True, False, True, False], "FX closed: Friday/Sunday 21:00 or 22:00 UTC with US DST")
        check(mb._fx_last_close(at(2026, 11, 2, 8)) == at(2026, 10, 30, 21)
              and mb._fx_last_close(at(2026, 11, 9)) == at(2026, 11, 6, 22), "last Friday 17:00 New York, in UTC")

        def prove(b, n=3):
            for _ in range(n):
                b.tick("EURUSD")
                clock[0] += 5
        live[0] = True
        b6, tz[0], clock[0] = mb.MT5Broker(magic=MAGIC), 3, at(2026, 10, 30, 12)
        prove(b6)
        clock[0], tz[0] = at(2026, 11, 2, 8) + 0.05, 2              # Monday after MEX's switch to UTC+2
        filled = int(at(2026, 11, 2, 8) + 7200)                     # first trade, same second as the quote
        b6.tick("EURUSD")
        check(b6._utc(filled) is None and b6._utc(int(at(2026, 10, 30, 20, 59) + 10800)) == mb._ts(at(2026, 10, 30, 20, 59)),
              "a stamp made since the weekend's close waits for a new proof; Friday's stamps keep UTC+3")
        clock[0] += 5
        prove(b6, 2)
        check(b6._offset == 7200 and b6._utc(filled) == mb._ts(at(2026, 11, 2, 8))
              and b6._utc(int(at(2026, 10, 30, 20, 59) + 10800)) == mb._ts(at(2026, 10, 30, 20, 59)),
              "fall-back: the new offset covers everything since Friday's close, the fill before its proof included")
        b7, tz[0], clock[0] = mb.MT5Broker(magic=MAGIC), 2, at(2027, 3, 12, 12)
        prove(b7)
        clock[0], tz[0] = at(2027, 3, 14, 21, 0, 3), 3              # US DST: FX reopens Sunday 21:00 UTC
        gap_stop = int(at(2027, 3, 14, 21, 0, 1) + 10800)
        check(fx_closed(clock[0]) is False and b7._utc(gap_stop) is None, "spring: a gap stop-out before the proof waits")
        prove(b7)
        check(b7._offset == 10800 and b7._utc(gap_stop) == mb._ts(at(2027, 3, 14, 21, 0, 1)),
              "spring: the reopen hour converts with the new offset")
        cached = int((clock[0] + 10800 - 7200) * 1000)              # gold's last tick before its daily break
        caught = int((clock[0] + 10800 - 2400) * 1000)              # the one a reconnect delivers: newer, 40 min old
        for g in (cached, cached, caught, caught):
            b7._learn_offset("XAUUSD", g)
            clock[0] += 5
        check(b7._offset == 10800 and len(b7._offsets) == 2,
              "a reconnect delivering one newer, 40 min old tick is no proof (it would read UTC+2.5)")
        b8 = mb.MT5Broker(magic=MAGIC)
        for _ in range(3):                                             # each poll looks twice: positions(), then a close
            g = int((clock[0] + 10800) * 1000)
            b8._learn_offset("EURUSD", g)
            clock[0] += 0.3
            b8._learn_offset("EURUSD", g)
            clock[0] += 300
        check(b8._offset == 10800, "a second look at the same stamp in the same poll keeps the chain (live.py, 300 s polls)")
        b9, g = mb.MT5Broker(magic=MAGIC), int((clock[0] + 10800) * 1000)
        for s in (g, g + 5000, g + 5000, g + 10000):                   # the middle stamp seen again 5 s later: a stall
            b9._learn_offset("XAUUSD", s)
            clock[0] += 5
        check(b9._offset is None, "the same stamp seen again seconds later is a stall and still breaks the chain")
    finally:
        mb.mt5, mb.time, mb._fx_closed = real, time, fx_closed


# ── hedging ────────────────────────────────────────────────────
@scenario
async def hedging_straddle_oco_tp_hold():
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(1))
    await armed(h, 1)
    places = b.calls_of("place")
    check(len(places) == 10, f"2N = 10 orders, got {len(places)}")
    check([c[1] for c in places] == ["buy", "sell"] * 5, "interleaved b0, s0, b1, s1 ...")
    check([c[6] for c in places] == [f"fxb:1:{i // 2}" for i in range(10)], f"comments {[c[6] for c in places]}")
    check(all(near(c[2], 0.2) for c in places), "1% of 10k at 10 pips = 1.0 lot split over 5 tickets = 0.2 each")
    for i in range(5):
        buy, sell = places[2 * i], places[2 * i + 1]
        d = (4.0 + 1.5 * i) * PIP
        check(near(buy[3], 1.10075) and near(sell[3], 1.09930), "buy-stop = ask + 7 pips, sell-stop = bid - 7 pips")
        check(near(buy[4], 1.09975) and near(sell[4], 1.10030), "same SL 10 pips from every order")
        check(near(buy[5], round(1.10075 + d, 5)) and near(sell[5], round(1.09930 - d, 5)), f"staggered TP {i}")
    armed_msg = h.updates(1)[-1]
    check(armed_msg["orders"] == 10 and armed_msg["fills"] == 0, f"armed update {armed_msg}")
    check(set(b.orders) <= set(h.c.burst_owned), "every order is in burst_owned")
    check(len(b.expire_ins) == 10 and all(200 < e <= 210.5 for e in b.expire_ins),
          f"broker-side expiry = release + expire + 120 s from now: {b.expire_ins}")

    b.move(1.10080)                                   # ask 1.10085: the 5 buy-stops fill
    await until(lambda: "triggered" in h.statuses(1), "triggered")
    check(h.at_trigger[0] == [], f"OCO: sell-stops cancelled before 'triggered' was reported: {h.at_trigger}")
    check(h.updates(1)[-1]["side"] == "buy", "side buy")
    await until(lambda: len(h.of("trade_opened")) == 5, "5 trade_opened")
    opened = h.of("trade_opened")
    check(all(t["trade"]["burst_id"] == 1 and "signal_id" not in t["trade"] for t in opened), "trade_opened burst_id")
    check(sorted(t["trade"]["tp"] for t in opened) == [1.10115, 1.1013, 1.10145, 1.1016, 1.10175], "TPs reported")
    check(all(near(t["trade"]["lots"], 0.2) and near(t["trade"]["entry_price"], 1.10085) for t in opened), "fills")

    b.move(1.10120)                                   # TP of ticket 0 hits at the broker
    await until(lambda: len(h.of("trade_closed")) == 1, "TP close reported")
    check(near(h.of("trade_closed")[0]["trade"]["pnl"], 7.0), f"3.5 pips x 0.2 lot = 7.0: {h.of('trade_closed')}")
    check(h.final(1) is None, "still running while 4 positions are open")
    await jump(30.5)                                  # hold time: the connector closes the other 4
    fin = await finished(h, 1)
    check(len(b.calls_of("close")) == 4 and all(c[2] is None for c in b.calls_of("close")), "4 full hold closes")
    closed = h.of("trade_closed")
    check(len(closed) == 5 and len({t["trade"]["ticket"] for t in closed}) == 5, "one trade_closed per position")
    check(h.statuses(1) == ["scheduled", "armed", "triggered", "closed"], f"sequence {h.statuses(1)}")
    check(fin["side"] == "buy" and fin["orders"] == 10 and fin["fills"] == 5, f"final {fin}")
    check(near(fin["pnl"], round(sum(t["trade"]["pnl"] for t in closed), 2), 0.011), f"pnl {fin}")
    check(not b.orders and not b.pos and not h.c.bursts and 1 in h.c.burst_done, "nothing left behind")
    h.close()


@scenario
async def hedging_flash_fill_and_tp_between_polls():
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(2))
    await armed(h, 2)
    b.move(1.10200)                                   # fills AND every TP in one tick: positions never seen open
    fin = await finished(h, 2)
    check(len(h.of("trade_opened")) == 5 and len(h.of("trade_closed")) == 5, "fills found through deal_entry")
    check(fin["status"] == "closed" and fin["fills"] == 5 and fin["side"] == "buy", f"final {fin}")
    check(near(fin["pnl"], -5.0, 0.011), f"0.5 pip slippage x 5 x 0.2 lot: {fin}")
    check(not b.orders, "sell side cancelled")
    h.close()

    # ticket 0 fills and hits its TP between two polls, its deals in history only 0.4 s later, while ticket 1 is
    # open: asked for until they show, then reported with its P&L
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(101, trades=2))
    await armed(h, 101)
    t0 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:101:0" and o["side"] == "buy")
    with b.lock:
        b.move(1.10120)                               # both buy-stops fill; ticket 0's TP (1.10115) at once
        for d in [d for d in b.deals if d["position"] == t0]:
            b.hide(d["order"], 0.4)
    await until(lambda: h.of("trade_closed"), "the flash fill reported", 2.0)
    closed = h.of("trade_closed")
    check([m["trade"]["ticket"] for m in closed] == [t0] and closed[0]["trade"]["pnl"] is not None
          and t0 in [m["trade"]["ticket"] for m in h.of("trade_opened")], f"ticket 0 opened and closed: {closed}")
    await jump(30.5)
    fin = await finished(h, 101)
    check(fin["status"] == "closed" and fin["fills"] == 2 and len(h.of("trade_closed")) == 2 and not b.pos,
          f"ticket 1 at hold time: {fin} {h.of('trade_closed')}")
    h.close()


@scenario
async def hedging_whipsaw_frozen_cancel():
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(3))
    await armed(h, 3)
    for t, o in list(b.orders.items()):
        if o["side"] == "sell":
            b.frozen[t] = 10 ** 6                      # inside the freeze level: every cancel fails
    b.move(1.10080)
    await until(lambda: "triggered" in h.statuses(3), "triggered buy")
    await asyncio.sleep(0.1)
    check(len(b.calls_of("cancel")) > 5, "frozen cancels are retried on the next polls")
    check(h.final(3) is None, "a failed cancel is never fatal")
    b.move(1.09920)                                   # the spike reverses: buys stopped out, sells fill
    await until(lambda: h.updates(3)[-1].get("side") == "both", "side both")
    await jump(30.5)
    fin = await finished(h, 3)
    check(fin["status"] == "closed" and fin["side"] == "both" and fin["fills"] == 10, f"final {fin}")
    check(len(h.of("trade_closed")) == 10 and not b.pos and not b.orders, "everything closed")
    h.close()


@scenario
async def hedging_fill_while_placing():
    """A short lead time: the spike comes while the straddle is still being placed."""
    b = SimBroker("hedging")
    h = Harness(b)
    place, placed = b.place_pending, []

    def place_then_spike(*a, **kw):
        placed.append(place(*a, **kw))
        if len(placed) == 4:                          # b0 s0 b1 s1 are in: the release
            b.move(1.10080)
        return placed[-1]
    b.place_pending = place_then_spike
    await h.c.on_burst(plan(15))
    await armed(h, 15)
    check(len(placed) == 4 and h.updates(15)[-1]["orders"] == 4, f"placing stopped at the fill: {len(placed)}")
    await until(lambda: "triggered" in h.statuses(15), "triggered")
    check(h.at_trigger[0] == [], f"OCO: both sell-stops cancelled at once: {h.at_trigger}")
    b.move(1.09900)                                   # the retrace finds no sell-stop to fill (buys stopped out)
    await jump(30.5)
    fin = await finished(h, 15)
    check(fin["status"] == "closed" and fin["side"] == "buy" and fin["fills"] == 2, f"no whipsaw: {fin}")
    check(any("filled while placing" in m for m in h.logs("warn")) and not b.orders and not b.pos, "reported, clean")
    h.close()


@scenario
async def hedging_expiry_without_fill():
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(4))
    await armed(h, 4)
    await asyncio.sleep(0.05)
    check(len(b.orders) == 10, "orders wait for the release")
    await jump(91)                                    # release + expire_seconds
    fin = await finished(h, 4)
    check(fin["status"] == "expired" and fin["fills"] == 0 and fin["pnl"] is None, f"final {fin}")
    check(not b.orders and len([c for c in b.calls_of("cancel") if c[2] == "ok"]) == 10, "all 10 cancelled")
    check(h.statuses(4) == ["scheduled", "armed", "expired"], f"sequence {h.statuses(4)}")
    h.close()


@scenario
async def cancel_waiting_and_armed_and_duplicates():
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(5, seconds_until=1000))   # waits ~940 s for its lead time
    await h.c.on_burst(plan(5, seconds_until=998))    # resent plan: answered "scheduled" again, no second runner
    await asyncio.sleep(0.05)
    check(h.statuses(5) == ["scheduled", "scheduled"] and len(h.c.bursts) == 1, f"duplicates {h.statuses(5)}")
    check(not h.c.burst_running("EURUSD"), "a waiting burst does not block signals")
    await h.c.on_burst_cancel(5)
    fin = await finished(h, 5)
    check(fin["status"] == "cancelled" and not b.calls_of("place"), "cancelled before placing anything")
    await h.c.on_burst(plan(5))                       # finished ids are ignored
    await asyncio.sleep(0.05)
    check(len(h.updates(5)) == 3 and not h.c.bursts, "a finished burst never restarts")

    await h.c.on_burst(plan(6))
    await armed(h, 6)
    check(h.c.burst_running("EURUSD"), "an armed burst owns the symbol")
    await h.c.on_burst_cancel(6)
    fin = await finished(h, 6)
    check(fin["status"] == "cancelled" and not b.orders and fin["orders"] == 10, f"cancel armed: {fin}")
    await h.c.on_burst_cancel(77)                     # unknown here (e.g. after a restart)
    check(h.updates(77) and h.updates(77)[0]["status"] == "cancelled", "unknown burst answered cancelled")
    h.close()


# ── netting ────────────────────────────────────────────────────
@scenario
async def netting_slices_from_real_entry():
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(10))
    await armed(h, 10)
    places = b.calls_of("place")
    check([(c[1], c[2], c[5], c[6]) for c in places] == [("buy", 1.0, None, "fxb:10:b"), ("sell", 1.0, None, "fxb:10:s")],
          f"two orders of n x lots, no TP: {places}")
    check(near(places[0][3], 1.10075) and near(places[0][4], 1.09975) and near(places[1][4], 1.10030), "prices / SL")
    b.move(1.10090)                                   # ask 1.10095: fills 2 pips above the order price
    await until(lambda: "triggered" in h.statuses(10), "triggered")
    check(h.at_trigger[0] == [], "OCO: the sell-stop was cancelled first")
    await until(lambda: h.of("trade_opened"), "trade_opened")
    t = h.of("trade_opened")[0]["trade"]
    check(near(t["lots"], 1.0) and near(t["entry_price"], 1.10095) and t["tp"] is None and t["burst_id"] == 10,
          f"one trade for the whole position: {t}")
    pid = t["ticket"]
    b.move(1.10120)                                   # +2.5 pips from the real entry (4.5 from the order price)
    await asyncio.sleep(0.1)
    check(not b.calls_of("close"), "slices count from the real entry, not the order price")
    b.fail_close = 1                                  # the first slice close is rejected once, then retried
    b.move(1.10140)                                   # +4.5: slice 0 (4.0)
    await until(lambda: pid in b.pos and near(b.pos[pid]["lots"], 0.8), "slice 0 closed")
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2], f"retried partial close {b.calls_of('close')}")
    check(any("could not close" in m for m in h.logs("warn")), "failed close reported")
    b.move(1.10170)                                   # +7.5: slices 1 (5.5) and 2 (7.0) in one poll
    await until(lambda: pid in b.pos and near(b.pos[pid]["lots"], 0.4), "slices 1-2 closed")
    check(near(b.calls_of("close")[-1][2], 0.4), "two slices closed with one deal")
    await jump(30.5)                                  # hold time: the remainder
    fin = await finished(h, 10)
    check(b.calls_of("close")[-1][2] is None and not b.pos, "remainder closed at hold time")
    closed = h.of("trade_closed")
    check(len(h.of("trade_opened")) == 1 and len(closed) == 1 and closed[0]["trade"]["ticket"] == pid
          and (closed[0]["trade"].get("burst_id"), closed[0]["trade"].get("lots")) == (10, 1.0),
          f"one trade_opened / trade_closed for the position, the close with its burst: {closed}")
    check(near(closed[0]["trade"]["pnl"], 69.0, 0.011), f"9.0 + 30.0 + 30.0 summed over the deals: {closed}")
    check(fin["status"] == "closed" and fin["fills"] == 5 and fin["orders"] == 2 and near(fin["pnl"], 69.0, 0.011),
          f"final {fin}")
    check(h.statuses(10) == ["scheduled", "armed", "triggered", "closed"], f"sequence {h.statuses(10)}")
    h.close()


@scenario
async def netting_split_orders_frozen_cancel_then_cancel():
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(11))
    await armed(h, 11)
    places = b.calls_of("place")
    check([(c[1], c[2], c[6]) for c in places] == [("buy", 0.5, "fxb:11:b1"), ("sell", 0.5, "fxb:11:s1"),
                                                   ("buy", 0.5, "fxb:11:b2"), ("sell", 0.5, "fxb:11:s2")],
          f"split into orders <= volume_max: {places}")
    s1 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:11:s1")
    b.frozen[s1] = 2                                  # 10029 twice, then the cancel goes through
    b.move(1.10080)                                   # both buy-stops fill and merge into one 1.0 position
    await until(lambda: not any(o["side"] == "sell" for o in b.orders.values()), "both sells cancelled")
    check([c[2] for c in b.calls_of("cancel") if c[1] == s1] == ["frozen", "frozen", "ok"], "frozen cancel retried")
    await until(lambda: h.of("trade_opened"), "trade_opened")
    check(near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0) and len(b.pos) == 1, "fills merged into one position")
    await h.c.on_burst_cancel(11)                     # positions already open still run to their hold time
    await asyncio.sleep(0.1)
    check(h.final(11) is None and len(b.pos) == 1, "cancel keeps the open position until hold time")
    await jump(30.5)
    fin = await finished(h, 11)
    check(fin["status"] == "cancelled" and fin["fills"] == 5 and fin["side"] == "buy" and fin["orders"] == 4,
          f"final {fin}")
    check(not b.pos and not b.orders and len(h.of("trade_closed")) == 1, "closed and reported")
    h.close()


@scenario
async def netting_blank_order_book_read():
    """One empty orders_get() read at the moment of the fill must not lose the other side: the OCO still cancels it."""
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(16))
    await armed(h, 16)
    with b.lock:
        b.blank = 1
        b.move(1.10080)                               # the buy-stop fills while the next read of the book is empty
    await until(lambda: not any(o["side"] == "sell" for o in b.orders.values()), "sell-stop cancelled after the bad read")
    b.move(1.09900)                                   # so the reversal cannot fill it
    await jump(30.5)
    fin = await finished(h, 16)
    check(fin["status"] == "closed" and fin["side"] == "buy" and not b.orders and not b.pos, f"final {fin}")
    h.close()


@scenario
async def netting_skips_when_a_position_is_open():
    for bid, magic, comment in ((12, MAGIC, "fx:5"), (13, 0, "manual"), (17, MAGIC, "fxb:98:b")):  # 98: an orphan
        b = SimBroker("netting")
        b.add_position("EURUSD", "buy", 0.1, comment, magic=magic)
        h = Harness(b)
        await h.c.on_burst(plan(bid))
        fin = await finished(h, bid)
        check(fin["status"] == "skipped" and "netting account" in fin["reason"] and "EURUSD" in fin["reason"],
              f"{comment}: {fin}")
        check(not b.calls_of("place") and h.logs("warn"), "nothing placed, reported as a warning")
        h.close()
    b = SimBroker("hedging")                          # hedging keeps positions apart: no skip
    b.add_position("EURUSD", "buy", 0.1, "fx:5")
    h = Harness(b)
    await h.c.on_burst(plan(14))
    await armed(h, 14)
    await h.c.on_burst_cancel(14)
    await finished(h, 14)
    h.close()


# ── both modes ─────────────────────────────────────────────────
@scenario
async def skips():
    cases = [
        ("exchange", dict(broker=SimBroker("exchange")), "exchange accounts are not supported"),
        ("real", dict(broker=SimBroker(demo=False)), "real account locked"),
        ("test on real", dict(broker=SimBroker(demo=False), allow_real=True, test=True), "demo accounts only"),
        ("algo off", dict(broker=SimBroker("netting"), algo=False), "Algo Trading is off"),
        ("spread", dict(broker=SimBroker(spread_pips=5)), "spread 5.0 pips > 3.0"),
        ("min lot", dict(broker=SimBroker("netting", balance=50)), "risk budget too small"),
        ("stops level", dict(broker=SimBroker(stops_pips=8)), "closer than the broker allows"),
        ("daily loss", dict(broker=SimBroker(), halted=True), "daily loss limit"),
    ]
    for bid, (name, kw, reason) in enumerate(cases, start=20):
        b = kw["broker"]
        b.algo = kw.get("algo", True)
        h = Harness(b, allow_real=kw.get("allow_real", False))
        if kw.get("halted"):                          # today started at 20k equity: 10k is far past the 3% limit
            h.c.risk.update_day(datetime.now(timezone.utc).date(), 20_000)
        await h.c.on_burst(plan(bid, test=kw.get("test", False)))
        fin = await finished(h, bid)
        check(fin["status"] == "skipped" and reason in (fin["reason"] or ""), f"{name}: {fin}")
        check(not b.calls_of("place") and any(reason in m for m in h.logs("warn")), f"{name}: placed / not reported")
        h.close()
    b = SimBroker(demo=False)                         # real account with allow_real_account = yes: runs
    h = Harness(b, allow_real=True)
    await h.c.on_burst(plan(30))
    await armed(h, 30)
    await h.c.on_burst_cancel(30)
    await finished(h, 30)
    h.close()
    b = SimBroker("netting")
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(31))
    fin = await finished(h, 31)
    check(fin["status"] == "skipped" and fin["reason"] == "dry-run" and not b.calls_of("place"), f"dry-run {fin}")
    check(any(m.startswith("[dry-run] news burst #31 would place 2 stop orders") for m in h.logs()), "dry-run report")
    h = Harness(PaperBroker())
    await h.c.on_burst(plan(32))
    check(h.statuses(32) == ["skipped"] and "paper mode" in h.updates(32)[0]["reason"], f"paper {h.updates(32)}")
    h.close()


@scenario
async def crash_cleans_up_and_reports_error():
    b = SimBroker("hedging")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    await h.c.on_burst(plan(40))
    await armed(h, 40)
    b.broken = True                                   # the terminal stops answering
    fin = await finished(h, 40)
    check(fin["status"] == "error" and "terminal disconnected" in fin["reason"], f"final {fin}")
    check(not b.orders, "pending orders cancelled in the finally block")
    h.close()
    b = SimBroker("netting")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    await h.c.on_burst(plan(41))
    await armed(h, 41)
    b.move(1.10080)
    await until(lambda: h.of("trade_opened"), "fill")
    b.broken = True
    fin = await finished(h, 41)
    check(fin["status"] == "error" and not b.pos and len(h.of("trade_closed")) == 1, f"position closed: {fin}")
    check(41 in h.c.burst_done and not h.c.bursts and any("failed" in m for m in h.logs("error")), "reported")
    h.close()


@scenario
async def shutdown_ends_running_bursts_first():
    """The Forex Bot app's stop (stdin closed) / Ctrl+C: every running burst cleans up and reports, then run() ends."""
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(60))
    await armed(h, 60)
    b.move(1.10080)
    await until(lambda: h.of("trade_opened"), "fill")
    main = asyncio.ensure_future(asyncio.sleep(3600))     # stands in for run()
    await h.c.shutdown(main)
    fin = h.final(60)
    check(fin and fin["status"] == "error" and fin["reason"] == "connector stopped", f"final {fin}")
    check(not b.pos and not b.orders and len(h.of("trade_closed")) == 1,
          f"position closed and reported before run() is cancelled: {b.pos} {h.of('trade_closed')}")
    check(not h.c.bursts and 60 in h.c.burst_done, "nothing left running")
    await until(lambda: main.done(), "run() cancelled")
    check(main.cancelled(), "run() cancelled after the bursts")
    h.close()
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(61))
    await armed(h, 61)
    await h.c.on_burst(plan(62, seconds_until=600))        # scheduled, not armed yet
    await until(lambda: "scheduled" in h.statuses(62), "62 scheduled")
    main = asyncio.ensure_future(asyncio.sleep(3600))
    await h.c.shutdown(main)
    fin = h.final(61)
    check(fin and fin["status"] == "error" and fin["reason"] == "connector stopped", f"final 61: {fin}")
    # one that was only waiting gets no final status: the server sends its plan again to the next start
    check(h.final(62) is None and h.statuses(62) == ["scheduled"] and 62 not in h.c.burst_done,
          f"62 left to the next start: {h.updates(62)}")
    check(not b.orders and not b.pos and not h.c.bursts, f"pending orders cancelled: {b.orders}")
    check(len(b.calls_of("place")) == 10, "the scheduled burst placed nothing")
    await until(lambda: main.done(), "run() cancelled")
    check(not any("clean-up was over" in m for m in h.logs()), "nothing left behind to report")
    h.close()
    h = Harness(SimBroker("hedging"))                     # no bursts: run() is cancelled at once
    main = asyncio.ensure_future(asyncio.sleep(3600))
    t0 = time.monotonic()
    await asyncio.gather(h.c.shutdown(main), h.c.shutdown(main))      # a second request (Ctrl+C, stdin) does nothing
    await until(lambda: main.done(), "run() cancelled")
    check(time.monotonic() - t0 < 2, f"no bursts: stopped at once ({time.monotonic() - t0:.1f} s)")
    h.close()


@scenario
async def shutdown_sweeps_what_a_refused_close_left():
    """One refused close in a burst's clean-up must not leave its other positions open, and the stop sweeps the
    one refused (each reported once); what cannot be removed in time is reported, and run() still ends."""
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(63))
    await armed(h, 63)
    b.move(1.10080)                                   # the 5 buy-stops fill
    await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
    b.fail_close = 1                                  # the clean-up's first close is refused
    seen, sweep_at_stop = [], h.c._sweep_at_stop

    async def counted(until, *a):
        with b.lock:
            seen.append(len(b.pos))
        await sweep_at_stop(until, *a)
    h.c._sweep_at_stop = counted
    main = asyncio.ensure_future(asyncio.sleep(3600))
    await h.c.shutdown(main)
    check(seen == [1], f"the clean-up closed the other 4 itself, only the refused one is left to the sweep: {seen}")
    closes = [c for c in b.calls_of("close")]
    tickets = [m["trade"]["ticket"] for m in h.of("trade_closed")]
    check(not b.pos and not b.orders, f"every position closed: {b.pos} {b.orders}")
    check(len(tickets) == 5 and len(set(tickets)) == 5, f"each reported once: {tickets}")
    check(len(closes) == 6, f"4 closed in the clean-up, the refused one by the stop's sweep: {closes}")
    check(not h.c.unreported and not h.c.unchecked, "nothing left to report")
    check(not any("clean-up was over" in m for m in h.logs()), "nothing left behind")
    await until(lambda: main.done(), "run() cancelled")
    h.close()

    b = SimBroker("hedging")                          # the broker refuses every close: reported when the time is up
    h = Harness(b)
    await h.c.on_burst(plan(64))
    await armed(h, 64)
    b.move(1.10080)
    await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
    b.fail_close = 10 ** 6
    main = asyncio.ensure_future(asyncio.sleep(3600))
    t0 = time.monotonic()
    await h.c.shutdown(main, budget=9)
    took = time.monotonic() - t0
    check(took < 10, f"the stop keeps to its time: {took:.1f} s")
    left = [m for m in h.logs("error") if "clean-up was over" in m]
    check(len(left) == 1 and "5 news-burst order(s) / position(s) not removed" in left[0], f"reported: {left}")
    check(not any("Cleaned up after" in m for m in h.logs()), f"sweeps that removed nothing say nothing: {h.logs()}")
    await until(lambda: main.done(), "run() cancelled")
    h.close()


@scenario
async def stop_while_results_are_reported():
    """A result being reported when the stop comes is handed to the sweeper, never lost: the runner cancelled in
    the middle of a report, a read that fails in the clean-up, the stop's time up in the middle of a sweep's report.
    A stop with nothing of a burst to wait for does not wait for MT5."""
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(66))
    await armed(h, 66)
    b.move(1.10080)                                   # the 5 buy-stops fill
    await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
    asked, real = [], b.closed_result

    def no_exit_yet(ticket):                          # the first read shows no exit: report_closed waits to read again
        asked.append(ticket)
        return None if len(asked) == 1 else real(ticket)
    b.closed_result = no_exit_yet
    b.move(1.10120)                                   # the TP of ticket 0 hits at the broker
    await until(lambda: asked, "its close being reported")
    main = asyncio.ensure_future(asyncio.sleep(3600))
    await h.c.shutdown(main)                          # the runner is cancelled in the middle of that report
    closed = {m["trade"]["ticket"]: m["trade"] for m in h.of("trade_closed")}
    check(len(h.of("trade_closed")) == 5 and len(closed) == 5, f"each of the 5 reported once: {h.of('trade_closed')}")
    tp = closed.get(asked[0], {})
    check(near(tp.get("pnl"), 7.0) and tp.get("burst_id") == 66, f"the TP close reported by the stop's sweep: {tp}")
    check(not b.pos and not h.c.unreported and not any("clean-up was over" in m for m in h.logs()), "nothing left")
    await until(lambda: main.done(), "run() cancelled")
    h.close()

    b = SimBroker("hedging")                          # one result read fails in the clean-up
    h = Harness(b)
    await h.c.on_burst(plan(67))
    await armed(h, 67)
    b.move(1.10080)
    await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
    failed, real = [], b.closed_result

    def fails_once(ticket):
        if not failed:
            failed.append(ticket)
            raise RuntimeError("history_deals_get failed: (-10005, 'IPC timeout')")
        return real(ticket)
    b.closed_result = fails_once
    main = asyncio.ensure_future(asyncio.sleep(3600))
    await h.c.shutdown(main)
    tickets = [m["trade"]["ticket"] for m in h.of("trade_closed")]
    check(failed and len(tickets) == 5 and len(set(tickets)) == 5 and failed[0] in tickets,
          f"the other 4 reported by the clean-up, the failed one by the stop's sweep: {tickets}")
    check(not b.pos and not h.c.unreported and not any("clean-up was over" in m for m in h.logs()), "nothing left")
    await until(lambda: main.done(), "run() cancelled")
    h.close()

    b = SimBroker("hedging")                          # the stop's time is up while a sweep reports
    h = Harness(b)
    gone = b.add_position("EURUSD", "sell", 0.1, "fxb:98:0")
    with b.lock:
        b.close_position(b.pos[gone])                 # closed (its SL, say) with no runner following it
    h.c.unreported[gone] = None
    real = b.closed_result

    def stuck(ticket):                                # MT5 stuck in the call past the stop's time
        time.sleep(2.5)
        return real(ticket)
    b.closed_result = stuck
    main = asyncio.ensure_future(asyncio.sleep(3600))
    t0 = time.monotonic()
    await h.c.shutdown(main, budget=5)
    took = time.monotonic() - t0
    left = [m for m in h.logs("error") if "clean-up was over" in m]
    check(took < 6 and not h.of("trade_closed") and gone in h.c.unreported,
          f"its report goes back to the sweeper ({took:.1f} s): {h.of('trade_closed')} {h.c.unreported}")
    check(len(left) == 1 and "still under way" in left[0] and "results not all reported" in left[0]
          and "did not answer" not in left[0], f"reported as it is: {left}")
    await until(lambda: main.done(), "run() cancelled")
    h.close()

    for scheduled in (False, True):                   # no bursts (or one only waiting), MT5 not answering
        b = SimBroker("hedging")
        h = Harness(b)
        if scheduled:
            await h.c.on_burst(plan(68, seconds_until=600))
            await until(lambda: "scheduled" in h.statuses(68), "68 scheduled")
        b.broken = True
        main = asyncio.ensure_future(asyncio.sleep(3600))
        t0 = time.monotonic()
        await h.c.shutdown(main)
        took = time.monotonic() - t0
        check(took < 3 and not any("clean-up was over" in m for m in h.logs()),
              f"one sweep, only logged{' (a burst only waiting leaves nothing)' if scheduled else ''}: "
              f"{took:.1f} s {h.logs()}")
        await until(lambda: main.done(), "run() cancelled")
        h.close()

    b = SimBroker("hedging")                          # no bursts; the sweep cut while it reads an orphan's history
    h = Harness(b)
    orphan = b.add_position("EURUSD", "sell", 0.1, "fxb:98:0")
    deals = b.position_deals

    def stuck_deals(pid):
        time.sleep(3.0)
        return deals(pid)
    b.position_deals = stuck_deals
    main = asyncio.ensure_future(asyncio.sleep(3600))
    await h.c.shutdown(main, budget=5)
    left = [m for m in h.logs("error") if "clean-up was over" in m]
    check(len(left) == 1 and "still under way" in left[0] and orphan in b.pos,
          f"cut before it closed anything: the user is told, the orphan left to the next start: {h.logs()}")
    await until(lambda: main.done(), "run() cancelled")
    h.close()

    b = SimBroker("hedging")                          # no bursts; an orphan's close stuck in MT5 past the stop's time
    h = Harness(b)
    orphan = b.add_position("EURUSD", "sell", 0.1, "fxb:98:0")
    close = b.close_position

    def stuck_close(pos, *a, **kw):
        time.sleep(3.0)
        return close(pos, *a, **kw)
    b.close_position = stuck_close
    main = asyncio.ensure_future(asyncio.sleep(3600))
    await h.c.shutdown(main, budget=5)
    left = [m for m in h.logs("error") if "clean-up was over" in m]
    check(len(left) == 1 and "still under way" in left[0] and "results not all reported" in left[0],
          f"the user is told, not a quiet stop: {h.logs()}")
    check(orphan in h.c.unreported and h.c.orphan_info.get(orphan, {}).get("burst_id") == 98,
          f"handed over before the close was sent: {h.c.unreported} {h.c.orphan_info}")
    await until(lambda: main.done(), "run() cancelled")
    await until(lambda: orphan not in b.pos, "the stuck close goes through", 5.0)
    b.close_position = close
    await h.c.sweep_orphans()                         # the next sweep (or start) reports it, with its burst
    done = [m["trade"] for m in h.of("trade_closed")]
    check(len(done) == 1 and done[0]["ticket"] == orphan and done[0].get("burst_id") == 98 and done[0]["pnl"] is not None,
          f"then reported once: {done}")
    h.close()


@scenario
async def stopping_refuses_new_signals_and_bursts():
    b = SimBroker("hedging")
    h = Harness(b)
    h.c.stopping = True
    await h.c.on_burst(plan(65))
    check(not h.updates(65) and not h.c.bursts, "a plan is not answered: the server keeps it for the next start")
    await h.c.on_signal(signal(b, 1201), manual=True)
    await h.c.on_signal(signal(b, 1202), manual=False)
    skipped = {m["signal_id"]: m["reason"] for m in h.of("signal_skipped")}
    check(skipped == {1201: "the connector is stopping", 1202: "the connector is stopping"} and not b.pos,
          f"no new trade: {skipped}")
    h.close()


@scenario
async def orphan_sweeper():
    b = SimBroker("hedging")
    h = Harness(b)
    h.c.bursts[7] = {"symbol": "EURUSD", "status": "armed"}     # a burst this connector still runs
    orphan_order = b.add_order("EURUSD", "buy", 0.1, 1.102, "fxb:99:0")
    live_order = b.add_order("EURUSD", "buy", 0.1, 1.102, "fxb:7:0")
    manual_order = b.add_order("EURUSD", "buy", 0.1, 1.102, "fxb:98:0", magic=0)
    orphan_pos = b.add_position("EURUSD", "sell", 0.1, "fxb:99:1")
    signal_pos = b.add_position("GBPUSD", "buy", 0.1, "fx:5")
    dry = Harness(b, dry_run=True)
    await dry.c.sweep_orphans()
    check(orphan_order in b.orders and orphan_pos in b.pos and any("[dry-run]" in m for m in dry.logs()), "dry-run")
    dry.close()
    await h.c.sweep_orphans()
    check(orphan_order not in b.orders and orphan_pos not in b.pos, "orphans removed")
    check(live_order in b.orders and manual_order in b.orders and signal_pos in b.pos, "others untouched")
    t = h.of("trade_closed")[0]["trade"] if h.of("trade_closed") else {}
    check(len(h.of("trade_closed")) == 1 and (t["ticket"], t.get("burst_id"), t.get("side"), t.get("lots"))
          == (orphan_pos, 99, "sell", 0.1), f"orphan close reported with its burst {h.of('trade_closed')}")
    check(any("removed 1 pending order(s), closed 1 position(s)" in m for m in h.logs("warn")), "report")
    await h.c.sweep_orphans()
    check(len(h.logs()) == 1, "nothing to do the second time: no report")
    # a close that goes through but raises: reported with its burst once its history shows it closed, once
    raised = b.add_position("EURUSD", "buy", 0.3, "fxb:97:1")
    entry, close = b.pos[raised]["entry_price"], b.close_position

    def through_then_raises(pos, comment="", price=None, volume=None):
        close(pos, comment, price, volume)
        raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
    b.close_position = through_then_raises
    await h.c.sweep_orphans()
    b.close_position = close
    check(raised not in b.pos and len(h.of("trade_closed")) == 1 and raised in h.c.unreported,
          f"not reported on a close that raised: {h.of('trade_closed')}")
    await h.c.sweep_orphans()
    await h.c.sweep_orphans()
    later = [m["trade"] for m in h.of("trade_closed")[1:]]
    check(len(later) == 1 and (later[0]["ticket"], later[0].get("burst_id"), later[0].get("side"),
                               later[0].get("lots")) == (raised, 97, "buy", 0.3)
          and near(later[0].get("entry_price"), entry) and later[0]["pnl"] is not None and raised not in h.c.unreported,
          f"then reported once with its burst: {later}")
    h.close()


@scenario
async def execute_and_snapshot_ignore_bursts():
    b = SimBroker("hedging")
    settings = {**SETTINGS, "max_open_trades": 1}
    h = Harness(b, settings=settings)
    await h.c.on_burst(plan(50))
    await armed(h, 50)
    sig = {"id": 501, "symbol": "EURUSD", "side": "buy", "price": b.ask(), "sl": b.ask() - 20 * PIP,
           "tp": b.ask() + 40 * PIP, "mode": "trend", "status": "open"}
    await h.c.execute(sig)
    check(h.of("signal_skipped") and h.of("signal_skipped")[0]["reason"] == "news burst running on EURUSD",
          f"skipped {h.of('signal_skipped')}")
    b.move(1.10080)
    await until(lambda: len(h.of("trade_opened")) == 5, "5 burst fills")
    gbp = b.ask("GBPUSD")
    await h.c.execute({**sig, "id": 502, "symbol": "GBPUSD", "price": gbp, "sl": gbp - 20 * PIP, "tp": gbp + 40 * PIP})
    opened = [m["trade"] for m in h.of("trade_opened") if m["trade"].get("signal_id") == 502]
    check(opened and opened[0]["symbol"] == "GBPUSD", "burst positions do not count for max_open_trades = 1")
    snap = h.of("snapshot")[-1]
    ids = sorted((p["symbol"], p["burst_id"]) for p in snap["positions"])
    check(ids == [("EURUSD", 50)] * 5 + [("GBPUSD", None)], f"snapshot burst_id {ids}")
    await jump(30.5)
    await finished(h, 50)
    await h.c.push_snapshot()                         # burst tickets left self.known: the runner reported them
    tickets = [m["trade"]["ticket"] for m in h.of("trade_closed")]
    check(len(tickets) == 5 and len(set(tickets)) == 5, f"no duplicate trade_closed from the snapshot: {tickets}")
    h.close()


@scenario
async def execute_rechecks_bursts_before_ordering():
    b = SimBroker("netting")
    h = Harness(b)
    sig = {"id": 601, "symbol": "EURUSD", "side": "buy", "price": b.ask(), "sl": b.ask() - 20 * PIP,
           "tp": b.ask() + 40 * PIP, "mode": "trend", "status": "open"}
    b.add_position("EURUSD", "sell", 0.1, "fxb:98:s")                 # left by a burst this connector no longer runs
    await h.c.execute(sig)
    check(h.of("signal_skipped")[-1]["reason"] == "a news-burst position on EURUSD is still being closed",
          f"orphan: {h.of('signal_skipped')}")
    with b.lock:
        b.pos.clear()
    h.c.bursts[70] = {"symbol": "EURUSD", "status": "scheduled"}       # waiting: does not block the signal yet
    spec = b.symbol_spec

    def burst_wakes(symbol):                          # ... but wakes up while execute() awaits the broker
        h.c.bursts[70]["status"] = "checking"
        return spec(symbol)
    b.symbol_spec = burst_wakes
    await h.c.execute({**sig, "id": 602})
    check(h.of("signal_skipped")[-1]["reason"] == "news burst running on EURUSD" and not b.pos
          and not h.of("trade_opened"), f"re-checked before the order: {h.of('signal_skipped')}")
    h.close()

    # the burst starts its run while execute() reads the positions, one of these there: decided by the positions as
    # read, as in v1.2.0 (a scalp still makes way, the reasons stay); the run is caught by the check before the order
    for comment, reason, read in (("fxb:72:0", "news burst running on EURUSD", True),      # the run's own
                                  ("fxs:123", "news burst running on EURUSD", True),
                                  ("fxb:98:0", "a news-burst position on EURUSD is still being closed", False),
                                  ("fx:9", "already have a position on EURUSD", False)):
        b = SimBroker("hedging")
        h = Harness(b)
        h.c.bursts[72] = {"id": 72, "symbol": "EURUSD", "status": "scheduled"}
        held = b.add_position("EURUSD", "sell", 0.1, comment)
        listed, reads = b.positions, []

        def burst_arms(all_magic=False):
            h.c.bursts[72]["status"] = "armed"
            return listed(all_magic)

        def account():
            reads.append(1)
            return SimBroker.account(b)
        b.positions, b.account = burst_arms, account
        await h.c.execute({**sig, "id": 603})
        scalp = comment.startswith("fxs:")
        if scalp:
            await until(lambda: h.of("trade_closed"), "the scalp's close reported")
        check(h.of("signal_skipped")[-1]["reason"] == reason and bool(reads) == read and not h.of("trade_opened")
              and list(b.pos) == ([] if scalp else [held])
              and [m["trade"]["ticket"] for m in h.of("trade_closed")] == ([held] if scalp else []),
              f"{comment}: {reason} (the account read: {read}; a scalp closed first): {h.of('signal_skipped')} "
              f"{reads} {list(b.pos)}")
        h.close()


@scenario
async def symbol_owners():
    b = SimBroker("hedging")
    b.bids.update(AUDUSD=0.65, EURGBP=0.85, USDCAD=1.36, NZDUSD=0.6, USDJPY=1.5, USDCHF=0.9)
    h = Harness(b)
    c = h.c
    check(c.symbol_owner("EURUSD") is None and c.symbol_owner("EURUSD", b.positions()) is None, "nothing there")
    c.bursts[70] = {"id": 70, "symbol": "EURUSD", "status": "scheduled"}
    check(c.symbol_owner("EURUSD") is None, "a run still waiting holds nothing yet")
    c.bursts[70]["status"] = "armed"
    c.bursts[71] = {"id": 71, "symbol": "USDJPY", "status": "checking", "kind": "rapid"}
    check(c.symbol_owner("EURUSD") == "burst" and c.symbol_owner("USDJPY") == "rapid"
          and c.symbol_owner("GBPUSD") is None, "a run past its wait, by its kind (none given: a news burst)")
    b.add_position("GBPUSD", "buy", 0.1, "fx:5")
    b.add_position("AUDUSD", "buy", 0.1, "")                         # this bot's, no tag: long-term, as before
    b.add_position("EURGBP", "buy", 0.1, "fxs:1730000000456")
    b.add_position("USDCAD", "sell", 0.1, "fxb:99:1")                # left by runs that are gone
    b.add_position("NZDUSD", "sell", 0.1, "fxr:1730000000123:2")
    b.add_position("USDJPY", "buy", 0.1, "fxr:71:0")
    b.add_position("EURUSD", "buy", 0.1, "fx:6")
    mine = b.positions()
    owners = {s: c.symbol_owner(s, mine) for s in ("GBPUSD", "AUDUSD", "EURGBP", "USDCAD", "NZDUSD", "USDJPY",
                                                   "EURUSD", "USDCHF")}
    check(owners == {"GBPUSD": "long", "AUDUSD": "long", "EURGBP": "scalp", "USDCAD": "burst", "NZDUSD": "rapid",
                     "USDJPY": "rapid", "EURUSD": "burst", "USDCHF": None}, f"each owner, a live run first: {owners}")
    check(c.symbol_owner("EURUSD", mine, runs=False) == "long" and c.symbol_owner("USDJPY", mine, runs=False) is None,
          "runs left out: by the positions only (a live run's own position holds nothing)")
    b.add_position("EURGBP", "buy", 0.1, "fx:7")
    b.add_position("USDCAD", "buy", 0.1, "fx:8")
    t = b.add_position("USDCHF", "buy", 0.1, "")                     # known only as a runner's ticket
    c.burst_owned[t] = 98
    mine = b.positions()
    owners = {s: c.symbol_owner(s, mine) for s in ("EURGBP", "USDCAD", "USDCHF")}
    check(owners == {"EURGBP": "scalp", "USDCAD": "long", "USDCHF": "burst"},
          f"a scalp before a long-term one, that before a gone run's; a gone runner's ticket: a news burst's {owners}")
    del c.bursts[70], c.bursts[71]
    check(c.symbol_owner("EURUSD", mine) == "long" and c.symbol_owner("USDJPY", mine) == "rapid",
          "the runs gone: what they left is theirs, a signal's is long-term")
    h.close()


@scenario
async def execute_ignores_rapid_runs():
    b = SimBroker("hedging")
    h = Harness(b, settings={**SETTINGS, "max_open_trades": 1})
    rid = 1730000000123                               # a Rapid run's id: a time in ms
    h.c.bursts[rid] = {"id": rid, "symbol": "GBPUSD", "status": "armed", "kind": "rapid"}
    b.add_position("GBPUSD", "buy", 0.1, f"fxr:{rid}:0")
    await h.c.execute(signal(b, 701))
    opened = [m["trade"] for m in h.of("trade_opened") if m["trade"].get("signal_id") == 701]
    check(opened and opened[0]["symbol"] == "EURUSD", "a Rapid run's position does not count for max_open_trades = 1")
    await h.c.execute(signal(b, 702, "GBPUSD"))
    check(h.of("signal_skipped")[-1]["reason"] == "news burst running on GBPUSD", f"{h.of('signal_skipped')}")
    del h.c.bursts[rid]                               # the run is gone, its position not closed yet
    await h.c.execute(signal(b, 703, "GBPUSD"))
    check(h.of("signal_skipped")[-1]["reason"] == "a news-burst position on GBPUSD is still being closed",
          f"{h.of('signal_skipped')}")
    await h.c.push_snapshot()
    snap = sorted((p["symbol"], p["burst_id"], p["strategy"]) for p in h.of("snapshot")[-1]["positions"])
    # a local Rapid run's id (its start in ms) is no Burst row's: rapid, with no burst_id
    check(snap == [("EURUSD", None, "long"), ("GBPUSD", None, "rapid")], f"snapshot: {snap}")
    h.close()


@scenario
async def one_long_term_position_per_symbol():
    for comment in ("fx:6", ""):                      # another signal's; this bot's with no tag (not a run's either)
        b = SimBroker("hedging")
        h = Harness(b)
        held = b.add_position("EURUSD", "buy", 0.1, comment)
        await h.c.execute(signal(b, 704))
        sk = h.of("signal_skipped")
        check(sk and sk[-1]["reason"] == "already have a position on EURUSD" and list(b.pos) == [held]
              and not h.of("trade_opened"), f"{comment!r}: one long-term position per symbol, even on hedging: {sk}")
        h.close()


@scenario
async def shutdown_sweep_with_rapid_ids():
    b = SimBroker("hedging")
    h = Harness(b)
    rid, bid = 1730000000123, 60                      # a Rapid run's id (ms) and a news burst's, both running
    h.c.bursts[rid] = {"id": rid, "symbol": "GBPUSD", "status": "armed", "kind": "rapid"}
    h.c.bursts[bid] = {"id": bid, "symbol": "EURUSD", "status": "armed"}
    left = [b.add_order("GBPUSD", "buy", 0.1, 1.302, f"fxr:{rid}:0"),
            b.add_position("GBPUSD", "sell", 0.1, f"fxr:{rid}:1"),
            b.add_order("EURUSD", "sell", 0.1, 1.098, f"fxb:{bid}:1"),
            b.add_position("EURUSD", "buy", 0.1, f"fxb:{bid}:0")]
    loop = asyncio.get_running_loop()
    await h.c._sweep_at_stop(loop.time() + 0.2)
    errors = h.logs("error")
    check(len(errors) == 1 and f"news burst(s) #{bid}, #{rid} still ending" in errors[0], f"in order: {errors}")
    check(all(t in b.orders or t in b.pos for t in left), "a running run's tickets are left to its own clean-up")
    h.c.bursts.clear()                                # both gone: what they left is swept, each by its tag
    await h.c._sweep_at_stop(loop.time() + 5)
    closed = sorted(m["trade"].get("burst_id") for m in h.of("trade_closed"))
    check(not b.orders and not b.pos and closed == [bid, rid], f"swept and reported with their run: {closed}")
    check(len(h.logs("error")) == 1, "nothing left to report")
    h.close()


@scenario
async def session_dispatch_and_outbox():
    b = SimBroker("hedging")
    h = Harness(b)
    del h.c.send                                      # the real send(): queued until welcome, then over the link
    opened, closed = {"type": "trade_opened", "trade": {"ticket": 1}}, {"type": "trade_closed", "trade": {"ticket": 1}}
    h.c.outbox = [opened]                             # from the outage
    await h.c.send(closed)                            # a runner after auth, before welcome: must not overtake it

    class FakeWS:
        def __init__(self, msgs):
            self.msgs = [json.dumps(m) for m in msgs]

        async def send(self, raw):
            h.msgs.append(json.loads(raw))

        async def __aiter__(self):
            for m in self.msgs:
                yield m

    welcome = {"type": "welcome", "user": {"email": "t@example.com"}, "settings": SETTINGS, "open_signals": [],
               "bursts": [plan(60, seconds_until=500)]}
    stop = await h.c.session(FakeWS([welcome, {"type": "burst", "burst": plan(60, seconds_until=498)},
                                     {"type": "burst_cancel", "id": 60}]))
    check(stop is False, "session ends normally")
    check(h.msgs[:2] == [opened, closed] and not h.c.outbox, f"outbox flushed first, in order: {h.msgs[:2]}")
    fin = await finished(h, 60)
    check(h.statuses(60) == ["scheduled", "scheduled", "cancelled"] and fin["orders"] == 0, f"{h.statuses(60)}")
    h.close()
    c = TConnector("http://127.0.0.1:9", "tok", SimBroker(), False, False)   # the real send(), link down
    await c.send({"type": "trade_closed", "trade": {"ticket": 2}})
    await c.send({"type": "log", "message": "x"})
    check([m["type"] for m in c.outbox] == ["trade_closed"], "only results are kept while offline")
    sent = []

    class Sock:
        async def send(self, raw):
            sent.append(json.loads(raw))

    sock = Sock()
    await c.flush_outbox(sock)
    check(sent == [{"type": "trade_closed", "trade": {"ticket": 2}}] and not c.outbox and c.ws is sock,
          "delivered after reconnect, then the link is live")
    c.mt5.shutdown(wait=False)


@scenario
async def outbox_cap_during_a_flush():
    """A result queued while a full outbox is being flushed (the link not live yet) trims the oldest, the one on its
    way: no result still unsent is lost, none is sent twice."""
    c = TConnector("http://127.0.0.1:9", "tok", SimBroker(), False, False)
    c.outbox = [{"type": "trade_closed", "trade": {"ticket": i}} for i in range(500)]
    sent = []

    class Sock:
        async def send(self, raw):
            if not sent:                                  # a scalp closes while the first result is on its way
                await c.send({"type": "trade_closed", "trade": {"ticket": 999}})
            sent.append(json.loads(raw)["trade"]["ticket"])

    await c.flush_outbox(Sock())
    check(sent == [*range(500), 999] and not c.outbox,
          f"all 501 sent once, in order: {len(sent)} sent, missing {sorted(({*range(500), 999}) - set(sent))[:5]}")
    c.mt5.shutdown(wait=False)


@scenario
async def run_queues_sends_until_welcome():
    """The real run(): a runner's result between auth and welcome waits behind the outage's results, so the
    server never sees a trade_closed before its trade_opened."""
    import connector.connector as cmod
    b = SimBroker("hedging")
    h = Harness(b)
    del h.c.send
    h.c.outbox = [{"type": "trade_opened", "trade": {"ticket": 1}}]
    welcome = {"type": "welcome", "user": {"email": "t@example.com"}, "settings": SETTINGS, "open_signals": [],
               "bursts": []}

    class FakeLink:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def send(self, raw):
            h.msgs.append(json.loads(raw))

        async def __aiter__(self):
            await h.c.send({"type": "trade_closed", "trade": {"ticket": 1}})     # auth is out, welcome not yet in
            for m in (welcome, {"type": "kicked", "reason": "token_revoked"}):
                yield json.dumps(m)

    real, cmod.connect = cmod.connect, lambda url, **kw: FakeLink()
    try:
        await h.c.run()
    finally:
        cmod.connect = real
        h.c.sweeper and h.c.sweeper.cancel()
    check([m["type"] for m in h.msgs[:3]] == ["auth", "trade_opened", "trade_closed"] and not h.c.outbox,
          f"order on the link: {[m['type'] for m in h.msgs]}")
    h.close()


# ── volume limits, failed reads and closes, partial fills ──────
def signal(b, sid, symbol="EURUSD", **kw):
    px = b.ask(symbol)
    return {"id": sid, "symbol": symbol, "side": "buy", "price": px, "sl": px - 20 * PIP, "tp": px + 40 * PIP,
            "mode": "trend", "status": "open", **kw}


class Link:
    """A websocket that delivers `msgs`, then ends (the connector would reconnect); sends are recorded."""
    def __init__(self, h, msgs):
        self.h, self.msgs = h, [json.dumps(m) for m in msgs]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, raw):
        self.h.msgs.append(json.loads(raw))

    async def __aiter__(self):
        for m in self.msgs:
            yield m


WELCOME = {"type": "welcome", "user": {"email": "t@example.com"}, "settings": SETTINGS, "open_signals": [],
           "bursts": []}


@scenario
def mt5_broker_volume_max_partial_fills_read_errors():
    """bot/mt5_broker.py against a fake MetaTrader5 module: closes go in deals of at most volume_max, a partial
    fill (10010) is a fill, a failed positions_get raises instead of looking like a flat account."""
    import bot.mt5_broker as mb
    real = mb.mt5
    if real is None:
        return print("  (MetaTrader5 package not installed: skipped)")
    sent, script, err, held = [], [], [(1, "Success")], [()]
    fake = SimpleNamespace(**{k: getattr(real, k) for k in dir(real) if k.isupper()})

    def order_send(req):                   # answers from `script` as (retcode, volume filled[, order]) or None (no
        sent.append(dict(req))             # answer), then DONE in full
        answer = script.pop(0) if script else (real.TRADE_RETCODE_DONE, req["volume"])
        if answer is None:
            return None
        return SimpleNamespace(retcode=answer[0], comment="", order=answer[2] if len(answer) > 2 else 700 + len(sent),
                               volume=answer[1], price=1.1)
    fake.order_send = order_send
    fake.last_error = lambda: err[0]
    fake.symbol_select = lambda s, on: True
    fake.symbol_info = lambda s: SimpleNamespace(point=0.00001, digits=5, trade_tick_value=1.0, trade_tick_size=0.00001,
                                                 volume_min=0.01, volume_step=0.01, volume_max=30.0,
                                                 trade_stops_level=0, filling_mode=2)    # IOC only: partial fills
    fake.symbol_info_tick = lambda s: SimpleNamespace(bid=1.1, ask=1.10005)
    fake.positions_get = lambda: held[0]
    mb.mt5 = fake
    try:
        b = mb.MT5Broker(magic=MAGIC)
        pos = {"ticket": 4, "symbol": "EURUSD", "side": "buy", "lots": 100.0}
        check(b.close_position(pos, "fxb exit") == 100.0 and [r["volume"] for r in sent] == [30.0, 30.0, 30.0, 10.0]
              and all(r["position"] == 4 and r["type"] == real.ORDER_TYPE_SELL for r in sent),
              f"a merged 100-lot position closes in deals of at most volume_max 30: {sent}")
        sent.clear()
        orders = []
        b.close_position(pos, "fxb exit", volume=45.0, orders=orders)
        check([r["volume"] for r in sent] == [30.0, 15.0] and orders == [701, 702],
              f"a part over volume_max, the order of each deal kept: {sent} {orders}")
        sent.clear()
        script[:] = [(real.TRADE_RETCODE_DONE_PARTIAL, 12.0)]
        b.close_position(pos, "fx exit", volume=30.0)
        check([r["volume"] for r in sent] == [30.0, 18.0], f"10010 on a close: one more deal for the rest {sent}")
        sent.clear()
        script[:] = [(real.TRADE_RETCODE_DONE, 30.0), (real.TRADE_RETCODE_REJECT, 0.0)]
        orders = []
        try:
            b.close_position(pos, "fx exit", volume=50.0, orders=orders)
            check(False, "a refused deal must raise")
        except mb.BrokerError as e:
            check(e.retcode == real.TRADE_RETCODE_REJECT and [r["volume"] for r in sent] == [30.0, 20.0]
                  and (e.done, e.unsure, orders) == (30.0, 0.0, [701]),
                  f"refused after 30 went out: done 30, nothing unsure {e} {sent} {e.done} {e.unsure} {orders}")
        sent.clear()
        script[:] = [(real.TRADE_RETCODE_DONE_PARTIAL, 12.0), None]
        try:
            b.close_position(pos, "fx exit", volume=50.0)
            check(False, "a deal with no answer must raise")
        except mb.BrokerError as e:
            check(e.retcode is None and (e.done, e.unsure) == (12.0, 30.0),
                  f"no answer for the next deal: 12 done, the 30 it sent unsure {e} {e.done} {e.unsure}")
        script[:] = [(real.TRADE_RETCODE_DONE, 30.0, 0), (10012, 0.0)]
        orders = []
        try:
            b.close_position(pos, "fx exit", volume=50.0, orders=orders)
            check(False, "a timeout must raise")
        except mb.BrokerError as e:
            check(e.retcode == 10012 and (e.done, e.unsure, orders) == (30.0, 20.0, []),
                  f"10012 (timeout) leaves the deal's outcome open: unsure; an order ticket 0 is not kept "
                  f"{e} {e.done} {e.unsure} {orders}")
        for code in (10008, 10011, 10028):
            script[:] = [(code, 0.0)]
            try:
                b.close_position(pos, "fx exit", volume=5.0)
                check(False, f"{code} on a close must raise")
            except mb.BrokerError as e:
                check((e.retcode, e.done, e.unsure) == (code, 0.0, 5.0), f"{code} leaves it open: unsure {e} {e.unsure}")
        script[:] = [(real.TRADE_RETCODE_DONE_PARTIAL, 0.0)]
        try:
            b.close_position(pos, "fx exit", volume=1.0)
            check(False, "a partial fill of nothing must raise, not loop")
        except mb.BrokerError:
            pass
        sent.clear()
        script[:] = [(real.TRADE_RETCODE_DONE_PARTIAL, 1.3)]
        res = b.open_market("EURUSD", "buy", 2.0, 1.099, 1.102, "fx:7")
        check(res["lots"] == 1.3 and res["ticket"] == 701 and len(sent) == 1 and sent[0]["volume"] == 2.0,
              f"10010 on an entry: a position of the filled lots, not an error {res}")
        script[:] = [(real.TRADE_RETCODE_REJECT, 0.0)]
        try:
            b.open_market("EURUSD", "buy", 2.0, 1.099, 1.102, "fx:8")
            check(False, "a refused entry must raise")
        except mb.BrokerError:
            pass
        check(b.positions() == [], "no positions: an empty list")
        held[0] = None
        check(b.positions(all_magic=True) == [], "None with last_error 'Success': no positions (some builds)")
        err[0] = (-10005, "IPC timeout")
        try:
            b.positions()
            check(False, "a failed positions_get must raise, never look flat")
        except mb.BrokerError as e:
            check("positions_get failed" in str(e), f"{e}")
    finally:
        mb.mt5 = real


@scenario
async def netting_exits_in_deals_of_volume_max_with_backoff():
    """A merged netting position bigger than volume_max: slices hit together go in deals <= volume_max, each counted
    once it went through; a close the broker keeps refusing is retried after a growing pause, not on every poll."""
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(80))
    await armed(h, 80)
    b.move(1.10080)                                   # both 0.5 buy-stops fill: one 1.0 position, entry 1.10085
    await until(lambda: h.of("trade_opened") and near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0), "merged fill")
    pid = h.of("trade_opened")[-1]["trade"]["ticket"]
    close, n = b.close_position, [0]

    def second_deal_refused(pos, comment="", price=None, volume=None):
        n[0] += 1
        if n[0] == 2:
            raise BrokerError("order rejected (10004): requote", 10004)
        return close(pos, comment, price, volume)
    b.close_position = second_deal_refused
    b.move(1.10175)                                   # +9.0 pips: slices 1-4 (4.0 5.5 7.0 8.5) in one poll
    await until(lambda: pid in b.pos and near(b.pos[pid]["lots"], 0.2), "slices 1-4 closed")
    await asyncio.sleep(0.05)
    check([c[2] for c in b.calls_of("close")] == [0.4, 0.4] and near(b.pos[pid]["lots"], 0.2),
          f"deals of 2 slices (0.4 <= 0.5); the refused one retried, nothing closed twice: {b.calls_of('close')}")
    b.close_position = close
    b.fail_close = 10 ** 6                            # from now on the broker refuses every close
    await jump(30.5)                                  # hold time
    await until(lambda: len(b.calls_of("close")) == 3, "hold-time close tried")
    await asyncio.sleep(0.2)                          # ~20 polls
    check(len(b.calls_of("close")) == 3, f"a refused close is not resent on every poll: {len(b.calls_of('close'))}")
    await jump(0.6)
    await until(lambda: len(b.calls_of("close")) == 4, "retried after 0.5 s")
    await asyncio.sleep(0.2)
    check(len(b.calls_of("close")) == 4, "then the pause doubles")
    b.fail_close = 0
    await jump(1.1)
    fin = await finished(h, 80)
    check(fin["status"] == "closed" and not b.pos and len(h.of("trade_closed")) == 1, f"final {fin}")
    check(sum("could not close" in m for m in h.logs("warn")) == 1, "a failing close is reported once")
    h.close()

    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(81))
    await armed(h, 81)
    b.move(1.10080)
    await until(lambda: h.of("trade_opened") and near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0), "merged fill")
    await jump(30.5)
    fin = await finished(h, 81)
    check([c[2] for c in b.calls_of("close")] == [None] and [c[2] for c in b.calls_of("deal")] == [0.5, 0.5]
          and fin["status"] == "closed" and not b.pos, f"hold time: the whole position, in deals <= 0.5 {b.calls}")
    h.close()


@scenario
async def netting_slice_partly_filled_then_refused():
    """MT5Broker.close_position can raise after part of a slice went out (10010, then the rest refused): the next
    deal is sized from the live position, so the slice is finished and nothing is closed twice."""
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(82))
    await armed(h, 82)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "merged fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    close, n = b.close_position, [0]

    def mt5_like(pos, comment="", price=None, volume=None):
        n[0] += 1
        if n[0] == 1 and volume:                      # like MT5Broker: 10010 for 0.1, then the rest refused
            with b.lock:
                b.calls.append(("close", pos["ticket"], volume))
                o = b._ticket()
                b._close(b.pos[pos["ticket"]], 0.1, b._px(b.pos[pos["ticket"]]), order=o)
            raise refused_after(0.1, [o])
        return close(pos, comment, price, volume)
    b.close_position = mt5_like
    b.move(1.10140)                                   # slice 1 of 5 (0.2 lots) only
    await until(lambda: len(b.calls_of("close")) >= 2, "slice finished")
    await asyncio.sleep(0.1)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.1] and near(b.pos[pid]["lots"], 0.8),
          f"the rest of the slice only: {b.calls_of('close')} {b.pos.get(pid)}")
    b.close_position = close
    await jump(30.5)                                  # hold time
    fin = await finished(h, 82)
    check(fin["status"] == "closed" and not b.pos, f"final {fin}")
    h.close()

    # a split order executed late: it lands after the poll's read of the position, before its slice-1 deal
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(83))
    await armed(h, 83)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:83:b2")
    with b.lock:
        late = b.orders.pop(b2)                       # triggered with b1, its execution lags at the broker
    quote = b.quote

    def lagging(sym):                                 # the runner reads positions, then the quote, then closes
        with b.lock:
            if late and b.bids[sym] >= 1.10130 - 1e-9:
                b._fill(late.copy(), b.ask(sym))
                late.clear()
        return quote(sym)
    b.quote = lagging                                 # patched before any poll can hold the original
    b.move(1.10080)                                   # b1 fills 0.5
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.move(1.10130)                                   # slice 1 due
    await until(lambda: b.calls_of("close"), "slice 1")
    await asyncio.sleep(0.05)
    check(near(b.pos[pid]["lots"], 0.8), f"slice 1: 0.2 of the 1.0 that entered {b.calls_of('close')}")
    for pips, left in ((5.6, 0.6), (7.1, 0.4)):       # slices 2 and 3, one at a time
        await jump(1.1)
        b.move(round(b.pos[pid]["entry_price"] + pips * PIP, 5))
        await until(lambda: near(b.pos[pid]["lots"], left) or len(b.calls_of("close")) > 3, f"{left} left")
        await asyncio.sleep(0.05)
        check(near(b.pos[pid]["lots"], left), f"one slice a deal: {b.calls_of('close')} {b.pos[pid]['lots']}")
    check(near(h.c.bursts[83]["trades"][pid]["lots"], 1.0) and near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0),
          f"the late fill counted and reported: {h.c.bursts[83]['trades'][pid]} {h.of('trade_opened')}")
    h.close()


@scenario
async def netting_entered_volume_counted_from_the_fills():
    """What entered a netting position is counted from the fills, never from a read of the position: a read right
    after our own deal may still show the volume before it (MT5 not synced yet), and a split order executed late may
    land between the runner's read and its deal (slice, last target or hold time)."""
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(84))
    await armed(h, 84)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    positions, close, stale, once = b.positions, b.close_position, [], [True]

    def synced_late(pos, comment="", price=None, volume=None):     # the next 3 reads still show the position before
        with b.lock:
            before = positions()
            res = close(pos, comment, price, volume)
            if once.pop() if once else False:
                stale.extend([before] * 3)
                b.move(round(e + 5.6 * PIP, 5))                         # and slice 2 is due at once
            return res
    def reads(all_magic=False):
        with b.lock:
            return stale.pop() if stale else positions(all_magic)
    b.close_position, b.positions = synced_late, reads
    b.move(round(e + 4.5 * PIP, 5))                                    # slice 1
    await until(lambda: b.calls_of("close") and not stale and not once, "stale reads")
    await asyncio.sleep(0.05)
    tr = h.c.bursts[84]["trades"][pid]
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.8) and near(tr["lots"], 1.0)
          and len(h.of("trade_opened")) == 1, f"no deal sized and no entry counted from a stale read: "
                                              f"{b.calls_of('close')} {tr} {h.of('trade_opened')}")
    for pips, left in ((5.6, 0.6), (7.1, 0.4), (8.6, 0.2)):
        b.move(round(e + pips * PIP, 5))
        await jump(0.6)
        await until(lambda: near(b.pos[pid]["lots"], left) or len(b.calls_of("close")) > 4, f"{left} left")
    b.move(round(e + 10.1 * PIP, 5))                                   # the last target
    await jump(0.6)
    await until(lambda: pid not in b.pos or len(b.calls_of("close")) > 5, "last target")
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2, 0.2, 0.2, None] and pid not in b.pos,
          f"one slice a deal, flat at the last target: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # split order b2 executed late, landing after the read of the poll where slices 1-4 are due
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(85))
    await armed(h, 85)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:85:b2")
    with b.lock:
        late = b.orders.pop(b2)
    quote = b.quote

    def lagging(sym):
        with b.lock:
            if late and b.bids[sym] >= 1.10171 - 1e-9:
                b._fill(late.copy(), b.ask(sym))
                late.clear()
        return quote(sym)
    b.quote = lagging
    b.move(1.10080)                                   # b1 fills 0.5 at 1.10085
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.move(1.10171)                                   # +8.6 pips: slices 1-4 due, from the 0.5 the poll read
    await until(lambda: not late and len(b.calls_of("close")) >= 2, "slices 1-4")
    await jump(1.1)
    await until(lambda: near(h.c.bursts[85]["trades"][pid]["lots"], 1.0), "the late fill counted")
    await until(lambda: len(b.calls_of("close")) >= 3, "what slices 1-4 owe of the late fill")
    check(near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0) and near(b.pos[pid]["lots"], 0.2)
          and [c[2] for c in b.calls_of("close")] == [0.4, None, 0.3],
          f"reported with all that entered, its share of slices 1-4 out at once: {h.of('trade_opened')} "
          f"{b.pos.get(pid)} {b.calls_of('close')}")
    await jump(0.6)
    b.move(round(b.pos[pid]["entry_price"] + 10.1 * PIP, 5))       # slice 5: the last target
    await until(lambda: pid not in b.pos or len(b.calls_of("close")) > 3, "last target")
    check(pid not in b.pos, f"the last 0.2 goes out at the last target, not at hold time: {b.calls_of('close')}")
    h.close()

    # b2 executed late, landing after the read, before the hold-time deal
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(86))
    await armed(h, 86)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:86:b2")
    with b.lock:
        late = b.orders.pop(b2)
    close = b.close_position

    def lagging_close(pos, comment="", price=None, volume=None):
        with b.lock:
            if late:
                b._fill(late.copy(), b.ask("EURUSD"))
                late.clear()
        return close(pos, comment, price, volume)
    b.close_position = lagging_close
    b.move(1.10080)
    await until(lambda: h.of("trade_opened"), "first fill")
    await asyncio.sleep(0.2)                          # ~20 polls: b2 stays asked for while the position is open
    await jump(30.5)                                  # hold time
    fin = await finished(h, 86)
    check(fin["status"] == "closed" and not b.pos and [c[2] for c in b.calls_of("close")] == [None, None],
          f"the late 0.5 closed too: {fin} {b.calls_of('close')}")
    check(near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0), f"and reported: {h.of('trade_opened')}")
    h.close()

    # a slice deal that went out in part (10010, then refused) while b2's late fill landed
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(87))
    await armed(h, 87)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:87:b2")
    with b.lock:
        late = b.orders.pop(b2)
    b.move(1.10080)
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    close, n = b.close_position, [0]

    def mt5_like(pos, comment="", price=None, volume=None):
        n[0] += 1
        if n[0] == 1:
            with b.lock:
                b.calls.append(("close", pos["ticket"], volume))
                p, o = b.pos[pos["ticket"]], b._ticket()
                b._close(p, 0.1, b._px(p), order=o)       # 10010: 0.1 of the 0.2
                b._fill(late.copy(), b.ask("EURUSD"))     # b2's late fill lands in the same moment
                p["entry_price"] = 1.10085                # keep the entry: only the volumes matter here
            raise refused_after(0.1, [o])
        return close(pos, comment, price, volume)
    b.close_position = mt5_like
    b.move(1.10126)                                   # +4.1 pips: slice 1 only
    await until(lambda: n[0] >= 1, "slice 1 tried")
    await jump(1.0)                                   # past the retry pause
    await until(lambda: len(b.calls_of("close")) >= 2, "slice 1 finished")
    await asyncio.sleep(0.05)
    tr = h.c.bursts[87]["trades"][pid]
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.1] and near(b.pos[pid]["lots"], 0.8) and near(tr["lots"], 1.0)
          and near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0),
          f"1.0 entered, 0.2 out: {b.calls_of('close')} {b.pos.get(pid)} {tr} {h.of('trade_opened')}")
    h.close()

    # whipsaw: a sell-stop whose cancel stays frozen fills against the merged long and only reduces it
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(88, offset_pips=3.0))     # buy-stops 1.10035, sell-stops 1.09970, buy SL 1.09935
    await armed(h, 88)
    s1 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:88:s1")
    b.frozen[s1] = 10 ** 6
    b.move(1.10040)                                   # b1 + b2 fill: one 1.0 long
    await until(lambda: h.of("trade_opened") and near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0), "long 1.0")
    pid = h.of("trade_opened")[-1]["trade"]["ticket"]
    b.move(1.09965)                                   # s1 fills: the long is down to 0.5
    await until(lambda: "both" == h.c.bursts[88]["side"], "whipsaw seen")
    await asyncio.sleep(0.05)
    check(near(b.pos[pid]["lots"], 0.5) and near(h.c.bursts[88]["trades"][pid]["lots"], 1.0)
          and [m["trade"]["lots"] for m in h.of("trade_opened")] == [1.0],
          f"the sell fill is no entry of the long: {b.pos.get(pid)} {h.of('trade_opened')}")
    await jump(30.5)
    fin = await finished(h, 88)
    check(fin["status"] == "closed" and not b.pos, f"final {fin}")
    h.close()


@scenario
async def netting_fill_accounting_edge_cases():
    """History behind the position list, a late fill at the last target, a whipsaw between two polls, a slow slice
    deal, a reversal (MT5 keeps the position identifier), and a late split order on the sell side."""
    # a stop order filled in part (IOC: 0.6 of 1.0) whose deal history shows one look later: sized from the deal
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(89))
    await armed(h, 89)
    buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
    b.hide(buy, 0.05)
    with b.lock:
        b.bids["EURUSD"] = 1.10090
        o = b.orders.pop(buy)
        b._fill({**o, "lots": 0.6}, b.ask("EURUSD"))
    await until(lambda: h.of("trade_opened") and buy not in h.c.bursts[89]["unknown"], "deal read")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    check(near(h.c.bursts[89]["trades"][pid]["lots"], 0.6) and [m["trade"]["lots"] for m in h.of("trade_opened")] == [0.6],
          f"the order's 1.0 is not taken for the fill: {h.c.bursts[89]['trades'][pid]} {h.of('trade_opened')}")
    for pips, left in ((4.1, 0.4), (5.6, 0.2), (7.1, 0.0)):
        b.move(round(e + pips * PIP, 5))
        await until(lambda: near(b.pos[pid]["lots"] if pid in b.pos else 0.0, left), f"{left} left")
        await jump(1.1)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2, None], f"slices of the 0.6: {b.calls_of('close')}")
    h.close()

    # 10 slices of 0.1, b1/b2 of 0.5, deals of up to 6 slices: b2 executes late, during the last-target deal
    b = SimBroker("netting", volume_max=0.6)
    h = Harness(b)
    await h.c.on_burst(plan(90, trades=10, hold_seconds=60))
    await armed(h, 90)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:90:b2")
    with b.lock:
        late = b.orders.pop(b2)
    close = b.close_position

    def lagging_full_close(pos, comment="", price=None, volume=None):
        with b.lock:
            if late and volume is None:
                b._fill(late.copy(), b.ask("EURUSD"))
                late.clear()
        return close(pos, comment, price, volume)
    b.close_position = lagging_full_close
    b.move(1.10080)                                   # b1 fills 0.5
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    b.move(round(e + 8.6 * PIP, 5))                   # slices 1-4: one 0.4 deal
    await until(lambda: near(b.pos[pid]["lots"], 0.1), "slices 1-4")
    await jump(0.6)
    b.move(round(e + 17.6 * PIP, 5))                  # the last target: a full close of the 0.1, b2's 0.5 lands
    await until(lambda: not late, "last-target deal")
    await jump(1.1)
    fin = await finished(h, 90)
    check([c[2] for c in b.calls_of("close")] == [0.4, None, None] and near(h.of("trade_opened")[-1]["trade"]["lots"], 1.0)
          and fin["status"] == "closed" and not b.pos,
          f"closed right after the last target, not at hold time: {b.calls_of('close')} {h.of('trade_opened')} {fin}")
    h.close()

    # whipsaw between two polls: b1 + b2 fill, then s1 + s2 close the position
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(91))
    await armed(h, 91)
    with b.lock:
        b.move(1.10080)
        b.move(1.09900)
    fin = await finished(h, 91)
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 1.0)]
          and fin["status"] == "closed", f"only what entered: {h.of('trade_opened')} {fin}")
    h.close()

    # a slice deal that takes 0.6 s, and MT5's list of positions and its history 0.3 s behind it (the two agree on
    # the volume before the deal): the settle counts from its return
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(92))
    await armed(h, 92)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    positions, close, stale, first = b.positions, b.close_position, {"until": 0.0, "snap": None}, [True]

    def slow_close(pos, comment="", price=None, volume=None):
        time.sleep(0.6)
        with b.lock:
            before, n = positions(), len(b.deals)
            res = close(pos, comment, price, volume)
            stale.update(until=time.monotonic() + 0.3, snap=before)
            for d in b.deals[n:]:
                b.hide(d["order"], 0.3)
            if first and first.pop():
                b.move(round(e + 5.6 * PIP, 5))       # slice 2 due at once
            return res
    def reads(all_magic=False):
        with b.lock:
            return stale["snap"] if time.monotonic() < stale["until"] else positions(all_magic)
    b.close_position, b.positions = slow_close, reads
    b.move(round(e + 4.5 * PIP, 5))
    await until(lambda: len(b.calls_of("close")) >= 2, "slice 2")
    await asyncio.sleep(0.8)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2] and near(b.pos[pid]["lots"], 0.6),
          f"slice 2 sized from a fresh read: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # reversal: the sell-stop (cancel frozen) fills 1.0 against what is left of a 0.4 long after its slice 1; MT5
    # keeps the identifier, and its history shows the in/out deal one look later than the position
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(93, offset_pips=3.0))     # buy-stop 1.10035 (SL 1.09935), sell-stop 1.09970
    await armed(h, 93)
    buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    with b.lock:                                      # IOC: 0.4 of the 1.0 buy-stop
        b.bids["EURUSD"] = 1.10035
        o = b.orders.pop(buy)
        b._fill({**o, "lots": 0.4}, b.ask("EURUSD"))
    await until(lambda: h.of("trade_opened"), "long")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.move(round(b.pos[pid]["entry_price"] + 4.1 * PIP, 5))       # the long's slice 1
    await until(lambda: near(b.pos[pid]["lots"], 0.2), "long: slice 1")
    await jump(0.6)
    b.hide(sell, 0.05)
    b.move(1.09965)                                   # the sell-stop: a 0.8 short, same identifier
    await until(lambda: len(h.of("trade_opened")) == 2 and sell not in h.c.bursts[93]["unknown"],
                "reversal reported, its deal read")
    await asyncio.sleep(0.1)
    p = next(iter(b.pos.values()))
    check(p["identifier"] == pid and p["side"] == "sell" and near(p["lots"], 0.8)
          and [(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 0.4), ("sell", 0.8)]
          and [c[2] for c in b.calls_of("close")] == [0.2],
          f"a new entry from what the short holds, its slices from 1: {p} {h.of('trade_opened')} {b.calls_of('close')}")
    e = p["entry_price"]
    for pips, left in ((4.1, 0.6), (5.6, 0.4), (7.1, 0.2), (8.6, 0.0)):
        await jump(1.1)
        b.move(round(e - pips * PIP - b.spread, 5))
        await until(lambda: near(b.pos[p["ticket"]]["lots"] if b.pos else 0.0, left), f"short: {left} left")
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2, 0.2, 0.2, None], f"short slices: {b.calls_of('close')}")
    h.close()

    # the sell side: split order s2 executed late, landing after the read of the poll where slices 1-4 are due
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(94))
    await armed(h, 94)
    s2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:94:s2")
    with b.lock:
        late = b.orders.pop(s2)
    quote, at = b.quote, [None]

    def lagging(sym):
        with b.lock:
            if late and at[0] and b.bids[sym] <= at[0] + 1e-9:
                b._fill(late.copy(), b.bids[sym])
                late.clear()
        return quote(sym)
    b.quote = lagging
    b.move(1.09920)                                   # s1 fills 0.5
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    at[0] = round(b.pos[pid]["entry_price"] - 8.6 * PIP - b.spread, 5)
    b.move(at[0])                                     # +8.6 pips: slices 1-4 due, from the 0.5 the poll read
    await until(lambda: not late and len(b.calls_of("close")) >= 2, "slices 1-4")
    await jump(1.1)
    await until(lambda: len(b.calls_of("close")) >= 3, "what slices 1-4 owe of the late fill")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("sell", 0.5), ("sell", 1.0)]
          and near(b.pos[pid]["lots"], 0.2) and [c[2] for c in b.calls_of("close")] == [0.4, None, 0.3],
          f"a short counts its late fill too: {h.of('trade_opened')} {b.pos.get(pid)} {b.calls_of('close')}")
    await jump(0.6)
    b.move(round(b.pos[pid]["entry_price"] - 10.1 * PIP - b.spread, 5))     # slice 5: the last target
    await until(lambda: pid not in b.pos or len(b.calls_of("close")) > 3, "last target")
    check(pid not in b.pos, f"the last 0.2 of the short goes out at the last target: {b.calls_of('close')}")
    h.close()


@scenario
async def fills_judged_by_the_position_history():
    """A position MT5 lists after its deal is in history (or that fills between two reads) is open, not come and
    gone; history behind the list of positions holds the slices back instead of skewing them."""
    # netting: listed 0.05 s after its deal
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(95))
    await armed(h, 95)
    buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
    b.unlist(buy, 0.05)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    await asyncio.sleep(0.1)
    check(not h.of("trade_closed") and not h.c.bursts[95]["trades"][buy]["closed"],
          f"not closed while unlisted: {h.of('trade_closed')}")
    b.move(round(b.pos[buy]["entry_price"] + 4.1 * PIP, 5))
    await until(lambda: near(b.pos[buy]["lots"], 0.8), "slice 1 of the late-listed position")
    b.unlist(buy, 0.05)                               # one that is followed already, missing from a few reads
    await asyncio.sleep(0.1)
    check(not h.of("trade_closed") and not h.c.bursts[95]["trades"][buy]["closed"],
          f"not closed while missing from the list: {h.of('trade_closed')}")
    await jump(30.5)
    fin = await finished(h, 95)
    check(fin["status"] == "closed" and not b.pos and len(h.of("trade_closed")) == 1
          and h.of("trade_closed")[0]["trade"]["pnl"] is not None, f"one close, with its P&L: {h.of('trade_closed')} {fin}")
    h.close()

    # hedging, two tickets a side, both listed late
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(96, trades=2))
    await armed(h, 96)
    buys = sorted(t for t, o in b.orders.items() if o["side"] == "buy")
    for t in buys:
        b.unlist(t, 0.05)
    b.move(1.10090)
    await until(lambda: len(h.of("trade_opened")) == 2, "fills")
    await asyncio.sleep(0.1)
    check(not h.of("trade_closed"), f"not closed while unlisted: {h.of('trade_closed')}")
    await jump(30.5)
    fin = await finished(h, 96)
    closed = h.of("trade_closed")
    check(fin["status"] == "closed" and not b.pos and sorted(m["trade"]["ticket"] for m in closed) == buys
          and all(m["trade"]["pnl"] is not None for m in closed), f"held, then closed at hold time: {closed} {fin}")
    h.close()

    # reversal by two split sell-stops (cancels frozen) against a 0.4 long; the second one's deal shows late
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(97, offset_pips=3.0))
    await armed(h, 97)
    sells = sorted(t for t, o in b.orders.items() if o["side"] == "sell")
    for t in sells:
        b.frozen[t] = 10 ** 6
    with b.lock:                                      # IOC: 0.2 of each 0.5 buy-stop
        b.bids["EURUSD"] = 1.10035
        for t in sorted(t for t, o in b.orders.items() if o["side"] == "buy"):
            b._fill({**b.orders.pop(t), "lots": 0.2}, b.ask("EURUSD"))
    await until(lambda: h.of("trade_opened"), "long")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.hide(sells[1], 0.05)
    b.move(1.09965)                                   # s1 reverses it (a 0.1 short), s2 adds 0.5
    await until(lambda: h.of("trade_opened")[-1]["trade"]["side"] == "sell" and not h.c.bursts[97]["unknown"],
                "reversal")
    await asyncio.sleep(0.1)
    p = next(iter(b.pos.values()))
    check(p["identifier"] == pid and near(p["lots"], 0.6)
          and [(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 0.4), ("sell", 0.6)],
          f"the short counted once: {p} {h.of('trade_opened')}")
    e = p["entry_price"]
    for pips, left in ((4.1, 0.4), (5.6, 0.2), (7.1, 0.0)):
        await jump(1.1)
        b.move(round(e - pips * PIP - b.spread, 5))
        await until(lambda: near(b.pos[p["ticket"]]["lots"] if b.pos else 0.0, left), f"short: {left} left")
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2, None], f"short slices: {b.calls_of('close')}")
    h.close()

    # reversed twice between two polls (long, short, long again; cancels frozen): the same side, a new entry
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(100, offset_pips=3.0, trades=10))     # slices of 0.1
    await armed(h, 100)
    for t in b.orders:
        b.frozen[t] = 10 ** 6
    with b.lock:                                      # b1 fills 0.4 (IOC); b2 is triggered, executed later
        b.bids["EURUSD"] = 1.10035
        b1, b2 = (b.orders.pop(t) for t in sorted(t for t, o in b.orders.items() if o["side"] == "buy"))
        b._fill({**b1, "lots": 0.4}, b.ask("EURUSD"))
    await until(lambda: h.of("trade_opened"), "long")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.move(round(b.pos[pid]["entry_price"] + 4.1 * PIP, 5))
    await until(lambda: near(b.pos[pid]["lots"], 0.3), "slice 1")
    await jump(0.6)
    with b.lock:
        b.bids["EURUSD"] = 1.09965                    # s1 0.5: a 0.2 short
        b._fill(b.orders.pop(min(t for t, o in b.orders.items() if o["side"] == "sell")), b.bids["EURUSD"])
        b.bids["EURUSD"] = 1.10040                    # b2 0.5: a 0.3 long
        b._fill(b2, b.ask("EURUSD"))
    await until(lambda: len(h.of("trade_opened")) == 2, "the new entry reported")
    p = next(iter(b.pos.values()))
    check(p["identifier"] == pid and p["side"] == "buy" and near(p["lots"], 0.3)
          and [(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 0.4), ("buy", 0.3)],
          f"a new entry of 0.3: {p} {h.of('trade_opened')}")
    await jump(1.1)
    b.move(round(p["entry_price"] + 4.1 * PIP, 5))   # its own slice 1
    await until(lambda: near(b.pos[p["ticket"]]["lots"], 0.2), "slice 1 of the new entry")
    check([c[2] for c in b.calls_of("close")] == [0.1, 0.1], f"slices from 1 again: {b.calls_of('close')}")
    h.close()

    # b2 executed late, after slice 1; its deal shows in history 0.05 s after the position grew
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(98))
    await armed(h, 98)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:98:b2")
    with b.lock:
        late = b.orders.pop(b2)
    b.move(1.10080)                                   # b1 fills 0.5
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.move(round(b.pos[pid]["entry_price"] + 4.1 * PIP, 5))
    await until(lambda: near(b.pos[pid]["lots"], 0.3), "slice 1")
    await jump(0.6)
    b.hide(b2, 0.05)
    with b.lock:
        b._fill(late, b.ask("EURUSD"))
    await until(lambda: near(h.c.bursts[98]["trades"][pid]["lots"], 1.0), "the late fill counted")
    await asyncio.sleep(0.1)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.8)
          and [m["trade"]["lots"] for m in h.of("trade_opened")] == [0.5, 1.0],
          f"nothing closed for a fill history did not show yet: {b.calls_of('close')} {b.pos.get(pid)} "
          f"{h.of('trade_opened')}")
    b.move(round(b.pos[pid]["entry_price"] + 5.6 * PIP, 5))       # slice 2, from the new average entry
    await until(lambda: near(b.pos[pid]["lots"], 0.6), "slice 2")
    h.close()

    # whipsaw between two polls, b2's deal in history late: reported with all that entered
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(99))
    await armed(h, 99)
    b.hide(next(t for t, o in b.orders.items() if o["comment"] == "fxb:99:b2"), 0.04)
    with b.lock:
        b.move(1.10080)
        b.move(1.09900)
    fin = await finished(h, 99)
    closed = h.of("trade_closed")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 1.0)]
          and fin["status"] == "closed" and len(closed) == 1 and closed[0]["trade"]["pnl"] is not None,
          f"one trade of all that entered, closed with its P&L: {h.of('trade_opened')} {closed} {fin}")
    h.close()


@scenario
async def history_against_reads_that_lag_or_miss():
    """Both views behind our own deal past the settle, a position that grows or reverses and closes between two
    polls, a fill whose deals show many polls late, and a position the list leaves out while its deals hold it."""
    # the list of positions and the history both 0.8 s behind slice 1 (past the 0.5 s settle): the two agree, yet
    # show none of what this runner closed, so the slice is not sent again
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(102))
    await armed(h, 102)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    positions, close, stale, first = b.positions, b.close_position, {"until": 0.0, "snap": None}, [True]

    def lagging_close(pos, comment="", price=None, volume=None):
        with b.lock:
            before, n = positions(), len(b.deals)
            res = close(pos, comment, price, volume)
            if first and first.pop():
                stale.update(until=time.monotonic() + 0.8, snap=before)
                for d in b.deals[n:]:
                    b.hide(d["order"], 0.8)
            return res
    def reads(all_magic=False):
        with b.lock:
            return stale["snap"] if time.monotonic() < stale["until"] else positions(all_magic)
    b.close_position, b.positions = lagging_close, reads
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await asyncio.sleep(1.0)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.8),
          f"slice 1 once: {b.calls_of('close')} {b.pos.get(pid)}")
    b.move(round(e + 5.6 * PIP, 5))                   # slice 2
    await until(lambda: near(b.pos[pid]["lots"], 0.6) or len(b.calls_of("close")) > 1, "slice 2")
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2], f"then slice 2: {b.calls_of('close')}")
    h.close()

    # b2 executed late and the stop hit before the next poll: reported with all that entered, then closed
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(103))
    await armed(h, 103)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:103:b2")
    with b.lock:
        late = b.orders.pop(b2)
    b.move(1.10080)                                   # b1 fills 0.5 at 1.10085
    await until(lambda: h.of("trade_opened"), "first fill")
    with b.lock:
        b.bids["EURUSD"] = 1.10095
        b._fill(late, b.ask("EURUSD"))                # b2 at 1.10100: 1.0 at 1.100925
        b.move(1.09900)                               # the SL (1.09975) closes all of it
    fin = await finished(h, 103)
    opened, closed = h.of("trade_opened"), h.of("trade_closed")
    check([m["trade"]["lots"] for m in opened] == [0.5, 1.0] and near(opened[-1]["trade"]["entry_price"], 1.100925)
          and opened[-1]["trade"]["sl"] == opened[0]["trade"]["sl"] == 1.09975,
          f"reported with the 1.0 that entered, at its average price: {opened}")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], -192.5) and near(fin["pnl"], -192.5),
          f"closed once with the P&L of the 1.0: {closed} {fin}")
    h.close()

    # a reversal (sell-stop, cancel frozen) and the short's stop, both between two polls: the short is reported too
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(104, offset_pips=3.0))    # buy-stop 1.10035, sell-stop 1.09970 (SL 1.10070)
    await armed(h, 104)
    buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    with b.lock:                                      # IOC: 0.4 of the 1.0 buy-stop
        b.bids["EURUSD"] = 1.10035
        b._fill({**b.orders.pop(buy), "lots": 0.4}, b.ask("EURUSD"))
    await until(lambda: h.of("trade_opened"), "long")
    with b.lock:
        b.move(1.09965)                               # the sell-stop: a 0.6 short at 1.09965
        b.move(1.10080)                               # its SL
    fin = await finished(h, 104)
    opened, closed = h.of("trade_opened"), h.of("trade_closed")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in opened] == [("buy", 0.4), ("sell", 0.6)]
          and near(opened[-1]["trade"]["entry_price"], 1.09965) and opened[-1]["trade"]["sl"] is None,
          f"the short reported as a new entry: {opened}")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], -102.0) and near(fin["pnl"], -102.0),
          f"closed once with the P&L of both: {closed} {fin}")
    h.close()

    # a fill and its stop before the first poll, its deals in history only ~20 polls later, nothing else open: asked
    # for until they show (as long as for a position not listed yet), then reported, and the other side cancelled
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(105))
    await armed(h, 105)
    buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    pending, polls = b.pending_orders, [0]

    def counted(symbol=None):
        polls[0] += 1
        return pending(symbol)
    b.pending_orders = counted
    with b.lock:
        b.move(1.10090)                               # fills 1.0 at 1.10095
        b.move(1.09960)                               # its SL (1.09975)
        for d in [d for d in b.deals if d["position"] == buy]:
            b.hide(d["order"], 10 ** 6)
        start = polls[0]
    await until(lambda: polls[0] >= start + 20, "20 polls")
    with b.lock:
        b.hidden.clear()
    await until(lambda: h.of("trade_closed"), "the fill reported", 2.0)
    fin = await finished(h, 105)
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 1.0)]
          and near(h.of("trade_closed")[0]["trade"]["pnl"], -135.0) and ("cancel", sell, "ok") in b.calls
          and fin["status"] == "closed" and near(fin["pnl"], -135.0),
          f"reported, the sell-stop cancelled: {h.of('trade_opened')} {h.of('trade_closed')} {fin}")
    h.close()

    # a followed position the list leaves out for longer than BURST_GONE polls while its deals hold it: not reported
    # closed by the burst; the sweeper closes it once listed and reports it once
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(106))
    await armed(h, 106)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    b.unlist(pid, 10 ** 6)
    fin = await finished(h, 106)
    check(fin["status"] == "closed" and not h.of("trade_closed") and pid in b.pos and pid in h.c.unreported,
          f"left to the sweeper, no close reported: {fin} {h.of('trade_closed')}")
    with b.lock:
        b.unlisted.clear()
    await h.c.sweep_orphans()
    closed = h.of("trade_closed")
    check(not b.pos and [m["trade"]["ticket"] for m in closed] == [pid] and closed[0]["trade"]["pnl"] is not None
          and pid not in h.c.unreported, f"closed by the sweeper, reported once: {closed} {b.pos}")
    h.close()


def refused_after(done, orders):
    """MT5Broker's error for a close that went through in part (10010, then the rest refused)."""
    e = BrokerError("order rejected (10004): requote", 10004)
    e.done, e.orders = done, orders
    return e


def stale_after(b, calls, seconds=0.8):
    """close_position wrapped: after call number `calls` (also one that raises), the list of positions and the
    history both stay `seconds` behind its deals (past the 0.5 s settle). Returns the wrapper's call counter."""
    positions, close, stale, n = b.positions, b.close_position, {"until": 0.0, "snap": None}, [0]

    def lagging_close(pos, comment="", price=None, volume=None):
        with b.lock:
            n[0] += 1
            before, k = positions(), len(b.deals)
            try:
                return close(pos, comment, price, volume)
            finally:
                if n[0] == calls:
                    stale.update(until=time.monotonic() + seconds, snap=before)
                    for d in b.deals[k:]:
                        b.hide(d["order"], seconds)

    def reads(all_magic=False):
        with b.lock:
            return stale["snap"] if time.monotonic() < stale["until"] else positions(all_magic)
    b.close_position, b.positions = lagging_close, reads
    return n


def views_back(b, snap, orders, seconds):
    """Both views back to an earlier read for `seconds`: the list of positions answers `snap`, the deals of `orders`
    leave the history."""
    positions, end = b.positions, time.monotonic() + seconds
    for o in orders:
        b.hide(o, seconds)
    b.positions = lambda all_magic=False: snap if time.monotonic() < end else positions(all_magic)


def no_answer_once(b):
    """close_position wrapped: the first call goes through but raises as MT5Broker does for a deal with no answer
    (all it sent unsure)."""
    close, calls = b.close_position, [0]

    def no_answer(pos, comment="", price=None, volume=None):
        calls[0] += 1
        res = close(pos, comment, price, volume)
        if calls[0] == 1:
            err = BrokerError("order_send failed: (-10005, 'IPC timeout')")
            err.unsure = volume or pos["lots"]
            raise err
        return res
    b.close_position = no_answer


def before_first_close(b, deal):
    """close_position wrapped: `deal()` runs under the broker's lock just before the first close (a deal that lands
    between our read of the position and our own)."""
    close, ran = b.close_position, []

    def wrapped(pos, comment="", price=None, volume=None):
        with b.lock:
            if not ran:
                ran.append(deal() or True)
            return close(pos, comment, price, volume)
    b.close_position = wrapped


@scenario
async def reductions_hand_offs_and_flash_entries():
    """What left an entry without this runner counting it, a sweeper that waits for the history of a closed position,
    a grown position reported before it is handed to the sweeper, and the entry of a fill that came and went."""
    # slice 1 goes through but its call raises (the reply lost), so this runner never counts it; slice 2's deal then
    # lags in both views past the settle: the history's own reduction is what left the entry, slice 2 is not resent
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(107))
    await armed(h, 107)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    n = stale_after(b, 2)
    close = b.close_position

    def reply_lost(pos, comment="", price=None, volume=None):
        res = close(pos, comment, price, volume)
        if n[0] == 1:
            raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
        return res
    b.close_position = reply_lost
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await jump(0.6)                                   # past the back-off of the failed close
    await asyncio.sleep(0.1)
    b.move(round(e + 5.6 * PIP, 5))                   # slice 2
    await until(lambda: len(b.calls_of("close")) > 1, "slice 2")
    await asyncio.sleep(1.0)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2] and near(b.pos[pid]["lots"], 0.6),
          f"slice 1 counted from the history, slice 2 once: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # a 0.4 reduction (the frozen sell-stop) and a price past target 3, all before the first poll: target 3 goes out on
    # that poll and its deal lags both views past the settle; the reduction counts from the first poll on
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(108))
    await armed(h, 108)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    stale_after(b, 1)
    with b.lock:
        b.move(1.10090)                               # 1.0 at 1.10095
        b._fill({**b.orders.pop(sell), "lots": 0.4}, 1.09930)
        b.move(1.10170)                               # +7.5 pips: target 3 (7.0)
        pid = next(iter(b.pos))
    await until(lambda: b.calls_of("close"), "target 3")
    await asyncio.sleep(1.0)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.4)
          and [m["trade"]["lots"] for m in h.of("trade_opened")] == [1.0],
          f"target 3 once: {b.calls_of('close')} {b.pos.get(pid)} {h.of('trade_opened')}")
    h.close()

    # slices 1 and 2 out, then the stop closes the rest with its deal not in history: handed to the sweeper, which
    # reports it only once the history shows the close (an earlier slice's exit is not its result)
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(109))
    await armed(h, 109)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: len(b.calls_of("close")) == 1, "slice 1")
    await asyncio.sleep(0.6)
    b.move(round(e + 6.0 * PIP, 5))                   # slice 2
    await until(lambda: len(b.calls_of("close")) == 2, "slice 2")
    await asyncio.sleep(0.6)
    with b.lock:
        k = len(b.deals)
        b.move(1.09960)                               # the SL closes the 0.6 left
        for d in b.deals[k:]:
            b.hide(d["order"], 10 ** 6)
    fin = await finished(h, 109)
    await h.c.sweep_orphans()
    check(fin["status"] == "closed" and not h.of("trade_closed") and pid in h.c.unreported,
          f"not reported while its history shows it open: {h.of('trade_closed')} {fin}")
    with b.lock:
        b.hidden.clear()
        real = b.closed_result(pid)
    await h.c.sweep_orphans()
    await h.c.sweep_orphans()
    closed = h.of("trade_closed")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], real["pnl"]) and real["pnl"] < 0
          and pid not in h.c.unreported, f"reported once with all of its P&L: {closed} {real}")
    h.close()

    # ... a history that never shows the close: reported after SWEEP_WAITS sweeps all the same, once
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(110))
    await armed(h, 110)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    with b.lock:
        k = len(b.deals)
        b.move(1.09960)
        for d in b.deals[k:]:
            b.hide(d["order"], 10 ** 6)
    await finished(h, 110)
    for _ in range(h.c.SWEEP_WAITS - 1):
        await h.c.sweep_orphans()
    check(not h.of("trade_closed") and pid in h.c.unreported, f"waited: {h.of('trade_closed')}")
    await h.c.sweep_orphans()
    await h.c.sweep_orphans()
    check([m["trade"]["ticket"] for m in h.of("trade_closed")] == [pid] and pid not in h.c.unreported
          and not h.c.unreported_waits, f"then reported once: {h.of('trade_closed')}")
    h.close()

    # b2 executed late, then the SL between two polls with its deal not in history: the 1.0 that entered is reported
    # before the position goes to the sweeper, which reports its close
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(111))
    await armed(h, 111)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:111:b2")
    with b.lock:
        late = b.orders.pop(b2)
    b.move(1.10080)                                   # b1 fills 0.5 at 1.10085
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    with b.lock:
        b.bids["EURUSD"] = 1.10095
        b._fill(late, b.ask("EURUSD"))                # b2 at 1.10100: 1.0 at 1.100925
        k = len(b.deals)
        b.move(1.09900)                               # the SL (1.09975)
        for d in b.deals[k:]:
            b.hide(d["order"], 10 ** 6)
    await finished(h, 111)
    opened = h.of("trade_opened")
    check([m["trade"]["lots"] for m in opened] == [0.5, 1.0] and near(opened[-1]["trade"]["entry_price"], 1.100925)
          and not h.of("trade_closed") and pid in h.c.unreported, f"the 1.0 reported, then handed over: {opened}")
    with b.lock:
        b.hidden.clear()
    await h.c.sweep_orphans()
    closed = h.of("trade_closed")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], -192.5), f"closed with the P&L of the 1.0: {closed}")
    h.close()

    # b1 and b2 fill, then the SL, all between two polls: reported with the 1.0 at its average price
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(112))
    await armed(h, 112)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:112:b2")
    buy_sl = b.orders[b2]["sl"]
    with b.lock:
        late = b.orders.pop(b2)
        b.move(1.10080)                               # b1 0.5 at 1.10085
        b.bids["EURUSD"] = 1.10095
        b._fill(late, b.ask("EURUSD"))                # b2 0.5 at 1.10100
        b.move(1.09950)                               # the SL closes the 1.0; the sell-stops (1.0993) not reached
    fin = await finished(h, 112)
    opened, closed = h.of("trade_opened"), h.of("trade_closed")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in opened] == [("buy", 1.0)]
          and near(opened[0]["trade"]["entry_price"], 1.100925) and opened[0]["trade"]["sl"] == buy_sl,
          f"the 1.0 at its average price: {opened}")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], fin["pnl"]) and fin["pnl"] < 0, f"{closed} {fin}")
    h.close()

    # s1 and s2 open a short, then b1 and b2 (the other side, placed first) close it, all between two polls: a short
    # at its average price, with the stops of a sell order (b1's deal is an exit)
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(113))
    await armed(h, 113)
    s2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:113:s2")
    sell_sl, sell_tp = b.orders[s2]["sl"], b.orders[s2]["tp"]
    with b.lock:
        late = b.orders.pop(s2)
        b.move(1.09925)                               # s1 0.5 at 1.09925
        b.bids["EURUSD"] = 1.09915
        b._fill(late, b.bids["EURUSD"])               # s2 0.5 at 1.09915: 1.0 short at 1.0992
        b.move(1.10070)                               # ask 1.10075: b1 and b2 close the short
        real = b.closed_result(next(d["position"] for d in b.deals))
    fin = await finished(h, 113)
    opened, closed = h.of("trade_opened"), h.of("trade_closed")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in opened] == [("sell", 1.0)]
          and near(opened[0]["trade"]["entry_price"], 1.0992) and opened[0]["trade"]["sl"] == sell_sl
          and opened[0]["trade"]["tp"] == sell_tp, f"the short at its average price, a sell order's stops: {opened}")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], real["pnl"]) and near(fin["pnl"], real["pnl"]),
          f"closed once: {closed} {real} {fin}")
    h.close()


@scenario
async def closes_that_raise_own_deals_and_late_reports():
    """A close that raised after it went through (in full or in part) with both views behind it past the back-off,
    an opposite fill that makes up the volume of our own deal still missing, positions whose deals are not in
    history when they are handed to the sweeper or closed by it, and a fill that reversed twice before a poll."""
    # slice 1 goes through with no answer (MT5Broker: unsure) and both views stay behind it: not sent again
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(114))
    await armed(h, 114)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    close, calls = b.close_position, [0]

    def no_answer(pos, comment="", price=None, volume=None):
        calls[0] += 1
        res = close(pos, comment, price, volume)
        if calls[0] == 1:
            err = BrokerError("order_send failed: (-10005, 'IPC timeout')")
            err.unsure = volume or pos["lots"]
            raise err
        return res
    b.close_position = no_answer
    stale_after(b, 1)
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await jump(0.6)                                   # past the back-off of the failed close
    await asyncio.sleep(0.9)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.8),
          f"slice 1 once: {b.calls_of('close')} {b.pos.get(pid)}")
    b.move(round(e + 5.6 * PIP, 5))                   # slice 2: out at once, nothing is unsure any more
    await until(lambda: len(b.calls_of("close")) > 1, "slice 2", 0.3)
    h.close()

    # a close with no answer that never reached the broker: not re-sent while its deal may still show, then after
    # BURST_UNSURE on the volume alone
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(121))
    await armed(h, 121)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    close, calls = b.close_position, [0]

    def lost(pos, comment="", price=None, volume=None):
        calls[0] += 1
        if calls[0] == 1:
            with b.lock:
                b.calls.append(("close", pos["ticket"], volume))
            raise RuntimeError("terminal not responding")
        return close(pos, comment, price, volume)
    b.close_position = lost
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await jump(0.6)                                   # past the back-off
    await asyncio.sleep(0.2)
    check(len(b.calls_of("close")) == 1 and near(b.pos[pid]["lots"], 1.0),
          f"not re-sent while it may still show: {b.calls_of('close')}")
    await jump(1.5)                                   # past BURST_UNSURE
    await until(lambda: len(b.calls_of("close")) == 2, "re-sent")
    await asyncio.sleep(0.1)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2] and near(b.pos[pid]["lots"], 0.8),
          f"then slice 1 once: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # 10010 for 0.1 of slice 1, then the rest refused, both views behind it: only the rest goes out
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(115))
    await armed(h, 115)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    close, calls = b.close_position, [0]

    def partly(pos, comment="", price=None, volume=None):
        calls[0] += 1
        if calls[0] == 1:
            with b.lock:
                b.calls.append(("close", pos["ticket"], volume))
                o = b._ticket()
                b._close(b.pos[pos["ticket"]], 0.1, b._px(b.pos[pos["ticket"]]), order=o)
            raise refused_after(0.1, [o])
        return close(pos, comment, price, volume)
    b.close_position = partly
    stale_after(b, 1)
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await jump(0.6)
    await until(lambda: len(b.calls_of("close")) >= 2, "the rest of slice 1")
    await asyncio.sleep(0.3)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.1] and near(b.pos[pid]["lots"], 0.8),
          f"the rest of slice 1 only: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the sell-stop (cancel frozen) fills 0.5 just before our slice-1 deal, which then lags both views; the price
    # reaches target 3 meanwhile. The stale pair adds up (0.5 left of 1.0), but our deal is not among its deals:
    # left alone until it is, then nothing is due (0.3 left is below the 0.4 target 3 leaves)
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(116))
    await armed(h, 116)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    b.move(1.10090)                                   # 1.0 at 1.10095
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    stale_after(b, 1)
    lagging = b.close_position

    def opposite_first(pos, comment="", price=None, volume=None):
        with b.lock:
            if sell in b.orders:
                b._fill({**b.orders.pop(sell), "lots": 0.5}, 1.09930)
            return lagging(pos, comment, price, volume)
    b.close_position = opposite_first
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    b.move(round(e + 7.5 * PIP, 5))                   # target 3 while both views lag
    await asyncio.sleep(1.2)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.3),
          f"nothing closed on the stale pair: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # a fill and its stop with none of its deals in history after BURST_GONE polls: handed to the sweeper, which
    # reports it once they show (not a close with no P&L)
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(117))
    await armed(h, 117)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    with b.lock:
        b.move(1.09960)                               # its SL
        for d in [d for d in b.deals if d["position"] == pid]:
            b.hide(d["order"], 10 ** 6)
    fin = await finished(h, 117)
    check(fin["status"] == "closed" and not h.of("trade_closed") and pid in h.c.unreported,
          f"handed to the sweeper: {h.of('trade_closed')} {fin}")
    with b.lock:
        b.hidden.clear()
        real = b.closed_result(pid)
    await h.c.sweep_orphans()
    closed = h.of("trade_closed")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], real["pnl"]) and real["pnl"] < 0
          and pid not in h.c.unreported, f"then reported with its P&L: {closed} {real}")
    h.close()

    # slice 1 out, then the list leaves the position out (handed to the sweeper); the sweeper's close lags in
    # history: not reported with slice 1's exit as its result, but once the history shows the close
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(118))
    await armed(h, 118)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: pid in b.pos and near(b.pos[pid]["lots"], 0.8), "slice 1")
    b.unlist(pid, 10 ** 6)
    fin = await finished(h, 118)
    check(fin["status"] == "closed" and pid in h.c.unreported and not h.of("trade_closed"), f"handed over: {fin}")
    close = b.close_position

    def unseen(pos, comment="", price=None, volume=None):
        with b.lock:
            k = len(b.deals)
            res = close(pos, comment, price, volume)
            for d in b.deals[k:]:
                b.hide(d["order"], 10 ** 6)
            return res
    b.close_position = unseen
    with b.lock:
        b.unlisted.clear()
    await h.c.sweep_orphans()
    check(not b.pos and not h.of("trade_closed") and pid in h.c.unreported,
          f"closed by the sweeper, not reported yet: {h.of('trade_closed')} {b.pos}")
    with b.lock:
        b.hidden.clear()
        real = b.closed_result(pid)
    await h.c.sweep_orphans()
    closed = h.of("trade_closed")
    check(len(closed) == 1 and near(closed[0]["trade"]["pnl"], real["pnl"]) and pid not in h.c.unreported,
          f"then reported once with all of its P&L: {closed} {real}")
    h.close()

    # b2 executed late and the SL, neither in history at the hand-off: the sweeper reports the 1.0 that entered
    # (at its average price) before its close
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(119))
    await armed(h, 119)
    b2 = next(t for t, o in b.orders.items() if o["comment"] == "fxb:119:b2")
    with b.lock:
        late = b.orders.pop(b2)
    b.move(1.10080)                                   # b1 fills 0.5 at 1.10085
    await until(lambda: h.of("trade_opened"), "first fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    with b.lock:
        b.bids["EURUSD"] = 1.10095
        k = len(b.deals)
        b._fill(late, b.ask("EURUSD"))                # b2 at 1.10100: 1.0 at 1.100925
        b.move(1.09900)                               # the SL (1.09975)
        for d in b.deals[k:]:
            b.hide(d["order"], 10 ** 6)
    await finished(h, 119)
    check([m["trade"]["lots"] for m in h.of("trade_opened")] == [0.5] and pid in h.c.unreported,
          f"handed over as the 0.5 its history showed: {h.of('trade_opened')}")
    with b.lock:
        b.hidden.clear()
    await h.c.sweep_orphans()
    opened, closed = h.of("trade_opened"), h.of("trade_closed")
    check([m["trade"]["lots"] for m in opened] == [0.5, 1.0] and near(opened[-1]["trade"]["entry_price"], 1.100925)
          and len(closed) == 1 and near(closed[0]["trade"]["pnl"], -192.5),
          f"the 1.0 reported, then its close: {opened} {closed}")
    h.close()

    # long, reversed short, reversed long again, and its SL, all before a poll: the first entry, then the last one
    b = SimBroker("netting", volume_max=0.5)
    h = Harness(b)
    await h.c.on_burst(plan(120))
    await armed(h, 120)
    o = {x["comment"]: t for t, x in b.orders.items()}
    with b.lock:
        b._fill({**b.orders.pop(o["fxb:120:b1"]), "lots": 0.2}, 1.1008)     # long 0.2
        b._fill({**b.orders.pop(o["fxb:120:s1"]), "lots": 0.4}, 1.0993)     # reversed: short 0.2
        b._fill({**b.orders.pop(o["fxb:120:b2"]), "lots": 0.4}, 1.1009)     # reversed again: long 0.2 at 1.1009
        b.move(1.09960)                                                      # b2's SL (1.09975)
        real = b.closed_result(next(d["position"] for d in b.deals))
    fin = await finished(h, 120)
    opened = [(m["trade"]["side"], m["trade"]["lots"], m["trade"]["entry_price"]) for m in h.of("trade_opened")]
    check([x[:2] for x in opened] == [("buy", 0.2), ("buy", 0.2)] and near(opened[0][2], 1.1008)
          and near(opened[1][2], 1.1009), f"the first entry, then the last: {opened}")
    check(len(h.of("trade_closed")) == 1 and near(fin["pnl"], real["pnl"]), f"closed once: {h.of('trade_closed')}")
    h.close()


@scenario
async def sync_gate_counts_own_closes_and_reports_with_their_burst():
    """What the history must show before slices are worked out: this runner's own closes by volume (a stop of the
    other side never stands in for one; a deal with no answer, one that went through in part and one that gave back
    no orders all count; a read that shows less than one before it is behind) and its deals by their orders; and
    burst positions reported with their burst: taken up at a crash, closed by a clean-up whose deal shows late,
    a reversal brought up to date only once."""
    # the sell-stop (cancel frozen) fills 0.2 just before slice 1, which goes through with no answer; both views lag
    # our deal while the price reaches target 2: the stop's 0.2 does not stand in for the close that may be missing
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(122))
    await armed(h, 122)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    b.move(1.10090)                                   # 1.0 at 1.10095
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    no_answer_once(b)
    stale_after(b, 1)
    before_first_close(b, lambda: b._fill({**b.orders.pop(sell), "lots": 0.2}, 1.09930))
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    b.move(round(e + 5.6 * PIP, 5))                   # target 2 while both views lag
    await asyncio.sleep(1.2)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.6),
          f"the stop's 0.2 is not our slice 1: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the same with a 0.2 the user closed by hand (an order not the burst's): it adds up to our slice, but our deal is
    # not among the history's: left alone until it is
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(132))
    await armed(h, 132)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    stale_after(b, 1)
    before_first_close(b, lambda: b._close(b.pos[pid], 0.2, b._px(b.pos[pid]), order=b._ticket(), magic=0))
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    b.move(round(e + 5.6 * PIP, 5))                   # target 2 while both views lag
    await asyncio.sleep(1.2)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.6),
          f"a close by hand does not stand in for our deal: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the stop fills 0.5 just before slice 1 (as in #116) and both views stay behind our deal past BURST_UNSURE: a
    # read showing less of our own closes than this runner made is behind however long it lasts (nothing goes out at
    # target 3: 0.3 left is below the 0.4 it leaves)
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(123))
    await armed(h, 123)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    stale_after(b, 1, seconds=1.5)
    before_first_close(b, lambda: b._fill({**b.orders.pop(sell), "lots": 0.5}, 1.09930))
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    b.move(round(e + 7.5 * PIP, 5))                   # target 3
    await jump(2.1)                                   # past BURST_UNSURE, both views still behind
    await asyncio.sleep(0.3)
    check(len(b.calls_of("close")) == 1, f"nothing on the stale pair past BURST_UNSURE: {b.calls_of('close')}")
    await asyncio.sleep(1.4)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.3),
          f"then nothing due: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # slice 1 goes through but the broker gives back no orders of its deals, and both views lag it while the price
    # reaches target 2: by its volume alone slice 1 is waited for, then slice 2 goes out once
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(124))
    await armed(h, 124)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    close = b.close_position

    def no_orders(pos, comment="", price=None, volume=None):
        close(pos, comment, price, volume)
    b.close_position = no_orders
    stale_after(b, 1)
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    b.move(round(e + 5.6 * PIP, 5))                   # target 2 while both views lag
    await until(lambda: len(b.calls_of("close")) > 1, "slice 2")
    await asyncio.sleep(0.3)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2] and near(b.pos[pid]["lots"], 0.6),
          f"slice 2 once, after slice 1 shows: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # 10010 for 0.1 of slice 1, then the rest refused with no orders given back, both views behind it: the 0.1 counts
    # by its volume, only the rest goes out
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(125))
    await armed(h, 125)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    close, calls = b.close_position, [0]

    def partly(pos, comment="", price=None, volume=None):
        calls[0] += 1
        if calls[0] == 1:
            with b.lock:
                b.calls.append(("close", pos["ticket"], volume))
                b._close(b.pos[pos["ticket"]], 0.1, b._px(b.pos[pos["ticket"]]))
            raise refused_after(0.1, [])
        return close(pos, comment, price, volume)
    b.close_position = partly
    stale_after(b, 1)
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await jump(0.6)                                   # past the back-off of the failed close
    await until(lambda: len(b.calls_of("close")) >= 2, "the rest of slice 1")
    await asyncio.sleep(0.3)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.1] and near(b.pos[pid]["lots"], 0.8),
          f"the rest of slice 1 only: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # slice 1 goes through with no answer and the history shows it at once: from then on it counts as ours, so slice
    # 2's deal lagging both views past BURST_UNSURE is waited for (then only target 3 goes out)
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(126))
    await armed(h, 126)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e, tr = b.pos[pid]["entry_price"], h.c.bursts[126]["trades"][pid]
    no_answer_once(b)
    stale_after(b, 2, seconds=1.5)
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: b.calls_of("close"), "slice 1")
    await jump(0.6)                                   # past the back-off
    await until(lambda: tr["done"] == 1, "slice 1 seen in history")
    b.move(round(e + 5.6 * PIP, 5))                   # slice 2
    await until(lambda: len(b.calls_of("close")) == 2, "slice 2")
    b.move(round(e + 7.5 * PIP, 5))                   # target 3
    await jump(2.1)                                   # past BURST_UNSURE, both views still behind slice 2
    await asyncio.sleep(0.3)
    check(len(b.calls_of("close")) == 2, f"nothing on the stale pair: {b.calls_of('close')}")
    await until(lambda: len(b.calls_of("close")) == 3, "target 3")
    await asyncio.sleep(0.2)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2, 0.2] and near(b.pos[pid]["lots"], 0.4),
          f"each slice once: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the stop fills 0.2 and a poll the views agree on sees it; then both views go back to before it (a read behind
    # one before it) with the price at target 1: nothing goes out (the stop's 0.2 stands for slice 1)
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(127))
    await armed(h, 127)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e, tr = b.pos[pid]["entry_price"], h.c.bursts[127]["trades"][pid]
    with b.lock:
        snap = b.positions()
        b._fill({**b.orders.pop(sell), "lots": 0.2}, 1.09930)
    await asyncio.sleep(0.2)
    check(near(tr["out"], 0.2) and near(tr["own"], 0.0), f"the stop's 0.2 seen, not as ours: {tr}")
    views_back(b, snap, [sell], 0.8)
    b.move(round(e + 4.5 * PIP, 5))                   # target 1
    await asyncio.sleep(1.1)
    check(not b.calls_of("close") and near(b.pos[pid]["lots"], 0.8),
          f"nothing on a read behind an earlier one: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the same before the first poll: the stop's 0.2 is in the history when the trade is taken up, and both views go
    # back to before it once its trade_opened is sent
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(128))
    await armed(h, 128)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    with b.lock:
        b.move(1.10090)
        pid = next(iter(b.pos))
        e, snap = b.pos[pid]["entry_price"], b.positions()
        b._fill({**b.orders.pop(sell), "lots": 0.2}, 1.09930)
    send, back = h.c.send, []

    async def opened_then_back(msg, keep=False):
        await send(msg, keep)
        if msg["type"] == "trade_opened" and not back:
            back.append(msg)
            views_back(b, snap, [sell], 0.8)
            b.move(round(e + 4.5 * PIP, 5))           # target 1
    h.c.send = opened_then_back
    await until(lambda: back, "fill")
    await asyncio.sleep(1.1)
    check([m["trade"]["lots"] for m in h.of("trade_opened")] == [1.0] and not b.calls_of("close")
          and near(b.pos[pid]["lots"], 0.8), f"what left it counted from its first poll: {b.calls_of('close')} "
                                            f"{h.of('trade_opened')} {b.pos.get(pid)}")
    h.close()

    # the terminal stops answering as the buy-stop fills, before a poll saw it: the clean-up takes the position up
    # (trade_opened), closes it and reports the close with its burst
    b = SimBroker("netting")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    await h.c.on_burst(plan(129))
    await armed(h, 129)
    reads = b.positions

    def dead_until_cleanup(all_magic=False):          # the clean-up cancels first: the runner's polls never see the fill
        if b.broken and not b.calls_of("cancel"):
            raise RuntimeError("terminal disconnected")
        return reads(all_magic)
    b.positions = dead_until_cleanup
    with b.lock:
        b.broken = True
        b.move(1.10090)
    fin = await finished(h, 129)
    opened, closed = h.of("trade_opened"), h.of("trade_closed")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in opened] == [("buy", 1.0)] and not b.pos
          and len(closed) == 1 and closed[0]["trade"].get("burst_id") == 129,
          f"taken up, closed and reported with its burst: {opened} {closed}")
    check(fin["status"] == "error" and fin["fills"] == 5 and near(fin["pnl"], closed[0]["trade"]["pnl"]), f"{fin}")
    h.close()

    # slice 1 out, then the terminal stops answering; the clean-up close's deal shows in history a moment late: its
    # report waits for it (slice 1's exit is not its result)
    b = SimBroker("netting")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    h.c.REPORT_TRIES = 100                            # ≈ 1 s of history reads: longer than the deal's lag below
    await h.c.on_burst(plan(130))
    await armed(h, 130)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    b.move(round(e + 4.5 * PIP, 5))                   # slice 1
    await until(lambda: pid in b.pos and near(b.pos[pid]["lots"], 0.8), "slice 1")
    close = b.close_position

    def late_in_history(pos, comment="", price=None, volume=None):
        with b.lock:
            k = len(b.deals)
            res = close(pos, comment, price, volume)
            for d in b.deals[k:]:
                b.hide(d["order"], 0.3)
            return res
    b.close_position = late_in_history
    b.broken = True
    fin = await finished(h, 130)
    await asyncio.sleep(0.4)                          # the clean-up close's deal in history: the real result
    real, closed = b.closed_result(pid), h.of("trade_closed")
    check(not b.pos and len(closed) == 1 and near(closed[0]["trade"]["pnl"], real["pnl"])
          and near(fin["pnl"], real["pnl"]), f"reported with all of its P&L: {closed} {real} {fin}")
    h.close()

    # a stop of the other side reverses the position and the list leaves it out: the reversal is reported when the
    # position goes to the sweeper, and not again when the sweeper closes it
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(131))
    await armed(h, 131)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    b.move(1.10090)                                   # 1.0 at 1.10095
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    with b.lock:
        b._fill({**b.orders.pop(sell), "lots": 1.5}, 1.09930)     # reversed: short 0.5
        b.unlist(pid, 10 ** 6)
    fin = await finished(h, 131)
    check(fin["status"] == "closed" and pid in h.c.unreported and not h.of("trade_closed"), f"handed over: {fin}")
    with b.lock:
        b.unlisted.clear()
    await h.c.sweep_orphans()
    opened, closed = [(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")], h.of("trade_closed")
    check(opened == [("buy", 1.0), ("sell", 0.5)] and not b.pos and len(closed) == 1
          and closed[0]["trade"].get("burst_id") == 131, f"the reversal reported once: {opened} {closed}")
    h.close()


def lost_placement_reply(b, sides=("sell",)):
    """place_pending wrapped: an order of `sides` goes into the book, but the call raises as MT5Broker does when
    order_send answers None (the reply lost)."""
    place = b.place_pending

    def lost(symbol, side, lots, price, sl, tp, comment="", expire_in=None):
        t = place(symbol, side, lots, price, sl, tp, comment, expire_in)
        if side in sides:
            raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
        return t
    b.place_pending = lost


def dead_while_broken(b):
    """Every terminal call raises while b.broken (a dead IPC), not only pending_orders."""
    for name in ("positions", "cancel_order", "deal_entry", "position_deals", "closed_result", "close_position",
                 "quote", "account"):
        f = getattr(b, name)

        def wrap(*a, _f=f, **k):
            if b.broken:
                raise RuntimeError("(-10005, 'IPC timeout')")
            return _f(*a, **k)
        setattr(b, name, wrap)


@scenario
async def unanswered_orders_hand_closes_and_unseen_fills():
    """Orders placed with no answer, a close by hand while a close of ours had none, fills that came and went while
    the terminal did not answer, and an orphan that changed after the sweep that took its info."""
    # the sell-stop goes into the book with no answer: followed by its comment, so the buy's fill cancels it (OCO)
    b = SimBroker("netting")
    h = Harness(b)
    lost_placement_reply(b)
    await h.c.on_burst(plan(140))
    await armed(h, 140)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    check(sell in h.c.bursts[140]["pending"] and h.updates(140)[-1]["orders"] == 2,
          f"followed: {h.c.bursts[140]['pending']} {h.updates(140)[-1]}")
    b.move(1.10090)
    await until(lambda: sell not in b.orders and h.of("trade_opened"), "the fill and the OCO cancel")
    check(("cancel", sell, "ok") in b.calls_of("cancel") and len(h.of("trade_opened")) == 1, f"{b.calls_of('cancel')}")
    h.close()

    # no placement answered: the burst is armed with the orders the book shows, not an error
    b = SimBroker("netting")
    h = Harness(b)
    lost_placement_reply(b, ("buy", "sell"))
    await h.c.on_burst(plan(141))
    await armed(h, 141)
    check(sorted(h.c.bursts[141]["pending"]) == sorted(b.orders) and h.updates(141)[-1]["orders"] == 2,
          f"armed with both: {h.updates(141)}")
    h.close()

    # slice 1 goes through with no answer (no order back) and a 0.2 closed by hand lands just before it; both views
    # then lag our deal: the hand close (magic 0) never stands in for it, target 2 sends nothing more
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(142))
    await armed(h, 142)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    no_answer_once(b)
    stale_after(b, 1)
    before_first_close(b, lambda: b._close(b.pos[pid], 0.2, b._px(b.pos[pid]), order=b._ticket(), magic=0))
    b.move(round(e + 4.5 * PIP, 5))                   # target 1
    await until(lambda: b.calls_of("close"), "slice 1")
    b.move(round(e + 5.6 * PIP, 5))                   # target 2 while both views lag our deal
    await asyncio.sleep(1.2)
    check([c[2] for c in b.calls_of("close")] == [0.2] and near(b.pos[pid]["lots"], 0.6),
          f"a hand close is not ours: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the terminal stops answering as the stops fill and hit their TP (hedging) / SL (netting): the clean-up finds
    # the fills in history and reports them, opened and closed, in the burst's result
    for mode, bid, first, second, opened in (("hedging", 143, 1.10080, 1.10200, 5), ("netting", 144, 1.10080, 1.09960, 1)):
        b = SimBroker(mode)
        h = Harness(b)
        h.c.BURST_MAX_ERRORS = 5
        await h.c.on_burst(plan(bid))
        await armed(h, bid)
        bal = b.balance
        with b.lock:
            b.broken = True
            b.move(first)
            b.move(second)
        fin = await finished(h, bid)
        real = round(b.balance - bal, 2)
        check(len(h.of("trade_opened")) == opened and len(h.of("trade_closed")) == opened and real
              and near(fin["pnl"], real) and fin["side"] == "buy" and fin["fills"] == 5,
              f"{mode}: fills that came and went reported: {fin} {real} {h.of('trade_closed')}")
        h.close()

    # ... and with every call failing until the clean-up is over: the sweeper finds them once the terminal is back
    for mode, bid, first, second, opened in (("hedging", 145, 1.10080, 1.10200, 5), ("netting", 146, 1.10080, 1.09960, 1)):
        b = SimBroker(mode)
        h = Harness(b)
        h.c.BURST_MAX_ERRORS = 5
        await h.c.on_burst(plan(bid))
        await armed(h, bid)
        dead_while_broken(b)
        bal = b.balance
        with b.lock:
            b.broken = True
            b.move(first)
            b.move(second)
        fin = await finished(h, bid)
        check(not h.of("trade_closed") and h.c.unchecked, f"{mode}: left to the sweeper {fin} {h.c.unchecked}")
        b.broken = False
        await h.c.sweep_orphans()
        await h.c.sweep_orphans()
        closed = h.of("trade_closed")
        check(len(h.of("trade_opened")) == opened and len(closed) == opened and not b.orders and not b.pos
              and all(m["trade"].get("burst_id") == bid for m in closed)
              and near(sum(m["trade"]["pnl"] for m in closed), round(b.balance - bal, 2)),
              f"{mode}: reported by the sweeper {h.of('trade_opened')} {closed}")
        check(not any(m["trade"]["ticket"] in h.c.unchecked for m in closed), f"done with: {h.c.unchecked}")
        h.close()

    # an orphan whose sweeper close raised changes before the next sweep (its frozen opposite stop reverses it):
    # its report says what it was at its close, not what the earlier sweep saw
    b = SimBroker("netting")
    h = Harness(b)
    pid = b.add_position("EURUSD", "buy", 1.0, "fxb:99:b")
    sell = b.add_order("EURUSD", "sell", 1.5, 1.09900, "fxb:99:s")
    b.frozen[sell], b.fail_close = 1, 1
    await h.c.sweep_orphans()
    check(pid in b.pos and pid in h.c.unreported and not h.of("trade_closed"), f"sweep 1 {h.c.unreported}")
    b.move(1.09890)                                   # short 0.5 at 1.0989 (the identifier stays)
    await h.c.sweep_orphans()
    t = h.of("trade_closed")[0]["trade"] if h.of("trade_closed") else {}
    check(not b.pos and len(h.of("trade_closed")) == 1 and (t["side"], t["lots"], t["entry_price"]) == ("sell", 0.5, 1.0989),
          f"reported as closed: {h.of('trade_closed')}")
    h.close()


@scenario
async def late_answers_tagged_positions_and_changed_orphans():
    """A close of ours that shows late never stands in for a later one; an order with no answer that the first look
    misses or that fills before any look; a fill of a followed position the clean-up finds; orphans that changed
    before their close was reported."""
    # slice 1 goes through with no answer and both views miss it past BURST_UNSURE: taken as not through and sent
    # again (answered). The views then show the first deal, not the second: as much volume of ours as expected, yet
    # not our deal (by its order), so target 2 waits for the history instead of sending a third slice
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(147))
    await armed(h, 147)
    b.move(1.10090)
    await until(lambda: h.of("trade_opened"), "fill")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    no_answer_once(b)
    close, positions, view, sent = b.close_position, b.positions, {"snap": None}, []

    def behind(pos, comment="", price=None, volume=None):
        with b.lock:                                  # both views stay where they were before this deal ...
            before, k = positions(), len(b.deals)
            try:
                return close(pos, comment, price, volume)
            finally:
                sent.append([d["order"] for d in b.deals[k:]])
                view["snap"] = before
                for d in b.deals[k:]:
                    b.hide(d["order"], 10 ** 6)
                if len(sent) == 2:                    # ... and the first deal shows from the second on
                    for o in sent[0]:
                        b.hidden.pop(o, None)
    b.close_position = behind
    b.positions = lambda all_magic=False: view["snap"] if view["snap"] is not None else positions(all_magic)
    b.move(round(e + 4.5 * PIP, 5))                   # target 1
    await until(lambda: len(sent) == 1, "slice 1")
    await jump(2.1)                                   # past BURST_UNSURE and the back-off: sent again
    await until(lambda: len(sent) == 2, "slice 1 again")
    await asyncio.sleep(0.6)                          # past the settle
    b.move(round(e + 5.6 * PIP, 5))                   # target 2 while both views lag the second deal
    await asyncio.sleep(0.5)
    with b.lock:
        view["snap"] = None
        b.hidden.clear()
    await asyncio.sleep(0.5)
    check([c[2] for c in b.calls_of("close")] == [0.2, 0.2] and near(b.pos[pid]["lots"], 0.6),
          f"target 2 is already out: {b.calls_of('close')} {b.pos.get(pid)}")
    h.close()

    # the sell-stop goes into the book with no answer and the look right after shows an empty book: followed from a
    # poll's look, so the buy's fill cancels it
    b = SimBroker("netting")
    h = Harness(b)
    place = b.place_pending

    def lost_then_blank(symbol, side, lots, price, sl, tp, comment="", expire_in=None):
        t = place(symbol, side, lots, price, sl, tp, comment, expire_in)
        if side == "sell":
            b.blank = 1
            raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
        return t
    b.place_pending = lost_then_blank
    await h.c.on_burst(plan(148))
    await armed(h, 148)
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    await until(lambda: sell in h.c.bursts[148]["pending"], "the sell-stop followed")
    b.move(1.10090)
    await until(lambda: sell not in b.orders and h.of("trade_opened"), "the OCO cancel")
    h.close()

    # the buy-stop goes into the book with no answer and fills before any look at the book: its position is the
    # burst's by its comment, so it is followed and the sell-stop cancelled
    b = SimBroker("netting")
    h = Harness(b)
    place = b.place_pending

    def lost_and_filled(symbol, side, lots, price, sl, tp, comment="", expire_in=None):
        t = place(symbol, side, lots, price, sl, tp, comment, expire_in)
        if side == "buy":
            b.move(1.10090)
            raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
        return t
    b.place_pending = lost_and_filled
    await h.c.on_burst(plan(149))
    await armed(h, 149)
    await until(lambda: h.of("trade_opened") and not b.orders, "followed by its comment, the sell-stop cancelled")
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 1.0)],
          f"{h.of('trade_opened')}")
    h.close()

    # a followed long, the OCO cancel frozen, then (the terminal not answering) the sell-stop reverses it and the short
    # hits its SL: the clean-up finds the sell-stop's fill in the followed position, so it takes up nothing new and
    # reports the short as the position's new entry
    b = SimBroker("netting")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    await h.c.on_burst(plan(150))
    await armed(h, 150)
    buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
    sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[sell] = 10 ** 6
    with b.lock:                                      # IOC: 0.4 of the 1.0 buy-stop
        b.bids["EURUSD"] = 1.10035
        b._fill({**b.orders.pop(buy), "lots": 0.4}, b.ask("EURUSD"))
    await until(lambda: h.of("trade_opened"), "long")
    bal = b.balance
    with b.lock:
        b.broken = True
        b.move(1.09925)                               # the sell-stop: a 0.6 short
        b.move(1.10040)                               # its SL
    fin = await finished(h, 150)
    real = round(b.balance - bal, 2)
    check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 0.4), ("sell", 0.6)]
          and len(h.of("trade_closed")) == 1 and real and near(fin["pnl"], real),
          f"nothing new taken up: {h.of('trade_opened')} {h.of('trade_closed')} {fin} {real}")
    h.close()

    # an orphan whose sweeper close raised is reversed by its frozen opposite stop and the short is closed, all
    # before the next sweep: reported as its history shows it once flat (the short), not as the first sweep saw it
    b = SimBroker("netting")
    h = Harness(b)
    pid = b.add_position("EURUSD", "buy", 1.0, "fxb:99:b")
    sell = b.add_order("EURUSD", "sell", 1.5, 1.09900, "fxb:99:s")
    b.frozen[sell], b.fail_close = 1, 1
    await h.c.sweep_orphans()
    check(pid in b.pos and pid in h.c.unreported and not h.of("trade_closed"), f"sweep 1 {h.c.unreported}")
    with b.lock:
        b.move(1.09890)                               # short 0.5 at 1.0989
        short = next(iter(b.pos.values()))
        b._close(short, 0.5, b._px(short))
    await h.c.sweep_orphans()
    t = h.of("trade_closed")[0]["trade"] if h.of("trade_closed") else {}
    check(len(h.of("trade_closed")) == 1 and (t["side"], t["lots"], t["entry_price"]) == ("sell", 0.5, 1.0989),
          f"reported as closed: {h.of('trade_closed')}")
    h.close()

    # an orphan grown by a fill whose deal the history shows only after the sweeper's close: its report has all
    # that entered it, as its history shows once flat
    b = SimBroker("netting")
    h = Harness(b)
    h.c.REPORT_TRIES = 100
    pid = b.add_position("EURUSD", "buy", 0.5, "fxb:99:b1")
    with b.lock:
        b.bids["EURUSD"] = 1.10100
        late = b.add_position("EURUSD", "buy", 0.5, "fxb:99:b2")      # merges into pid at 1.10105
        b.hide(late, 0.3)
    await h.c.sweep_orphans()
    t = h.of("trade_closed")[0]["trade"] if h.of("trade_closed") else {}
    check(not b.pos and len(h.of("trade_closed")) == 1 and t["ticket"] == pid
          and (t["side"], t["lots"]) == ("buy", 1.0) and near(t["entry_price"], 1.10055),
          f"reported with the 1.0: {h.of('trade_closed')}")
    h.close()


def lost_replies_unread(b, reads):
    """Every placement goes into the book with no answer, and the next `reads` looks at the book fail."""
    place, book, failing = b.place_pending, b.pending_orders, [0]

    def lost(symbol, side, lots, price, sl, tp, comment="", expire_in=None):
        place(symbol, side, lots, price, sl, tp, comment, expire_in)
        failing[0] = reads
        raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")

    def pending_orders(symbol=None):
        if failing[0]:
            failing[0] -= 1
            raise RuntimeError("orders_get failed: (-10005, 'IPC timeout')")
        return book(symbol)
    b.place_pending, b.pending_orders = lost, pending_orders


@scenario
async def unread_strays_and_a_sweep_during_the_clean_up():
    """Orders sent with no answer while the book cannot be read; the sweeper's tick during a crashed burst's
    clean-up."""
    # no placement answered and the looks after fail for a while: the burst waits for a look that shows its orders
    b = SimBroker("netting")
    h = Harness(b)
    lost_replies_unread(b, 3)
    await h.c.on_burst(plan(151))
    await armed(h, 151)
    check(sorted(h.c.bursts[151]["pending"]) == sorted(b.orders) and h.updates(151)[-1]["orders"] == 2,
          f"armed with both: {h.updates(151)}")
    h.close()

    # ... and with a burst_cancel meanwhile: both found, then cancelled
    b = SimBroker("netting")
    h = Harness(b)
    lost_replies_unread(b, 3)
    place = b.place_pending

    def cancelled(*a, **k):
        h.c.bursts[152]["cancel"] = True
        return place(*a, **k)
    b.place_pending = cancelled
    await h.c.on_burst(plan(152))
    fin = await finished(h, 152)
    check(fin["status"] == "cancelled" and not b.orders, f"nothing left in the book: {fin} {b.orders}")
    h.close()

    # ... the looks keep failing until the burst gives up: its clean-up looks once more and cancels what it finds
    b = SimBroker("netting")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 3
    lost_replies_unread(b, 3)
    await h.c.on_burst(plan(153))
    fin = await finished(h, 153)
    check(fin["status"] == "error" and not b.orders and len(b.calls_of("cancel")) == 2,
          f"cleaned up: {fin} {b.orders} {b.calls_of('cancel')}")
    h.close()

    # hedging, the sell-stop's OCO cancel frozen; the terminal stops answering as the buys hit their SL and the
    # sell-stop fills and hits its TP, its deals late in history. A sweep while the clean-up still waits for a close
    # to show leaves that burst alone: every trade is reported closed once, with all of the P&L
    b = SimBroker("hedging")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    h.c.REPORT_TRIES = 300
    await h.c.on_burst(plan(154))
    await armed(h, 154)
    s0 = min(t for t, o in b.orders.items() if o["side"] == "sell")
    b.frozen[s0] = 10 ** 6
    b.move(1.10080)
    await until(lambda: len(h.of("trade_opened")) == 5, "5 buys followed")
    buys = [m["trade"]["ticket"] for m in h.of("trade_opened")]
    bal = b.balance
    with b.lock:
        b.broken = True
        k = len(b.deals)
        b.move(1.09925)                               # the buys' SL, s0 fills
        b.move(1.09880)                               # s0's TP
        for d in b.deals[k:]:
            if d["position"] == buys[0]:
                b.hide(d["order"], 1.2)
            if d["position"] == s0:
                b.hide(d["order"], 0.4)
    await until(lambda: s0 in h.c.unchecked, "s0 left to look up")
    b.broken = False
    await asyncio.sleep(0.45)
    await h.c.sweep_orphans()                         # inside the clean-up's report loop
    check(s0 in h.c.unchecked and s0 not in h.c.bursts[154]["trades"], "left alone while its clean-up runs")
    await finished(h, 154)
    for _ in range(3):
        await h.c.sweep_orphans()
    kinds = {}
    for m in h.msgs:
        if m["type"] in ("trade_opened", "trade_closed"):
            kinds.setdefault(m["trade"]["ticket"], []).append(m["type"][6:])
    real = round(b.balance - bal, 2)
    check(len(kinds) == 6 and all(v == ["opened", "closed"] for v in kinds.values()) and not h.c.unchecked
          and near(sum(m["trade"]["pnl"] for m in h.of("trade_closed")), real),
          f"each reported closed once: {kinds} {real} {h.of('trade_closed')}")
    h.close()


@scenario
async def strays_and_positions_by_the_runs_own_tag():
    """A run takes as its own only what carries its own tag, kind and id: the orders of other runs left in the book
    (an earlier burst that crashed, a Rapid run) are the sweeper's."""
    # the sell-stop goes in with no answer while other runs' orders wait on that symbol: only its own is followed,
    # and the buy's fill cancels it (OCO), not theirs
    b = SimBroker("netting")
    h = Harness(b)
    others = [b.add_order("EURUSD", "sell", 0.1, 1.09000, c) for c in ("fxb:99:0", "fxr:99:0", "fxr:156:0")]
    lost_placement_reply(b)
    await h.c.on_burst(plan(156))
    await armed(h, 156)
    st = h.c.bursts[156]
    sell = next(t for t, o in b.orders.items() if o["comment"] == "fxb:156:s")
    check(sell in st["pending"] and h.c.burst_owned.get(sell) == 156 and st["orders"] == 2
          and not set(others) & (st["placed"] | set(st["pending"]) | set(h.c.burst_owned)),
          f"its own followed, the others not: {st['placed']} {h.c.burst_owned} {others}")
    b.move(1.10090)
    await until(lambda: sell not in b.orders and h.of("trade_opened"), "the fill and the OCO cancel")
    await asyncio.sleep(0.3)                          # a few polls more
    check(all(t in b.orders for t in others) and not [c for c in b.calls_of("cancel") if c[1] in others]
          and len(h.of("trade_opened")) == 1, f"the other runs' orders left alone: {b.calls_of('cancel')} {b.orders}")
    h.close()

    # a Rapid run and a news burst with the same id: each adopts and owns only what carries its own kind's tag
    b = SimBroker("hedging")
    h = Harness(b)
    rapid = {"id": 7, "kind": "rapid", "symbol": "EURUSD", "placed": set(), "pending": {}, "orders": 0}
    burst = {"id": 7, "symbol": "EURUSD", "placed": set(), "pending": {}, "orders": 0}       # no kind: a news burst
    fxr, fxb = (b.add_order("EURUSD", "buy", 0.1, 1.10500, f"{c}:7:0") for c in ("fxr", "fxb"))
    for st in (rapid, burst):
        h.c._burst_strays(st, b.pending_orders("EURUSD"))
    check((rapid["placed"], set(rapid["pending"]), rapid["orders"]) == ({fxr}, {fxr}, 1)
          and (burst["placed"], set(burst["pending"]), burst["orders"]) == ({fxb}, {fxb}, 1),
          f"each follows its own kind's order only: {rapid} {burst}")
    for c in ("fxr:7:1", "fxb:7:1"):
        b.add_position("EURUSD", "buy", 0.1, c)
    held = {p["comment"]: p for p in b.positions()}
    owns = [h.c._owns(st, held[c]) for st in (rapid, burst) for c in ("fxr:7:1", "fxb:7:1")]
    check(owns == [True, False, False, True], f"each owns its own kind's position only: {owns}")
    h.close()


@scenario
async def repeated_close_request_and_offline_reports():
    b = SimBroker("hedging")
    h = Harness(b)
    sig = signal(b, 711)
    await h.c.execute(sig)
    t = h.of("trade_opened")[-1]["trade"]["ticket"]
    b.fail_close = 10 ** 6
    await h.c.close_ticket(t, "closed from web app")
    await jump(3)
    await h.c.close_ticket(t, "closed from web app")  # Close clicked again while queued: answered again
    warns = [m for m in h.logs("warn") if f"Could not close #{t}:" in m]
    check(len(warns) == 2 and h.c.closing[t]["tries"] == 1, f"a new request is answered: {warns} {h.c.closing}")
    await jump(3)
    await h.c.retry_closes()
    check(len(b.calls_of("close")) == 6, "its retries start over from the new request (5 s)")
    await jump(2.5)
    await h.c.retry_closes()
    check(len(b.calls_of("close")) == 9 and len([m for m in h.logs("warn") if f"#{t}" in m]) == 2,
          "close_loop's own retries stay quiet")
    h.close()

    b = SimBroker("hedging")
    h = Harness(b)
    del h.c.send                                      # the real send(), the server unreachable
    sig = signal(b, 712)
    await h.c.execute(sig)
    t = next(iter(b.pos))
    b.fail_close = 10 ** 6
    await h.c.close_ticket(t, "signal #712 closed")
    await h.c.report("an info line")
    for _ in range(12):
        await jump(60)
        await h.c.retry_closes()
    kept = [(m["level"], m["message"]) for m in h.c.outbox if m["type"] == "log"]
    check(len(kept) == 2 and kept[0][0] == "warn" and kept[1][0] == "error" and "Gave up" in kept[1][1]
          and all(f"#{t}:" in m for _, m in kept),
          f"the close warning and the give-up wait for the server, other logs do not: {kept}")
    # a warned close that then succeeds, or finds the position gone: the line settling it waits too
    h.c.outbox.clear()
    b.fail_close = 2 * 3                              # the request and the first retry fail, the next one closes
    had = set(b.pos)
    await h.c.execute(signal(b, 713, "GBPUSD"))
    (t2,) = set(b.pos) - had
    await h.c.close_ticket(t2, "signal #713 closed")
    for _ in range(3):
        await jump(10.1)
        await h.c.retry_closes()
    check(t2 not in b.pos, "closed on a retry")
    had = set(b.pos)
    await h.c.execute(signal(b, 714, "GBPUSD"))
    (t3,) = set(b.pos) - had
    b.fail_close = 10 ** 6
    await h.c.close_ticket(t3, "signal #714 closed")
    with b.lock:
        del b.pos[t3]                                 # its stop-loss hit meanwhile
    await jump(5.1)
    await h.c.retry_closes()
    kept = [(m["type"], m.get("level"), m.get("message", "")) for m in h.c.outbox if m["type"] != "trade_opened"]
    check([k[:2] for k in kept] == [("log", "warn"), ("log", "info"), ("trade_closed", None), ("log", "warn"),
                                    ("log", "info"), ("trade_closed", None)]
          and kept[1][2].startswith(f"Closed #{t2} ") and kept[4][2] == f"#{t3} is closed (signal #714 closed)"
          and not h.c.closing, f"warning, then what settled it: {kept}")
    h.close()


@scenario
async def failed_close_retried_until_the_position_is_gone():
    b = SimBroker("hedging")
    h = Harness(b)
    sig = signal(b, 701)
    await h.c.execute(sig)
    t = h.of("trade_opened")[-1]["trade"]["ticket"]
    b.fail_close = 10 ** 6                            # a requote spell longer than the quick attempts
    await h.c.on_signal_closed({**sig, "status": "closed", "close_reason": "target"})
    check(t in b.pos and t in h.c.closing and len(b.calls_of("close")) == 3, "3 quick attempts, then queued")
    check(sum(f"Could not close #{t}:" in m and "Retrying" in m for m in h.logs("warn")) == 1 and not h.logs("error"),
          f"one warning, '#ticket:' first (the web app's test card reads it): {h.logs()}")
    await h.c.retry_closes()
    check(len(b.calls_of("close")) == 3, "not before its pause")
    await jump(5.1)
    await h.c.retry_closes()
    check(len(b.calls_of("close")) == 6 and h.c.closing[t]["tries"] == 2, "retried 5 s later")
    await jump(5.1)
    await h.c.retry_closes()
    check(len(b.calls_of("close")) == 6, "the pause grows (10 s)")
    b.fail_close = 0
    await jump(5.1)
    h.c.SNAPSHOT_EVERY = 0.01
    loop = asyncio.create_task(h.c.close_loop())      # the periodic loop does the retry (with or without a server)
    try:
        await until(lambda: h.of("trade_closed"), "closed and reported")
    finally:
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
    check(t not in b.pos and not h.c.closing and [m["trade"]["ticket"] for m in h.of("trade_closed")] == [t]
          and len(h.logs("warn")) == 1, "closed on a retry, one trade_closed, no new warning")

    sig2 = signal(b, 702, "GBPUSD")
    await h.c.execute(sig2)
    t2 = h.of("trade_opened")[-1]["trade"]["ticket"]
    b.fail_close = 10 ** 6
    await h.c.close_ticket(t2, "closed from web app")
    for _ in range(15):
        await jump(60)
        await h.c.retry_closes()
    errors = [m for m in h.logs("error") if f"#{t2}" in m]
    check(len(errors) == 1 and "10 min" in errors[0] and f"#{t2}:" in errors[0] and t2 not in h.c.closing
          and t2 in b.pos,
          f"given up after 10 min, reported once: {errors}")

    sig3 = signal(b, 703)
    await h.c.execute(sig3)
    t3 = h.of("trade_opened")[-1]["trade"]["ticket"]
    await h.c.close_ticket(t3, "signal #703 closed")
    check(t3 in h.c.closing, "queued")
    b.move(b.bids["EURUSD"] - 25 * PIP)               # its stop-loss hits meanwhile
    sent = len(b.calls_of("close"))
    await jump(5.1)
    await h.c.retry_closes()
    check(t3 not in b.pos and not h.c.closing and len(b.calls_of("close")) == sent and len(h.logs("error")) == 1,
          "gone: dropped without another close or an error")
    h.close()


@scenario
async def unreadable_positions_on_signal_closed_and_dry_run_queue():
    b = SimBroker("hedging")
    h = Harness(b)
    sig = signal(b, 851)
    await h.c.execute(sig)
    t = next(iter(b.pos))
    h.c.known.pop(t)                                  # as if the snapshot right after the open had failed
    check(t not in h.c.known and h.c.signal_of.get(t) == 851, "opened, but the last snapshot missed it")
    b.pos_errors = 1
    await h.c.on_signal_closed({**sig, "status": "closed"})
    check(t in h.c.closing and not h.logs("warn"), "queued from the ticket map, no warning yet")
    await jump(5.1)
    await h.c.retry_closes()
    check(t not in b.pos and not h.c.closing and not h.logs("warn"), "closed on the retry, never warned")
    h.close()

    b = SimBroker("hedging")
    h = Harness(b, dry_run=True)
    t = b.add_position("EURUSD", "buy", 0.1, "fx:852")
    b.pos_errors = 3
    await h.c.close_ticket(t, "closed from web app")
    check(t in h.c.closing, "dry-run: an unreadable close is queued")
    for _ in range(4):
        await jump(61)
        await h.c.retry_closes()
    check(not h.c.closing and sum("[dry-run] would close" in m for m in h.logs()) == 1 and t in b.pos,
          f"dry-run: answered once, then dropped: {h.logs()}")
    h.close()


@scenario
async def positions_read_error_is_not_a_flat_account():
    b = SimBroker("hedging")
    h = Harness(b)
    sig = signal(b, 801)
    await h.c.execute(sig)
    t = h.of("trade_opened")[-1]["trade"]["ticket"]
    b.pos_errors = 1
    await h.c.snapshot()
    await h.c.push_snapshot()
    check(not h.of("trade_closed") and t in h.c.known, "a failed read reports no close")
    b.pos_errors = 1
    await h.c.on_signal(signal(b, 802), manual=True)
    sk = h.of("signal_skipped")
    check(len(b.pos) == 1 and sk and sk[-1]["signal_id"] == 802 and "positions_get failed" in sk[-1]["reason"],
          f"no second entry while the positions are unreadable: {sk}")
    b.pos_errors = 1
    await h.c.on_signal_closed({**sig, "status": "closed"})
    check(t in b.pos and t in h.c.closing, "signal_closed while unreadable: queued for a retry, not lost")
    await jump(5.1)
    await h.c.retry_closes()
    check(t not in b.pos and not h.c.closing, "closed on the retry")
    await h.c.execute(signal(b, 803, "GBPUSD"))
    t3 = h.of("trade_opened")[-1]["trade"]["ticket"]
    b.pos_errors = 3                                  # every quick attempt of a web-app close
    await h.c.close_ticket(t3, "closed from web app")
    check(t3 in b.pos and t3 in h.c.closing, "web-app close while unreadable: queued")
    b.pos_errors = 1
    check(await h.c.session(Link(h, [WELCOME])) is False, "a failing first snapshot does not end the connector")
    h.close()

    import connector.connector as cmod
    b = SimBroker("hedging")
    pid = b.add_position("EURUSD", "buy", 0.1, "fx:5")
    h = Harness(b)
    b.pos_errors = 2                                  # the start-up sweep and the first read of the positions
    kicked = {"type": "kicked", "reason": "token_revoked"}
    real, cmod.connect = cmod.connect, lambda url, **kw: Link(h, [WELCOME, kicked])
    try:
        await h.c.run()
    finally:
        cmod.connect = real
        h.c.sweeper and h.c.sweeper.cancel()
    snaps = h.of("snapshot")
    check(snaps and [p["ticket"] for p in snaps[-1]["positions"]] == [pid] and not h.of("trade_closed"),
          f"start-up survives unreadable positions: {[m['type'] for m in h.msgs]}")
    h.close()

    b = SimBroker("netting")
    b.pos_errors = 1
    h = Harness(b)
    await h.c.on_burst(plan(82))
    fin = await finished(h, 82)
    check(fin["status"] == "skipped" and "could not read the open positions" in fin["reason"]
          and not b.calls_of("place"),
          f"netting burst pre-check: unreadable is not flat {fin}")
    h.close()
    b = SimBroker("netting")
    h = Harness(b)
    await h.c.on_burst(plan(83))
    await armed(h, 83)
    b.move(1.10080)
    await until(lambda: h.of("trade_opened"), "fill")
    b.pos_errors = 5
    await until(lambda: not b.pos_errors, "5 failed reads")
    await asyncio.sleep(0.05)
    check(not h.of("trade_closed") and h.final(83) is None,
          "the monitor skips those polls: its fill is not taken for closed")
    await jump(30.5)
    fin = await finished(h, 83)
    check(fin["status"] == "closed" and len(h.of("trade_closed")) == 1 and not b.pos, f"final {fin}")
    h.close()


@scenario
async def exchange_account_never_merges_a_signal():
    for mode, reason in (("exchange", "another position on EURUSD (exchange account"),
                         ("netting", "another position on EURUSD (netting account"), ("hedging", None)):
        b = SimBroker(mode)
        manual = b.add_position("EURUSD", "sell", 1.0, "manual", magic=0)
        h = Harness(b)
        await h.c.on_signal(signal(b, 901, test=True), manual=False)
        sk = h.of("signal_skipped")
        if reason:
            check(sk and reason in sk[-1]["reason"] and not h.of("trade_opened") and list(b.pos) == [manual],
                  f"{mode}: {sk}")
        else:
            check(h.of("trade_opened") and not sk, f"hedging keeps positions apart: {sk}")
        h.close()


@scenario
async def partial_fills_and_reporting_errors_still_count_as_trades():
    b = SimBroker("hedging")
    h = Harness(b)
    b.partial = 0.3                                   # IOC: 0.3 of the 0.5 lots filled (10010)
    await h.c.on_signal(signal(b, 1001), manual=True)
    t = h.of("trade_opened")
    check(t and near(t[-1]["trade"]["lots"], 0.3) and not h.of("signal_skipped"),
          f"a partial fill is a trade: {h.msgs}")
    b.partial = None
    opened, snaps = b.open_market, len(h.of("snapshot"))

    def opened_then_busy(*a, **kw):
        res = opened(*a, **kw)
        b.acct_errors = 1                             # the snapshot right after the open fails
        return res
    b.open_market = opened_then_busy
    await h.c.on_signal(signal(b, 1002, "GBPUSD"), manual=True)
    check([m["trade"]["signal_id"] for m in h.of("trade_opened")] == [1001, 1002] and not h.of("signal_skipped")
          and len(h.of("snapshot")) == snaps, f"a failed snapshot after the open is no skip: {h.of('signal_skipped')}")
    h.close()

    b = SimBroker("hedging")
    h = Harness(b)
    b.lost_reply = 1                                  # order_send answered None, yet the position is there
    await h.c.on_signal(signal(b, 1003), manual=True)
    await until(lambda: h.of("trade_opened"), "the position found after the lost answer")
    pos = next(iter(b.pos.values()))
    t = h.of("trade_opened")
    check(t and t[-1]["trade"]["ticket"] == pos["ticket"] and near(t[-1]["trade"]["lots"], pos["lots"])
          and h.c.signal_of[pos["ticket"]] == 1003 and not h.of("signal_skipped"), f"adopted: {h.msgs}")
    b.fail_open = 1
    await h.c.on_signal(signal(b, 1004, "GBPUSD"), manual=True)
    sk = h.of("signal_skipped")
    check(sk and sk[-1]["signal_id"] == 1004 and "10004" in sk[-1]["reason"] and len(b.pos) == 1,
          f"a refusal is still a skip: {sk}")
    h.close()


@scenario
async def test_signals_skip_only_the_drift_check():
    b = SimBroker("hedging")
    h = Harness(b)
    far = round(b.ask() + 30 * PIP, 5)                # the server's feed quotes 30 pips above this broker
    base = {"symbol": "EURUSD", "side": "buy", "price": far, "sl": far - 20 * PIP, "tp": far + 40 * PIP,
            "mode": "trend", "status": "open"}
    await h.c.on_signal({**base, "id": 1101}, manual=False)
    check(h.of("signal_skipped")[-1]["reason"] == "price moved 30.0 pips from the signal" and not b.pos,
          f"a real signal still checks the drift: {h.of('signal_skipped')}")
    await h.c.on_signal({**base, "id": 1102, "test": True}, manual=False)
    t = h.of("trade_opened")
    check(t and near(t[-1]["trade"]["entry_price"], b.ask()) and near(t[-1]["trade"]["sl"], b.ask() - 20 * PIP)
          and near(t[-1]["trade"]["tp"], b.ask() + 40 * PIP) and near(t[-1]["trade"]["lots"], 0.05),
          f"test signal: re-anchored on this broker's price, test risk (0.1%): {t}")
    await h.c.on_signal({**base, "id": 1103, "symbol": "GBPUSD", "test": True, "expires_at": "2020-01-01T00:00:00"},
                        manual=False)
    check(h.of("signal_skipped")[-1]["reason"] == "signal expired", "every other check still applies")
    h.close()
    b = SimBroker(demo=False)
    h = Harness(b, allow_real=True)
    await h.c.on_signal({**base, "id": 1104, "test": True}, manual=False)
    check(h.of("signal_skipped")[-1]["reason"] == "test signals only run on demo accounts" and not b.pos,
          "real account")
    h.close()


@scenario
async def prices_at_the_symbols_digits():
    """A signal's dry-run line and the report of its trade give each price at the symbol's own digits: gold 2, a
    currency pair 5."""
    gold = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01)

    def broker():
        b = SimBroker("hedging")
        b.bids["XAUUSD"] = 2650.123                                     # ask 2650.12305 (the spread is a pair's)
        spec = b.symbol_spec
        b.symbol_spec = lambda s: gold if s == "XAUUSD" else spec(s)
        return b

    def xau(b, sid):
        px = b.ask("XAUUSD")
        return {"id": sid, "symbol": "XAUUSD", "side": "buy", "price": px, "sl": px - 5.0, "tp": px + 10.0,
                "mode": "trend", "status": "open"}

    b = broker()
    h = Harness(b, dry_run=True)
    await h.c.execute(xau(b, 1201))
    await h.c.execute(signal(b, 1202))
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(said == ["[dry-run] would buy 0.2 XAUUSD @ 2650.12 SL 2645.12 TP 2660.12",
                   "[dry-run] would buy 0.5 EURUSD @ 1.10005 SL 1.09805 TP 1.10405"] and not b.pos, f"dry-run: {said}")
    h.close()
    b = broker()
    h = Harness(b)
    await h.c.execute(xau(b, 1203))
    await h.c.execute(signal(b, 1204))
    said = [m for m in h.logs() if m.startswith("Opened")]
    check(said == ["Opened buy 0.2 XAUUSD @ 2650.12 (signal #1203)", "Opened buy 0.5 EURUSD @ 1.10005 (signal #1204)"],
          f"opened: {said}")
    h.close()


@scenario
def files_stay_out_of_the_project():
    """What a connector keeps across restarts (tickets, outbox, the scalper's day and open scalps) goes to TMP in
    these tests: a regression never writes, or overwrites, the files of a connector running from this folder."""
    h = Harness(SimBroker())
    s = h.c.scalper
    paths = [h.c._map_file(), h.c._outbox_file(), s._day_file(), s._open_file(), s.daybook.path]
    h.close()
    check(all(p.parent == TMP for p in paths), f"kept in TMP: {paths}")
    now = {p.name: p.stat().st_mtime_ns for p in (ROOT / "connector").glob("*.json")}
    check(now == KEPT, f"nothing written to connector/ by the scenarios: "
                       f"{[k for k in sorted({*now, *KEPT}) if now.get(k) != KEPT.get(k)]}")


# ── v1.3.0: pips × scale, the stop in money, hold X / Y, pre-news analysis ──
def bars(n, slope, r, base=1.1, wide=None):
    """n bars closing `slope` apart, each `r` above and below its close; wide: the last 10 that far (a blow-out)."""
    import pandas as pd
    close = [base + slope * i for i in range(n)]
    rr = [wide if wide and i >= n - 10 else r for i in range(n)]
    return pd.DataFrame({"time": range(n), "open": close, "high": [c + x for c, x in zip(close, rr)],
                         "low": [c - x for c, x in zip(close, rr)], "close": close, "tick_volume": [1] * n})


def view(b, look):
    """Bars that make the analysis say: "up" (buy), "down" (sell), "flat" (both), "blow" (skip: M1 blew out)."""
    slope = {"up": 0.0003, "down": -0.0003}.get(look, 0.0)
    b.bars = {"H1": bars(H1_BARS, slope * 10, 0.0005), "M1": bars(M1_BARS, slope, 0.00005,
                                                                   wide=0.003 if look == "blow" else None)}


class Caught(logging.Handler):
    """The connector's log lines, DEBUG up, while attached: `with Caught() as lines` (none reach the console)."""
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.lines, self.log = [], logging.getLogger("connector")

    def emit(self, record):
        self.lines.append(record.getMessage())

    def __enter__(self):
        self.was = self.log.level, self.log.propagate
        self.log.setLevel(logging.DEBUG)
        self.log.propagate = False
        self.log.addHandler(self)
        return self.lines

    def __exit__(self, *exc):
        self.log.removeHandler(self)
        self.log.setLevel(self.was[0])
        self.log.propagate = self.was[1]


def refused(params, code):
    try:
        clamp_burst(params)
    except ValueError as e:
        return str(e) == code
    return False


@scenario
def clamp_burst_v130_keys():
    """The new keys: missing = v1.2.0 (stop in pips, no analysis, Y = X); Y under X is X; sl_money by its unit; a stop
    unit or analysis switch that cannot be read refuses the plan."""
    old = clamp_burst(plan(1)["params"])
    check(old["sl_unit"] == "pips" and old["analysis"] is False and old["max_hold_seconds"] == old["hold_seconds"] == 30
          and near(old["lots"], 0.10) and near(old["sl_money"], 2.0), f"a v1.2.0 plan: {old}")
    check(clamp_burst({"hold_seconds": 40, "max_hold_seconds": 20})["max_hold_seconds"] == 40
          and clamp_burst({"hold_seconds": 40, "max_hold_seconds": 99999})["max_hold_seconds"] == 1800
          and clamp_burst({"max_hold_seconds": "junk"})["max_hold_seconds"] == 30
          and clamp_burst({"max_hold_seconds": 120})["max_hold_seconds"] == 120, "Y: raised to X, capped, follows X")
    usd = clamp_burst({"sl_unit": "usd"})
    check(usd["sl_unit"] == "usd" and near(usd["sl_money"], 100.0), f"usd default {usd}")
    check(near(clamp_burst({"sl_unit": "usd", "sl_money": 1e9})["sl_money"], 100_000.0)
          and near(clamp_burst({"sl_unit": "pct", "sl_money": 50})["sl_money"], 10.0)
          and near(clamp_burst({"sl_unit": "pct", "sl_money": 0})["sl_money"], 0.1)
          and near(clamp_burst({"sl_unit": "pct", "sl_money": float("nan")})["sl_money"], 2.0), "sl_money by unit")
    check(near(clamp_burst({"lots": 0})["lots"], 0.01) and near(clamp_burst({"lots": 500})["lots"], 100.0), "lots")
    check(clamp_burst({"analysis": True})["analysis"] is True, "analysis on")
    check(refused({"sl_unit": "eur"}, "invalid_unit") and refused({"sl_unit": None}, "invalid_unit")
          and refused({"analysis": "false"}, "invalid_analysis") and refused({"analysis": 1}, "invalid_analysis"),
          "unreadable unit / switch refused")


@scenario
async def legacy_plan_unchanged_and_watched():
    """A v1.2.0 plan runs as in v1.2.0 (stop in pips, both sides, every fill closed at exactly X, in profit or not,
    as "hold time"); the analysis is only watched, reported once as shadow, and bars that fail or hang change
    nothing."""
    for look in ("none", "slow", "up"):
        b = SimBroker("hedging")
        if look == "slow":                               # bar reads that hang a while, then fail
            b.bars = {"H1": lambda: time.sleep(0.2) or 1 / 0, "M1": lambda: time.sleep(0.2) or 1 / 0}
        elif look == "up":
            view(b, "up")
        h = Harness(b)
        quote, quotes = b.quote, []
        with Caught() as lines:
            await h.c.on_burst(plan(1))
            await armed(h, 1)
            places = b.calls_of("place")
            check(len(places) == 10 and all(near(c[2], 0.2) for c in places) and near(places[0][3], 1.10075)
                  and near(places[1][3], 1.09930) and near(places[0][4], 1.09975) and near(places[0][5], 1.10115),
                  f"{look}: v1.2.0's straddle {places[:2]}")
            b.quote = lambda symbol: quotes.append(symbol) or quote(symbol)     # from here on: fills and exits
            b.move(1.10080)                              # the 5 buy-stops fill at 1.10085
            await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
            b.move(1.10100)                              # in profit, but Y = X: held to X
            await jump(29)
            check(not b.calls_of("close"), f"{look}: closed before X")
            await jump(1.5)
            await finished(h, 1)
            await until(lambda: h.analyses(1), "the watch-only analysis")
            await asyncio.sleep(0.05)
        check(len(b.calls_of("close")) == 5 and h.statuses(1) == ["scheduled", "armed", "triggered", "closed"],
              f"{look}: {h.statuses(1)}")
        check(sum("(hold time)" in s for s in lines) == 5 and not any("in profit after" in s for s in lines),
              f"{look}: closed as 'hold time': {[s for s in lines if 'closed' in s]}")
        check(not quotes, f"{look}: Y = X: no price read for the exits (the release not looked at): {quotes}")
        got = h.analyses(1)
        check(len(got) == 1 and got[0]["shadow"] is True, f"{look}: one shadow analysis {got}")
        check(got[0]["action"] == "buy" if look == "up" else got[0]["parts"]["trend"] is None, f"{look}: {got}")
        check(any("Analysis (watch only), would have" in s for s in lines), f"{look}: logged as watch only")
        h.close()


@scenario
async def watch_only_analysis_that_raises_changes_nothing():
    """The watch-only analysis failing in itself (here its log line) is a debug line only: the run arms and closes as
    in v1.2.0, no analysis goes to the server, and the side task it ran in ends without an exception (nothing awaits
    it: one that escaped would only be asyncio's 'Task exception was never retrieved')."""
    cc = sys.modules[Connector.__module__]
    real = cc.describe

    def loud(d, *a, shadow=False, **kw):
        if shadow:
            raise RuntimeError("describe failed")
        return real(d, *a, shadow=shadow, **kw)
    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    side, tasks = h.c._burst_side, []
    h.c._burst_side = lambda coro: tasks.append(side(coro)) or tasks[-1]     # every task run beside the burst
    cc.describe = loud
    try:
        with Caught() as lines:
            await h.c.on_burst(plan(1))
            await armed(h, 1)
            await until(lambda: any("watch-only analysis failed (describe failed)" in s for s in lines),
                        "the watch-only analysis' debug line")
            b.move(1.10080)                              # the 5 buy-stops fill at 1.10085
            await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
            await jump(30.5)
            fin = await finished(h, 1)
    finally:
        cc.describe = real
    check(fin["status"] == "closed" and len(b.calls_of("place")) == 10 and len(b.calls_of("close")) == 5
          and not h.analyses(1), f"run as in v1.2.0, no analysis sent: {fin} {h.analyses(1)}")
    check(len(tasks) == 3 and all(t.done() and not t.cancelled() and t.exception() is None for t in tasks),
          f"the watch-only task and its bar reads ended cleanly: {tasks}")
    h.close()


@scenario
async def legacy_reports_word_for_word():
    """A v1.2.0 plan's reports say its pips as the user set them, as v1.2.0 did (2.555, not 2.56): the dry-run line
    and the spread skip, word for word."""
    p = {"sl_pips": 12.345, "tp_start_pips": 4.125, "tp_step_pips": 1.375, "max_spread_pips": 2.555}
    b = SimBroker("hedging")
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(1, **p))
    await finished(h, 1)
    n, lots = burst_lots(10_000.0, 1.0, 12.345, 5, b.symbol_spec("EURUSD"))
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(said == [f"[dry-run] news burst #1 would place 10 stop orders on EURUSD: buy-stop 1.10075 / sell-stop 1.09930, "
                   f"{n} × {lots} lots per side, SL 12.345 pips, TP 4.125 + i × 1.375 pips"], f"dry-run: {said}")
    h.close()
    h = Harness(SimBroker(spread_pips=5))
    await h.c.on_burst(plan(2, **p))
    check((await finished(h, 2))["reason"] == "spread 5.0 pips > 2.555", f"spread: {h.final(2)}")
    h.close()


@scenario
async def gold_pips_scaled():
    """On gold (scale 5) every pip setting is 5 pair pips: offset, stop, targets, the spread cap; reports say so."""
    gold = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, scale=5.0)

    def broker(spread_pips):
        b = SimBroker("hedging")
        b.bids["XAUUSD"], b.spread = 2650.0, spread_pips * PIP        # quoted as spread_pips
        spec = b.symbol_spec
        b.symbol_spec = lambda s: gold if s == "XAUUSD" else spec(s)
        return b

    b = broker(10)                                       # over 3 pips, under 3 x 5
    h = Harness(b)
    await h.c.on_burst({**plan(1), "symbol": "XAUUSD"})
    await armed(h, 1)
    places, ask = b.calls_of("place"), b.ask("XAUUSD")
    check(len(places) == 10 and all(near(c[2], 0.04) for c in places), f"1% at a 50 pip stop: 5 x 0.04 {places[:2]}")
    for i in range(5):
        buy, sell = places[2 * i], places[2 * i + 1]
        check(near(buy[3], ask + 3.5) and near(sell[3], 2650.0 - 3.5), "offset 35 pips")
        check(near(buy[4], buy[3] - 5.0) and near(sell[4], sell[3] + 5.0), "stop 50 pips")
        check(near(buy[5], buy[3] + 2.0 + 0.75 * i) and near(sell[5], sell[3] - 2.0 - 0.75 * i),
              f"targets 20 + i x 7.5 pips: {buy} {sell}")
    h.close()
    h = Harness(broker(10), dry_run=True)
    await h.c.on_burst({**plan(2), "symbol": "XAUUSD"})
    await finished(h, 2)
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(len(said) == 1 and "SL 50.0 pips" in said[0] and "TP 20.0 + i × 7.5 pips" in said[0], f"dry-run: {said}")
    h.close()
    h = Harness(broker(16))
    await h.c.on_burst({**plan(3), "symbol": "XAUUSD"})
    check((await finished(h, 3))["reason"] == "spread 16.0 pips > 15.0", f"spread cap x 5: {h.final(3)}")
    h.close()


@scenario
async def long_term_spread_cap_scaled():
    """A signal's spread cap is in a pair's pips too: 3 x 5 on gold, 3 x 2 on GBPJPY (a 4 pip spread opens both),
    3 on EURUSD as in v1.2.0 (skipped, in v1.2.0's words); over the scaled cap, the reason says the scaled cap."""
    specs = {"XAUUSD": SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, scale=5.0),
             "GBPJPY": SymbolSpec("GBPJPY", pip=0.01, pip_value_per_lot=6.7, digits=3, point=0.001, scale=2.0)}

    def broker(spread_pips):
        b = SimBroker("hedging", spread_pips=spread_pips)                 # quoted as spread_pips on every symbol
        b.bids.update(XAUUSD=2650.0, GBPJPY=190.0)
        spec = b.symbol_spec
        b.symbol_spec = lambda s: specs.get(s) or spec(s)
        return b

    def sig(b, sid, symbol, stop):
        px = b.ask(symbol)
        return {"id": sid, "symbol": symbol, "side": "buy", "price": px, "sl": px - stop, "tp": px + 2 * stop,
                "mode": "trend", "status": "open"}

    b = broker(4)                                        # over 3 pips, under 3 x 2 and 3 x 5
    h = Harness(b)
    await h.c.execute(sig(b, 1301, "XAUUSD", 5.0))
    await h.c.execute(sig(b, 1302, "GBPJPY", 0.2))
    await h.c.execute(signal(b, 1303))
    opened = [m["trade"]["symbol"] for m in h.of("trade_opened")]
    skipped = [(m["signal_id"], m["reason"]) for m in h.of("signal_skipped")]
    check(opened == ["XAUUSD", "GBPJPY"] and len(b.pos) == 2, f"gold and GBPJPY opened: {opened} {skipped}")
    check(skipped == [(1303, "spread 4.0 pips > 3.0")], f"EURUSD skipped as in v1.2.0: {skipped}")
    h.close()
    b = broker(16)                                       # over 3 x 2 and 3 x 5
    h = Harness(b)
    await h.c.execute(sig(b, 1304, "XAUUSD", 5.0))
    await h.c.execute(sig(b, 1305, "GBPJPY", 0.2))
    skipped = [(m["signal_id"], m["reason"]) for m in h.of("signal_skipped")]
    check(skipped == [(1304, "spread 16.0 pips > 15.0"), (1305, "spread 16.0 pips > 6.0")] and not b.pos,
          f"over the scaled cap: {skipped}")
    h.close()


async def dry_money(margin=None, balance=10_000.0, volume_min=None, **params):
    """A dry run of a plan with a stop in money: its final report, the log lines sent, the broker. volume_min: the
    broker's minimum lot on EURUSD (else 0.01)."""
    b = SimBroker("hedging", balance=balance)
    b.margin = margin
    if volume_min is not None:
        spec = b.symbol_spec
        b.symbol_spec = lambda s: replace(spec(s), volume_min=volume_min)
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(1, **params))
    fin = await finished(h, 1)
    h.close()
    return fin, h.logs(), b


@scenario
async def money_stop_sizes():
    """sl_unit pct / usd: the user's lots on every order, the stop where they lose the money together; never inside
    three spreads (lots cut instead), never past the largest pip stop; a side within half the free margin."""
    fin, said, _ = await dry_money(sl_unit="pct", sl_money=1.0, lots=0.1)
    check(fin["reason"] == "dry-run" and "10 stop orders" in said[-1]
          and "5 × 0.1 lots per side, SL 20.0 pips (≈ 100.00 at risk)" in said[-1], f"1% = 100 on 0.5 lot: {said}")
    _, said, _ = await dry_money(sl_unit="pct", sl_money=0.3, lots=0.1)        # 6 pips < 3 x the 3 pip cap
    check("5 × 0.06 lots per side, SL 9.0 pips" in said[-1], f"floor: 9 pips, lots cut to fit 30: {said}")
    _, said, _ = await dry_money(sl_unit="usd", sl_money=500.0, lots=0.01, trades=1)
    check("1 × 0.01 lots per side, SL 200.0 pips" in said[-1], f"capped at 200 pips: {said}")
    fin, _, _ = await dry_money(sl_unit="usd", sl_money=1.0, lots=0.01, trades=40)
    check(fin["status"] == "skipped" and "too little for 40 × 0.01 lots" in fin["reason"], f"too little: {fin}")
    fin, said, b = await dry_money(margin=20_000.0, sl_unit="pct", sl_money=1.0, lots=0.1)
    check("5 × 0.05 lots per side, SL 20.0 pips" in said[-1]
          and any("the free margin allows 0.05 lots per order" in s for s in said)
          and len(b.calls_of("margin")) == 2, f"margin: 0.5 lot needs 10000 of 5000: {said}")
    fin, _, _ = await dry_money(margin=10_000_000.0, sl_unit="pct", sl_money=1.0, lots=0.1)
    check(fin["reason"] == "not enough free margin for 5 × 0.01 lots of EURUSD", f"no margin: {fin}")
    fin, said, _ = await dry_money(sl_unit="pips", sl_money=1.0, lots=3.0)     # pips: lots and money unused
    check("5 × 0.2 lots per side, SL 10.0 pips, TP" in said[-1] and "at risk" not in said[-1], f"pips: {said}")
    # the reasons the burst history shows: no balance, and the user's lots under the broker's minimum lot
    for unit, money in (("pct", 1.0), ("usd", 100.0)):
        fin, _, _ = await dry_money(balance=0.0, sl_unit=unit, sl_money=money, lots=0.1)
        check(fin["status"] == "skipped" and fin["reason"] == "no money to risk: the balance is not above 0",
              f"{unit}, balance 0: {fin}")
    fin, _, _ = await dry_money(volume_min=0.1, sl_unit="pct", sl_money=1.0, lots=0.05)
    check(fin["status"] == "skipped" and fin["reason"] == "0.05 lots is under the minimum lot size of EURUSD (0.1)",
          f"0.05 lots, min 0.1: {fin}")
    # a margin the terminal cannot tell (the call raises, or answers NaN / inf): not known, so the user's lots, as
    # when it answers None (the broker refuses an order it cannot cover anyway)
    for bad in (RuntimeError("order_calc_margin failed"), float("nan"), float("inf")):
        b = SimBroker("hedging")

        def margin_for(symbol, side, lots, b=b, bad=bad):
            with b.lock:
                b.calls.append(("margin", side, lots))
            if isinstance(bad, Exception):
                raise bad
            return bad
        b.margin_for = margin_for
        h = Harness(b)
        await h.c.on_burst(plan(1, sl_unit="pct", sl_money=1.0, lots=0.1))
        await armed(h, 1)
        places = b.calls_of("place")
        check(len(places) == 10 and all(near(c[2], 0.1) for c in places) and len(b.calls_of("margin")) == 2
              and not any("free margin" in m for m in h.logs()), f"margin {bad!r}: {places[:2]} {h.logs()}")
        await h.c.on_burst_cancel(1)
        check((await finished(h, 1))["status"] == "cancelled", f"margin {bad!r}: {h.final(1)}")
        h.close()


@scenario
async def money_stop_over_volume_max():
    """The user's lots over the broker's volume_max (2 x 2.0 lots at a 25 pip stop, max 1.0): hedging caps every
    order at it (one ticket per order: the run risks less), netting splits each side into orders of at most it."""
    for mode, want in (("hedging", [1.0] * 4), ("netting", [1.0] * 8)):
        b = SimBroker(mode, balance=1_000_000.0, volume_max=1.0)
        h = Harness(b)
        await h.c.on_burst(plan(1, trades=2, sl_unit="usd", sl_money=1000.0, lots=2.0))
        await armed(h, 1)
        places = b.calls_of("place")
        check([c[2] for c in places] == want and near(places[0][3] - places[0][4], 25 * PIP),
              f"{mode}: {[c[2] for c in places]} {places[:1]}")
        await h.c.on_burst_cancel(1)
        await finished(h, 1)
        h.close()


@scenario
async def dry_run_says_what_the_orders_risk():
    """A money stop's dry run says what the orders of a side risk at their stop (orders x lots x stop x pip value),
    and the money too when that is more: the stop capped at 200 pips, the lots cut to fit the closest stop allowed or
    the free margin, fewer orders of the minimum lot at half the risk, a ticket capped at volume_max (hedging)."""
    _, said, _ = await dry_money(sl_unit="pct", sl_money=1.0, lots=0.1)
    check(said[-1].endswith("5 × 0.1 lots per side, SL 20.0 pips (≈ 100.00 at risk), TP 4.0 + i × 1.5 pips"),
          f"all of the money: {said}")
    for params, want in (({"sl_unit": "usd", "sl_money": 500.0, "lots": 0.01, "trades": 1},
                          "1 × 0.01 lots per side, SL 200.0 pips (≈ 20.00 at risk, 500.00 allowed), TP"),
                         ({"sl_unit": "pct", "sl_money": 0.3, "lots": 0.1},
                          "5 × 0.06 lots per side, SL 9.0 pips (≈ 27.00 at risk, 30.00 allowed), TP"),
                         ({"margin": 20_000.0, "sl_unit": "pct", "sl_money": 1.0, "lots": 0.1},
                          "5 × 0.05 lots per side, SL 20.0 pips (≈ 50.00 at risk, 100.00 allowed), TP")):
        _, said, _ = await dry_money(**params)
        check(want in said[-1], f"{params}: {said}")
    b = SimBroker("hedging")
    view(b, "flat")                                      # unclear: half of 5 x 0.01 is 2 x 0.01 (of 50)
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(1, analysis=True, sl_unit="pct", sl_money=1.0, lots=0.01))
    await finished(h, 1)
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(len(said) == 1 and "2 × 0.01 lots per side, SL 200.0 pips (≈ 40.00 at risk, 50.00 allowed)" in said[0],
          f"half the risk: {said}")
    h.close()
    for mode, want in (("hedging", "2 × 1.0 lots per side, SL 25.0 pips (≈ 500.00 at risk, 1000.00 allowed), TP"),
                       ("netting", "2 × 2.0 lots per side, SL 25.0 pips (≈ 1000.00 at risk)")):
        b = SimBroker(mode, balance=1_000_000.0, volume_max=1.0)
        h = Harness(b, dry_run=True)
        await h.c.on_burst(plan(1, trades=2, sl_unit="usd", sl_money=1000.0, lots=2.0))
        await finished(h, 1)
        said = [m for m in h.logs() if m.startswith("[dry-run]")]
        check(len(said) == 1 and want in said[0], f"{mode}, volume_max 1.0: {said}")
        h.close()


@scenario
async def money_stop_not_held_to_the_unused_pip_stop():
    """A broker's closest stop (8 pips) past the plan's pip stop (5) but inside its offset (10): a stop in money does
    not use the pip stop (it is sized against the broker's floor itself), so the run arms with its stop 20 pips away;
    the same plan with its stop in pips is skipped as closer than the broker allows."""
    params = {"sl_money": 1.0, "sl_pips": 5, "offset_pips": 10, "lots": 0.1}
    b = SimBroker("hedging", stops_pips=8)
    h = Harness(b)
    await h.c.on_burst(plan(1, sl_unit="pct", **params))
    await armed(h, 1)
    places = b.calls_of("place")
    check(len(places) == 10 and all(near(abs(c[3] - c[4]), 20 * PIP) for c in places)
          and near(places[0][3], 1.10105), f"1% on 5 x 0.1: a 20 pip stop, offset 10: {places[:2]}")
    await h.c.on_burst_cancel(1)
    await finished(h, 1)
    h.close()
    b = SimBroker("hedging", stops_pips=8)
    h = Harness(b)
    await h.c.on_burst(plan(2, sl_unit="pips", **params))
    fin = await finished(h, 2)
    check(fin["status"] == "skipped" and fin["reason"] == "offset or stop-loss closer than the broker allows (8.0 pips)"
          and not b.calls_of("place"), f"pips: a 5 pip stop under 8: {fin}")
    h.close()


@scenario
async def gold_money_stop_scaled():
    """On gold (scale 5) a money stop's floor and cap are in gold's pips too: 3 x the spread cap x 5 = 45 at least
    (a quoted spread of 1 under it: lots cut to fit the money), 200 x 5 = 1000 at most (600 kept, 1500 capped)."""
    gold = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, scale=5.0)
    for bid, usd, lots, sl_d, got_lots in ((1, 20.0, 0.1, 4.5, 0.04), (2, 60.0, 0.01, 60.0, 0.01),
                                           (3, 150.0, 0.01, 100.0, 0.01)):
        b = SimBroker("hedging")
        b.bids["XAUUSD"], b.spread = 2650.0, 1 * PIP                  # quoted as 1 pip
        spec = b.symbol_spec
        b.symbol_spec = lambda s, spec=spec: gold if s == "XAUUSD" else spec(s)
        h = Harness(b)
        await h.c.on_burst({**plan(bid, trades=1, sl_unit="usd", sl_money=usd, lots=lots), "symbol": "XAUUSD"})
        await armed(h, bid)
        buy, sell = b.calls_of("place")
        check(near(buy[3] - buy[4], sl_d) and near(sell[4] - sell[3], sl_d) and near(buy[2], got_lots),
              f"{usd} $ on {lots} lots: a stop of {sl_d * 10:g} gold pips, {got_lots} lots: {buy} {sell}")
        await h.c.on_burst_cancel(bid)
        await finished(h, bid)
        h.close()


@scenario
async def gold_netting_slices_scaled():
    """Netting on gold (scale 5): the connector's own slices go at tp_start x 5 gold pips and every tp_step x 5 after
    it (20, then 27.5), one of the run's lots at each; a tenth of a pip short of a target, none."""
    gold = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, scale=5.0)
    b = SimBroker("netting")
    b.bids["XAUUSD"], b.spread = 2650.0, 1 * PIP                      # quoted as 1 pip
    spec = b.symbol_spec
    b.symbol_spec = lambda s: gold if s == "XAUUSD" else spec(s)
    h = Harness(b)
    await h.c.on_burst({**plan(1, trades=3), "symbol": "XAUUSD"})
    await armed(h, 1)
    st = h.c.bursts[1]
    check(st["n"] == 3 and near(st["lots"], 0.06) and st["tp"] == (20.0, 7.5),
          f"1% at 50 gold pips: 3 x 0.06, targets 20 + i x 7.5: {st['n']} {st['lots']} {st['tp']}")
    buy = next(c for c in b.calls_of("place") if c[1] == "buy")
    check(near(buy[2], 0.18) and buy[5] is None, f"one 0.18 buy-stop, no TP of its own: {buy}")
    b.move(buy[3], "XAUUSD")                             # filled at the ask
    await until(lambda: h.of("trade_opened"), "the buy filled")
    pid = h.of("trade_opened")[0]["trade"]["ticket"]
    e = b.pos[pid]["entry_price"]
    for pips, want, left in ((19.9, [], 0.18), (20.0, [0.06], 0.12), (27.4, [0.06], 0.12),
                             (27.5, [0.06, 0.06], 0.06)):
        await jump(1.1)
        b.move(round(e + pips * gold.pip, 5), "XAUUSD")
        if len(want) > len(b.calls_of("close")):
            await until(lambda: len(b.calls_of("close")) == len(want), f"{pips} gold pips: a slice")
        await asyncio.sleep(0.1)
        got = [c[2] for c in b.calls_of("close")]
        check(len(got) == len(want) and all(near(x, y) for x, y in zip(got, want)) and near(b.pos[pid]["lots"], left),
              f"{pips} gold pips: slices {got}, {b.pos[pid]['lots']} left")
    await jump(30)                                       # the rest at X
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and [c[2] for c in b.calls_of("close")][2:] == [None] and not b.pos,
          f"the rest at X: {b.calls_of('close')} {fin}")
    h.close()


@scenario
async def money_stop_fill_too_far_closed_at_once():
    """A money stop's fill that slipped so far that it risks over 1.25 x its money is closed at once, before X."""
    b = SimBroker("hedging")
    h = Harness(b)
    with Caught() as lines:
        await h.c.on_burst(plan(1, trades=1, sl_unit="pct", sl_money=1.0, lots=0.1, tp_start_pips=500))
        await armed(h, 1)
        buy = b.calls_of("place")[0]
        check(near(buy[3], 1.10075) and near(buy[4], 1.09075) and near(buy[2], 0.1), f"100 pip stop: {buy}")
        b.move(1.10400)                                  # filled at 1.10405: 133 pips from its stop, 133 > 125
        await until(lambda: b.calls_of("close"), "closed at once")
        fin = await finished(h, 1)
    check(fin["status"] == "closed" and any("too far from its stop (133.00 at risk, 100.00 allowed)" in s
                                            for s in h.logs()), f"warned: {h.logs()}")
    check(any("(filled too far from its stop)" in s for s in lines), "the close says why")
    h.close()


@scenario
async def money_stop_slip_counts_one_tickets_share():
    """The slip check weighs a fill against its own share of the money: on hedging 1 of 5 tickets (100 / 5 = 20, so
    over 25 slipped), on netting the one position holding every lot (the whole 100, so over 125)."""
    for mode, want in (("hedging", "(26.60 at risk, 20.00 allowed)"), ("netting", "(133.00 at risk, 100.00 allowed)")):
        b = SimBroker(mode)
        h = Harness(b)
        await h.c.on_burst(plan(1, trades=5, sl_unit="pct", sl_money=1.0, lots=0.02, tp_start_pips=500))
        await armed(h, 1)
        buy = b.calls_of("place")[0]
        check(near(buy[3], 1.10075) and near(buy[4], 1.09075), f"{mode}: 100 pips for 1% on 5 x 0.02: {buy}")
        b.move(1.10400)                                  # filled at 1.10405: 133 pips from the stop
        fills = 5 if mode == "hedging" else 1
        await until(lambda: len(b.calls_of("close")) >= fills, f"{mode}: closed at once")
        fin = await finished(h, 1)
        warned = [s for s in h.logs("warn") if "closed at once" in s]
        check(fin["status"] == "closed" and len(warned) == fills and all(want in s for s in warned) and not b.pos,
              f"{mode}: each fill against its share {want}: {warned}")
        h.close()


@scenario
async def money_stop_slip_at_the_symbols_pip_value():
    """The slip check weighs the risk at the symbol's own pip and pip value (GBPJPY: 0.01, 6.70 a lot, scale 2): $67
    on 0.1 lot is a 100 pip stop; a fill that risks just under 1.25 x 67 is kept, one just over is closed at once."""
    jpy = SymbolSpec("GBPJPY", pip=0.01, pip_value_per_lot=6.7, digits=3, point=0.001, scale=2.0)
    for gap, closed in ((-0.002, False), (0.002, True)):       # yen from the fill that risks exactly 1.25 x 67
        b = SimBroker("hedging")
        b.bids["GBPJPY"] = 190.0
        spec = b.symbol_spec
        b.symbol_spec = lambda s, spec=spec: jpy if s == "GBPJPY" else spec(s)
        h = Harness(b)
        await h.c.on_burst({**plan(1, trades=1, sl_unit="usd", sl_money=67.0, lots=0.1, tp_start_pips=500),
                            "symbol": "GBPJPY"})
        await armed(h, 1)
        buy = b.calls_of("place")[0]
        check(buy[1] == "buy" and near(buy[2], 0.1) and near(buy[3] - buy[4], 1.0), f"a 100 pip stop on 0.1: {buy}")
        edge = buy[4] + 67.0 * Scalper.RISK_SLACK / (6.7 * 0.1) * 0.01     # 1.25 yen from the stop
        b.move(edge + gap - b.spread, "GBPJPY")          # the buy-stop fills at the ask
        await until(lambda: h.of("trade_opened"), "the buy filled")
        risk = (h.of("trade_opened")[0]["trade"]["entry_price"] - buy[4]) / 0.01 * 6.7 * 0.1
        if closed:
            await until(lambda: b.calls_of("close"), "closed at once")
            fin = await finished(h, 1)
            warned = [s for s in h.logs("warn") if "closed at once" in s]
            check(fin["status"] == "closed" and len(warned) == 1 and f"({risk:.2f} at risk, 67.00 allowed)" in warned[0]
                  and risk > 67.0 * 1.25, f"{risk:.2f} > 83.75: closed at once: {warned}")
        else:
            await asyncio.sleep(0.1)
            check(not b.calls_of("close") and len(b.pos) == 1 and not any("closed at once" in s for s in h.logs())
                  and 67.0 < risk < 67.0 * 1.25, f"{risk:.2f} < 83.75: kept: {b.calls_of('close')} {h.logs()}")
            await jump(30.5)                             # closed at X as any fill
            fin = await finished(h, 1)
            check(fin["status"] == "closed" and len(b.calls_of("close")) == 1, f"kept to X: {fin}")
        h.close()


@scenario
async def money_stop_unseen_fill_taken_up_at_a_crash():
    """A fill the runner never saw (the terminal stopped answering, then the target closed it), taken up by the
    clean-up as far from its stop as a slip: it is gone already, so it is reported opened and closed, never as
    'closed at once' and never closed by the bot."""
    b = SimBroker("hedging")
    h = Harness(b)
    h.c.BURST_MAX_ERRORS = 5
    await h.c.on_burst(plan(1, trades=1, sl_unit="pct", sl_money=1.0, lots=0.1))
    await armed(h, 1)
    with b.lock:
        b.broken = True
        b.move(1.10400)                                  # filled at 1.10405 (133 pips from its stop), target hit too
    fin = await finished(h, 1)
    check(fin["status"] == "error" and len(h.of("trade_opened")) == 1 and len(h.of("trade_closed")) == 1,
          f"taken up and reported: {fin} {h.of('trade_opened')}")
    check(not b.calls_of("close") and not any("closed at once" in s for s in h.logs()), f"no slip: {h.logs()}")
    h.close()


@scenario
async def money_stop_flash_fill_already_gone():
    """A fill as far from its stop that also hit its target before a poll saw it open is gone already: reported
    opened and closed, never closed by the bot nor reported as 'closed at once'."""
    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst(plan(1, trades=1, sl_unit="pct", sl_money=1.0, lots=0.1))
    await armed(h, 1)
    b.move(1.10400)                                      # filled at 1.10405 (133 pips from its stop), target hit too
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["fills"] == 1 and len(h.of("trade_opened")) == 1
          and len(h.of("trade_closed")) == 1, f"the flash fill reported: {fin}")
    check(not b.calls_of("close") and not any("closed at once" in s for s in h.logs()), f"no slip close: {h.logs()}")
    h.close()


@scenario
async def money_stop_latched_exit_kept_when_the_position_grows():
    """Netting: a close already decided (in profit after X, its first try refused) stands when a split order merges in
    later, far from its stop: the grown position is reported, but not as a slip, and the close keeps its reason."""
    b = SimBroker("netting", volume_max=0.1)             # 2 x 0.1 lots per side: two orders per side
    b.fail_close = 1
    place, buys = b.place_pending, []

    def place_late(symbol, side, lots, price, sl, tp, comment="", expire_in=None):
        if side == "buy":
            buys.append(comment)
            if len(buys) == 2:                           # the second buy-stop sits 60 pips higher: it fills later
                price += 60 * PIP
        return place(symbol, side, lots, price, sl, tp, comment, expire_in)
    b.place_pending = place_late
    h = Harness(b)
    with Caught() as lines:
        await h.c.on_burst(plan(1, trades=2, sl_unit="pct", sl_money=1.0, lots=0.1, tp_start_pips=500,
                                max_hold_seconds=120))
        await armed(h, 1)
        b.move(1.10080)                                  # the first fills at 1.10085, 51 pips from its 50 pip stop
        await until(lambda: h.of("trade_opened"), "the first buy filled")
        b.move(1.10100)                                  # in profit
        await jump(30.5)
        await until(lambda: b.calls_of("close"), "the close decided, refused once")
        b.move(1.10680)                                  # the second fills at 1.10685: 0.2 lots at 1.10385, 81 pips
        await until(lambda: len(h.of("trade_opened")) == 2, "the position grown")
        fin = await finished(h, 1)
    check([m["trade"]["lots"] for m in h.of("trade_opened")] == [0.1, 0.2] and fin["status"] == "closed"
          and not b.pos, f"grown, then closed: {h.of('trade_opened')} {fin}")
    check(not any("closed at once" in s for s in h.logs()) and any("(in profit after 30s)" in s for s in lines)
          and not any("(filled too far from its stop)" in s for s in lines),
          f"the decided close kept: {h.logs()} {[s for s in lines if 'closed' in s]}")
    h.close()


async def held(close_fails=0, frozen=0, **params):
    """One buy filled at 1.10085 (pre-release mid 1.100025) on a plan with hold X 30 s / Y 120 s; (harness, broker)."""
    b = SimBroker("hedging")
    b.fail_close, b.frozen_close = close_fails, frozen
    h = Harness(b)
    await h.c.on_burst(plan(1, trades=1, tp_start_pips=500, max_hold_seconds=120, **params))
    await armed(h, 1)
    b.move(1.10080)
    await until(lambda: "triggered" in h.statuses(1) and h.of("trade_opened"), "the buy filled")
    return h, b


@scenario
async def hold_until_profit_or_y():
    """Hold X / Y: in profit from X on; in a loss kept to Y; closed at once when the price is back through the
    pre-release mid; a close decided is kept (a fading profit does not undo it); 10029 retried without a warning."""
    with Caught() as lines:
        h, b = await held()
        b.move(1.10100)                                  # in profit
        await jump(29)
        check(not b.calls_of("close"), "profit before X: kept")
        await jump(1.5)
        await finished(h, 1)
        check(any("(in profit after 30s)" in s for s in lines), f"profit: {lines[-3:]}")
        h.close()

        h, b = await held()
        b.move(1.10060)                                  # a loss, still above the pre-release mid
        await jump(31)
        await asyncio.sleep(0.05)
        check(not b.calls_of("close"), "a loss after X: kept until Y")
        await jump(90)
        await finished(h, 1)
        check(any("(max hold 120s)" in s for s in lines), f"max: {lines[-3:]}")
        h.close()

        h, b = await held()
        b.move(1.09990)                                  # mid 1.099925: back under the pre-release mid
        await until(lambda: b.calls_of("close"), "closed against the release")
        await finished(h, 1)
        check(any("(turned against the release)" in s for s in lines), f"against: {lines[-3:]}")
        h.close()

        h, b = await held(close_fails=1)
        b.move(1.10100)
        await jump(30.5)                                 # in profit: the close is refused once
        await until(lambda: b.calls_of("close"), "a close tried")
        b.move(1.10060)                                  # now a loss, before Y: the decision stands
        await jump(1)
        await finished(h, 1)
        check(len(b.calls_of("close")) == 2 and not b.pos, f"latched: {b.calls_of('close')}")
        h.close()

        h, b = await held(frozen=2)
        b.move(1.10100)
        await jump(30.5)
        for _ in range(3):
            await jump(5)
        await finished(h, 1)
        check(len(b.calls_of("close")) == 3 and not any("could not close" in s for s in h.logs())
              and any("frozen, close retried" in s for s in lines), f"10029 quiet: {h.logs()}")
        h.close()

        h, b = await held()
        real, shown = b.positions, {"profit": 0.01}
        b.positions = lambda all_magic=False: [{**x, **shown} for x in real(all_magic)]
        seen = len(lines)
        await jump(31)
        await asyncio.sleep(0.05)
        check(not b.calls_of("close"), "a cent of profit after X (under one point on 1 lot, 1.00): kept")
        shown["profit"] = 1.5
        await until(lambda: b.calls_of("close"), "closed once in profit")
        await finished(h, 1)
        check(any("(in profit after 30s)" in s for s in lines[seen:]), f"profit: {lines[seen:]}")
        h.close()


@scenario
async def frozen_close_warned_on_a_legacy_plan_or_when_it_lasts():
    """A close refused as frozen (10029): a v1.2.0 plan warns at once, as v1.2.0 did; a v1.3.0 plan retries quietly
    and warns once, only when it stayed frozen BURST_FROZEN tries (the broker's own TP / SL did not take it)."""
    for params, frozen, warned in (({}, 2, 1), ({"max_hold_seconds": 120}, Connector.BURST_FROZEN - 1, 0),
                                   ({"max_hold_seconds": 120}, Connector.BURST_FROZEN, 1)):
        b = SimBroker("hedging")
        b.frozen_close = frozen
        h = Harness(b)
        await h.c.on_burst(plan(1, trades=1, tp_start_pips=500, **params))
        await armed(h, 1)
        b.move(1.10080)                                  # the buy-stop fills at 1.10085
        await until(lambda: h.of("trade_opened"), "the buy filled")
        b.move(1.10100)                                  # in profit at X
        await jump(30.5)
        for n in range(1, frozen + 1):                   # each refused try, then its back-off (5 s at most)
            await until(lambda: len(b.calls_of("close")) >= n, f"close try {n}")
            await jump(5)
        fin = await finished(h, 1)
        said = [m for m in h.logs("warn") if "could not close" in m]
        check(fin["status"] == "closed" and len(b.calls_of("close")) == frozen + 1 and not b.pos
              and len(said) == warned and all("(order rejected (10029): frozen), retrying" in m for m in said),
              f"{params or 'v1.2.0'}, frozen {frozen} times: {said} {b.calls_of('close')}")
        h.close()


@scenario
async def netting_reversal_drops_the_replaced_sides_exit():
    """Netting: a close decided for a long (back through the pre-release mid, refused) is not done to the short a
    sell-stop reverses it into, also while the history is behind the list (lag): the short has its own hold from
    the reversal, closed once in profit after X from then."""
    for lag in (0.0, 0.3):
        b = SimBroker("netting")
        h = Harness(b)
        with Caught() as lines:
            await h.c.on_burst(plan(1, trades=1, offset_pips=3.0, tp_start_pips=500, max_hold_seconds=120))
            await armed(h, 1)                            # buy-stop 1.10035, sell-stop 1.09970, 1.0 lot each
            buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
            sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
            b.frozen[sell] = b.fail_close = 10 ** 6       # the sell-stop stays; every close is refused
            with b.lock:                                 # IOC: 0.4 of the 1.0 buy-stop
                b.bids["EURUSD"] = 1.10035
                o = b.orders.pop(buy)
                b._fill({**o, "lots": 0.4}, b.ask("EURUSD"))
            await until(lambda: h.of("trade_opened"), "long")
            await jump(10)
            b.move(1.09990)                              # mid 1.099925: back under the pre-release mid 1.100025
            await until(lambda: b.calls_of("close"), "the long's close decided, refused")
            with b.lock:
                b.fail_close = 0
                if lag:
                    b.hide(sell, lag)
                b.move(1.09965)                          # the sell-stop: a 0.6 short, same identifier
            if lag:
                await jump(5)                            # the long's close due again while the history is behind
            await until(lambda: len(h.of("trade_opened")) == 2, f"lag {lag}: the reversal reported")
            tried = len(b.calls_of("close"))
            b.move(1.09900)                              # the short in profit, the release's way
            await jump(25)                               # 35 s from the long's fill, 25 from the reversal
            await asyncio.sleep(0.1)
            check(len(b.calls_of("close")) == tried and len(b.pos) == 1,
                  f"lag {lag}: the short kept before its own X: {b.calls_of('close')[tried:]}")
            await jump(6)
            fin = await finished(h, 1)
        check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 0.4), ("sell", 0.6)]
              and fin["status"] == "closed" and len(b.calls_of("close")) == tried + 1 and not b.pos,
              f"lag {lag}: {h.of('trade_opened')} {b.calls_of('close')} {fin}")
        check(any("(in profit after 30s)" in s for s in lines)
              and not any("(turned against the release)" in s for s in lines),
              f"lag {lag}: closed for its own reason: {[s for s in lines if 'closed' in s]}")
        h.close()


@scenario
async def netting_reversal_on_a_legacy_plan_keeps_x_from_the_first_fill():
    """Netting, a v1.2.0 plan (no v1.3.0 keys: Y = X): a sell-stop that reverses the long into a short does not start
    the hold again; the short is closed at X from the long's fill, as v1.2.0 counted it, as "hold time" (also while
    the history is behind the list: lag)."""
    for lag in (0.0, 0.3):
        b = SimBroker("netting")
        h = Harness(b)
        with Caught() as lines:
            await h.c.on_burst(plan(1, trades=1, offset_pips=3.0, tp_start_pips=500))
            await armed(h, 1)                            # buy-stop 1.10035, sell-stop 1.09970, 1.0 lot each
            buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
            sell = next(t for t, o in b.orders.items() if o["side"] == "sell")
            b.frozen[sell] = 10 ** 6                     # the sell-stop stays
            with b.lock:                                 # IOC: 0.4 of the 1.0 buy-stop
                b.bids["EURUSD"] = 1.10035
                o = b.orders.pop(buy)
                b._fill({**o, "lots": 0.4}, b.ask("EURUSD"))
            await until(lambda: h.of("trade_opened"), "long")
            await jump(24)
            with b.lock:
                if lag:
                    b.hide(sell, lag)
                b.move(1.09965)                          # the sell-stop: a 0.6 short, same identifier
            await until(lambda: len(h.of("trade_opened")) == 2, f"lag {lag}: the reversal reported")
            await jump(4)                                # 28 s from the long's fill
            await asyncio.sleep(0.1)
            check(not b.calls_of("close") and len(b.pos) == 1, f"lag {lag}: kept before X: {b.calls_of('close')}")
            await jump(2)                                # X from the long's fill, 6 s from the reversal
            await until(lambda: b.calls_of("close"), f"lag {lag}: closed at X from the first fill", 2.0)
            fin = await finished(h, 1)
        check([(m["trade"]["side"], m["trade"]["lots"]) for m in h.of("trade_opened")] == [("buy", 0.4), ("sell", 0.6)]
              and fin["status"] == "closed" and len(b.calls_of("close")) == 1 and not b.pos,
              f"lag {lag}: {h.of('trade_opened')} {b.calls_of('close')} {fin}")
        check(sum("(hold time)" in s for s in lines) == 1, f"lag {lag}: as v1.2.0 closed it: "
                                                            f"{[s for s in lines if 'closed' in s]}")
        h.close()


@scenario
async def analysis_picks_the_sides():
    """analysis on: clear = one side at full size, unclear = both at half the risk (pips: half the % / money: half
    the lots, same stop), blow-out = skipped with its reason; the answer goes to the server once, not as shadow."""
    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    await h.c.on_burst(plan(1, analysis=True))
    await armed(h, 1)
    places = b.calls_of("place")
    check(len(places) == 5 and all(c[1] == "buy" and near(c[2], 0.2) for c in places), f"buy side only: {places}")
    check(h.updates(1)[-1]["orders"] == 5, f"5 orders: {h.updates(1)[-1]}")
    b.move(1.10080)
    await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
    await jump(30.5)
    fin = await finished(h, 1)
    got = h.analyses(1)
    check(fin["status"] == "closed" and len(got) == 1 and got[0]["action"] == "buy" and got[0]["shadow"] is False
          and set(got[0]) == {"action", "score", "reason", "parts", "shadow"}, f"one analysis: {got}")
    h.close()

    b = SimBroker("netting")
    view(b, "down")
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(2, analysis=True))
    await finished(h, 2)
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(len(said) == 1 and "1 stop orders on EURUSD: sell-stop 1.09930, 5 × 0.2 lots per side" in said[0],
          f"sell side only: {said}")
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst({**plan(3, analysis=True), "context": {"expect": 0.2, "ai": 0.5}})
    await armed(h, 3)
    places = b.calls_of("place")
    got = h.analyses(3)
    check(len(places) == 10 and all(near(c[2], 0.1) for c in places), f"both at 0.5%: {places[:2]}")
    check(got[0]["action"] == "both" and got[0]["parts"]["ai"] == 0.5 and got[0]["parts"]["expect"] == 0.2,
          f"the server's context used: {got}")
    h.close()

    fin, said, b = None, None, SimBroker("hedging")
    view(b, "flat")
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(4, analysis=True, sl_unit="pct", sl_money=1.0, lots=0.1))
    await finished(h, 4)
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check("5 × 0.05 lots per side, SL 20.0 pips (≈ 50.00 at risk)" in said[0], f"money: half the lots: {said}")
    h.close()

    b = SimBroker("hedging")
    view(b, "blow")
    h = Harness(b)
    await h.c.on_burst(plan(5, analysis=True))
    fin = await finished(h, 5)
    check(fin["status"] == "skipped" and fin["reason"] == describe(h.analyses(5)[0]) and "jumped" in fin["reason"]
          and not b.calls_of("place"), f"blow-out: {fin}")
    h.close()


@scenario
async def analysis_that_raises_is_no_view():
    """An analysis that raises (a last guard: analyse never does on odd bars) is no view at all: both sides at half
    the risk, as a weak one, and reported so; the run is not skipped."""
    cc = sys.modules[Connector.__module__]
    real = cc.analyse
    cc.analyse = lambda *a, **kw: 1 / 0
    try:
        b = SimBroker("hedging")
        view(b, "up")                                    # bars that would say buy
        h = Harness(b)
        await h.c.on_burst(plan(1, analysis=True))
        await armed(h, 1)
    finally:
        cc.analyse = real
    places, got = b.calls_of("place"), h.analyses(1)
    check(len(places) == 10 and {c[1] for c in places} == {"buy", "sell"} and all(near(c[2], 0.1) for c in places),
          f"both sides at 0.5%: {places[:2]}")
    check(len(got) == 1 and got[0]["action"] == "both" and got[0]["reason"] == "weak" and got[0]["shadow"] is False
          and set(got[0]["parts"].values()) == {None}, f"reported as no view: {got}")
    await h.c.on_burst_cancel(1)
    check((await finished(h, 1))["status"] == "cancelled", f"{h.final(1)}")
    h.close()


@scenario
async def unclear_view_under_the_minimum_lot():
    """An unclear view on a money stop whose half lot is under the minimum lot: fewer orders of the minimum lot (half
    of 5 x 0.01 is 2 x 0.01), or the run skipped when not even one fits (half of 1 x 0.01)."""
    s = SymbolSpec("EURUSD", pip=PIP, pip_value_per_lot=10.0)               # min 0.01, step 0.01
    got = [Connector._halved(n, lots, s) for n, lots in ((5, 0.01), (1, 0.01), (4, 0.05))]
    check(got == [(2, 0.01), (0, 0.0), (4, 0.02)], f"_halved: {got}")
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b, dry_run=True)
    await h.c.on_burst(plan(1, analysis=True, sl_unit="pct", sl_money=1.0, lots=0.01))
    await finished(h, 1)
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(len(said) == 1 and "would place 4 stop orders" in said[0] and "2 × 0.01 lots per side" in said[0],
          f"2 x 0.01 per side: {said}")
    h.close()
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(plan(2, analysis=True, sl_unit="pct", sl_money=1.0, lots=0.01, trades=1))
    fin = await finished(h, 2)
    check(fin["status"] == "skipped" and fin["reason"] == "half the risk is under the minimum lot size"
          and not b.calls_of("place"), f"none fits: {fin}")
    h.close()


@scenario
async def analysis_without_m1_in_time_and_unknown_kinds():
    """Bars not in within ANALYSIS_WAIT count as missing (H1 alone is not clear: both sides); a run kind or stop
    unit the connector does not know is skipped, never guessed."""
    b = SimBroker("hedging")
    view(b, "up")
    m1 = b.bars["M1"]
    b.bars["M1"] = lambda: time.sleep(0.3) or m1
    was, Connector.ANALYSIS_WAIT = Connector.ANALYSIS_WAIT, 0.05
    try:
        h = Harness(b)
        await h.c.on_burst(plan(1, analysis=True))
        await armed(h, 1)
    finally:
        Connector.ANALYSIS_WAIT = was
    got = h.analyses(1)
    check(len(b.calls_of("place")) == 10 and got[0]["action"] == "both" and got[0]["parts"]["momentum"] is None
          and got[0]["parts"]["trend"] == 1.0, f"M1 late: {got}")
    h.close()

    b = SimBroker("hedging")
    h = Harness(b)
    await h.c.on_burst({**plan(2), "kind": "turbo"})
    await h.c.on_burst(plan(3, sl_unit="eur"))
    check(h.final(2)["reason"] == "unknown run kind" and h.final(3)["reason"] == "invalid plan (invalid_unit)"
          and not b.calls_of("place") and {2, 3} <= h.c.burst_done and not h.c.bursts, f"{h.final(2)} {h.final(3)}")
    h.close()


async def one_side_placing(spike: bool, lag: str | None = None, unreadable: str | None = None):
    """A buy-only run (the analysis picked it) whose book reads empty after its first 2 orders: they filled (spike)
    or the read was blank; (harness, broker, the tickets placed). lag: MT5 shows the fills late in its list of
    positions ("list", their deals already in history) or in its history ("history", the positions listed).
    unreadable: the broker method ("positions", "deal_entry") that raises an IPC error while the run is placing,
    each failed call kept in b.raised."""
    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    b.raised = []
    if unreadable:
        read = getattr(b, unreadable)

        def unread(*a, **kw):
            if "armed" not in h.statuses(1):            # placing
                b.raised.append(unreadable)
                raise RuntimeError(f"{unreadable} failed: (-10005, 'IPC timeout')")
            return read(*a, **kw)
        setattr(b, unreadable, unread)
    place, placed = b.place_pending, []

    def place_then(*a, **kw):
        placed.append(place(*a, **kw))
        if len(placed) == 2:                             # b0 b1 are in: the release fills both, or a read is blank
            if spike:
                b.move(1.10080)
                for t in placed:                         # a position's identifier is its order's ticket (hedging)
                    if lag == "list":
                        b.unlist(t, 0.15)
                    elif lag == "history":
                        b.hide(t, 0.15)
            else:
                b.blank = 1
        return placed[-1]
    b.place_pending = place_then
    await h.c.on_burst(plan(1, analysis=True))
    await armed(h, 1)
    return h, b, placed


@scenario
async def one_side_filled_while_placing():
    """A run on one side has no other side left in the book: an empty book is a fill when a position of the run
    shows it, or the first order's deal does while the list of positions is behind (placing stops, no late orders at
    stale prices), and still a bad read when nothing filled, also when those reads fail."""
    h, b, placed = await one_side_placing(spike=True)
    check(len(placed) == 2 and h.updates(1)[-1]["orders"] == 2 and len(b.calls_of("place")) == 2,
          f"placing stopped at the fill: {b.calls_of('place')}")
    check(any("filled while placing" in m for m in h.logs("warn"))
          and not any("rejections" in m for m in h.logs("warn")), f"reported: {h.logs('warn')}")
    await until(lambda: len(h.of("trade_opened")) == 2, "2 fills")
    await jump(30.5)
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["side"] == "buy" and fin["fills"] == 2 and not b.orders and not b.pos,
          f"final {fin}")
    h.close()

    for lag in ("list", "history"):                      # either one alone shows the fill: placing stops all the same
        h, b, placed = await one_side_placing(spike=True, lag=lag)
        check(len(placed) == 2 and len(b.calls_of("place")) == 2
              and any("filled while placing" in m for m in h.logs("warn")),
              f"{lag} behind: placing stopped at the fill: {b.calls_of('place')} {h.logs('warn')}")
        await until(lambda: len(h.of("trade_opened")) == 2, f"{lag} behind: 2 fills")
        await jump(30.5)
        fin = await finished(h, 1)
        check(fin["status"] == "closed" and fin["fills"] == 2 and not b.orders and not b.pos, f"{lag}: final {fin}")
        h.close()

    h, b, placed = await one_side_placing(spike=False)
    check(len(placed) == 5 and not b.blank and not any("filled while placing" in m for m in h.logs("warn")),
          f"a blank read: all 5 placed {len(placed)} {h.logs('warn')}")
    await h.c.on_burst_cancel(1)
    fin = await finished(h, 1)
    check(fin["status"] == "cancelled" and not b.orders, f"cancelled {fin}")
    h.close()

    # the blank read, and the read that would show a fill fails (an IPC error): not a fill, and not the run's end
    for unread in ("positions", "deal_entry"):
        h, b, placed = await one_side_placing(spike=False, unreadable=unread)
        check(len(placed) == 5 and b.raised and set(b.raised) == {unread} and h.updates(1)[-1]["orders"] == 5
              and not any("filled while placing" in m for m in h.logs("warn")),
              f"{unread} unreadable: all 5 placed, armed {len(placed)} {b.raised} {h.statuses(1)} {h.logs('warn')}")
        await h.c.on_burst_cancel(1)
        fin = await finished(h, 1)
        check(fin["status"] == "cancelled" and not b.orders and "error" not in h.statuses(1),
              f"{unread} unreadable: cancelled, never an error {fin}")
        h.close()


@scenario
async def analysis_straddle_from_the_price_after_the_bars():
    """The bars take a moment: the straddle and the pre-release mid (news_against) come from the price after them."""
    b = SimBroker("hedging")
    view(b, "flat")
    m1 = b.bars["M1"]
    b.bars["M1"] = lambda: b.move(1.10100) or m1         # the price moves 10 pips while the M1 bars are read
    h = Harness(b)
    await h.c.on_burst(plan(1, analysis=True))
    await armed(h, 1)
    places = b.calls_of("place")
    check(len(places) == 10 and near(places[0][3], 1.10175) and near(places[1][3], 1.10030),
          f"7 pips from the price after the bars: {places[:2]}")
    check(near(h.c.bursts[1]["pre_mid"], 1.101025), f"pre-release mid: {h.c.bursts[1]['pre_mid']}")
    await h.c.on_burst_cancel(1)
    await finished(h, 1)
    h.close()


@scenario
async def analysis_spread_checked_again_after_the_bars():
    """The spread widens while the bars are read (the seconds before a release): over its cap on the price after them,
    the run is skipped for the spread, in v1.2.0's words, with its stop in pips or in money, and no order goes out;
    still within it, the run goes on."""
    for bid, params, spread in ((1, {}, 4.0), (2, {"sl_unit": "pct", "sl_money": 1.0, "lots": 0.1}, 4.0),
                                (3, {}, 2.5)):
        b = SimBroker("hedging")
        view(b, "up")
        m1 = b.bars["M1"]
        b.bars["M1"] = lambda b=b, m1=m1, spread=spread: setattr(b, "spread", spread * PIP) or m1
        h = Harness(b)
        await h.c.on_burst(plan(bid, analysis=True, **params))
        if spread > 3.0:
            fin = await finished(h, bid)
            check(fin["status"] == "skipped" and fin["reason"] == f"spread {spread} pips > 3.0"
                  and not b.calls_of("place") and [a["action"] for a in h.analyses(bid)] == ["buy"],
                  f"#{bid} {params}: skipped on the spread after the bars: {fin} {b.calls_of('place')}")
        else:
            await armed(h, bid)
            places = b.calls_of("place")
            check(len(places) == 5 and all(c[1] == "buy" for c in places) and near(places[0][3], 1.10095),
                  f"#{bid}: within the cap, the buy side from the price after the bars: {places[:1]}")
            await h.c.on_burst_cancel(bid)
            await finished(h, bid)
        h.close()


@scenario
async def h1_bars_asked_for_with_the_plan():
    """The H1 bars are asked for when the plan comes if it leaves at least BARS_EARLY before the check (and read only
    then); a plan that leaves less asks for them at the check."""
    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    await h.c.on_burst(plan(1, analysis=True, seconds_until=60 + Connector.BARS_EARLY + 5))
    await until(lambda: b.calls_of("rates"), "the H1 bars asked for")
    check(b.calls_of("rates") == [("rates", "H1", H1_BARS)] and h.statuses(1) == ["scheduled"],
          f"H1 at once: {b.calls_of('rates')}")
    await jump(10.5)
    await armed(h, 1)
    check(b.calls_of("rates") == [("rates", "H1", H1_BARS), ("rates", "M1", M1_BARS)], f"H1 once: {b.calls_of('rates')}")
    await h.c.on_burst_cancel(1)
    await finished(h, 1)
    h.close()

    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    await h.c.on_burst(plan(2, analysis=True, seconds_until=60 + Connector.BARS_EARLY - 1))
    await asyncio.sleep(0.1)
    check(not b.calls_of("rates") and h.c.bursts[2]["h1"] is None, f"none before the check: {b.calls_of('rates')}")
    await jump(4.5)
    await armed(h, 2)
    check(sorted(c[1] for c in b.calls_of("rates")) == ["H1", "M1"], f"both at the check: {b.calls_of('rates')}")
    await h.c.on_burst_cancel(2)
    await finished(h, 2)
    h.close()


# ── v1.3.0: News Rapid (kind "rapid"): stop-limits sized by the analysis' confidence ──
def rapid(bid, context=None, seconds_until=0.5, **params):
    """A News Rapid plan: 3 orders per side each risking 0.2 % (20 of 10k) at an 8 pip stop (0.25 lots), triggers 2
    pips out, the limit at the stops level (0.5 pip behind), TP 10 (confidence 0) .. 15 (1) per order, expiry 30 s,
    hold 30 s, the analysis on. On SimBroker's 1.10000 / 1.10005: a buy-stop-limit 1.10025 limit 1.10020 (SL 1.09940)
    and a sell-stop-limit 1.09980 limit 1.09985 (SL 1.10065), their TP 4 pips from the limit at confidence 0."""
    p = {"orders": 3, "risk_min": 0.2, "risk_max": 0.2, "sl_pips": 8.0, "offset_pips": 2.0, "limit_pips": 0.0,
         "tp_min_usd": 10.0, "tp_max_usd": 15.0, "expire_seconds": 30, "hold_seconds": 30, "analysis": True,
         "lead_seconds": 60}
    p.update(params)
    return {**plan(bid, seconds_until=seconds_until), "kind": "rapid", "params": p,
            **({"context": context} if context else {})}


def placed_as(c, side, lots, trigger, sl, tp, comment, limit):
    """A stop-limit's place call (SimBroker.calls) is `side` `lots` at `trigger` / `limit` with that SL, TP, comment."""
    return len(c) == 8 and c[1] == side and c[6] == comment and all(
        near(a, b) for a, b in zip((c[2], c[3], c[4], c[5], c[7]), (lots, trigger, sl, tp, limit)))


async def run_cancelled(h, bid):
    """Cancels run `bid`: it ends "cancelled" with nothing left in the book."""
    await h.c.on_burst_cancel(bid)
    fin = await finished(h, bid)
    check(fin["status"] == "cancelled" and not h.b.orders, f"#{bid} cancelled: {fin} {h.b.orders}")


@scenario
def rapid_params_adapter_and_paper():
    """A Rapid plan's params are the rapid block as clamp_rapid clamps it (Y follows X when not set), the check's lead
    the burst's default when the plan has none, no spread cap unless it sets one, never run as a v1.2.0 plan.
    MT5Adapter hands kind and limit_price to MT5Broker (its default still a plain stop) and order_state, pending_room,
    stop_limit_allowed through; paper mode refuses pending orders (nothing there to hold them)."""
    p = Connector._rapid_params(None)
    check(p["kind"] == "rapid" and p["orders"] == 10 and p["max_hold_seconds"] == p["hold_seconds"] == 30
          and p["lead_seconds"] == 60 and p["max_spread_pips"] is None and p["sl_unit"] == "pips" and p["analysis"]
          and not Connector._legacy({**p, "analysis": False}), f"defaults: {p}")
    p = Connector._rapid_params({"lead_seconds": 5, "max_spread_pips": 99, "max_hold_seconds": 90, "orders": 99})
    check((p["lead_seconds"], p["max_spread_pips"], p["max_hold_seconds"], p["orders"]) == (10, 20.0, 90, 40),
          f"clamped: {p}")
    try:
        Connector._rapid_params({"sl_unit": "eur"})
        check(False, "a stop unit it cannot read refuses the plan")
    except ValueError as e:
        check(str(e) == "invalid_unit", f"{e}")
    got = []
    a = MT5Adapter.__new__(MT5Adapter)                  # no MT5Broker (no terminal): one that echoes its calls
    a.b = type("Echo", (), {"place_pending": lambda self, *args: got.append(args) or 7,
                            "order_state": lambda self, t: {"state": "placed", "ticket": t},
                            "pending_room": lambda self: 3, "stop_limit_allowed": lambda self, s: s == "EURUSD"})()
    check(a.place_pending("EURUSD", "buy", 0.25, 1.10025, 1.0994, 1.1006, "fxb:1:0", 60, kind="stop_limit",
                          limit_price=1.1002) == 7
          and got[-1] == ("EURUSD", "buy", 0.25, 1.10025, 1.0994, 1.1006, "fxb:1:0", 60, "stop_limit", 1.1002)
          and a.place_pending("EURUSD", "sell", 1.0, 1.0993, 1.1003, None, "fxb:1:s") == 7
          and got[-1][-4:] == ("fxb:1:s", None, "stop", None), f"kind and limit passed through: {got}")
    check(a.order_state(9) == {"state": "placed", "ticket": 9} and a.pending_room() == 3
          and a.stop_limit_allowed("EURUSD") and not a.stop_limit_allowed("XAUUSD"), "the rest passed through")
    paper = PaperBroker()
    try:
        paper.place_pending("EURUSD", "buy", 0.1, 1.1003, 1.099, 1.101, "fxb:1:0", kind="stop_limit",
                            limit_price=1.1002)
        check(False, "paper mode places no pending order")
    except BrokerError as e:
        check(str(e) == "paper mode: pending orders need MetaTrader 5", f"paper: {e}")
    check(paper.pending_orders() == [] and paper.order_state(1) is None and paper.pending_room() is None
          and paper.stop_limit_allowed("EURUSD") is False, "paper: no book, no stop-limits")


@scenario
async def classic_burst_places_plain_stops_as_before():
    """A news run of kind "burst" (or none: v1.2.0's plan) places plain stops through place_pending's default path:
    neither kind nor limit sent (an adapter or fake without them still works), none of Rapid's broker calls."""
    b = SimBroker("hedging")
    place, kws = b.place_pending, []
    b.place_pending = lambda *a, **kw: kws.append(kw) or place(*a, **kw)
    h = Harness(b)
    await h.c.on_burst(plan(1))
    await h.c.on_burst({**plan(2), "kind": "burst", "symbol": "GBPUSD"})
    await armed(h, 1)
    await armed(h, 2)
    check(len(kws) == 20 and all(kw == {} for kw in kws) and all(len(c) == 7 for c in b.calls_of("place"))
          and {o["kind"] for o in b.orders.values()} == {"stop"} and not b.calls_of("stop_limit_allowed")
          and {h.c.bursts[1]["order_kind"], h.c.bursts[2]["order_kind"]} == {"stop"}, f"plain stops: {kws[:1]}")
    b.move(1.10080)                                      # the buy-stops fill at the market, as before
    await until(lambda: len(h.of("trade_opened")) == 5, "5 fills")
    check(all("strategy" not in m["trade"] and m["trade"]["burst_id"] == 1 for m in h.of("trade_opened"))
          and not b.calls_of("state"), f"a burst's reports as before: {h.of('trade_opened')[:1]}")
    await h.c.push_snapshot()
    check({p["strategy"] for p in h.of("snapshot")[-1]["positions"]} == {"burst"}, "the snapshot says burst")
    await jump(30.5)
    check((await finished(h, 1))["status"] == "closed", f"{h.final(1)}")
    await run_cancelled(h, 2)
    h.close()


@scenario
async def rapid_straddle_triggered_filled_other_side_cancelled():
    """A Rapid straddle (the analysis unclear: both sides at its confidence, 0 here): 3 stop-limits per side, each
    with its SL and TP at the broker measured from its limit (10 at confidence 0: 4 pips on 0.25 lots). A side whose
    trigger is reached is a limit now, still pending: no fill, the other side stays. The first FILL (at the limit)
    cancels the other side before the run says triggered; the broker's TP closes them; the reports say rapid."""
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    places, st = b.calls_of("place"), h.c.bursts[1]
    buy, sell = (1.10025, 1.0994, 1.1006), (1.0998, 1.10065, 1.09945)       # trigger, SL, TP
    check(len(places) == 6 and all(placed_as(places[2 * i], "buy", 0.25, *buy, f"fxb:1:{i}", 1.1002)
                                   and placed_as(places[2 * i + 1], "sell", 0.25, *sell, f"fxb:1:{i}", 1.09985)
                                   for i in range(3)),
          f"stop-limits, interleaved, SL 8 / TP 4 pips from the limit: {places}")
    check(len(b.expire_ins) == 6 and all(140 < e <= 150.5 for e in b.expire_ins),
          f"broker-side expiry = release + Rapid's expire (30 s) + 120 s from now: {b.expire_ins}")
    check(st["order_kind"] == "stop_limit" and near(st["c"], 0.0) and near(st["tp_usd"], 10.0)
          and {o["kind"] for o in b.orders.values()} == {"stop_limit"} and h.analyses(1)[0]["action"] == "both",
          f"the run: c {st['c']}, TP {st['tp_usd']}")
    b.move(1.10025)                                      # the buys' trigger: buy limits at 1.10020 now
    await until(lambda: all(r["triggered"] for r in st["pending"].values() if r["side"] == "buy"), "triggers seen")
    await asyncio.sleep(0.05)
    check(len(b.orders) == 6 and not b.pos and "triggered" not in h.statuses(1) and not b.calls_of("cancel")
          and {o["kind"] for o in b.orders.values() if o["side"] == "buy"} == {"limit"},
          f"triggered is no fill: {h.statuses(1)} {b.calls_of('cancel')}")
    b.move(1.10015)                                      # back at the limit: the 3 buys fill at 1.10020
    await until(lambda: len(h.of("trade_opened")) == 3, "3 fills")
    check(h.at_trigger == [[]] and len(b.calls_of("cancel")) == 3 and not b.orders,
          f"the sells cancelled before 'triggered': {h.at_trigger} {b.calls_of('cancel')}")
    opened = [m["trade"] for m in h.of("trade_opened")]
    check(all(t["burst_id"] == 1 and t["strategy"] == "rapid" and t["side"] == "buy" and near(t["entry_price"], 1.1002)
              and near(t["sl"], 1.0994) and near(t["tp"], 1.1006) for t in opened), f"trade_opened: {opened}")
    await h.c.push_snapshot()
    snap = h.of("snapshot")[-1]["positions"]
    check(len(snap) == 3 and all(p["burst_id"] == 1 and p["strategy"] == "rapid" for p in snap), f"snapshot: {snap}")
    b.move(1.10060)                                      # the TP at the broker
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["side"] == "buy" and fin["fills"] == 3 and near(fin["pnl"], 30.0)
          and not b.pos and not b.calls_of("close"), f"closed at the broker's TP: {fin} {b.calls_of('close')}")
    check(all(m["trade"].get("strategy") == "rapid" for m in h.of("trade_closed")), f"{h.of('trade_closed')[:1]}")
    # its id was kept (rapid_open.json) while it could leave something behind, and forgotten once nothing was left
    check(not h.c.rapid_news and json.loads(h.c._rapid_file(h.c.scalper._open_file()).read_text()) == {},
          f"a clean run leaves no news id: {h.c.rapid_news}")
    h.close()


@scenario
async def rapid_triggered_never_filled_is_no_fill():
    """A triggered stop-limit whose price never comes back to its limit is no fill: both sides stay until the
    expiry cancels everything still pending (the limits too), the run "expired" with no trade. One the broker removes
    itself (its own expiry) is looked up in its history: gone unfilled, dropped at once, never a fill. After a crash
    the clean-up hands the sweeper only an order that may have filled, not one whose history shows it expired."""
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    st = h.c.bursts[1]
    b.move(1.10025)                                      # buy limits at 1.10020; the price never comes back
    await until(lambda: all(r["triggered"] for r in st["pending"].values() if r["side"] == "buy"), "triggers seen")
    await jump(31)                                       # past the expiry
    fin = await finished(h, 1)
    check(fin["status"] == "expired" and not fin["fills"] and fin["side"] is None and not h.of("trade_opened")
          and not b.orders and len(b.calls_of("cancel")) == 6 and "triggered" not in h.statuses(1),
          f"expired, not a fill: {fin} {b.calls_of('cancel')}")
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    with Caught() as lines:
        await h.c.on_burst(rapid(2))
        await armed(h, 2)
        st = h.c.bursts[2]
        b.move(1.10025)
        await until(lambda: all(r["triggered"] for r in st["pending"].values() if r["side"] == "buy"), "triggers")
        gone = sorted(t for t, o in b.orders.items() if o["side"] == "buy")
        for t in gone:
            b.expire(t)                                  # the broker's own expiry, before the run's
        await until(lambda: not any(t in st["pending"] or t in st["unknown"] for t in gone), "dropped")
    check(all(f"Rapid run #2: order #{t} expired without a fill" in lines for t in gone) and len(st["pending"]) == 3
          and st["side"] is None and "triggered" not in h.statuses(2) and not h.of("trade_opened"),
          f"no fill: {st['side']} {h.statuses(2)} {[s for s in lines if 'without' in s]}")
    await run_cancelled(h, 2)
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(3, seconds_until=600))     # waiting: its state is set up by hand, as a crash leaves it
    st = h.c.bursts[3]
    expired, unseen = (b.place_pending("EURUSD", "buy", 0.25, 1.10025, 1.0994, 1.1006, f"fxb:3:{i}", None,
                                       "stop_limit", 1.1002) for i in range(2))
    live = b.place_pending("EURUSD", "sell", 0.25, 1.0998, 1.10065, 1.09945, "fxb:3:0", None, "stop_limit", 1.09985)
    b.expire(expired)                                    # gone unfilled, in its history
    b.orders.pop(unseen)                                 # gone, its history not showing it yet
    rec = {"side": "buy", "lots": 0.25, "sl": 1.0994, "tp": 1.1006, "limit": 1.1002, "triggered": True}
    st["placed"].update((expired, unseen, live))
    st["pending"][live] = {**rec, "side": "sell", "limit": 1.09985, "triggered": False}
    st["unknown"].update({expired: [rec, 0], unseen: [dict(rec), 0]})
    await h.c._burst_cleanup(st)
    check(("cancel", live, "ok") in b.calls and not b.orders and expired not in h.c.unchecked
          and unseen in h.c.unchecked and ("state", expired) in b.calls, f"handed on: {list(h.c.unchecked)}")
    h.c.unchecked.clear()
    st["pending"].clear()
    st["unknown"].clear()
    await run_cancelled(h, 3)
    h.close()


@scenario
async def rapid_blank_order_book_read_is_no_fill():
    """One empty orders_get() read while a Rapid run's stop-limits wait (netting_blank_order_book_read's, with stop-
    limits): their history says they are still in the book ("placed"), so none is dropped as gone unfilled; back in
    the next read, they are pending again and cancelled at expiry."""
    for mode in ("netting", "hedging"):
        b = SimBroker(mode)
        view(b, "flat")
        h = Harness(b)
        with Caught() as lines:
            await h.c.on_burst(rapid(1))
            await armed(h, 1)
            st, tickets = h.c.bursts[1], sorted(b.orders)
            n = len(b.calls_of("state"))
            b.blank = 1                                  # the next read of the book is empty
            await until(lambda: {c[1] for c in b.calls_of("state")[n:]} >= set(tickets), "their history looked up")
            await until(lambda: sorted(st["pending"]) == tickets and not st["unknown"], "pending again")
            await jump(31)                               # past the expiry
            fin = await finished(h, 1)
        check(len(tickets) == (2 if mode == "netting" else 6) and fin["status"] == "expired" and not fin["fills"]
              and all(("cancel", t, "ok") in b.calls for t in tickets) and not b.orders and not h.of("trade_opened"),
              f"{mode}: cancelled at expiry: {fin} {b.calls_of('cancel')}")
        check(not any("without a fill" in s for s in lines), f"{mode}: none taken as gone unfilled: "
                                                             f"{[s for s in lines if 'without' in s]}")
        h.close()


@scenario
async def rapid_order_state_unreadable_is_looked_up_again():
    """A stop-limit gone from the book with no deal in history yet, its history unreadable (order_state fails: an IPC
    timeout), may have filled: never dropped as gone unfilled, it is looked up again. Once its deal shows (its
    position listed) the fill is taken up and the other side cancelled. After a crash the clean-up hands such an order
    to the sweeper, also one whose history, could it be read, would show it expired."""
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(1, orders=1))
    await armed(h, 1)
    st, asked = h.c.bursts[1], []

    def order_state(t):
        asked.append(t)
        raise RuntimeError("history_orders_get failed: (-10005, 'IPC timeout')")
    b.order_state = order_state
    buy, sell = (next(t for t, o in b.orders.items() if o["side"] == s) for s in ("buy", "sell"))
    b.hide(buy, 10**6)                                   # its deal not in history yet, its position not listed yet
    b.unlist(buy, 10**6)
    b.move(1.10025)
    b.move(1.10015)                                      # the buy fills at its limit (1.10020)
    await until(lambda: asked.count(buy) >= 2, "its history asked again")
    check(buy in st["unknown"] and sell in st["pending"] and st["side"] is None and not b.calls_of("cancel")
          and not h.of("trade_opened"), f"looked up again, not dropped: {st['side']} {b.calls_of('cancel')}")
    with b.lock:
        b.hidden.clear()
        b.unlisted.clear()
    await until(lambda: h.of("trade_opened") and ("cancel", sell, "ok") in b.calls, "the fill taken up")
    check(buy not in st["unknown"] and st["side"] == "buy"
          and [m["trade"]["ticket"] for m in h.of("trade_opened")] == [buy] and not b.orders,
          f"the buy's fill, the sell cancelled: {st['side']} {h.of('trade_opened')}")
    b.move(1.10060)                                      # its TP at the broker
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["fills"] == 1 and near(fin["pnl"], 10.0), f"closed at its TP: {fin}")
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(2, seconds_until=600))     # waiting: its state is set up by hand, as a crash leaves it
    st = h.c.bursts[2]
    t = b.place_pending("EURUSD", "buy", 0.25, 1.10025, 1.0994, 1.1006, "fxb:2:0", None, "stop_limit", 1.1002)
    b.expire(t)                                          # gone unfilled, but its history cannot be read
    asked.clear()
    b.order_state = order_state
    st["placed"].add(t)
    st["unknown"][t] = [{"side": "buy", "lots": 0.25, "sl": 1.0994, "tp": 1.1006, "limit": 1.1002, "triggered": True},
                        0]
    await h.c._burst_cleanup(st)
    check(t in h.c.unchecked and asked == [t], f"handed to the sweeper: {list(h.c.unchecked)} {asked}")
    h.c.unchecked.clear()
    st["unknown"].clear()
    await run_cancelled(h, 2)
    h.close()


def dealt_now(b):
    """SimBroker `b` with its deals stamped now (UTC), not at its fixed release time: a Rapid close is then booked to
    today, the day Rapid's room rolled its day to. `b` back."""
    deal = b._deal

    def stamped(*a, **kw):
        deal(*a, **kw)
        b.deals[-1]["time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    b._deal = stamped
    return b


@scenario
async def rapid_news_id_kept_while_something_is_owed():
    """A Rapid news run that ended keeps its id in rapid_news (and rapid_open.json) while a report of it is owed: a
    position handed to the sweeper (out of the list as the run ended, or its clean-up's close refused after a crash)
    or an order left to look up (unchecked); and after a failed run, whose clean-up may miss what it could not read.
    Its position stays Rapid's (strategy_of), the sweeper's close of it is reported as Rapid's and booked to Rapid's
    day, and the id is forgotten at the first sweep that finds nothing of the run left."""
    for case in ("out of the list as it ended", "its clean-up's close refused"):
        b = dealt_now(SimBroker("hedging"))
        view(b, "flat")
        h = Harness(b)
        path = h.c._rapid_file(h.c.scalper._open_file())
        await h.c.on_burst(rapid(1, orders=1))
        await armed(h, 1)
        buy = next(t for t, o in b.orders.items() if o["side"] == "buy")
        b.move(1.10025)
        b.move(1.10015)                                  # the buy fills at its limit, 0.5 pip down on 0.25 lots: -1.25
        await until(lambda: h.of("trade_opened") and not b.orders, f"{case}: filled, the sell cancelled")
        if case == "out of the list as it ended":
            b.unlist(buy, 10**6)                         # BURST_GONE polls without it: left to the sweeper
        else:                                            # the terminal gone: the run fails, its clean-up's close too
            h.c.BURST_MAX_ERRORS, b.fail_close, b.broken = 5, 1, True
        fin = await finished(h, 1)
        with b.lock:
            b.unlisted.clear()
        b.broken = False
        held = await h.c.call(b.positions)
        check(fin["status"] == ("closed" if case == "out of the list as it ended" else "error")
              and buy in h.c.unreported and h.c.rapid_news == {1}
              and json.loads(path.read_text()) == {"1": {"news": True}}
              and [h.c.strategy_of(x) for x in held] == ["rapid"],
              f"{case}: its id kept, its position Rapid's: {fin} {h.c.rapid_news} {list(h.c.unreported)}")
        await h.c.sweep_orphans()
        closes = [m["trade"] for m in h.of("trade_closed")]
        check(len(closes) == 1 and closes[0]["ticket"] == buy and closes[0]["burst_id"] == 1
              and closes[0]["strategy"] == "rapid" and near(closes[0]["pnl"], -1.25) and not b.pos
              and near(h.c.rapid_day.day["pnl"], -1.25) and h.c.rapid_booked == {buy: True},
              f"{case}: swept, reported and booked as Rapid's: {closes} {h.c.rapid_day.day}")
        check(h.c.rapid_news == {1}, f"{case}: still kept by the sweep that closed it: {h.c.rapid_news}")
        await h.c.sweep_orphans()
        check(not h.c.rapid_news and json.loads(path.read_text()) == {},
              f"{case}: nothing of the run left: its id forgotten: {h.c.rapid_news}")
        h.close()

    # failed before any fill: nothing handed on, the id kept all the same until a sweep finds nothing of the run
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    path = h.c._rapid_file(h.c.scalper._open_file())
    h.c.BURST_MAX_ERRORS = 5
    await h.c.on_burst(rapid(2, orders=1))
    await armed(h, 2)
    b.broken = True
    fin = await finished(h, 2)
    b.broken = False
    check(fin["status"] == "error" and not b.orders and not h.c.unreported and not h.c.unchecked
          and h.c.rapid_news == {2} and json.loads(path.read_text()) == {"2": {"news": True}},
          f"failed: its id kept: {fin} {h.c.rapid_news}")
    await h.c.sweep_orphans()
    check(not h.c.rapid_news and json.loads(path.read_text()) == {}, f"then forgotten: {h.c.rapid_news}")
    h.close()

    # cancelled, an order of it left to look up: its fill, closed at its stop meanwhile, is Rapid's
    b = dealt_now(SimBroker("hedging"))
    h = Harness(b)
    path = h.c._rapid_file(h.c.scalper._open_file())
    await h.c.on_burst(rapid(3, seconds_until=600))     # waiting: its state is set up by hand, as a crash leaves it
    st = h.c.bursts[3]
    st["spec"] = b.symbol_spec("EURUSD")
    h.c.rapid_news.add(3)                                # as before its first order
    h.c._save_rapid_open()
    t = b.place_pending("EURUSD", "buy", 0.25, 1.10025, 1.0994, 1.1006, "fxb:3:0", None, "stop_limit", 1.1002)
    with b.lock:                                         # filled at its limit, closed at its stop: 8 pips, -20
        o = b.orders.pop(t)
        b.done[t] = ("filled", o)
        b._fill(o, 1.1002)
        b._close(b.pos[t], 0.25, 1.0994)
    b.hide(t, 10**6)                                     # its deals not in history yet
    st["placed"].add(t)
    st["unknown"][t] = [{"side": "buy", "lots": 0.25, "sl": 1.0994, "tp": 1.1006, "limit": 1.1002, "triggered": True},
                        0]
    await h.c._burst_cleanup(st)
    check(list(h.c.unchecked) == [t], f"handed to the sweeper: {list(h.c.unchecked)}")
    st["unknown"].clear()
    await run_cancelled(h, 3)
    check(h.c.rapid_news == {3} and json.loads(path.read_text()) == {"3": {"news": True}},
          f"cancelled, an order left to look up: its id kept: {h.c.rapid_news}")
    await h.c.sweep_orphans()                            # its deals still hidden: the order is looked up again later
    check(h.c.rapid_news == {3} and json.loads(path.read_text()) == {"3": {"news": True}}
          and list(h.c.unchecked) == [t] and not h.of("trade_closed"),
          f"a sweep that cannot see its fill yet keeps the id: {h.c.rapid_news} {list(h.c.unchecked)}")
    with b.lock:
        b.hidden.clear()
    await h.c.sweep_orphans()
    opened, closes = [m["trade"] for m in h.of("trade_opened")], [m["trade"] for m in h.of("trade_closed")]
    check([x["ticket"] for x in opened] == [t] and opened[0]["strategy"] == "rapid" and len(closes) == 1
          and closes[0]["ticket"] == t and closes[0]["burst_id"] == 3 and closes[0]["strategy"] == "rapid"
          and near(closes[0]["pnl"], -20.0) and near(h.c.rapid_day.day["pnl"], -20.0) and not h.c.unchecked,
          f"its fill taken up, reported and booked as Rapid's: {opened} {closes} {h.c.rapid_day.day}")
    await h.c.sweep_orphans()
    check(not h.c.rapid_news and json.loads(path.read_text()) == {}, f"then forgotten: {h.c.rapid_news}")
    h.close()


@scenario
async def rapid_one_side_or_none_by_the_analysis():
    """The analysis always runs before a Rapid run places. Clear: one side, at its confidence (0.7: TP 13.5 = 5.4
    pips); a blow-out skips the run. Switched off it only watches (shadow): both sides, its confidence still setting
    the take-profit (and the risk)."""
    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    places, got = b.calls_of("place"), h.analyses(1)
    check(len(places) == 3 and all(placed_as(c, "buy", 0.25, 1.10025, 1.0994, 1.10074, f"fxb:1:{i}", 1.1002)
                                   for i, c in enumerate(places))
          and len(got) == 1 and got[0]["action"] == "buy" and got[0]["shadow"] is False, f"buy only: {places} {got}")
    await run_cancelled(h, 1)
    h.close()

    b = SimBroker("hedging")
    view(b, "blow")
    h = Harness(b)
    await h.c.on_burst(rapid(2))
    fin = await finished(h, 2)
    check(fin["status"] == "skipped" and fin["reason"] == describe(h.analyses(2)[0]) and "jumped" in fin["reason"]
          and not b.calls_of("place") and f"Rapid run #2 on EURUSD skipped: {fin['reason']}" in h.logs("warn"),
          f"blow-out: {fin} {h.logs('warn')}")
    h.close()

    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    with Caught() as lines:
        await h.c.on_burst(rapid(3, analysis=False))
        await armed(h, 3)
        await asyncio.sleep(0.05)
    places, got = b.calls_of("place"), h.analyses(3)
    check(len(places) == 6 and [c[1] for c in places] == ["buy", "sell"] * 3
          and all(near(c[5], 1.10074 if c[1] == "buy" else 1.09931) for c in places)
          and len(got) == 1 and got[0]["action"] == "buy" and got[0]["shadow"] is True,
          f"shadow: both sides, TP at confidence 0.7: {places[:2]} {got}")
    # the plan's view is the shadow one: once armed, the watch-only analysis is not worked out a second time
    said = [s for s in lines if s.startswith(("Rapid run #3: Analysis (watch only)", "News burst #3: Analysis (watch"))]
    check([c for c in b.calls_of("rates") if c[1] == "M1"] == [("rates", "M1", M1_BARS)] and len(said) == 1,
          f"the view worked out once: {b.calls_of('rates')} {said}")
    await run_cancelled(h, 3)
    h.close()

    # the price moved while the bars were read (the first rates call moves it 5 pips): the triggers and limits are
    # measured from the price after the bars, and so is the pre-release mid
    b = SimBroker("hedging")
    view(b, "flat")
    rates, moved = b.rates, []

    def late(symbol, timeframe, count):
        if not moved:
            moved.append(timeframe)
            b.move(1.1005)
        return rates(symbol, timeframe, count)
    b.rates = late
    h = Harness(b)
    await h.c.on_burst(rapid(4))
    await armed(h, 4)
    st, places, spec = h.c.bursts[4], b.calls_of("place"), b.symbol_spec("EURUSD")
    bid, ask, _ = b.quote("EURUSD")
    buy, sell = (stop_limit_prices(s, bid, ask, spec, 2.0, 0.0) for s in ("buy", "sell"))
    check(moved and near(bid, 1.1005) and near(buy[0], 1.10075) and near(buy[1], 1.1007) and near(sell[0], 1.1003)
          and near(sell[1], 1.10035) and len(places) == 6
          and all(placed_as(places[2 * i], "buy", 0.25, buy[0], buy[1] - 8 * PIP, buy[1] + 4 * PIP, f"fxb:4:{i}",
                            buy[1])
                  and placed_as(places[2 * i + 1], "sell", 0.25, sell[0], sell[1] + 8 * PIP, sell[1] - 4 * PIP,
                                f"fxb:4:{i}", sell[1]) for i in range(3))
          and near(st["pre_mid"], (bid + ask) / 2) and near(st["pre_mid"], 1.100525),
          f"placed from the price after the bars: {moved} {places[:2]} pre-release mid {st['pre_mid']}")
    await run_cancelled(h, 4)
    h.close()


@scenario
async def rapid_skipped_without_stop_limits_or_inside_the_freeze_level():
    """A symbol that takes no stop-limit orders skips the run first (no plain-stop fallback, no analysis); a trigger
    within the freeze level plus the spread skips it (an order that close could not be cancelled); one just outside
    is placed, its stop and take-profit never under that floor."""
    b = SimBroker("hedging")
    view(b, "flat")
    b.stop_limit = False
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    fin = await finished(h, 1)
    check(fin["status"] == "skipped" and fin["reason"] == "stop-limit orders are not allowed on EURUSD"
          and not b.calls_of("place") and not b.calls_of("rates") and not h.analyses(1), f"not allowed: {fin}")
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    b.freeze = 2 * PIP                                   # 2 + the 0.5 pip spread: over the 2 pip offset
    h = Harness(b)
    await h.c.on_burst(rapid(2))
    fin = await finished(h, 2)
    check(fin["status"] == "skipped" and fin["reason"] == "the trigger 2 pips from the price is within the freeze "
          "level and spread of EURUSD (2.5 pips): an order that close could not be cancelled"
          and not b.calls_of("place"), f"freeze: {fin}")
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    b.freeze = 1.5 * PIP                                 # 1.5 + 0.5 = the offset: placed
    h = Harness(b)
    await h.c.on_burst(rapid(3, tp_min_usd=1.0))       # 0.4 pips: held to the floor, 2 pips
    await armed(h, 3)
    places = b.calls_of("place")
    check(len(places) == 6 and placed_as(places[0], "buy", 0.25, 1.10025, 1.0994, 1.1004, "fxb:3:0", 1.1002),
          f"just outside: placed {places[:1]}")
    await run_cancelled(h, 3)
    h.close()


@scenario
async def rapid_netting_one_order_per_side():
    """Netting: one stop-limit per side carrying the run's orders (3 x 0.25 = 0.75 lots) with its own SL and TP at
    the broker (a burst's netting orders have no TP: the connector exits its slices; Rapid leaves a fill to its TP and
    the hold time), split only above volume_max. The fill is one position, its TP closes it."""
    b = SimBroker("netting")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    places = b.calls_of("place")
    check(len(places) == 2 and placed_as(places[0], "buy", 0.75, 1.10025, 1.0994, 1.1006, "fxb:1:b", 1.1002)
          and placed_as(places[1], "sell", 0.75, 1.0998, 1.10065, 1.09945, "fxb:1:s", 1.09985),
          f"one per side, with its TP: {places}")
    b.move(1.10025)
    b.move(1.10015)
    await until(lambda: h.of("trade_opened"), "the buy filled")
    check(len(b.pos) == 1 and not b.orders and near(h.of("trade_opened")[0]["trade"]["lots"], 0.75), f"{b.pos}")
    b.move(1.10060)
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["side"] == "buy" and fin["fills"] == 3 and near(fin["pnl"], 30.0)
          and not b.calls_of("close") and not b.pos, f"its TP: {fin} {b.calls_of('close')}")
    h.close()

    b = SimBroker("netting", volume_max=0.5)
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(2))
    await armed(h, 2)
    got = [(c[1], c[2], c[6]) for c in b.calls_of("place")]
    check(got == [("buy", 0.38, "fxb:2:b1"), ("sell", 0.38, "fxb:2:s1"), ("buy", 0.37, "fxb:2:b2"),
                  ("sell", 0.37, "fxb:2:s2")], f"0.75 over volume_max 0.5: two per side {got}")
    await run_cancelled(h, 2)
    h.close()


@scenario
async def rapid_cut_to_the_pending_order_limit_and_the_margin():
    """The account's pending-order limit cuts the orders per side (hedging: one ticket per order and side; netting:
    one per side carries them all), the cut reported; no room skips the run; a room that cannot be read is no cap. A
    side may take RAPID_MARGIN_USE of the free margin the other Rapid runs left (their orders hold none at the broker
    until filled), held only while the run is on and none of its orders filled (then it is in the account's margin)."""
    for bid, mode, cap, errors, placed, why in (
            (1, "hedging", 4, 0, 4, "pending-order limit"),
            (2, "hedging", 1, 0, 0, "the account's pending-order limit leaves no room for an order"),
            (3, "netting", 2, 0, 2, None), (4, "netting", 1, 0, 0, "the account's pending-order limit leaves no "
                                                                   "room for an order"),
            (5, "hedging", None, 1, 6, None)):
        b = SimBroker(mode)
        view(b, "flat")
        b.limit_orders, b.room_errors = cap, errors
        h = Harness(b)
        await h.c.on_burst(rapid(bid))
        if not placed:
            fin = await finished(h, bid)
            check(fin["status"] == "skipped" and fin["reason"] == why and not b.calls_of("place"), f"#{bid}: {fin}")
        else:
            await armed(h, bid)
            cut = [m for m in h.logs("warn") if "orders per side" in m]
            check(len(b.calls_of("place")) == placed and cut == ([f"Rapid run #{bid}: 2 of 3 orders per side ({why})"]
                                                                  if why else []), f"#{bid}: {placed} {cut}")
            await run_cancelled(h, bid)
        h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    b.margin = 20_000.0                                  # 5000 for 0.25 lots: half the free margin fits one
    h = Harness(b)
    await h.c.on_burst(rapid(6))
    await armed(h, 6)
    check(len(b.calls_of("place")) == 2 and "Rapid run #6: 1 of 3 orders per side (free margin)" in h.logs("warn")
          and near(h.c.bursts[6]["margin"], 5000.0) and near(h.c.bursts[6]["reserved"], 20.0),
          f"cut to the margin: {h.logs('warn')}")
    await h.c.on_burst({**rapid(7), "symbol": "GBPUSD"})
    fin = await finished(h, 7)
    check(fin["status"] == "skipped" and fin["reason"] == "not enough free margin for one order of 0.25 lots on GBPUSD",
          f"#6 holds half the free margin: {fin}")
    await run_cancelled(h, 6)
    await h.c.on_burst({**rapid(8), "symbol": "GBPUSD"})
    await armed(h, 8)
    check(len(b.calls_of("place")) == 4, f"released with #6: {b.calls_of('place')[2:]}")
    await run_cancelled(h, 8)
    h.close()

    b = SimBroker("hedging")
    view(b, "flat")
    b.margin = 6000.0                                    # 1500 for 0.25 lots: 3 fit in half the free margin, not in
    h = Harness(b)                                       # half of what is left with 4500 reserved
    await h.c.on_burst(rapid(9))
    await armed(h, 9)
    check(len(b.calls_of("place")) == 6 and near(h.c.bursts[9]["margin"], 4500.0)
          and not [m for m in h.logs("warn") if "orders per side" in m], f"#9 not cut: {h.logs('warn')}")
    b.move(1.10025)
    b.move(1.10015)                                      # #9's buys filled: its margin is the account's now
    await until(lambda: h.c.bursts[9]["side"] == "buy" and len(h.of("trade_opened")) == 3, "#9 filled")
    await h.c.on_burst({**rapid(10), "symbol": "GBPUSD"})
    await armed(h, 10)
    check(len(b.calls_of("place")) == 12 and not [m for m in h.logs("warn") if "orders per side" in m],
          f"#10 not cut by #9's reservation once #9 filled: {h.logs('warn')}")
    await run_cancelled(h, 10)
    await jump(30.5)                                     # #9 at its hold time
    fin = await finished(h, 9)
    check(fin["status"] == "closed" and fin["fills"] == 3 and not b.pos, f"#9: {fin}")
    h.close()

    # a side whose order needs more margin (a sell 1.5 x a buy here): the count per side is what fits for that side
    b = SimBroker("hedging")
    view(b, "flat")

    def margin_for(symbol, side, lots):
        with b.lock:
            b.calls.append(("margin", side, lots))
            return round((9000.0 if side == "sell" else 6000.0) * lots, 2)
    b.margin_for = margin_for                            # 0.25 lots: a buy 1500 (3 fit in half of 10 000), a sell
    h = Harness(b)                                       # 2250 (only 2 do)
    await h.c.on_burst(rapid(11))
    await armed(h, 11)
    check(len(b.calls_of("place")) == 4 and "Rapid run #11: 2 of 3 orders per side (free margin)" in h.logs("warn")
          and near(h.c.bursts[11]["margin"], 2 * 2250.0) and {c[1] for c in b.calls_of("margin")} == {"buy", "sell"},
          f"cut to the sell side's margin: {h.logs('warn')} {h.c.bursts[11]['margin']}")
    await run_cancelled(h, 11)
    h.close()


@scenario
async def rapid_take_profit_and_risk_by_confidence():
    """Each order's take-profit in money follows the analysis' confidence, as its risk does: weak (unclear, 0) 10 =
    4 pips on 0.25 lots; strong (clear, the forecast and the AI agreeing: 1) 15 = 6 pips; at risk_max 0.4 the strong
    one risks 40 (0.5 lots, 15 = 3 pips). The dry run says it. A take-profit under the closest stop allowed is held
    there, and logged."""
    for bid, look, ctx, params, said in (
            (1, "flat", None, {}, "6 stop-limit orders on EURUSD: buy-stop-limit 1.10025 limit 1.10020 / "
             "sell-stop-limit 1.09980 limit 1.09985, 3 × 0.25 lots per side, SL 8.0 pips, TP 4.0 pips (10.00 per "
             "order), confidence 0.00"),
            (2, "up", {"expect": 1, "ai": 1}, {}, "3 stop-limit orders on EURUSD: buy-stop-limit 1.10025 limit "
             "1.10020, 3 × 0.25 lots per side, SL 8.0 pips, TP 6.0 pips (15.00 per order), confidence 1.00"),
            (3, "up", {"expect": 1, "ai": 1}, {"risk_max": 0.4}, "3 stop-limit orders on EURUSD: buy-stop-limit "
             "1.10025 limit 1.10020, 3 × 0.5 lots per side, SL 8.0 pips, TP 3.0 pips (15.00 per order), confidence "
             "1.00"),
            (4, "flat", None, {"tp_min_usd": 1.0, "tp_max_usd": 1.0}, "6 stop-limit orders on EURUSD: buy-stop-limit "
             "1.10025 limit 1.10020 / sell-stop-limit 1.09980 limit 1.09985, 3 × 0.25 lots per side, SL 8.0 pips, TP "
             "1.5 pips (3.75 per order), confidence 0.00")):
        b = SimBroker("hedging")
        view(b, look)
        h = Harness(b, dry_run=True)
        with Caught() as lines:
            await h.c.on_burst(rapid(bid, ctx, **params))
            fin = await finished(h, bid)
        got = [m for m in h.logs() if m.startswith("[dry-run]")]
        check(fin["reason"] == "dry-run" and got == [f"[dry-run] Rapid run #{bid} would place {said}"]
              and not b.calls_of("place"), f"#{bid}: {got}")
        held = [s for s in lines if "held to" in s]
        check(held == (["Rapid run #4: take-profit of 1.00 held to 1.5 pips on 0.25 lots"] if bid == 4 else []),
              f"#{bid}: {held}")
        h.close()

    # unclear at 0.4 (the chart up, the forecast and the AI against it: under CLEAR, both sides): its confidence is
    # still |score|, both sides sized and their take-profits set as size_order gives them at 0.4
    b = SimBroker("hedging")
    view(b, "up")
    h = Harness(b)
    await h.c.on_burst(rapid(5, {"expect": -1, "ai": -1}))
    await armed(h, 5)
    st, places, got = h.c.bursts[5], b.calls_of("place"), h.analyses(5)
    o = size_order(st["p"], 0.4, 10_000.0, b.symbol_spec("EURUSD"), 0.5)
    tp = {"buy": 1.1002 + o["tp_pips"] * PIP, "sell": 1.09985 - o["tp_pips"] * PIP}
    check(len(got) == 1 and got[0]["action"] == "both" and got[0]["reason"] == "weak" and near(got[0]["score"], 0.4)
          and near(st["c"], 0.4) and near(st["tp_usd"], 10.0 + 0.4 * (15.0 - 10.0)) and near(o["tp_usd"], st["tp_usd"])
          and near(st["lots"], o["lots"]) and len(places) == 6 and [c[1] for c in places] == ["buy", "sell"] * 3
          and all(near(c[2], o["lots"]) and near(c[5], tp[c[1]]) for c in places),
          f"unclear at 0.4: both sides at its confidence: {got} c {st['c']} TP {st['tp_usd']} {o} {places[:2]}")
    await run_cancelled(h, 5)
    h.close()


async def rapid_filled(close_fails=0, frozen=0):
    """One Rapid buy filled at its limit 1.10020 (pre-release mid 1.100025; TP 1.10060, SL 1.09940) on a plan with
    hold X 30 s / Y 120 s; (harness, broker)."""
    b = SimBroker("hedging")
    b.fail_close, b.frozen_close = close_fails, frozen
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(1, orders=1, max_hold_seconds=120))
    await armed(h, 1)
    b.move(1.10025)                                      # the buy's trigger: a limit at 1.10020 now
    b.move(1.10015)                                      # back at it: filled there
    await until(lambda: "triggered" in h.statuses(1) and h.of("trade_opened"), "the buy filled")
    return h, b


@scenario
async def rapid_hold_until_profit_or_y():
    """Hold X / Y with Rapid's keys: in profit from X on; in a loss kept to Y; closed at once when the price is back
    through the pre-release mid (against); a close decided is kept; 10029 retried without a warning."""
    with Caught() as lines:
        h, b = await rapid_filled()
        b.move(1.10040)                                  # in profit, under the TP
        await jump(29)
        check(not b.calls_of("close"), "profit before X: kept")
        await jump(1.5)
        await finished(h, 1)
        check(any("(in profit after 30s)" in s for s in lines), f"profit: {lines[-3:]}")
        h.close()

        h, b = await rapid_filled()
        b.move(1.10010)                                  # a loss, still above the pre-release mid
        await jump(31)
        await asyncio.sleep(0.05)
        check(not b.calls_of("close"), "a loss after X: kept until Y")
        await jump(90)
        await finished(h, 1)
        check(any("(max hold 120s)" in s for s in lines), f"max: {lines[-3:]}")
        h.close()

        h, b = await rapid_filled()
        b.move(1.09995)                                  # mid 1.099975: back under the pre-release mid
        await until(lambda: b.calls_of("close"), "closed against the release")
        fin = await finished(h, 1)
        check(any("(turned against the release)" in s for s in lines) and fin["status"] == "closed",
              f"against: {lines[-3:]}")
        h.close()

        h, b = await rapid_filled(close_fails=1)
        b.move(1.10040)
        await jump(30.5)                                 # in profit: the close is refused once
        await until(lambda: b.calls_of("close"), "a close tried")
        b.move(1.10010)                                  # now a loss, before Y: the decision stands
        await jump(1)
        await finished(h, 1)
        check(len(b.calls_of("close")) == 2 and not b.pos, f"latched: {b.calls_of('close')}")
        h.close()

        h, b = await rapid_filled(frozen=2)
        b.move(1.10040)
        await jump(30.5)
        for _ in range(3):
            await jump(5)
        await finished(h, 1)
        check(len(b.calls_of("close")) == 3 and not any("could not close" in s for s in h.logs())
              and any("frozen, close retried" in s for s in lines), f"10029 quiet: {h.logs()}")
        h.close()


@scenario
async def rapid_unanswered_stop_limit_followed():
    """A stop-limit that went into the book with no answer is followed by its comment, with its limit as the book
    shows it, so the first fill cancels it (OCO)."""
    b = SimBroker("netting")
    view(b, "flat")
    place = b.place_pending

    def lost(*a, **kw):
        t = place(*a, **kw)
        if a[1] == "sell":
            raise RuntimeError("order_send failed: (-10005, 'IPC timeout')")
        return t
    b.place_pending = lost
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    st, sell = h.c.bursts[1], next(t for t, o in b.orders.items() if o["side"] == "sell")
    check(near(st["pending"][sell]["limit"], 1.09985) and st["pending"][sell]["triggered"] is False
          and h.updates(1)[-1]["orders"] == 2, f"followed: {st['pending']}")
    b.move(1.10025)
    b.move(1.10015)
    await until(lambda: ("cancel", sell, "ok") in b.calls, "the sell cancelled by the buy's fill")
    await jump(30.5)
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["side"] == "buy" and not b.pos and not b.orders, f"{fin}")
    h.close()


@scenario
async def rapid_gold_scaled():
    """On gold (scale 5) Rapid's pip settings are 5 pair pips: the trigger's offset (10 gold pips), the stop (40),
    the spread cap; the limit sits the stops level behind the trigger; the take-profit is its money on its lots (10
    on 0.05 lots = 20 gold pips)."""
    gold = SymbolSpec("XAUUSD", pip=0.1, pip_value_per_lot=10.0, digits=2, point=0.01, min_stop_distance=0.1,
                      scale=5.0)

    def broker(spread):
        b = SimBroker("hedging")
        b.bids["XAUUSD"], b.spread, b.stops = 2650.0, spread, 0.1
        spec, quote = b.symbol_spec, b.quote
        b.symbol_spec = lambda s: gold if s == "XAUUSD" else spec(s)
        b.quote = lambda s: (b.bids[s], b.ask(s), round(b.spread / gold.pip, 2)) if s == "XAUUSD" else quote(s)
        view(b, "flat")
        return b

    b = broker(0.2)                                      # 2 gold pips
    h = Harness(b)
    await h.c.on_burst({**rapid(1), "symbol": "XAUUSD"})
    await armed(h, 1)
    places = b.calls_of("place")
    check(len(places) == 6 and placed_as(places[0], "buy", 0.05, 2651.2, 2647.1, 2653.1, "fxb:1:0", 2651.1)
          and placed_as(places[1], "sell", 0.05, 2649.0, 2653.1, 2647.1, "fxb:1:0", 2649.1),
          f"offset 10, stop 40, TP 20 gold pips: {places[:2]}")
    await run_cancelled(h, 1)
    h.close()
    h = Harness(broker(0.2), dry_run=True)
    await h.c.on_burst({**rapid(2), "symbol": "XAUUSD"})
    await finished(h, 2)
    said = [m for m in h.logs() if m.startswith("[dry-run]")]
    check(said == ["[dry-run] Rapid run #2 would place 6 stop-limit orders on XAUUSD: buy-stop-limit 2651.20 limit "
                   "2651.10 / sell-stop-limit 2649.00 limit 2649.10, 3 × 0.05 lots per side, SL 40.0 pips, TP 20.0 "
                   "pips (10.00 per order), confidence 0.00"], f"dry-run: {said}")
    h.close()
    b = broker(1.6)                                      # 16 gold pips: over 3 x 5
    h = Harness(b)
    await h.c.on_burst({**rapid(3, max_spread_pips=3.0), "symbol": "XAUUSD"})
    fin = await finished(h, 3)
    check(fin["reason"] == "spread 16.0 pips > 15.0" and not b.calls_of("place"), f"spread cap x 5: {fin}")
    h.close()


@scenario
async def rapid_oco_cancels_a_triggered_opposite_order():
    """A whipsaw before the first fill: the first order of each side triggered (a limit now, type 2 / 3), then the
    buy fills. The OCO cancels every sell, the triggered one too (a limit left in the book would open the opposite
    position): the run is a buy, no sell ever opens."""
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    st = h.c.bursts[1]
    with b.lock:
        first = {}
        for o in sorted(b.orders.values(), key=lambda o: o["ticket"]):
            first.setdefault(o["side"], o)
        for o in first.values():                         # as move() turns a stop-limit whose trigger it crosses
            o.update(type=2 if o["side"] == "buy" else 3, kind="limit", price=o["price_stoplimit"], price_stoplimit=0.0)
        sells = sorted(t for t, o in b.orders.items() if o["side"] == "sell")
    await until(lambda: sum(r["triggered"] for r in st["pending"].values()) == 2, "both triggers seen")
    b.move(1.09983)                  # under the sell limit 1.09985: the buy limit fills at 1.10020, the sell one waits
    await until(lambda: h.of("trade_opened"), "the buy filled")
    await until(lambda: not any(o["side"] == "sell" for o in b.orders.values()), "the sells cancelled")
    check(st["side"] == "buy" and all(("cancel", t, "ok") in b.calls for t in sells) and len(sells) == 3
          and not any(p["side"] == "sell" for p in b.pos.values()), f"every sell cancelled: {b.calls_of('cancel')}")
    await jump(31)                                       # the expiry, then the fill's hold time
    fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["side"] == "buy" and fin["fills"] == 1 and not b.orders and not b.pos
          and all(m["trade"]["side"] == "buy" for m in h.of("trade_opened")), f"a buy only: {fin}")
    h.close()


@scenario
async def rapid_triggered_while_placing_is_no_fill():
    """A stop-limit that triggers while the run is still placing (a limit in the book now, not filled) is no fill:
    every order of both sides is placed, the run armed, nothing cancelled."""
    b = SimBroker("hedging")
    view(b, "flat")
    place = b.place_pending

    def first_triggers(*a, **kw):
        t = place(*a, **kw)
        if a[1] == "buy" and a[6].endswith(":0"):        # the first buy: its trigger reached right after it went in
            with b.lock:
                o = b.orders[t]
                o.update(type=2, kind="limit", price=o["price_stoplimit"], price_stoplimit=0.0)
        return t
    b.place_pending = first_triggers
    h = Harness(b)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    await asyncio.sleep(0.05)
    check(len(b.calls_of("place")) == 6 and not b.calls_of("cancel") and "triggered" not in h.statuses(1)
          and not any("filled while placing" in m for m in h.logs()) and h.updates(1)[-1]["orders"] == 6,
          f"all placed: {b.calls_of('place')} {h.logs()}")
    await run_cancelled(h, 1)
    h.close()


@scenario
async def rapid_partly_filled_then_expired_is_a_fill():
    """A triggered stop-limit filled in part (0.1 of 0.25 lots), the rest then expired by the broker, its deal and
    position not shown yet: its history says expired with volume left over (volume_current < volume_initial), which
    is no "left unfilled". The order is looked at again and its fill taken up once the position shows."""
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    with Caught() as lines:
        await h.c.on_burst(rapid(1, orders=1))
        await armed(h, 1)
        st = h.c.bursts[1]
        b.move(1.10025)                                  # the buy's trigger: a limit at 1.10020 now
        await until(lambda: all(r["triggered"] for r in st["pending"].values() if r["side"] == "buy"), "triggered")
        with b.lock:
            buy, sell = (next(t for t, o in b.orders.items() if o["side"] == s) for s in ("buy", "sell"))
            o = b.orders[buy]
            o["volume_current"] = 0.15                   # 0.1 of its 0.25 lots filled at its limit
            b._fill({**o, "lots": 0.1}, o["price"])
            b.hide(buy, 0.1)                             # its deal and position not shown yet
            b.unlist(buy, 0.1)
            b.expire(buy)                                # the rest expired by the broker
        await until(lambda: h.of("trade_opened") and ("cancel", sell, "ok") in b.calls, "the part filled taken up")
        opened = h.of("trade_opened")[0]["trade"]
        check(opened["ticket"] == buy and near(opened["lots"], 0.1) and opened["strategy"] == "rapid"
              and ("state", buy) in b.calls and not any(f"order #{buy} expired without a fill" in s for s in lines),
              f"a fill, not dropped: {opened} {[s for s in lines if 'without' in s]}")
        b.move(1.10060)                                  # its TP
        fin = await finished(h, 1)
    check(fin["status"] == "closed" and fin["side"] == "buy" and fin["fills"] == 1 and near(fin["pnl"], 4.0)
          and not b.pos and not b.orders, f"reported: {fin}")
    h.close()


@scenario
async def rapid_news_runs_past_the_long_term_daily_limit():
    """The long-term daily loss limit (risk.halted) stops the classic burst only: a Rapid news run has its own
    (rapid_room) and still places its stop-limits that day."""
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    h.c.risk.update_day(datetime.now(timezone.utc).date(), 20_000.0)     # today began at 20k: 10k is past its 3 %
    h.c.risk.halted = True
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    await h.c.on_burst({**plan(2), "symbol": "GBPUSD"})
    fin = await finished(h, 2)
    check(len(b.calls_of("place")) == 6 and all(len(c) == 8 for c in b.calls_of("place"))
          and fin["status"] == "skipped" and fin["reason"] == "daily loss limit (3.0%) reached",
          f"Rapid placed, the classic burst skipped: {fin} {len(b.calls_of('place'))}")
    await run_cancelled(h, 1)
    h.close()

    # not halted before: the limit is passed by the time the Rapid run plans. It does not check it (nor halt): the
    # classic burst after it is the first to see it
    b = SimBroker("hedging")
    view(b, "flat")
    h = Harness(b)
    h.c.risk.update_day(datetime.now(timezone.utc).date(), 20_000.0)
    await h.c.on_burst(rapid(1))
    await armed(h, 1)
    check(len(b.calls_of("place")) == 6 and not h.c.risk.halted, f"Rapid placed, not halted: {h.final(1)}")
    await h.c.on_burst({**plan(2), "symbol": "GBPUSD"})
    fin = await finished(h, 2)
    check(len(b.calls_of("place")) == 6 and fin["status"] == "skipped"
          and fin["reason"] == "daily loss limit (3.0%) reached" and h.c.risk.halted,
          f"the classic burst skipped, halted from then on: {fin}")
    await run_cancelled(h, 1)
    h.close()


@scenario
async def classic_burst_leaves_rapid_out_of_the_long_term_limit():
    """A Rapid loss over the long-term daily limit (400 lost, the limit 300 of 10k at 3 %), booked to Rapid's own
    day: the classic burst's long-term check counts the equity without it (as Connector.execute), so it places its
    stops and the long-term limit is not reached."""
    b = SimBroker("hedging")
    h = Harness(b)
    today = datetime.now(timezone.utc).date()
    h.c.risk.update_day(today, 10_000.0)                 # the day began at 10k
    h.c.rapid_day.roll(10_000.0)
    h.c.rapid_day.book(today.isoformat(), -400.0, 9001)
    b.balance -= 400.0                                   # what Rapid lost is out of the account
    await h.c.on_burst(plan(1))
    await armed(h, 1)
    check(len(b.calls_of("place")) == 10 and not h.c.risk.halted and near(h.c.rapid_pnl_today(), -400.0)
          and h.final(1) is None, f"placed: {len(b.calls_of('place'))} halted {h.c.risk.halted}")
    await run_cancelled(h, 1)
    h.close()


# ── runner ─────────────────────────────────────────────────────
async def main() -> int:
    Connector.BURST_POLL, Connector.BURST_RESULT_WAIT, Connector.CLOSE_PAUSE = 0.01, 0.01, 0.01
    failed = 0
    for fn in TESTS:
        t0 = time.monotonic()
        try:
            res = fn()
            if asyncio.iscoroutine(res):
                await res
            print(f"PASS {fn.__name__} ({time.monotonic() - t0:.2f}s)")
        except Exception as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
            traceback.print_exc()
    return failed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO if "-v" in sys.argv else logging.CRITICAL,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    picked = [fn for fn in TESTS if fn.__name__ in sys.argv[1:]]     # scenarios named on the command line, else all
    TESTS[:] = picked or TESTS
    try:
        with asyncio.Runner(loop_factory=SkewLoop) as runner:
            failures = runner.run(main())
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} scenarios passed")
    sys.exit(1 if failures else 0)
