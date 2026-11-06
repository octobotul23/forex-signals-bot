"""FX Signals connector: runs on the client's Windows PC next to MetaTrader 5.

It keeps one secure WebSocket open to the platform server, receives signals, and (depending on the
client's settings in the web app) places, manages and reports trades in the client's own MT5 account.
Broker login details never leave this PC.

    python connector.py                 # uses connector.ini in this folder
    python connector.py --dry-run       # log what it would do, place no orders
    python connector.py --paper         # no MT5 needed: simulated fills, for trying the platform out
    python connector.py --stop-on-stdin-eof   # (the Forex Bot app) stops cleanly once its stdin is closed

Safety: real-money accounts are refused unless `allow_real_account = yes` is set in connector.ini.
News burst: before a high-impact release the server sends a plan; the connector places a straddle of stop
orders (or, when its pre-news analysis is clear, one side of it), cancels the other side on the first fill and
closes each fill after its hold time (in profit from X seconds on, at Y at the latest).
News Rapid (a plan of kind "rapid", bot/rapid.py): the same runner with stop-limit orders, sized by the pre-news
analysis' confidence, each with its own stop and take-profit at the broker.
"""
import argparse
import asyncio
import configparser
import contextlib
import json
import logging
import math
import os
import re
import signal
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))            # bundled download: bot/ sits next to this file
sys.path.insert(0, str(HERE.parent))     # source checkout: forex-bot/bot

from websockets.asyncio.client import connect                     # noqa: E402
from websockets.exceptions import ConnectionClosed, InvalidStatus  # noqa: E402

from bot.hold import hold_action, min_profit, news_against     # noqa: E402
from bot.news_view import H1_BARS, M1_BARS, analyse, describe  # noqa: E402
from bot.news_view import decide as decide_view                # noqa: E402
from bot.rapid import (RUN_KIND, clamp_rapid, confidence_from_score, confidence_from_stretch, fit_orders,  # noqa: E402
                       local_comment, size_order, stop_limit_prices)
from bot.rapid import split_volume as rapid_split               # noqa: E402
from bot.risk import RiskManager, burst_lots, money_for, money_stop, stop_floor_pips    # noqa: E402
from bot.scalp import (KEEP_SECONDS, SecondBars, clamp_short, closes_soon, daily_limit_money,  # noqa: E402
                       decide, lots_for_money, risk_money, scalp_against, stretch)
from bot.scalp import RULE as SCALP_RULE                        # noqa: E402
from bot.symbols import SymbolSpec, pip_size, symbol_scale     # noqa: E402

VERSION = "1.3.0"
MAGIC = 20261020
TEST_RISK_PCT = 0.1                         # test signals (auto-trading check) risk at most this % of balance
log = logging.getLogger("connector")
FX_TAG = re.compile(r"fx:(\d+)")
# a run's orders/positions: fxb:<burst id>:<slot> (news burst), fxr:<run id>:<i> (Rapid), fxs:<scalp id> (short-term)
RUN_TAG = re.compile(r"fx([brs]):(\d+)")
RUN_KINDS = {"b": "burst", "r": "rapid", "s": "scalp"}
# burst params: (min, max, type, default) = the server's clamps, repeated here because a burst places real orders.
# A key a plan lacks (one from a server before v1.3.0) gives the run v1.2.0 made: max_hold_seconds (Y) follows
# hold_seconds (X; its default None), the stop in pips, no pre-news analysis
BURST_LIMITS = {"trades": (1, 40, int, 10), "lead_seconds": (10, 600, int, 60), "offset_pips": (1, 100, float, 7.0),
                "sl_pips": (2, 200, float, 10.0), "tp_start_pips": (1, 500, float, 4.0),
                "tp_step_pips": (0, 100, float, 1.5), "hold_seconds": (5, 600, int, 30),
                "expire_seconds": (10, 1800, int, 90), "risk_pct": (0.1, 5.0, float, 1.0),
                "max_spread_pips": (0.2, 20.0, float, 3.0), "lots": (0.01, 100.0, float, 0.10),
                "max_hold_seconds": (5, 1800, int, None)}
BURST_UNITS = ("pips", "pct", "usd")        # sl_unit: the stop in pips, or the money it may lose (% of balance, $)
BURST_MONEY = {"pct": (0.1, 10.0, 2.0), "usd": (1.0, 100_000.0, 100.0)}   # sl_money by its unit: (min, max, default)
KEEP_OFFLINE = ("trade_opened", "trade_closed", "burst_update")   # sent after a reconnect if the link was down


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_iso(v: str | None) -> datetime | None:
    return datetime.fromisoformat(v.replace("Z", "")) if v else None


def run_tag(comment: str | None) -> tuple[str, int] | None:
    """The run an order or position belongs to, by its comment: ("burst" | "rapid" | "scalp", its id). None for any
    other: a long-term signal's (fx:<id>), one opened by hand or by another EA."""
    m = RUN_TAG.match(comment or "")
    return (RUN_KINDS[m.group(1)], int(m.group(2))) if m else None


def clamp_burst(params: dict) -> dict:
    """A plan's params clamped as the server clamps them; a number missing or unreadable is its default. Y
    (max_hold_seconds) under X is raised to X, never swapped. sl_money follows its unit's range (missing: the unit's
    default; in pips it is not used). Raises ValueError("invalid_unit" | "invalid_analysis") for a stop unit or an
    analysis switch it cannot read: a burst never trades a guess at those ("false" must never switch it on)."""
    params = params if isinstance(params, dict) else {}
    out = {}
    for k, (lo, hi, cast, default) in BURST_LIMITS.items():
        try:
            v = float(params[k])
        except (KeyError, TypeError, ValueError, OverflowError):
            v = math.nan
        out[k] = cast(min(max(v, lo), hi)) if math.isfinite(v) else default
    out["max_hold_seconds"] = max(out["hold_seconds"], out["max_hold_seconds"] or 0)
    unit, analysis = params.get("sl_unit", "pips"), params.get("analysis", False)
    if unit not in BURST_UNITS:
        raise ValueError("invalid_unit")
    if not isinstance(analysis, bool):
        raise ValueError("invalid_analysis")
    lo, hi, default = BURST_MONEY.get(unit, BURST_MONEY["pct"])
    try:
        v = float(params["sl_money"])
    except (KeyError, TypeError, ValueError, OverflowError):
        v = math.nan
    out.update(sl_unit=unit, sl_money=min(max(v, lo), hi) if math.isfinite(v) else default, analysis=analysis)
    return out


def split_volume(total: float, spec: SymbolSpec) -> list[float]:
    """Netting burst: one order per side, split evenly into chunks of at most volume_max."""
    steps = round(total / spec.volume_step)
    parts = -(-steps // max(1, math.floor(spec.volume_max / spec.volume_step + 1e-9)))
    return [round((steps // parts + (i < steps % parts)) * spec.volume_step, 8) for i in range(parts)]


def position_net(deals: list[dict]) -> dict:
    """What a position's own deals (oldest first) add up to: the side and lots it holds, what entered its current
    side since it last started over (its side and average price also once flat again), how often it started over
    (reversed, or opened again once flat), the side, lots and average price of what entered it first, the orders
    its deals came from, what each order took off its current side (`cut`) and which of those orders were not this
    bot's (`foreign`: a close by hand, another EA's). Every deal counts by its direction only, so a netting reversal
    adds up the same whether MT5 books it as one in/out deal or as an out and an in."""
    net = entered = cost = 0.0
    flips, first, last, cut, foreign = 0, None, None, {}, set()   # first: (entered, cost) of the first entry, once over
    for i, d in enumerate(deals):
        lots, buy, px = d["lots"], d["side"] == "buy", d.get("price") or 0.0
        if abs(net) < 1e-9 and i:                                    # flat, then opened again: a new entry
            flips, first, cut, foreign = flips + 1, first or (entered, cost), {}, set()
            entered, cost, last = lots, lots * px, d["side"]
        elif abs(net) < 1e-9 or (net > 0) == buy:                    # opens or adds to it
            entered, cost, last = round(entered + lots, 8), cost + lots * px, d["side"]
        elif lots > abs(net) + 1e-9:                                 # reverses it: what is left over entered
            flips, first, cut, foreign = flips + 1, first or (entered, cost), {}, set()
            entered = round(lots - abs(net), 8)
            cost, last = entered * px, d["side"]
        else:                                                        # reduces or closes it
            cut[d.get("order")] = round(cut.get(d.get("order"), 0.0) + lots, 8)
            if d.get("mine") is False:
                foreign.add(d.get("order"))
        net = round(net + (lots if buy else -lots), 8)
    first_entered, first_cost = first or (entered, cost)
    return {"side": "buy" if net > 1e-9 else "sell" if net < -1e-9 else None, "lots": abs(net), "entered": entered,
            "flips": flips, "first_side": deals[0]["side"] if deals else None, "first_entered": first_entered,
            "first_entry": round(first_cost / first_entered, 8) if first_entered and first_cost else None,
            "last_side": last, "entry": round(cost / entered, 8) if entered and cost else None,
            "orders": {d.get("order") for d in deals} - {None, 0}, "cut": cut, "foreign": foreign}


# ── paper broker (no MT5) ──────────────────────────────────────
class PaperBroker:
    """Simulated account: fills at the signal's reference price, closes when the server closes the signal."""
    def __init__(self, balance: float = 10_000.0):
        self.balance, self.positions_, self.next_ticket, self.prices = balance, {}, 1000, {}

    def connect(self):
        log.info("Paper mode: simulated account with %.2f USD", self.balance)

    def account(self):
        profit = sum(p["profit"] for p in self.positions_.values())
        return {"login": 0, "server": "paper", "currency": "USD", "balance": round(self.balance, 2),
                "equity": round(self.balance + profit, 2), "free_margin": round(self.balance, 2), "leverage": 100,
                "demo": True, "trade_allowed": True}

    def terminal_trading_enabled(self):
        return True

    def symbol_spec(self, symbol):
        if "XAU" in symbol.upper():         # gold: quoted with 2 digits, its pip 0.1 (10 per lot of 100 oz)
            return SymbolSpec(symbol=symbol, pip=pip_size(symbol), pip_value_per_lot=10.0, digits=2, point=0.01,
                              scale=symbol_scale(symbol))
        jpy = "JPY" in symbol
        return SymbolSpec(symbol=symbol, pip=pip_size(symbol), pip_value_per_lot=6.7 if jpy else 10.0,
                          digits=3 if jpy else 5, point=0.001 if jpy else 0.00001, scale=symbol_scale(symbol))

    def quote(self, symbol):
        p = self.prices.get(symbol)
        return (p, p, 0.0) if p else (None, None, 0.0)

    def rates(self, symbol, timeframe, count):
        """No price history here: what reads it does without (as when MT5 fails to answer)."""
        from bot.mt5_broker import BrokerError
        raise BrokerError("no history in paper mode")

    def positions(self, all_magic=False):
        return list(self.positions_.values())

    def open_market(self, symbol, side, lots, sl, tp, comment=""):
        price = self.prices[symbol]
        self.next_ticket += 1
        self.positions_[self.next_ticket] = {"ticket": self.next_ticket, "symbol": symbol, "side": side, "lots": lots,
                                             "entry_price": price, "sl": sl, "tp": tp, "price": price, "profit": 0.0,
                                             "open_time": utcnow().isoformat(), "comment": comment}
        return {"ticket": self.next_ticket, "price": price, "lots": lots}

    def close_position(self, pos, comment="", price=None):
        p = self.positions_.pop(pos["ticket"])
        exit_price = price or self.prices.get(p["symbol"], p["entry_price"])
        sign = 1 if p["side"] == "buy" else -1
        spec = self.symbol_spec(p["symbol"])
        p["pnl"] = round((exit_price - p["entry_price"]) * sign / spec.pip * spec.pip_value_per_lot * p["lots"], 2)
        p["exit_price"] = exit_price
        self.balance += p["pnl"]
        self.closed = getattr(self, "closed", {})
        self.closed[p["ticket"]] = p

    def closed_result(self, ticket):
        p = getattr(self, "closed", {}).get(ticket)
        return {"exit_price": p["exit_price"], "exit_time": utcnow().isoformat(), "pnl": p["pnl"]} if p else None

    # pending orders are not simulated: news bursts and Rapid runs are skipped in paper mode before they place one, so
    # an order that still comes here (a stop or a stop-limit) is refused with the reason, never filled on a guess
    def place_pending(self, symbol, side, lots, price, sl, tp, comment="", expire_in=None, kind="stop",
                      limit_price=None):
        from bot.mt5_broker import BrokerError
        raise BrokerError("paper mode: pending orders need MetaTrader 5")

    def pending_orders(self, symbol=None):
        return []

    def order_state(self, ticket):
        return None

    def pending_room(self):
        return None

    def stop_limit_allowed(self, symbol):
        return False


# ── MT5 adapter ────────────────────────────────────────────────
class MT5Adapter:
    def __init__(self, magic: int):
        from bot.mt5_broker import MT5Broker
        self.b = MT5Broker(magic=magic, deviation=20)

    def connect(self):
        self.b.connect()

    def account(self):
        return self.b.account()

    def terminal_trading_enabled(self):
        return self.b.terminal_trading_enabled()

    def symbol_spec(self, symbol):
        return self.b.symbol_spec(symbol)

    def quote(self, symbol):
        t = self.b.tick(symbol)
        spec = self.b.symbol_spec(symbol)
        return t.bid, t.ask, (t.ask - t.bid) / spec.pip

    def rates(self, symbol, timeframe, count):
        return self.b.rates(symbol, timeframe, count)

    def ticks_since(self, symbol, after_msc, count=5000, back_s=60):
        return self.b.ticks_since(symbol, after_msc, count, back_s)

    def margin_for(self, symbol, side, lots):
        return self.b.margin_for(symbol, side, lots)

    def positions(self, all_magic=False):
        return self.b.positions(all_magic)

    def open_market(self, symbol, side, lots, sl, tp, comment=""):
        return self.b.open_market(symbol, side, lots, sl, tp, comment)

    def close_position(self, pos, comment="", price=None, volume=None):
        """The tickets of the orders whose deals closed something (also on the error, when one is raised)."""
        orders = []
        try:
            self.b.close_position(pos, comment or "fx exit", volume=volume, orders=orders)
        except Exception as e:
            e.orders = orders
            raise
        return orders

    def closed_result(self, ticket):
        return self.b.closed_result(ticket)

    def place_pending(self, symbol, side, lots, price, sl, tp, comment="", expire_in=None, kind="stop",
                      limit_price=None):
        """kind "stop_limit": `price` is the trigger, `limit_price` the limit (MT5Broker.place_pending)."""
        return self.b.place_pending(symbol, side, lots, price, sl, tp, comment, expire_in, kind, limit_price)

    def pending_orders(self, symbol=None):
        return self.b.pending_orders(symbol)

    def order_state(self, ticket):
        return self.b.order_state(ticket)

    def pending_room(self):
        return self.b.pending_room()

    def stop_limit_allowed(self, symbol):
        return self.b.stop_limit_allowed(symbol)

    def cancel_order(self, ticket):
        return self.b.cancel_order(ticket)

    def deal_entry(self, ticket):
        return self.b.deal_entry(ticket)

    def position_deals(self, position):
        return self.b.position_deals(position)


# ── connector ──────────────────────────────────────────────────
class Connector:
    def __init__(self, server: str, token: str, broker, allow_real: bool, dry_run: bool, suffix: str = ""):
        base = server.rstrip("/")
        self.url = ("wss://" + base[8:] if base.startswith("https://") else "ws://" + base.removeprefix("http://")) \
            + "/ws/connector"
        self.token, self.broker, self.allow_real, self.dry_run, self.suffix = token, broker, allow_real, dry_run, suffix
        self.paper = isinstance(broker, PaperBroker)
        self.mt5 = ThreadPoolExecutor(max_workers=1)      # the MT5 package is not thread-safe: one thread only
        self.ws = None
        self.settings: dict = {}
        self.risk: RiskManager | None = None
        self.known: dict[int, dict] = {}                   # ticket -> position (to detect closes)
        self.signal_of: dict[int, int] = self._load_map()   # ticket -> signal id
        self.handled: set[int] = set()                     # signal ids already acted on this session
        self.lock = asyncio.Lock()
        self.bursts: dict[int, dict] = {}                  # burst id -> runner state (waiting or running)
        self.burst_done: set[int] = set()                  # finished this session: repeated plans are ignored
        self.burst_owned: dict[int, int] = {}              # order/position ticket -> burst id, owned by a runner
        self.burst_tasks: set[asyncio.Task] = set()        # a burst's side work (bars, the watch-only analysis)
        self.outbox: list[dict] = self._load_outbox()      # results produced while the link was down
        self.closing: dict[int, dict] = {}                 # ticket -> a close that failed, retried by close_loop
        self.unreported: dict[int, tuple | None] = {}      # burst positions whose close could not be checked -> (burst, trade)
        self.unreported_waits: dict[int, int] = {}         # ... and the sweeps each waited for its history
        self.orphan_info: dict[int, dict] = {}             # ... with no runner's trade: what its trade_closed carries
        self.unchecked: dict[int, list] = {}               # orders of a crashed burst that left the book -> [burst,
                                                           # order, sweeps]: a fill of theirs not taken up yet
        self.demo = True                                   # the account's, read at start
        self.sweeper = self.closer = None
        self.stopping = False                              # shutdown began: no new trades or bursts
        self.sweep_lock = asyncio.Lock()
        self.rapid = clamp_rapid(None)                     # the user's Rapid block (apply_settings)
        self.scalper = Scalper(self)                       # the short-term strategy
        # Rapid's own day (rapid_day.json beside scalp_day.json) against its own daily loss limit, and its local runs
        # (on a short-term signal) kept across restarts (rapid_open.json): those the last run left are taken up at start;
        # with them the ids of its news runs that sent orders, so what those leave is Rapid's after a restart too
        self.rapid_day = DayBook(self._rapid_file(self.scalper._day_file()),
                                 lambda start: daily_limit_money(self.rapid, start), tickets=True)
        self.rapid_written: dict = {}                      # what rapid_open.json holds (written only when it changes)
        self.rapid_news: set[int] = set()                  # Rapid news runs that sent orders, until nothing is left
        self.rapid_left: dict[int, dict] = {} if self.paper else self._load_rapid_open()
        self.rapid_cool: dict[str, float] = {}             # symbol -> when a local run may start there again
        # ticket -> booked to Rapid's day (False: a guess so far): those of the day kept too, so a trade the last run
        # booked just before it died is not booked again
        self.rapid_booked: dict[int, bool] = {} if self.paper else self.rapid_day.booked_tickets()
        self.rapid_booking: dict[int, dict] = {}           # ticket -> Rapid trade gone and not booked yet ({"id": its
                                                           # run, "profit": what it showed last}; _rapid_owed)
        self.rapid_last = 0                                # the last local run's id (its start in ms)

    # persistence of ticket -> signal (brokers may truncate order comments)
    def _map_file(self) -> Path:
        return HERE / ("paper_tickets.json" if self.paper else "tickets.json")

    def _load_map(self) -> dict[int, int]:
        try:
            return {int(k): int(v) for k, v in json.loads(self._map_file().read_text()).items()}
        except (OSError, ValueError):
            return {}

    def _save_map(self):
        path = self._map_file()
        tmp = path.with_name(path.name + ".tmp")
        try:
            tmp.write_text(json.dumps(self.signal_of))
            os.replace(tmp, path)                        # whole or not at all, even if the process is killed mid-write
        except OSError:
            pass

    # the outbox, kept across restarts: a result queued while the link was down (a scalp the last run closed, a
    # session the server ended) still reaches the server after the next start. Resending one is harmless: the server
    # books trades by ticket. Not in paper mode (a simulation: its trades are gone with it).
    def _outbox_file(self) -> Path:
        return HERE / "outbox.json"

    def _load_outbox(self) -> list[dict]:
        if self.paper:
            return []
        try:
            got = [m for m in json.loads(self._outbox_file().read_text()) if isinstance(m, dict)]
        except (OSError, ValueError, TypeError):
            return []
        if got:
            log.warning("%s result(s) the last run could not send: sent once connected", len(got))
        return got

    def _save_outbox(self):
        if not self.paper:
            Scalper._write(self._outbox_file(), self.outbox)

    # Rapid's files, beside short-term's (scalp_day.json -> rapid_day.json, scalp_open.json -> rapid_open.json).
    # rapid_open.json: the local runs open now and those whose results are not all reported yet, so a run cut short
    # (a crash, a restart) is still cleaned up, reported and booked after the next start (_rapid_adopt); and, as
    # {"news": true}, each Rapid news run that sent orders until nothing of it is left (rapid_news), so its fxb:
    # leftovers are Rapid's, not the classic burst's, after the next start too. Not in paper mode (no run there)
    @staticmethod
    def _rapid_file(path: Path) -> Path:
        name = path.name
        return path.with_name("rapid" + name[5:] if name.startswith("scalp") else "rapid-" + name)

    def _load_rapid_open(self) -> dict[int, dict]:
        try:
            raw = json.loads(self._rapid_file(self.scalper._open_file()).read_text())
            news = {int(rid) for rid, r in raw.items() if r.get("news") is True}
            got = {int(rid): {"symbol": str(r["symbol"]), "side": str(r["side"]), "demo": bool(r.get("demo", True)),
                              "digits": int(r.get("digits") or 5), "pip": float(r.get("pip") or 0.0),
                              "orders": {int(t): dict(o) for t, o in (r.get("orders") or {}).items()},
                              "trades": {int(t): dict(x) for t, x in (r.get("trades") or {}).items()}}
                   for rid, r in raw.items() if r.get("news") is not True}
        except OSError:             # none: nothing was left
            return {}
        except (ValueError, TypeError, KeyError, AttributeError):   # gone bad: written over at the next save
            self.rapid_written = None
            return {}
        self.rapid_written, self.rapid_news = raw, news
        if got:
            log.warning("Rapid: %s run(s) left by the last run: their orders are removed and their positions closed "
                        "now", len(got))
        if news:
            log.info("Rapid: what news run(s) %s may have left is booked to Rapid's day", sorted(news))
        return got

    def _save_rapid_open(self):
        """rapid_open.json: each local Rapid run open now, or handed to the sweeper and not all reported yet, and the
        Rapid news runs of rapid_news. Written only when it changed: never for a user without Rapid runs."""
        if self.paper:
            return
        runs = {st["id"]: st for st in self.bursts.values() if st.get("local")}
        for st in [c[0] for c in self.unreported.values() if c] + [r[0] for r in self.unchecked.values()]:
            if st.get("local"):
                runs.setdefault(st["id"], st)
        data = {str(rid): self._rapid_record(st) for rid, st in runs.items()}
        data.update({str(bid): {"news": True} for bid in sorted(self.rapid_news)})
        if data != self.rapid_written:
            Scalper._write(self._rapid_file(self.scalper._open_file()), data)
            self.rapid_written = data

    def _rapid_record(self, st: dict) -> dict:
        """What rapid_open.json keeps of local run `st`: its symbol, side and account, its symbol's digits and pip, its
        orders that may still fill (pending, or out of the book unexplained) and its trades not reported closed."""
        orders = {**{t: rec for t, (rec, _) in st["unknown"].items()}, **st["pending"],
                  **{t: r[1] for t, r in self.unchecked.items() if r[0] is st}}
        trades = {pid: tr for pid, tr in st["trades"].items()
                  if not tr["closed"] or (self.unreported.get(pid) or (None,))[0] is st}
        spec = st["spec"]
        return {"symbol": st["symbol"], "side": st["sides"][0], "demo": st["demo"],
                "digits": spec.digits if spec else 5, "pip": spec.pip if spec else 0.0,
                "orders": {str(t): {k: o.get(k) for k in ("side", "lots", "sl", "tp")} for t, o in orders.items()},
                "trades": {str(pid): {"side": tr["side"], "lots": tr["lots"], "entry": tr.get("entry"),
                                      "opened_at": tr["opened_at"]} for pid, tr in trades.items()}}

    async def _rapid_adopt(self):
        """A start: the local Rapid runs the last run left (rapid_open.json) are handed to the sweeper as runs whose
        runner is gone, before its first sweep (run). That sweep removes their orders and closes their positions at
        once (their fxr: comments say whose they are), and each trade of theirs is reported, and booked to Rapid's
        day, once its history shows it closed: also one closed at its stop-loss or take-profit while the connector
        was away, and the fill of an order that left the book meanwhile (_burst_take_up). An order whose history
        shows it left unfilled is dropped at once, and a trade booked by its result already (the last run died
        before rapid_open.json followed) is not reported again."""
        if not self.rapid_left:
            return
        try:
            book = {o["ticket"] for o in await self.call(self.broker.pending_orders)}
        except Exception as e:      # every order is looked up in history by the sweeps instead
            log.warning("Rapid: could not read the pending orders (%s)", e)
            book = set()
        for rid, rec in self.rapid_left.items():
            st = self._local_run(rid, rec["symbol"], rec["side"], 0.0, None)
            st.update(status="closed", demo=rec["demo"],
                      spec=SymbolSpec(rec["symbol"], rec["pip"] or pip_size(rec["symbol"]), 0.0, rec["digits"]))
            for pid, t in rec["trades"].items():
                st["trades"][pid] = tr = self._new_trade(0.0, t["side"], float(t["lots"]), None)
                tr.update(closed=True, entry=t.get("entry"), sl=None, tp=None,
                          opened_at=t.get("opened_at") or tr["opened_at"])
                if self.rapid_booked.get(pid) is not True:      # a guess: reported again, its result replaces it
                    self.unreported[pid] = (st, tr)
                    self._rapid_owed(pid, rid)
            for t, o in rec["orders"].items():
                if t not in book and not await self._never_filled(st, t):    # one in the book: the sweep removes it
                    self.unchecked[t] = [st, o, 0]
        self.rapid_left = {}

    async def call(self, fn, *args, **kwargs):
        return await asyncio.get_running_loop().run_in_executor(self.mt5, lambda: fn(*args, **kwargs))

    async def send(self, msg: dict, keep: bool = False):
        if self.ws is not None:
            try:
                return await self.ws.send(json.dumps(msg, default=str))
            except ConnectionClosed:
                pass
        if keep or msg.get("type") in KEEP_OFFLINE:    # runners keep trading across a reconnect: keep their results
            self.outbox = self.outbox[-499:] + [msg]
            self._save_outbox()

    async def flush_outbox(self, ws):
        """After `welcome`: results queued while the link was down go first and in order (a trade_closed must
        not overtake its trade_opened); only then may live sends use this link."""
        sent = 0
        try:
            while self.outbox:
                m = self.outbox[0]
                await ws.send(json.dumps(m, default=str))
                # by identity: a send() meanwhile may have trimmed the list (its 500 cap), dropping this one already
                self.outbox = [x for x in self.outbox if x is not m]
                sent += 1
        finally:
            if sent:
                self._save_outbox()
        self.ws = ws

    async def report(self, message: str, level: str = "info", keep: bool = False):
        """`keep`: sent after a reconnect if the link is down (what the user must act on)."""
        getattr(log, "warning" if level == "warn" else level)(message)
        await self.send({"type": "log", "level": level, "message": message}, keep)

    def apply_settings(self, s: dict):
        self.settings = s
        self.risk_cfg = {k: s.get(k) for k in ("risk_per_trade_pct", "max_open_trades", "daily_loss_limit_pct",
                                               "max_spread_pips")}
        old = self.risk
        self.risk = RiskManager(self.risk_cfg)
        if old:                                          # keep today's starting equity / halt state
            self.risk.day, self.risk.day_start_equity, self.risk.halted = old.day, old.day_start_equity, old.halted
        log.info("Settings: execution=%s modes=%s risk=%s%%", s.get("execution"),
                 [m for m, on in s.get("modes", {}).items() if on], s.get("risk_per_trade_pct"))
        self.scalper.apply(s)
        self.apply_rapid(s.get("rapid"))

    def apply_rapid(self, body):
        """The user's Rapid block (bot/rapid.py clamp_rapid; none, from a server before v1.3.0: off). A value gone bad
        where it is stored is its default, as for short-term ("false" never switches it on)."""
        try:
            cfg = clamp_rapid(body)
        except ValueError as e:
            log.warning("Rapid settings: %s (the bad value is left out)", e)
            cfg = clamp_rapid(None, body)
        old, self.rapid = self.rapid, cfg
        if cfg == old or not (cfg["enabled"] or old["enabled"]):
            return
        unit = "%" if cfg["risk_unit"] == "pct" else " (account currency)"
        log.info("Rapid: %s, %s orders, risk %s-%s%s per order, signal runs %s", "ON" if cfg["enabled"] else "off",
                 cfg["orders"], cfg["risk_min"], cfg["risk_max"], unit,
                 "on (demo accounts only)" if cfg["triggers"]["signals"] else "off")

    def wants(self, sig: dict) -> bool:
        s = self.settings
        if not s.get("modes", {}).get(sig["mode"]):
            return False
        if s.get("symbols") and sig["symbol"] not in s["symbols"]:
            return False
        if sig["mode"] == "news_ai" and (sig.get("confidence") or 0) < s.get("min_confidence", 0):
            return False
        return True

    # ── main loop with reconnect ───────────────────────────────
    async def run(self):
        await self.call(self.broker.connect)
        acct = await self.call(self.broker.account)
        if not acct["demo"] and not self.allow_real:
            log.error("This is a REAL-MONEY account (%s). The connector only trades demo accounts unless you set "
                      "allow_real_account = yes in connector.ini. Exiting.", acct["login"])
            return
        self.demo = bool(acct["demo"])
        if not self.dry_run and not await self.call(self.broker.terminal_trading_enabled):
            log.warning("'Algo Trading' is OFF in MetaTrader 5: orders will be rejected until you switch it on.")
        if not self.paper:
            try:
                await self._rapid_adopt()       # the local Rapid runs the last run left: cleaned up by the sweep below
            except Exception as e:
                log.warning("Rapid: taking up what the last run left failed: %s", e)
            try:
                await self.sweep_orphans()
            except Exception as e:
                log.warning("News-burst clean-up failed: %s", e)
            self.sweeper = asyncio.create_task(self.sweep_loop())
        self.closer = asyncio.create_task(self.close_loop())      # closing needs MT5, not the server
        self.scalper.start()                                      # so does closing a scalp in time
        # its first look closes what the last run left open (and reports what closed meanwhile) before the server is
        # asked in: a session the server ends at once (a revoked token) must not leave those to their stop-loss
        await self.scalper.first_look()
        try:
            for p in await self.call(self.broker.positions):
                self.known[p["ticket"]] = p
        except Exception as e:      # the first snapshot fills it
            log.warning("Could not read the open positions: %s", e)
        delay = 3
        while True:
            try:
                async with connect(self.url, open_timeout=15, ping_interval=20, ping_timeout=20,
                                   max_size=2**20) as ws:
                    acct = await self.call(self.broker.account)
                    await ws.send(json.dumps({"type": "auth", "token": self.token, "version": VERSION,
                                              "account": acct, "paper": self.paper}))
                    delay = 3                   # self.ws is set on welcome, after the outbox (see flush_outbox)
                    if await self.session(ws):
                        break
            except (OSError, ConnectionClosed, InvalidStatus, asyncio.TimeoutError) as e:
                log.warning("Connection lost (%s). Reconnecting in %ss...", type(e).__name__, delay)
            finally:
                self.ws = None
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
        await self._end_scalps()

    SCALP_STOP_WAIT = 15.0      # the server ended the session for good: how long closing the scalps may take (s)

    async def _end_scalps(self):
        """The server ended the session for good, and the process ends with run(): the scalps are closed first
        (nothing would close them at their hold time any more); one still open after that is reported."""
        if self.paper:
            return
        try:
            await asyncio.wait_for(self.scalper.stop(), self.SCALP_STOP_WAIT)
        except Exception as e:      # MT5 busy or gone, or the time is up
            log.warning("Closing the short-term trades did not finish (%s)", str(e) or type(e).__name__)
        await self._scalps_left()

    async def _scalps_left(self, skip=()):
        """After the scalper's stop: each scalp still open (its close failed, or the time ran out first) is reported,
        but those in `skip` (reported as a close that kept failing)."""
        if self.dry_run:
            return
        for t, s in list(self.scalper.open.items()):
            if t not in skip:
                await self.report(f"Could not close #{t}: the connector stopped before closing this short-term trade "
                                  f"({s['info'].get('symbol')}). Close it in MetaTrader 5 (its stop-loss still "
                                  "applies).", "error", keep=True)

    STOP_WAIT = 40.0            # a stop: how long the bursts' clean-up and what follows it may take in all (s); the
    #                             Forex Bot app waits longer than this before it ends the connector (CONNECTOR_GRACE)
    STOP_SWEEP_PAUSE = 0.5      # a stop: between the sweeps of what the bursts left

    async def shutdown(self, main: asyncio.Task, budget: float | None = None):
        """A clean stop, as Ctrl+C gives. New signals and bursts are refused; running news bursts are cancelled
        first, so each one removes its orders, closes its positions and reports that while the link is still up;
        what they could not remove is swept until nothing is left (what is left when the time is up is reported);
        a close asked for that kept failing is tried once more; a trade being placed is let finish; then `main`
        (run) is cancelled. A second call does nothing."""
        if self.stopping:
            return
        self.stopping = True
        loop = asyncio.get_running_loop()
        end = loop.time() + (self.STOP_WAIT if budget is None else budget)

        async def step(what, coro, until):
            try:
                return await asyncio.wait_for(coro, max(0.1, until - loop.time()))
            except Exception as e:      # MT5 busy or gone, or the time is up
                log.warning("Stopping: %s did not finish (%s)", what, str(e) or type(e).__name__)

        try:
            await asyncio.sleep(0)        # a burst task created just now starts: one cancelled before its first step
            #                               never runs its finally, so it would neither clean up nor report
            live = [st for st in list(self.bursts.values()) if st.get("task") and not st["task"].done()]
            runners = [st["task"] for st in live]
            placed = any(st["status"] != "scheduled" for st in live)     # past waiting: it may leave something
            log.info("Stopping%s...", f": ending {len(runners)} news burst(s) first" if runners else "")
            for t in runners:
                t.cancel()
            # the scalps are closed meanwhile, not after the bursts' clean-up: that may take all the time there is
            scalps = None if self.paper else asyncio.ensure_future(self.scalper.stop())
            if runners:
                await asyncio.wait(runners, timeout=max(0.1, end - 8 - loop.time()))
            if not (self.paper or self.dry_run):
                await self._sweep_at_stop(end - 3, placed)
            if scalps:
                await step("closing the short-term trades", scalps, end - 2.5)
            if self.closing:
                for c in self.closing.values():         # closes asked for that kept failing: one last try each
                    c["at"] = 0
                await step("retrying the closes that failed", self.retry_closes(), end - 2)
                for ticket, c in list(self.closing.items()):
                    await self.report(f"Could not close #{ticket}: the connector stopped ({c['why']}). Close it in "
                                      "MetaTrader 5 (its stop-loss and take-profit still apply).", "error", keep=True)
            await self._scalps_left(skip=self.closing)   # a scalp whose close the time cut short
            # a trade being placed is let finish and report (no new one starts now); the lock is kept from here on
            if await step("waiting for the trade being placed", self.lock.acquire(), end - 1):
                await step("the last snapshot", self.snapshot(), end)
        finally:
            main.cancel()

    async def _sweep_at_stop(self, until: float, ended: bool = True):
        """A stop: what the cancelled bursts left (an order not cancelled, a position not closed, a result not
        reported: their runners leave these to the sweeper) is swept every STOP_SWEEP_PAUSE until nothing is left.
        What is still left at `until` is reported (the next start sweeps it too). `ended`: this stop ended bursts
        past their waiting phase (what they left may take a few sweeps to show); with none ended and nothing found
        or waiting to be reported, one sweep is all: one that cannot run (MT5 not answering) is only logged and left
        to the next start, one the time cut (it may have sent a close) is reported."""
        loop = asyncio.get_running_loop()
        left = running = why = None
        cut = False                     # a sweep was under way when the time was up: it may have sent a close
        while True:
            running = sorted(st["id"] for st in self.bursts.values() if st["status"] != "scheduled")
            if not running:             # their own clean-up first: the sweeper leaves a running burst's tickets alone
                try:
                    left = await asyncio.wait_for(self.sweep_orphans(), max(0.1, until - loop.time()))
                except TimeoutError:    # a sweep still under way when the time was up (MT5 slow, or stuck in a call)
                    left, why, cut = None, "the clean-up was still under way when the time was up", True
                    log.warning("News-burst clean-up: the time was up before it was over")
                except Exception as e:  # MT5 busy or gone
                    left, why = None, "MetaTrader 5 did not answer"
                    log.warning("News-burst clean-up failed: %s", str(e) or type(e).__name__)
                if left == 0 and not self.unreported and not self.unchecked:
                    return
                if not (ended or cut or left or self.unreported or self.unchecked):
                    log.warning("Stopping: no news-burst clean-up this time (MetaTrader 5 did not answer); the next "
                                "start does it")
                    return
            if loop.time() + self.STOP_SWEEP_PAUSE >= until:
                break
            await asyncio.sleep(self.STOP_SWEEP_PAUSE)
        parts = []
        if running:
            parts.append("news burst(s) #" + ", #".join(map(str, running)) + " still ending")
        elif left is None:
            parts.append(why)
        if left:
            parts.append(f"{left} news-burst order(s) / position(s) not removed")
        if self.unreported or self.unchecked:
            parts.append("news-burst results not all reported")
        await self.report("The connector stopped before the news-burst clean-up was over (" + "; ".join(parts) +
                          "): check MetaTrader 5 and remove what is left (positions keep their stop-loss and "
                          "take-profit). The next start cleans it up too.", "error", keep=True)

    async def session(self, ws) -> bool:
        """Handle one connection. Returns True when the connector should stop for good."""
        snap = asyncio.create_task(self.snapshot_loop())
        try:
            async for raw in ws:
                msg = json.loads(raw)
                kind = msg.get("type")
                if kind == "welcome":
                    self.scalper.symbols = [str(x) for x in msg.get("symbols") or []]
                    self.scalper.paused = False
                    self.apply_settings(msg["settings"])
                    log.info("Connected to %s as %s", self.url, msg["user"]["email"])
                    for sig in msg.get("open_signals", []):       # never auto-enter stale signals after a reconnect
                        self.handled.add(sig["id"])
                        if self.paper:
                            self.broker.prices.setdefault(sig["symbol"] + self.suffix, sig["price"])
                    await self.flush_outbox(ws)
                    for plan in msg.get("bursts") or []:
                        await self.on_burst(plan)
                    await self.snapshot()
                elif kind == "burst":
                    await self.on_burst(msg.get("burst") or {})
                elif kind == "burst_cancel":
                    await self.on_burst_cancel(msg.get("id"))
                elif kind == "settings":
                    if isinstance(msg.get("symbols"), list):    # the pairs the engine trades may have changed too
                        self.scalper.symbols = [str(x) for x in msg["symbols"]]
                    self.apply_settings(msg["settings"])
                elif kind == "signal":
                    await self.on_signal(msg["signal"], manual=False)
                elif kind == "execute":
                    await self.on_signal(msg["signal"], manual=True)
                elif kind == "signal_closed":
                    await self.on_signal_closed(msg["signal"])
                elif kind == "close":
                    await self.close_ticket(int(msg["ticket"]), "closed from web app")
                elif kind == "ping":
                    await self.send({"type": "pong"})
                elif kind in ("error", "kicked", "replaced"):
                    reason = msg.get("reason") or msg.get("message")
                    log.error("Server closed the session: %s", reason)
                    if reason in ("invalid_token", "token_revoked"):
                        log.error("Create a new token in the web app (Connector page) if this is unexpected.")
                        return True
                    if reason in ("banned", "account_deleted"):
                        log.error("This account can no longer use the platform. Contact support.")
                        return True
                    if kind == "replaced":
                        return True
                    if reason == "subscription_inactive":
                        self.scalper.paused = True      # until a welcome; the open scalps still close at their time
                        log.error("Subscription inactive. Will retry in 10 minutes.")
                        await asyncio.sleep(600)
                    return False
        finally:
            snap.cancel()
        return False

    # ── signals ────────────────────────────────────────────────
    async def on_signal(self, sig: dict, manual: bool):
        if self.paper:
            self.broker.prices[sig["symbol"] + self.suffix] = sig["price"]
        mode = self.settings.get("execution", "notify")
        if not manual:
            if sig["id"] in self.handled:
                return
            if mode != "auto":
                log.info("Signal #%s %s %s (execution=%s: not trading automatically)", sig["id"], sig["side"],
                         sig["symbol"], mode)
                return
            if not self.wants(sig):
                return
        if self.stopping:
            return await self.skip(sig, "the connector is stopping")
        self.handled.add(sig["id"])
        async with self.lock:
            try:
                await self.execute(sig)
            except Exception as e:  # broker errors must never kill the connector
                await self.skip(sig, f"error: {e}")

    async def skip(self, sig: dict, reason: str):
        log.info("Skipped signal #%s %s %s: %s", sig["id"], sig["side"], sig["symbol"], reason)
        await self.send({"type": "signal_skipped", "signal_id": sig["id"], "reason": reason})

    async def execute(self, sig: dict):
        symbol = sig["symbol"] + self.suffix
        if sig.get("status", "open") != "open":
            return await self.skip(sig, "signal already closed")
        exp = parse_iso(sig.get("expires_at"))
        if exp and exp <= utcnow():
            return await self.skip(sig, "signal expired")
        if self.symbol_owner(symbol):           # a news burst (or Rapid run) past its waiting phase
            runs = [st for st in self.bursts.values() if st["symbol"] == symbol and st["status"] != "scheduled"]
            if not all(st.get("local") for st in runs):     # a news run keeps the symbol
                return await self.skip(sig, f"news burst running on {symbol}")
            # local Rapid runs (on a short-term signal) make way: the signal goes first, as over a scalp
            if not await self.rapid_make_way(symbol, runs):
                return await self.skip(sig, f"the Rapid run on {symbol} could not be ended")
        mine = await self.call(self.broker.positions)
        # what the positions as read hold (a run that woke up during the read: the check before the order skips it)
        owner = self.symbol_owner(symbol, mine, runs=False)
        if owner == "scalp":
            # a short-term trade holds the symbol (netting: the order would merge into it): the signal goes first
            if not await self.scalper.make_way(symbol):
                return await self.skip(sig, f"the short-term trade on {symbol} could not be closed")
            mine = await self.call(self.broker.positions)
            owner = self.symbol_owner(symbol, [p for p in mine if self.scalp_id(p) is None],   # one listed is closed
                                      runs=False)
        positions = [p for p in mine if self.burst_id(p) is None and self.scalp_id(p) is None]   # long-term ones
        if any(self.signal_id(p) == sig["id"] for p in positions):
            return await self.skip(sig, "already in this trade")
        if owner == "long":
            return await self.skip(sig, f"already have a position on {symbol}")
        if owner:       # left by a run that is gone (netting: the signal would merge into it; the sweeper closes it)
            return await self.skip(sig, f"a news-burst position on {symbol} is still being closed")
        acct = await self.call(self.broker.account)
        if not acct["demo"] and not self.allow_real:
            return await self.skip(sig, "real account locked (allow_real_account = no)")
        if sig.get("test") and not acct["demo"]:
            return await self.skip(sig, "test signals only run on demo accounts")
        margin = acct.get("margin_mode") or "netting"
        if margin != "hedging":
            # one position per symbol (netting and exchange): an order would merge into a position this connector
            # does not manage (opened by hand or by another EA), changing its size and its stop-loss
            others = [p for p in await self.call(self.broker.positions, all_magic=True)
                      if p["symbol"] == symbol and p["ticket"] not in {q["ticket"] for q in mine}]
            if others:
                return await self.skip(sig, f"another position on {symbol} ({margin} account: the order would merge "
                                            f"into it)")
        # long-term's own losses: the short-term and Rapid results today are left out (each has its own limit)
        equity = acct["equity"] - self.scalper.pnl_today() - self.rapid_pnl_today(mine)
        self.risk.update_day(utcnow().date(), equity)
        if self.risk.check_daily_loss(equity):
            await self.report(f"Daily loss limit reached ({self.risk.daily_loss_pct}%): no new trades today", "warn")
        bid, ask, spread = await self.call(self.broker.quote, symbol)
        if bid is None:
            return await self.skip(sig, f"no price for {symbol}")
        spec = await self.call(self.broker.symbol_spec, symbol)
        k = spec.scale if spec.scale > 0 else 1.0      # the caps are in a currency pair's pips: × the symbol's scale
        ok, why = self.risk.can_open(len(positions), spread, k)
        if not ok:
            return await self.skip(sig, why)
        entry = ask if sig["side"] == "buy" else bid
        drift = abs(entry - sig["price"]) / spec.pip
        # a test signal is made just now, from the server's feed (which may quote apart from this broker), and its
        # stop/target are re-anchored below on this broker's price anyway
        if not sig.get("test") and drift > float(self.settings.get("max_price_drift_pips", 5)) * k:
            return await self.skip(sig, f"price moved {drift:.1f} pips from the signal")
        # keep the signal's stop/target DISTANCES, re-anchored on this broker's actual price
        sign = 1 if sig["side"] == "buy" else -1
        sl = entry - sign * abs(sig["price"] - sig["sl"])
        tp = entry + sign * abs(sig["tp"] - sig["price"]) if sig.get("tp") else None
        if spec.min_stop_distance and abs(entry - sl) < spec.min_stop_distance:
            return await self.skip(sig, "stop-loss closer than the broker allows")
        lots = self.risk.lot_size(acct["balance"], entry, sl, spec)
        if sig.get("test"):
            test_risk = RiskManager({**self.risk_cfg, "risk_per_trade_pct": min(TEST_RISK_PCT, self.risk.risk_pct)})
            lots = min(lots, test_risk.lot_size(acct["balance"], entry, sl, spec))
        if lots <= 0:
            return await self.skip(sig, "risk per trade too small for the minimum lot size")
        if self.dry_run:
            d = spec.digits
            return await self.report(f"[dry-run] would {sig['side']} {lots} {symbol} @ {entry:.{d}f} SL {sl:.{d}f}"
                                     + (f" TP {tp:.{d}f}" if tp else ""))
        # again, with no await before the order goes into the MT5 queue: a burst may have woken up meanwhile, and
        # its netting pre-check must either run after this order (and see it) or have stopped us here
        if self.burst_running(symbol):
            return await self.skip(sig, f"news burst running on {symbol}")
        try:
            res = await self.call(self.broker.open_market, symbol, sig["side"], lots, sl, tp, f"fx:{sig['id']}")
        except Exception:
            res = await self._opened_anyway(sig)
            if res is None:
                raise
        # the position exists from here on: nothing below may report the signal as skipped
        self.signal_of[res["ticket"]] = sig["id"]
        self._save_map()
        try:
            await self.send({"type": "trade_opened", "trade": {
                "ticket": res["ticket"], "signal_id": sig["id"], "symbol": symbol, "side": sig["side"],
                "lots": res["lots"], "entry_price": res["price"], "sl": sl, "tp": tp, "demo": acct["demo"],
                "opened_at": utcnow().isoformat()}})
            await self.report(f"Opened {sig['side']} {res['lots']} {symbol} @ {res['price']:.{spec.digits}f} "
                              f"({'test signal' if sig.get('test') else 'signal'} #{sig['id']})")
        except Exception as e:
            log.warning("Signal #%s opened as #%s, but reporting it failed: %s", sig["id"], res["ticket"], e)
        await self.snapshot()

    async def _opened_anyway(self, sig: dict) -> dict | None:
        """After a failed open_market: MT5 may still have opened it (no answer to order_send is not a refusal).
        A position carrying this signal's tag is ours (execute() checked there was none before)."""
        try:
            p = next((p for p in await self.call(self.broker.positions) if self.signal_id(p) == sig["id"]), None)
        except Exception:
            return None
        return p and {"ticket": p["ticket"], "price": p["entry_price"], "lots": p["lots"]}

    def scalp_id(self, pos: dict) -> int | None:
        s = self.scalper.open.get(pos["ticket"])
        if s:
            return s["id"]
        tag = run_tag(pos.get("comment"))
        return tag[1] if tag and tag[0] == "scalp" else None

    def signal_id(self, pos: dict) -> int | None:
        if pos["ticket"] in self.signal_of:
            return self.signal_of[pos["ticket"]]
        m = FX_TAG.search(pos.get("comment") or "")
        return int(m.group(1)) if m else None

    async def on_signal_closed(self, sig: dict):
        if self.paper and sig.get("close_price"):
            self.broker.prices[sig["symbol"] + self.suffix] = sig["close_price"]
        if not self.settings.get("follow_exits", True):
            return
        why = f"signal #{sig['id']} {sig.get('close_reason') or 'closed'}"
        try:
            tickets = [p["ticket"] for p in await self.call(self.broker.positions) if self.signal_id(p) == sig["id"]]
        except Exception as e:      # positions unreadable now: the server sends this only once, so retry it later
            mine = {t for t, p in self.known.items() if self.signal_id(p) == sig["id"]}
            for t in mine | {t for t, s in self.signal_of.items() if s == sig["id"]}:
                await self._close_failed(t, why, e, warn=False)    # old map entries may be long gone: warn on retry
            return
        for t in tickets:
            await self.close_ticket(t, why, price=sig.get("close_price") if self.paper else None)

    CLOSE_PAUSE = 0.5           # between the quick attempts of one close
    CLOSE_RETRY_FOR = 600       # a close that keeps failing is retried this long (s), then reported and given up

    async def close_ticket(self, ticket: int, why: str, price: float | None = None, snap: bool = True,
                           retry: bool = False):
        """`retry`: close_loop's own attempt at a queued close; otherwise a request (the server's) to answer."""
        async with self.lock:
            err, mine = None, False         # mine: closed by this call (its close time is now, MT5's clock or not)
            for attempt in range(3):        # a requote or a busy trade context usually passes a moment later
                if attempt:
                    await asyncio.sleep(self.CLOSE_PAUSE)
                try:
                    pos = next((p for p in await self.call(self.broker.positions) if p["ticket"] == ticket), None)
                except Exception as e:      # unreadable is not gone
                    err = e
                    continue
                if not pos:                 # closed (stop-loss, take-profit, by hand, or by an earlier deal)
                    c = self.closing.pop(ticket, None)
                    err = None
                    if c and c["warned"]:   # settles the warning (kept for the server if it is unreachable)
                        await self.report(f"#{ticket} is closed ({why})", keep=True)
                    break
                if self.dry_run:
                    self.closing.pop(ticket, None)
                    return await self.report(f"[dry-run] would close #{ticket} ({why})")
                try:
                    await self.call(self.broker.close_position, pos, "fx exit", price=price)
                except Exception as e:
                    err = e
                    continue
                c = self.closing.pop(ticket, None)
                err, mine = None, True
                await self.report(f"Closed #{ticket} {pos['symbol']} ({why})", keep=bool(c and c["warned"]))
                break
            if err is not None:
                await self._close_failed(ticket, why, err, retry=retry)
            else:                           # a scalp's: reported (and booked) now, the scalper may be stopped
                self.scalper.closed(ticket, now=mine)
        if snap:
            await self.snapshot()

    async def _close_failed(self, ticket: int, why: str, err: Exception, warn: bool = True, retry: bool = False):
        """The server asks only once: a failed close is retried by close_loop with a growing pause (5 s .. 60 s)
        until the position is gone, for CLOSE_RETRY_FOR at most; then it is reported once and left to its own
        stop-loss / take-profit. A new request for a queued close (Close clicked again) is answered again and
        starts the retries over. Messages keep "#ticket:" first: the web app's test card reads them; they wait
        in the outbox while the server is unreachable (close_loop goes on without it)."""
        now = asyncio.get_running_loop().time()
        c = self.closing.get(ticket)
        if c is None or not retry:
            self.closing[ticket] = {"why": why, "since": now, "tries": 1, "at": now + 5,
                                    "warned": warn or bool(c and c["warned"])}
            if warn:
                await self.report(f"Could not close #{ticket}: {err} ({why}). Retrying.", "warn", keep=True)
            return
        if now - c["since"] >= self.CLOSE_RETRY_FOR:
            del self.closing[ticket]
            return await self.report(f"Could not close #{ticket}: {err} ({why}). Gave up after "
                                     f"{self.CLOSE_RETRY_FOR // 60} min: close it in MetaTrader 5 (its stop-loss and "
                                     "take-profit still apply).", "error", keep=True)
        if not c["warned"]:
            c["warned"] = True
            await self.report(f"Could not close #{ticket}: {err} ({why}). Retrying.", "warn", keep=True)
        c["tries"] += 1
        c["at"] = now + min(5 * 2 ** (c["tries"] - 1), 60)

    async def retry_closes(self):
        now = asyncio.get_running_loop().time()
        due = [t for t, c in self.closing.items() if now >= c["at"]]
        for ticket in due:
            if ticket in self.closing:
                await self.close_ticket(ticket, self.closing[ticket]["why"], snap=False, retry=True)
        if due:                     # report what closed (trade_closed waits in the outbox while offline)
            await self.snapshot()

    # ── snapshots & closed-trade detection ─────────────────────
    SNAPSHOT_EVERY = 5

    async def snapshot_loop(self):
        while True:
            await asyncio.sleep(self.SNAPSHOT_EVERY)
            await self.snapshot()

    async def close_loop(self):
        while True:
            await asyncio.sleep(self.SNAPSHOT_EVERY)
            try:
                await self.retry_closes()
            except Exception as e:
                log.warning("Close retry failed: %s", e)

    async def snapshot(self):
        try:
            await self.push_snapshot()
        except Exception as e:      # MT5 busy or briefly away: the next one catches up
            log.warning("Snapshot failed: %s", e)

    async def push_snapshot(self):
        acct = await self.call(self.broker.account)
        positions = await self.call(self.broker.positions)
        current = {p["ticket"]: p for p in positions}
        for ticket in set(self.known) - set(current):
            gone = self.known[ticket]       # the burst runner and the scalper report their own closes
            if self.burst_id(gone) is not None or self.scalp_id(gone) is not None:
                continue
            res = await self.report_closed(ticket)
            log.info("Position #%s closed: P&L %s", ticket, res and res["pnl"])
        self.known = current
        await self.send({"type": "snapshot", "account": acct, "positions": [
            {**p, "signal_id": self.signal_id(p), "burst_id": self.snapshot_burst_id(p), "strategy": self.strategy_of(p)}
            for p in positions]})

    def snapshot_burst_id(self, p: dict) -> int | None:
        """A position's burst_id as the snapshot shows it: its news run's (fxb:, or a ticket a news runner owns), none
        for a local Rapid run's (fxr:: its id, a start in ms, is no Burst row's)."""
        bid = self.burst_id(p)
        if (run_tag(p.get("comment")) or ("",))[0] == RUN_KIND or (self.bursts.get(bid) or {}).get("local"):
            return None
        return bid

    def strategy_of(self, p: dict) -> str:
        """A position's strategy as the snapshot shows it: "rapid" (a local Rapid run's fxr: comment, or a news run
        whose runner is, or was, a Rapid one: rapid_news keeps that across a restart), "burst" (any other news run's),
        "short" (a scalp's), else "long"."""
        tag, bid = run_tag(p.get("comment")), self.burst_id(p)
        if tag and tag[0] == RUN_KIND or bid is not None and (
                bid in self.rapid_news or (self.bursts.get(bid) or {}).get("kind") == RUN_KIND):
            return RUN_KIND
        return "burst" if bid is not None else "short" if self.scalp_id(p) is not None else "long"

    async def report_closed(self, ticket: int, wait: float = 1.0, info: dict | None = None) -> dict | None:
        """`info`: what the server books a news-burst trade from if its trade_opened never arrived. A Rapid trade's
        (its strategy "rapid": a runner's close, its clean-up's or the sweeper's) is booked to Rapid's day too."""
        res = None
        for _ in range(3):            # history can lag a moment behind the position list
            res = await self.call(self.broker.closed_result, ticket) or res
            if res and res["exit_time"]:   # exit_time None: MT5's clock not proven by a live tick yet
                break
            await asyncio.sleep(wait)
        await self.send({"type": "trade_closed", "trade": {
            **(info or {}), "ticket": ticket, "exit_price": res and res["exit_price"], "pnl": res and res["pnl"],
            "closed_at": (res and res["exit_time"]) or utcnow().isoformat()}})
        if (info or {}).get("strategy") == RUN_KIND:
            await self._rapid_closed(ticket, res)
        return res

    # ── news burst ─────────────────────────────────────────────
    BURST_POLL = 0.1            # monitor interval (s)
    BURST_RESULT_WAIT = 0.3     # pause between closed_result retries
    BURST_SETTLE = 0.5          # after a slice deal: the next read of the position may not show it yet
    BURST_UNSURE = 2.0          # after a close: how long the history is waited for to show its deals (else volume counts)
    REPORT_TRIES = 10           # a position the connector closed: history reads (BURST_RESULT_WAIT apart) until it is flat
    BURST_MAX_ERRORS = 50       # consecutive failed polls before a burst gives up (≈ 5 s of a dead terminal)
    BURST_GONE = 30             # polls a fill may stay out of the list of positions, its deals not adding up to nothing
    SWEEP_EVERY = 30
    SWEEP_WAITS = 10            # sweeps a closed burst position's report waits for its history to show the close
    ANALYSIS_WAIT = 2.0         # pre-news analysis: how long the M1 bars (and H1, if not in yet) are waited for
    BARS_EARLY = 5.0            # H1 bars are asked for on the plan when it leaves at least this long before the check
    BURST_FROZEN = 5            # closes of a position refused as frozen (10029) before a v1.3.0 run warns (≈ 7 s)

    def burst_id(self, x: dict) -> int | None:
        """The run (news burst or Rapid) an order or position belongs to: by its comment, or a ticket a runner owns."""
        tag = run_tag(x.get("comment"))
        if tag and tag[0] in ("burst", "rapid"):
            return tag[1]
        return self.burst_owned.get(x["ticket"]) or self.burst_owned.get(x.get("identifier"))

    def burst_running(self, symbol: str) -> bool:
        """Past its waiting phase a burst owns the symbol (on netting anything else would merge into it)."""
        return any(st["symbol"] == symbol and st["status"] != "scheduled" for st in self.bursts.values())

    def symbol_owner(self, symbol: str, positions: list[dict] = (), runs: bool = True) -> str | None:
        """What holds `symbol`, in the order it goes first: a news burst or Rapid run past its waiting phase ("burst" /
        "rapid": its orders are placed, filled or not; `runs` False: not looked at); then, of `positions` (this bot's,
        as read; none given: only the runs are looked at), a short-term trade ("scalp"), a long-term one ("long": a
        signal's, or any other not a run's) and one a run that is gone left there (its kind; the sweeper closes it).
        None: nothing holds it."""
        if runs:
            for st in self.bursts.values():
                if st["symbol"] == symbol and st["status"] != "scheduled":
                    return st.get("kind", "burst")
        held = [p for p in positions if p["symbol"] == symbol]
        if any(self.scalp_id(p) is not None for p in held):
            return "scalp"
        if any(self.burst_id(p) is None for p in held):
            return "long"
        gone = [run_tag(p.get("comment")) for p in held if self._orphan(p)]
        if gone:                                # with no tag (known by a ticket a runner owned): a news burst's
            return "rapid" if gone[0] and gone[0][0] == "rapid" else "burst"
        return None

    @staticmethod
    def _run_tag(st: dict) -> tuple[str, int]:
        """What run_tag reads off the comments of a run's orders: its kind's (a burst's fxb:<id>, a local Rapid run's
        fxr:<id>), or the tag it names (a Rapid news run: "burst", fxb:<id> as every news run's)."""
        return st.get("tag") or st.get("kind", "burst"), st["id"]

    @staticmethod
    def _owns(st: dict, x: dict) -> bool:
        """Opened by one of this burst's orders: one it placed, or one it has no ticket of (no answer) by its comment."""
        return x["symbol"] == st["symbol"] and ((x.get("identifier") or x["ticket"]) in st["placed"]
                                                 or x["ticket"] in st["placed"]
                                                 or run_tag(x.get("comment")) == Connector._run_tag(st))

    @staticmethod
    def _run_name(st: dict) -> str:
        """How the reports name a run."""
        return "Rapid run" if st.get("kind") == RUN_KIND else "News burst"

    @staticmethod
    def _run_ref(st: dict) -> dict:
        """What a run's trade reports carry to say whose they are: a news run's burst_id (a Rapid one's strategy too);
        a local Rapid run's strategy only (it has no Burst row on the server)."""
        if st.get("local"):
            return {"strategy": RUN_KIND}
        return {"burst_id": st["id"], **({"strategy": RUN_KIND} if st.get("kind") == RUN_KIND else {})}

    async def rapid_room(self, st: dict | None, acct: dict) -> float | None:
        """What is still free of Rapid's own daily loss limit (rapid_day.json) for run `st` (money; None: no limit):
        the limit, less today's Rapid result, less what each other live Rapid run holds of it (its reservation,
        st["reserved"]: count × the money each order risks at its stop, released once the run ends and leaves
        self.bursts; what its positions lose now when that is more) and what the Rapid positions of no live run lose
        now (left to the sweeper). A Rapid trade closed and not booked yet loses what it showed last (rapid_booking).
        Reaching the limit is told once a day."""
        self.rapid_day.roll(acct["balance"])
        try:
            held = await self.call(self.broker.positions)
        except Exception:           # as last seen
            held = list(self.known.values())
        loss: dict = {}             # run id -> what its positions lose now
        for x in held:
            if self.strategy_of(x) == RUN_KIND:
                bid = self.burst_id(x)
                loss[bid] = loss.get(bid, 0.0) + max(0.0, -(x.get("profit") or 0.0))
        for b in self._rapid_unbooked(held):
            loss[b["id"]] = loss.get(b["id"], 0.0) + max(0.0, -b["profit"])
        at_risk = 0.0
        for o in self.bursts.values():
            if o is not st and o.get("kind") == RUN_KIND and o["status"] != "scheduled":
                at_risk += max(o.get("reserved") or 0.0, loss.pop(o["id"], 0.0))
        free = self.rapid_day.free(at_risk + sum(loss.values()))
        if free is not None and free <= 0 and self.rapid_day.reached() and not self.rapid_day.day["warned"]:
            self.rapid_day.day["warned"] = True
            self.rapid_day.save()
            await self.report(f"Rapid daily loss limit reached ({-self.rapid_day.day['pnl']:.2f} lost today): no new "
                              "Rapid run until tomorrow (UTC). The other strategies go on.", "warn", keep=True)
        return free

    def rapid_pnl_today(self, positions: list[dict] | None = None) -> float:
        """Today's Rapid result (UTC): booked to its day, plus what its open positions show (`positions`: as read;
        None: as last seen; one booked already is closed, or counted by its guess, and is left out) and what its
        trades closed and not booked yet showed last (rapid_booking). The long-term daily loss limit leaves it out,
        as short-term's: Rapid has its own."""
        if self.paper:              # no run there
            return 0.0
        held = list(self.known.values() if positions is None else positions)
        return self.rapid_day.today() + sum(p.get("profit") or 0.0 for p in held if self.strategy_of(p) == RUN_KIND
                                            and (p.get("identifier") or p["ticket"]) not in self.rapid_booked) \
            + sum(b["profit"] for b in self._rapid_unbooked(held))

    def _rapid_owed(self, pid: int, rid: int | None, profit: float | None = None):
        """Rapid trade `pid` of run `rid` is out of the list of positions (closed, or not listed for a moment) and not
        booked yet: until _rapid_closed books it, what it showed last (`profit`; None: as seen before) counts to
        Rapid's day (rapid_pnl_today, rapid_room), so the long-term daily limit does not take its loss for one of its
        own, nor Rapid's limit miss it."""
        if pid in self.rapid_booked:
            return
        if profit is None:
            profit = (self.rapid_booking.get(pid) or self.known.get(pid) or {}).get("profit")
        self.rapid_booking[pid] = {"id": rid, "profit": profit or 0.0}

    def _rapid_unbooked(self, held) -> list[dict]:
        """Of rapid_booking, the trades not in `held` (as read: one listed again counts as a position) nor booked."""
        listed = {x["ticket"] for x in held} | {x.get("identifier") for x in held}
        return [b for pid, b in self.rapid_booking.items() if pid not in listed and pid not in self.rapid_booked]

    async def _rapid_closed(self, ticket: int, res: dict | None):
        """A Rapid trade closed (report_closed): its result is added to Rapid's day it closed on (UTC; a day it starts
        begins from the balance before it), once per ticket. One whose result could not be read counts the loss it
        showed last, the web app's log asks the user to check it, and a result read later replaces that guess. Booked,
        it leaves rapid_booking (counted there until then)."""
        pnl = res and res["pnl"]
        done = self.rapid_booked.get(ticket)
        if done or (done is False and pnl is None):
            self.rapid_booking.pop(ticket, None)
            return
        if pnl is None:
            seen = self.known.get(ticket) or self.rapid_booking.get(ticket) or {}
            pnl = round(min(0.0, seen.get("profit") or 0.0), 2)
            await self.report(f"Rapid #{ticket}: its result could not be read, so today's Rapid loss counts "
                              f"{-pnl:.2f} (what it showed last). Check it in MetaTrader 5's history.", "warn",
                              keep=True)
        day, balance = self.rapid_day.day_of(res and res["exit_time"]), None
        if day > self.rapid_day.day["day"]:
            try:
                balance = (await self.call(self.broker.account))["balance"] - pnl
            except Exception as e:      # MT5 busy: the day's start as the last day ended
                log.warning("Rapid: could not read the balance (%s)", e)
        guess = not (res and res["pnl"] is not None)
        self.rapid_booked[ticket] = not guess
        self.rapid_booking.pop(ticket, None)
        self.rapid_day.book(day, pnl, ticket, balance, guess)
        log.info("Rapid #%s closed: P&L %s (today %.2f)", ticket, res and res["pnl"], self.rapid_day.day["pnl"])

    RAPID_WAY_WAIT = 10.0       # a long-term signal: how long the local Rapid runs on its symbol may take to end (s)
    RAPID_SKIP_EVERY = 900      # a local run skipped: told in the web app at most this often per symbol (s)

    async def rapid_make_way(self, symbol: str, runs: list[dict]) -> bool:
        """(With the connector's lock held: execute.) A long-term signal on `symbol` goes first: the local Rapid runs
        there are ended by their runners, their orders removed and their positions closed at once (st["way"]). True
        once they are over (within RAPID_WAY_WAIT)."""
        for st in runs:
            if not st["way"]:
                log.info("Rapid run #%s on %s: a long-term signal on the same symbol goes first, ending it", st["id"],
                         symbol)
            st["cancel"] = st["way"] = True
            st["wake"].set()
        tasks = [st["task"] for st in runs if st.get("task")]
        if tasks:
            await asyncio.wait(tasks, timeout=self.RAPID_WAY_WAIT)
        return all(st["id"] not in self.bursts for st in runs)

    def _local_run(self, rid: int, sym: str, side: str, c: float, stretch_at) -> dict:
        """A local Rapid run's state, as on_burst builds a news run's: started now on a short-term signal (no plan,
        no Burst row: kind "rapid", local, fxr: comments, no tag), on the signal's side only, at confidence `c`, its
        hold X / Y the user's Rapid block's. Past X the rule turning against it closes it (`against`, kept up to date
        by Scalper.step from `stretch` at the signal); `way`: a long-term signal ends it (rapid_make_way)."""
        p = self._rapid_params(self.rapid)
        return {"id": rid, "kind": RUN_KIND, "symbol": sym, "test": False, "title": "short-term signal", "p": p,
                "release": asyncio.get_running_loop().time(), "status": "checking", "wake": asyncio.Event(),
                "cancel": False, "expired": False, "netting": False, "demo": True, "spec": None, "n": 0, "lots": 0.0,
                "orders": 0, "side": None, "pnl": 0.0, "placed": set(), "pending": {}, "unknown": {},
                "to_cancel": set(), "trades": {}, "warned": set(), "context": None, "pre_mid": None, "h1": None,
                "analysed": True, "sl_pips": p["sl_pips"], "money": None, "sides": (side,), "frozen": {},
                "tp": (0.0, 0.0), "order_kind": "stop_limit", "tp_usd": None, "c": c, "reserved": 0.0, "margin": 0.0,
                "local": True, "stretch": stretch_at, "against": False, "way": False}

    async def start_local(self, sym: str, d: dict):
        """A short-term signal on `sym` (Scalper.step, Rapid's signal trigger on) starts a local Rapid run: stop-limit
        orders on the signal's side only, run at once by the news runner (run_burst), at the confidence of how far the
        price stretched (confidence_from_stretch; bot/rapid.py sizes each order from it). Demo accounts only. One run
        per symbol at a time, none for cooldown_seconds after one ended there, none shortly before a news run checks
        the symbol (a run lasts its orders' expiry, then Y at most), none where anything holds it (symbol_owner, any
        position, a run's order still pending) and none with no room left of Rapid's daily limit."""
        sc, b = self.scalper, self.broker
        async with self.lock:
            loop = asyncio.get_running_loop()
            now, p = loop.time(), self._rapid_params(self.rapid)
            if not sc.rapid_on() or now < self.rapid_cool.get(sym, 0.0) or self.symbol_owner(sym):
                return
            if not self.demo:
                return sc.say("rapid-demo", "Rapid: signal runs need a demo account: none starts on this one", 86400)
            life = p["expire_seconds"] + p["max_hold_seconds"]
            if sc._burst_soon(sym, now, life):
                return
            if closes_soon(utcnow(), life):
                return sc.say("rapid-closes", "Rapid: no new run around the daily and weekly close (17:00 New York)",
                              3600, "info")
            if any(x["symbol"] == sym for x in await self.call(b.positions, all_magic=True)):
                return
            if any(self.burst_id(o) is not None for o in await self.call(b.pending_orders, sym)):
                return
            acct = await self.call(b.account)
            if not acct["demo"]:
                return sc.say("rapid-demo", "Rapid: signal runs need a demo account: none starts on this one", 86400)
            free = await self.rapid_room(None, acct)
            if free is not None and free <= 0:
                return
            rid = self.rapid_last = max(self.rapid_last + 1, int(time.time() * 1000))
            c = confidence_from_stretch(d.get("stretch"), SCALP_RULE["z"])
            st = self.bursts[rid] = self._local_run(rid, sym, d["side"], c, d.get("stretch"))
            log.info("Rapid run #%s: a %s signal on %s (stretch %s, confidence %.2f)", rid, d["side"], sym,
                     d.get("stretch"), c)
            st["task"] = asyncio.create_task(self.run_burst(st))
            self._save_rapid_open()

    def rapid_margin(self, st: dict) -> float:
        """The margin the other Rapid runs reserved (st["margin"]: what one side of theirs needs) while none of their
        orders filled: taken off the free margin run `st` may use (once filled, it is in the account's margin)."""
        return sum(o.get("margin") or 0.0 for o in self.bursts.values() if o is not st and o.get("kind") == RUN_KIND
                   and o.get("status") != "scheduled" and not o.get("side"))

    def rapid_book(self, st: dict, failed: bool = False):
        """A Rapid run ended (its P&L in st["pnl"]; each of its closes was booked to Rapid's day as reported,
        _rapid_closed) and its reservations went with it (it left self.bursts). A local run's symbol then rests for
        cooldown_seconds, and rapid_open.json forgets it once nothing of it is left to report. A news run's id leaves
        rapid_news now when its runner handed nothing to the sweeper and did not fail (a clean-up after a crash may
        miss what it could not read); else the sweeper drops it once nothing of the run is left (_rapid_forget)."""
        if st.get("local"):
            self.rapid_cool[st["symbol"]] = asyncio.get_running_loop().time() + self.rapid["cooldown_seconds"]
            self._save_rapid_open()
        elif st["id"] in self.rapid_news and not failed and not any(
                c and c[0] is st for c in self.unreported.values()) and not any(
                r[0] is st for r in self.unchecked.values()):
            self.rapid_news.discard(st["id"])
            self._save_rapid_open()

    async def burst_status(self, st: dict, status: str, reason: str | None = None):
        st["status"] = status
        if st.get("local"):           # a local Rapid run: no Burst row on the server, its trades are reported alone
            return
        msg = {"type": "burst_update", "id": st["id"], "status": status}
        if status != "scheduled":
            fills = st["n"] * (2 if st["side"] == "both" else 1) if st["netting"] and st["side"] else len(st["trades"])
            msg.update(side=st["side"], orders=st["orders"], fills=fills, reason=reason,
                       pnl=round(st["pnl"], 2) if st["trades"] else None)
        await self.send(msg)

    async def on_burst(self, plan: dict):
        try:
            bid, until = int(plan["id"]), float(plan.get("seconds_until") or 0)
        except (KeyError, TypeError, ValueError):
            return
        if bid in self.burst_done or self.stopping:   # stopping: no answer, so the server keeps it for the next start
            return
        if bid in self.bursts:                    # the server resends a plan until it sees "scheduled"
            if self.bursts[bid]["status"] == "scheduled":
                await self.send({"type": "burst_update", "id": bid, "status": "scheduled"})
            return
        why, kind = None, plan.get("kind", "burst")
        if self.paper:
            why = "paper mode: bursts need MetaTrader 5"
        elif kind not in ("burst", RUN_KIND):           # a run of a kind this connector does not know: never guessed
            why = "unknown run kind"
        else:
            try:
                p = clamp_burst(plan.get("params") or {}) if kind == "burst" else self._rapid_params(plan.get("params"))
            except ValueError as e:                     # a stop unit, switch or number it cannot read
                why = f"invalid plan ({e})"
        if why:
            self.burst_done.add(bid)
            await self.report(f"{'Rapid run' if kind == RUN_KIND else 'News burst'} #{bid} skipped: {why}", "warn")
            return await self.send({"type": "burst_update", "id": bid, "status": "skipped", "reason": why})
        ctx = plan.get("context")
        st = {"id": bid, "kind": kind, "symbol": str(plan.get("symbol") or "") + self.suffix,
              "test": bool(plan.get("test")), "title": str(plan.get("event_title") or ""), "p": p,
              "release": asyncio.get_running_loop().time() + until, "status": "scheduled", "wake": asyncio.Event(),
              "cancel": False, "expired": False, "netting": False, "demo": True, "spec": None, "n": 0, "lots": 0.0,
              "orders": 0, "side": None, "pnl": 0.0, "placed": set(), "pending": {}, "unknown": {},
              "to_cancel": set(), "trades": {}, "warned": set(),
              # v1.3.0: the server's context for the analysis, the mid just before the release (news_against), the
              # stop and targets in this symbol's pips, the money a money stop risks (None: a stop in pips), the sides
              # it places (one when the analysis picked it), closes refused as frozen (by position ticket)
              "context": ctx if isinstance(ctx, dict) else None, "pre_mid": None, "h1": None, "analysed": False,
              "sl_pips": p["sl_pips"], "money": None, "sides": ("buy", "sell"), "frozen": {},
              "tp": (p["tp_start_pips"], p["tp_step_pips"]) if kind == "burst" else (0.0, 0.0),
              # Rapid: stop-limit orders (a burst's are stops), each order's take-profit in money and the confidence
              # that set it, what the run holds of Rapid's daily limit and of the free margin (rapid_room,
              # rapid_margin); tag: its comments' (fxb:, as every news run's); local: a run the connector started on
              # a signal (fxr: comments, no Burst row)
              "order_kind": "stop" if kind == "burst" else "stop_limit", "tp_usd": None, "c": None, "reserved": 0.0,
              "margin": 0.0, "tag": "burst", "local": False}
        self.bursts[bid] = st
        await self.burst_status(st, "scheduled")
        log.info("%s #%s scheduled: %s (%s) in %.0fs", self._run_name(st), bid, st["symbol"], st["title"], until)
        st["task"] = asyncio.create_task(self.run_burst(st))
        if until - p["lead_seconds"] >= self.BARS_EARLY:    # the H1 bars, long before the check needs them
            st["h1"] = self._burst_side(self._burst_bars(st["symbol"], "H1", H1_BARS))

    @staticmethod
    def _rapid_params(params) -> dict:
        """A Rapid plan's params: the user's rapid block as bot/rapid.py clamp_rapid clamps it (a key missing is its
        default; a value it cannot read raises ValueError), plus what the runner reads as for a burst: Y resolved (none:
        X), the check's lead (the server's, else the burst's default) and a spread cap only when the plan sets one
        (None: none; the trigger's distance already allows for the spread). kind "rapid": never run as a v1.2.0 plan."""
        params = params if isinstance(params, dict) else {}
        p = clamp_rapid(params)
        p["max_hold_seconds"] = p.get("max_hold_seconds", p["hold_seconds"])
        for k in ("lead_seconds", "max_spread_pips"):
            lo, hi, cast, default = BURST_LIMITS[k]
            try:
                v = float(params[k])
            except (KeyError, TypeError, ValueError, OverflowError):
                v = math.nan
            p[k] = cast(min(max(v, lo), hi)) if math.isfinite(v) else default if k == "lead_seconds" else None
        p["kind"] = RUN_KIND
        return p

    async def on_burst_cancel(self, bid):
        try:
            bid = int(bid)
        except (TypeError, ValueError):
            return
        st = self.bursts.get(bid)
        if st:
            st["cancel"] = True
            st["wake"].set()
        elif bid not in self.burst_done:          # e.g. after a restart: nothing of it runs here any more
            await self.send({"type": "burst_update", "id": bid, "status": "cancelled",
                             "reason": "not running on this connector"})

    async def run_burst(self, st: dict):
        status, reason = "error", None
        try:
            status, reason = await self._burst(st)
        except asyncio.CancelledError:
            # stopped while only waiting: nothing to clean up, and no final status, so the server sends the plan
            # again to the connector's next start (a final one would end the burst for good)
            status = None if st["status"] == "scheduled" else "error"
            reason = "connector stopped"
            raise
        except Exception as e:  # a crash must never leave orders or positions behind
            reason = f"{type(e).__name__}: {e}"
            log.exception("%s #%s failed", self._run_name(st), st["id"])
        finally:
            if status is None:
                self.bursts.pop(st["id"], None)
                log.info("%s #%s: still waiting when the connector stopped, left to its next start",
                         self._run_name(st), st["id"])
            else:
                if status == "error":
                    await self._burst_cleanup(st)
                    await self.report(f"{self._run_name(st)} #{st['id']} on {st['symbol']} failed: {reason}", "error")
                self.bursts.pop(st["id"], None)     # a Rapid run's reservations (rapid_room, rapid_margin) go with it
                self.burst_done.add(st["id"])
                if st["kind"] == RUN_KIND:
                    self.rapid_book(st, status == "error")
                await self.burst_status(st, status, reason and reason[:300])
                log.info("%s #%s %s: %s orders, side %s, P&L %.2f", self._run_name(st), st["id"], status, st["orders"],
                         st["side"], st["pnl"])

    async def _burst(self, st: dict) -> tuple[str, str | None]:
        p, sym = st["p"], st["symbol"]
        wait = st["release"] - p["lead_seconds"] - asyncio.get_running_loop().time()
        if wait > 0:
            try:
                await asyncio.wait_for(st["wake"].wait(), wait)
            except TimeoutError:
                pass
        if st["cancel"]:
            return "cancelled", None
        st["status"] = "checking"
        why, orders = await self._burst_plan(st)
        if why:
            msg = f"{self._run_name(st)} #{st['id']} on {sym} skipped: {why}"
            # a local run (a short-term signal's) may be skipped on every signal: told once per RAPID_SKIP_EVERY
            if st.get("local") and not self.scalper._due(f"rapid-skip:{sym}", self.RAPID_SKIP_EVERY):
                log.info(msg)
            else:
                await self.report(msg, "warn")
            return "skipped", why
        if self.dry_run and st["order_kind"] == "stop_limit":
            await self.report(self._rapid_dry_run(st, orders))
            return "skipped", "dry-run"
        if self.dry_run:
            d, k = st["spec"].digits, st["spec"].scale if st["spec"].scale > 0 else 1.0
            first = {o[0]: o[2] for o in reversed(orders)}          # each side's first order: one straddle
            # pips as the user set them when nothing was scaled or worked out (v1.2.0's wording), else to 2 decimals
            sl = p["sl_pips"] if k == 1 and st["money"] is None else round(st["sl_pips"], 2)
            tp = (p["tp_start_pips"], p["tp_step_pips"]) if k == 1 else tuple(round(v, 2) for v in st["tp"])
            # a money stop: what the orders of a side risk at it, and the money too when that is more (the stop capped
            # at the largest pip setting, the lots cut to the step, to volume_max or to fit the free margin)
            at_risk = ""
            if st["money"] is not None:
                risk = f"{st['n'] * st['lots'] * st['sl_pips'] * st['spec'].pip_value_per_lot:.2f}"
                money = f"{st['money']:.2f}"
                at_risk = f" (≈ {risk} at risk" + ("" if risk == money else f", {money} allowed") + ")"
            await self.report(f"[dry-run] news burst #{st['id']} would place {len(orders)} stop orders on {sym}: "
                              + " / ".join(f"{s}-stop {first[s]:.{d}f}" for s in ("buy", "sell") if s in first)
                              + f", {st['n']} × {st['lots']} lots per side, SL {sl} pips" + at_risk
                              + ("" if st["netting"] else f", TP {tp[0]} + i × {tp[1]} pips"))
            return "skipped", "dry-run"
        if st["kind"] == RUN_KIND and not st.get("local"):
            # a Rapid news run: its id kept (rapid_open.json) before its first order is sent, so whatever it leaves
            # (its fxb: comments are every news run's) is Rapid's after a restart too (strategy_of, _orphan_info)
            self.rapid_news.add(st["id"])
            self._save_rapid_open()
        fails, err = 0, ""
        for k, (side, lots, price, sl, tp, comment, *limit) in enumerate(orders):
            if st["cancel"] or fails >= 3:
                break
            # big bursts take seconds to place and the spike may come meanwhile: on a fill stop at once, so the
            # monitor cancels the other side now instead of this loop adding orders a retrace would fill
            if k and k % 2 == 0 and await self._burst_filled(st):
                await self.report(f"{self._run_name(st)} #{st['id']}: filled while placing, {st['orders']} of "
                                  f"{len(orders)} orders placed", "warn")
                break
            # a stop-limit (Rapid): `price` is its trigger, `limit` the price it turns into a limit at
            kw = {"kind": "stop_limit", "limit_price": limit[0]} if limit else {}
            try:
                # the broker drops the order 2 min after our own expiry even if this PC is gone by then
                expire_in = st["release"] + p["expire_seconds"] + 120 - asyncio.get_running_loop().time()
                t = await self.call(self.broker.place_pending, sym, side, lots, price, sl, tp, comment, expire_in, **kw)
            except Exception as e:
                fails, err = fails + 1, str(e)
                continue
            fails = 0
            st["placed"].add(t)
            st["pending"][t] = {"side": side, "lots": lots, "sl": sl, "tp": tp,
                                **({"limit": limit[0], "triggered": False} if limit else {})}
            self.burst_owned[t] = st["id"]
            st["orders"] += 1
        if st.get("local"):           # its orders kept across a restart (rapid_open.json)
            self._save_rapid_open()
        if fails or err:              # an order sent with no answer may be in the book all the same: with none
            # placed, a burst gives up only once a read shows none of its orders (or after ≈ 5 s of failed reads),
            # so one that did go through is followed (and cancelled with the rest on a burst_cancel)
            await self._burst_look(st, 1 if st["orders"] else self.BURST_MAX_ERRORS)
        if not st["orders"]:
            return ("cancelled", None) if st["cancel"] else ("error", f"no order was accepted: {err}")
        if fails >= 3:
            await self.report(f"{self._run_name(st)} #{st['id']}: stopped placing after 3 rejections ({err})", "warn")
        await self.burst_status(st, "armed")
        log.info("%s #%s armed: %s orders on %s", self._run_name(st), st["id"], st["orders"], sym)
        # the analysis only watched: what it would have done, beside the run (a Rapid run's ran before it placed)
        if not p["analysis"] and not st["analysed"]:
            self._burst_side(self._burst_shadow(st))
        return await self._burst_monitor(st)

    async def _burst_plan(self, st: dict) -> tuple[str | None, list]:
        """Pre-checks, then the orders to place: (reason to skip, []) or (None, [(side, lots, price, sl, tp, comment)]);
        a Rapid run's from _rapid_plan, each with its limit price last."""
        p, sym, b = st["p"], st["symbol"], self.broker
        if asyncio.get_running_loop().time() >= st["release"] + p["expire_seconds"]:
            return "too late: the release window has passed", []
        acct = await self.call(b.account)
        if not acct["demo"] and not self.allow_real:
            return "real account locked (allow_real_account = no)", []
        if st["test"] and not acct["demo"]:
            return "test bursts run on demo accounts only", []
        mode = acct.get("margin_mode") or "netting"
        if mode == "exchange":
            return "exchange accounts are not supported", []
        if any(o is not st and o["symbol"] == sym and o["status"] != "scheduled" for o in self.bursts.values()):
            return f"another news burst is running on {sym}", []
        st["netting"], st["demo"] = mode == "netting", bool(acct["demo"])
        # any position: a manual one, a signal's, or one an earlier burst left behind (the sweeper would close it
        # together with this burst's volume)
        if st["netting"]:
            try:
                held = await self.call(b.positions, all_magic=True)
            except Exception as e:    # unreadable is not flat
                return f"could not read the open positions ({e})", []
            if any(x["symbol"] == sym for x in held):
                return f"netting account: the open position on {sym} would merge with the burst", []
        if not self.dry_run and not await self.call(b.terminal_trading_enabled):
            return "Algo Trading is off in MetaTrader 5", []
        if st.get("kind") == RUN_KIND:    # Rapid: its own daily loss limit (rapid_room), not the long-term one
            return await self._rapid_plan(st, acct)
        if self.risk:
            equity = acct["equity"] - self.scalper.pnl_today() - self.rapid_pnl_today()     # as Connector.execute
            self.risk.update_day(utcnow().date(), equity)
            self.risk.check_daily_loss(equity)
            if self.risk.halted:
                return f"daily loss limit ({self.risk.daily_loss_pct}%) reached", []
        bid, ask, spread = await self.call(b.quote, sym)
        if bid is None:
            return f"no price for {sym}", []
        spec = await self.call(b.symbol_spec, sym)
        # every pip setting is in a currency pair's pips: × the symbol's scale (gold 5; 1 on the pairs of v1.2.0)
        k = spec.scale if spec.scale > 0 else 1.0
        cap = p["max_spread_pips"] * k
        # said as the user set it when not scaled (v1.2.0's wording), else 2 decimals
        wide = f" pips > {p['max_spread_pips'] if k == 1 else round(cap, 2)}"
        if spread > cap:
            return f"spread {spread:.1f}{wide}", []
        off_pips = p["offset_pips"] * k
        # a money stop is worked out below; params without the v1.3.0 keys (not clamped here) are v1.2.0's
        sl_pips = p["sl_pips"] * k if p.get("sl_unit", "pips") == "pips" else None
        if spec.min_stop_distance and min(off_pips, sl_pips or off_pips) * spec.pip < spec.min_stop_distance:
            return f"offset or stop-loss closer than the broker allows ({spec.min_stop_distance / spec.pip:.1f} pips)", []
        sides, half = ("buy", "sell"), False
        if p.get("analysis"):  # pre-news analysis: one side when it is clear, both at half the risk when not, or none
            d = await self._burst_view(st)
            log.info("News burst #%s: %s", st["id"], describe(d))
            await self._burst_analysis(st, d)
            if d["action"] == "skip":
                return describe(d), []
            sides, half = (d["action"],) if d["action"] in sides else sides, d["action"] == "both"
            # the bars took a moment: the straddle from the price now, and its spread within the cap too (spreads
            # widen in the seconds before a release)
            bid, ask, spread = await self.call(b.quote, sym)
            if bid is None:
                return f"no price for {sym}", []
            if spread > cap:
                return f"spread {spread:.1f}{wide}", []
        money = None
        if sl_pips is not None:
            n, lots = burst_lots(acct["balance"], p["risk_pct"] / 2 if half else p["risk_pct"], sl_pips, p["trades"],
                                 spec)
            if not n:
                return "risk budget too small for the minimum lot size", []
        else:
            why, n, lots, sl_pips, money = await self._burst_money(st, acct, spec, spread, sides, half)
            if why:
                return why, []
        st.update(spec=spec, n=n, lots=lots, sl_pips=sl_pips, tp=(p["tp_start_pips"] * k, p["tp_step_pips"] * k),
                  money=money, pre_mid=(bid + ask) / 2, sides=sides)
        off, sl_d, tag = off_pips * spec.pip, sl_pips * spec.pip, f"fxb:{st['id']}"
        buy, sell = ask + off, bid - off          # taken once: all orders form one straddle
        orders = []
        if st["netting"]:     # one position per symbol: one order per side, no TP, the connector exits the slices
            vols = split_volume(n * lots, spec)
            for j, v in enumerate(vols):
                part = str(j + 1) if len(vols) > 1 else ""
                orders += [("buy", v, buy, buy - sl_d, None, f"{tag}:b{part}"),
                           ("sell", v, sell, sell + sl_d, None, f"{tag}:s{part}")]
        else:                 # hedging: N tickets per side, interleaved so a late spike still finds both sides
            for i in range(n):
                d = (st["tp"][0] + i * st["tp"][1]) * spec.pip
                orders += [("buy", lots, buy, buy - sl_d, buy + d, f"{tag}:{i}"),
                           ("sell", lots, sell, sell + sl_d, sell - d, f"{tag}:{i}")]
        return None, [o for o in orders if o[0] in sides]    # one side: the analysis picked it

    async def _burst_money(self, st: dict, acct: dict, spec, spread: float, sides: tuple, half: bool) -> tuple:
        """A stop set in money (% of balance or $, bot/risk.py): (reason to skip, 0, 0, 0, 0) or (None, n, lots,
        sl_pips, money). The user's lots on each of `trades` orders per side, the stop as far as the money allows
        (between the closest one the broker and the spread allow and the largest the pip setting allows); half: the
        lots halved, the stop kept (the analysis was not clear). A side may take at most Scalper.MARGIN_USE of the
        free margin: the lots are cut to fit (risking less), the run skipped when not even the minimum lot fits."""
        p, sym, k = st["p"], st["symbol"], spec.scale if spec.scale > 0 else 1.0
        money = money_for(p["sl_unit"], p["sl_money"], acct["balance"])
        if not money:
            return "no money to risk: the balance is not above 0", 0, 0.0, 0.0, 0.0
        floor = stop_floor_pips(spec, spread, p["max_spread_pips"] * k)
        sl_pips, lots = money_stop(money, p["lots"], p["trades"], spec, floor, BURST_LIMITS["sl_pips"][1] * k)
        if not lots:
            if p["lots"] + 1e-9 < spec.volume_min:
                return f"{p['lots']} lots is under the minimum lot size of {sym} ({spec.volume_min})", 0, 0.0, 0.0, 0.0
            return (f"{money:.2f} at risk is too little for {p['trades']} × {spec.volume_min} lots with the closest "
                    f"stop allowed on {sym} ({floor:.1f} pips)"), 0, 0.0, 0.0, 0.0
        n = p["trades"]
        if half:
            n, lots = self._halved(n, lots, spec)
            money /= 2
            if not n:
                return "half the risk is under the minimum lot size", 0, 0.0, 0.0, 0.0
        if not st["netting"]:
            lots = min(lots, spec.volume_max)        # one ticket per order (netting splits what is over it instead)
        need = 0.0
        for side in sides:
            try:
                m = await self.call(self.broker.margin_for, sym, side, n * lots)
            except Exception as e:                   # not known: the broker refuses an order it cannot cover anyway
                log.debug("News burst #%s: no margin for %s (%s)", st["id"], sym, e)
                m = None
            if m is not None and math.isfinite(m):
                need = max(need, m)
        cap = (acct.get("free_margin") or 0.0) * Scalper.MARGIN_USE
        if need > cap:                               # as many lots as the margin allows (it risks less)
            fit = round(math.floor(lots * cap / need / spec.volume_step + 1e-9) * spec.volume_step, 8) \
                if cap > 0 else 0.0
            if fit < spec.volume_min:
                return f"not enough free margin for {n} × {spec.volume_min} lots of {sym}", 0, 0.0, 0.0, 0.0
            await self.report(f"News burst #{st['id']}: the free margin allows {fit} lots per order on {sym}, not "
                              f"{lots}: smaller orders", "warn")
            lots = fit
        return None, n, lots, sl_pips, money

    @staticmethod
    def _halved(n: int, lots: float, spec) -> tuple[int, float]:
        """Half the volume of n orders of `lots`: half the lots (DOWN to the step); under the minimum lot, fewer
        orders of the minimum lot instead (as burst_lots). (0, 0.0) when not even one fits."""
        half = round(math.floor(lots / 2 / spec.volume_step + 1e-9) * spec.volume_step, 8)
        if half >= spec.volume_min:
            return n, half
        k = math.floor(n * lots / 2 / spec.volume_min + 1e-9)
        return (k, spec.volume_min) if k >= 1 else (0, 0.0)

    async def _rapid_plan(self, st: dict, acct: dict) -> tuple[str | None, list]:
        """A Rapid run's orders (bot/rapid.py), after the checks every run makes: (reason to skip, []) or (None,
        [(side, lots, trigger, sl, tp, comment, limit)]): stop-limits, each with its stop and take-profit at the
        broker, both measured from its limit. The analysis always runs first, as its confidence c (|score|) sets the
        money each order risks and its take-profit in money; with its switch on it also picks the sides (one when it
        is clear, both at c when not, none on a skip), off it is only reported (shadow) and both sides are placed. The
        count per side is cut to what fits (_rapid_fit); netting places one order per side carrying them all (split
        only above volume_max), hedging one ticket per order. There is no plain-stop fallback. A local run (a short-term
        signal's) trades demo accounts only, has no analysis (its side and confidence are the signal's) and its
        comments are local_comment's."""
        p, sym, b = st["p"], st["symbol"], self.broker
        if st.get("local") and not acct["demo"]:
            return "Rapid signal runs trade demo accounts only", []
        if not await self.call(b.stop_limit_allowed, sym):
            return f"stop-limit orders are not allowed on {sym}", []
        spec = await self.call(b.symbol_spec, sym)
        k = spec.scale if spec.scale > 0 else 1.0
        cap = None if p["max_spread_pips"] is None else p["max_spread_pips"] * k
        if st.get("local"):
            sides, c = st["sides"], st["c"]
        else:
            d = await self._burst_view(st)
            log.info("%s #%s: %s", self._run_name(st), st["id"], describe(d, shadow=not p["analysis"]))
            await self._burst_analysis(st, d, shadow=not p["analysis"])
            sides = ("buy", "sell")
            if p["analysis"]:
                if d["action"] == "skip":
                    return describe(d), []
                sides = (d["action"],) if d["action"] in sides else sides
            c = confidence_from_score(d.get("score"))
        bid, ask, spread = await self.call(b.quote, sym)        # the bars took a moment: the price now
        if bid is None:
            return f"no price for {sym}", []
        if cap is not None and spread > cap:
            return f"spread {spread:.1f} pips > {round(cap, 2)}", []
        o = size_order(p, c, acct["balance"], spec, spread)
        if "skip" in o:
            return o["skip"], []
        legs = {}                     # side -> (trigger, sl, tp, limit), taken once: all orders form one straddle
        for side in sides:
            px = stop_limit_prices(side, bid, ask, spec, p["offset_pips"], p["limit_pips"])
            if isinstance(px, str):   # within the freeze level and spread, or the stops level
                return px, []
            sign = 1 if side == "buy" else -1
            legs[side] = (px[0], round(px[1] - sign * o["sl_pips"] * spec.pip, spec.digits),
                          round(px[1] + sign * o["tp_pips"] * spec.pip, spec.digits), px[1])
        n, why = await self._rapid_fit(st, acct, spec, sides, o)
        if not n:
            return why, []
        if o["tp_clamped"]:           # between the closest stop allowed and TP_MAX_SL stops
            log.info("%s #%s: take-profit of %.2f held to %s pips on %s lots", self._run_name(st), st["id"],
                     o["tp_usd"], o["tp_pips"], o["lots"])
        # a news run's comments are fxb:<id>, a local run's fxr:<id> (run_tag reads either back as the run's)
        tag, orders = f"fx{self._run_tag(st)[0][0]}:{st['id']}", []
        if st["netting"]:     # one position per symbol: one order per side, split only above volume_max
            vols = rapid_split(n * o["lots"], spec)
            for j, v in enumerate(vols):
                part = str(j + 1) if len(vols) > 1 else ""
                orders += [(s, v, *legs[s][:3], f"{tag}:{s[0]}{part}", legs[s][3]) for s in sides]
        else:                 # hedging: one ticket per order, interleaved so a late spike still finds both sides
            for i in range(n):
                orders += [(s, o["lots"], *legs[s][:3], f"{tag}:{i}", legs[s][3]) for s in sides]
        if st.get("local"):           # fxr:<id>:<i>, each order its own index (a comment too long is never cut)
            try:
                orders = [(*x[:5], local_comment(st["id"], i), x[6]) for i, x in enumerate(orders)]
            except ValueError as e:
                return f"invalid order comment ({e})", []
        st.update(spec=spec, n=n, lots=o["lots"], sl_pips=o["sl_pips"], tp=(o["tp_pips"], 0.0), money=n * o["money"],
                  pre_mid=(bid + ask) / 2, sides=sides, tp_usd=o["tp_usd"], c=c)
        return None, orders

    async def _rapid_fit(self, st: dict, acct: dict, spec, sides: tuple, o: dict) -> tuple[int, str | None]:
        """How many orders of size_order's `o` a Rapid run places per side (fit_orders): its `orders` cut to Rapid's
        daily limit (rapid_room), to the account's pending-order limit (as orders per side: hedging places one ticket
        per order and side, netting one per side and volume_max chunk of them all; not readable: no cap, the broker
        refuses what is over it) and to RAPID_MARGIN_USE of the free margin the other Rapid runs left (rapid_margin;
        a margin not known: no cap). (0, why the run is skipped) when not one fits; a cut is reported. What the run
        then holds is st["reserved"] (count × the money each order risks) and st["margin"] (one side's)."""
        p, sym, b = st["p"], st["symbol"], self.broker
        try:
            room = await self.call(b.pending_room)
        except Exception as e:
            log.debug("%s #%s: room for pending orders not known (%s)", self._run_name(st), st["id"], e)
            room = None
        if room is not None and st["netting"]:
            room = next((i for i in range(p["orders"], 0, -1)
                         if len(sides) * len(rapid_split(i * o["lots"], spec)) <= room), 0)
        elif room is not None:
            room //= len(sides)
        need = None
        for side in sides:
            try:
                m = await self.call(b.margin_for, sym, side, o["lots"])
            except Exception as e:    # not known: the broker refuses an order it cannot cover anyway
                log.debug("%s #%s: no margin for %s (%s)", self._run_name(st), st["id"], sym, e)
                m = None
            if m is not None and math.isfinite(m):
                need = max(need or 0.0, m)
        left = await self.rapid_room(st, acct)
        # from here to the reservation nothing is awaited: two runs starting together never take the same room
        free = (acct.get("free_margin") or 0.0) - self.rapid_margin(st)
        n, why = fit_orders(p["orders"], o["money"], left, room, need, free)
        if not n:
            return 0, {"daily limit": f"Rapid's daily loss limit leaves no room for an order risking {o['money']:.2f}",
                       "pending-order limit": "the account's pending-order limit leaves no room for an order",
                       "free margin": f"not enough free margin for one order of {o['lots']} lots on {sym}",
                       }.get(why, why)
        st.update(reserved=n * o["money"], margin=n * (need or 0.0))
        if why:
            await self.report(f"{self._run_name(st)} #{st['id']}: {n} of {p['orders']} orders per side ({why})", "warn")
        return n, None

    @staticmethod
    def _rapid_dry_run(st: dict, orders: list) -> str:
        """A Rapid run's dry-run line: each side's first stop-limit (trigger and limit), the lots, the stop and the
        take-profit in pips and what the take-profit makes per order, at the analysis' confidence."""
        spec, first = st["spec"], {o[0]: o for o in reversed(orders)}
        d, tp = spec.digits, st["tp"][0]
        return (f"[dry-run] Rapid run #{st['id']} would place {len(orders)} stop-limit orders on {st['symbol']}: "
                + " / ".join(f"{s}-stop-limit {first[s][2]:.{d}f} limit {first[s][6]:.{d}f}"
                             for s in ("buy", "sell") if s in first)
                + f", {st['n']} × {st['lots']} lots per side, SL {round(st['sl_pips'], 2)} pips, TP {round(tp, 2)} pips"
                + f" ({tp * st['lots'] * spec.pip_value_per_lot:.2f} per order), confidence {st['c']:.2f}")

    async def _burst_view(self, st: dict) -> dict:
        """The pre-news analysis of the burst's symbol (bot/news_view.py): its H1 bars (asked for with the plan, or
        now) and M1 bars, each waited for at most ANALYSIS_WAIT; bars not in by then, or not read, count as missing.
        The spread was checked before: within its cap (and _burst_plan checks it again on the price after the bars)."""
        if st["h1"] is None:
            st["h1"] = self._burst_side(self._burst_bars(st["symbol"], "H1", H1_BARS))
        m1 = self._burst_side(self._burst_bars(st["symbol"], "M1", M1_BARS))
        await asyncio.wait({st["h1"], m1}, timeout=self.ANALYSIS_WAIT)
        h1, m1 = (t.result() if t.done() and not t.cancelled() else None for t in (st["h1"], m1))
        try:
            return analyse(h1, m1, st["context"], True)
        except Exception as e:                       # analyse never raises on odd bars; this is a last guard
            log.debug("News burst #%s: analysis failed (%s)", st["id"], e)
            return decide_view(None, None, None, None, True, False)

    def _burst_side(self, coro) -> asyncio.Task:
        """`coro` run beside a burst, a reference kept until it ends (the loop keeps tasks only weakly)."""
        task = asyncio.create_task(coro)
        self.burst_tasks.add(task)
        task.add_done_callback(self.burst_tasks.discard)
        return task

    async def _burst_bars(self, sym: str, timeframe: str, count: int):
        """`count` bars of `sym` for the analysis, or None when they cannot be read: it does without them, the burst
        never fails on them."""
        try:
            return await self.call(self.broker.rates, sym, timeframe, count)
        except Exception as e:
            log.debug("News burst: no %s bars of %s (%s)", timeframe, sym, e)
            return None

    async def _burst_analysis(self, st: dict, d: dict, shadow: bool = False):
        """The analysis' answer to the server, once per burst: a burst_update with no status (it is only stored)."""
        if st["analysed"]:
            return
        st["analysed"] = True
        if st.get("local"):           # a local Rapid run has no Burst row to store it in
            return
        await self.send({"type": "burst_update", "id": st["id"], "analysis": {
            "action": d.get("action"), "score": d.get("score"), "reason": d.get("reason"), "parts": d.get("parts"),
            "shadow": shadow}})

    async def _burst_shadow(self, st: dict):
        """With its switch off the analysis is only watched: worked out once the orders are in, logged and reported as
        what it would have done. It never touches the run, and anything that goes wrong in it is a debug line only."""
        try:
            d = await self._burst_view(st)
            log.info("News burst #%s: %s", st["id"], describe(d, shadow=True))
            await self._burst_analysis(st, d, shadow=True)
        except Exception as e:
            log.debug("News burst #%s: watch-only analysis failed (%s)", st["id"], e)

    def _burst_strays(self, st: dict, book: list[dict]):
        """Orders of this burst (by their comment) it never got the ticket of, a placement with no answer that went
        through: followed as its own from now on, so the OCO cancels them, the sync gate counts their fills as an
        opposite stop and the positions they open are this burst's."""
        for o in book:
            if run_tag(o.get("comment")) == self._run_tag(st) and o["ticket"] not in st["placed"]:
                t = o["ticket"]
                st["placed"].add(t)
                st["pending"][t] = {"side": o["side"], "lots": o["lots"], "sl": o.get("sl"), "tp": o.get("tp")}
                if st.get("order_kind") == "stop_limit":    # its limit: the price it waits at once triggered
                    triggered = o.get("kind") == "limit"
                    st["pending"][t].update(limit=o["price"] if triggered else o.get("price_stoplimit"),
                                            triggered=triggered)
                self.burst_owned[t] = st["id"]
                st["orders"] += 1
                log.warning("News burst #%s: order #%s was placed with no answer, now followed", st["id"], t)

    async def _burst_look(self, st: dict, tries: int = 1) -> bool:
        """Reads the book for this burst's orders sent with no answer (_burst_strays), up to `tries` times
        BURST_POLL apart while the read fails. False if it never could."""
        for i in range(tries):
            try:
                self._burst_strays(st, await self.call(self.broker.pending_orders, st["symbol"]))
                return True
            except Exception as e:
                if i + 1 >= tries:
                    log.warning("News burst #%s: could not read its orders: %s", st["id"], e)
                    return False
                await asyncio.sleep(self.BURST_POLL)
        return False

    async def _burst_filled(self, st: dict) -> bool:
        """While placing: has an order placed so far left the book? A read error counts as no, and so does an empty
        book on a straddle (both of its sides never fill at once: a bad read). A run on one side (the analysis picked
        it) may fill every order placed so far in one move: an empty book is then a fill only when a position of the
        run, or the first order's deal, shows it. A stop-limit whose trigger was reached is still in the book (a limit
        now, on the same ticket): no fill."""
        try:
            live = {o["ticket"] for o in await self.call(self.broker.pending_orders, st["symbol"])}
        except Exception:
            return False
        if live or not st["pending"] or len(st["sides"]) > 1:
            return bool(live) and any(t not in live for t in st["pending"])
        try:
            if any(self._owns(st, x) for x in await self.call(self.broker.positions)):
                return True
            return await self.call(self.broker.deal_entry, next(iter(st["pending"]))) is not None
        except Exception:
            return False

    async def _burst_monitor(self, st: dict) -> tuple[str, str | None]:
        loop, errors = asyncio.get_running_loop(), 0
        while True:
            now = loop.time()
            if now >= st["release"] + st["p"]["expire_seconds"]:
                st["expired"] = True
            try:
                await self._burst_poll(st, now)
                errors = 0
            except Exception as e:     # a hiccup at news time must not end the burst; a dead terminal does
                errors += 1
                if errors >= self.BURST_MAX_ERRORS:
                    raise
                if errors == 1:
                    log.warning("News burst #%s: broker error (%s), retrying", st["id"], e)
            if not st["pending"] and not st["unknown"] and all(tr["closed"] for tr in st["trades"].values()) \
                    and (st["trades"] or st["expired"] or st["cancel"]):
                return ("cancelled" if st["cancel"] else "closed" if st["trades"] else "expired"), None
            await asyncio.sleep(self.BURST_POLL)

    async def _burst_poll(self, st: dict, now: float):
        b, sym, p, loop = self.broker, st["symbol"], st["p"], asyncio.get_running_loop()
        book = await self.call(b.pending_orders, sym)
        self._burst_strays(st, book)
        live = {o["ticket"] for o in book}
        open_pos = {x.get("identifier") or x["ticket"]: x for x in await self.call(b.positions) if self._owns(st, x)}
        hist = {}                     # position -> what its own deals add up to (position_net), read once a poll
        # 1. an order that left the book without our cancel filled (or the broker removed it). One that is back
        # was a bad read (some builds answer None for a moment): it is pending again, so cancels still reach it
        for t in [t for t in st["unknown"] if t in live]:
            st["pending"][t] = st["unknown"].pop(t)[0]
        for t in [t for t in st["pending"] if t not in live]:
            st["unknown"][t] = [st["pending"].pop(t), 0]
            st["to_cancel"].discard(t)
        if st["order_kind"] == "stop_limit":
            self._burst_triggered(st, book)
        sides, flash = [], {}
        for t, rec in list(st["unknown"].items()):
            deal = None
            if t not in open_pos or st["netting"]:    # netting: the fill may have merged into a position
                deal = await self.call(b.deal_entry, t)
                if deal is None:
                    # a stop-limit whose history says it left the book unfilled (a limit triggered but never reached,
                    # then cancelled or expired; or rejected) is no fill: dropped at once
                    if st["order_kind"] == "stop_limit" and await self._never_filled(st, t):
                        del st["unknown"][t]
                        continue
                    # history may lag, and at news time a triggered order may be executed late: asked again while
                    # this burst holds a position (a late fill still counts), else given up after BURST_GONE polls (a
                    # fill that came and went before a poll is known only from its deals)
                    rec[1] += 1
                    if rec[1] >= self.BURST_GONE and all(tr["closed"] for tr in st["trades"].values()):
                        del st["unknown"][t]
                    continue
            sides.append(rec[0]["side"])
            pid = (deal or {}).get("position") or t
            if deal and pid not in open_pos and pid not in st["trades"]:
                # not in the list of positions: opened and closed between two polls, or not listed yet (MT5 may add
                # the deal before the position, and a fill may land between our reads). Taken as come and gone only
                # once the position's own deals add up to nothing; until then the order is looked at again
                h = await self._position(hist, pid)
                if (h is None or h["side"]) and rec[1] < self.BURST_GONE:
                    rec[1] += 1
                    continue
                flash.setdefault(pid, ([], h))[0].append((rec[0], deal))
            del st["unknown"][t]
        sides += [x["side"] for pid, x in open_pos.items() if pid not in st["trades"]]
        # 2. OCO: the first fill cancels the other side at once
        if sides:
            old, side = st["side"], sides[0] if len(set(sides)) == 1 else "both"
            st["side"] = side if old in (None, side) else "both"
            if st["side"] != old:
                self._burst_doomed(st)
                await self._burst_cancels(st)
                await self.burst_status(st, "triggered")
        # 3. fills, then positions that are gone (TP / SL / our close)
        # tr["lots"]: every lot that entered the position. On netting it comes from the position's own deals, which
        # add up to the side and lots it holds: a split order (or the rest of a partial fill) that merged in, even
        # after slices went out, and a reversal by a stop on the other side (MT5 keeps the identifier; what is left
        # over is a new entry, its slices from 1). Slices are worked out only on a poll where those deals add up to
        # the position as read, and that show what left its current side as far as this runner knows (tr["out"]),
        # what this runner closed of it (tr["own"]: by volume, deals of this bot's magic only and fills of the burst's
        # own orders not counted, so neither an opposite stop nor a close by hand ever stands in for a close still
        # missing), its deals among them (by their orders) and what
        # a close with no answer may have closed (tr["unsure"]); after BURST_UNSURE only the volumes count.
        # Otherwise one of the two is behind (history may lag the list of positions, the list may lag our own deal,
        # or both may lag that deal together) and the position is left alone until they agree
        synced = set()
        for pid, x in open_pos.items():
            tr = st["trades"].get(pid)
            h = await self._position(hist, pid) if st["netting"] else None
            if not st["netting"] or h and h["side"] == x["side"] and abs(h["lots"] - x["lots"]) < 1e-6 and (
                    tr is None or tr["side"] != x["side"] or tr["flips"] not in (None, h["flips"])
                    or self._shows_closes(st, tr, h, now)):
                synced.add(pid)
            seen = h["entered"] if h and pid in synced else x["lots"]
            if tr is None:
                st["trades"][pid] = tr = self._new_trade(now, x["side"], seen, *((h["flips"], self._cuts(st, h))
                                                         if h and pid in synced else (None,)))
                self.burst_owned[x["ticket"]] = self.burst_owned[pid] = st["id"]
                await self._burst_opened(st, pid, x["side"], seen, x["entry_price"], x["sl"], x["tp"], tr)
                continue
            tr["gone"] = 0
            if st.get("kind") == RUN_KIND:      # what it shows: counted to Rapid's day once it is gone (_rapid_owed)
                tr["profit"] = x.get("profit")
            if not h or pid not in synced:
                continue
            if x["side"] != tr["side"] or tr["flips"] not in (None, h["flips"]):
                # a new entry (reversed, or started over): never a close decided for the side it replaced (that side's
                # price back through the pre-release mid is this one's move with the release); its own hold from now
                # (a v1.2.0 plan: X from the first fill, as v1.2.0 counted it)
                tr.update(side=x["side"], lots=0.0, done=0, out=0.0, own=0.0, exit=None)
                if not self._legacy(p):
                    tr.update(filled_at=now, fails=0, retry_at=0.0)
            # all that left this entry, ours or not (a deal that went through and then raised, an opposite stop), and
            # what of it was not the burst's own orders: a later read that shows less is behind
            out, own = self._cuts(st, h)
            tr.update(flips=h["flips"], out=max(tr["out"], out), own=max(tr["own"], own), unsure=0.0, mine=set())
            if seen > tr["lots"] + 1e-9:
                tr["lots"] = seen
                await self._burst_opened(st, pid, x["side"], seen, x["entry_price"], x["sl"], x["tp"], tr)
        for pid, (fills, h) in flash.items():
            # what entered first, at its average price, with the stops of an order on its side (an order of the
            # other side may have closed it: its deal is an exit)
            side, lots = (h["first_side"], h["first_entered"]) if h else (fills[0][1]["side"], fills[0][1]["lots"])
            o, deal = next((f for f in fills if f[0]["side"] == side), fills[0])
            # flips 0: it stands for the first entry, so a position reversed back to that side is caught up at its close
            st["trades"][pid] = tr = self._new_trade(now, side, lots, 0 if h else None)
            self.burst_owned[pid] = st["id"]
            await self._burst_opened(st, pid, side, lots, h and h["first_entry"] or deal["price"], o["sl"], o["tp"], tr,
                                     gone=True)
        for pid, tr in st["trades"].items():
            if tr["closed"] or pid in open_pos:
                continue
            if st.get("kind") == RUN_KIND:      # counted to Rapid's day as it showed last, until booked
                self._rapid_owed(pid, st["id"], tr.get("profit"))
            # not listed: closed once its own deals add up to nothing (a read may miss a position MT5 is still
            # adding), or after BURST_GONE polls without it
            h = await self._position(hist, pid)
            if (h is None or h["side"]) and tr["gone"] < self.BURST_GONE:
                tr["gone"] += 1
                continue
            tr["closed"] = True
            with self._owed(pid, (st, tr)):
                if h:
                    await self._burst_catch_up(st, tr, pid, h)
                # its deals say it is open, or are not in history at all: the sweeper closes it once listed, and
                # reports it once they show it closed
                if h is None or h["side"]:
                    self.unreported[pid] = (st, tr)
                    continue
                res = await self.report_closed(pid, self.BURST_RESULT_WAIT, self._burst_info(st, tr))
            st["pnl"] += (res and res["pnl"]) or 0.0
        # 4. cancel / expiry; rebuilt every poll, so failed cancels (frozen) and orders back from `unknown` are retried
        self._burst_doomed(st)
        if st["to_cancel"]:
            await self._burst_cancels(st)
        # 5. exits: hold time X / Y for every fill (bot/hold.py: in profit from X on, against the release while a Y
        # above X is set, at Y whatever it shows; the first answer is kept, the close retried until it goes through);
        # netting also closes the staggered slices itself
        quote = None
        for pid, tr in sorted(st["trades"].items(), key=lambda kv: kv[1]["filled_at"]):
            x = open_pos.get(pid)
            if x is None or tr["closed"] or now < tr["retry_at"]:
                continue
            # netting, reversed and its deals not showing it yet: the trade (and any exit decided) is still the side it
            # replaced, so it waits for them (step 3 then starts the new entry); past Y it goes all the same
            if x["side"] != tr["side"] and now - tr["filled_at"] < p["max_hold_seconds"] and not self._legacy(p):
                continue
            if not tr.get("exit") and st.get("way"):      # a long-term signal on the symbol goes first: out now
                tr["exit"] = "way"
            if not tr.get("exit"):
                against = False
                if st.get("local"):     # a signal's run: the Scalper's rule turned against it (Scalper.step)
                    against = bool(st.get("against"))
                elif p["max_hold_seconds"] > p["hold_seconds"]:    # Y = X (v1.2.0): the signal is not looked at
                    quote = quote or await self.call(b.quote, sym)
                    against = news_against(x["side"], quote[0], quote[1], st["pre_mid"])
                tr["exit"] = hold_action(now - tr["filled_at"], p["hold_seconds"], p["max_hold_seconds"],
                                         x.get("profit"), min_profit(st["spec"], x["lots"]), against)
            if tr.get("exit"):
                ok = (await self._burst_close(st, x, None, self._exit_text(p, tr["exit"], st.get("local"))))[0]
                self._burst_retry(tr, loop.time(), ok, 1.0)
            elif pid in synced and st["netting"] and st["kind"] == "burst":    # Rapid: its TP is at the broker
                quote = quote or await self.call(b.quote, sym)
                long = x["side"] == "buy"
                gain = ((quote[0] - x["entry_price"]) if long else (x["entry_price"] - quote[1])) / st["spec"].pip
                k = tr["done"]
                while k < st["n"] and gain + 1e-6 >= st["tp"][0] + k * st["tp"][1]:
                    k += 1
                # slices hit together go in deals of at most volume_max, each counted once it went through. A deal
                # closes what the live position holds beyond the slices still due (slices 1..j out leave
                # tr["lots"] - j x lots), so one that went through only in part (10010, then the rest refused)
                # is finished by the next deal, never sent in full again, and an entry merged late (even after
                # the last target) goes out at once. The position is looked at again only once a read can show
                # the deal: BURST_SETTLE from when it returned
                per = max(1, math.floor(st["spec"].volume_max / st["lots"] + 1e-9))
                while True:
                    j = min(k, tr["done"] + per)
                    vol = round(x["lots"] - max(tr["lots"] - j * st["lots"], 0.0), 8)
                    if vol <= st["spec"].volume_step / 2:      # already out: a deal that failed after it filled
                        if j == tr["done"]:
                            break
                        tr["done"] = j
                        continue
                    full = vol >= x["lots"] - st["spec"].volume_step / 2
                    ok, orders, done, unsure = await self._burst_close(st, x, None if full else vol,
                                                                       f"target {j}/{st['n']}")
                    self._burst_retry(tr, loop.time(), ok, 1.0 if full else self.BURST_SETTLE)
                    tr["mine"].update(orders)               # the next slice waits for the history to show these
                    tr["check_until"] = loop.time() + self.BURST_UNSURE
                    if not ok:                              # what went through before it raised counts as ours
                        tr["own"], tr["unsure"] = round(tr["own"] + done, 8), unsure
                        break
                    tr["done"], tr["own"], x = j, round(tr["own"] + vol, 8), {**x, "lots": round(x["lots"] - vol, 8)}
                    if full:
                        break

    @staticmethod
    def _burst_triggered(st: dict, book: list[dict]):
        """Stop-limits whose trigger was reached: a limit on the same ticket now (MT5 type 2 / 3), still pending until
        it fills at its limit (only a fill is one for the OCO) or is cancelled with the rest at expiry."""
        for o in book:
            rec = st["pending"].get(o["ticket"])
            if rec is not None and not rec.get("triggered") and (o.get("kind") == "limit" or o.get("type") in (2, 3)):
                rec["triggered"] = True
                log.info("%s #%s: order #%s triggered, a %s limit at %s now", Connector._run_name(st), st["id"],
                         o["ticket"], o["side"], o["price"])

    async def _never_filled(self, st: dict, t: int) -> bool:
        """Order `t`, gone from the book with no deal: True when its history shows it left unfilled (cancelled, expired
        or rejected, nothing of it filled). Not known (history behind the book, a failed read) or filled: False, it
        is looked up again."""
        try:
            s = await self.call(self.broker.order_state, t)
        except Exception:
            return False
        if not s or s.get("state") not in ("canceled", "expired", "rejected") \
                or (s.get("volume_current") or 0.0) + 1e-9 < (s.get("volume_initial") or 0.0):
            return False
        log.info("%s #%s: order #%s %s without a fill", self._run_name(st), st["id"], t, s["state"])
        return True

    async def _position(self, seen: dict, pid: int) -> dict | None:
        """position_net of `pid` from its deal history, read once a poll (`seen`); None while the history shows no
        deal of it (or cannot be read)."""
        if pid not in seen:
            try:
                deals = await self.call(self.broker.position_deals, pid)
            except Exception:
                deals = None
            seen[pid] = position_net(deals) if deals else None
        return seen[pid]

    @staticmethod
    def _shows_closes(st: dict, tr: dict, h: dict, now: float) -> bool:
        """Does the history show what left the trade's current side as far as this runner knows (a read that shows
        less is behind) and what this runner closed of it: as much volume of this bot's closes (not fills of the
        burst's own orders, nor a close by hand), its deals (by their orders) and what a close with no answer may have
        closed? Past check_until only
        the volumes count: a close with no answer that still does not show is taken as not gone through."""
        out, own = (v + 1e-6 for v in Connector._cuts(st, h))
        if out < tr["out"] or own < tr["own"]:              # a read that shows less than one before it is behind
            return False
        return now >= tr["check_until"] or own >= tr["own"] + tr["unsure"] and tr["mine"] <= h["orders"]

    @staticmethod
    def _cuts(st: dict, h: dict) -> tuple[float, float]:
        """What left the current side of a position by its deals (h): in all, and by this bot's own closes (not
        fills of the burst's own orders, an opposite stop, nor a close by hand or by another EA)."""
        out = round(h["entered"] - h["lots"], 8)
        return out, round(out - sum(v for o, v in h["cut"].items() if o in st["placed"] or o in h["foreign"]), 8)

    @staticmethod
    def _new_trade(now: float, side: str, lots: float, flips: int | None, cuts: tuple = (0.0, 0.0)) -> dict:
        """A fill this runner follows: lots that entered its current side, how often its position had started over
        (None: not known yet), what left that side (out) and what of it this bot's own closes took (own: not fills of
        the burst's own orders, nor a close by hand or by another EA)."""
        return {"filled_at": now, "opened_at": utcnow().isoformat(), "lots": lots, "side": side, "flips": flips,
                "done": 0, "out": cuts[0], "own": cuts[1], "closed": False, "gone": 0, "retry_at": 0.0, "fails": 0,
                "unsure": 0.0, "check_until": 0.0, "mine": set(), "exit": None}     # exit: hold_action's, latched

    @staticmethod
    def _burst_info(st: dict, tr: dict) -> dict:
        """What a burst trade's trade_closed carries: the server books the trade from it if it never got its
        trade_opened (as last reported)."""
        return {**Connector._run_ref(st), "symbol": st["symbol"], "side": tr["side"], "lots": tr["lots"],
                "entry_price": tr.get("entry"), "demo": st["demo"], "opened_at": tr["opened_at"]}

    async def _burst_catch_up(self, st: dict, tr: dict, pid: int, h: dict):
        """Before its close is reported: a trade grown or reversed with no poll in between that agreed with it is
        reported as its deals show it (its last entry, at its average price)."""
        if h["last_side"] != tr["side"] or tr["flips"] not in (None, h["flips"]):
            tr.update(side=h["last_side"], lots=0.0, sl=None, tp=None)
        tr["flips"] = h["flips"]
        if h["entered"] > tr["lots"] + 1e-9:
            tr["lots"] = h["entered"]
            await self._burst_opened(st, pid, tr["side"], h["entered"], h["entry"] or tr["entry"], tr["sl"], tr["tp"],
                                     tr)

    async def _report_when_closed(self, pid: int, ctx: tuple | None = None, info: dict | None = None) -> dict | None:
        """Reports a position this connector just closed once its deals add up to nothing: until then the history
        may show only an earlier slice's exit, which is not its result. Not flat by then (or cut short): left to the
        sweeper. `info`: what the report carries when no runner's trade (ctx) stands for it."""
        with self._owed(pid, ctx, info):
            for _ in range(self.REPORT_TRIES):
                h = await self._position({}, pid)
                if h and not h["side"]:
                    if ctx:
                        await self._burst_catch_up(*ctx, pid, h)
                    return await self.report_closed(pid, self.BURST_RESULT_WAIT,
                                                    self._burst_info(*ctx) if ctx else self._closed_info(info, h))
                await asyncio.sleep(self.BURST_RESULT_WAIT)
        self.unreported[pid] = ctx
        if not ctx and info:
            self.orphan_info[pid] = info
        return None

    @contextlib.contextmanager
    def _owed(self, pid: int, ctx: tuple | None, info: dict | None = None):
        """The report of a closed burst position under way: cut short (a stop cancels it, MT5 or a read fails), it
        is handed to the sweeper, which reports it, never lost. `ctx`, `info`: as for _report_when_closed."""
        try:
            yield
        except BaseException:
            self.unreported[pid] = ctx
            if info and not ctx:
                self.orphan_info[pid] = info
            raise

    @staticmethod
    def _burst_retry(tr: dict, now: float, ok: bool, wait: float):
        """When to look at a fill again after a close: `wait` after one that went through; after a failed one 0.5 s,
        doubling up to 5 s (a lasting rejection must not be resent on every poll)."""
        tr["fails"] = 0 if ok else tr["fails"] + 1
        tr["retry_at"] = now + (wait if ok else min(0.5 * 2 ** (tr["fails"] - 1), 5.0))

    async def _burst_opened(self, st: dict, pid: int, side: str, lots: float, entry: float, sl, tp, tr: dict,
                            gone: bool = False):
        """trade_opened for a fill (or what it grew to). gone: a flash fill, opened and closed between two polls."""
        tr.update(entry=entry, sl=sl, tp=tp)        # what was reported, for a report brought up to date at its close
        d = st["spec"].digits
        await self.send({"type": "trade_opened", "trade": {
            "ticket": pid, **self._run_ref(st), "symbol": st["symbol"], "side": side, "lots": lots,
            "entry_price": entry, "sl": round(sl, d) if sl else None, "tp": round(tp, d) if tp else None,
            "demo": st["demo"], "opened_at": tr["opened_at"]}})
        if st.get("local"):           # its positions kept across a restart (rapid_open.json)
            self._save_rapid_open()
        # a money stop: a fill that slipped so far from its stop that it risks over RISK_SLACK times its share of the
        # money is closed at once (the stop is never moved), as a short-term trade (Scalper). A flash fill is gone
        # already (at its own stop or target): nothing to close, and no warning that it was
        if st.get("money") and sl and not gone and not tr["closed"] and not tr.get("exit") and st["n"] and st["lots"]:
            share = st["money"] * lots / (st["n"] * st["lots"])
            risk = abs(entry - sl) / st["spec"].pip * st["spec"].pip_value_per_lot * lots
            if risk > share * Scalper.RISK_SLACK:
                tr["exit"] = "slip"
                await self.report(f"{self._run_name(st)} #{st['id']}: #{pid} filled at {entry:.{d}f}, too far from its "
                                  f"stop ({risk:.2f} at risk, {share:.2f} allowed): closed at once", "warn")

    @staticmethod
    def _legacy(p: dict) -> bool:
        """A plan run as v1.2.0 ran it: the stop in pips, no analysis, Y = X (a plan without the v1.3.0 keys too).
        Never a Rapid plan."""
        return p.get("kind") != RUN_KIND and p["sl_unit"] == "pips" and not p["analysis"] \
            and p["max_hold_seconds"] <= p["hold_seconds"]

    @staticmethod
    def _exit_text(p: dict, why: str, local: bool = False) -> str:
        """The reason a fill is closed (hold_action's answer, "slip", or "way": a long-term signal goes first), for
        the log. local: a signal's Rapid run (it turns against the signal, not a release)."""
        if why == "slip":
            return "filled too far from its stop"
        if why == "way":
            return "a long-term signal on the same symbol"
        if p["max_hold_seconds"] <= p["hold_seconds"]:
            return "hold time"                      # Y = X: the close of v1.2.0, at X in profit or not
        return {"profit": f"in profit after {p['hold_seconds']}s",
                "against": "turned against the signal" if local else "turned against the release"}.get(
            why, f"max hold {p['max_hold_seconds']}s")

    @staticmethod
    def _burst_doomed(st: dict):
        """Pending orders that must go: all of them after a cancel, expiry or whipsaw, else the side that did not fill."""
        if st["cancel"] or st["expired"] or st["side"] == "both":
            st["to_cancel"].update(st["pending"])
        elif st["side"]:
            st["to_cancel"].update(t for t, o in st["pending"].items() if o["side"] != st["side"])

    async def _burst_cancels(self, st: dict):
        for t in sorted(st["to_cancel"]):
            if t not in st["pending"]:
                st["to_cancel"].discard(t)
                continue
            try:
                removed = await self.call(self.broker.cancel_order, t)
            except Exception as e:     # e.g. 10029 frozen (price inside the freeze level): retried next poll
                if t not in st["warned"]:
                    st["warned"].add(t)
                    log.warning("News burst #%s: could not cancel order #%s (%s), retrying", st["id"], t, e)
                continue
            st["to_cancel"].discard(t)
            if removed:                # False: it filled meanwhile, the next poll picks the fill up
                st["pending"].pop(t, None)

    async def _burst_close(self, st: dict, pos: dict, volume: float | None,
                           why: str) -> tuple[bool, list, float, float]:
        """(went through, the orders of its deals, what it closed before it raised, what it may have closed unseen:
        a deal sent with no answer)"""
        try:
            orders = await self.call(self.broker.close_position, pos, "fxb exit", volume=volume)
        except Exception as e:         # requote / frozen at news time: retried next poll
            t, warn = pos["ticket"], True
            if getattr(e, "retcode", None) == 10029 and not self._legacy(st["p"]):
                # frozen (inside the freeze level): the broker's own TP / SL is about to fire, so a quiet retry, warned
                # about only once it stayed frozen BURST_FROZEN tries (a v1.2.0 plan warns at once, as v1.2.0 did)
                st["frozen"][t] = st["frozen"].get(t, 0) + 1
                warn = st["frozen"][t] >= self.BURST_FROZEN
                log.debug("News burst #%s: #%s frozen, close retried", st["id"], t)
            if warn and t not in st["warned"]:
                st["warned"].add(t)
                await self.report(f"{self._run_name(st)} #{st['id']}: could not close #{t} ({e}), retrying", "warn")
            unsure = getattr(e, "unsure", None)
            if unsure is None:         # not MT5Broker's: one with no retcode may have gone through
                unsure = 0.0 if getattr(e, "retcode", None) is not None else float(volume or pos["lots"])
            return False, list(getattr(e, "orders", None) or ()), float(getattr(e, "done", 0.0) or 0.0), unsure
        log.info("News burst #%s: closed %s lots of #%s (%s)", st["id"], volume or pos["lots"], pos["ticket"], why)
        return True, list(orders or ()), 0.0, 0.0

    async def _burst_adopt(self, st: dict, pid: int, x: dict):
        """A position of this burst its runner never took up (it crashed first): reported as a trade before it is
        closed, as its deals show it when they add up to it, so its close is reported too."""
        h = await self._position({}, pid)
        agree = h and h["side"] == x["side"] and abs(h["lots"] - x["lots"]) < 1e-6
        st["trades"][pid] = tr = self._new_trade(0.0, x["side"], h["entered"] if agree else x["lots"],
                                                 h["flips"] if agree else None)
        st["side"] = st["side"] or x["side"]
        self.burst_owned[x["ticket"]] = self.burst_owned[pid] = st["id"]
        await self._burst_opened(st, pid, x["side"], tr["lots"], agree and h["entry"] or x["entry_price"], x["sl"],
                                 x["tp"], tr)

    async def _burst_cleanup(self, st: dict):
        """After a crash: cancel this burst's orders, close its positions (the sweeper retries what fails here), and
        take up the fills its runner never saw, also one already closed again (its SL/TP hit while the terminal did
        not answer): the orders that left the book are looked up in history, here or by the sweeper (once this
        clean-up is over). Orders sent with no answer that no read has shown yet are looked for first."""
        await self._burst_look(st)
        recs = {**st["pending"], **{t: rec for t, (rec, _) in st["unknown"].items()}}
        left = set(st["unknown"])           # filled (or removed by the broker) before the runner looked them up
        for t in list(st["pending"]):
            try:
                if not await self.call(self.broker.cancel_order, t):
                    left.add(t)
            except Exception as e:          # still in the book (the sweeper cancels it) or the terminal is gone
                log.warning("News burst #%s: could not cancel order #%s: %s", st["id"], t, e)
                left.add(t)
        failed = None
        try:
            held = await self.call(self.broker.positions)
        except Exception as e:
            held, failed = [], e
        for x in held:              # each on its own: one refused close must not leave the others open
            if not self._owns(st, x):
                continue
            try:
                if (x.get("identifier") or x["ticket"]) not in st["trades"]:
                    await self._burst_adopt(st, x.get("identifier") or x["ticket"], x)
                await self.call(self.broker.close_position, x, "fxb exit")
            except Exception as e:
                log.warning("News burst #%s: could not close position #%s: %s", st["id"], x["ticket"], e)
                failed = e
        if failed is not None:      # unreadable or not closed: the sweeper closes what is left and reports it
            log.warning("News burst #%s: clean-up incomplete: %s", st["id"], failed)
            self.unreported.update({pid: (st, tr) for pid, tr in st["trades"].items() if not tr["closed"]})
            self.unchecked.update({t: [st, recs[t], 0] for t in left})
            return
        listed = {x["ticket"] for x in held} | {x.get("identifier") for x in held}
        for t in left:
            try:
                # no fill in history yet: the sweeper looks again (a stop-limit whose history shows it left unfilled,
                # a limit triggered but never reached, has nothing to look for)
                if await self._burst_take_up(st, t, recs[t], listed) is None and not (
                        st.get("order_kind") == "stop_limit" and await self._never_filled(st, t)):
                    self.unchecked[t] = [st, recs[t], 0]
            except Exception as e:
                log.warning("News burst #%s: could not look up order #%s: %s", st["id"], t, e)
                self.unchecked[t] = [st, recs[t], 0]
        try:
            for pid, tr in list(st["trades"].items()):
                if tr["closed"]:
                    continue
                tr["closed"] = True
                if st.get("kind") == RUN_KIND:
                    self._rapid_owed(pid, st["id"], tr.get("profit"))
                try:
                    res = await self._report_when_closed(pid, (st, tr))
                except Exception as e:      # handed to the sweeper: the others are reported all the same
                    log.warning("News burst #%s: could not report the result of #%s: %s", st["id"], pid, e)
                    continue
                st["pnl"] += (res and res["pnl"]) or 0.0
        except BaseException:               # cut short (the connector stops): the sweeper reports what is left
            self.unreported.update({pid: (st, tr) for pid, tr in st["trades"].items() if not tr["closed"]})
            raise

    async def _burst_take_up(self, st: dict, t: int, rec: dict, listed: set) -> int | None:
        """Order `t` of a burst whose runner is gone left the book: a fill of it into a position no trade of the
        burst stands for and that is not listed (it came and went unseen) is taken up as the runner's flash path
        does, reported as opened (what entered first). Its pid, 0 when there is nothing to take up, None while the
        history does not show the order filled."""
        deal = await self.call(self.broker.deal_entry, t)
        if deal is None:
            return None
        pid = deal.get("position") or t
        if pid in st["trades"] or pid in listed:      # followed already, or closed and reported once listed
            return 0
        h = await self._position({}, pid)
        side, lots = (h["first_side"], h["first_entered"]) if h else (deal["side"], deal["lots"])
        st["trades"][pid] = tr = self._new_trade(0.0, side, lots, 0 if h else None)
        st["side"] = side if st["side"] in (None, side) else "both"
        self.burst_owned[pid] = st["id"]
        sl, tp = (rec.get("sl"), rec.get("tp")) if rec.get("side") == side else (None, None)
        await self._burst_opened(st, pid, side, lots, h and h["first_entry"] or deal["price"], sl, tp, tr, gone=True)
        return pid

    @staticmethod
    def _closed_info(info: dict | None, h: dict | None) -> dict | None:
        """An orphan's report info as its history shows its last entry once flat (it may have grown or reversed
        after the info was taken)."""
        if not info or not h:
            return info
        return {**info, "side": h["last_side"] or info["side"], "lots": h["entered"] or info["lots"],
                "entry_price": h["entry"] or info["entry_price"]}

    # ── orphan sweeper ─────────────────────────────────────────
    def _orphan(self, x: dict) -> bool:
        bid = self.burst_id(x)
        return bid is not None and bid not in self.bursts

    def _rapid_forget(self, book: list[dict], held: list[dict]):
        """rapid_news drops each Rapid news run nothing is left of: not running, none of its orders in `book` nor of
        its positions in `held` (as this sweep read them), no report of it owed (unreported, unchecked, orphan_info).
        rapid_open.json follows at the end of the sweep."""
        if not self.rapid_news:
            return
        left = {self.burst_id(x) for x in [*book, *held]} | set(self.bursts) \
            | {c[0]["id"] for c in self.unreported.values() if c} | {r[0]["id"] for r in self.unchecked.values()} \
            | {i.get("burst_id") for i in self.orphan_info.values()}
        self.rapid_news &= left

    async def sweep_orphans(self) -> int:
        """Burst orders/positions whose runner is gone (crash, restart, a close that kept failing) are removed.
        How many of them are still there (not removed this time). One sweep at a time (sweep_loop's and a stop's):
        two would close the same position and report it twice. A Rapid run's (a local run's fxr:, a news run's of
        rapid_news) are booked to Rapid's day as reported, and rapid_open.json forgets what is over."""
        async with self.sweep_lock:
            try:
                return await self._sweep_orphans()
            finally:
                self._save_rapid_open()

    async def _sweep_orphans(self) -> int:
        b = self.broker
        book = await self.call(b.pending_orders)
        orders = [o for o in book if self._orphan(o)]
        everything = await self.call(b.positions)
        self._rapid_forget(book, everything)
        positions = [x for x in everything if self._orphan(x)]
        held = {x["ticket"] for x in everything} | {x.get("identifier") for x in everything}
        live = {o["ticket"] for o in book}
        for t, rec in list(self.unchecked.items()):     # a crashed burst's orders: a fill its clean-up missed
            if self.bursts.get(rec[0]["id"]) is rec[0]:  # its clean-up is still reporting: looked at once it is over
                continue
            st, order, rec[2] = rec[0], rec[1], rec[2] + 1
            try:
                pid = None if t in live else await self._burst_take_up(st, t, order, held)
            except Exception as e:                      # history unreadable: asked again next sweep
                log.warning("News-burst clean-up: could not look up order #%s: %s", t, e)
                pid = None
            if pid:                                     # reported once its history shows it closed (below)
                st["trades"][pid]["closed"] = True
                self.unreported[pid] = (st, st["trades"][pid])
                if st.get("kind") == RUN_KIND:
                    self._rapid_owed(pid, st["id"])
            if pid is not None or rec[2] >= self.SWEEP_WAITS:
                del self.unchecked[t]
        for pid in [pid for pid in self.unreported if pid not in held]:     # closed by its SL/TP meanwhile
            h = await self._position({}, pid)
            waits = self.unreported_waits[pid] = self.unreported_waits.get(pid, 0) + 1
            if (h is None or h["side"]) and waits < self.SWEEP_WAITS:       # history not showing the close yet
                continue
            ctx, info = self.unreported.pop(pid), self.orphan_info.pop(pid, None)
            self.unreported_waits.pop(pid, None)
            with self._owed(pid, ctx, info):    # cut short (the stop's time is up, MT5 fails): back for the next sweep
                if ctx and h:             # what entered after the history its runner handed it over with
                    await self._burst_catch_up(*ctx, pid, h)
                await self.report_closed(pid, self.BURST_RESULT_WAIT,
                                         self._burst_info(*ctx) if ctx else self._closed_info(info, h))
        if not orders and not positions:
            return 0
        if self.dry_run:
            whose = self._swept_runs({self.strategy_of(x) for x in orders + positions})
            await self.report(f"[dry-run] would remove {len(orders)} order(s) and close {len(positions)} "
                              f"position(s) left by {whose}", "warn")
            return len(orders) + len(positions)
        removed = closed = 0
        swept = set()                   # the strategies of what was removed or closed: the message names them
        for o in orders:
            try:
                await self.call(b.cancel_order, o["ticket"])
                removed += 1
                swept.add(self.strategy_of(o))
            except Exception as e:
                log.warning("Could not cancel news-burst order #%s: %s", o["ticket"], e)
        for x in positions:
            pid = x.get("identifier") or x["ticket"]
            # no runner's trade stands for it (a restart, a crash before its runner took it up): its report says
            # what the server books it as if its trade_opened never arrived
            info = None if self.unreported.get(pid) else await self._orphan_info(pid, x)
            # handed over before the close is sent: a close that raises may have gone through all the same, or its
            # SL may close it meanwhile, and the stop's time may cut this sweep while MT5 is in the call; reported
            # once its history shows it closed (a close really refused leaves it listed, and the next sweep retries)
            self.unreported.setdefault(pid, None)
            if info:
                self.orphan_info[pid] = info
            try:
                await self.call(b.close_position, x, "fxb cleanup")
            except Exception as e:
                log.warning("Could not close news-burst position #%s: %s", x["ticket"], e)
                continue
            closed += 1
            kind = self.strategy_of(x)
            swept.add(kind)
            if kind == RUN_KIND:                    # counted to Rapid's day as it showed, until booked
                self._rapid_owed(pid, self.burst_id(x), x.get("profit"))
            self.unreported_waits.pop(pid, None)
            self.orphan_info.pop(pid, None)
            await self._report_when_closed(pid, self.unreported.pop(pid, None), info)
        if removed or closed:     # what failed is in the log; the next sweep tries again
            await self.report(f"Cleaned up after {self._swept_runs(swept)}: removed {removed} pending order(s), "
                              f"closed {closed} position(s)", "warn")
        return len(orders) - removed + len(positions) - closed

    @staticmethod
    def _swept_runs(kinds: set[str]) -> str:
        """Whose orders / positions a sweep cleaned up (`kinds`: their strategy_of), for its message: "an earlier news
        burst" (the classic burst's: the words up to v1.2.0), "an earlier Rapid run" (a local run's fxr:, or a news
        run's of a Rapid runner), or both."""
        if RUN_KIND not in kinds:
            return "an earlier news burst"
        return "an earlier Rapid run" if kinds == {RUN_KIND} else "an earlier news burst and Rapid run"

    async def _orphan_info(self, pid: int, x: dict) -> dict:
        """What the report of an orphan with no runner's trade carries: its burst and its current entry as its deals
        show it (as listed when they show none); a Rapid run's (strategy_of: a local run's fxr:, a news run's of
        rapid_news) its strategy too, so the server books it as Rapid's (a local run's with no Burst row), and
        report_closed into Rapid's day."""
        h = await self._position({}, pid)
        rapid = self.strategy_of(x) == RUN_KIND
        return {"burst_id": self.burst_id(x), **({"strategy": RUN_KIND} if rapid else {}), "symbol": x["symbol"],
                "side": h and h["last_side"] or x["side"],
                "lots": h and h["entered"] or x["lots"], "entry_price": h and h["entry"] or x["entry_price"],
                "demo": self.demo, "opened_at": x.get("open_time")}

    async def sweep_loop(self):
        while True:
            await asyncio.sleep(self.SWEEP_EVERY)
            try:
                await self.sweep_orphans()
            except Exception as e:
                log.warning("News-burst clean-up failed: %s", e)


# ── short-term (scalping) ──────────────────────────────────────
class DayBook:
    """A strategy's result for the day (UTC), against its own daily loss limit, kept across restarts in a json file
    (short-term: scalp_day.json): the day, the balance it started from, what was booked to it, whether reaching the
    limit was told, and the losses counted for results that could not be read (by ticket). `limit`: the daily loss
    limit in money from the day's starting balance (0: none). `tickets` (Rapid's): the tickets booked by their
    result are kept with the day too ("booked", in the same write as the P&L), so a trade reported just before a
    crash is not booked, nor reported, again after the restart (booked_tickets)."""

    def __init__(self, path: Path, limit, tickets: bool = False):
        self.path, self.limit, self.tickets = path, limit, tickets
        self.day = self.load()

    def _new(self, day: str, start: float) -> dict:
        d = {"day": day, "start": start, "pnl": 0.0, "warned": False, "guessed": {}}
        if self.tickets:
            d["booked"] = []
        return d

    def load(self) -> dict:
        try:
            d = json.loads(self.path.read_text())
            got = {"day": str(d["day"]), "start": float(d["start"]), "pnl": float(d["pnl"]),
                   "warned": bool(d.get("warned")),
                   "guessed": {str(t): float(v) for t, v in (d.get("guessed") or {}).items()}}
            if self.tickets:
                got["booked"] = [int(t) for t in d.get("booked") or []]
            return got
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            return self._new("", 0.0)

    def save(self):
        Scalper._write(self.path, self.day)

    def roll(self, balance: float, day: str | None = None):
        """A day other than the one kept (today by default) starts over, from `balance`."""
        day = day or utcnow().date().isoformat()
        if self.day["day"] != day:
            self.day = self._new(day, float(balance))
            self.save()

    def today(self) -> float:
        """What is booked to today (UTC): nothing while the day kept is an earlier one."""
        return self.day["pnl"] if self.day["day"] == utcnow().date().isoformat() else 0.0

    def free(self, at_risk: float) -> float | None:
        """What is still free of the day's limit once `at_risk` (what is open, or closed and not booked yet) is
        counted too: None, no limit."""
        lim = self.limit(self.day["start"])
        if lim <= 0:
            return None
        return lim + self.day["pnl"] - at_risk

    def reached(self) -> bool:
        return -self.day["pnl"] >= self.limit(self.day["start"])

    @staticmethod
    def day_of(closed_at: str | None) -> str:
        """The day (UTC) a result that closed at `closed_at` (None: now) counts to: never one after today."""
        today = utcnow().date().isoformat()
        return min(str(closed_at or today)[:10], today)

    def book(self, day: str, pnl: float, ticket: int, balance: float | None = None, guess: bool = False) -> float:
        """Adds a result to `day` (day_of): the day kept, or a later one, which starts from `balance` (the balance
        before it; None: as the day kept ended, when that is known). One of an earlier day is not booked: that day is
        gone. `guess`: the loss counted for a result that could not be read, kept for the day by ticket: the same
        ticket booked again that day (MT5 had only left it out of its list: still open, it was closed and reported
        later) replaces it. Returns what it took back of an earlier guess."""
        if day > self.day["day"]:
            if balance is None and self.day["start"] > 0:
                balance = self.day["start"] + self.day["pnl"]
            if balance is not None:
                self.roll(balance, day)
        if day != self.day["day"]:
            return 0.0
        guessed = self.day.setdefault("guessed", {})
        took = guessed.pop(str(ticket), 0.0)
        if guess:
            guessed[str(ticket)] = -pnl
        elif self.tickets and ticket not in self.day.setdefault("booked", []):
            self.day["booked"].append(ticket)
        self.day["pnl"] = round(self.day["pnl"] + pnl + took, 2)
        if took and not self.reached():
            self.day["warned"] = False          # under the limit again: told again if it is reached
        self.save()
        return took

    def booked_tickets(self) -> dict[int, bool]:
        """The tickets booked to the day kept (`tickets`): True by their result, False by a guess so far."""
        return {**{int(t): False for t in self.day.get("guessed") or {}},
                **{int(t): True for t in self.day.get("booked") or []}}


class Scalper:
    """The short-term strategy: many quick trades, each closed in profit from `hold_seconds` (X) on and at
    `max_hold_seconds` (Y) at the latest (holds(); with no Y set, at exactly X as up to v1.2.0). It decides here,
    once per second, from this broker's own ticks (one-second bars, as its rule was tested), so nothing has to come
    from the server in time. Long-term signals and news bursts go first on a symbol (netting: one position per
    symbol): no scalp opens next to another position or shortly before a news burst checks its symbol, and a
    long-term signal closes the scalp in its way (Connector.execute). Its daily loss limit is its own: once the
    day's short-term losses reach it, only short-term stops, until the next day (UTC). What is still free of that
    limit also caps each trade's risk, counting what the open scalps risk at their stops."""
    STEP = 0.25                 # between two looks at the ticks and the open scalps (s)
    KEEP = KEEP_SECONDS         # seconds of one-second bars kept per symbol (its first look fetches as much)
    STALE = 60                  # a symbol whose last tick is older than this (s) is not traded (market shut, no feed)
    BURST_GAP = 15              # a scalp must be over this long (s) before a news burst checks its symbol
    MARGIN_USE = 0.5            # a scalp may take at most this share of the free margin
    STRAYS_EVERY = 10           # how often (s) positions are looked at for a scalp not followed (an open cut short)
    REPORT_TRIES = 10           # a closed scalp's result: history reads (REPORT_GAP apart) before it is given up
    REPORT_GAP = 3.0
    CLOCK_WAIT, CLOCK_STEP = 120.0, 5.0     # ... its close time unknown (MT5's clock not proven): waited for (s)
    FIRST_LOOK = 10.0           # a start: how long the first look at what the last run left open may hold it up (s)
    SPEC_EVERY = 3600           # a symbol's data (its pip, what the rule measures in) is read again after this (s)
    RISK_SLACK = 1.25           # filled so far from its stop that it risks over this many times its money: closed

    def __init__(self, conn: "Connector"):
        self.c = conn
        self.cfg = clamp_short(None)
        self.symbols: list[str] = []             # the pairs the server's engine trades (welcome), with no suffix
        self.bars: dict[str, SecondBars] = {}
        self.offsets: deque = deque()            # (local time, a fresh tick's stamp minus local time in ms), last min
        self.decided: dict[str, int] = {}        # symbol -> the last second the rule was asked about
        # ticket -> the scalp: what its reports carry, when it must close (those of the last run: at once)
        self.open: dict[int, dict] = {} if conn.paper else self._load_open()
        self.booking: dict[int, dict] = {}       # ticket -> a closed scalp whose result is not booked yet
        self.reports: set[asyncio.Task] = set()
        self.daybook = DayBook(self._day_file(), lambda start: daily_limit_money(self.cfg, start))
        self.task: asyncio.Task | None = None
        self.looked = asyncio.Event()            # the loop's first step is over
        self.said: dict[str, float] = {}         # a message's key -> when it was last logged (no flood)
        self.specs: dict[str, tuple] = {}        # symbol -> (when it was read, its SymbolSpec or None)
        self.paused = False                      # the server refused the session (subscription): no new scalp
        self.last_id = 0
        self.strays_at = 0.0

    def apply(self, s: dict):
        st = s.get("short_term")
        try:
            cfg = clamp_short(st)
        except ValueError as e:     # a value gone bad where it is stored: that one is the default ("false" is off)
            log.warning("Short-term settings: %s (the bad value is left out)", e)
            cfg = clamp_short(None, st)
        old, self.cfg = self.cfg, cfg
        if self.cfg == old:
            return
        c, (x, y) = self.cfg, self.holds()
        unit = "%" if c["risk_unit"] == "pct" else " (account currency)"
        log.info("Short-term: %s, risk %s-%s%s per trade, hold %s, at most %s open", "ON" if c["enabled"] else "off",
                 c["risk_min"], c["risk_max"], unit, f"{x}s" if y <= x else f"{x}s if in profit, {y}s at most",
                 c["max_open"])
        if c["enabled"] and self.c.paper:
            log.warning("Short-term needs MetaTrader 5: it does not trade in paper mode")

    def holds(self) -> tuple[int, int]:
        """(X, Y) in seconds: a scalp in profit is closed from X on, any scalp at Y (bot/hold.py). With no Y set (a
        user saved under v1.2.0) Y is X: every scalp closes at exactly X, as v1.2.0 closed it."""
        x = self.cfg["hold_seconds"]
        return x, max(x, self.cfg.get("max_hold_seconds") or x)

    def active(self) -> bool:
        """New scalps may open: switched on, execution on auto, MT5 (not paper), not stopping, the session not
        refused by the server (subscription inactive: until its next welcome)."""
        return bool(self.cfg["enabled"] and self.c.settings.get("execution") == "auto" and not self.c.paper
                    and not self.c.stopping and not self.paused)

    def watch(self) -> list[str]:
        return [s + self.c.suffix for s in (self.cfg["symbols"] or self.symbols)]

    def rapid_on(self) -> bool:
        """The rule's signals start local Rapid runs (Connector.start_local) instead of scalps: Rapid on with its
        signal trigger, short-term off (on, it keeps them: the server never leaves both on), execution on auto, MT5,
        not stopping, the session not refused."""
        r = self.c.rapid
        return bool(r["enabled"] and (r.get("triggers") or {}).get("signals") and not self.cfg["enabled"]
                    and self.c.settings.get("execution") == "auto" and not self.c.paper and not self.c.stopping
                    and not self.paused)

    def rapid_watch(self) -> list[str]:
        """The pairs whose signals start Rapid runs: Rapid's own list, else short-term's, else the engine's."""
        return [s + self.c.suffix for s in (self.c.rapid["symbols"] or self.cfg["symbols"] or self.symbols)]

    def _due(self, key: str, every: float) -> bool:
        now = time.monotonic()
        if now - self.said.get(key, -1e9) < every:
            return False
        self.said[key] = now
        return True

    def say(self, key: str, message: str, every: float, level: str = "warning"):
        """Logs `message` at most once per `every` seconds (a reason to skip repeats every second)."""
        if self._due(key, every):
            getattr(log, level)(message)

    async def tell(self, key: str, message: str, every: float):
        """say(), and the web app shows it too (the connector's log there): what the user may want to change."""
        if self._due(key, every):
            await self.c.report(message, "warn")

    # the day's short-term result, kept across restarts (DayBook)
    def _day_file(self) -> Path:
        return HERE / "scalp_day.json"

    @property
    def day(self) -> dict:
        return self.daybook.day

    def _load_day(self) -> dict:
        """What the day's file holds now."""
        return self.daybook.load()

    # the open scalps (and those closed, their result not booked yet), kept across restarts: one that closed while
    # the connector was away is still reported and booked to its day
    def _open_file(self) -> Path:
        return HERE / "scalp_open.json"

    def _load_open(self) -> dict:
        try:
            got = {int(t): {"id": int(s["id"]), "risk": float(s["risk"]), "info": dict(s["info"]), "close_at": 0.0,
                            "away": True} for t, s in json.loads(self._open_file().read_text()).items()}
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            return {}
        if got:
            log.warning("Short-term: %s trade(s) left by the last run: closed or reported now", len(got))
        return got

    def _save_open(self):
        self._write(self._open_file(), {t: {"id": s["id"], "risk": s["risk"], "info": s["info"]}
                                        for t, s in {**self.booking, **self.open}.items()})

    @staticmethod
    def _write(path: Path, data):
        tmp = path.with_name(path.name + ".tmp")
        try:
            tmp.write_text(json.dumps(data, default=str))
            os.replace(tmp, path)                        # whole or not at all, even if the process is killed mid-write
        except OSError:
            pass

    def _free(self) -> float | None:
        """What is still free of today's limit once the open scalps' risk is counted, and that of those closed whose
        result is not booked yet (one that closed while the connector was away may wait for MT5's clock): None, no
        limit."""
        return self.daybook.free(sum(s["risk"] for s in (*self.open.values(), *self.booking.values())))

    def pnl_today(self) -> float:
        """Today's short-term result (UTC): booked, plus what the open scalps (and those closed, not booked yet)
        showed at their last look. The long-term daily loss limit leaves it out: each has its own limit."""
        if self.c.paper:            # it never trades there (its day file is the MetaTrader 5 account's)
            return 0.0
        return self.daybook.today() + sum(s.get("profit") or 0.0 for s in (*self.open.values(), *self.booking.values()))

    # the loop
    def start(self):
        if not self.c.paper and self.task is None:
            self.task = asyncio.create_task(self.loop())

    async def first_look(self):
        """A start: waits for the loop's first step (FIRST_LOOK at most), which closes the scalps the last run left
        open and reports those that closed meanwhile."""
        if self.task:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.looked.wait(), self.FIRST_LOOK)

    def adopt(self, positions: list[dict]):
        """Scalps this connector is not following (left open by its last run, an open cut short): closed at once.
        One taken for closed by a read of the positions that left it out (MT5 may) is followed again, as it was;
        its report stands down (_report)."""
        back = [p["ticket"] for p in positions if p["ticket"] in self.booking]
        for t in back:
            self.open[t] = self.booking.pop(t)
        if back:
            self._save_open()
            log.info("Short-term: %s trade(s) taken for closed are still open: followed again", len(back))
        new = [p for p in positions if p["ticket"] not in self.open and self.c.scalp_id(p) is not None]   # by its tag
        for p in new:
            self.open[p["ticket"]] = {"id": self.c.scalp_id(p), "risk": 0.0, "close_at": 0.0,
                                      "info": {"strategy": "short", "symbol": p["symbol"], "side": p["side"],
                                               "lots": p["lots"], "entry_price": p["entry_price"], "sl": p.get("sl"),
                                               "tp": p.get("tp"), "demo": self.c.demo, "opened_at": p.get("open_time")}}
        if new:
            self._save_open()
            log.warning("Short-term: closing %s trade(s) it was not following (left by the last run?)", len(new))

    async def loop(self):
        while True:     # until stop(): stopping, no new scalp opens, but the open ones still close at their time
            try:
                await self.step()
            except Exception as e:      # MT5 busy or briefly away: the next step catches up
                self.say("step", f"Short-term: {type(e).__name__}: {e}", 60)
            self.looked.set()
            await asyncio.sleep(self.STEP)

    async def step(self):
        now = asyncio.get_running_loop().time()
        if self.open or self.booking or now >= self.strays_at:     # booking: one may be listed again (adopt)
            await self.manage(now)
        # the pairs of the scalps open that may be held past X (a Y above it): still read while no new scalp may open
        # (switched off, execution on manual) or once left out of the list, so the rule turning against one still
        # closes it; with Y = X none, as in v1.2.0
        held = {s["info"].get("symbol") for s in self.open.values() if s.get("max_hold", 0) > s.get("hold", 0)}
        held.discard(None)
        # the same for the local Rapid runs (a short-term signal's) held past X: the rule turning against one ends it
        runs = [st for st in self.c.bursts.values()
                if st.get("local") and st["p"]["max_hold_seconds"] > st["p"]["hold_seconds"]]
        held |= {st["symbol"] for st in runs}
        active = self.active()
        rapid = not active and self.rapid_on()      # the signals start Rapid runs instead of scalps
        if not (active or rapid):
            for sym in set(self.bars) | set(self.decided):
                if sym not in held:     # switched on again later: its bars start over (no gap inside them)
                    self.bars.pop(sym, None)
                    self.decided.pop(sym, None)
            if not held:
                return
        watch = self.watch() if active else self.rapid_watch() if rapid else []
        syms = watch + sorted(held - set(watch))
        got, at = await self.c.call(self._read, syms)
        for r in got.values():
            if isinstance(r, tuple) and r[0]:
                self.offsets.append((at, r[0][-1][0] - at * 1000))
        while self.offsets and self.offsets[0][0] < at - 60:
            self.offsets.popleft()
        if not self.offsets:            # no tick for a minute on any pair: the market is shut
            return
        # the broker's clock: a tick is never stamped after the moment it is read, so the largest gap seen is the
        # closest to the real one; the second just over is the one the rule is asked about
        sec = int((at * 1000 + max(o for _, o in self.offsets)) // 1000) - 1
        picks, cut = [], len(self.c.suffix)
        for sym in syms:
            r = got.get(sym)
            if isinstance(r, Exception):
                self.say(f"ticks:{sym}", f"Short-term: no ticks for {sym} ({r})", 300)
                continue
            bars = self.bars.setdefault(sym, SecondBars(self.KEEP))
            bars.add(r[0])
            if r[1] or not bars.bars or sec <= self.decided.get(sym, 0):    # r[1]: still catching up on ticks
                continue
            self.decided[sym] = sec
            done = bars.upto(sec)
            if not done or sec - done[-1][0] > self.STALE:
                continue
            spec = await self._spec(sym)    # its pip as this broker has it (gold, an index: not a currency pair's)
            if spec is None:
                continue
            # its stop is sized to the longest a scalp may be held: Y
            d = decide(sym[:len(sym) - cut], done, sec, spec.pip, self.holds()[1])
            # a scalp open here that may be held past X (a Y above it): has the rule turned against it since it
            # opened? (manage closes it then; with Y = X nothing is looked at, as in v1.2.0)
            mine = [s for s in self.open.values() if s["info"].get("symbol") == sym
                    and s.get("max_hold", 0) > s.get("hold", 0)]
            ours = [st for st in runs if st["symbol"] == sym]     # a local Rapid run's: its side against the signal's
            if mine or ours:
                now_stretch = stretch(sym[:len(sym) - cut], done, sec, spec.pip)
                for s in mine:
                    s["against"] = scalp_against(s["info"]["side"], s.get("stretch"), now_stretch,
                                                 d["side"] if d else None)
                for st in ours:
                    st["against"] = scalp_against(st["sides"][0], st.get("stretch"), now_stretch,
                                                  d["side"] if d else None)
            if d and sym in watch:      # a pair read only for a scalp open there: no new one
                picks.append((float(d["strength"]), sym, d))
        if rapid:                       # each signal a local Rapid run (start_local: one per symbol, demo only)
            for _, sym, d in sorted(picks, key=lambda x: -x[0]):
                await self.c.start_local(sym, d)
            return
        for _, sym, d in sorted(picks, key=lambda x: -x[0]):      # the surest first when only some fit
            if len(self.open) >= self.cfg["max_open"]:
                break
            await self.enter(sym, d)

    def _read(self, syms: list[str]):
        """(In the MT5 thread.) Each symbol's new ticks and whether there may be more of them, and when they were read."""
        out = {}
        for s in syms:
            b = self.bars.get(s)
            first = b is None or not b.last_msc
            n = 50_000 if first else 5_000
            try:
                ticks = self.c.broker.ticks_since(s, 0 if first else b.last_msc, n, self.KEEP)
                out[s] = (ticks, len(ticks) >= n)
            except Exception as e:
                out[s] = e
        return out, time.time()

    async def _spec(self, sym: str) -> SymbolSpec | None:
        """`sym`'s broker data, read again every SPEC_EVERY; None while it cannot be read (asked again a minute later,
        the last one read is used meanwhile)."""
        now = asyncio.get_running_loop().time()
        at, spec = self.specs.get(sym, (-1e18, None))
        if now - at < self.SPEC_EVERY:
            return spec
        try:
            spec = await self.c.call(self.c.broker.symbol_spec, sym)
        except Exception as e:      # MT5 busy, or the symbol gone from Market Watch
            self.say(f"spec:{sym}", f"Short-term: no symbol data for {sym} ({e})", 300)
            self.specs[sym] = (now - self.SPEC_EVERY + 60, spec)
            return spec
        self.specs[sym] = (now, spec)
        return spec

    def _burst_soon(self, symbol: str, now: float, hold: float | None = None) -> bool:
        """A news run on `symbol` checks it (and wants it flat) before a scalp opened now would be over (at Y; `hold`:
        a local Rapid run's life instead). A local run is no news run: symbol_owner keeps its symbol."""
        gap = (self.holds()[1] if hold is None else hold) + self.BURST_GAP
        return any(st["symbol"] == symbol and not st.get("local")
                   and st["release"] - st["p"]["lead_seconds"] - now < gap for st in self.c.bursts.values())

    async def enter(self, sym: str, d: dict):
        c, b = self.c, self.c.broker
        async with c.lock:
            loop = asyncio.get_running_loop()
            if not self.active() or len(self.open) >= self.cfg["max_open"]:
                return
            if closes_soon(utcnow(), self.holds()[1]):
                return self.say("closes", "Short-term: no new trade around the daily and weekly close (17:00 New "
                                          "York)", 3600, "info")
            if c.burst_running(sym) or self._burst_soon(sym, loop.time()):
                return
            # every position, this bot's or not: on netting the order would merge into it, and long-term goes first
            if any(p["symbol"] == sym for p in await c.call(b.positions, all_magic=True)):
                return
            # a news burst's order still waiting there (one left by a burst that ended, before the sweeper removes
            # it): filled, it would merge into the scalp
            if any(c.burst_id(o) is not None for o in await c.call(b.pending_orders, sym)):
                return
            acct = await c.call(b.account)
            if not acct["demo"] and not c.allow_real:
                return self.say("real", "Short-term: real account locked (allow_real_account = no)", 3600)
            self.daybook.roll(acct["balance"])
            free = self._free()
            if free is not None and free <= 0:
                if self.daybook.reached() and not self.day["warned"]:
                    self.day["warned"] = True
                    self.daybook.save()
                    await c.report(f"Short-term daily loss limit reached ({-self.day['pnl']:.2f} lost today): "
                                   "short-term trading paused until tomorrow (UTC). Long-term trading goes on.",
                                   "warn", keep=True)
                return
            bid, ask, spread = await c.call(b.quote, sym)
            if bid is None:
                return
            spec = await c.call(b.symbol_spec, sym)
            self.specs[sym] = (loop.time(), spec)
            # the spread cap is in a currency pair's pips: × the symbol's scale (gold 5; 1 on the pairs of v1.2.0)
            if spread > self.cfg["max_spread_pips"] * (spec.scale if spec.scale > 0 else 1.0):
                return
            side, sl_pips, tp_pips = d["side"], float(d["sl_pips"]), d.get("tp_pips")
            if spec.min_stop_distance and sl_pips * spec.pip < spec.min_stop_distance:
                return self.say(f"stop:{sym}", f"Short-term: a {sl_pips} pip stop on {sym} is closer than the broker "
                                               "allows", 3600)
            money = risk_money(self.cfg, acct["balance"], d["strength"])
            allowed = money if free is None else min(money, free)
            lots = lots_for_money(allowed, sl_pips, spec)
            if lots <= 0:
                return self.say(f"lots:{sym}", f"Short-term: risking {money:.2f} with a {sl_pips} pip stop is less "
                                               f"than the minimum lot on {sym}", 900)
            need = await c.call(b.margin_for, sym, side, lots)
            cap = acct["free_margin"] * self.MARGIN_USE
            if need is not None and need > cap:     # as many lots as the margin allows (it risks less)
                fit = round(math.floor(lots * cap / need / spec.volume_step + 1e-9) * spec.volume_step, 8) \
                    if cap > 0 else 0.0
                if fit < spec.volume_min:
                    return await self.tell(f"margin:{sym}", f"Short-term: not enough free margin for {spec.volume_min} "
                                                            f"lots of {sym}: no short-term trade there", 3600)
                await self.tell(f"smaller:{sym}", f"Short-term: the free margin allows {fit} lots of {sym}, not "
                                                  f"{lots}: its trades there are smaller", 3600)
                lots = fit
            sign = 1 if side == "buy" else -1
            entry = ask if side == "buy" else bid
            sl = entry - sign * sl_pips * spec.pip
            tp = entry + sign * float(tp_pips) * spec.pip if tp_pips else None
            if c.dry_run:
                return self.say(f"dry:{sym}", f"[dry-run] short-term would {side} {lots} {sym} "
                                              f"@ {entry:.{spec.digits}f} SL {sl:.{spec.digits}f}", 60, "info")
            # again, with no await before the order goes into the MT5 queue: a stop, or a news burst that woke up
            # while this one waited for MT5, must either have stopped it here or run after it (and see it)
            if not self.active() or c.burst_running(sym) or self._burst_soon(sym, loop.time()):
                return
            sid = self.last_id = max(self.last_id + 1, int(time.time() * 1000))
            try:
                res = await c.call(b.open_market, sym, side, lots, sl, tp, f"fxs:{sid}")
            except Exception as e:
                res = await self._opened_anyway(sid)
                if res is None:
                    return self.say(f"open:{sym}", f"Short-term: {side} {sym} refused: {e}", 120)
            # what it risks: from the price it was filled at (a slip moves the entry, not the stop, and the stop is
            # never moved), so a slip against it risks more than its money
            risk = round(abs(res["price"] - sl) / spec.pip * spec.pip_value_per_lot * res["lots"], 2)
            too_far = risk > allowed * self.RISK_SLACK
            conf = round(max(0.0, min(1.0, float(d["strength"]))) * 100)
            info = {"strategy": "short", "symbol": sym, "side": side, "lots": res["lots"], "entry_price": res["price"],
                    "sl": sl, "tp": tp, "demo": acct["demo"], "opened_at": utcnow().isoformat(), "risk": risk,
                    "confidence": conf}
            x, y = self.holds()
            # close_at: X is over (0: closed at once); then its hold as set now, what counts as a profit on its lots
            # and the rule's stretch at its entry (manage)
            self.open[res["ticket"]] = {"id": sid, "risk": risk, "info": info,
                                        "close_at": 0.0 if too_far else loop.time() + x, "opened": loop.time(),
                                        "hold": x, "max_hold": y, "min_profit": min_profit(spec, res["lots"]),
                                        "stretch": d.get("stretch")}
            self._save_open()
            log.info("Short-term: %s %s %s @ %s, confidence %s%%, risk %.2f", side, res["lots"], sym, res["price"],
                     conf, risk)
            await c.send({"type": "trade_opened", "trade": {"ticket": res["ticket"], "signal_id": None, **info}})
            if too_far:
                await c.report(f"Short-term: {side} {res['lots']} {sym} was filled at {res['price']}, too far from its "
                               f"stop ({risk:.2f} at risk, {allowed:.2f} allowed): closed at once", "warn")
                await self.close_locked(res["ticket"], "filled too far from its stop")

    async def _opened_anyway(self, sid: int) -> dict | None:
        """After a failed open: MT5 may have opened it all the same (no answer is not a refusal)."""
        try:
            p = next((p for p in await self.c.call(self.c.broker.positions) if p.get("comment") == f"fxs:{sid}"), None)
        except Exception:
            return None
        return p and {"ticket": p["ticket"], "price": p["entry_price"], "lots": p["lots"]}

    async def manage(self, now: float):
        """Reports the scalps that closed (stop-loss, take-profit, by hand, by a long-term signal) and closes the
        ones whose hold is over (_exit). The first reason found is kept: the close is tried again at every step until
        it is done, even once the profit that called for it is gone (a close that failed is close_loop's meanwhile)."""
        held = {p["ticket"]: p for p in await self.c.call(self.c.broker.positions)}
        if now >= self.strays_at or not self.booking.keys().isdisjoint(held):    # one taken for closed is listed
            self.strays_at = now + self.STRAYS_EVERY
            self.adopt(list(held.values()))
        for t, s in list(self.open.items()):
            if t not in held:
                self.closed(t)
                continue
            s["profit"] = held[t].get("profit")     # pnl_today, and whether it is in profit (_exit)
            if t in self.c.closing:
                continue
            why = s.get("exit") or self._exit(s, now)
            if why:
                if not s.get("exit") and why != "short-term hold time":
                    log.info("Short-term #%s: %s, closing it", t, why[len("short-term: "):])
                s["exit"] = why
                await self.close(t, why)

    @staticmethod
    def _exit(s: dict, now: float) -> str | None:
        """Why an open scalp is closed now, None to keep it (bot/hold.py hold_action): from X on as soon as it is in
        profit, at Y whatever it shows, and before that as soon as the rule turned against it (step). With Y = X (none
        set: a user saved under v1.2.0) it closes at exactly X as "short-term hold time", as v1.2.0 closed it; so does
        one with close_at 0, at once (left by the last run, not followed from its open, filled too far from its
        stop)."""
        if "opened" not in s or not s["close_at"] or s["max_hold"] <= s["hold"]:
            return "short-term hold time" if now >= s["close_at"] else None
        x, y = s["hold"], s["max_hold"]
        why = hold_action(now - s["opened"], x, y, s.get("profit"), s["min_profit"], s.get("against", False))
        if why is None:
            return None
        return {"profit": f"short-term: in profit after {x}s", "against": "short-term: turned against"}.get(
            why, f"short-term: max hold {y}s")

    async def close(self, ticket: int, why: str) -> bool:
        async with self.c.lock:
            return await self.close_locked(ticket, why)

    async def close_locked(self, ticket: int, why: str) -> bool:
        """With the connector's lock held. True once the position is gone (reported then); a close that fails is
        retried by close_loop (and warned about) while the scalp stays followed."""
        try:
            p = next((p for p in await self.c.call(self.c.broker.positions) if p["ticket"] == ticket), None)
            if p is not None:
                if self.c.dry_run:
                    return False
                await self.c.call(self.c.broker.close_position, p, "fxs exit")
        except Exception as e:
            await self.c._close_failed(ticket, why, e)
            return False
        self.closed(ticket, now=p is not None)
        return True

    async def make_way(self, symbol: str) -> bool:
        """(With the connector's lock held: Connector.execute.) A long-term signal on `symbol` goes first: the scalp
        there is closed. True once none is left on it."""
        mine = await self.c.call(self.c.broker.positions)
        self.adopt(mine)
        ok = True
        for p in mine:
            if p["symbol"] == symbol and p["ticket"] in self.open:
                ok = await self.close_locked(p["ticket"], "a long-term signal on the same symbol") and ok
        return ok

    def closed(self, ticket: int, now: bool = False):
        """`ticket` is gone: its result is reported (and booked). `now`: closed by this run just now. Each report
        gets its own entry: one found open again (adopt) and closed later is reported by its new report only."""
        s = self.open.pop(ticket, None)
        if s is not None:
            self.booking[ticket] = s = dict(s)
            self._save_open()
            task = asyncio.create_task(self._report(ticket, s, wait=s.get("away", False) and not now))
            self.reports.add(task)
            task.add_done_callback(self.reports.discard)

    async def _clock_known(self, ticket: int):
        """Just after a start (or over a weekend) no live tick has proven MetaTrader 5's clock yet, so a close's time
        is unknown: reported as the time now, a scalp that closed while the connector was away would be booked to
        today. Waits CLOCK_WAIT at most for the clock (each history read looks at a tick of the symbol, which proves
        it while the market is open); only then is the time now taken. The scalp stays in scalp_open.json meanwhile,
        and its risk counts against today's limit (_free). Only for a scalp of the last run that closed while the
        connector was away: one this run saw close closed just now."""
        end = time.monotonic() + self.CLOCK_WAIT
        while time.monotonic() < end:
            try:
                res = await self.c.call(self.c.broker.closed_result, ticket)
            except Exception:           # MT5 busy: _report's own retries deal with it
                return
            if not res or res["exit_time"]:
                return
            await asyncio.sleep(self.CLOCK_STEP)

    async def _report(self, ticket: int, s: dict, wait: bool = False):
        """Reported once its result is in the history (the last try reports it without one). A result never read
        (MT5 busy, or the history never shows it) is booked as a loss of all it risked at its stop, so the daily
        limit does not miss it, and the web app's log asks the user to check it in MetaTrader 5's history. Stands
        down, reporting and booking nothing, once `s` is no longer its booking entry: the scalp was found open again
        (adopt), and a later close reports it."""
        if wait:
            await self._clock_known(ticket)
        res = None
        for i in range(self.REPORT_TRIES):
            try:
                if i < self.REPORT_TRIES - 1 and not await self.c.call(self.c.broker.closed_result, ticket):
                    raise LookupError("not in the history yet")
                if self.booking.get(ticket) is not s:
                    return
                res = await self.c.report_closed(ticket, 0.5, s["info"])
                break
            except Exception as e:      # history unreadable (MT5 busy) or behind: asked again
                self.say("report", f"Short-term: could not read the result of #{ticket} yet: {e}", 60)
                await asyncio.sleep(self.REPORT_GAP)
        if self.booking.get(ticket) is not s:
            return
        pnl = res and res["pnl"]
        if pnl is None:
            risked = await self._risked(s)
            await self.c.report(f"Short-term #{ticket}: its result could not be read, so today's short-term loss "
                                f"counts {risked:.2f} (what it risked at its stop). Check it in MetaTrader 5's "
                                "history.", "warn", keep=True)
        took = await self._book(-risked if pnl is None else pnl, res and res["exit_time"], ticket, guess=pnl is None)
        if self.booking.get(ticket) is s:
            del self.booking[ticket]
        self._save_open()
        log.info("Short-term #%s closed: P&L %s (today %.2f)", ticket, pnl, self.day["pnl"])
        if took and pnl is not None:
            await self.c.report(f"Short-term #{ticket}: it was still open after all, and its result is in now "
                                f"({pnl:+.2f}): the {took:.2f} counted for it before is taken back.", "info", keep=True)

    async def _risked(self, s: dict) -> float:
        """What a scalp risked at its stop. One adopted (risk 0: not followed from its open) is measured from its
        stop-loss and lots, else from the loss it last showed."""
        if s["risk"] > 0:
            return s["risk"]
        info = s["info"]
        if info.get("sl"):                          # MT5 lists no stop-loss as 0
            with contextlib.suppress(Exception):    # no symbol data
                spec = await self._spec(info["symbol"])
                return round(abs(float(info["entry_price"]) - float(info["sl"])) / spec.pip * spec.pip_value_per_lot
                             * float(info["lots"]), 2)
        return round(max(0.0, -(s.get("profit") or 0.0)), 2)

    async def _book(self, pnl: float, closed_at: str | None, ticket: int, guess: bool = False) -> float:
        """Adds a closed scalp's result to the short-term day it closed on (UTC): today's, or for one that closed
        while the connector was away, that day's if it is the last one kept (DayBook.book). A day it starts begins
        from the balance before it, read from the account. Returns what it took back of an earlier guess."""
        day, balance = self.daybook.day_of(closed_at), None
        if day > self.day["day"]:
            try:
                balance = (await self.c.call(self.c.broker.account))["balance"] - pnl
            except Exception as e:      # MT5 busy: the day's start as the last day ended
                log.warning("Short-term: could not read the balance (%s)", e)
        return self.daybook.book(day, pnl, ticket, balance, guess)

    async def stop(self):
        """A stop: no new scalp; every open one (followed or not) is closed and reported. A close that fails is
        left to the stop's last retries and reported by it."""
        if self.task:
            self.task.cancel()
            await asyncio.wait([self.task], timeout=1)
        self.adopt(await self.c.call(self.c.broker.positions))
        for t in list(self.open):
            await self.close(t, "the connector is stopping")
        if self.reports:
            await asyncio.wait(set(self.reports), timeout=5)


def main():
    ap = argparse.ArgumentParser(description="FX Signals connector for MetaTrader 5")
    ap.add_argument("--config", default=str(HERE / "connector.ini"))
    ap.add_argument("--server", help="overrides connector.ini")
    ap.add_argument("--token", help="overrides connector.ini (or set FX_CONNECTOR_TOKEN)")
    ap.add_argument("--dry-run", action="store_true", help="log decisions, place no orders")
    ap.add_argument("--paper", action="store_true", help="simulated account, no MetaTrader 5 needed")
    ap.add_argument("--stop-on-stdin-eof", action="store_true",
                    help="stop cleanly (as Ctrl+C) once stdin is closed: how the Forex Bot app stops it")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(HERE / "connector.log", encoding="utf-8")])
    cfg = configparser.ConfigParser()
    cfg.read(args.config, encoding="utf-8")
    sec = cfg["connector"] if cfg.has_section("connector") else {}
    server = args.server or sec.get("server", "http://127.0.0.1:8000")
    token = args.token or os.getenv("FX_CONNECTOR_TOKEN") or sec.get("token", "")
    if not token:
        sys.exit("No token. Create one in the web app (Connector page) and paste it into connector.ini.")
    allow_real = str(sec.get("allow_real_account", "no")).strip().lower() in ("yes", "true", "1")
    broker = PaperBroker() if args.paper else MT5Adapter(int(sec.get("magic", MAGIC)))
    conn = Connector(server, token, broker, allow_real, args.dry_run, sec.get("symbol_suffix", "").strip())

    running = {"loop": None, "main": None, "stopper": None, "ctrl_c": 0}

    def stop():
        """From any thread: Connector.shutdown on the loop (it does nothing a second time)."""
        def begin():
            running["stopper"] = asyncio.ensure_future(conn.shutdown(running["main"]))   # kept: a task may be
        try:                                                                           # collected otherwise
            running["loop"].call_soon_threadsafe(begin)
        except (AttributeError, RuntimeError):  # not started yet, or the loop is already closed: stopping anyway
            pass

    def on_ctrl_c(signum, frame):
        """The first Ctrl+C stops as the app does (news bursts clean up first); the third stops at once."""
        running["ctrl_c"] += 1
        if running["loop"] is None or running["ctrl_c"] >= 3:
            raise KeyboardInterrupt
        if running["ctrl_c"] == 1:
            log.info("Stopping cleanly (open news bursts end first). Ctrl+C twice more stops at once.")
            stop()
        else:
            log.warning("Still stopping: one more Ctrl+C stops at once (a news burst's orders may stay behind).")

    def watch():                                 # blocks until the app closes the pipe (or dies: the pipe closes too)
        try:                                     # os.read, not sys.stdin: a daemon thread blocked inside the buffered
            fd = sys.stdin.fileno()              # reader holds its lock, and Python's exit then fails on it
            while os.read(fd, 65536):
                pass
        except (OSError, ValueError):
            pass
        stop()

    async def run():
        running["loop"], running["main"] = asyncio.get_running_loop(), asyncio.current_task()
        if args.stop_on_stdin_eof and sys.stdin:
            threading.Thread(target=watch, name="stdin-watch", daemon=True).start()
        await conn.run()

    signal.signal(signal.SIGINT, on_ctrl_c)      # set before asyncio.run, so it does not put its own in
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Stopped.")


if __name__ == "__main__":
    main()
