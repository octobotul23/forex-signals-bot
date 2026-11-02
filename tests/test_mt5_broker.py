"""MT5Broker's pending orders (v1.3.0, bot/mt5_broker.py): the plain stop sent exactly as v1.2.0 sent it, stop-limit
orders (type, trigger, limit on the pullback side, the symbol's order_mode checked before anything is sent), the
filling mode learnt per symbol and kind, pending_orders' type / kind / price_stoplimit, order_state and pending_room.

    .venv\\Scripts\\python tests\\test_mt5_broker.py [scenario ...]

bot/mt5_broker.py runs against a fake MetaTrader5 module (the real package is never imported) holding the package's
own constant values, and a fake terminal whose answers follow the M0 demo probe (FOK filling, a stop-limit that
turns into a limit on the same ticket). No terminal, no network, no files written. Exit code 0 = every check passed.
"""
import logging
import sys
import time
import traceback
import types
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# bot/mt5_broker.py binds MetaTrader5 on import: give it this fake, so the real package is never loaded
FAKE = types.ModuleType("MetaTrader5")
sys.modules["MetaTrader5"] = FAKE
# the values of the MetaTrader5 package (5.0.x)
for _name, _value in {
        "ORDER_TYPE_BUY": 0, "ORDER_TYPE_SELL": 1, "ORDER_TYPE_BUY_LIMIT": 2, "ORDER_TYPE_SELL_LIMIT": 3,
        "ORDER_TYPE_BUY_STOP": 4, "ORDER_TYPE_SELL_STOP": 5, "ORDER_TYPE_BUY_STOP_LIMIT": 6,
        "ORDER_TYPE_SELL_STOP_LIMIT": 7, "ORDER_TYPE_CLOSE_BY": 8,
        "ORDER_FILLING_FOK": 0, "ORDER_FILLING_IOC": 1, "ORDER_FILLING_RETURN": 2,
        "ORDER_TIME_GTC": 0, "ORDER_TIME_DAY": 1, "ORDER_TIME_SPECIFIED": 2,
        "ORDER_STATE_STARTED": 0, "ORDER_STATE_PLACED": 1, "ORDER_STATE_CANCELED": 2, "ORDER_STATE_PARTIAL": 3,
        "ORDER_STATE_FILLED": 4, "ORDER_STATE_REJECTED": 5, "ORDER_STATE_EXPIRED": 6, "ORDER_STATE_REQUEST_ADD": 7,
        "ORDER_STATE_REQUEST_MODIFY": 8, "ORDER_STATE_REQUEST_CANCEL": 9,
        "TRADE_ACTION_DEAL": 1, "TRADE_ACTION_PENDING": 5, "TRADE_ACTION_REMOVE": 8,
        "TRADE_RETCODE_PLACED": 10008, "TRADE_RETCODE_DONE": 10009, "TRADE_RETCODE_DONE_PARTIAL": 10010,
        "TRADE_RETCODE_INVALID_PRICE": 10015, "TRADE_RETCODE_INVALID_EXPIRATION": 10022,
        "TRADE_RETCODE_FROZEN": 10029, "TRADE_RETCODE_INVALID_FILL": 10030,
        "ACCOUNT_TRADE_MODE_DEMO": 0}.items():
    setattr(FAKE, _name, _value)

import bot.mt5_broker as MB              # noqa: E402

PASSED: list[str] = []
FAILED: list[str] = []
MAGIC = 20240611
STAMP = 2_000_000_000                    # the symbols' last tick, on the broker's clock (ahead of this PC: used as now)
FOK, IOC, RETURN = FAKE.ORDER_FILLING_FOK, FAKE.ORDER_FILLING_IOC, FAKE.ORDER_FILLING_RETURN
BUY_STOP, SELL_STOP, BUY_SL, SELL_SL = 4, 5, 6, 7
NEW_KEYS = ("type", "kind", "price_stoplimit")


def check(ok, label: str, detail=""):
    d = repr(detail) if not isinstance(detail, str) else detail
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {d[:600]}" if not ok and detail != "" else ""))
    (PASSED if ok else FAILED).append(label)


class Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(record.getMessage())


LOG = Records()
_mt5_log = logging.getLogger("mt5")
_mt5_log.addHandler(LOG)
_mt5_log.setLevel(logging.INFO)
_mt5_log.propagate = False


# ── fake MetaTrader 5 terminal ──────────────────────────────────
def info(point, digits, **kw):
    """symbol_info as the M0 probe read it: FOK only, every expiry mode, every order type (order_mode 127)."""
    base = dict(point=point, digits=digits, trade_tick_value=1.0, trade_tick_size=point, volume_min=0.01,
                volume_step=0.01, volume_max=100.0, trade_stops_level=5, trade_freeze_level=0, filling_mode=1,
                expiration_mode=15, order_mode=127)
    base.update(kw)
    return SimpleNamespace(**{k: v for k, v in base.items() if v is not ...})


def order(ticket, type_, price, state=1, magic=MAGIC, **kw):
    """A TradeOrder as orders_get / history_orders_get return it."""
    base = dict(ticket=ticket, symbol="EURUSD", type=type_, state=state, magic=magic, price_open=price, sl=0.0,
                tp=0.0, volume_initial=1.0, volume_current=1.0, price_stoplimit=0.0, comment="fxr:1:0")
    base.update(kw)
    return SimpleNamespace(**{k: v for k, v in base.items() if v is not ...})


class Terminal:
    def __init__(self):
        self.symbols = {
            "EURUSD": info(0.00001, 5),
            "GBPUSD": info(0.00001, 5),
            "XAUUSD": info(0.01, 2, trade_freeze_level=20),
            "USDJPY": info(0.001, 3, order_mode=127 & ~8),        # no stop-limit orders
            "GBPJPY": info(0.001, 3, order_mode=...),              # an old build: no order_mode field
            "AUDUSD": info(0.00001, 5, order_mode=None),
        }
        self.sent: list[dict] = []
        self.calls: list[str] = []
        self.script: list = []          # answers to give first: (retcode, comment) or None (no answer)
        self.fillings = {}              # (symbol, "stop" | "stop_limit") -> filling modes accepted (default FOK)
        self.no_expiry: set[str] = set()
        self.next_ticket = 500
        self.book: list = []
        self.history: list = []
        self.account = SimpleNamespace(login=1, server="Fake-Demo", trade_mode=0, limit_orders=100)
        self.total = 0

    def order_send(self, req):
        self.sent.append(dict(req))
        if self.script:
            a = self.script.pop(0)
            return None if a is None else SimpleNamespace(retcode=a[0], comment=a[1], order=0)
        kind = "stop_limit" if req["type"] in (BUY_SL, SELL_SL) else "stop"
        if req.get("type_time") == FAKE.ORDER_TIME_SPECIFIED and req["symbol"] in self.no_expiry:
            return SimpleNamespace(retcode=10022, comment="Invalid expiration", order=0)
        if req["type_filling"] not in self.fillings.get((req["symbol"], kind), (FOK,)):
            return SimpleNamespace(retcode=10030, comment="Unsupported filling mode", order=0)
        self.next_ticket += 1
        return SimpleNamespace(retcode=10009, comment="Request executed", order=self.next_ticket)

    def orders_get(self, **kw):
        self.calls.append("orders_get")
        if self.book is None:
            return None
        if "ticket" in kw:
            return tuple(o for o in self.book if o.ticket == kw["ticket"])
        return tuple(o for o in self.book if "symbol" not in kw or o.symbol == kw["symbol"])

    def history_orders_get(self, **kw):
        self.calls.append("history_orders_get")
        if self.history is None:
            return None
        return tuple(o for o in self.history if o.ticket == kw.get("ticket"))


TERM = Terminal()
FAKE.last_error = lambda: (1, "Success")
FAKE.symbol_select = lambda s, on: True
FAKE.symbol_info = lambda s: TERM.symbols.get(s)
FAKE.symbol_info_tick = lambda s: SimpleNamespace(bid=1.1, ask=1.10005, time=STAMP, time_msc=STAMP * 1000)
FAKE.order_send = lambda req: TERM.order_send(req)
FAKE.orders_get = lambda **kw: TERM.orders_get(**kw)
FAKE.history_orders_get = lambda **kw: TERM.history_orders_get(**kw)
FAKE.orders_total = lambda: TERM.total
FAKE.account_info = lambda: TERM.account


def fresh() -> "MB.MT5Broker":
    global TERM
    TERM = Terminal()
    LOG.lines.clear()
    return MB.MT5Broker(magic=MAGIC)


def v120_request(b, symbol, side, lots, price, sl, tp, comment="", expire_in=None, fill=None) -> dict:
    """v1.2.0's place_pending request, copied verbatim (fill: the mode it had learnt for the symbol, None at first):
    a plain stop is still sent exactly so."""
    spec = b.symbol_spec(symbol)
    req = {"action": FAKE.TRADE_ACTION_PENDING, "symbol": symbol, "volume": float(lots),
           "type": FAKE.ORDER_TYPE_BUY_STOP if side == "buy" else FAKE.ORDER_TYPE_SELL_STOP,
           "price": round(price, spec.digits), "sl": round(sl, spec.digits) if sl else 0.0,
           "tp": round(tp, spec.digits) if tp else 0.0, "magic": b.magic, "comment": comment[:31],
           "type_time": FAKE.ORDER_TIME_GTC, "type_filling": b._filling(symbol) if fill is None else fill}
    if expire_in and symbol not in b._gtc_only and FAKE.symbol_info(symbol).expiration_mode & 4:  # SPECIFIED
        req["type_time"], req["expiration"] = FAKE.ORDER_TIME_SPECIFIED, int(b._server_now(symbol) + expire_in)
    return req


def raises(fn, exc=Exception):
    """The exception `fn()` raised (None when it did not, or raised another type)."""
    try:
        fn()
    except exc as e:
        return e
    except Exception:
        return None
    return None


# ── scenarios ───────────────────────────────────────────────────
def test_plain_stop_unchanged():
    b = fresh()
    cases = [("EURUSD", "buy", 1.0, 1.1007549, 1.0997549, None, "fxb:1:b", None),
             ("EURUSD", "sell", 0.5, 1.0993, 1.1003, 1.0989, "fxb:1:s", 150.0),
             ("XAUUSD", "buy", 0.01, 2650.123, 2649.004, 2651.996, "fxb:12345:b", 30.0),
             ("USDJPY", "sell", 2.0, 151.2345, None, None, "x" * 40, 0),
             ("GBPJPY", "buy", 0.3, 190.1234, 190.0001, 190.2229, "fxb:9:b", None)]
    for symbol, side, lots, price, sl, tp, comment, expire_in in cases:
        want = v120_request(b, symbol, side, lots, price, sl, tp, comment, expire_in,
                            b._pending_fill.get((symbol, "stop")))
        TERM.sent.clear()
        t = b.place_pending(symbol, side, lots, price, sl, tp, comment, expire_in)
        got = TERM.sent
        check(len(got) == 1 and list(got[0].items()) == list(want.items()) and t == TERM.next_ticket,
              f"plain {side} stop on {symbol} (expiry {expire_in}): the v1.2.0 request, key for key", (got, want))
    check(TERM.sent[0]["type"] == BUY_STOP and "stoplimit" not in TERM.sent[0], "no stoplimit on a plain stop")
    # literally, for one: the request v1.2.0 sent for a sell stop with an expiry
    b = fresh()
    b.place_pending("EURUSD", "sell", 1.0, 1.0993, 1.1003, None, "fxb:1:s", 150.0)
    check(TERM.sent == [{"action": 5, "symbol": "EURUSD", "volume": 1.0, "type": SELL_STOP, "price": 1.0993,
                         "sl": 1.1003, "tp": 0.0, "magic": MAGIC, "comment": "fxb:1:s", "type_time": 2,
                         "type_filling": FOK, "expiration": STAMP + 150}], "the request, written out", TERM.sent)
    check(b.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:1:b", None, "stop") == TERM.next_ticket
          and TERM.sent[-1]["type"] == BUY_STOP, "kind='stop' given explicitly: the same path")
    # a plain stop needs nothing from order_mode: a symbol without stop-limit orders, or without the field, takes it
    for symbol in ("USDJPY", "GBPJPY", "AUDUSD"):
        check(raises(lambda: b.place_pending(symbol, "buy", 1.0, 151.5, 151.0, None, "fxb:2:b")) is None,
              f"plain stop on {symbol} (order_mode {getattr(TERM.symbols[symbol], 'order_mode', 'missing')})")

    # the retries as before: a refused expiry -> good-till-cancelled; FOK refused -> RETURN, kept for the symbol
    b = fresh()
    TERM.fillings[("EURUSD", "stop")] = (RETURN,)
    TERM.no_expiry.add("EURUSD")
    b.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:3:b", 60.0)
    check([(r["type_filling"], r["type_time"], "expiration" in r) for r in TERM.sent]
          == [(FOK, 2, True), (FOK, 0, False), (RETURN, 0, False)],
          "10022 then 10030: good-till-cancelled, then RETURN", TERM.sent)
    check("EURUSD" in b._gtc_only and sum("good-till-cancelled" in m for m in LOG.lines) == 1, "logged once", LOG.lines)
    TERM.sent.clear()
    b.place_pending("EURUSD", "sell", 1.0, 1.0993, 1.1003, None, "fxb:3:s", 60.0)
    want = v120_request(b, "EURUSD", "sell", 1.0, 1.0993, 1.1003, None, "fxb:3:s", 60.0, fill=RETURN)
    check(len(TERM.sent) == 1 and TERM.sent[0] == want, "then sent at once with what worked", TERM.sent)
    TERM.script[:] = [(10015, "Invalid price")]
    e = raises(lambda: b.place_pending("EURUSD", "buy", 1.0, 1.1, 1.09, None, "fxb:3:b"), MB.BrokerError)
    check(e is not None and e.retcode == 10015, f"any other refusal raises with its retcode ({e})")
    TERM.script[:] = [(10008, "Order placed")]
    check(b.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:3:b") == 0, "10008 (placed) is a success")


def test_stop_limit_request():
    b = fresh()
    check(b.stop_limit_allowed("EURUSD") and b.stop_limit_allowed("XAUUSD") and not b.stop_limit_allowed("USDJPY")
          and not b.stop_limit_allowed("GBPJPY") and not b.stop_limit_allowed("AUDUSD"),
          "stop_limit_allowed: order_mode flag 8; a missing or empty field is no")
    # buy: trigger above the market, limit below the trigger (the pullback side); sell: the mirror
    t = b.place_pending("EURUSD", "buy", 1.0, 1.1007549, 1.0987549, 1.1012549, "fxr:1730000000000:0", 30.0,
                        kind="stop_limit", limit_price=1.1007049)
    r = TERM.sent[-1]
    check(t == TERM.next_ticket and r["type"] == BUY_SL and r["price"] == 1.10075 and r["stoplimit"] == 1.1007
          and r["sl"] == 1.09875 and r["tp"] == 1.10125,
          "buy stop-limit: type 6, price = trigger, stoplimit = limit", r)
    plain = v120_request(b, "EURUSD", "buy", 1.0, 1.1007549, 1.0987549, 1.1012549, "fxr:1730000000000:0", 30.0)
    check({k: v for k, v in r.items() if k != "stoplimit"} == {**plain, "type": BUY_SL},
          "and otherwise the plain stop's request: action, volume, magic, comment, expiry, filling", (r, plain))
    b.place_pending("EURUSD", "sell", 2.5, 1.0992451, 1.1012451, 1.0987451, "fxr:1730000000000:1", None,
                    kind="stop_limit", limit_price=1.0992951)
    r = TERM.sent[-1]
    check(r["type"] == SELL_SL and r["price"] == 1.09925 and r["stoplimit"] == 1.0993 and r["volume"] == 2.5
          and r["type_time"] == FAKE.ORDER_TIME_GTC and "expiration" not in r,
          "sell stop-limit: type 7, limit above the trigger, good-till-cancelled without an expiry", r)
    b.place_pending("XAUUSD", "buy", 0.1, 2650.4567, 2648.4567, None, "fxr:7:0", 45.0, kind="stop_limit",
                    limit_price=2650.4049)
    r = TERM.sent[-1]
    check((r["price"], r["stoplimit"], r["sl"], r["tp"], r["expiration"])
          == (2650.46, 2650.4, 2648.46, 0.0, STAMP + 45), "gold: prices rounded to its 2 digits", r)
    check(len(b.place_pending.__defaults__) == 4 and b.place_pending.__defaults__[2:] == ("stop", None),
          "kind / limit_price are new keywords after expire_in (the connector's positional calls are unchanged)")


def test_stop_limit_refused_before_sending():
    b = fresh()
    for symbol, why in (("USDJPY", "without flag 8"), ("GBPJPY", "no order_mode field"), ("AUDUSD", "order_mode None")):
        TERM.sent.clear()
        e = raises(lambda: b.place_pending(symbol, "buy", 1.0, 151.5, 151.0, None, "fxr:1:0", 30.0,
                                           kind="stop_limit", limit_price=151.49), MB.BrokerError)
        check(e is not None and str(e) == f"stop-limit orders are not allowed on {symbol}" and e.retcode is None
              and TERM.sent == [], f"{symbol} ({why}): a readable error with no retcode, nothing sent ({e})")
    check(not any(k[1] == "stop_limit" for k in b._pending_fill), "nothing learnt from a refusal")
    bad = [("no limit price", dict(kind="stop_limit"), "buy", 1.1008),
           ("buy limit above its trigger", dict(kind="stop_limit", limit_price=1.1010), "buy", 1.1008),
           ("buy limit at its trigger", dict(kind="stop_limit", limit_price=1.1008), "buy", 1.1008),
           ("buy limit at its trigger once rounded", dict(kind="stop_limit", limit_price=1.100796), "buy", 1.100804),
           ("sell limit below its trigger", dict(kind="stop_limit", limit_price=1.0990), "sell", 1.0993),
           ("sell limit at its trigger", dict(kind="stop_limit", limit_price=1.0993), "sell", 1.0993),
           ("limit NaN", dict(kind="stop_limit", limit_price=float("nan")), "sell", 1.0993),
           ("unknown kind", dict(kind="limit", limit_price=1.0990), "buy", 1.1008)]
    for what, kw, side, price in bad:
        TERM.sent.clear()
        e = raises(lambda: b.place_pending("EURUSD", side, 1.0, price, None, None, "fxr:1:0", **kw), ValueError)
        check(e is not None and TERM.sent == [], f"{what}: ValueError, nothing sent ({e})")


def test_filling_per_kind():
    b = fresh()
    # this broker takes plain stops only with RETURN and stop-limits with FOK (on EURUSD)
    TERM.fillings[("EURUSD", "stop")] = (RETURN,)
    b.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxb:1:b")
    check([r["type_filling"] for r in TERM.sent] == [FOK, RETURN], "plain stop: FOK refused, RETURN taken", TERM.sent)
    TERM.sent.clear()
    b.place_pending("EURUSD", "buy", 1.0, 1.1008, 1.0998, None, "fxr:1:0", kind="stop_limit", limit_price=1.10075)
    check([r["type_filling"] for r in TERM.sent] == [FOK], "stop-limit: FOK first, not the stop's RETURN", TERM.sent)
    TERM.sent.clear()
    b.place_pending("EURUSD", "sell", 1.0, 1.0993, 1.1003, None, "fxb:1:s")
    b.place_pending("EURUSD", "sell", 1.0, 1.0993, 1.1003, None, "fxr:1:1", kind="stop_limit", limit_price=1.09935)
    check([r["type_filling"] for r in TERM.sent] == [RETURN, FOK], "each kind sent at once with its own mode",
          TERM.sent)
    check(b._pending_fill == {("EURUSD", "stop"): RETURN, ("EURUSD", "stop_limit"): FOK},
          "keyed by (symbol, kind)", b._pending_fill)
    # the other way round, and per symbol: a stop-limit refused with FOK leaves GBPUSD's plain stops on FOK
    TERM.sent.clear()
    TERM.fillings[("GBPUSD", "stop_limit")] = (RETURN,)
    b.place_pending("GBPUSD", "buy", 1.0, 1.3008, 1.2998, None, "fxr:2:0", kind="stop_limit", limit_price=1.30075)
    b.place_pending("GBPUSD", "buy", 1.0, 1.3008, 1.2998, None, "fxb:2:b")
    check([(r["type"], r["type_filling"]) for r in TERM.sent] == [(BUY_SL, FOK), (BUY_SL, RETURN), (BUY_STOP, FOK)],
          "GBPUSD: the stop-limit learns RETURN, its plain stop still starts with FOK", TERM.sent)
    check(b._pending_fill[("GBPUSD", "stop_limit")] == RETURN and b._pending_fill[("GBPUSD", "stop")] == FOK
          and ("XAUUSD", "stop") not in b._pending_fill, "each (symbol, kind) on its own", b._pending_fill)
    # a refused expiry is the symbol's, whatever the kind
    TERM.sent.clear()
    TERM.no_expiry.add("XAUUSD")
    b.place_pending("XAUUSD", "sell", 0.1, 2649.5, 2651.5, None, "fxr:3:0", 30.0, kind="stop_limit",
                    limit_price=2649.55)
    b.place_pending("XAUUSD", "sell", 0.1, 2649.5, 2651.5, None, "fxb:3:s", 30.0)
    check([(r["type"], r["type_time"]) for r in TERM.sent] == [(SELL_SL, 2), (SELL_SL, 0), (SELL_STOP, 0)]
          and "XAUUSD" in b._gtc_only, "the stop-limit's refused expiry: both kinds good-till-cancelled", TERM.sent)


def test_pending_orders():
    b = fresh()
    TERM.book = [
        order(11, BUY_STOP, 1.1008, sl=1.0998, comment="fxb:1:b"),
        order(12, SELL_SL, 1.0992, sl=1.1012, tp=1.0987, price_stoplimit=1.09925, comment="fxr:5:1"),
        order(13, FAKE.ORDER_TYPE_BUY_LIMIT, 1.10075, sl=1.0988, tp=1.1013, comment="fxr:5:0"),  # triggered
        order(14, FAKE.ORDER_TYPE_SELL_LIMIT, 1.09935, comment="fxr:6:1"),
        order(15, BUY_SL, 1.1008, price_stoplimit=1.10075, comment="fxr:6:0"),
        order(16, SELL_STOP, 1.0993, comment="fxb:2:s", price_stoplimit=...),    # a fake without the field
        order(17, FAKE.ORDER_TYPE_BUY, 1.10005, state=0, comment="fx exit"),          # a deal on its way
        order(18, BUY_STOP, 1.2, magic=0, comment="manual"),                         # not this bot's
    ]
    got = {o["ticket"]: o for o in b.pending_orders()}
    check(sorted(got) == [11, 12, 13, 14, 15, 16, 17], "this bot's orders only", sorted(got))
    want = {11: ("buy", BUY_STOP, "stop", 0.0), 12: ("sell", SELL_SL, "stop_limit", 1.09925),
            13: ("buy", 2, "limit", 0.0), 14: ("sell", 3, "limit", 0.0), 15: ("buy", BUY_SL, "stop_limit", 1.10075),
            16: ("sell", SELL_STOP, "stop", 0.0), 17: ("buy", 0, "market", 0.0)}
    for t, w in want.items():
        o = got[t]
        check((o["side"], o["type"], o["kind"], o["price_stoplimit"]) == w and isinstance(o["type"], int)
              and isinstance(o["price_stoplimit"], float), f"order {t}: side / type / kind / price_stoplimit {w}", o)
    o = got[12]
    check(o == {"ticket": 12, "symbol": "EURUSD", "side": "sell", "price": 1.0992, "sl": 1.1012, "tp": 1.0987,
                "lots": 1.0, "comment": "fxr:5:1", "type": SELL_SL, "kind": "stop_limit", "price_stoplimit": 1.09925},
          "a waiting stop-limit: price = the trigger, price_stoplimit = the limit", o)
    o = got[11]
    check({k: v for k, v in o.items() if k not in NEW_KEYS} == {"ticket": 11, "symbol": "EURUSD", "side": "buy",
                                                                "price": 1.1008, "sl": 1.0998, "tp": None,
                                                                "lots": 1.0, "comment": "fxb:1:b"}
          and list(o)[:8] == ["ticket", "symbol", "side", "price", "sl", "tp", "lots", "comment"],
          "the v1.2.0 fields unchanged (sl / tp 0 -> None), the new ones after them", o)
    check(got[13]["price"] == 1.10075 and got[13]["sl"] == 1.0988 and got[13]["tp"] == 1.1013,
          "a triggered stop-limit (now a limit): price = the limit, SL / TP kept", got[13])
    check([o["ticket"] for o in b.pending_orders("XAUUSD")] == [] and len(b.pending_orders("EURUSD")) == 7,
          "by symbol")
    TERM.book = None
    check(b.pending_orders() == [], "None with no error: no orders, as before")
    FAKE.last_error = lambda: (-10005, "IPC timeout")
    try:
        check(raises(b.pending_orders, MB.BrokerError) is not None, "a failed read still raises")
    finally:
        FAKE.last_error = lambda: (1, "Success")


def test_order_state():
    b = fresh()
    TERM.book = [order(21, BUY_SL, 1.1008, price_stoplimit=1.10075, volume_initial=2.0, volume_current=2.0),
                 order(22, FAKE.ORDER_TYPE_BUY_LIMIT, 1.10075, volume_initial=2.0, volume_current=2.0),
                 order(23, FAKE.ORDER_TYPE_SELL_LIMIT, 1.09935, state=3, volume_initial=2.0, volume_current=0.5),
                 order(24, SELL_SL, 1.0992, state=0, price_stoplimit=1.09925),
                 order(25, SELL_SL, 1.0992, state=7, price_stoplimit=1.09925),
                 order(26, BUY_SL, 1.1008, state=8, price_stoplimit=1.10075),
                 order(27, BUY_SL, 1.1008, state=9, price_stoplimit=1.10075),
                 order(28, BUY_STOP, 1.1008, state=42, price_stoplimit=...),
                 order(31, BUY_SL, 1.1008, magic=0, price_stoplimit=1.10075)]
    TERM.history = [order(41, FAKE.ORDER_TYPE_BUY_LIMIT, 1.10075, state=4, volume_initial=2.0, volume_current=0.0),
                    order(42, SELL_SL, 1.0992, state=2, price_stoplimit=1.09925),
                    order(43, FAKE.ORDER_TYPE_SELL_LIMIT, 1.09935, state=6),
                    order(44, BUY_SL, 1.1008, state=5, price_stoplimit=1.10075),
                    order(45, BUY_STOP, 1.1008, state=42),
                    order(46, SELL_STOP, 1.0993, state=4, volume_current=0.0, price_stoplimit=...)]
    s = b.order_state(21)
    check(s == {"state": "placed", "type": BUY_SL, "kind": "stop_limit", "volume_initial": 2.0, "volume_current": 2.0,
                "price_open": 1.1008, "price_stoplimit": 1.10075}, "a waiting stop-limit", s)
    s = b.order_state(22)
    check(s["state"] == "placed" and s["type"] == 2 and s["kind"] == "limit" and s["price_open"] == 1.10075
          and s["price_stoplimit"] == 0.0, "triggered: the same ticket, now a limit at the limit price", s)
    s = b.order_state(23)
    check(s["state"] == "partial" and s["volume_current"] == 0.5 and s["kind"] == "limit", "partly filled", s)
    for t, what in ((24, "started (0)"), (25, "started (7, being added)"), (26, "placed (8, being modified)"),
                    (27, "placed (9, being removed)"), (28, "placed (42, a state this code does not know)"),
                    (31, "placed (another magic's order: looked up by ticket)")):
        s = b.order_state(t)
        check(s is not None and s["state"] == what.split()[0], f"in the book: {what}", s)
    s = b.order_state(41)
    check(s == {"state": "filled", "type": 2, "kind": "limit", "volume_initial": 2.0, "volume_current": 0.0,
                "price_open": 1.10075, "price_stoplimit": 0.0}, "filled at the limit (history)", s)
    for t, want in ((42, "canceled"), (43, "expired"), (44, "rejected"), (46, "filled")):
        s = b.order_state(t)
        check(s is not None and s["state"] == want, f"history: {want}", s)
    check(b.order_state(45) is None, "history with a state this code does not know: unknown (None)")
    check(b.order_state(99) is None, "no such order anywhere: None")
    TERM.calls.clear()
    b.order_state("21")
    check(TERM.calls == ["orders_get"], "the book first; the history only when it is not there", TERM.calls)
    TERM.calls.clear()
    b.order_state(41)
    check(TERM.calls == ["orders_get", "history_orders_get"], "then the history", TERM.calls)
    # an order that left the book between the reads: the history answers
    TERM.book.append(order(41, FAKE.ORDER_TYPE_BUY_LIMIT, 1.10075))
    check(b.order_state(41)["state"] == "placed", "in the book: the book answers")
    TERM.book.pop()
    # fakes / builds that ignore the ticket filter, or answer None
    TERM.orders_get = lambda **kw: tuple(TERM.book)
    try:
        check(b.order_state(22)["type"] == 2 and b.order_state(41)["state"] == "filled",
              "a list holding other tickets: the one asked for is picked")
    finally:
        del TERM.orders_get
    TERM.book, TERM.history = None, None
    check(b.order_state(21) is None, "both reads answer None: unknown")


def test_pending_room():
    b = fresh()
    TERM.total = 37
    check(b.pending_room() == 63, "100 allowed, 37 open: 63")
    TERM.total = 100
    check(b.pending_room() == 0, "full: 0")
    TERM.total = 120
    check(b.pending_room() == 0, "more open than the cap (lowered meanwhile): 0, never negative")
    TERM.account.limit_orders = 0
    check(b.pending_room() is None, "limit_orders 0: no cap (None)")
    del TERM.account.limit_orders
    check(b.pending_room() is None, "no limit_orders field: no cap (None)")
    TERM.account.limit_orders = 50
    TERM.total = None
    e = raises(b.pending_room, MB.BrokerError)
    check(e is not None and "orders_total" in str(e), f"orders_total unreadable: raises ({e})")
    TERM.account = None
    e = raises(b.pending_room, MB.BrokerError)
    check(e is not None and "account_info" in str(e), f"account_info unreadable: raises ({e})")


TESTS = [test_plain_stop_unchanged, test_stop_limit_request, test_stop_limit_refused_before_sending,
         test_filling_per_kind, test_pending_orders, test_order_state, test_pending_room]


def main():
    started = time.time()
    picked = [fn for fn in TESTS if fn.__name__ in sys.argv[1:]]     # scenarios named on the command line, else all
    for fn in picked or TESTS:
        print(f"\n== {fn.__name__}")
        try:
            fn()
        except Exception:
            check(False, f"{fn.__name__}: raised", traceback.format_exc())
    check(sys.modules["MetaTrader5"] is FAKE and MB.mt5 is FAKE, "the real MetaTrader5 package was never loaded")
    if FAILED:
        print("\nfailed:\n  " + "\n  ".join(FAILED))
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - started:.1f} s)")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
