"""The user's dashboard: which numbers show, in what order, each over its own number of days."""
import math

# kind -> (has a period in days, has a strategy scope)
KINDS = {
    "equity": (False, False),          # connector: equity + balance
    "open_signals": (False, False),
    "open_trades": (False, True),
    "pnl": (True, True),               # my closed trades
    "win_rate": (True, True),
    "trades": (True, True),
    "profit_factor": (True, True),
    "avg_trade": (True, True),
    "sig_win_rate": (True, False),     # the engine's signals (all users see the same)
    "sig_pips": (True, False),
}
# a trade's strategy (user_trades.strategy); "all" counts every trade, test trades (strategy "test") too. rapid
# (v1.3.0): Rapid's trades, news runs and signal runs alike
SCOPES = ("all", "long", "short", "burst", "news", "custom", "rapid")
# a signal's mode -> its trades' strategy (signal modes only: a Rapid trade comes from a run, never from a signal)
MODE_SCOPE = {"trend": "long", "news_ai": "news", "custom": "custom"}
DAYS = (1, 365)
MAX_TILES = 12
# today's four numbers, plus the account's own result
DEFAULT = {"tiles": [{"k": "equity"}, {"k": "open_signals"}, {"k": "sig_win_rate", "days": 30},
                     {"k": "sig_pips", "days": 30}, {"k": "pnl", "days": 30, "scope": "all"}]}


def clamp_dashboard(body) -> dict:
    """A clean copy of `body`: known kinds only (unknown ones are dropped, so an older server keeps working),
    days clamped to 1..365, at most MAX_TILES. Raises ValueError("invalid_dashboard" | "invalid_number")."""
    if not isinstance(body, dict) or not isinstance(body.get("tiles"), list):
        raise ValueError("invalid_dashboard")
    tiles = []
    for tile in body["tiles"]:
        if not isinstance(tile, dict) or not isinstance(tile.get("k"), str) or tile["k"] not in KINDS:
            continue
        has_days, has_scope = KINDS[tile["k"]]
        out = {"k": tile["k"]}
        if has_days:
            try:
                d = float(tile.get("days", 30))
            except (TypeError, ValueError, OverflowError):
                raise ValueError("invalid_number")
            if not math.isfinite(d):
                raise ValueError("invalid_number")
            out["days"] = int(max(DAYS[0], min(DAYS[1], round(d))))
        if has_scope:
            out["scope"] = tile.get("scope") if tile.get("scope") in SCOPES else "all"
        tiles.append(out)
        if len(tiles) == MAX_TILES:
            break
    return {"tiles": tiles}


# the market clock (v1.3.2): the top bar's clock and the dashboard's sessions card, each on its own switch, and the four
# trading sessions, each followed or not, with its hours in its own city's time (whole hours of the city's working
# day; the web app shows them in the viewer's time, with each city's daylight saving). Nothing the connector needs
SESSIONS = ("sydney", "tokyo", "london", "newyork")
MARKET_DEFAULT = {"pill": True, "widget": True, "sessions": {
    "sydney": {"on": True, "from": 7, "to": 16}, "tokyo": {"on": True, "from": 9, "to": 18},
    "london": {"on": True, "from": 8, "to": 17}, "newyork": {"on": True, "from": 8, "to": 17}}}


def _hour(v, lo: int, hi: int) -> int:
    if isinstance(v, bool):
        raise ValueError("invalid_number")
    try:
        h = float(v)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("invalid_number")
    if not math.isfinite(h):
        raise ValueError("invalid_number")
    return int(max(lo, min(hi, round(h))))


def clamp_market(body, cur=None) -> dict:
    """A clean copy of the market clock's settings: `body`'s keys over `cur`'s (the stored block; none or not an object:
    the defaults), per session and per field, so a save of one switch or one session keeps the rest; unknown keys are
    dropped. A session opens at a whole hour 0-23 and closes at 1-24, after it opens (a close at or before the opening
    is moved to an hour after it). Raises ValueError("invalid_market") for a body or a session that is no object or a
    switch that is no true / false, "invalid_number" for an hour that is no number."""
    if body is None:
        body = {}
    if not isinstance(body, dict) or not isinstance(body.get("sessions", {}), dict):
        raise ValueError("invalid_market")
    if any(k in body and not isinstance(body[k], bool) for k in ("pill", "widget")):
        raise ValueError("invalid_market")
    base = cur if isinstance(cur, dict) else MARKET_DEFAULT
    out = {k: bool(body[k]) if k in body else bool(base.get(k, MARKET_DEFAULT[k])) for k in ("pill", "widget")}
    got, had = body.get("sessions", {}), base.get("sessions") if isinstance(base.get("sessions"), dict) else {}
    out["sessions"] = {}
    for name in SESSIONS:
        b, d = got.get(name, {}), MARKET_DEFAULT["sessions"][name]
        c = had.get(name) if isinstance(had.get(name), dict) else d
        if not isinstance(b, dict) or ("on" in b and not isinstance(b["on"], bool)):
            raise ValueError("invalid_market")
        frm = _hour(b.get("from", c.get("from", d["from"])), 0, 23)
        to = _hour(b.get("to", c.get("to", d["to"])), 1, 24)
        out["sessions"][name] = {"on": bool(b["on"]) if "on" in b else bool(c.get("on", d["on"])),
                                 "from": frm, "to": to if to > frm else frm + 1}
    return out
