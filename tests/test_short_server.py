"""Short-term strategy and the dashboard, server side: the user settings (short_term, dashboard), the trades a
connector reports with their strategy, risk and confidence, /api/trades?strategy= and /api/trades/stats, and the
migration of an older database. FastAPI's TestClient with a fake connector.

    .venv\\Scripts\\python tests\\test_short_server.py

Runs on a throw-away SQLite file with the demo price feed; never writes to data/ or logs/, never uses the
network, never starts the engine's loops. Exit code 0 = every check passed.
"""
import faulthandler
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-short-server-"))
DB_FILE = TMP / "platform.db"
DATA_DIR = ROOT / "data"

# settings are read when server modules are imported, so configure first (.env never overrides these)
os.environ.update(DATABASE_URL=f"sqlite:///{DB_FILE.as_posix()}", PRICE_FEED="demo", COOKIE_SECURE="false",
                  SECRET_KEY="short-test-" + "k" * 40, DEMO_SECONDS_PER_BAR="0", ANTHROPIC_API_KEY="",
                  ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="")
# server.main attaches logs/server.log unless the root logger already has a handler for that file
_guard = logging.NullHandler()
_guard.baseFilename = str((ROOT / "logs" / "server.log").resolve())
_console = logging.StreamHandler()
_console.setLevel(logging.ERROR)
logging.getLogger().handlers[:0] = [_guard, _console]

# a database from before the short-term strategy: user_trades with burst_id but no strategy, one plain trade and
# one burst trade in it
_c = sqlite3.connect(DB_FILE)
_c.execute("""CREATE TABLE user_trades (id INTEGER NOT NULL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE, signal_id INTEGER, ticket INTEGER NOT NULL,
    symbol VARCHAR(20) NOT NULL, side VARCHAR(4) NOT NULL, lots FLOAT NOT NULL, entry_price FLOAT NOT NULL,
    sl FLOAT, tp FLOAT, opened_at DATETIME NOT NULL, closed_at DATETIME, exit_price FLOAT, pnl FLOAT,
    status VARCHAR(10) NOT NULL, demo BOOLEAN NOT NULL, burst_id INTEGER)""")
for i, burst in ((1, None), (2, 7)):
    _c.execute("INSERT INTO user_trades (id, user_id, ticket, symbol, side, lots, entry_price, opened_at, status, demo,"
               " burst_id) VALUES (?, 999, ?, 'EURUSD', 'buy', 0.1, 1.1, '2026-01-01 00:00:00', 'closed', 1, ?)",
               (i, 10 + i, burst))
_c.commit()
_c.close()
_data_before = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import inspect, select     # noqa: E402

import server.main as main                 # noqa: E402
from bot.scalp import DEFAULTS as SHORT_DEFAULTS  # noqa: E402
from bot.scalp import MAX_HOLD_TOP, NEW_USER_MAX_HOLD  # noqa: E402
from server.dashboard import DEFAULT as DASH_DEFAULT  # noqa: E402
from server.db import SessionLocal, Signal, User, UserTrade, utcnow  # noqa: E402
from server.db import engine as db_engine  # noqa: E402
from server.engine import engine           # noqa: E402
from server.settings import DATABASE_URL   # noqa: E402

H = {"x-fx": "1"}                          # the CSRF header every state-changing API call needs
FAILED: list[str] = []
OPEN: list = []
NEW_SHORT = {**SHORT_DEFAULTS, "max_hold_seconds": NEW_USER_MAX_HOLD}   # a user created from v1.3.0 on: Y set
NEW_PAIRS = ("XAUUSD", "GBPJPY")           # the pairs added in v1.3.0: a connector before it never gets them


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


def session_of(client) -> str:
    tok = client.cookies.get("fx_session")
    client.cookies.clear()
    return tok


def use(client, tok: str):
    client.cookies.clear()
    client.cookies.set("fx_session", tok)


def connect(client, token: str, version="1.3.0", demo=True):
    """A fake connector: the hello the real one sends, then the server's welcome."""
    ws = client.websocket_connect("/ws/connector").__enter__()
    OPEN.append(ws)
    ws.send_json({"type": "auth", "token": token, "version": version, "paper": False,
                  "account": {"login": 7, "server": "Test-Demo", "currency": "USD", "balance": 10000.0,
                              "equity": 10000.0, "demo": demo, "trade_allowed": True}})
    return ws, ws.receive_json()


def close(ws):
    OPEN.remove(ws)
    try:
        ws.__exit__(None, None, None)
    except Exception:
        pass


def sync(ws) -> list[dict]:
    """Everything the server sent so far, once it has handled everything we sent (ping -> pong)."""
    ws.send_json({"type": "ping"})
    out = []
    while (m := ws.receive_json())["type"] != "pong":
        out.append(m)
    return out


def err(r) -> str:
    return f"{r.status_code} {r.json().get('detail')}"


def trade(ticket: int, **kw) -> dict:
    return {"ticket": ticket, "symbol": "EURUSD", "side": "buy", "lots": 0.5, "entry_price": 1.1, "sl": 1.0995,
            "tp": None, "demo": True, "opened_at": utcnow().isoformat(), **kw}


def rows_of(user_id: int) -> dict[int, UserTrade]:
    with SessionLocal() as db:
        return {t.ticket: t for t in db.scalars(select(UserTrade).where(UserTrade.user_id == user_id))}


def test_flow():
    async def fake_loop(fn, every, first_delay=0):
        pass

    engine._loop = fake_loop                     # Engine.start creates its tasks, none of them runs
    main.SETUP_FILE = TMP / "setup_token.txt"    # not data/setup_token.txt
    with TestClient(main.app) as client:
        try:
            _flow(client)
        finally:
            for ws in list(OPEN):             # an open socket keeps the TestClient from shutting down
                close(ws)


def _flow(client):
    # ── the older database ──
    cols = {c["name"] for c in inspect(db_engine).get_columns("user_trades")}
    check({"strategy", "risk", "confidence"} <= cols, "init_db adds strategy, risk and confidence to user_trades", cols)
    old = rows_of(999)
    check(old[11].strategy == "long" and old[12].strategy == "burst" and old[11].risk is None,
          "existing trades get their strategy: burst when they carry a burst, long otherwise",
          {k: v.strategy for k, v in old.items()})
    try:
        main.init_db()
        check(True, "init_db runs again on a migrated database")
    except Exception as e:
        check(False, "init_db runs again on a migrated database", e)

    # ── accounts ──
    client.post("/api/setup", headers=H, json={"token": main._setup_token, "email": "founder@test.local",
                                               "password": "founder-pass-1", "name": "Founder"})
    founder = session_of(client)
    ids = {}
    for name in ("alice", "bob", "carol"):
        r = client.post("/api/auth/register", headers=H, json={"email": f"{name}@test.local", "accept_terms": True,
                                                               "password": f"{name}-pass-1"})
        ids[name] = (session_of(client), r.json()["id"])
    (alice, alice_id), (bob, bob_id), (carol, carol_id) = ids["alice"], ids["bob"], ids["carol"]
    use(client, alice)
    token = client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"]

    # ── settings ──
    r = client.get("/api/me/settings").json()
    symbols = r["symbols"]
    check(r["settings"]["short_term"] == NEW_SHORT and r["settings"]["dashboard"] == DASH_DEFAULT,
          "GET /api/me/settings: short_term and dashboard start at their defaults (a new user's Y: 120 s)")
    check(SHORT_DEFAULTS["enabled"] is False, "short-term starts switched off")
    ws, welcome = connect(client, token)
    check(welcome.get("type") == "welcome" and welcome.get("symbols") == symbols
          and welcome["settings"]["short_term"] == NEW_SHORT,
          "the connector's welcome carries the short-term settings and the pairs the engine trades",
          {k: welcome.get(k) for k in ("type", "symbols")})

    r = client.put("/api/me/settings", headers=H, json={"short_term": {
        "enabled": True, "risk_unit": "usd", "risk_min": 50, "risk_max": 1, "hold_seconds": 5, "max_open": 99,
        "daily_loss": 0, "loss_unit": "usd", "max_spread_pips": 99, "symbols": ["EURUSD", "XXXYYY", 3, "EURUSD"],
        "bogus": 1}})
    st = r.json()["settings"]["short_term"]
    check(r.status_code == 200 and st == {"enabled": True, "risk_unit": "usd", "risk_min": 1.0, "risk_max": 50.0,
                                          "hold_seconds": 10, "loss_unit": "usd", "daily_loss": 1.0, "max_open": 10,
                                          "max_spread_pips": 10.0, "symbols": ["EURUSD"], "max_hold_seconds": 120},
          "PUT short_term: every range, min/max swapped, the pairs the engine trades only, unknown keys dropped", st)
    pushed = [m for m in sync(ws) if m["type"] == "settings"]
    check(len(pushed) == 1 and pushed[0]["settings"]["short_term"] == st, "a short-term change reaches the connector",
          pushed)
    check(pushed and pushed[0].get("symbols") == symbols, "the settings push carries the pairs the engine trades",
          pushed and pushed[0].get("symbols"))
    r = client.put("/api/me/settings", headers=H, json={"short_term": {"risk_unit": "pct"}})
    st2 = r.json()["settings"]["short_term"]
    check(st2["risk_unit"] == "pct" and (st2["risk_min"], st2["risk_max"]) == (0.5, 2.0) and st2["daily_loss"] == 1.0,
          "changing the unit alone starts its amounts from that unit's defaults (50 $ never becomes 50 %)", st2)
    sync(ws)
    for body, code in (({"short_term": "on"}, "invalid_short_term"),
                       ({"short_term": {"risk_min": "lots"}}, "invalid_number"),
                       ({"short_term": {"hold_seconds": float("inf")}}, None),
                       ({"short_term": {"risk_unit": "eur"}}, "invalid_unit"),
                       ({"short_term": {"symbols": "EURUSD"}}, "invalid_symbols"),
                       ({"dashboard": {"tiles": "all"}}, "invalid_dashboard"),
                       ({"dashboard": {"tiles": [{"k": "pnl", "days": "week"}]}}, "invalid_number"),
                       ({"short_term": {"hold_seconds": 10**400}}, "invalid_number"),    # an int too big for a float
                       ({"short_term": {"max_open": -10**400}}, "invalid_number")):
        try:
            r = client.put("/api/me/settings", headers=H, json=body)
            ok = r.status_code == 400 and (code is None or r.json().get("detail") == code)
            detail = err(r)
        except ValueError:                       # the client cannot even send Infinity as JSON: just as well
            ok, detail = True, ""
        check(ok, f"PUT {str(body)[:70]} -> 400 {code or ''}".rstrip(), detail)
    real_clamp = main.clamp_short

    def refuse(*_a, **_k):                       # what bot/scalp.py raises for e.g. {"enabled": "false"}
        raise ValueError("invalid_enabled")
    main.clamp_short = refuse
    try:
        r = client.put("/api/me/settings", headers=H, json={"short_term": {"enabled": "false"}})
    finally:
        main.clamp_short = real_clamp
    check(r.status_code == 400 and r.json().get("detail") == "invalid_enabled",
          "a ValueError of clamp_short is a 400 with its code as the detail (invalid_enabled)", err(r))
    check(client.get("/api/me/settings").json()["settings"]["short_term"] == st2, "a refused PUT changes nothing")
    check(not [m for m in sync(ws) if m["type"] == "settings"], "and sends the connector nothing")

    tiles = [{"k": "win_rate", "days": 7, "scope": "short"}, {"k": "nope"}, {"k": "pnl", "days": 9999},
             {"k": "equity", "days": 3, "scope": "short"}, {"k": "trades", "scope": "news"},
             {"k": "avg_trade", "days": 14, "scope": "custom"}, {"k": "open_trades", "scope": "test"}]
    r = client.put("/api/me/settings", headers=H, json={"dashboard": {"tiles": tiles}})
    check(r.status_code == 200 and r.json()["settings"]["dashboard"] == {"tiles": [
        {"k": "win_rate", "days": 7, "scope": "short"}, {"k": "pnl", "days": 365, "scope": "all"}, {"k": "equity"},
        {"k": "trades", "days": 30, "scope": "news"}, {"k": "avg_trade", "days": 14, "scope": "custom"},
        {"k": "open_trades", "scope": "all"}]},
        "PUT dashboard: tiles clamped, unknown kinds dropped, news and custom scopes kept (test is no scope)",
        r.json().get("settings", {}).get("dashboard"))
    check(not [m for m in sync(ws) if m["type"] == "settings"], "a dashboard change is not pushed to the connector")
    r = client.put("/api/me/settings", headers=H, json={"dashboard": {"tiles": []}})
    check(r.json()["settings"]["dashboard"] == {"tiles": []}, "an empty dashboard is allowed (every tile hidden)")
    use(client, bob)
    check(client.get("/api/me/settings").json()["settings"]["dashboard"] == DASH_DEFAULT,
          "another user's dashboard is untouched")
    use(client, alice)

    # stored settings that went bad (an older release, a hand edit) never break the page
    with SessionLocal() as db:
        u = db.get(User, alice_id)
        u.settings = {**u.settings, "short_term": "junk", "dashboard": {"tiles": "x"}}
        db.commit()
    r = client.get("/api/me/settings")
    check(r.status_code == 200 and r.json()["settings"]["short_term"] == SHORT_DEFAULTS
          and r.json()["settings"]["dashboard"] == DASH_DEFAULT, "bad stored values read back as the defaults",
          r.text[:300])
    with SessionLocal() as db:
        u = db.get(User, alice_id)
        u.settings = {**u.settings, "short_term": {"risk_min": "x", "hold_seconds": 1e9, "symbols": [{"a": 1}]}}
        db.commit()
    st3 = client.get("/api/me/settings").json()["settings"]["short_term"]
    check(st3["risk_min"] == SHORT_DEFAULTS["risk_min"] and st3["hold_seconds"] == 300 and st3["symbols"] == [],
          "a bad stored value goes back to its default, the others are clamped", st3)

    # ── short-term pairs the engine does not trade (an admin removed them): left out wherever settings go out ──
    for uid, pairs in ((alice_id, ["EURUSD", "GBPUSD", "EURGBP"]), (bob_id, ["GBPUSD", "EURGBP"])):
        with SessionLocal() as db:
            u = db.get(User, uid)
            u.settings = {**(u.settings or {}), "short_term": {**SHORT_DEFAULTS, "enabled": True, "symbols": pairs}}
            db.commit()
    got = client.get("/api/me/settings").json()["settings"]["short_term"]["symbols"]
    check(got == ["EURUSD", "GBPUSD"], "GET settings leaves out a stored short-term pair the engine does not trade", got)
    r = client.put("/api/me/settings", headers=H, json={"follow_exits": True})
    pushed = [m for m in sync(ws) if m["type"] == "settings"]
    check(r.json()["settings"]["short_term"]["symbols"] == ["EURUSD", "GBPUSD"] and len(pushed) == 1
          and pushed[0]["settings"]["short_term"]["symbols"] == ["EURUSD", "GBPUSD"] and pushed[0]["symbols"] == symbols,
          "a PUT that leaves short_term alone: its reply and push leave it out too, the push carries the engine's pairs",
          pushed)
    use(client, bob)
    bob_token = client.post("/api/connector/tokens", headers=H, json={"label": "bob"}).json()["token"]
    bws, bob_welcome = connect(client, bob_token, demo=False)
    check(bob_welcome["settings"]["short_term"]["symbols"] == ["GBPUSD"], "and so does the connector's welcome",
          bob_welcome["settings"]["short_term"])

    # an admin changes the engine's pairs: each connector online gets its user's settings again, with the new pairs
    use(client, founder)
    fewer = ["EURUSD", "USDJPY"]
    r = client.put("/api/admin/engine", headers=H, json={"symbols": fewer})
    a, b = ([m for m in sync(x) if m["type"] == "settings"] for x in (ws, bws))
    check(r.status_code == 200 and len(a) == 1 and a[0]["symbols"] == fewer
          and a[0]["settings"]["short_term"]["symbols"] == ["EURUSD"],
          "admin removes pairs: the connector gets its settings again, with the engine's new pairs", a or err(r))
    check(len(b) == 1 and b[0]["symbols"] == fewer and b[0]["settings"]["short_term"]["symbols"] == [],
          "every connector online gets its own user's settings", b)
    check(b and b[0]["settings"]["short_term"]["enabled"] is False
          and b[0]["settings"].get("short_term_note") == "pairs_gone"
          and a and a[0]["settings"]["short_term"]["enabled"] is True and "short_term_note" not in a[0]["settings"],
          "none of a user's pairs left: short-term is served off (no pair would mean every pair), the others keep it",
          b and b[0]["settings"])
    use(client, bob)
    web = client.get("/api/me/settings").json()["settings"]
    check(web["short_term"]["enabled"] is True and web["short_term"]["symbols"] == []
          and web.get("short_term_note") == "pairs_gone",
          "the web app gets the stored switch, no pairs and the note that says why", web.get("short_term"))

    def bob_st():
        with SessionLocal() as db:
            return db.get(User, bob_id).settings["short_term"]

    def bob_pushed():
        return [m["settings"]["short_term"] for m in sync(bws) if m["type"] == "settings"]

    def web_put(**patch):          # what the web app sends: the whole short-term object it was served (saveShort)
        return client.put("/api/me/settings", headers=H, json={"short_term": {**web["short_term"], **patch,
                                                                               "symbols_picked": "symbols" in patch}})

    r, st, p = web_put(hold_seconds=45), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["symbols"] == ["GBPUSD", "EURGBP"] and st["enabled"] is True
          and st["hold_seconds"] == 45 and r.json()["settings"]["short_term"]["enabled"] is True
          and r.json()["settings"].get("short_term_note") == "pairs_gone"
          and p and p[-1]["enabled"] is False and p[-1]["symbols"] == [],
          "the web app's save (its empty pairs) keeps the stored pairs and switch; the connector still gets it off",
          (st, p))
    r, st = client.put("/api/me/settings", headers=H, json={"short_term": {"hold_seconds": 50}}), bob_st()
    check(r.status_code == 200 and st["symbols"] == ["GBPUSD", "EURGBP"] and st["enabled"] is True
          and st["hold_seconds"] == 50, "a short-term save without pairs keeps them too", st)
    bob_pushed()
    r, st, p = web_put(enabled=False), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["enabled"] is False and st["symbols"] == ["GBPUSD", "EURGBP"]
          and r.json()["settings"]["short_term"]["enabled"] is False and p and p[-1]["enabled"] is False,
          "the switch turned off from the banner: stored off, the pairs kept", (st, p))
    r, st, p = web_put(enabled=True), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["enabled"] is True and st["symbols"] == ["GBPUSD", "EURGBP"]
          and r.json()["settings"]["short_term"]["enabled"] is True and p and p[-1]["enabled"] is False
          and p[-1]["symbols"] == [], "turned on again with no traded pair: stored on, the connector still off", (st, p))
    r, st, p = web_put(symbols=[]), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["symbols"] == ["GBPUSD", "EURGBP"] and p and p[-1]["enabled"] is False,
          "a pair picked and unpicked again on the banner (picked, but []): the stored pairs stay", (st, p))
    use(client, founder)           # the admin adds GBPUSD back; bob's page, open since, still shows the banner
    client.put("/api/admin/engine", headers=H, json={"symbols": fewer + ["GBPUSD"]})
    sync(ws), bob_pushed()
    use(client, bob)
    r, st, p = web_put(hold_seconds=33), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["symbols"] == ["GBPUSD", "EURGBP"] and st["hold_seconds"] == 33
          and p and p[-1]["enabled"] is True and p[-1]["symbols"] == ["GBPUSD"],
          "a page left open from before a pair came back: its save (its old empty list, not picked) keeps the stored "
          "pairs, never 'every pair'", (st, p))
    use(client, founder)
    client.put("/api/admin/engine", headers=H, json={"symbols": fewer})
    sync(ws), bob_pushed()
    use(client, bob)
    r = web_put(symbols=["GBPUSD"])
    check(r.status_code == 400 and r.json().get("detail") == "pairs_not_traded",
          "pairs chosen of which none is traded: 400 pairs_not_traded, never stored as 'every pair'", err(r))
    check(not bob_pushed() and bob_st()["symbols"] == ["GBPUSD", "EURGBP"], "and nothing is stored or pushed")
    r, st, p = web_put(symbols=["EURUSD"]), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["symbols"] == ["EURUSD"] and st["enabled"] is True
          and "short_term_note" not in r.json()["settings"] and p and p[-1]["enabled"] is True
          and p[-1]["symbols"] == ["EURUSD"],
          "a traded pair picked from the banner: short-term trades it again, as the stored switch says", (st, p))
    web = r.json()["settings"]
    r, st, p = web_put(symbols=[]), bob_st(), bob_pushed()
    check(r.status_code == 200 and st["symbols"] == [] and p and p[-1]["symbols"] == [] and p[-1]["enabled"] is True,
          "with a traded pair stored, an empty list is every pair again (the user's choice)", (st, p))
    with SessionLocal() as db:     # back as it was for what follows
        u = db.get(User, bob_id)
        u.settings = {**u.settings, "short_term": {**SHORT_DEFAULTS, "enabled": True, "symbols": ["GBPUSD", "EURGBP"]}}
        db.commit()
    # some of alice's pairs removed: her page shows the one left; its saves keep the removed ones for when they return
    use(client, alice)
    shown = client.get("/api/me/settings").json()["settings"]["short_term"]
    for body in ({**shown, "hold_seconds": 41, "symbols_picked": False}, {**shown, "hold_seconds": 42}):
        r = client.put("/api/me/settings", headers=H, json={"short_term": body})
        with SessionLocal() as db:
            st = db.get(User, alice_id).settings["short_term"]
        check(r.status_code == 200 and shown["symbols"] == ["EURUSD"] and st["hold_seconds"] == body["hold_seconds"]
              and st["symbols"] == ["EURUSD", "GBPUSD", "EURGBP"],
              "a save from the page, with or without the flag (an older page), keeps the pairs it was not shown", st)
    r = client.put("/api/me/settings", headers=H, json={"short_term": {**shown, "symbols": ["EURUSD", "USDJPY"],
                                                                       "symbols_picked": True}})
    with SessionLocal() as db:
        st = db.get(User, alice_id).settings
        check(r.status_code == 200 and st["short_term"]["symbols"] == ["EURUSD", "USDJPY"]
              and "symbols_picked" not in st["short_term"], "pairs picked on the page are stored (the flag is not)", st)
        st["short_term"] = {**st["short_term"], "symbols": ["EURUSD", "GBPUSD", "EURGBP"]}   # back as it was
        db.get(User, alice_id).settings = {**st}
        db.commit()
    sync(ws)
    use(client, founder)
    r = client.put("/api/admin/engine", headers=H, json={"symbols": fewer, "trial_days": 3})
    check(r.status_code == 200 and not [m for m in sync(ws) + sync(bws) if m["type"] == "settings"],
          "the same pairs again, or other engine settings: nothing is pushed")
    client.put("/api/admin/engine", headers=H, json={"symbols": symbols})
    a = [m for m in sync(ws) if m["type"] == "settings"]
    sync(bws)
    use(client, alice)
    both = ["EURUSD", "GBPUSD"]
    check(len(a) == 1 and a[0]["symbols"] == symbols and a[0]["settings"]["short_term"]["symbols"] == both
          and client.get("/api/me/settings").json()["settings"]["short_term"]["symbols"] == both,
          "a pair added back is the user's again (the stored list keeps it)", a)

    # ── trades with their strategy ──
    sid = 424242
    open101 = {"type": "trade_opened", "trade": trade(101, signal_id=None, strategy="short", risk=12.5, confidence=50)}
    ws.send_json(open101)
    ws.send_json({"type": "trade_opened", "trade": trade(102, signal_id=sid, side="sell")})
    ws.send_json({"type": "trade_opened", "trade": trade(105, signal_id=None, strategy="short", risk=3,
                                                         confidence="high")})
    sync(ws)
    rows = rows_of(alice_id)
    check(rows[101].strategy == "short" and rows[101].risk == 12.5 and rows[101].confidence == 50
          and rows[101].signal_id is None, "a scalp is booked as short with its risk and confidence",
          rows[101].to_dict())
    check(rows[102].strategy == "long" and rows[102].signal_id == sid and rows[102].risk is None,
          "a trade of a signal that is not stored (any more) is long", rows[102].to_dict())
    check(rows[105].confidence is None, "a confidence that is no number is left empty")
    d = rows[101].to_dict()
    check(d.get("strategy") == "short" and d.get("risk") == 12.5 and d.get("confidence") == 50,
          "the trade's dict (API, WebSocket) carries strategy, risk and confidence", d)

    now = utcnow()
    ws.send_json({"type": "trade_closed", "trade": {"ticket": 101, "exit_price": 1.099, "pnl": -5.0,
                                                    "closed_at": now.isoformat()}})
    ws.send_json({"type": "trade_closed", "trade": {"ticket": 102, "exit_price": 1.098, "pnl": 20.0,
                                                    "closed_at": now.isoformat()}})
    # a scalp whose open never reached the server: booked from its close
    ws.send_json({"type": "trade_closed", "trade": trade(103, strategy="short", risk=4, confidence=80, pnl=6.0,
                                                         exit_price=1.1012,
                                                         closed_at=(now - timedelta(days=10)).isoformat())})
    # a close for a trade nobody knows, that says nothing of itself: ignored (as before)
    ws.send_json({"type": "trade_closed", "trade": {"ticket": 104, "pnl": 99.0, "closed_at": now.isoformat()}})
    sync(ws)
    rows = rows_of(alice_id)
    check(rows[103].strategy == "short" and rows[103].status == "closed" and rows[103].pnl == 6.0
          and rows[103].risk == 4, "a scalp's close with no open before it is booked as short", rows[103].to_dict())
    check(104 not in rows, "an unknown close that carries no strategy is still ignored")
    # the connector's outbox may send an open again after its close (a save of outbox.json that failed): the very
    # same message
    ws.send_json(open101)
    sync(ws)
    r101 = rows_of(alice_id)[101]
    check(r101.status == "closed" and r101.pnl == -5.0 and r101.closed_at is not None,
          "an open sent again after its close does not reopen the trade", r101.to_dict())

    r = client.get("/api/trades", params={"strategy": "short"}).json()
    check(sorted(t["ticket"] for t in r["trades"]) == [101, 103, 105] and r["summary"]["closed"] == 2,
          "/api/trades?strategy=short: the scalps only", [t["ticket"] for t in r["trades"]])
    r = client.get("/api/trades", params={"strategy": "long"}).json()
    check([t["ticket"] for t in r["trades"]] == [102], "/api/trades?strategy=long", r["trades"])
    check(len(client.get("/api/trades", params={"strategy": "bogus"}).json()["trades"]) == 4,
          "an unknown strategy filter lists every trade")

    # ── /api/trades/stats ──
    r = client.get("/api/trades/stats", params={"days": "7,30"})
    s = r.json()
    p7, p30 = s["periods"]["7"], s["periods"]["30"]
    check(r.status_code == 200 and set(s["periods"]) == {"7", "30"}, "one block per period asked", s)
    check(p7["all"] == {"closed": 2, "wins": 1, "win_rate": 50.0, "pnl": 15.0, "avg": 7.5, "profit_factor": 4.0},
          "7 days, all strategies: closed, wins, win rate, P&L, average, profit factor", p7["all"])
    check(p7["short"]["closed"] == 1 and p7["short"]["pnl"] == -5.0 and p7["short"]["profit_factor"] == 0.0
          and p7["short"]["win_rate"] == 0.0, "7 days, short: only the scalp closed in the last 7 days", p7["short"])
    check(p30["short"] == {"closed": 2, "wins": 1, "win_rate": 50.0, "pnl": 1.0, "avg": 0.5, "profit_factor": 1.2},
          "30 days, short: the 10-day-old scalp counts too", p30["short"])
    check(p30["long"]["closed"] == 1 and p30["long"]["pnl"] == 20.0 and p30["burst"]["closed"] == 0
          and p30["burst"]["win_rate"] is None, "30 days: long and burst apart", (p30["long"], p30["burst"]))
    check(s["open"] == {"all": 1, "long": 0, "short": 1, "burst": 0, "news": 0, "custom": 0, "rapid": 0},
          "the trades open now, by strategy", s["open"])
    check(s["account"] == "demo", "the numbers say which account they are about", s.get("account"))
    s = client.get("/api/trades/stats", params={"days": "0,9999, 30"}).json()
    check(set(s["periods"]) == {"1", "365", "30"}, "periods are clamped to 1..365", list(s["periods"]))
    check(client.get("/api/trades/stats").json()["periods"].keys() == {"30"}, "no period asked: 30 days")
    check(client.get("/api/trades/stats", params={"days": "week"}).status_code == 400, "a period that is no number: 400")
    check(len(client.get("/api/trades/stats", params={"days": ",".join(map(str, range(1, 16)))}).json()["periods"])
          == 12, "at most 12 periods")
    check(client.get("/api/trades/stats", params={"days": "1," * 40}).status_code == 422, "a too long list is refused")

    # ── a signal's trade follows the signal's mode; a test signal's trade is test ──
    with SessionLocal() as db:
        sigs = {m: Signal(mode=m, symbol="EURUSD", side="buy", price=1.1, sl=1.09, tp=1.12, status="closed")
                for m in ("trend", "news_ai", "custom")}
        db.add_all(sigs.values())
        db.commit()
        sig_ids = {m: x.id for m, x in sigs.items()}
    booked = ((111, sig_ids["trend"]), (112, sig_ids["news_ai"]), (113, sig_ids["custom"]),
              (114, main.TEST_SIGNAL_BASE + 77))
    for ticket, signal_id in booked:
        ws.send_json({"type": "trade_opened", "trade": trade(ticket, signal_id=signal_id)})
    sync(ws)
    rows = rows_of(alice_id)
    check([rows[x].strategy for x, _ in booked] == ["long", "news", "custom", "test"],
          "a signal's trade is booked by the signal's mode: trend long, news_ai news, custom custom; a test one test",
          [rows[x].strategy for x, _ in booked])
    for name, want in (("news", [112]), ("custom", [113])):
        got = [x["ticket"] for x in client.get("/api/trades", params={"strategy": name}).json()["trades"]]
        check(got == want, f"/api/trades?strategy={name}", got)
    s = client.get("/api/trades/stats").json()
    check(s["open"] == {"all": 5, "long": 1, "short": 1, "burst": 0, "news": 1, "custom": 1, "rapid": 0},
          "open now: news and custom apart, a test trade in all only", s["open"])
    for ticket, pnl in ((111, 1.0), (112, -2.0), (113, 3.0), (114, 10.0)):
        ws.send_json({"type": "trade_closed", "trade": {"ticket": ticket, "exit_price": 1.1, "pnl": pnl,
                                                        "closed_at": utcnow().isoformat()}})
    sync(ws)
    p7 = client.get("/api/trades/stats", params={"days": "7"}).json()["periods"]["7"]
    check((p7["long"]["closed"], p7["long"]["pnl"], p7["news"]["pnl"], p7["custom"]["pnl"]) == (2, 21.0, -2.0, 3.0),
          "7 days: long (trend only), news and custom apart", p7)
    check(p7["all"]["closed"] == 6 and p7["all"]["pnl"] == 27.0,
          "all counts every trade, the test one too (15 + 1 - 2 + 3 + 10)", p7["all"])

    # ── demo and real money apart ──
    ws.send_json({"type": "trade_opened", "trade": trade(120, signal_id=sig_ids["trend"], demo=False)})
    ws.send_json({"type": "trade_opened", "trade": trade(121, strategy="short", demo=False)})
    ws.send_json({"type": "trade_closed", "trade": {"ticket": 120, "exit_price": 1.2, "pnl": 50.0,
                                                    "closed_at": utcnow().isoformat()}})
    sync(ws)
    s = client.get("/api/trades/stats", params={"days": "7"}).json()
    check(s["account"] == "demo" and s["periods"]["7"]["all"]["closed"] == 6 and s["open"]["all"] == 1,
          "by default the account the connector reports (demo here, although the newest trade is real)", s)
    s = client.get("/api/trades/stats", params={"days": "7", "account": "real"}).json()
    check(s["account"] == "real" and s["periods"]["7"]["all"]["closed"] == 1 and s["periods"]["7"]["long"]["pnl"] == 50.0
          and s["open"] == {"all": 1, "long": 0, "short": 1, "burst": 0, "news": 0, "custom": 0, "rapid": 0},
          "?account=real: the real-money trades only, open ones too", s)
    s = client.get("/api/trades/stats", params={"days": "7", "account": "demo"}).json()
    check(s["account"] == "demo" and s["periods"]["7"]["all"]["pnl"] == 27.0, "?account=demo", s)
    r = client.get("/api/trades/stats", params={"account": "both"})
    check(r.status_code == 400 and r.json().get("detail") == "invalid_account", "an unknown account: 400", err(r))
    # paper mode numbers its tickets from 1001 again at each start: an open on the ticket of a trade closed before,
    # opened at another time, is a new trade (open, nothing of the old result left)
    ws.send_json({"type": "trade_opened", "trade": trade(103, strategy="short", risk=2)})
    sync(ws)
    r103 = rows_of(alice_id)[103]
    check(r103.status == "open" and r103.pnl is None and r103.exit_price is None and r103.closed_at is None
          and r103.risk == 2, "an open on a closed trade's ticket, opened at another time: a new trade", r103.to_dict())
    use(client, bob)
    check(client.get("/api/trades/stats").json()["account"] == "real",
          "a connector on a real account: real by default, with no trade yet")
    use(client, carol)
    check(client.get("/api/trades/stats").json()["account"] == "demo", "no connector, no trade: demo")
    with SessionLocal() as db:
        for ticket, demo, hours, pnl in ((1, True, 48, 5.0), (2, False, 1, -3.0)):
            opened = utcnow() - timedelta(hours=hours)
            db.add(UserTrade(user_id=carol_id, ticket=ticket, symbol="EURUSD", side="buy", lots=0.1, entry_price=1.1,
                             opened_at=opened, closed_at=opened, pnl=pnl, status="closed", demo=demo, strategy="long"))
        db.commit()
    s = client.get("/api/trades/stats").json()
    check(s["account"] == "real" and s["periods"]["30"]["all"]["pnl"] == -3.0,
          "no connector: the account of the newest trade", s)
    use(client, bob)
    s = client.get("/api/trades/stats", params={"days": "30"}).json()
    check(s["periods"]["30"]["all"]["closed"] == 0 and s["open"]["all"] == 0, "another user sees only their own trades")
    client.cookies.clear()
    check(client.get("/api/trades/stats").status_code == 401, "stats need a login")
    _v130(client, founder, symbols, ws, bws)
    close(ws)


def _v130(client, founder, symbols, ws, bws):
    """v1.3.0: short-term's Y (max_hold_seconds) never under X, and none for a user saved by v1.2.0 until they set
    one; a connector before v1.3.0 never gets the pairs added in it, in the engine's pairs or the short-term ones."""
    old = [x for x in symbols if x not in NEW_PAIRS]
    check(set(NEW_PAIRS) <= set(symbols) and len(old) == len(symbols) - 2, "the engine trades XAUUSD and GBPJPY",
          symbols)
    client.cookies.clear()
    r = client.post("/api/auth/register", headers=H, json={"email": "dave@test.local", "accept_terms": True,
                                                           "password": "dave-pass-1"})
    dave, dave_id = session_of(client), r.json()["id"]
    use(client, dave)

    def put(**patch):
        r = client.put("/api/me/settings", headers=H, json={"short_term": patch})
        with SessionLocal() as db:
            return r, db.get(User, dave_id).settings["short_term"]

    def ys(st):
        return st["hold_seconds"], st.get("max_hold_seconds")

    with SessionLocal() as db:
        st = db.get(User, dave_id).settings["short_term"]
    check(st.get("max_hold_seconds") == NEW_USER_MAX_HOLD == 120, "a new user is stored with Y = 120 s", st)
    r, st = put(hold_seconds=200)
    check(r.status_code == 200 and ys(st) == (200, 200), "an X above Y raises Y to X", st)
    r, st = put(max_hold_seconds=100)
    check(r.status_code == 200 and ys(st) == (200, 200), "a Y under X is raised to X", st)
    r, st = put(max_hold_seconds=10**6)
    check(r.status_code == 200 and ys(st) == (200, MAX_HOLD_TOP), f"Y at most {MAX_HOLD_TOP} s", st)
    r, st = put(hold_seconds=20, max_hold_seconds=45)
    check(r.status_code == 200 and ys(st) == (20, 45)
          and r.json()["settings"]["short_term"]["max_hold_seconds"] == 45, "X and Y set together", st)
    r, st2 = put(max_hold_seconds="soon")
    check(r.status_code == 400 and r.json().get("detail") == "invalid_number" and st2 == st,
          "a Y that is no number: 400 invalid_number, nothing stored", err(r))
    r, st = put(hold_seconds=25)
    check(r.status_code == 200 and ys(st) == (25, 45), "a PUT without Y keeps the stored one", st)
    r, st = put(max_hold_seconds=None)
    check(r.status_code == 200 and "max_hold_seconds" not in st and st["hold_seconds"] == 25
          and "max_hold_seconds" not in r.json()["settings"]["short_term"],
          "Y sent as null: removed, it follows X again (as v1.2.0 held a trade)", st)
    r, st = put(hold_seconds=30)
    check(r.status_code == 200 and ys(st) == (30, None), "... and stays removed", st)
    r, st = put(max_hold_seconds=45)
    check(r.status_code == 200 and ys(st) == (30, 45), "set again after it", st)

    # dave as v1.2.0 stored him: short-term without Y, on a new pair too
    with SessionLocal() as db:
        u = db.get(User, dave_id)
        u.settings = {**u.settings, "short_term": {**SHORT_DEFAULTS, "enabled": True,
                                                   "symbols": ["EURUSD", "XAUUSD", "GBPJPY"]}}
        db.commit()
    web = client.get("/api/me/settings").json()["settings"]["short_term"]
    check("max_hold_seconds" not in web and web["symbols"] == ["EURUSD", "XAUUSD", "GBPJPY"],
          "a v1.2.0 user: no Y (it follows X); the web app gets every pair", web)
    token = client.post("/api/connector/tokens", headers=H, json={"label": "dave"}).json()["token"]
    dws, welcome = connect(client, token, "1.2.0")
    st = welcome["settings"]["short_term"]
    check(welcome.get("symbols") == old and st["symbols"] == ["EURUSD"] and st["enabled"] is True
          and "max_hold_seconds" not in st and "short_term_note" not in welcome["settings"],
          "a 1.2.0 connector's welcome: the engine's pairs and the short-term ones without XAUUSD and GBPJPY",
          {"symbols": welcome.get("symbols"), "short_term": st})
    r, st = put(hold_seconds=45)
    pushed = [m for m in sync(dws) if m["type"] == "settings"]
    check(r.status_code == 200 and "max_hold_seconds" not in st and st["hold_seconds"] == 45
          and "max_hold_seconds" not in r.json()["settings"]["short_term"],
          "a v1.2.0 user's PUT that sends no Y stores none", st)
    check(len(pushed) == 1 and pushed[0]["symbols"] == old
          and pushed[0]["settings"]["short_term"]["symbols"] == ["EURUSD"]
          and pushed[0]["settings"]["short_term"]["hold_seconds"] == 45,
          "the settings push to a 1.2.0 connector leaves the new pairs out too", pushed)
    r, st = put(symbols=["XAUUSD"], symbols_picked=True)
    pushed = [m["settings"] for m in sync(dws) if m["type"] == "settings"]
    web = r.json()["settings"]
    check(r.status_code == 200 and st["symbols"] == ["XAUUSD"] and web["short_term"]["symbols"] == ["XAUUSD"]
          and web["short_term"]["enabled"] is True and "short_term_note" not in web,
          "gold alone picked: stored and shown on the web app", (st, web))
    check(len(pushed) == 1 and pushed[0]["short_term"]["enabled"] is False and pushed[0]["short_term"]["symbols"] == []
          and pushed[0].get("short_term_note") == "pairs_gone",
          "a 1.2.0 connector whose user picked new pairs only gets short-term off (no pair would mean every pair)",
          pushed)
    put(symbols=["EURUSD", "XAUUSD", "GBPJPY"], symbols_picked=True)
    sync(dws)

    # the admin changes the engine's pairs: each connector gets them as its version takes them
    use(client, founder)
    fewer = [x for x in symbols if x != "AUDUSD"]
    r = client.put("/api/admin/engine", headers=H, json={"symbols": fewer})
    d, a = ([m for m in sync(x) if m["type"] == "settings"] for x in (dws, ws))
    sync(bws)
    check(r.status_code == 200 and len(d) == 1 and d[0]["symbols"] == [x for x in fewer if x not in NEW_PAIRS]
          and d[0]["settings"]["short_term"]["symbols"] == ["EURUSD"],
          "admin changes the pairs: a 1.2.0 connector gets them without the new ones", d or err(r))
    check(len(a) == 1 and a[0]["symbols"] == fewer, "a 1.3.0 connector gets all of them", a)
    client.put("/api/admin/engine", headers=H, json={"symbols": symbols})
    sync(dws), sync(ws), sync(bws)

    # the same user on a 1.3.0 connector: every pair
    use(client, dave)
    close(dws)
    dws, welcome = connect(client, token, "1.3.0")
    check(welcome.get("symbols") == symbols
          and welcome["settings"]["short_term"]["symbols"] == ["EURUSD", "XAUUSD", "GBPJPY"],
          "a 1.3.0 connector's welcome has every pair, the short-term ones too",
          {"symbols": welcome.get("symbols"), "short_term": welcome["settings"]["short_term"]})
    close(dws)
    client.cookies.clear()


def _timeout():
    print("FAIL timeout: a WebSocket wait never returned", flush=True)
    faulthandler.dump_traceback(all_threads=True)
    os._exit(3)


def main_():
    watchdog = threading.Timer(120, _timeout)
    watchdog.daemon = True
    watchdog.start()
    check(DATABASE_URL == os.environ["DATABASE_URL"] and str(TMP.as_posix()) in DATABASE_URL, "test database is a temp file")
    try:
        test_flow()
    finally:
        watchdog.cancel()
        db_engine.dispose()
        after = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}
        check(after == _data_before, "nothing written to data/")
        check(not any(getattr(h, "baseFilename", "").endswith("server.log") and not isinstance(h, logging.NullHandler)
                      for h in logging.getLogger().handlers), "no handler writing logs/server.log")
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nall checks passed")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main_()
