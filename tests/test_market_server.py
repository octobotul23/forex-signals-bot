"""v1.3.2's market clock settings on the server: clamp_market (ranges, a close at or before the opening, the stored
block kept for what a save does not send, unknown keys dropped, wrong types refused), db.User.merged_settings (a user
stored before v1.3.2, or with a broken block, gets the defaults), and PUT /api/me/settings through FastAPI's TestClient
on a temp database: a save of one switch or one session keeps the rest, a wrong body is a 400 with its code and changes
nothing, and a clock-only save is never pushed to the connector (a save with another key still is).

    .venv\\Scripts\\python tests\\test_market_server.py

Writes only to a temp folder (its database); never to data/ or logs/. Exit code 0 = every check passed.
"""
import copy
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-market-server-"))
DATA_DIR = ROOT / "data"
os.environ.update(DATABASE_URL=f"sqlite:///{(TMP / 'platform.db').as_posix()}", PRICE_FEED="demo", COOKIE_SECURE="false",
                  SECRET_KEY="market-test-" + "k" * 40, DEMO_SECONDS_PER_BAR="0", ANTHROPIC_API_KEY="",
                  ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="")
_guard = logging.NullHandler()             # server.main attaches logs/server.log unless a handler for it is there
_guard.baseFilename = str((ROOT / "logs" / "server.log").resolve())
_console = logging.StreamHandler()
_console.setLevel(logging.ERROR)
logging.getLogger().handlers[:0] = [_guard, _console]
_data_before = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}

from fastapi.testclient import TestClient  # noqa: E402

import server.main as main                 # noqa: E402
from server.dashboard import MARKET_DEFAULT, clamp_market  # noqa: E402
from server.db import SessionLocal, User   # noqa: E402
from server.db import engine as db_engine  # noqa: E402
from server.engine import engine           # noqa: E402
from server.settings import DATABASE_URL, DEFAULT_USER_SETTINGS  # noqa: E402

H = {"x-fx": "1"}                          # the CSRF header every state-changing API call needs
FAILED: list[str] = []
DEF = copy.deepcopy(MARKET_DEFAULT)


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


def with_session(name, **kw):
    out = copy.deepcopy(DEF)
    out["sessions"][name].update(kw)
    return out


def raises(fn, code) -> bool:
    try:
        fn()
    except ValueError as e:
        return str(e) == code
    return False


def test_clamp():
    check(clamp_market(None) == DEF and clamp_market({}) == DEF and clamp_market({}, "broken") == DEF,
          "nothing sent, nothing stored (or a stored block that is no object): the defaults")
    check(DEFAULT_USER_SETTINGS["market"] == DEF, "a new user's settings carry the defaults")
    got = clamp_market({"sessions": {"london": {"from": 7.4, "to": 30}, "tokyo": {"from": -3, "to": 0}}})
    check(got["sessions"]["london"] == {"on": True, "from": 7, "to": 24}
          and got["sessions"]["tokyo"] == {"on": True, "from": 0, "to": 1},
          "hours rounded and kept in range: opens 0-23, closes 1-24", got["sessions"])
    got = clamp_market({"sessions": {"newyork": {"from": 23, "to": 5}, "sydney": {"from": 10, "to": 10}}})
    check(got["sessions"]["newyork"] == {"on": True, "from": 23, "to": 24}
          and got["sessions"]["sydney"] == {"on": True, "from": 10, "to": 11},
          "a close at or before the opening moves to an hour after it", got["sessions"])
    cur = clamp_market({"pill": False, "sessions": {"tokyo": {"on": False, "from": 10, "to": 19}}})
    got = clamp_market({"widget": False, "sessions": {"tokyo": {"to": 20}}}, cur)
    check(got == {"pill": False, "widget": False,
                  "sessions": {**DEF["sessions"], "tokyo": {"on": False, "from": 10, "to": 20}}},
          "what a save does not send stays as stored: the other switch, the other sessions, the session's other fields",
          got)
    got = clamp_market({"bogus": 1, "sessions": {"paris": {"on": True}, "london": {"on": False, "x": 9}}})
    check(got == with_session("london", on=False), "unknown keys and sessions are dropped", got)
    check(clamp_market({}, {"sessions": {"london": "x"}}) == DEF, "a stored session that is no object: its defaults")
    for body, code in (("on", "invalid_market"), ([], "invalid_market"), ({"sessions": []}, "invalid_market"),
                       ({"sessions": {"london": 8}}, "invalid_market"), ({"pill": "false"}, "invalid_market"),
                       ({"widget": 1}, "invalid_market"), ({"sessions": {"tokyo": {"on": "yes"}}}, "invalid_market"),
                       ({"sessions": {"london": {"from": "eight"}}}, "invalid_number"),
                       ({"sessions": {"london": {"to": True}}}, "invalid_number"),
                       ({"sessions": {"london": {"from": float("nan")}}}, "invalid_number"),
                       ({"sessions": {"london": {"to": None}}}, "invalid_number"),
                       ({"sessions": {"london": {"from": 10 ** 400}}}, "invalid_number")):
        check(raises(lambda: clamp_market(body), code), f"refused with {code}: {body!r:.60}")


def test_merged():
    old = User(email="old@test.local", password_hash="x", settings={"execution": "auto"})
    check(old.merged_settings()["market"] == DEF, "a user stored before v1.3.2 (no market block): the defaults")
    broken = User(email="b@test.local", password_hash="x", settings={"market": {"sessions": {"london": {"from": "x"}}}})
    check(broken.merged_settings()["market"] == DEF, "a stored block that does not clamp: the defaults, no error")
    kept = User(email="k@test.local", password_hash="x", settings={"market": with_session("sydney", on=False)})
    check(kept.merged_settings()["market"] == with_session("sydney", on=False), "a stored block is served as stored")


def test_put():
    async def fake_loop(fn, every, first_delay=0):
        pass

    engine._loop = fake_loop                     # Engine.start creates its tasks, none of them runs
    main.SETUP_FILE = TMP / "setup_token.txt"    # not data/setup_token.txt
    with TestClient(main.app) as client:
        client.post("/api/setup", headers=H, json={"token": main._setup_token, "email": "founder@test.local",
                                                   "password": "founder-pass-1", "name": "Founder"})
        r = client.post("/api/auth/register", headers=H, json={"email": "mia@test.local", "accept_terms": True,
                                                               "password": "mia-pass-12"})
        uid = r.json()["id"]
        token = client.post("/api/connector/tokens", headers=H, json={"label": "test"}).json()["token"]
        r = client.get("/api/me/settings").json()
        check(r["settings"]["market"] == DEF, "GET /api/me/settings: the clock starts at its defaults")
        r = client.put("/api/me/settings", headers=H, json={"market": {"pill": False}})
        check(r.status_code == 200 and r.json()["settings"]["market"] == {**DEF, "pill": False},
              "PUT market.pill alone: the pill off, the rest as it was", r.json().get("settings", {}).get("market"))
        r = client.put("/api/me/settings", headers=H, json={"market": {"sessions": {"tokyo": {"on": False},
                                                                                     "london": {"from": 7, "to": 7}}}})
        want = {**DEF, "pill": False, "sessions": {**DEF["sessions"], "tokyo": {"on": False, "from": 9, "to": 18},
                                                   "london": {"on": True, "from": 7, "to": 8}}}
        check(r.status_code == 200 and r.json()["settings"]["market"] == want,
              "PUT one session's fields: those change (a close at the opening moves), the rest stays",
              r.json().get("settings", {}).get("market"))
        with SessionLocal() as db:
            stored = db.get(User, uid).settings.get("market")
        check(stored == want, "the clock is stored with the user", stored)
        for body, code in (({"market": "on"}, "invalid_market"), ({"market": {"pill": "no"}}, "invalid_market"),
                           ({"market": {"sessions": {"london": {"from": "x"}}}}, "invalid_number")):
            r = client.put("/api/me/settings", headers=H, json=body)
            check(r.status_code == 400 and r.json().get("detail") == code, f"PUT {body} -> 400 {code}",
                  f"{r.status_code} {r.json()}")
        check(client.get("/api/me/settings").json()["settings"]["market"] == want, "a refused save changes nothing")
        r = client.put("/api/me/settings", json={"market": {"widget": False}})
        check(r.status_code in (400, 403), "a save without the CSRF header is refused", r.status_code)

        ws = client.websocket_connect("/ws/connector").__enter__()
        try:
            ws.send_json({"type": "auth", "token": token, "version": "1.3.0", "paper": False,
                          "account": {"login": 7, "server": "Test-Demo", "currency": "USD", "balance": 10000.0,
                                      "equity": 10000.0, "demo": True, "trade_allowed": True}})
            welcome = ws.receive_json()
            check(welcome.get("type") == "welcome", "a fake connector is online", welcome.get("type"))

            def sync():
                ws.send_json({"type": "ping"})
                out = []
                while (m := ws.receive_json())["type"] != "pong":
                    out.append(m)
                return out

            sync()
            client.put("/api/me/settings", headers=H, json={"market": {"widget": False}})
            pushes = [m for m in sync() if m["type"] == "settings"]
            check(not pushes, "a clock-only save is not pushed to the connector", pushes)
            client.put("/api/me/settings", headers=H, json={"market": {"widget": True}, "follow_exits": True})
            pushes = [m for m in sync() if m["type"] == "settings"]
            check(len(pushes) == 1, "a save with another key still is (once)", len(pushes))
        finally:
            try:
                ws.__exit__(None, None, None)
            except Exception:
                pass


def main_():
    check(DATABASE_URL == os.environ["DATABASE_URL"] and str(TMP.as_posix()) in DATABASE_URL, "test database is a temp file")
    try:
        test_clamp()
        test_merged()
        test_put()
    finally:
        db_engine.dispose()
        after = {p.name: p.stat().st_mtime_ns for p in DATA_DIR.glob("*") if p.name in ("setup_token.txt", ".secret_key")}
        check(after == _data_before, "nothing written to data/")
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nall checks passed")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main_()
