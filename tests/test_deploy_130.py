"""updates/v1.3.0 deploy_130.py check, the read-only look after the restart, on throw-away copies of a live folder (its
logs, a database, server/main.py, the connector and the bot modules it zips): every item PASSes when v1.3.0 runs well,
and each FAILs on its own (server version, the web page's ?v=, connector 1.3.0, the MT5 feed, new errors, bursts
columns, the migration flag, GBPJPY and XAUUSD traded, their fresh bars, trades without a strategy, the connector
download), with the verdict and the rollback command; log records from before the restart, a broker's refusal and
asyncio's port-probe noise do not count, Thursday's bar is still fresh on a Monday; the database is opened read-only
(a missing one is not created) and stays as it was; the kit's constants match the code (version, pairs, flag, ?v=, log
texts and stamps); the dev tree's connector download carries every bot module the connector imports, also one a bot
module brings in; the real GET goes straight to the server while a proxy is set (as the launcher's); prepare refuses a
files\\ without this release's bot/version.py before touching anything, and runs with it (backups, database copy,
state; on throw-away folders); the command line still offers prepare / copy / rollback, and check's verdict is its exit
code.

    .venv\\Scripts\\python tests\\test_deploy_130.py

Never uses the network: the kit's GET is a fake here, its opener and urlopen are replaced so a slip fails instead of
connecting, and the real GET runs with Python's HTTP handler stubbed (nothing is sent).
Never touches D:\\Forex Bot; writes only to a temp folder, no __pycache__ in updates/. Exit code 0 = every check passed.
"""
import ast
import contextlib
import email.message
import importlib.util
import io
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import urllib.error
import urllib.request
import urllib.response
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KIT = ROOT / "updates" / "v1.3.0" / "deploy_130.py"
TREE = re.search(r'^APP_VERSION = "([^"]+)"', (ROOT / "bot" / "version.py").read_text(encoding="utf-8"), re.M).group(1)
if TREE != "1.3.0":     # this kit's checks are made of v1.3.0's tree (?v=15, its version); a later tree has its own kit
    print(f"skipped: the dev tree is v{TREE}, the v1.3.0 kit is checked against v1.3.0's files "
          "(tests/test_deploy_131.py checks the v1.3.1 kit)")
    sys.exit(0)
TMP =Path(tempfile.mkdtemp(prefix="fx-deploy-130-"))
FAILED: list[str] = []
ASKED: list[str] = []                      # the paths the fake fetch was asked for, in order
OPENED: list[tuple] = []                   # sqlite3.connect calls the kit made: (target, kwargs)
NET: list[str] = []                        # urlopen calls that reached the guard (must stay empty)
NOW = datetime.now(timezone.utc).replace(microsecond=0)
SINCE = (NOW - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")       # the restart, UTC
SIX = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"]
SCHEMA = """
CREATE TABLE bursts (id INTEGER PRIMARY KEY, status VARCHAR(16), kind VARCHAR(8) NOT NULL DEFAULT 'burst',
                     analysis TEXT);
CREATE TABLE engine_state (key VARCHAR(60) PRIMARY KEY, value TEXT);
CREATE TABLE app_settings (key VARCHAR(60) PRIMARY KEY, value TEXT);
CREATE TABLE user_trades (id INTEGER PRIMARY KEY, symbol VARCHAR(20), strategy VARCHAR(8));
"""


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


def load_kit():
    spec = importlib.util.spec_from_file_location("deploy_130", KIT)
    kit = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode, was = True, sys.dont_write_bytecode      # no __pycache__ in updates/
    try:
        spec.loader.exec_module(kit)
    finally:
        sys.dont_write_bytecode = was
    return kit


kit = load_kit()


def stamp(minutes: float) -> str:
    """A log stamp `minutes` from now (negative: before), in local time as the app's logs write it."""
    return (NOW + timedelta(minutes=minutes)).astimezone().strftime("%Y-%m-%d %H:%M:%S") + ",125"


def srv(minutes: float, level: str, msg: str, name: str = "server") -> str:
    """A server.log record (server/main.py _LOG_FORMAT)."""
    return f"{stamp(minutes)} {level:<7} {name}: {msg}"


def plain(minutes: float, level: str, msg: str) -> str:
    """An app.log or connector.log record (the launcher's and the connector's format: no logger name)."""
    return f"{stamp(minutes)} {level:<7} {msg}"


def bar(days: float) -> str:
    """A decided D1 bar's open time `days` ago, as engine_state mode_bars stores it (naive UTC, ISO)."""
    return (NOW - timedelta(days=days)).replace(tzinfo=None, hour=21, minute=0, second=0).isoformat()


SRV_BEFORE = [srv(-120, "ERROR", "engine task failed: an error from before the restart"),
              srv(-119, "INFO", "Connector online for user 1 (v1.2.0)"),
              srv(-118, "WARNING", "price tick XAUUSD: RuntimeError: no history (v1.2.0 never traded it)")]
SRV_AFTER = [srv(-9, "INFO", "Engine started with mt5 feed", "server.engine"),
             srv(-8, "INFO", "Connector online for user 1 (v1.3.0)"),
             # a pair that failed once and loads since: its fresh bar decides, not the warning
             srv(-8, "WARNING", "price tick GBPJPY: RuntimeError: no history yet", "server.engine"),
             srv(-7, "ERROR", "Exception in callback _ProactorBasePipeTransport._call_connection_lost(None)",
                 "asyncio") + "\nhandle: <Handle _ProactorBasePipeTransport._call_connection_lost(None)>\n"
             "Traceback (most recent call last):\n  File \"proactor_events.py\", line 165, in _call_connection_lost\n"
             "ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host",
             srv(-6, "ERROR", "Burst #12: order refused: BrokerError: 10019 no money")]
APP = [plain(-30, "ERROR", "server stopped answering (before the restart)"),
       plain(-9.5, "INFO", "Forex Bot V1.3.0 starting from D:\\Forex Bot")]
CONN = [plain(-8, "INFO", "Connected as user 1 (demo account, hedging)")]
BARS = {"trend|EURUSD|D1": bar(1), "trend|GBPJPY|D1": bar(1), "trend|XAUUSD|D1": bar(1)}
INDEX = (ROOT / "server" / "web" / "index.html").read_text(encoding="utf-8")
PAGES = {"/api/public/info": json.dumps({"version": "1.3.0", "setup_required": False, "signup_open": False,
                                         "trial_days": 7, "ai_enabled": True, "telegram_enabled": False,
                                         "feed": "mt5"}),
         "/": INDEX}
SHIPPED = kit.connector_files(ROOT / "server" / "main.py")     # what the dev tree's connector download zips


def make_live(name: str, srv_after: list[str] | None = None, conn: list[str] | None = None, sql: tuple = (),
              files: dict | None = None, db: bool = True) -> Path:
    """A live folder in TMP: the dev tree's server/main.py and connector download files, the logs (records before and
    after the restart), a v1.3.0 database; `sql` changes the database, `files` replaces (text) or removes (None) files."""
    root = TMP / name
    for rel in ["server/main.py", *SHIPPED]:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / rel, root / rel)
    (root / "logs").mkdir()
    (root / "logs" / "server.log").write_text("\n".join(SRV_BEFORE + (SRV_AFTER if srv_after is None else srv_after))
                                              + "\n", encoding="utf-8")
    (root / "logs" / "app.log").write_text("\n".join(APP) + "\n", encoding="utf-8")
    (root / "connector" / "connector.log").write_text("\n".join(CONN if conn is None else conn) + "\n",
                                                      encoding="utf-8")
    (root / "data").mkdir()
    if db:
        c = sqlite3.connect(root / "data" / "platform.db")
        try:
            c.executescript(SCHEMA)
            c.executemany("INSERT INTO engine_state (key, value) VALUES (?, ?)",
                          [(kit.FLAG, json.dumps({"added": ["GBPJPY", "XAUUSD"]})), ("mode_bars", json.dumps(BARS))])
            c.execute("INSERT INTO app_settings (key, value) VALUES ('symbols', ?)",
                      (json.dumps(SIX + ["GBPJPY", "XAUUSD"]),))
            c.executemany("INSERT INTO user_trades (symbol, strategy) VALUES (?, ?)",
                          [("EURUSD", "long"), ("GBPUSD", "short"), ("USDJPY", "burst"), ("XAUUSD", "rapid")])
            c.execute("INSERT INTO bursts (status, kind) VALUES ('closed', 'rapid')")
            for s in sql:
                c.execute(s)
            c.commit()
        finally:
            c.close()
    for rel, text in (files or {}).items():
        if text is None:
            (root / rel).unlink()
        else:
            (root / rel).write_text(text, encoding="utf-8")
    return root


def run(root: Path, pages: dict | None = None, since: str = SINCE) -> tuple[bool, str]:
    """kit.check on `root` with the fake GET answering from PAGES (+ `pages`; an exception there is raised): (its
    result, what it printed)."""
    answers = {**PAGES, **(pages or {})}

    def fetch(path: str) -> str:
        ASKED.append(path)
        got = answers[path]
        if isinstance(got, Exception):
            raise got
        return got

    kit.fetch = fetch
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ok = kit.check(since, root)
    return ok, buf.getvalue()


def fails(out: str) -> list[str]:
    return [ln[5:] for ln in out.splitlines() if ln.startswith("FAIL ")]


def failed_alone(out: str, ok: bool, *, label: str, want: str, total: int = 12):
    """One item failed, the one whose line has `want`; the verdict counts it and gives the rollback command."""
    got = fails(out)
    check(not ok and len(got) == 1 and want in got[0] and f"SOMETHING FAILED (1 of {total})" in out
          and kit.ROLLBACK in out and "Reporneste" in out and "ALL GOOD" not in out, label, out)


def test_all_good():
    root = make_live("good")
    db = root / "data" / "platform.db"
    before = (db.read_bytes(), db.stat().st_mtime_ns)
    ASKED.clear()
    ok, out = run(root)
    passed = [ln for ln in out.splitlines() if ln.startswith("PASS ")]
    check(ok and not fails(out) and len(passed) == 12 and "ALL GOOD: v1.3.0 runs (12 checks passed)" in out
          and kit.ROLLBACK not in out, "a v1.3.0 that runs well: the 12 items PASS, ALL GOOD, no rollback advice", out)
    check(ASKED == ["/api/public/info", "/"], "the only GETs: /api/public/info and /", ASKED)
    check("PASS no new errors in server.log, app.log, connector.log (0)" in out,
          "errors before the restart, a broker's refusal (BrokerError) and asyncio's port-probe noise with its "
          "traceback do not count", out)
    check("PASS GBPJPY has fresh bars (its D1 bar of" in out and "PASS XAUUSD has fresh bars" in out,
          "fresh bars: the D1 bar each new pair got decided on (engine_state mode_bars); GBPJPY's warning from its "
          "first tick does not count once a fresh bar is there", out)
    check("pairs it added: GBPJPY, XAUUSD" in out and "(the stored pairs)" in out,
          "the flag's pairs are shown; the pairs come from the stored list", out)
    check("the connector download carries the" in out, "the connector download item", out)
    check((db.read_bytes(), db.stat().st_mtime_ns) == before and sorted(p.name for p in db.parent.iterdir())
          == ["platform.db"], "the database is unchanged and no journal or WAL file was made")
    check(OPENED and all(kw.get("uri") is True and str(target).startswith("file:") and str(target).endswith("?mode=ro")
                         for target, kw in OPENED),
          "the database is opened read-only (sqlite file: URI, ?mode=ro)", OPENED)


def test_server():
    root = make_live("server")
    ok, out = run(root, {"/api/public/info": json.dumps({"version": "1.2.0"})})
    failed_alone(out, ok, label="the server reports 1.2.0: FAIL (the old code still runs)",
                 want="the server runs v1.3.0 (/api/public/info: 1.2.0)")
    ok, out = run(root, {"/api/public/info": urllib.error.URLError("[WinError 10061] connection refused"),
                         "/": urllib.error.URLError("[WinError 10061] connection refused")})
    got = fails(out)
    check(not ok and len(got) == 2 and "the server answers (/api/public/info): URLError" in got[0]
          and "the web page (/): URLError" in got[1] and "PASS connector 1.3.0 online" in out
          and "SOMETHING FAILED (2 of 12)" in out,
          "the server does not answer: both pages FAIL, the other items are still checked", out)
    ok, out = run(root, {"/": INDEX.replace("?v=15", "?v=14")})
    failed_alone(out, ok, label="a page asking for ?v=14 (an older or cached index.html): FAIL",
                 want="the web page asks for v1.3.0's files (?v=15; also ?v=14)")
    ok, out = run(root, {"/": INDEX.replace("/static/app.js?v=15", "/static/app.js?v=14", 1)})
    failed_alone(out, ok, label="a page asking for ?v=15 and ?v=14: FAIL", want="also ?v=14")
    ok, out = run(root, {"/": re.sub(r"\?v=\d+", "", INDEX)})
    failed_alone(out, ok, label="a page asking for no ?v= at all (query strings stripped, another page): FAIL",
                 want="the web page asks for v1.3.0's files (?v=15)")


def test_logs():
    keep = [r for r in SRV_AFTER if "Connector online" not in r]
    ok, out = run(make_live("old-connector", srv_after=keep + [srv(-8, "INFO", "Connector online for user 1 (v1.2.0)")]))
    failed_alone(out, ok, label="only connector 1.2.0 came online since the restart: FAIL",
                 want="connector 1.3.0 online (server.log)")
    ok, out = run(make_live("no-connector", srv_after=keep))
    failed_alone(out, ok, label="no connector online since the restart (1.2.0's line before it does not "
                 "count): FAIL", want="connector 1.3.0 online")
    feed = [r.replace("with mt5 feed", "with demo feed") for r in SRV_AFTER]
    ok, out = run(make_live("demo-feed", srv_after=feed))
    failed_alone(out, ok, label="the engine started on the demo feed: FAIL",
                 want="the engine started on the MT5 feed (server.log)")
    tb = (plain(-5, "ERROR", "Rapid run #4 failed") + "\nTraceback (most recent call last):\n"
          "  File \"connector.py\", line 1474, in _run\nKeyError: 'tp_usd'")
    ok, out = run(make_live("errors", srv_after=SRV_AFTER + [srv(-4, "ERROR", "engine task failed: boom")],
                            conn=CONN + [tb]))
    failed_alone(out, ok, label="an ERROR in server.log and a traceback in connector.log since the restart: "
                 "FAIL, both counted", want="no new errors in server.log, app.log, connector.log (2)")
    check("      " + srv(-4, "ERROR", "engine task failed: boom") in out
          and f"      {plain(-5, 'ERROR', 'Rapid run #4 failed')} ... KeyError: 'tp_usd'" in out,
          "each error is shown on one line, a traceback with the error it ends with", out)
    crit = srv(-4, "CRITICAL", "engine stopped: the database is locked", "server.engine")
    ok, out = run(make_live("critical", srv_after=SRV_AFTER + [crit]))
    failed_alone(out, ok, label="a CRITICAL record in server.log since the restart (no ERROR one): FAIL",
                 want="no new errors in server.log, app.log, connector.log (1)")
    check("      " + crit in out, "the CRITICAL record is shown", out)
    warn = plain(-5, "WARNING", "Rapid #4: its close could not be reported, asked again next sweep")
    tb = (warn + "\nTraceback (most recent call last):\n  File \"connector.py\", line 2210, in report_closed\n"
          "KeyError: 'pnl'")
    ok, out = run(make_live("traceback", conn=CONN + [tb]))
    failed_alone(out, ok, label="a traceback under a WARNING record in connector.log (no ERROR, no CRITICAL): FAIL",
                 want="no new errors in server.log, app.log, connector.log (1)")
    check(f"      {warn} ... KeyError: 'pnl'" in out, "the record is shown with the error its traceback ends with", out)

    def mirrored(minutes: float, *texts: str, proc: str = "server") -> list[str]:
        """app.log lines: the launcher stamps each line a process printed, also each line of its traceback."""
        return [f"{stamp(minutes)} INFO    [{proc}] {t}" for t in texts]

    noise = mirrored(-3, f"{srv(-3, 'ERROR', kit.QUIET[1] + '(None)', 'asyncio')}",
                     "handle: <Handle _ProactorBasePipeTransport._call_connection_lost(None)>",
                     "Traceback (most recent call last):",
                     '  File "proactor_events.py", line 165, in _call_connection_lost',
                     "    self._sock.shutdown(socket.SHUT_RDWR)",
                     "ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host")
    app = "\n".join(APP + noise + mirrored(-2, "INFO:     connection open")) + "\n"
    ok, out = run(make_live("app-noise", files={"logs/app.log": app}))
    check(ok and "ALL GOOD" in out, "asyncio's port-probe noise as app.log copies it (each traceback line stamped "
          "by the launcher): not an error, still ALL GOOD", out)
    asgi = mirrored(-2, "ERROR:    Exception in ASGI application", "Traceback (most recent call last):",
                    '  File "main.py", line 900, in list_bursts', "KeyError: 'kind'")
    thread = mirrored(-1, "Exception in thread learn:", "Traceback (most recent call last):",
                      '  File "engine.py", line 850, in _learn', "ValueError: not enough price history")
    logged = mirrored(-0.5, srv(-0.5, "ERROR", "engine task failed: boom", "server.engine"),
                      "Traceback (most recent call last):", '  File "engine.py", line 177, in _prices',
                      "RuntimeError: boom")
    for name, block, want in (("asgi", asgi, "KeyError: 'kind'"), ("thread", thread, "ValueError: not enough"),
                              ("logged", logged, "RuntimeError: boom")):
        app = "\n".join(APP + noise + block) + "\n"
        ok, out = run(make_live(f"app-{name}", files={"logs/app.log": app}))
        failed_alone(out, ok, label=f"app.log: a {name} traceback right after the asyncio noise is its own error, "
                     "not part of the noise: FAIL, counted once", want="no new errors in server.log, app.log, "
                     "connector.log (1)")
        check(want in out, f"the {name} error is shown with the error its traceback ends with", out)
    app = "\n".join(APP + noise[:2] + asgi) + "\n"          # the noise's traceback cut short (the server stopped)
    ok, out = run(make_live("app-cut", files={"logs/app.log": app}))
    failed_alone(out, ok, label="app.log: uvicorn's own error record (\"ERROR:    ...\") after asyncio noise whose "
                 "traceback never came is a new record, not part of the noise: FAIL", want="no new errors in "
                 "server.log, app.log, connector.log (1)")
    root = make_live("no-logs")
    for f in (root / "logs" / "app.log", root / "connector" / "connector.log"):
        f.unlink()
    ok, out = run(root)
    check(ok, "a log that is not there (app.log, connector.log) has no records: still ALL GOOD", out)


def test_database():
    ok, out = run(make_live("no-kind", sql=("DROP TABLE bursts", "CREATE TABLE bursts (id INTEGER PRIMARY KEY, "
                                                                  "status VARCHAR(16))")))
    failed_alone(out, ok, label="bursts without kind and analysis: FAIL",
                 want="bursts.kind and bursts.analysis exist")
    ok, out = run(make_live("no-flag", sql=(f"DELETE FROM engine_state WHERE key = '{kit.FLAG}'",)))
    failed_alone(out, ok, label="the migration flag missing: FAIL", want=f"migration flag {kit.FLAG} set")
    ok, out = run(make_live("no-gold", sql=("UPDATE app_settings SET value = '" + json.dumps(SIX + ["GBPJPY"])
                                            + "' WHERE key = 'symbols'",)))
    failed_alone(out, ok, label="a stored list without XAUUSD: FAIL, the missing pair named",
                 want="GBPJPY and XAUUSD are traded (the stored pairs): XAUUSD missing")
    ok, out = run(make_live("defaults", sql=("DELETE FROM app_settings WHERE key = 'symbols'",)))
    check(ok and "PASS GBPJPY and XAUUSD are traded (the default pairs)" in out,
          "no stored list: the defaults, which carry both: PASS", out)
    ok, out = run(make_live("null-strategy", sql=("INSERT INTO user_trades (symbol, strategy) VALUES ('EURUSD', NULL)",)))
    failed_alone(out, ok, label="a trade without a strategy: FAIL, counted",
                 want="every trade has its strategy (1 without)")
    root = make_live("no-db", db=False)
    ok, out = run(root)
    got = fails(out)
    check(not ok and len(got) == 1 and "the database (platform.db): OperationalError" in got[0]
          and "PASS the connector download" in out,
          "no database: one FAIL, the items after it are still checked", out)
    check(not (root / "data" / "platform.db").exists(), "a missing database is not created (opened read-only)")


def test_bars():
    clean = [r for r in SRV_AFTER if "price tick" not in r]       # no pair warned about since the restart
    stale = dict(BARS, **{"trend|GBPJPY|D1": bar(10)})
    ok, out = run(make_live("stale", srv_after=clean, sql=(f"UPDATE engine_state SET value = '{json.dumps(stale)}' "
                                          "WHERE key = 'mode_bars'",)))
    failed_alone(out, ok, label="GBPJPY's newest decided bar 10 days old: FAIL, check again advised",
                 want="GBPJPY has no fresh bar yet (newest: D1 ")
    check("check again in a few minutes" in out, "the stale-bar line says to check again", out)
    # a weekend: no D1 bar is decided Friday to Sunday, so Thursday's is still the newest late on Monday (about 4.5
    # days old: fresh); one a day older than BARS_DAYS allows is not
    weekend = dict(BARS, **{"trend|GBPJPY|D1": bar(4.5), "trend|XAUUSD|D1": bar(4.5)})
    ok, out = run(make_live("weekend", srv_after=clean, sql=(f"UPDATE engine_state SET value = '{json.dumps(weekend)}' "
                                            "WHERE key = 'mode_bars'",)))
    check(ok and not fails(out) and len([ln for ln in out.splitlines() if ln.startswith("PASS ")]) == 12
          and "PASS GBPJPY has fresh bars" in out and "PASS XAUUSD has fresh bars" in out,
          "Thursday's bar seen late on Monday (4.5 days): the 12 items PASS", out)
    older = dict(BARS, **{"trend|GBPJPY|D1": bar(6), "trend|XAUUSD|D1": bar(6)})
    ok, out = run(make_live("week-old", srv_after=clean, sql=(f"UPDATE engine_state SET value = '{json.dumps(older)}' "
                                             "WHERE key = 'mode_bars'",)))
    got = fails(out)
    check(not ok and len(got) == 2 and all("has no fresh bar yet (newest: D1 " in g and "check again" in g for g in got)
          and "SOMETHING FAILED (2 of 12)" in out and kit.ROLLBACK in out,
          "bars 6 days old: GBPJPY and XAUUSD FAIL, check again advised", out)
    gone = {k: v for k, v in BARS.items() if "XAUUSD" not in k}
    warn = srv(-5, "WARNING", "price tick XAUUSD: RuntimeError: symbol XAUUSD not found", "server.engine")
    ok, out = run(make_live("failing", srv_after=SRV_AFTER + [warn],
                            sql=(f"UPDATE engine_state SET value = '{json.dumps(gone)}' WHERE key = 'mode_bars'",)))
    failed_alone(out, ok, label="XAUUSD with no decided bar and a price tick warning since the restart: "
                 "FAIL with the warning", want="XAUUSD: its bars fail to load (server.log)")
    check("      " + warn in out, "the warning is shown", out)
    ok, out = run(make_live("not-yet", sql=(f"UPDATE engine_state SET value = '{json.dumps(gone)}' "
                                            "WHERE key = 'mode_bars'",)))
    failed_alone(out, ok, label="XAUUSD with no decided bar yet while other pairs have theirs (its warning "
                 "from before the restart does not count): FAIL", want="XAUUSD has no fresh bar yet (none decided on)")
    ok, out = run(make_live("no-mode-bars", srv_after=clean,
                            sql=("DELETE FROM engine_state WHERE key = 'mode_bars'",)))
    check(ok and "PASS GBPJPY: no load error logged" in out and "PASS XAUUSD: no load error logged" in out,
          "no mode decides bars on its own timeframe (no mode_bars): PASS when no load error is logged", out)
    ok, out = run(make_live("no-mode-bars-failing", srv_after=clean + [warn],
                            sql=("DELETE FROM engine_state WHERE key = 'mode_bars'",)))
    failed_alone(out, ok, label="no mode_bars and a price tick warning: FAIL",
                 want="XAUUSD: its bars fail to load")
    aware = dict(BARS, **{"trend|XAUUSD|D1": (NOW - timedelta(days=1)).isoformat(), "trend|GBPJPY|D1": "not a time"})
    ok, out = run(make_live("odd-bars", srv_after=clean, sql=(f"UPDATE engine_state SET value = '{json.dumps(aware)}' "
                                             "WHERE key = 'mode_bars'",)))
    failed_alone(out, ok, label="a bar time with its zone counts (XAUUSD fresh); one that is no time is "
                 "skipped (GBPJPY: none decided on)", want="GBPJPY has no fresh bar yet (none decided on)")


def test_download():
    main = (ROOT / "server" / "main.py").read_text(encoding="utf-8")
    check('"bot/hold.py", ' in main and '"bot/symbols.py",' in main, "the dev main.py lists hold.py and symbols.py")
    ok, out = run(make_live("no-hold", files={"server/main.py": main.replace('"bot/hold.py", ', "", 1)}))
    failed_alone(out, ok, label="CONNECTOR_FILES without bot/hold.py (a new module): FAIL, named",
                 want="the connector download carries")
    check("      not listed: bot/hold.py" in out, "the module not listed is named", out)
    # a new module is wanted on its own (NEW_MODULES), not only through an import: here connector.py imports nothing
    # from bot.news_view, and a CONNECTOR_FILES without it still fails
    conn = (ROOT / "connector" / "connector.py").read_text(encoding="utf-8")
    bare = conn.replace("from bot.news_view import", "# from bot.news_view import")
    view = kit.bot_modules(make_live("no-view-import", files={"connector/connector.py": bare}))
    check(bare.count("# from bot.news_view import") == 2 and "bot/news_view.py" not in view,
          "connector.py with its two bot.news_view imports commented out imports nothing from it", sorted(view))
    ok, out = run(make_live("no-news-view", files={"connector/connector.py": bare,
                                                   "server/main.py": main.replace('"bot/news_view.py", ', "", 1)}))
    failed_alone(out, ok, label="CONNECTOR_FILES without bot/news_view.py, which connector.py no longer imports: "
                 "still FAIL (a new module)", want="the connector download carries")
    check("      not listed: bot/news_view.py" in out and '"bot/news_view.py", ' in main,
          "... and bot/news_view.py is named", out)
    ok, out = run(make_live("no-symbols", files={"server/main.py": main.replace('"bot/symbols.py",', "", 1)}))
    check(not ok and "      not listed: bot/symbols.py" in out,
          "CONNECTOR_FILES without bot/symbols.py (imported by connector.py and the bot modules): FAIL", out)
    ok, out = run(make_live("absent", files={"bot/rapid.py": None}))
    check(not ok and "      listed, not on disk: bot/rapid.py" in out and len(fails(out)) == 1,
          "a listed file that is not on disk: FAIL, named", out)
    ok, out = run(make_live("no-list", files={"server/main.py": main.replace("CONNECTOR_FILES = [", "FILES = [", 1)}))
    failed_alone(out, ok, label="a main.py without CONNECTOR_FILES: FAIL",
                 want="the connector download (server/main.py CONNECTOR_FILES): ValueError")
    extra = (ROOT / "connector" / "connector.py").read_text(encoding="utf-8") + "\n\ndef _later():\n" \
        "    import bot.backtest_extra\n    from bot.other_extra import thing\n"
    ok, out = run(make_live("import-later", files={"connector/connector.py": extra}))
    check(not ok and "      not listed: bot/backtest_extra.py, bot/other_extra.py" in out,
          "imports inside a function count (import bot.x and from bot.x import y)", out)
    # a module connector.py gets only through a bot module of its own (bot/hold.py: from .x)
    deep = (ROOT / "bot" / "hold.py").read_text(encoding="utf-8") + "\n\nfrom .extra_dep import thing\n"
    ok, out = run(make_live("import-deep", files={"bot/hold.py": deep}))
    failed_alone(out, ok, label="a bot module that bot/hold.py imports (from .x), not listed: FAIL",
                 want="the connector download carries")
    check("      not listed: bot/extra_dep.py" in out, "... and it is named (the bot modules' own imports are read)", out)


def test_since():
    ASKED.clear()
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            kit.check("03.11.2026 10:00", make_live("since"))
        got = None
    except SystemExit as e:
        got = str(e)
    check(got is not None and "check wants the restart time in UTC" in got and ASKED == [],
          "a restart time in another format: stopped with the format it wants, before any GET", got)
    later = (NOW + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    ok, out = run(make_live("since-later"), since=later)
    check(not ok and "FAIL connector 1.3.0 online" in out and "FAIL the engine started on the MT5 feed" in out,
          "a restart time after every record: nothing since then counts", out)


def test_contract():
    """The kit's constants and the texts it looks for are the code's (parsed, not imported: no server start)."""
    version = ast.literal_eval(re.search(r'APP_VERSION = ("[^"]+")', (ROOT / "bot" / "version.py")
                                         .read_text(encoding="utf-8")).group(1))
    conn_src = (ROOT / "connector" / "connector.py").read_text(encoding="utf-8")
    conn_version = re.search(r'^VERSION = "([^"]+)"', conn_src, re.M).group(1)
    check(kit.VERSION == version == conn_version, "VERSION is bot/version.py's and the connector's",
          (kit.VERSION, version, conn_version))
    db_src = (ROOT / "server" / "db.py").read_text(encoding="utf-8")
    pairs = ast.literal_eval(re.search(r"^NEW_SYMBOLS_130 = (\([^)]*\))", db_src, re.M).group(1))
    check(kit.NEW_PAIRS == pairs, "NEW_PAIRS is server/db.py NEW_SYMBOLS_130", (kit.NEW_PAIRS, pairs))
    check(f'set_state(db, "{kit.FLAG}"' in db_src, "FLAG is the key init_db sets", kit.FLAG)
    check(set(re.findall(r"\?v=(\d+)", INDEX)) == {kit.ASSETS}, "ASSETS is the ?v= of server/web/index.html",
          re.findall(r"\?v=(\d+)", INDEX))
    main_src = (ROOT / "server" / "main.py").read_text(encoding="utf-8")
    eng_src = (ROOT / "server" / "engine.py").read_text(encoding="utf-8")
    check('log.info("Connector online for user %s (v%s)", user.id, first.get("version"))' in main_src,
          "the connector's line in server.log is the one check looks for")
    check('log.info("Engine started with %s feed", self.feed.name)' in eng_src and 'name = "mt5"' in
          (ROOT / "server" / "feeds" / "prices.py").read_text(encoding="utf-8"),
          "the engine's start line and the MT5 feed's name are the ones check looks for")
    check('why = f"price tick {symbol}: {type(e).__name__}: {e}"' in eng_src and 'set_state(db, "mode_bars"' in eng_src,
          "the failing-pair warning and engine_state mode_bars are the ones check reads")
    launcher = (ROOT / "launcher" / "forexbot.py").read_text(encoding="utf-8")
    check(f'_NOISE = "{kit.QUIET[1]}"' in launcher, "the asyncio noise check skips is the launcher's _NOISE")
    formats = {"server": ast.literal_eval(re.search(r'^_LOG_FORMAT = ("[^"]+")', main_src, re.M).group(1)),
               "launcher": re.search(r'basicConfig\(level=logging.INFO, format="([^"]+)"', launcher).group(1),
               "connector": re.search(r'basicConfig\(level=logging.INFO, format="([^"]+)"', conn_src).group(1)}
    rec = logging.LogRecord("server", logging.ERROR, __file__, 1, "boom", None, None)
    lines = {k: logging.Formatter(f).format(rec) for k, f in formats.items()}
    check(all(kit.STAMP.match(ln) and kit.ERRORS.search(ln) for ln in lines.values()),
          "a record of server.log, app.log and connector.log starts with the stamp check reads, and an ERROR one "
          "is seen as an error", lines)
    check(kit.connector_files(ROOT / "server" / "main.py") == SHIPPED and isinstance(SHIPPED, list),
          "CONNECTOR_FILES is read from main.py without importing it", SHIPPED)
    need = kit.bot_modules(ROOT)
    check({"bot/__init__.py", "bot/hold.py", "bot/news_view.py", "bot/rapid.py", "bot/risk.py", "bot/scalp.py",
           "bot/symbols.py", "bot/mt5_broker.py"} <= need, "bot_modules finds what connector.py imports, at the top "
           "and inside functions, and what those import (from .x)", sorted(need))
    check((kit.NEW_MODULES | need) <= set(SHIPPED) and all((ROOT / f).is_file() for f in SHIPPED),
          "the dev tree's connector download carries every bot module the connector imports, all on disk",
          sorted((kit.NEW_MODULES | need) - set(SHIPPED)))


def test_prepare_guard():
    """prepare refuses, before it reads or writes anything, a files\\ whose bot/version.py (new in v1.3.0: the server
    imports it, the launcher's title reads it) is missing or names another release, read in files\\ (a live folder
    already naming 1.3.0 lets no older files\\ through); with this release's it runs: the live files that change backed
    up, the new ones listed, the database copied, the state written, nothing copied over. The dev tree's names this
    release."""
    check(kit.app_version(ROOT) == kit.VERSION, "the dev tree's bot/version.py names the kit's VERSION",
          kit.app_version(ROOT))
    saved = {k: getattr(kit, k) for k in ("DEV", "LIVE", "BACKUP", "STATE")}
    ver = lambda v: f'APP_VERSION = "{v}"\n'
    cases = {   # name: (bot/version.py in files\, the live one; None: not there), whether prepare runs
        "no version in files\\": (None, None, False), "an old version in files\\": (ver("1.2.0"), None, False),
        "no version line in files\\": ("# empty\n", None, False),
        "1.2.0 in files\\, 1.3.0 in the live folder": (ver("1.2.0"), ver("1.3.0"), False),
        "this release in files\\, over v1.2.0 (no bot/version.py)": (ver(kit.VERSION), None, True),
        "this release in files\\, over a live folder naming 1.2.0": (ver(kit.VERSION), ver("1.2.0"), True)}
    for i, (name, (text, live_text, runs)) in enumerate(cases.items()):
        here = TMP / f"prepare-{i}"
        files, live, state = here / "files", here / "live", here / "st.json"
        for root, version in ((files, text), (live, live_text)):
            (root / "server").mkdir(parents=True)
            (root / "README.md").write_text("# the same in both\n", encoding="utf-8")
            if version is not None:
                (root / "bot").mkdir()
                (root / "bot" / "version.py").write_text(version, encoding="utf-8")
        (files / "server" / "main.py").write_text("# changed\n", encoding="utf-8")
        (live / "server" / "main.py").write_text("# v1.2.0\n", encoding="utf-8")
        (files / "bot").mkdir(exist_ok=True)
        (files / "bot" / "rapid.py").write_text("# new\n", encoding="utf-8")
        (live / "data" / "backups").mkdir(parents=True)
        c = sqlite3.connect(live / "data" / "platform.db")
        try:
            c.executescript("CREATE TABLE signals (id INTEGER PRIMARY KEY); INSERT INTO signals VALUES (1), (2);")
        finally:
            c.close()
        kit.DEV, kit.LIVE, kit.BACKUP, kit.STATE = files, live, live / "backups" / "before-v1.3.0", state
        n = len(OPENED)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                kit.prepare()
            got = None
        except SystemExit as e:
            got = str(e)
        finally:
            for k, v in saved.items():
                setattr(kit, k, v)
        if not runs:
            check(got is not None and "files\\bot\\version.py names" in got and "nothing done" in got
                  and not (live / "backups").exists() and not any((live / "data" / "backups").iterdir())
                  and not state.exists() and len(OPENED) == n,
                  f"prepare with {name}: refused, nothing backed up or opened", got)
            continue
        st = json.loads(state.read_text(encoding="utf-8")) if state.exists() else {}
        changed = ["server/main.py"] + (["bot/version.py"] if live_text else [])
        new = ["bot/rapid.py"] + ([] if live_text else ["bot/version.py"])
        backup = live / "backups" / "before-v1.3.0"
        copies = list((live / "data" / "backups").glob("platform-before-v130-*.db"))
        kept = [(backup / rel).read_text(encoding="utf-8") == (live / rel).read_text(encoding="utf-8")
                for rel in changed if (backup / rel).is_file()]
        signals = None
        if len(copies) == 1:
            c = sqlite3.connect(copies[0])
            try:
                signals = c.execute("SELECT count(*) FROM signals").fetchone()[0]
            finally:
                c.close()
        check(got is None and sorted(st.get("changed", [])) == sorted(changed)
              and sorted(st.get("new", [])) == sorted(new) and st.get("db_backup") == str(copies[0] if copies else None) and kept == [True] * len(changed)
              and (backup / "NEW-FILES.txt").is_file()
              and (backup / "NEW-FILES.txt").read_text(encoding="utf-8").split() == sorted(new) and signals == 2,
              f"prepare with {name}: runs (the changed files backed up, the new ones listed, the database copied, the "
              "state written)", (got, st, kept, signals))
        check((live / "server" / "main.py").read_text(encoding="utf-8") == "# v1.2.0\n"
              and not (live / "bot" / "rapid.py").exists()
              and any(str(t).startswith("file:") and str(t).endswith("platform.db?mode=ro") for t, _ in OPENED[n:]),
              f"prepare with {name}: nothing copied over the live files, its database opened read-only", OPENED[n:])


def test_fetch():
    """The real fetch, with Python's HTTP handler stubbed (nothing is sent) while a proxy is set, as in Windows with no
    bypass for 127.0.0.1: the request goes straight to the live server's address (req.host: through a proxy it would be
    the proxy's), with a timeout, and the body comes back as text."""
    seen = []

    def stub(handler, req):
        seen.append((req.full_url, req.host, req.timeout))
        answer = urllib.response.addinfourl(io.BytesIO('{"version": "1.3.0", "x": "ă"}'.encode("utf-8")),
                                            email.message.Message(), req.full_url, 200)
        answer.msg = "OK"
        return answer

    dead = "http://10.255.255.1:9"                      # a proxy that answers nobody
    names = ("getproxies", "proxy_bypass", "_opener")
    saved = {n: getattr(urllib.request, n) for n in names}, urllib.request.HTTPHandler.http_open
    urllib.request.getproxies = lambda: {"http": dead, "https": dead}
    urllib.request.proxy_bypass = lambda host: False
    urllib.request._opener = None                       # urlopen's own opener, built with that proxy
    urllib.request.HTTPHandler.http_open = stub
    try:
        got = load_kit().fetch("/api/public/info")
    finally:
        for n, v in saved[0].items():
            setattr(urllib.request, n, v)
        urllib.request.HTTPHandler.http_open = saved[1]
    check(seen == [("http://127.0.0.1:8000/api/public/info", "127.0.0.1:8000", 5)] and json.loads(got)["x"] == "ă",
          "fetch GETs http://127.0.0.1:8000 + the path straight, never through a proxy set, 5 s timeout, decodes "
          "UTF-8", (seen, got))


def test_command_line():
    """The script's command line, run as the user does, with every opener made to fail so nothing can connect: usage
    lists every command, check wants its time, and its verdict is the exit code (main, with check stood in: 1 when an
    item failed, 0 when all passed). prepare / copy / rollback are not run here (they work on D:\\Forex Bot)."""
    def script(*args: str) -> subprocess.CompletedProcess:
        code = ("import runpy, sys, urllib.request\n"
                "def no(*a, **k): raise OSError('no network in tests')\n"
                "urllib.request.urlopen = urllib.request.OpenerDirector.open = no\n"
                f"sys.argv = [{str(KIT)!r}, *{list(args)!r}]\n"
                f"runpy.run_path({str(KIT)!r}, run_name='__main__')\n")
        return subprocess.run([sys.executable, "-B", "-X", "utf8", "-c", code], capture_output=True, text=True,
                              encoding="utf-8", cwd=TMP, timeout=60)

    for args in ((), ("check",), ("deploy",)):
        p = script(*args)
        check(p.returncode != 0 and 'prepare | copy | check "<restart UTC>" | rollback' in p.stderr,
              f"{' '.join(args) or 'no command'}: the usage, listing check", (p.returncode, p.stderr[-300:]))
    p = script("check", "yesterday")
    check(p.returncode != 0 and "check wants the restart time in UTC" in p.stderr,
          "check with a time in another format: stops before reading or asking anything", (p.returncode, p.stderr))
    real, asked, codes = kit.check, [], []
    try:
        for verdict in (False, True):
            kit.check = lambda since, v=verdict: asked.append(since) or v
            try:
                kit.main(["deploy_130.py", "check", SINCE])
                codes.append("no exit")
            except SystemExit as e:
                codes.append(e.code)
    finally:
        kit.check = real
    check(codes == [1, 0] and asked == [SINCE, SINCE],
          "check \"<UTC>\": exit code 1 when an item failed, 0 when every one passed (what a script relies on)",
          (codes, asked))
    check(all(callable(getattr(kit, c, None)) for c in ("prepare", "copy", "rollback", "rollback_db", "check")),
          "prepare, copy, rollback (and rollback_db) are still there")
    check(not (KIT.parent / "__pycache__").exists() and not (KIT.parent / "pyc_check.pyc").exists(),
          "nothing written next to the kit (no __pycache__ in updates/)")


def main_():
    real_connect = sqlite3.connect

    def spy(target, *a, **kw):
        OPENED.append((target, kw))
        return real_connect(target, *a, **kw)

    def guard(*a, **kw):
        NET.append(str(a[:1]))
        raise OSError("no network in tests")

    kit.sqlite3 = types.SimpleNamespace(connect=spy, Error=sqlite3.Error, Connection=sqlite3.Connection)
    real_urlopen, urllib.request.urlopen = urllib.request.urlopen, guard
    real_direct, kit._DIRECT = kit._DIRECT, types.SimpleNamespace(open=guard)      # the opener the real fetch uses
    try:
        for t in (test_all_good, test_server, test_logs, test_database, test_bars, test_download, test_since,
                  test_contract, test_prepare_guard):
            t()
    finally:
        urllib.request.urlopen, kit._DIRECT = real_urlopen, real_direct
    test_fetch()
    test_command_line()
    check(not NET, "no test reached urlopen through the kit", NET)
    shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nall checks passed")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main_()
