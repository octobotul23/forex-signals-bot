"""The Forex Bot app (launcher/forexbot.py): what it reads from the processes' output, how it restarts a process that
stops, the order it starts them in, a taken port, a missing connector.ini, stop / start / restart, a server that stops
answering or never answers, the connector's clean stop (also the real connector.py, in paper mode against a closed
port), the rows it shows, and that a crash of the app takes its processes with it.

    .venv\\Scripts\\python tests\\test_launcher.py

The server and the connector are stand-ins (a tiny HTTP server on a free port, a script that prints connector log
lines): port 8000, MetaTrader 5, the real server and connector are never touched; nothing is written to data/ or logs/.
Exit code 0 = every check passed.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "launcher"))
sys.path.insert(0, str(ROOT))

import forexbot as fb  # noqa: E402

PASSED: list[str] = []
FAILED: list[str] = []
TMP = Path(tempfile.mkdtemp(prefix="fxb-launcher-"))
fb.PREFS = TMP / "app.json"                      # never data/app.json
MARK = f"FXB_LAUNCHER_TEST_{os.getpid()}"         # in every stand-in's command line, to find them


def check(ok, label: str, detail=""):
    d = repr(detail) if not isinstance(detail, str) else detail
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {d[:600]}" if not ok and detail != "" else ""))
    (PASSED if ok else FAILED).append(label)


def until(cond, timeout=20.0, step=0.1) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(step)
    return bool(cond())


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


SERVER = r'''
import http.server, sys
port = int(sys.argv[1])
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == "/api/public/info" else 404); self.end_headers(); self.wfile.write(b"{}")
    def log_message(self, *a): pass
print("2026-10-28 10:00:00,000 INFO    engine: Engine started with mt5 feed", flush=True)
srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), H)
print(f"INFO:     Uvicorn running on http://127.0.0.1:{port} (Press CTRL+C to quit)", flush=True)
srv.serve_forever()
'''
CONNECTOR = r'''
import sys
print("2026-10-28 10:00:01,000 INFO    Settings: execution=auto modes=['trend'] risk=1.0%", flush=True)
print("2026-10-28 10:00:01,100 INFO    Connected to ws://127.0.0.1:1/ws/connector as someone@example.com", flush=True)
sys.stdin.read()                                   # the app closes it to stop the connector (--stop-on-stdin-eof)
print("2026-10-28 10:00:02,000 INFO    clean stop", flush=True)
'''
STUBBORN = "import sys, time; print('up', flush=True); time.sleep(600)"     # ignores its stdin closing
HUNG_SERVER = r'''
import http.server, sys, time
port, answers, n = int(sys.argv[1]), int(sys.argv[2]), [0]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        n[0] += 1
        if n[0] > answers:
            time.sleep(60)                         # hangs: the app's check gives up after 3 s
        self.send_response(200); self.end_headers(); self.wfile.write(b"{}")
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
'''


def stand_ins(port: int, ini: Path):
    py = fb.PYTHON
    return dict(port=port, server_argv=[py, "-c", SERVER, str(port), MARK],
                connector_argv=[py, "-c", CONNECTOR, MARK], ini=ini)


def alive(pid: int) -> bool:
    out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
    return str(pid) in out


def marked() -> list[str]:
    """Every process still running with this run's mark in its command line (venv stubs and real interpreters)."""
    out = subprocess.run(["powershell", "-NoProfile", "-Command",
                          f"Get-CimInstance Win32_Process | Where-Object {{ $_.CommandLine -like '*{MARK}*' -and "
                          f"$_.Name -eq 'python.exe' }} | ForEach-Object {{ $_.ProcessId }}"],
                         capture_output=True, text=True).stdout
    return out.split()


# ── tests ──────────────────────────────────────────────────────
def test_reading_the_output():
    st = fb.Status()
    feed = [
        ("server", "INFO:     Started server process [64768]"),
        ("server", "2026-10-28 23:30:12,849 INFO    engine: Engine started with mt5 feed"),
        ("server", "INFO:     127.0.0.1:50364 - \"GET /api/me/settings HTTP/1.1\" 200 OK"),
        ("connector", "2026-10-28 09:54:27,124 INFO    Connected to MT5: account 1 on Broker-Demo (DEMO)"),
        ("connector", "2026-10-28 09:54:27,135 INFO    Settings: execution=auto modes=['trend', 'news_ai'] risk=1.5%"),
    ]
    for proc, line in feed:
        st.feed(proc, line)
    check(st.server_feed == "mt5", "server: its feed", st.server_feed)
    check(st.link == "starting", "connector: 'Connected to MT5' is not the link to the server", st.link)
    check(st.settings == ("auto", ["trend", "news_ai"], "1.5"), "connector: its settings", st.settings)
    st.feed("connector", "2026-10-28 09:54:27,135 INFO    Connected to ws://127.0.0.1:8000/ws/connector as a@b.c")
    check(st.link == "online", "connector: online once connected to the server", st.link)
    st.feed("connector", "2026-10-28 10:33:55,281 WARNING Connection lost (ConnectionClosedError). Reconnecting in 3s...")
    check(st.link == "offline" and st.problem and st.problem[1] == "connector" and st.problem[0] == "10:33",
          "connector: offline on a lost link, which is the last problem", (st.link, st.problem))
    st.feed("connector", "2026-10-28 02:15:00,374 INFO    News burst #4 scheduled: AUDUSD (CPI) in 900s")
    st.feed("connector", "2026-10-28 02:29:00,101 INFO    News burst #4 armed: 2 orders on AUDUSD")
    check(st.bursts_live == {"4"}, "a burst armed is live", st.bursts_live)
    st.feed("connector", "2026-10-28 02:30:22,005 INFO    News burst #4: closed 0.49 lots of #646713952 (target 1/10)")
    check(st.bursts_live == {"4"} and len(st.activity) == 2, "a slice closed: still live, not listed as activity",
          (st.bursts_live, list(st.activity)))
    st.feed("connector", "2026-10-28 02:30:31,052 INFO    News burst #4 closed: 2 orders, side buy, P&L 232.26")
    check(not st.bursts_live and st.activity[0] == ("02:30", "News burst #4 closed: 2 orders, side buy, P&L 232.26"),
          "its final line ends it and is the latest activity", (st.bursts_live, list(st.activity)))
    st.feed("connector", "2026-10-28 03:00:00,000 INFO    News burst #5 armed: 2 orders on EURUSD")
    st.feed("connector", "2026-10-28 03:00:05,000 ERROR   News burst #5 failed")
    check(not st.bursts_live, "a failed burst is over too", st.bursts_live)
    for line in ("Opened buy 0.37 EURUSD @ 1.11304 (test signal #1)", "Closed #646707231 EURUSD (signal #1 finished)",
                 "Position #646707231 closed: P&L -3.33"):
        st.feed("connector", f"2026-10-28 01:06:34,433 INFO    {line}")
    check([a[1] for a in list(st.activity)[:3]] == ["Position #646707231 closed: P&L -3.33",
                                                   "Closed #646707231 EURUSD (signal #1 finished)",
                                                   "Opened buy 0.37 EURUSD @ 1.11304 (test signal #1)"],
          "trades are activity, newest first", list(st.activity))
    st.feed("connector", "2026-10-28 03:10:00,000 ERROR   Server closed the session: Another connector logged in with "
                         "your account.")
    check(st.stop_reason == "replaced", "replaced by another connector", st.stop_reason)
    st.feed("connector", "2026-10-28 03:05:00,000 INFO    News burst #9 armed: 2 orders on GBPUSD")
    check(st.bursts_live == {"9"}, "burst #9 live", st.bursts_live)
    st.connector_started()
    check(st.stop_reason is None and st.link == "starting" and not st.bursts_live, "a new start forgets the old run",
          (st.stop_reason, st.link, st.bursts_live))
    st.feed("connector", "2026-10-28 03:10:00,000 ERROR   Server closed the session: invalid_token")
    check(st.stop_reason == "invalid_token", "invalid token", st.stop_reason)
    st.feed("connector", "No token. Create one in the web app (Connector page) and paste it into connector.ini.")
    check(st.stop_reason == "no_token", "no token at all", st.stop_reason)
    st.feed("connector", "2026-10-28 03:10:00,000 ERROR   This is a REAL-MONEY account (1). The connector only trades "
                         "demo accounts unless you set allow_real_account = yes in connector.ini. Exiting.")
    check(st.stop_reason == "real_account", "a real account refused", st.stop_reason)
    st.feed("server", "ERROR:    [Errno 10048] error while attempting to bind on address ('127.0.0.1', 8000): only "
                      "one usage of each socket address is normally permitted")
    check(st.port_busy and st.problem[1] == "server", "server: a taken port", (st.port_busy, st.problem))
    st.feed("server", "INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)")
    check(not st.port_busy, "server: listening again")
    st.feed("server", "")
    check(st.lines[-1][1].startswith("INFO:     Uvicorn"), "blank lines are skipped")
    st.feed("server", "2026-10-28 11:00:00,000 INFO    feed: EURUSD 1.10048 (10048 is a price here)")
    st.feed("connector", "2026-10-28 11:00:00,000 INFO    Opened buy 0.10 EURUSD @ 1.10048 (signal #7)")
    check(not st.port_busy, "a price with 10048 in it is not a taken port")


def test_tracebacks():
    st = fb.Status()
    st.feed("server", "2026-10-28 11:00:00,000 ERROR   engine: Learning cycle failed")
    for line in ("Traceback (most recent call last):", '  File "server/learning.py", line 1, in run',
                 "    raise sqlite3.OperationalError", "sqlite3.OperationalError: database is locked"):
        st.feed("server", line)
    check(st.problem == ("11:00", "server", "engine: Learning cycle failed — sqlite3.OperationalError: database is "
                                            "locked"), "the error logged with a traceback gets its last line", st.problem)
    for line in ("", "During handling of the above exception, another exception occurred:", "",
                 "Traceback (most recent call last):", '  File "x.py", line 2, in y', "RuntimeError: boom"):
        st.feed("server", line)
    check(st.problem[2] == "engine: Learning cycle failed — RuntimeError: boom",
          "a chained traceback keeps the error it belongs to", st.problem)
    st.feed("server", "2026-10-28 11:01:00,000 INFO    engine: tick")
    st.feed("connector", "2026-10-28 11:01:00,000 INFO    Connected to ws://x/ws/connector as a@b.c")
    for line in ("Traceback (most recent call last):", '  File "connector.py", line 9, in run', "KeyError: 'ticket'"):
        st.feed("connector", line)
    check(st.problem[1:] == ("connector", "KeyError: 'ticket'"), "a traceback with no error logged before it",
          st.problem)
    st.feed("server", "2026-10-28 11:02:00,000 WARNING engine: feed slow")
    st.feed("server", "2026-10-28 11:02:01,000 INFO    engine: tick")
    st.feed("server", "ValueError: printed by something else")
    check(st.problem[2] == "engine: feed slow", "an exception-like line outside a traceback changes nothing",
          st.problem)
    st.feed("server", "ERROR:    Exception in ASGI application")
    for line in ("Traceback (most recent call last):", '  File "a.py", line 3, in b',
                 "ZeroDivisionError: division by zero"):
        st.feed("server", line)
    check(st.problem[2] == "ERROR:    Exception in ASGI application — ZeroDivisionError: division by zero",
          "uvicorn's own error line", st.problem)
    st.feed("server", "2026-10-28 11:03:00,000 ERROR   asyncio: Exception in callback "
                      "_ProactorBasePipeTransport._call_connection_lost(None)")
    for line in ("handle: <Handle _ProactorBasePipeTransport._call_connection_lost(None)>",
                 "Traceback (most recent call last):", '  File "proactor_events.py", line 165, in _call_connection_lost',
                 "    self._sock.shutdown(socket.SHUT_RDWR)",
                 "ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host"):
        st.feed("server", line)
    check(st.problem[2] == "ERROR:    Exception in ASGI application — ZeroDivisionError: division by zero",
          "asyncio's Windows noise (a client that hung up) and its traceback are no problem", st.problem)
    st.feed("server", "2026-10-28 11:04:00,000 ERROR   engine: price_tick failed")
    for line in ("Traceback (most recent call last):", "BrokerError: No price data"):
        st.feed("server", line)
    check(st.problem[2] == "engine: price_tick failed — BrokerError: No price data",
          "  the next real error after it is one again", st.problem)


def test_restart_pauses():
    lines = []
    c = fb.Child("quitter", [fb.PYTHON, "-c", "print('bye')", MARK], fb.Job(), lambda p, l: lines.append(l))
    c.PAUSES = (0.2, 0.5, 1.0)
    c.start()
    pid = c.proc.pid
    check(until(lambda: c.proc.poll() is not None, 15), "the stand-in ran and stopped by itself")
    t0 = time.monotonic()
    check(not c.check(t0) and c.proc is None and c.retry_at is not None and abs(c.retry_at - t0 - 0.2) < 0.01,
          "first stop: started again after the first pause", (c.retry_at, t0))
    check(not c.check(t0 + 0.1), "not before its pause is over")
    check(c.check(t0 + 0.25) and c.proc is not None and c.proc.pid != pid, "started again once it is over")
    until(lambda: c.proc.poll() is not None, 15)
    t1 = time.monotonic()
    c.check(t1)
    check(abs(c.retry_at - t1 - 0.5) < 0.01, "second stop in a row: the next pause", c.retry_at - t1)
    c.check(t1 + 0.6)
    until(lambda: c.proc.poll() is not None, 15)
    t2 = time.monotonic()
    c.check(t2)
    c.check(t2 + 1.1)
    until(lambda: c.proc.poll() is not None, 15)
    t3 = time.monotonic()
    c.check(t3)
    check(abs(c.retry_at - t3 - 1.0) < 0.01, "the pauses stop growing at the last one", c.retry_at - t3)
    c.check(t3 + 1.1)
    until(lambda: c.proc.poll() is not None, 15)
    c.started = time.monotonic() - c.STABLE - 1          # as if it had run for more than 10 minutes
    t4 = time.monotonic()
    c.check(t4)
    check(abs(c.retry_at - t4 - 0.2) < 0.01, "after a long run, the first pause again", c.retry_at - t4)
    c.stop()
    check(not c.wanted and c.retry_at is None and c.fails == 0 and not c.check(time.monotonic() + 99),
          "a stopped child stays stopped, its count of stops cleared", c.fails)
    check("bye" in lines, "its output was read", lines)
    c2 = fb.Child("missing", [str(TMP / "no-such.exe")], fb.Job(), lambda p, l: lines.append(l))
    c2.PAUSES = (0.2,)
    c2.start()
    check(c2.proc is None and c2.retry_at is not None and any("could not start" in l for l in lines),
          "a program that cannot start is tried again later", lines[-1:])
    c2.stop()


def test_supervisor():
    port, ini = free_port(), TMP / "connector.ini"
    ini.write_text("[connector]\n", encoding="utf-8")
    st = fb.Status()
    sup = fb.Supervisor(st, **stand_ins(port, ini))
    sup.server.PAUSES = sup.connector.PAUSES = (0.3, 0.6)
    check(fb.server_view(sup)[1] == "s_off" and fb.connector_view(sup)[1] == "c_off", "before start: both off")
    sup.start()
    check(until(lambda: sup.server.running, 10), "the server starts")
    check(not sup.connector.wanted, "the connector waits for the server to answer")
    check(until(lambda: sup.ready, 20), "the server answers")
    check(sup.events.get(timeout=2) == "ready", "the window hears once that it is ready")
    check(until(lambda: sup.connector.running, 10), "then the connector starts")
    check(until(lambda: st.link == "online" and st.settings == ("auto", ["trend"], "1.0"), 10),
          "its output says online, with its settings", (st.link, st.settings))
    check(fb.server_view(sup)[1] == "s_running" and fb.connector_view(sup)[1] == "c_online", "rows: running, online",
          (fb.server_view(sup), fb.connector_view(sup)))

    old = sup.connector.proc.pid
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(old)], capture_output=True)
    check(until(lambda: sup.connector.retry_at is not None or (sup.connector.proc and sup.connector.proc.pid != old), 10),
          "a connector that dies is noticed")
    check(until(lambda: sup.connector.running and sup.connector.proc.pid != old, 10), "and started again")
    old_srv = sup.server.proc.pid
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(old_srv)], capture_output=True)
    check(until(lambda: not sup.ready, 10), "a server that dies is no longer ready")
    check(until(lambda: sup.ready and sup.server.proc.pid != old_srv, 20), "and is started again and answers")
    check(sup.events.empty(), "no second 'ready' for the window (it opened the web app once)")

    t0 = time.monotonic()
    sup.request("stop")
    check(not sup.want_running, "stop asked: the user wants it stopped")
    check(until(lambda: not sup.active and not sup.stopping and sup.server.proc is None
                and sup.connector.proc is None, 15), "stop: both stopped")
    check(not fb.port_open("127.0.0.1", port), "stop: the port is free once the stop is over (a restart finds it free)")
    check(time.monotonic() - t0 < 10 and any(p == "connector" and l.endswith("clean stop") for p, l in st.lines),
          "stop: the connector stopped cleanly (its stdin closed), not after its grace time", time.monotonic() - t0)
    check(not sup.ready and not sup.stopping and sup.not_answering == 0 and sup.server.fails == 0
          and sup.connector.fails == 0, "stop: nothing of the old run counted",
          (sup.ready, sup.stopping, sup.not_answering, sup.server.fails, sup.connector.fails))
    check(until(lambda: not marked(), 15), "stop: no process left", marked())
    check(not sup.pending and not fb.asks_before_closing(sup), "stop carried out: the window closes without asking")
    check(until(lambda: not fb.port_open("127.0.0.1", port), 10), "stop: the port is free")
    check(fb.server_view(sup)[1] == "s_off" and fb.connector_view(sup)[1] == "c_off", "rows after stop: off")

    sup.request("start")
    check(until(lambda: sup.ready and sup.connector.running, 25), "start again: both up")
    s1, c1 = sup.server.proc.pid, sup.connector.proc.pid
    busy, quiet = [], []

    def watching():
        for _ in range(200):
            busy.append(st.port_busy)
            quiet.append(not fb.asks_before_closing(sup))
            time.sleep(0.05)
    watch = threading.Thread(target=watching, daemon=True)
    watch.start()
    sup.request("restart")
    check(sup.pending and sup.want_running, "restart asked: pending (the window takes no second click) until done")
    check(until(lambda: sup.ready and sup.connector.running and sup.server.proc.pid != s1
                and sup.connector.proc.pid != c1, 30), "restart: new processes, both up")
    check(not alive(s1) and not alive(c1), "restart: the old ones are gone")
    watch.join(15)
    check(not any(busy), "restart: never shown as 'port taken' (the stop waits for the port)")
    check(not any(quiet), "restart: closing the window asks first all along, also while it stops")
    check(until(lambda: not sup.pending, 10), "restart: carried out")
    sup.close()
    check(not sup._thread.is_alive() and until(lambda: not marked(), 15), "close: supervisor and processes gone",
          marked())


def test_taken_port_and_missing_ini():
    port, ini = free_port(), TMP / "absent.ini"
    holder = socket.socket()
    holder.bind(("127.0.0.1", port))
    holder.listen()
    st = fb.Status()
    sup = fb.Supervisor(st, **stand_ins(port, ini))
    sup.start()
    check(until(lambda: st.port_busy, 10), "a taken port is seen")
    time.sleep(1)
    check(not sup.server.wanted and fb.server_view(sup)[1] == "s_busy", "the server waits for it",
          fb.server_view(sup))
    holder.close()
    check(until(lambda: sup.ready, 25), "once it is free the server starts")
    check(until(lambda: sup.no_ini, 10) and not sup.connector.wanted, "no connector.ini: the connector is not started")
    check(fb.connector_view(sup)[1:] == ("c_no_ini", "c_no_ini_d"), "row: connector.ini missing",
          fb.connector_view(sup))
    ini.write_text("[connector]\n", encoding="utf-8")
    check(until(lambda: sup.connector.running, 10), "once it is there the connector starts")
    sup.close()
    check(until(lambda: not marked(), 15), "closed: nothing left", marked())


def test_rows():
    st = fb.Status()
    sup = SimpleNamespace(status=st, active=True, stopping=False, ready=False, not_answering=0, no_ini=False,
                          server=SimpleNamespace(retry_at=None),
                          connector=SimpleNamespace(wanted=False, retry_at=None))
    check(fb.server_view(sup)[1] == "s_starting", "server starting")
    check(fb.connector_view(sup)[1] == "c_wait", "connector waiting for the server")
    sup.ready, sup.connector.wanted = True, True
    check(fb.connector_view(sup)[1] == "c_starting", "connector starting")
    st.link = "offline"
    check(fb.connector_view(sup)[1] == "c_offline", "connector reconnecting")
    st.link = "online"
    check(fb.connector_view(sup)[1] == "c_online", "connector online")
    sup.stopping, sup.active = True, False
    check(fb.server_view(sup)[1] == "x_stopping" and fb.connector_view(sup)[1] == "x_stopping",
          "both stopping (the connector may be ending a burst)", (fb.server_view(sup), fb.connector_view(sup)))
    sup.stopping, sup.active = False, True
    check(fb.server_view(sup)[1] == "s_running", "server running")
    sup.not_answering = 6
    check(fb.server_view(sup)[1] == "s_hung", "server running but not answering for a minute")
    sup.server.retry_at, sup.ready = 5.0, False
    check(fb.server_view(sup)[1] == "s_retry", "server waiting to start again")
    sup.connector.retry_at = 5.0
    for reason, want in (("replaced", "c_replaced"), ("invalid_token", "c_no_token"), ("no_token", "c_no_token"),
                         ("token_revoked", "c_no_token"), ("real_account", "c_real"), (None, "s_retry"),
                         ("subscription_inactive", "s_retry")):
        st.stop_reason = reason
        check(fb.connector_view(sup)[1] == want, f"connector stopped because {reason}: {want}", fb.connector_view(sup))
    for mt5, want in ((None, "m_check"), ({"missing": True}, "m_missing"), ({"open": False}, "m_closed"),
                      ({"open": True, "connected": False}, "m_nolink"),
                      ({"open": True, "connected": True, "algo": False}, "m_off"),
                      ({"open": True, "connected": True, "algo": True}, "m_ok")):
        st.mt5 = mt5
        check(fb.mt5_view(st)[1] == want, f"MetaTrader 5 {mt5}: {want}", fb.mt5_view(st))
    for lang in ("ro", "en"):
        keys = set(fb.TEXT[lang])
        used = {"s_off", "s_starting", "s_running", "s_busy", "s_busy_d", "s_hung", "s_retry", "s_feed", "c_off",
                "c_wait", "c_starting", "c_online", "c_offline", "c_no_ini", "c_no_ini_d", "c_replaced", "c_no_token",
                "c_real", "c_detail", "m_check", "m_closed", "m_closed_d", "m_missing", "m_off", "m_off_d",
                "m_nolink", "m_ok", "m_detail", "x_stopping", "q_title", "q_body", "q_burst", "q_min", "q_quit",
                "q_cancel", "qs_title", "qs_body", "qs_yes", "qr_title", "qr_body", "qr_yes", "stopping"}
        check(used <= keys, f"{lang}: every text the rows use", used - keys)
    check(set(fb.TEXT["ro"]) == set(fb.TEXT["en"]), "ro and en have the same texts",
          set(fb.TEXT["ro"]) ^ set(fb.TEXT["en"]))


def test_prefs_and_connector_map():
    check(fb.load_prefs() == {"keep_awake": True, "open_web": True, "lang": "ro"}, "defaults with no file")
    fb.save_prefs({"keep_awake": False, "open_web": True, "lang": "en"})
    check(fb.load_prefs()["keep_awake"] is False and fb.load_prefs()["lang"] == "en", "saved and read back")
    fb.PREFS.write_text("{broken", encoding="utf-8")
    check(fb.load_prefs()["keep_awake"] is True, "a broken file: defaults")
    check(not fb.PREFS.with_name(fb.PREFS.name + ".tmp").exists(), "no temp file left")
    sys.path.insert(0, str(ROOT / "connector"))
    import connector as C
    target = TMP / "tickets.json"
    fake = SimpleNamespace(signal_of={1: 2, 3: 4}, _map_file=lambda: target)
    C.Connector._save_map(fake)
    check(json.loads(target.read_text()) == {"1": 2, "3": 4} and not (TMP / "tickets.json.tmp").exists(),
          "connector: tickets.json written whole, no temp file left")
    target.unlink()
    target.mkdir()                                     # a write that fails leaves nothing broken behind
    C.Connector._save_map(fake)
    check(target.is_dir(), "connector: a failed write raises nothing")
    check(C.VERSION == "1.3.0" and fb.connector_version() == "1.3.0", "connector version 1.3.0, as the app reads it",
          fb.connector_version())
    from bot.version import APP_VERSION
    check(fb.app_version() == APP_VERSION == "1.3.2" and fb.window_title() == "Forex Bot V1.3.2"
          and fb.is_app_title(fb.window_title()) and fb.is_app_title("Forex Bot V1.3.1") and fb.is_app_title("Forex Bot V1.3.0") and fb.is_app_title("Forex Bot"),
          "the app's window title \"Forex Bot V1.3.2\" (bot/version.py); v1.3.1's, v1.3.0's and v1.2.0's "
          "\"Forex Bot\" are the app's too",
          fb.window_title())


def test_graceful_stop():
    lines = []
    c = fb.Child("connector", [fb.PYTHON, "-c", CONNECTOR, MARK], fb.Job(), lambda p, l: lines.append(l), graceful=20)
    c.start()
    check(until(lambda: any("Connected to ws" in l for l in lines), 15), "the stand-in runs", lines)
    proc, t0 = c.proc, time.monotonic()
    c.stop()
    took = time.monotonic() - t0
    check(proc.returncode == 0 and took < 5 and until(lambda: any(l.endswith("clean stop") for l in lines), 5),
          "its stdin closed: it ends by itself, at once", (proc.returncode, took, lines[-1:]))
    c = fb.Child("stubborn", [fb.PYTHON, "-c", STUBBORN, MARK], fb.Job(), lambda p, l: lines.append(l), graceful=1.5)
    c.start()
    check(until(lambda: "up" in lines, 15), "a stand-in that ignores its stdin runs")
    proc, t0 = c.proc, time.monotonic()
    c.stop()
    took = time.monotonic() - t0
    check(proc.poll() is not None and 1.4 < took < 12, "it is ended once its grace time is over", took)
    check(until(lambda: not marked(), 15), "nothing left", marked())


def test_real_connector_stops_cleanly():
    """connector.py itself (a copy, so its log and tickets files land in a temp folder), paper mode, against a port
    nothing listens on: closing its stdin stops it as Ctrl+C would, exit code 0 and "Stopped." in its output."""
    import shutil
    here = TMP / "real-connector"
    here.mkdir()
    shutil.copy2(ROOT / "connector" / "connector.py", here / "connector.py")
    shutil.copytree(ROOT / "bot", here / "bot", ignore=shutil.ignore_patterns("__pycache__"))
    lines = []
    argv = [fb.PYTHON, "-X", "utf8", str(here / "connector.py"), "--paper", "--stop-on-stdin-eof", "--server",
            f"http://127.0.0.1:{free_port()}", "--token", "x", "--config", str(here / f"{MARK}.ini")]
    c = fb.Child("connector", argv, fb.Job(), lambda p, l: lines.append(l), graceful=20)
    c.start()
    check(until(lambda: any("Reconnecting" in l or "Connection lost" in l or "Cannot reach" in l for l in lines), 30),
          "it runs and tries the server", lines[-5:])
    proc, t0 = c.proc, time.monotonic()
    c.stop()
    took = time.monotonic() - t0
    until(lambda: any(l.endswith("Stopped.") for l in lines), 5)
    check(proc.returncode == 0 and took < 15 and any(l.endswith("Stopped.") for l in lines),
          "stdin closed: it stopped cleanly", (proc.returncode, took, lines[-5:]))
    check((here / "connector.log").exists() and "Stopped." in (here / "connector.log").read_text(encoding="utf-8"),
          "it logged to its own folder (the temp copy)")
    check(until(lambda: not marked(), 15), "nothing left", marked())
    # it ends by itself (the server refuses its token) while the app still holds its stdin open: the thread that
    # waits on stdin must not break Python's exit (a buffered read there did: a fatal error, exit code not 0)
    port = free_port()
    server = subprocess.Popen([fb.PYTHON, "-c", REFUSING_SERVER, str(port), MARK])
    try:
        check(until(lambda: fb.port_open("127.0.0.1", port), 20), "the stand-in server listens")
        lines = []
        argv[argv.index("--server") + 1] = f"http://127.0.0.1:{port}"
        c = fb.Child("connector", argv, fb.Job(), lambda p, l: lines.append(l), graceful=20)
        c.start()
        proc = c.proc
        check(until(lambda: proc.poll() is not None, 30), "token refused: it ends by itself", lines[-5:])
        check(proc.returncode == 0 and any("invalid_token" in l for l in lines),
              "a clean exit with its stdin still open", (proc.returncode, lines[-5:]))
        c.stop()
    finally:
        server.kill()
        server.wait(10)
    check(until(lambda: not marked(), 15), "nothing left", marked())


REFUSING_SERVER = r'''
import asyncio, json, sys
from websockets.asyncio.server import serve

async def handler(ws):
    await ws.recv()
    await ws.send(json.dumps({"type": "error", "reason": "invalid_token"}))
    await asyncio.sleep(2)

async def main():
    async with serve(handler, "127.0.0.1", int(sys.argv[1])):
        await asyncio.sleep(120)

asyncio.run(main())
'''


def test_server_not_answering():
    port, ini = free_port(), TMP / "absent-2.ini"
    st = fb.Status()
    sup = fb.Supervisor(st, port=port, server_argv=[fb.PYTHON, "-c", HUNG_SERVER, str(port), "2", MARK],
                        connector_argv=[fb.PYTHON, "-c", CONNECTOR, MARK], ini=ini)
    sup.server.PAUSES = (0.3,)
    sup.ANSWER_EVERY, sup.HUNG_CHECKS = 0.2, 2
    try:
        sup.start()
        check(until(lambda: sup.ready, 20), "it answers at first")
        first = sup.server.proc.pid
        check(until(lambda: sup.server.proc is not None and sup.server.proc.pid != first, 30),
              "it stops answering: ended and started again", st.problem)
        check(st.problem and st.problem[1] == "server" and "has not answered for 2 checks" in st.problem[2],
              "the reason is the last problem", st.problem)
        check(until(lambda: sup.ready, 20), "the new one answers")
        check(not alive(first), "the hung one is gone")
    finally:
        sup.close()
    check(until(lambda: not marked(), 15), "closed: nothing left", marked())
    port = free_port()
    st = fb.Status()
    sup = fb.Supervisor(st, port=port, server_argv=[fb.PYTHON, "-c", STUBBORN, MARK],
                        connector_argv=[fb.PYTHON, "-c", CONNECTOR, MARK], ini=ini)
    sup.server.PAUSES = (0.3,)
    sup.START_LIMIT = 2
    try:
        sup.start()
        check(until(lambda: sup.server.running, 10), "a server that never answers starts")
        first = sup.server.proc.pid
        check(until(lambda: sup.server.proc is not None and sup.server.proc.pid != first, 20),
              "it is ended after START_LIMIT and started again", st.problem)
        check(st.problem and "has not answered within 2 s" in st.problem[2], "with that reason", st.problem)
        check(not sup.ready and not sup.connector.wanted, "the connector still waits for it")
    finally:
        sup.close()
    check(until(lambda: not marked(), 15), "closed: nothing left", marked())


def test_crash_takes_the_processes():
    """The app killed outright (no clean-up of its own): its job ends both processes."""
    port = free_port()
    ini = TMP / "crash.ini"
    ini.write_text("[connector]\n", encoding="utf-8")
    app = f'''
import sys, time
sys.path.insert(0, {str(ROOT / "launcher")!r})
import forexbot as fb
sup = fb.Supervisor(fb.Status(), port={port}, server_argv={[fb.PYTHON, "-c", SERVER, str(port), MARK]!r},
                    connector_argv={[fb.PYTHON, "-c", CONNECTOR, MARK]!r}, ini=fb.Path({str(ini)!r}))
sup.start()
while not (sup.ready and sup.connector.running):
    time.sleep(0.1)
print("up", flush=True)
time.sleep(600)
'''
    proc = subprocess.Popen([fb.PYTHON, "-c", app], stdout=subprocess.PIPE, text=True)
    watchdog = threading.Timer(90, proc.kill)           # an app that never comes up must not hang the tests
    watchdog.start()
    line = proc.stdout.readline().strip()
    watchdog.cancel()
    check(line == "up", "the app is up with both processes", line)
    check(len(marked()) >= 4, "server and connector run (venv launcher + interpreter each)", marked())
    subprocess.run(["taskkill", "/F", "/PID", str(proc.pid)], capture_output=True)   # its venv launcher: its
    proc.wait(10)                                                                    # interpreter dies at once
    check(until(lambda: not marked(), 15), "killed outright: no process of it left", marked())
    check(until(lambda: not fb.port_open("127.0.0.1", port), 10), "and the port is free")


def test_direct_and_packages():
    """The server check never goes through a proxy set for the user; the packages marker matches what install.ps1
    writes (PowerShell's own Get-FileHash / Set-Content)."""
    import hashlib
    import http.server
    port = free_port()

    class Info(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path == "/api/public/info" else 404)
            self.end_headers()

        def log_message(self, *a):
            pass
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), Info)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    saved = {k: os.environ.get(k) for k in ("HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy")}
    try:
        for k in saved:
            os.environ.pop(k, None)
        os.environ["HTTP_PROXY"] = os.environ["http_proxy"] = f"http://127.0.0.1:{free_port()}"   # nothing there
        check(fb.server_answers(port), "a proxy that does not work does not stop the check")
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v
        srv.shutdown()
        srv.server_close()

    root = TMP / "pkg"
    (root / ".venv").mkdir(parents=True)
    lock = root / "requirements-lock.txt"
    lock.write_bytes(b"websockets==15.0.1\r\n")
    old = fb.ROOT, fb.MARKER
    fb.ROOT, fb.MARKER = root, root / ".venv" / ".forexbot-ok"
    try:
        check(not fb.packages_current(), "no marker: not current")
        r = subprocess.run(["powershell", "-NoProfile", "-Command",
                            f"Set-Content -Path '{fb.MARKER}' -Value (Get-FileHash '{lock}' -Algorithm SHA256).Hash "
                            "-Encoding ascii"], capture_output=True, text=True)
        check(r.returncode == 0 and fb.MARKER.exists(), "PowerShell wrote the marker as install.ps1 does", r.stderr)
        check(fb.packages_current(), "the marker install.ps1 writes matches the lock file",
              (fb.MARKER.read_bytes(), hashlib.sha256(lock.read_bytes()).hexdigest()))
        lock.write_bytes(b"websockets==15.0.2\r\n")
        check(not fb.packages_current(), "the lock file changed: not current")
    finally:
        fb.ROOT, fb.MARKER = old

    # a package update that cannot run or fails: in the window's last problem, nothing waits for a click
    runs, real_run, real_current, real_root = [], fb.subprocess.run, fb.packages_current, fb.ROOT

    def no_powershell(argv, **kw):
        runs.append(argv)
        raise FileNotFoundError(2, "The system cannot find the file specified", "powershell")
    try:
        # update_packages runs only from the app's own .venv: the one running these tests, also when they test a
        # copy of the app (which has no .venv of its own). Nothing runs there: install.ps1 and the marker are fakes
        fb.ROOT = Path(sys.prefix).resolve().parent
        fb.packages_current = lambda: True
        fb.subprocess.run = no_powershell
        st = fb.Status()
        fb.update_packages(st)
        check(not runs and st.problem is None, "packages current: install.ps1 is not run", runs)
        fb.packages_current = lambda: False
        fb.update_packages(st)
        check(len(runs) == 1 and "-FromApp" in runs[0] and st.problem and st.problem[1] == "app"
              and "install.ps1 did not finish" in st.problem[2], "no PowerShell: shown, the app goes on", st.problem)
        fb.subprocess.run = lambda argv, **kw: runs.append(argv) or subprocess.CompletedProcess(argv, 1)
        st = fb.Status()
        fb.update_packages(st)
        check(st.problem and "(1)" in st.problem[2], "install.ps1 failed: shown with its exit code", st.problem)
        fb.subprocess.run = lambda argv, **kw: runs.append(argv) or subprocess.CompletedProcess(argv, 0)
        st = fb.Status()
        fb.update_packages(st)
        check(st.problem and "no marker" in st.problem[2], "install.ps1 ended without the marker: shown", st.problem)
    finally:
        fb.subprocess.run, fb.packages_current, fb.ROOT = real_run, real_current, real_root


def test_window_fits():
    """The window's size and place come from the screen without its taskbar."""
    left, top, right, bottom = fb.work_area(None)
    fx, fy, cap = fb.frame_size()
    check(right - left > 300 and bottom - top > 300 and left >= 0 and top >= 0, "a work area",
          (left, top, right, bottom))
    check(0 <= fx < 60 and 0 <= fy < 60 and 10 < cap < 120, "a frame and a title bar", (fx, fy, cap))


def main():
    started = time.time()
    tests = (test_reading_the_output, test_tracebacks, test_restart_pauses, test_supervisor,
             test_taken_port_and_missing_ini, test_rows, test_prefs_and_connector_map, test_graceful_stop,
             test_real_connector_stops_cleanly, test_server_not_answering, test_crash_takes_the_processes,
             test_direct_and_packages, test_window_fits)
    for fn in tests:
        print(f"\n== {fn.__name__}")
        t0 = time.time()
        try:
            fn()
        except Exception:
            check(False, f"{fn.__name__}: raised", traceback.format_exc())
        print(f"     ({fn.__name__}: {time.time() - t0:.1f} s)")
    left = marked()
    for pid in left:
        subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
    check(not left, "no stand-in left running at the end", left)
    check(not (ROOT / "data" / "app.json").exists() or (ROOT / "data" / "app.json").stat().st_mtime < started,
          "data/app.json untouched")
    if FAILED:
        print("\nfailed:\n  " + "\n  ".join(FAILED))
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - started:.1f} s)")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
