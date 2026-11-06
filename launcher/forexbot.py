"""Forex Bot: the whole platform as one Windows app.

Starting it (the "Forex Bot" shortcut, or "Forex Bot.bat") opens a small window that runs, in the background:
  - the platform server: web app, API, signal engine, news bursts, self-learning (main.py serve, port 8000)
  - the MetaTrader 5 connector, which places the trades on this PC's MT5 terminal (connector/connector.py)
It starts either of them again if it stops (or the server stops answering), shows what they and MetaTrader 5 are
doing and opens the web app in its own window. Closing the window asks first: nothing trades while the app is
closed. The connector is stopped cleanly (its stdin is closed: a running news burst removes its orders and closes its
positions first), the server outright. Both processes are in a Windows job object, so they end with the app even if
it crashes; MetaTrader 5 itself is never closed by it.

    .venv\\Scripts\\pythonw.exe launcher\\forexbot.py              the app
    .venv\\Scripts\\python.exe launcher\\forexbot.py --headless    no window: logs to the console, Ctrl+C stops
"""
import argparse
import collections
import ctypes
import gc
import hashlib
import json
import logging
import logging.handlers
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent
HOST, PORT = "127.0.0.1", 8000
LOG_DIR = ROOT / "logs"
PREFS = ROOT / "data" / "app.json"
ICON = HERE / "forexbot.ico"
LOGO = HERE / "forexbot-logo.png"
WINDOWS = os.name == "nt"
# the processes get python.exe next to the interpreter running this (pythonw.exe has no stdout for their logs)
PYTHON = str(Path(sys.executable).with_name("python.exe")) if Path(sys.executable).with_name("python.exe").exists() \
    else sys.executable

log = logging.getLogger("forexbot")


# ── Windows plumbing ───────────────────────────────────────────
class Job:
    """A Windows job object: its processes end when this app ends, even on a crash (KILL_ON_JOB_CLOSE). Processes
    they start themselves (MetaTrader 5, when it was not open yet) are left out of it (SILENT_BREAKAWAY_OK)."""

    def __init__(self):
        self.handle = None
        if not WINDOWS:
            return
        from ctypes import wintypes
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateJobObjectW.restype = wintypes.HANDLE
        k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]

        class Basic(ctypes.Structure):
            _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64), ("PerJobUserTimeLimit", ctypes.c_int64),
                        ("LimitFlags", wintypes.DWORD), ("MinimumWorkingSetSize", ctypes.c_size_t),
                        ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", wintypes.DWORD),
                        ("Affinity", ctypes.c_size_t), ("PriorityClass", wintypes.DWORD),
                        ("SchedulingClass", wintypes.DWORD)]

        class Extended(ctypes.Structure):
            _fields_ = [("BasicLimitInformation", Basic), ("IoInfo", ctypes.c_uint64 * 6),
                        ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t),
                        ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]
        self.k32 = k32
        handle = k32.CreateJobObjectW(None, None)
        info = Extended()
        info.BasicLimitInformation.LimitFlags = 0x2000 | 0x1000     # KILL_ON_JOB_CLOSE | SILENT_BREAKAWAY_OK
        if handle and k32.SetInformationJobObject(handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
            self.handle = handle                                    # 9 = JobObjectExtendedLimitInformation
        else:
            log.warning("No job object (error %s): the processes may outlive a crash of the app",
                        ctypes.get_last_error())

    def add(self, proc: subprocess.Popen):
        if self.handle and not self.k32.AssignProcessToJobObject(self.handle, int(proc._handle)):
            log.warning("Could not put pid %s in the job (error %s)", proc.pid, ctypes.get_last_error())


def keep_awake(on: bool):
    """Asks Windows not to sleep while the bot runs (the screen may still turn off). Per thread: call it from the
    thread that lives as long as the app (the window's)."""
    if WINDOWS:
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | (0x00000001 if on else 0))


def port_open(host: str, port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))    # never through a proxy set in Windows


def server_answers(port: int) -> bool:
    try:
        with _DIRECT.open(f"http://{HOST}:{port}/api/public/info", timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


def _version_in(path: Path, name: str) -> str:
    """The `NAME = "1.3.0"` line of an app file, read as text (the app imports none of their code); "?" if none."""
    try:
        m = re.search(rf'^{name} = "([^"]+)"', path.read_text(encoding="utf-8"), re.M)
        return m.group(1) if m else "?"
    except OSError:
        return "?"


def connector_version() -> str:
    return _version_in(ROOT / "connector" / "connector.py", "VERSION")


def app_version() -> str:
    """The app's release (bot/version.py APP_VERSION, which every release bumps; the web app shows the same)."""
    return _version_in(ROOT / "bot" / "version.py", "APP_VERSION")


APP_NAME = "Forex Bot"


def window_title() -> str:
    """"Forex Bot V1.3.0": the window's title shows the release that runs."""
    ver = app_version()
    return APP_NAME if ver == "?" else f"{APP_NAME} V{ver}"


def is_app_title(title: str) -> bool:
    """A title of this app's window, whatever its release: "Forex Bot V<version>", or "Forex Bot" (up to v1.2.0, a
    window still open across an update)."""
    return title == APP_NAME or re.fullmatch(re.escape(APP_NAME) + r" V\d+(\.\d+)*", title or "") is not None


# ── what the processes say ─────────────────────────────────────
_STAMP = re.compile(r"^\d{4}-\d\d-\d\d (\d\d:\d\d):\d\d,\d+ (\w+)\s+(.*)$")
_ACCESS = re.compile(r'^INFO:\s+[\d.:]+ - "')           # uvicorn's access lines: not worth keeping
_EXCEPTION = re.compile(r"[A-Za-z_][\w.]*(Error|Exception|Exit|Interrupt)\b")   # a traceback's last line
# asyncio on Windows logs a client that hung up abruptly (a port probe) as an error with a traceback: nothing failed
_NOISE = "Exception in callback _ProactorBasePipeTransport._call_connection_lost"


class Status:
    """What the window shows, read from the processes' output (feed) and from MetaTrader 5 (the probe). Written by
    the reader / probe threads one field at a time, read by the window: no field is ever half-written."""

    def __init__(self):
        self.lines = collections.deque(maxlen=500)   # (process, line): the last output of both
        self.activity = collections.deque(maxlen=8)  # (hh:mm, text): trades and news bursts
        self.problem = None                          # (hh:mm, process, text): the last warning or error
        self.server_feed = None
        self.port_busy = False                       # the server could not listen on its port
        self.link = "starting"                       # connector <-> server: starting | online | offline
        self.stop_reason = None                      # why the connector last stopped by itself
        self.settings = None                         # (execution, [modes], risk %) as the connector last said
        self.bursts_live: set[str] = set()           # news bursts armed and not over yet
        self.mt5: dict | None = None                 # the probe's last look
        self._tb: dict[str, str | None] = {}         # process -> the problem its running traceback belongs to

    def connector_started(self):
        self.link, self.stop_reason, self.bursts_live = "starting", None, set()

    def server_started(self):
        self.port_busy = False

    def feed(self, proc: str, line: str):
        if not line.strip():
            return
        self.lines.append((proc, line))
        m = _STAMP.match(line)
        hhmm, level, msg = (m.group(1), m.group(2), m.group(3)) if m else (time.strftime("%H:%M"), "", line)
        if proc == "server":
            self._server(msg)
        else:
            self._connector(hhmm, msg)
        noise = _NOISE in msg
        if (level in ("WARNING", "ERROR", "CRITICAL") or msg.startswith("ERROR:")) and not noise:
            self.problem = (hhmm, proc, msg)
        if m:
            self._tb.pop(proc, None)
            if noise:
                self._tb[proc] = _NOISE                # its traceback is noise too
        elif msg.startswith("Traceback (most recent call last)"):
            # what went wrong is the traceback's last line: it goes after the error logged with it (a chained
            # traceback keeps the same error)
            if proc not in self._tb:
                prev = self.problem
                self._tb[proc] = prev[2] if prev and prev[1] == proc and self._recent(proc, prev[2]) else None
        elif proc in self._tb and _EXCEPTION.match(msg) and self._tb[proc] != _NOISE:
            base = self._tb[proc]
            self.problem = (self.problem[0] if base else hhmm, proc, f"{base} — {msg}" if base else msg)

    def _recent(self, proc: str, msg: str) -> bool:
        """`msg` is among the last lines of `proc`: an error logged with this traceback, not an older one."""
        return any(p == proc and msg in line for p, line in list(self.lines)[-8:])

    def _server(self, msg: str):
        if "Uvicorn running on" in msg:
            self.port_busy = False
        elif "[Errno 10048]" in msg or "error while attempting to bind" in msg or \
                "address already in use" in msg.lower():
            self.port_busy = True
        m = re.search(r"Engine started with (\w+) feed", msg)
        if m:
            self.server_feed = m.group(1)

    def _connector(self, hhmm: str, msg: str):
        if msg.startswith("Connected to ") and " as " in msg and "MT5" not in msg:
            self.link = "online"
        elif msg.startswith("Connection lost"):
            self.link = "offline"
        elif msg.startswith("Settings: execution="):
            m = re.match(r"Settings: execution=(\w+) modes=\[(.*?)\] risk=(\S+?)%", msg)
            if m:
                self.settings = (m.group(1), re.findall(r"'(\w+)'", m.group(2)), m.group(3))
        elif msg.startswith("Server closed the session:"):
            self.stop_reason = "replaced" if "Another connector" in msg else msg.split(":", 1)[1].strip()
        elif msg.startswith("No token"):
            self.stop_reason = "no_token"
        elif "REAL-MONEY account" in msg:
            self.stop_reason = "real_account"
        m = re.match(r"News burst #(\d+) (armed|\w+: \d+ orders|failed)", msg)
        if m:
            (self.bursts_live.add if m.group(2) == "armed" else self.bursts_live.discard)(m.group(1))
        if re.match(r"(Opened (buy|sell) |Closed #|Position #\d+ closed|News burst #\d+ (scheduled|armed|\w+: \d+ "
                    r"orders|failed))", msg):
            self.activity.appendleft((hhmm, msg))


# ── the processes ──────────────────────────────────────────────
class Child:
    """One background process the app keeps running: started again after a pause that grows while it keeps
    stopping (3 s ... 5 min), back to the shortest once it has run for 10 minutes. With `graceful` (seconds) it is
    stopped by closing its stdin and given that long to end by itself before it is ended outright."""
    PAUSES = (3, 10, 30, 60, 120, 300)
    STABLE = 600

    def __init__(self, name: str, argv: list[str], job: Job, on_line, on_start=None, graceful: float = 0):
        self.name, self.argv, self.job, self.on_line, self.on_start = name, argv, job, on_line, on_start
        self.graceful = graceful
        self.proc: subprocess.Popen | None = None
        self.wanted = False            # should be running (False before the first start and after stop())
        self.fails = 0
        self.started = 0.0
        self.retry_at: float | None = None
        self.last_exit: int | None = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self):
        self.wanted, self.retry_at = True, None
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        try:
            self.proc = subprocess.Popen(
                self.argv, cwd=str(ROOT), env=env, stdin=subprocess.PIPE if self.graceful else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1,
                creationflags=0x08000000 if WINDOWS else 0)          # CREATE_NO_WINDOW: no console window
        except OSError as e:
            log.error("%s could not start: %s", self.name, e)
            self.on_line(self.name, f"ERROR: could not start: {e}")
            self._stopped(time.monotonic(), None)
            return
        self.job.add(self.proc)
        self.started = time.monotonic()
        if self.on_start:
            self.on_start()
        threading.Thread(target=self._read, args=(self.proc,), name=f"{self.name}-output", daemon=True).start()
        log.info("%s started (pid %s)", self.name, self.proc.pid)

    def _read(self, proc: subprocess.Popen):
        try:
            for line in proc.stdout:
                self.on_line(self.name, line.rstrip("\r\n"))
        except (OSError, ValueError):
            pass

    def _stopped(self, now: float, code):
        up = now - self.started if self.proc else 0
        self.fails = 1 if up >= self.STABLE else self.fails + 1
        pause = self.PAUSES[min(self.fails, len(self.PAUSES)) - 1]
        _close_stdin(self.proc)
        self.proc, self.retry_at, self.last_exit = None, now + pause, code
        log.warning("%s stopped (exit code %s after %.0f s): starting it again in %s s", self.name, code, up, pause)

    def check(self, now: float) -> bool:
        """Each tick: notices that it stopped by itself and starts it again once its pause is over. True when it
        was started again now."""
        if not self.wanted:
            return False
        if self.proc is not None and self.proc.poll() is not None:
            self._stopped(now, self.proc.returncode)
        if self.proc is None and self.retry_at is not None and now >= self.retry_at:
            self.start()
            return True
        return False

    def stop(self, timeout: float = 8.0):
        self.wanted, self.retry_at, self.fails = False, None, 0
        proc, self.proc = self.proc, None
        if proc is None or proc.poll() is not None:
            _close_stdin(proc)
            return
        if self.graceful:
            _close_stdin(proc)                        # the connector's signal to stop as Ctrl+C would
            try:
                proc.wait(self.graceful)
                log.info("%s stopped cleanly", self.name)
                return
            except subprocess.TimeoutExpired:
                log.warning("%s did not stop within %s s: ending it", self.name, self.graceful)
        proc.terminate()                              # TerminateProcess: like a crash, which both are built for
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout)
            except subprocess.TimeoutExpired:         # the job still ends it with the app
                log.error("%s (pid %s) does not end", self.name, proc.pid)
                return
        log.info("%s stopped", self.name)


def _close_stdin(proc: subprocess.Popen | None):
    if proc is not None and proc.stdin:
        try:
            proc.stdin.close()
        except OSError:
            pass


class Supervisor:
    """Runs the server, then (once it answers) the connector, and keeps both running. Every start, stop and check
    happens on its own thread; the window only sends it commands (start / stop / restart / quit). A server that
    stops answering for HUNG_CHECKS checks in a row, or never answers within START_LIMIT s, is started again."""
    TICK = 0.5
    ANSWER_EVERY = 10             # s between checks once it answers (every second until then)
    HUNG_CHECKS = 12              # ~2 min without an answer: started again
    START_LIMIT = 300             # s a new server may take to answer the first time
    CONNECTOR_GRACE = 50          # s the connector has to end its news bursts when stopped (its own limit: 40)
    PORT_FREE_WAIT = 5            # s a stopped server's port may take to be free again

    def __init__(self, status: Status, port: int = PORT, server_argv=None, connector_argv=None, ini: Path | None = None):
        self.status, self.port = status, port
        self.job = Job()
        self.server = Child("server", server_argv or [PYTHON, "-X", "utf8", str(ROOT / "main.py"), "serve",
                                                      "--host", HOST, "--port", str(port)], self.job, self._line,
                            on_start=status.server_started)
        self.connector = Child("connector", connector_argv or [PYTHON, "-X", "utf8",
                                                               str(ROOT / "connector" / "connector.py"),
                                                               "--stop-on-stdin-eof"],
                               self.job, self._line, on_start=status.connector_started, graceful=self.CONNECTOR_GRACE)
        self.ini = ini or ROOT / "connector" / "connector.ini"
        self.active = False           # the user wants the bot running
        self.stopping = False         # stopping both right now (the connector may be ending a news burst)
        self.ready = False            # the server answers
        self.ever_ready = False
        self.not_answering = 0        # checks in a row the running server did not answer
        self.no_ini = False
        self.events: queue.Queue = queue.Queue()   # to the window: "ready" (first time the server answers)
        self.want_running = False     # what the last command asked for: a restart still stopping is "running"
        self._cmds: queue.Queue = queue.Queue()
        self._asked = self._done = 0  # commands asked for / carried out (pending: the window waits for them)
        self._asked_lock = threading.Lock()
        self._next_look = 0.0
        self._thread = threading.Thread(target=self._loop, name="supervisor", daemon=True)

    # commands (any thread)
    def start(self):
        self.request("start")
        if not self._thread.is_alive():
            self._thread.start()

    def request(self, cmd: str):
        with self._asked_lock:
            self._asked += 1
            if cmd in ("start", "restart", "stop"):
                self.want_running = cmd != "stop"
        self._cmds.put(cmd)

    @property
    def pending(self) -> bool:
        """A command was asked for that the supervisor has not carried out yet (it may be busy a moment, starting
        the server): the window takes no other one meanwhile."""
        return self._done != self._asked

    def close(self, timeout: float | None = None):
        """Stops both processes and the supervisor; returns once they are stopped."""
        if self._thread.is_alive():
            self.request("quit")
            self._thread.join(self.CONNECTOR_GRACE + 40 if timeout is None else timeout)
        else:
            self._stop_all()

    def _line(self, proc: str, line: str):
        self.status.feed(proc, line)
        if not _ACCESS.match(line):
            log.info("[%s] %s", proc, line)

    # the supervisor thread
    def _loop(self):
        while True:
            try:
                cmd = self._cmds.get(timeout=self.TICK)
            except queue.Empty:
                cmd = None
            try:
                if cmd in ("stop", "restart", "quit"):
                    self._stop_all()
                if cmd == "quit":
                    return
                if cmd in ("start", "restart"):
                    self.active, self._next_look = True, 0.0
                self._tick(time.monotonic())
            except Exception:
                log.exception("supervisor %s failed", cmd or "tick")
            finally:
                if cmd is not None:
                    self._done += 1

    def _stop_all(self):
        self.stopping = True                           # before `active`: the window never sees a stopped bot meanwhile
        self.active = False
        had_server = self.server.running
        for child in (self.connector, self.server):    # the connector first: it may end a news burst, reporting it
            try:
                child.stop()
            except Exception:
                log.exception("Stopping the %s failed", child.name)
        end = time.monotonic() + self.PORT_FREE_WAIT   # a restart right away would find the port still taken
        while had_server and port_open(HOST, self.port) and time.monotonic() < end:
            time.sleep(0.2)
        self.ready, self.not_answering, self.stopping = False, 0, False

    def _tick(self, now: float):
        if not self.active:
            return
        st = self.status
        if not self.server.wanted:                     # first start: only once nothing else listens on the port
            if now < self._next_look:
                return
            self._next_look = now + 3
            if port_open(HOST, self.port):
                st.port_busy = True
                return
            st.port_busy = False
            self.server.start()
        elif self.server.check(now):
            self.ready = False
        if not self.server.running:
            self.ready = False
        elif now >= self._next_look:
            answers = server_answers(self.port)
            self._next_look = now + (self.ANSWER_EVERY if self.ready else 1)
            if answers:
                self.not_answering = 0
                if not self.ready:
                    self.ready = True
                    if not self.ever_ready:
                        self.ever_ready = True
                        self.events.put("ready")
            elif self.ready:
                self.not_answering += 1
                if self.not_answering >= self.HUNG_CHECKS:
                    self._end_server(f"has not answered for {self.not_answering} checks")
            elif now - self.server.started > self.START_LIMIT:
                self._end_server(f"has not answered within {self.START_LIMIT} s of starting")
        if not self.connector.wanted:
            if self.ready:
                self.no_ini = not self.ini.exists()
                if not self.no_ini:
                    self.connector.start()
        else:
            self.connector.check(now)

    def _end_server(self, why: str):
        """Ends a server that runs but does not answer; the next tick notices it stopped and starts it again."""
        log.error("The server %s: starting it again", why)
        self.status.feed("server", f"ERROR:    The server {why}: starting it again")
        self.ready, self.not_answering = False, 0
        try:
            self.server.proc.terminate()
        except OSError:
            pass


class MT5Probe(threading.Thread):
    """Every 10 s: is MetaTrader 5 open, connected to the broker, Algo Trading on; which account, balance, equity,
    open positions. Its first look opens the terminal if it is closed (MetaTrader5.initialize does) so the server and
    the connector find it; later it only re-attaches when the terminal runs again, never reopens it on its own."""
    EVERY = 10

    def __init__(self, status: Status):
        super().__init__(name="mt5-probe", daemon=True)
        self.status, self.halt = status, threading.Event()

    def run(self):
        try:
            import MetaTrader5 as mt5
        except ImportError:
            self.status.mt5 = {"open": False, "missing": True}
            return
        attached, first = False, True
        while not self.halt.is_set():
            try:
                if not attached and (first or _terminal_running()):
                    attached = bool(mt5.initialize())
                first = False
                t = mt5.terminal_info() if attached else None
                a = mt5.account_info() if t is not None else None
                if t is None or a is None:
                    if attached:
                        mt5.shutdown()
                        attached = False
                    self.status.mt5 = {"open": False}
                else:
                    self.status.mt5 = {
                        "open": True, "algo": bool(t.trade_allowed), "connected": bool(t.connected),
                        "server": a.server, "demo": a.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO, "currency": a.currency,
                        "balance": a.balance, "equity": a.equity, "positions": mt5.positions_total() or 0}
            except Exception as e:
                log.warning("MetaTrader 5 look failed: %s", e)
            self.halt.wait(self.EVERY)
        if attached:
            mt5.shutdown()


def _terminal_running() -> bool:
    try:
        out = subprocess.run(["tasklist", "/FI", "IMAGENAME eq terminal64.exe", "/NH"], capture_output=True, text=True,
                             timeout=10, creationflags=0x08000000 if WINDOWS else 0).stdout
        return "terminal64.exe" in out.lower()
    except Exception:
        return True


def open_web_app(port: int = PORT):
    """The web app in its own window (Edge app mode: no tabs or address bar), else the default browser."""
    url = f"http://{HOST}:{port}/"
    for base in (os.environ.get("ProgramFiles(x86)"), os.environ.get("ProgramFiles"), os.environ.get("LOCALAPPDATA")):
        edge = Path(base or "") / "Microsoft" / "Edge" / "Application" / "msedge.exe"
        if base and edge.exists():
            try:
                subprocess.Popen([str(edge), f"--app={url}", "--window-size=1380,900"], close_fds=True)
                return
            except OSError:
                pass
    webbrowser.open(url)


# ── settings of the app itself ──────────────────────────────────
def load_prefs() -> dict:
    prefs = {"keep_awake": True, "open_web": True, "lang": "ro"}
    try:
        prefs.update(json.loads(PREFS.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        pass
    return prefs


def save_prefs(prefs: dict):
    try:
        PREFS.parent.mkdir(parents=True, exist_ok=True)
        tmp = PREFS.with_name(PREFS.name + ".tmp")
        tmp.write_text(json.dumps(prefs, indent=2), encoding="utf-8")
        os.replace(tmp, PREFS)
    except OSError as e:
        log.warning("Could not save %s: %s", PREFS, e)


def setup_logging(console: bool):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [logging.handlers.RotatingFileHandler(
        LOG_DIR / "app.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")]
    if console and sys.stdout:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", handlers=handlers)
    # pythonw.exe has no stderr: what would be printed there goes to the log instead
    sys.excepthook = lambda *ei: log.critical("Forex Bot failed", exc_info=ei)
    threading.excepthook = lambda a: log.error("Thread %s failed", a.thread.name if a.thread else "?",
                                               exc_info=(a.exc_type, a.exc_value, a.exc_traceback))


# ── the window ─────────────────────────────────────────────────
TEXT = {
    "ro": {
        "sub": "Platforma și conectorul MetaTrader 5, într-o singură aplicație",
        "eyebrow": "STARE", "activity": "ULTIMA ACTIVITATE", "problem": "ULTIMA PROBLEMĂ", "none": "Nimic încă.",
        "server": "Server", "connector": "Conector", "mt5": "MetaTrader 5",
        "s_off": "Oprit", "s_starting": "Pornește…", "s_running": "Pornit", "s_busy": "Portul {port} e ocupat",
        "s_busy_d": "Alt program (poate un server pornit separat) folosește portul. Închide-l: pornesc singur după.",
        "s_hung": "Nu răspunde", "s_retry": "Repornește în {s} s", "s_feed": "feed {feed}",
        "c_off": "Oprit", "c_wait": "Așteaptă serverul", "c_starting": "Pornește…", "c_online": "Conectat", "c_offline": "Se reconectează…",
        "c_no_ini": "Lipsește connector.ini", "c_no_ini_d": "Pune connector.ini (cu token-ul din pagina Conector) în "
                                                       "folderul connector.",
        "c_replaced": "Alt conector a preluat contul", "c_no_token": "Token lipsă sau invalid",
        "c_real": "Cont REAL refuzat (doar demo)", "c_detail": "v{ver} · execuție {exe} · {modes} · risc {risk}%",
        "m_check": "Se verifică…", "m_closed": "MetaTrader 5 e închis", "m_closed_d": "Deschide MT5: fără el botul nu "
                                                                                    "poate tranzacționa.",
        "m_missing": "Pachetul MetaTrader5 lipsește", "m_off": "Algo Trading OPRIT",
        "m_off_d": "Apasă butonul „Algo Trading” din MT5, altfel ordinele sunt refuzate.",
        "m_nolink": "Fără legătură cu brokerul", "m_ok": "Algo Trading pornit",
        "m_detail": "{server} ({kind}) · sold {bal} {cur} · equity {eq} · {pos} poziții deschise",
        "open": "Deschide aplicația", "restart": "Repornește", "logs": "Jurnale", "stop": "Oprește tot",
        "start": "Pornește tot", "awake": "Ține PC-ul treaz cât rulează botul",
        "open_auto": "Deschide aplicația la pornire", "from": "Rulează din {root}",
        "q_title": "Închizi Forex Bot?",
        "q_body": "Cât timp aplicația e închisă, botul nu tranzacționează și nu urmărește știrile.",
        "q_burst": "Atenție: un burst la știri e în desfășurare. Dacă oprești acum, ordinele lui se anulează și "
                   "pozițiile lui se închid.",
        "q_min": "Minimizează (botul merge mai departe)", "q_quit": "Oprește botul și închide", "q_cancel": "Anulează",
        "qs_title": "Oprești botul acum?", "qs_body": "Serverul și conectorul se opresc: botul nu tranzacționează până "
                                                      "apeși „Pornește tot”.", "qs_yes": "Oprește",
        "qr_title": "Repornești acum?", "qr_body": "Serverul și conectorul se opresc și pornesc din nou.",
        "qr_yes": "Repornește",
        "stopping": "Se opresc serverul și conectorul…", "x_stopping": "Se oprește…",
    },
    "en": {
        "sub": "The platform and the MetaTrader 5 connector, in one app",
        "eyebrow": "STATUS", "activity": "LATEST ACTIVITY", "problem": "LATEST PROBLEM", "none": "Nothing yet.",
        "server": "Server", "connector": "Connector", "mt5": "MetaTrader 5",
        "s_off": "Stopped", "s_starting": "Starting…", "s_running": "Running", "s_busy": "Port {port} is taken",
        "s_busy_d": "Another program (maybe a server started separately) uses the port. Close it: I start right after.",
        "s_hung": "Not answering", "s_retry": "Restarting in {s} s", "s_feed": "{feed} feed",
        "c_off": "Stopped", "c_wait": "Waiting for the server", "c_starting": "Starting…", "c_online": "Connected",
        "c_offline": "Reconnecting…", "c_no_ini": "connector.ini is missing",
        "c_no_ini_d": "Put connector.ini (with the token from the Connector page) in the connector folder.",
        "c_replaced": "Another connector took over the account", "c_no_token": "Token missing or invalid",
        "c_real": "REAL account refused (demo only)", "c_detail": "v{ver} · execution {exe} · {modes} · risk {risk}%",
        "m_check": "Checking…", "m_closed": "MetaTrader 5 is closed",
        "m_closed_d": "Open MT5: without it the bot cannot trade.", "m_missing": "MetaTrader5 package missing",
        "m_off": "Algo Trading is OFF", "m_off_d": "Press the “Algo Trading” button in MT5, or orders are rejected.",
        "m_nolink": "Not connected to the broker", "m_ok": "Algo Trading on",
        "m_detail": "{server} ({kind}) · balance {bal} {cur} · equity {eq} · {pos} open positions",
        "open": "Open the app", "restart": "Restart", "logs": "Logs", "stop": "Stop all", "start": "Start all",
        "awake": "Keep the PC awake while the bot runs", "open_auto": "Open the app on start",
        "from": "Running from {root}", "q_title": "Close Forex Bot?",
        "q_body": "While the app is closed the bot does not trade or follow the news.",
        "q_burst": "Careful: a news burst is running right now. Stopping now cancels its orders and closes its "
                   "positions.",
        "q_min": "Minimise (the bot keeps going)", "q_quit": "Stop the bot and close", "q_cancel": "Cancel",
        "qs_title": "Stop the bot now?", "qs_body": "The server and the connector stop: the bot does not trade until "
                                                    "you press “Start all”.", "qs_yes": "Stop",
        "qr_title": "Restart now?", "qr_body": "The server and the connector stop and start again.",
        "qr_yes": "Restart",
        "stopping": "Stopping the server and the connector…", "x_stopping": "Stopping…",
    },
}
INK, INK2, CARD, LINE = "#101010", "#171717", "#1b1b1b", "#2b2b2b"
PAPER, MUTED, FAINT, GOLD, GOLD2 = "#F1EFE9", "#9d9b96", "#6b6a67", "#C9A96E", "#d6b984"
UP, DOWN, WARN, INFO = "#8CC59A", "#E08A7C", "#E3B860", "#9AA8E8"


def server_view(sup: Supervisor) -> tuple[str, str, str]:
    """(colour, state key, detail key) of the server row: keys of TEXT, "" for none."""
    st, srv = sup.status, sup.server
    if sup.stopping:
        return WARN, "x_stopping", ""
    if not sup.active:
        return FAINT, "s_off", ""
    if st.port_busy and not sup.ready:
        return DOWN, "s_busy", "s_busy_d"
    if srv.retry_at is not None:
        return WARN, "s_retry", ""
    if sup.ready and sup.not_answering >= 6:
        return WARN, "s_hung", ""
    if sup.ready:
        return UP, "s_running", ""
    return INFO, "s_starting", ""


def connector_view(sup: Supervisor) -> tuple[str, str, str]:
    st, con = sup.status, sup.connector
    if sup.stopping:
        return WARN, "x_stopping", ""
    if not sup.active:
        return FAINT, "c_off", ""
    if not con.wanted:
        return (DOWN, "c_no_ini", "c_no_ini_d") if sup.no_ini else (FAINT, "c_wait", "")
    if con.retry_at is not None:
        reason = {"replaced": "c_replaced", "no_token": "c_no_token", "invalid_token": "c_no_token",
                  "token_revoked": "c_no_token", "real_account": "c_real"}.get(st.stop_reason or "")
        return (DOWN, reason, "") if reason else (WARN, "s_retry", "")
    return {"online": (UP, "c_online", ""), "offline": (WARN, "c_offline", "")}.get(st.link, (INFO, "c_starting", ""))


def mt5_view(st: Status) -> tuple[str, str, str]:
    m = st.mt5
    if m is None:
        return INFO, "m_check", ""
    if m.get("missing"):
        return DOWN, "m_missing", ""
    if not m.get("open"):
        return DOWN, "m_closed", "m_closed_d"
    if not m.get("connected"):
        return WARN, "m_nolink", ""
    if not m.get("algo"):
        return DOWN, "m_off", "m_off_d"
    return UP, "m_ok", ""


CLICK_GAP = 1.0                 # s: clicks on Stop / Start / Restart closer than this are one click


def asks_before_closing(sup: Supervisor) -> bool:
    """Closing the window stops the bot: asked first while it runs or is about to (a restart still stopping, a
    start not carried out yet); a bot stopped, or stopping because the user said so, just closes."""
    return sup.active or sup.want_running


def work_area(root) -> tuple[int, int, int, int]:
    """The screen without the taskbar: left, top, right, bottom in pixels."""
    if WINDOWS:
        from ctypes import wintypes
        r = wintypes.RECT()
        if ctypes.windll.user32.SystemParametersInfoW(0x30, 0, ctypes.byref(r), 0):     # SPI_GETWORKAREA
            return r.left, r.top, r.right, r.bottom
    return 0, 0, root.winfo_screenwidth(), root.winfo_screenheight()


def frame_size() -> tuple[int, int, int]:
    """What Windows puts around a window's inside: side frame, top/bottom frame, title bar (pixels)."""
    if not WINDOWS:
        return 0, 0, 0
    m = ctypes.windll.user32.GetSystemMetrics
    pad = m(92)                                                    # SM_CXPADDEDBORDER
    return m(32) + pad, m(33) + pad, m(4)                          # SM_CXSIZEFRAME, SM_CYSIZEFRAME, SM_CYCAPTION


def run_window(sup: Supervisor, probe: MT5Probe, prefs: dict):
    import tkinter as tk
    from tkinter import font as tkfont

    if WINDOWS:
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("ForexBot.App")  # own taskbar icon
    root = tk.Tk()
    root.title(window_title())
    root.configure(bg=INK)
    root.report_callback_exception = lambda *ei: log.error("Window callback failed", exc_info=ei)
    s = max(1.0, root.winfo_fpixels("1i") / 96)       # the screen's scale (150% -> 1.5): the sizes below are at 100%
    nonlocal_state = {"awake": None, "relaunch": False, "closing": False, "dlg": None, "clicked": 0.0}
    closed = threading.Event()                        # set by the closing thread once both processes are stopped
    if ICON.exists():
        try:
            root.iconbitmap(default=str(ICON))
        except tk.TclError:
            pass
    families = set(tkfont.families(root))
    face = next((f for f in ("Inter Tight", "Segoe UI Variable Text", "Segoe UI") if f in families), "TkDefaultFont")
    F = {k: (face, size, *style) for k, (size, style) in {
        "title": (20, ("bold",)), "sub": (10, ()), "eyebrow": (8, ("bold",)), "name": (11, ("bold",)),
        "state": (11, ()), "detail": (9, ()), "btn": (10, ("bold",)), "small": (9, ())}.items()}
    t = TEXT.get(prefs.get("lang"), TEXT["ro"])
    ver = connector_version()

    head = tk.Frame(root, bg=INK)
    head.pack(fill="x", padx=24, pady=(22, 6))
    try:
        root.logo = tk.PhotoImage(file=str(LOGO))         # kept on root: Tk drops images nothing references
        tk.Label(head, image=root.logo, bg=INK).pack(side="left", padx=(0, 12))
    except tk.TclError:
        pass
    words = tk.Frame(head, bg=INK)
    words.pack(side="left", fill="x", expand=True)
    title_row = tk.Frame(words, bg=INK)
    title_row.pack(fill="x")
    tk.Label(title_row, text="Forex", font=(face, 20), fg=PAPER, bg=INK).pack(side="left")
    tk.Label(title_row, text=" Bot", font=F["title"], fg=GOLD, bg=INK).pack(side="left")
    lang_btn = tk.Label(title_row, text="EN" if prefs.get("lang") == "ro" else "RO", font=F["eyebrow"], fg=MUTED,
                        bg=INK, cursor="hand2")
    lang_btn.pack(side="right")
    tk.Label(words, text=t["sub"], font=F["sub"], fg=MUTED, bg=INK, anchor="w").pack(fill="x", pady=(2, 0))

    def eyebrow(text):
        tk.Label(root, text=text, font=F["eyebrow"], fg=GOLD, bg=INK, anchor="w").pack(fill="x", padx=24,
                                                                                     pady=(14, 6))

    def wrapping(label):
        """Its text wraps at the label's width, whatever the window's size."""
        label.bind("<Configure>", lambda e: label.configure(wraplength=max(50, e.width - 2)))
        return label

    eyebrow(t["eyebrow"])
    rows = {}
    for key in ("server", "connector", "mt5"):
        card = tk.Frame(root, bg=CARD, highlightthickness=1, highlightbackground=LINE)
        card.pack(fill="x", padx=24, pady=4)
        top = tk.Frame(card, bg=CARD)
        top.pack(fill="x", padx=14, pady=(10, 0))
        dot = tk.Canvas(top, width=12, height=12, bg=CARD, highlightthickness=0)
        dot.pack(side="left", pady=2)
        oval = dot.create_oval(2, 2, 11, 11, fill=FAINT, outline="")
        tk.Label(top, text=t[key], font=F["name"], fg=PAPER, bg=CARD).pack(side="left", padx=(8, 0))
        state = tk.Label(top, text="", font=F["state"], fg=PAPER, bg=CARD)
        state.pack(side="right")
        detail = wrapping(tk.Label(card, text="", font=F["detail"], fg=MUTED, bg=CARD, anchor="w", justify="left"))
        detail.pack(fill="x", padx=14, pady=(2, 10))
        rows[key] = (dot, oval, state, detail)

    eyebrow(t["activity"])
    activity = wrapping(tk.Label(root, text=t["none"], font=F["small"], fg=PAPER, bg=INK, anchor="nw",
                                 justify="left"))
    activity.pack(fill="x", padx=24)
    problem_head = tk.Label(root, text=t["problem"], font=F["eyebrow"], fg=GOLD, bg=INK, anchor="w")
    problem = wrapping(tk.Label(root, text="", font=F["small"], fg=MUTED, bg=INK, anchor="nw", justify="left"))

    buttons = tk.Frame(root, bg=INK)
    buttons.pack(side="bottom", fill="x", padx=24, pady=(8, 18))
    foot = tk.Label(root, text=t["from"].format(root=ROOT), font=F["small"], fg=FAINT, bg=INK, anchor="w")
    foot.pack(side="bottom", fill="x", padx=24)
    checks = tk.Frame(root, bg=INK)
    checks.pack(side="bottom", fill="x", padx=24, pady=(6, 2))

    def button(parent, text, cmd, primary=False):
        b = tk.Button(parent, text=text, command=cmd, font=F["btn"], relief="flat", bd=0, cursor="hand2",
                      padx=16, pady=8, bg=GOLD if primary else "#242424", fg=INK if primary else PAPER,
                      activebackground=GOLD2 if primary else "#2e2e2e", activeforeground=INK if primary else PAPER,
                      disabledforeground=FAINT)
        return b

    def dialog(title, body, warning, choices):
        """A question over the window, one at a time: choices are (text, action or None, primary)."""
        old = nonlocal_state["dlg"]
        if old is not None and old.winfo_exists():
            old.deiconify()
            old.lift()
            old.focus_force()
            return
        root.deiconify()                               # closed from the taskbar while minimised: show it first
        root.lift()
        dlg = tk.Toplevel(root, bg=INK)
        nonlocal_state["dlg"] = dlg
        dlg.title(title)
        dlg.transient(root)
        dlg.resizable(False, False)
        tk.Label(dlg, text=title, font=F["name"], fg=PAPER, bg=INK, anchor="w").pack(fill="x", padx=22, pady=(18, 4))
        tk.Label(dlg, text=body, font=F["small"], fg=MUTED, bg=INK, anchor="w", justify="left",
                 wraplength=int(420 * s)).pack(fill="x", padx=22)
        if warning:
            tk.Label(dlg, text=warning, font=F["small"], fg=WARN, bg=INK, anchor="w", justify="left",
                     wraplength=int(420 * s)).pack(fill="x", padx=22, pady=(6, 0))
        box = tk.Frame(dlg, bg=INK)
        box.pack(fill="x", padx=22, pady=(16, 18))

        def pick(action):
            nonlocal_state["dlg"] = None
            dlg.destroy()
            if action:
                action()
        for i, (text, action, primary) in enumerate(choices):
            button(box, text, lambda a=action: pick(a), primary=primary).pack(fill="x", pady=(8 if i else 0, 0))
        dlg.protocol("WM_DELETE_WINDOW", lambda: pick(None))
        dlg.update_idletasks()
        dlg.geometry(f"+{root.winfo_rootx() + int(40 * s)}+{root.winfo_rooty() + int(120 * s)}")
        dlg.grab_set()

    def confirmed(cmd, title, body, yes):
        """Stop / restart: asked first only while a news burst runs (stopping ends it)."""
        if sup.status.bursts_live:
            dialog(t[title], t[body], t["q_burst"], [(t[yes], lambda: sup.request(cmd), False),
                                                     (t["q_cancel"], None, True)])
        else:
            sup.request(cmd)

    def once(action):
        """Stop / Start / Restart: a double-click, or a second click before the bot has followed the first, is one
        click (Stop then Start would restart it, or Restart twice)."""
        def run():
            now = time.monotonic()
            if now - nonlocal_state["clicked"] >= CLICK_GAP and not (sup.pending or sup.stopping):
                nonlocal_state["clicked"] = now
                action()
        return run

    def stop_or_start():                               # what its label says, not what the bot did a moment ago
        if stop_b.cget("text") == t["stop"]:
            confirmed("stop", "qs_title", "qs_body", "qs_yes")
        else:
            sup.request("start")

    open_b = button(buttons, t["open"], lambda: open_web_app(sup.port), primary=True)
    open_b.pack(fill="x")
    row2 = tk.Frame(buttons, bg=INK)
    row2.pack(fill="x", pady=(8, 0))
    restart_b = button(row2, t["restart"], once(lambda: confirmed("restart", "qr_title", "qr_body", "qr_yes")))
    logs_b = button(row2, t["logs"], lambda: os.startfile(str(LOG_DIR)) if WINDOWS else None)
    stop_b = button(row2, t["stop"], once(stop_or_start))
    for b in (restart_b, stop_b):                      # Tk runs a button's command on each release: a double-click's
        b.bind("<Double-Button-1>", lambda e: "break")  # second press is not let reach it
    for i, b in enumerate((restart_b, logs_b, stop_b)):
        b.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 4, 0 if i == 2 else 4))
        row2.columnconfigure(i, weight=1, uniform="b")

    awake_v, open_v = tk.BooleanVar(value=bool(prefs.get("keep_awake"))), tk.BooleanVar(value=bool(prefs.get("open_web")))

    def toggled():
        prefs["keep_awake"], prefs["open_web"] = awake_v.get(), open_v.get()
        save_prefs(prefs)

    for var, label in ((awake_v, t["awake"]), (open_v, t["open_auto"])):
        tk.Checkbutton(checks, text=label, variable=var, command=toggled, font=F["small"], fg=PAPER, bg=INK,
                       activebackground=INK, activeforeground=PAPER, selectcolor=INK2, highlightthickness=0,
                       anchor="w").pack(fill="x")

    def switch_lang(_e=None):
        if nonlocal_state["closing"]:                  # closing: the window goes away once both are stopped
            return
        prefs["lang"] = "en" if prefs.get("lang") == "ro" else "ro"
        save_prefs(prefs)
        nonlocal_state["relaunch"] = True
        root.destroy()

    lang_btn.bind("<Button-1>", switch_lang)

    def fmt(key, **kw):
        return t[key].format(**kw)

    def money(v):
        return f"{v:,.2f}".replace(",", " ")

    def refresh():
        if closed.is_set():                            # Tk is only touched from this thread: destroyed here
            root.destroy()
            return
        now = time.monotonic()
        st = sup.status
        for key, (colour, state_key, detail_key) in (("server", server_view(sup)),
                                                      ("connector", connector_view(sup)), ("mt5", mt5_view(st))):
            dot, oval, state, detail = rows[key]
            dot.itemconfigure(oval, fill=colour)
            child = sup.server if key == "server" else sup.connector
            secs = max(0, round((child.retry_at or now) - now))
            state.configure(text=fmt(state_key, s=secs, port=sup.port) if state_key else "",
                            fg=colour if colour in (DOWN, WARN) else PAPER)
            if detail_key:
                text = fmt(detail_key)
            elif key == "server" and sup.ready:
                text = f"http://{HOST}:{sup.port}" + (f" · {fmt('s_feed', feed=st.server_feed)}"
                                                      if st.server_feed else "")
            elif key == "connector" and st.settings and con_running():
                exe, modes, risk = st.settings
                text = fmt("c_detail", ver=ver, exe=exe, modes=", ".join(modes) or "—", risk=risk)
            elif key == "mt5" and st.mt5 and st.mt5.get("open") and st.mt5.get("server"):
                m = st.mt5
                text = fmt("m_detail", server=m["server"], kind="DEMO" if m["demo"] else "REAL", bal=money(m["balance"]),
                           cur=m["currency"], eq=money(m["equity"]), pos=m["positions"])
            else:
                text = ""
            detail.configure(text=text)
        acts = list(st.activity)[:4]
        activity.configure(text="\n".join(f"{hhmm}  {msg}" for hhmm, msg in acts) if acts else t["none"])
        if st.problem:
            hhmm, proc, msg = st.problem
            problem.configure(text=f"{hhmm}  [{proc}] {msg[:300]}")
            if not problem_head.winfo_ismapped():
                problem_head.pack(fill="x", padx=24, pady=(14, 6))
                problem.pack(fill="x", padx=24)
        if not nonlocal_state["closing"]:
            busy = sup.stopping or sup.pending or now - nonlocal_state["clicked"] < CLICK_GAP
            open_b.configure(state="normal" if sup.ready else "disabled")
            stop_b.configure(text=t["stop"] if sup.active or busy else t["start"],
                             state="disabled" if busy else "normal")
            restart_b.configure(state="normal" if sup.active and not busy else "disabled")
        want_awake = bool(awake_v.get() and sup.active)
        if want_awake != nonlocal_state["awake"]:
            keep_awake(want_awake)
            nonlocal_state["awake"] = want_awake
        try:
            while True:
                if sup.events.get_nowait() == "ready" and open_v.get():
                    open_web_app(sup.port)
        except queue.Empty:
            pass
        root.after(400, refresh)                       # also while closing: it shows the stop and ends the window

    def con_running():
        return sup.connector.running

    def quit_all():
        nonlocal_state["closing"] = True
        for w in (open_b, restart_b, logs_b, stop_b):
            w.configure(state="disabled")
        foot.configure(text=t["stopping"], fg=GOLD)

        def worker():
            try:
                sup.close()
            finally:
                probe.halt.set()
                closed.set()
        threading.Thread(target=worker, name="closing", daemon=True).start()

    def on_close():
        if nonlocal_state["closing"]:
            return
        if not asks_before_closing(sup):
            quit_all()
            return
        dialog(t["q_title"], t["q_body"], t["q_burst"] if sup.status.bursts_live else None,
               [(t["q_min"], root.iconify, True), (t["q_quit"], quit_all, False), (t["q_cancel"], None, False)])

    root.protocol("WM_DELETE_WINDOW", on_close)
    # sized for the screen's scale, never bigger than the screen without its taskbar; centred in it
    root.update_idletasks()
    left, top, right, bottom = work_area(root)
    fx, fy, cap = frame_size()
    room_w, room_h = right - left - 2 * fx, bottom - top - cap - 2 * fy
    w = min(max(int(600 * s), root.winfo_reqwidth()), room_w)
    h = min(max(int(760 * s), root.winfo_reqheight()), room_h)
    root.minsize(min(int(560 * s), w), min(int(640 * s), h))
    x = left + (room_w - w) // 2
    y = top + max(0, (room_h - h) // 2 - int(20 * s))
    root.geometry(f"{w}x{h}+{x}+{y}")
    refresh()
    root.mainloop()
    keep_awake(False)
    return nonlocal_state["relaunch"]


# ── packages ───────────────────────────────────────────────────
MARKER = ROOT / ".venv" / ".forexbot-ok"         # install.ps1 writes the lock file's hash here once pip is done


def packages_current() -> bool:
    try:
        want = hashlib.sha256((ROOT / "requirements-lock.txt").read_bytes()).hexdigest().upper()
        return MARKER.read_text(encoding="ascii").strip().upper() == want
    except OSError:
        return False


def update_packages(status: Status | None = None):
    """requirements-lock.txt changed since the last setup (or it never finished): install.ps1 brings the packages up
    to date in a console window of its own, before the server starts. One that fails keeps its window open a minute
    (its messages), and the window's last problem says so: nothing waits for a click (the bot may start unattended)."""
    if not WINDOWS or packages_current() or Path(sys.prefix).resolve() != (ROOT / ".venv").resolve():
        return
    log.info("The packages differ from requirements-lock.txt: running install.ps1")
    try:
        code = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                               str(HERE / "install.ps1"), "-NoShortcuts", "-FromApp"], cwd=ROOT,
                              creationflags=subprocess.CREATE_NEW_CONSOLE).returncode
    except OSError as e:            # no PowerShell to run it with
        code = e
    if code or not packages_current():
        msg = f"install.ps1 did not finish ({code or 'no marker'}): starting with the packages there are"
        log.error(msg)
        if status is not None:
            status.problem = (time.strftime("%H:%M"), "app", msg)


# ── one instance only ──────────────────────────────────────────
def already_running() -> bool:
    """True when another Forex Bot app runs on this PC (its window is brought to the front instead)."""
    if not WINDOWS:
        return False
    from ctypes import wintypes
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateMutexW.restype = wintypes.HANDLE
    k32.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
    already_running.mutex = k32.CreateMutexW(None, False, "Local\\ForexBot.App")   # kept for the app's lifetime
    if ctypes.get_last_error() != 183:                                            # ERROR_ALREADY_EXISTS
        return False
    hwnd = find_app_window()
    if hwnd:
        ctypes.windll.user32.ShowWindow(hwnd, 9)                                   # SW_RESTORE
        ctypes.windll.user32.SetForegroundWindow(hwnd)
    return True


def find_app_window() -> int:
    """The window of the Forex Bot app that runs, 0 if none is found: a Tk window (class TkTopLevel, so a folder
    window named "Forex Bot" is not it) whose title is_app_title, of whatever release it runs. Only looks."""
    if not WINDOWS:
        return 0
    from ctypes import wintypes
    user32, found = ctypes.windll.user32, []
    title, cls = ctypes.create_unicode_buffer(256), ctypes.create_unicode_buffer(64)

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def each(hwnd, _):
        user32.GetClassNameW(hwnd, cls, len(cls))
        if cls.value == "TkTopLevel" and user32.GetWindowTextW(hwnd, title, len(title)) and is_app_title(title.value):
            found.append(hwnd)
            return False                                                            # the first one: enough
        return True
    user32.EnumWindows(each, 0)
    return found[0] if found else 0


def main():
    ap = argparse.ArgumentParser(description="Forex Bot: the platform server and the MT5 connector in one app")
    ap.add_argument("--headless", action="store_true", help="no window: log to the console, Ctrl+C stops")
    args = ap.parse_args()
    setup_logging(console=args.headless)
    moved = ROOT / "MUTAT-LA-D-Forex-Bot.txt"          # left in a folder the app moved away from: start nothing here
    if moved.exists():
        note = moved.read_text(encoding="utf-8", errors="replace").strip()
        log.error("Not started: %s", note)
        if WINDOWS and not args.headless:
            ctypes.windll.user32.MessageBoxW(None, note, "Forex Bot", 0x30)       # MB_ICONWARNING
        return
    if already_running():
        log.info("Forex Bot already runs: brought its window to the front")
        return
    log.info("%s starting from %s (connector v%s)", window_title(), ROOT, connector_version())
    status = Status()
    update_packages(status)
    sup = Supervisor(status)
    probe = MT5Probe(status)
    probe.start()
    sup.start()
    if args.headless:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        sup.close()
        probe.halt.set()
        return
    prefs = load_prefs()
    try:
        while run_window(sup, probe, prefs):        # True: the language changed, draw the window again
            gc.collect()        # the old window's Tk objects go here, on their own thread: collected on another
            prefs = load_prefs()  # thread they would end Tcl from there, and Tcl aborts the whole app
        gc.collect()
    except Exception:
        log.exception("The window failed: stopping the bot")
    finally:
        if sup.active or sup.server.running or sup.connector.running:
            sup.close()
        probe.halt.set()
    log.info("Forex Bot closed")


if __name__ == "__main__":
    main()
