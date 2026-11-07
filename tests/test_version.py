"""The app's version (bot/version.py APP_VERSION, which every release bumps): the server reports it (main.VERSION, set
from it and never a literal of its own, GET /api/public/info, the web app's header shows "V" + it), the launcher's
window title reads it ("Forex Bot V1.3.0") and a second launch brings that window to the front, and the connector
keeps its own VERSION, never importing it (the server gates plans on the connector's).

    .venv\\Scripts\\python tests\\test_version.py

Runs on a throw-away SQLite file; never opens a window, never starts the engine or the app, never uses the network
(the second launch's Win32 calls are a stand-in); nothing is written to data/ or logs/. Exit code 0 = every check passed.
"""
import ast
import ctypes
import logging
import os
import re
import shutil
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "launcher"))
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-version-"))
DATA_DIR = ROOT / "data"

# settings are read when server modules are imported, so configure first (.env never overrides these)
os.environ.update(DATABASE_URL=f"sqlite:///{(TMP / 'platform.db').as_posix()}", PRICE_FEED="demo",
                  COOKIE_SECURE="false", SECRET_KEY="version-test-" + "k" * 40, DEMO_SECONDS_PER_BAR="0",
                  ANTHROPIC_API_KEY="", ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="")
# server.main attaches logs/server.log unless the root logger already has a handler for that file
_guard = logging.NullHandler()
_guard.baseFilename = str((ROOT / "logs" / "server.log").resolve())
_console = logging.StreamHandler()
_console.setLevel(logging.ERROR)
logging.getLogger().handlers[:0] = [_guard, _console]
_before = {p.name: p.stat().st_mtime_ns for d in (DATA_DIR, ROOT / "logs") if d.exists() for p in d.iterdir()}

from fastapi.testclient import TestClient  # noqa: E402

import forexbot as fb                      # noqa: E402
import server.main as main                 # noqa: E402
from bot.version import APP_VERSION        # noqa: E402
from server.db import engine as db_engine  # noqa: E402
from server.settings import DATABASE_URL   # noqa: E402

fb.PREFS = TMP / "app.json"                # never data/app.json
FAILED: list[str] = []


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


def dotted(v) -> tuple | None:
    """(1, 3, 0) of "1.3.0"; None for anything else."""
    return tuple(int(x) for x in v.split(".")) if isinstance(v, str) and re.fullmatch(r"\d+\.\d+\.\d+", v) else None


def test_constant():
    check(dotted(APP_VERSION) is not None, "APP_VERSION is a dotted version (major.minor.patch)", APP_VERSION)
    check(dotted(APP_VERSION) >= (1, 3, 0), "APP_VERSION: at least this release (1.3.0)", APP_VERSION)
    text = (ROOT / "bot" / "version.py").read_text(encoding="utf-8")
    check(re.search(rf'^APP_VERSION = "{re.escape(APP_VERSION)}"', text, re.M) is not None
          and "every release bumps it" in text, "bot/version.py: one APP_VERSION line, the note that every release "
          "bumps it (the launcher reads that line as text)")
    tree = ast.parse(text)
    check(all(isinstance(n, (ast.Assign, ast.Expr)) for n in tree.body),
          "bot/version.py imports nothing: anything may read it, the launcher without importing it")


def test_server():
    check(main.VERSION == APP_VERSION and main.app.version == APP_VERSION,
          "main.VERSION and the FastAPI app's version are APP_VERSION (no longer v1.0's stale 1.0.0)",
          (main.VERSION, main.app.version))
    # whatever today's number: main.py takes it from bot/version.py, never writes one of its own (a literal "1.3.0"
    # equals APP_VERSION today, and at the next bump /api/public/info and the header would show the old release)
    tree = ast.parse((ROOT / "server" / "main.py").read_text(encoding="utf-8"))
    imported = any(isinstance(n, ast.ImportFrom) and n.module == "bot.version"
                   and [a.name for a in n.names if a.asname is None] == ["APP_VERSION"] for n in tree.body)
    sets = [n.value for n in tree.body if isinstance(n, ast.Assign)
            and any(isinstance(x, ast.Name) and x.id == "VERSION" for x in n.targets)]
    named = [getattr(v, "id", type(v).__name__) for v in sets]
    check(imported and named == ["APP_VERSION"], "server/main.py: VERSION = APP_VERSION (imported from bot.version), "
          "set once, no literal", named)
    told = [kw.value for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "FastAPI"
            for kw in n.keywords if kw.arg == "version"]
    info = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "public_info"]
    told += [v for f in info for n in ast.walk(f) if isinstance(n, ast.Dict)
             for k, v in zip(n.keys, n.values) if isinstance(k, ast.Constant) and k.value == "version"]
    named = [getattr(v, "id", type(v).__name__) for v in told]
    check(named == ["VERSION", "VERSION"], "the FastAPI app's version and /api/public/info's (public_info) are "
          "VERSION, never a literal", named)
    main.init_db()                        # the tables public_info reads; the app's startup (engine) never runs
    client = TestClient(main.app)
    r = client.get("/api/public/info")
    info = r.json() if r.status_code == 200 else {}
    check(r.status_code == 200 and info.get("version") == APP_VERSION,
          "GET /api/public/info: version is APP_VERSION (what the web app's header shows as V + it)", r.text[:300])
    check({"setup_required", "signup_open", "trial_days", "feed"} <= set(info),
          "... next to what it said before", sorted(info))


def test_launcher():
    check(fb.app_version() == APP_VERSION, "launcher: app_version reads bot/version.py", fb.app_version())
    check(fb.window_title() == f"Forex Bot V{APP_VERSION}", "launcher: the window title is \"Forex Bot V<version>\"",
          fb.window_title())
    check(fb.connector_version() not in ("?", "") and dotted(fb.connector_version()) is not None,
          "launcher: connector_version still reads connector.py", fb.connector_version())
    titles = {"Forex Bot V1.3.0": True, f"Forex Bot V{APP_VERSION}": True, "Forex Bot V2.0.11": True,
              "Forex Bot": True, "Forex Bot V": False, "Forex Bot V1.3.0 (2)": False, "Forex Bot.bat": False,
              "Închizi Forex Bot?": False, "Close Forex Bot?": False, "": False, None: False}
    got = {t: fb.is_app_title(t) for t in titles}
    check(got == titles, "is_app_title: this app's window of any release (v1.2.0's \"Forex Bot\" too), never its "
          "close question or another window", got)

    # read as text from ROOT: another release's file, or none (the title is then the bare name)
    real = fb.ROOT
    other = TMP / "other"
    (other / "bot").mkdir(parents=True)
    (other / "bot" / "version.py").write_text('"""x"""\nAPP_VERSION = "9.8.7"       # every release bumps it\n',
                                              encoding="utf-8")
    try:
        fb.ROOT = other
        seen = (fb.app_version(), fb.window_title())
        fb.ROOT = TMP / "none"
        gone = (fb.app_version(), fb.window_title(), fb.connector_version())
    finally:
        fb.ROOT = real
    check(seen == ("9.8.7", "Forex Bot V9.8.7"), "the title follows the file it reads (another release's)", seen)
    check(gone == ("?", "Forex Bot", "?"), "no bot/version.py: the title is \"Forex Bot\", nothing raises", gone)

    # run_window titles its window with it: a stand-in Tk, stopped at the title (no window is ever made)
    import tkinter

    class Stop(Exception):
        pass

    titled = []

    class FakeTk:
        def __init__(self, *a, **kw):
            pass

        def title(self, t):
            titled.append(t)
            raise Stop

    tk, windows = tkinter.Tk, fb.WINDOWS
    tkinter.Tk, fb.WINDOWS = FakeTk, False       # WINDOWS off: no DPI / taskbar id set on this process
    try:
        fb.run_window(None, None, {})
    except Stop:
        pass
    finally:
        tkinter.Tk, fb.WINDOWS = tk, windows
    check(titled == [f"Forex Bot V{APP_VERSION}"], "run_window: the window's title is window_title()", titled)

    hwnd = fb.find_app_window()                  # only looks (EnumWindows); 0 when no app window is open
    check(isinstance(hwnd, int) and hwnd >= 0, "find_app_window: a window handle or 0, nothing raises", hwnd)
    fb.WINDOWS = False
    try:
        check(fb.find_app_window() == 0 and fb.already_running() is False, "not on Windows: no window, never running")
    finally:
        fb.WINDOWS = windows


class FakeUser32:
    """user32 over a list of (hwnd, class, title) windows, in EnumWindows' order; records what is done to them."""
    def __init__(self, windows):
        self.windows, self.done = windows, []

    def EnumWindows(self, cb, _):                # noqa: N802   (the Win32 names)
        for hwnd, _, _ in self.windows:
            if not cb(hwnd, 0):
                break
        return True

    def _of(self, hwnd):
        return next(w for w in self.windows if w[0] == hwnd)

    def GetClassNameW(self, hwnd, buf, n):       # noqa: N802
        buf.value = self._of(hwnd)[1][:n - 1]
        return len(buf.value)

    def GetWindowTextW(self, hwnd, buf, n):      # noqa: N802
        buf.value = self._of(hwnd)[2][:n - 1]
        return len(buf.value)

    def FindWindowW(self, cls, title):           # noqa: N802   the first window with that title (v1.2.0's way)
        return next((h for h, c, t in self.windows if t == title and cls in (None, c)), 0)

    def ShowWindow(self, hwnd, how):             # noqa: N802
        self.done.append(("ShowWindow", hwnd, how))

    def SetForegroundWindow(self, hwnd):         # noqa: N802
        self.done.append(("SetForegroundWindow", hwnd))


def win32(user32: FakeUser32, last_error: int):
    """A stand-in for the launcher's ctypes (fb.ctypes): user32, and a kernel32 whose CreateMutexW leaves `last_error`
    (183 = ERROR_ALREADY_EXISTS: the mutex was there, another app runs). Returns (the made mutexes, the stand-in)."""
    made = []

    def create_mutex(*a):                        # a function: restype / argtypes are set on it, as on ctypes' own
        made.append(a)
        return 77
    k32 = types.SimpleNamespace(CreateMutexW=create_mutex)
    return made, types.SimpleNamespace(windll=types.SimpleNamespace(user32=user32), WINFUNCTYPE=lambda *a: lambda f: f,
                                       create_unicode_buffer=ctypes.create_unicode_buffer, c_void_p=ctypes.c_void_p,
                                       WinDLL=lambda name, use_last_error=False: k32, get_last_error=lambda: last_error)


def test_second_launch():
    """A second launch finds the running app's window by its class and title (find_app_window: the Tk window named
    "Forex Bot V<version>", or v1.2.0's "Forex Bot", never a folder window of that name or the close question) and
    brings it to the front (already_running). Win32 is a stand-in: no window is looked at or moved, no mutex is made."""
    folder, ask = (1, "CabinetWClass", "Forex Bot"), (3, "TkTopLevel", "Închizi Forex Bot?")
    app, old = (2, "TkTopLevel", f"Forex Bot V{APP_VERSION}"), (4, "TkTopLevel", "Forex Bot")
    user32 = FakeUser32([folder, ask, app, old])
    real, windows, find = fb.ctypes, fb.WINDOWS, fb.find_app_window
    try:
        fb.WINDOWS = True
        _, fb.ctypes = win32(user32, 183)
        found = fb.find_app_window()
        user32.windows = [folder, (5, "TkTopLevel", "Close Forex Bot?"), old]
        found_old = fb.find_app_window()
        user32.windows = [folder, ask]
        found_none = fb.find_app_window()
        user32.windows = [folder, ask, app, old]
        fb.find_app_window = find
        made, fb.ctypes = win32(user32, 183)     # the mutex was there: another app runs
        running, front, mutex = fb.already_running(), list(user32.done), list(made)
        user32.done.clear()
        made, fb.ctypes = win32(user32, 0)       # the mutex is new: the first launch
        first = (fb.already_running(), list(user32.done))
        user32.done.clear()
        made, fb.ctypes = win32(user32, 183)
        user32.windows = [folder]                # running, but its window is not found: nothing is moved
        hidden = (fb.already_running(), list(user32.done))
    finally:
        fb.ctypes, fb.WINDOWS, fb.find_app_window = real, windows, find
        vars(fb.already_running).pop("mutex", None)
    check(found == 2, "find_app_window: the Tk window titled \"Forex Bot V<version>\" (not the folder window named "
          "\"Forex Bot\" before it, nor the close question)", found)
    check(found_old == 4 and found_none == 0, "... v1.2.0's \"Forex Bot\" Tk window too; none: 0",
          (found_old, found_none))
    check(running is True and front == [("ShowWindow", 2, 9), ("SetForegroundWindow", 2)]
          and [m[2] for m in mutex] == ["Local\\ForexBot.App"],
          "already_running on a second launch: True, the running app's \"Forex Bot V<version>\" window restored and "
          "brought to the front", (running, front, mutex))
    check(first == (False, []) and hidden == (True, []),
          "a first launch: not running, no window touched; running with no window found: True, nothing moved",
          (first, hidden))


def test_connector():
    """The connector has its own VERSION (the server gates plans on it, a 1.2.0 connector keeps working): a literal of
    its own, never bot/version.py's, which is not in the connector download."""
    src = (ROOT / "connector" / "connector.py").read_text(encoding="utf-8")
    m = re.search(r'^VERSION = "([^"]+)"', src, re.M)
    check(m is not None and dotted(m.group(1)) is not None and m.group(1) == fb.connector_version(),
          "connector.py: VERSION a dotted literal, as the launcher reads it", m and m.group(1))
    check(m is not None and dotted(m.group(1)) <= dotted(APP_VERSION),
          "the connector's version is never past the app's", (m and m.group(1), APP_VERSION))
    files = [f for f in main.CONNECTOR_FILES if f.endswith(".py")]
    imports = {}
    for f in files:
        for n in ast.walk(ast.parse((ROOT / f).read_text(encoding="utf-8"))):
            if isinstance(n, ast.ImportFrom) and n.module:
                imports.setdefault(f, set()).add(n.module)
            elif isinstance(n, ast.Import):
                imports.setdefault(f, set()).update(a.name for a in n.names)
    users = sorted(f for f, mods in imports.items() if {"bot.version", "version"} & mods
                   or any(m.startswith("bot.version") for m in mods))
    check("bot/version.py" not in main.CONNECTOR_FILES and not users and "connector/connector.py" in files,
          "no file of the connector download imports bot/version.py (not in it either)", users)


def main_():
    check(DATABASE_URL == os.environ["DATABASE_URL"] and TMP.as_posix() in DATABASE_URL, "test database is a temp file")
    try:
        test_constant()
        test_server()
        test_launcher()
        test_second_launch()
        test_connector()
    finally:
        db_engine.dispose()
        after = {p.name: p.stat().st_mtime_ns for d in (DATA_DIR, ROOT / "logs") if d.exists() for p in d.iterdir()}
        check(after == _before, "nothing written to data/ or logs/", sorted(set(after) ^ set(_before)))
        check(not any(getattr(h, "baseFilename", "").endswith("server.log") and not isinstance(h, logging.NullHandler)
                      for h in logging.getLogger().handlers), "no handler writing logs/server.log")
        shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nall checks passed")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main_()
