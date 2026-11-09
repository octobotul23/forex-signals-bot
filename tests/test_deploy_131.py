"""updates/v1.3.1 deploy_131.py on throw-away folders: the kit's constants match the code (version, the one before,
connector, ?v=) and its files\\ is the dev tree's; prepare refuses a files\\ that is not v1.3.1, a live app that is not
v1.3.0 and an earlier backup, each before touching anything, and otherwise backs up exactly the files that change and
copies the database (the live one stays as it was); copy refuses a files\\ that changed since prepare and otherwise
leaves the live files identical; rollback puts v1.3.0 back byte for byte and removes the new files; check PASSes every
item when v1.3.1 runs well and each item FAILs on its own (server version, ?v=, the live files, connector 1.3.0, the MT5
feed, new errors; a server that does not answer), with the verdict and the rollback command; log records from before
the restart and a broker's refusal do not count; the command line offers prepare / copy / check / rollback and check's
verdict is its exit code.

    .venv\\Scripts\\python tests\\test_deploy_131.py

Never uses the network (the kit's GET is a fake, its opener and urlopen are guards) and never touches D:\\Forex Bot:
writes only to a temp folder, no __pycache__ in updates/. Exit code 0 = every check passed.
"""
import contextlib
import importlib.util
import io
import json
import re
import shutil
import sqlite3
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
KIT = ROOT / "updates" / "v1.3.1" / "deploy_131.py"
TMP = Path(tempfile.mkdtemp(prefix="fx-deploy-131-"))
FAILED: list[str] = []
NET: list = []
TREE = re.search(r'^APP_VERSION = "([^"]+)"', (ROOT / "bot" / "version.py").read_text(encoding="utf-8"), re.M).group(1)
if TREE != "1.3.1":     # this kit's checks are made of v1.3.1's tree (?v=16, its version); a later tree has its own kit
    print(f"skipped: the dev tree is v{TREE}, the v1.3.1 kit is checked against v1.3.1's files "
          "(tests/test_deploy_132.py checks the v1.3.2 kit)")
    sys.exit(0)
NOW = datetime.now(timezone.utc).replace(microsecond=0)
SINCE = (NOW - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")       # the restart, UTC


def check(ok, label: str, detail=""):
    print(f"{'ok  ' if ok else 'FAIL'} {label}" + (f"\n       got: {detail}" if not ok and detail != "" else ""))
    if not ok:
        FAILED.append(label)


def load():
    spec = importlib.util.spec_from_file_location("deploy_131_under_test", KIT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def guard(*a, **k):
    NET.append(a)
    raise AssertionError("the test reached for the network")


kit = load()
kit._DIRECT.open = guard
urllib.request.urlopen = guard
REAL = {k: getattr(kit, k) for k in ("DEV", "LIVE", "BACKUP", "STATE", "HERE", "fetch")}


def run(fn, *a):
    """fn(*a) with its output caught: (result, output, exit message or None)."""
    buf, code = io.StringIO(), None
    with contextlib.redirect_stdout(buf):
        try:
            res = fn(*a)
        except SystemExit as e:
            res, code = None, str(e.code)
    return res, buf.getvalue(), code


def write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def ver(v: str) -> str:
    return f'"""The app version."""\nAPP_VERSION = "{v}"\n'


def tree_files(root: Path) -> dict:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_constants():
    tree = re.search(r'^APP_VERSION = "([^"]+)"', (ROOT / "bot" / "version.py").read_text(encoding="utf-8"), re.M)
    conn = re.search(r'^VERSION = "([^"]+)"', (ROOT / "connector" / "connector.py").read_text(encoding="utf-8"), re.M)
    index = (ROOT / "server" / "web" / "index.html").read_text(encoding="utf-8")
    check(kit.VERSION == tree.group(1) == "1.3.1", "VERSION is bot/version.py's", (kit.VERSION, tree.group(1)))
    check(kit.CONNECTOR == conn.group(1), "CONNECTOR is the connector's VERSION", (kit.CONNECTOR, conn.group(1)))
    check(kit.BEFORE == "1.3.0" != kit.VERSION, "it goes over v1.3.0", kit.BEFORE)
    check(set(re.findall(r"\?v=(\d+)", index)) == {kit.ASSETS}, "ASSETS is the ?v= of server/web/index.html",
          (kit.ASSETS, set(re.findall(r"\?v=(\d+)", index))))
    check(kit.KIT130.is_file(), "the v1.3.0 kit (its log reader) is next to it", kit.KIT130)
    k = kit._kit130()
    check(all(callable(getattr(k, n, None)) for n in ("log_entries", "brief")) and hasattr(k, "ERRORS")
          and hasattr(k, "QUIET"), "the v1.3.0 kit offers log_entries, brief, ERRORS, QUIET")
    files = kit.DEV
    if files.is_dir():
        got = tree_files(files)
        stale = [rel for rel, data in got.items() if not (ROOT / rel).is_file() or (ROOT / rel).read_bytes() != data]
        check(not stale, "files\\ is the dev tree's (every file in it identical)", stale)
        need = {"bot/version.py", "server/web/index.html", "server/web/app.js", "server/web/i18n.js",
                "server/web/app.css"}
        check(need <= set(got), "files\\ carries the version and the web files", sorted(need - set(got)))
        check(not any("__pycache__" in rel or rel.startswith(("data/", "logs/")) or rel.endswith((".env", ".ini"))
                      for rel in got), "files\\ carries no data, logs, secrets or __pycache__")
    else:
        check(False, "files\\ is there", files)


def fake_live(name: str) -> tuple[Path, Path]:
    """A throw-away live folder (v1.3.0) and a files\\ for it (v1.3.1); the kit points at them."""
    live, dev = TMP / name / "live", TMP / name / "files"
    write(live / "bot" / "version.py", ver("1.3.0"))
    write(live / "server" / "web" / "app.js", "old app\n")
    write(live / "server" / "web" / "index.html", "<script src=/static/app.js?v=15></script>\n")
    write(live / "tests" / "test_web.py", "same\n")
    write(live / "logs" / "server.log", "")
    (live / "data").mkdir(parents=True)
    con = sqlite3.connect(live / "data" / "platform.db")
    con.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY)")
    con.execute("INSERT INTO signals VALUES (1)")
    con.commit(), con.close()
    write(dev / "bot" / "version.py", ver("1.3.1"))
    write(dev / "server" / "web" / "app.js", "new app\n")
    write(dev / "server" / "web" / "index.html", "<script src=/static/app.js?v=16></script>\n")
    write(dev / "server" / "web" / "clock.js", "new file\n")
    write(dev / "tests" / "test_web.py", "same\n")
    here = TMP / name / "kit"
    here.mkdir()
    kit.DEV, kit.LIVE, kit.HERE = dev, live, here
    kit.BACKUP, kit.STATE = live / "backups" / "before-v1.3.1", here / "deploy_131_state.json"
    return live, dev


def test_prepare_copy_rollback():
    live, dev = fake_live("round")
    before = tree_files(live)
    db_bytes = (live / "data" / "platform.db").read_bytes()
    untouched = lambda: tree_files(live) == before and not kit.STATE.exists()       # noqa: E731

    write(dev / "bot" / "version.py", ver("1.3.2"))
    _, _, code = run(kit.prepare)
    check(code and "not 1.3.1" in code and untouched(), "prepare refuses a files\\ that is not v1.3.1, nothing done",
          code)
    write(dev / "bot" / "version.py", ver("1.3.1"))
    write(live / "bot" / "version.py", ver("1.3.1"))
    _, _, code = run(kit.prepare)
    write(live / "bot" / "version.py", ver("1.3.0"))
    check(code and "not v1.3.0" in code and tree_files(live) == before and not kit.STATE.exists(),
          "prepare refuses a live app that is not v1.3.0, nothing done", code)
    kit.BACKUP.mkdir(parents=True)
    _, _, code = run(kit.prepare)
    kit.BACKUP.rmdir()
    check(code and "exists already" in code and tree_files(live) == before and not kit.STATE.exists(),
          "prepare refuses over an earlier backup, nothing done", code)

    _, out, code = run(kit.prepare)
    st = json.loads(kit.STATE.read_text(encoding="utf-8")) if kit.STATE.exists() else {}
    check(code is None and st.get("changed") == ["bot/version.py", "server/web/app.js", "server/web/index.html"]
          and st.get("new") == ["server/web/clock.js"], "prepare lists the changed and the new files (not the same ones)",
          (code, st))
    backed = {rel: (kit.BACKUP / rel).read_bytes() for rel in st.get("changed", []) if (kit.BACKUP / rel).is_file()}
    check(backed == {rel: before[rel] for rel in st.get("changed", [])}, "the backup holds the live files that change")
    copies = list((live / "data" / "backups").glob("platform-before-v131-*.db"))
    n = sqlite3.connect(copies[0]).execute("select count(*) from signals").fetchone()[0] if copies else None
    check(len(copies) == 1 and n == 1 and (live / "data" / "platform.db").read_bytes() == db_bytes,
          "prepare copies the database (1 signal), the live one stays as it was", (copies, n))

    write(dev / "server" / "web" / "extra.js", "late\n")
    _, _, code = run(kit.copy)
    check(code and "run prepare again" in code and (live / "server" / "web" / "app.js").read_text() == "old app\n",
          "copy refuses a files\\ that changed since prepare, nothing copied", code)
    (dev / "server" / "web" / "extra.js").unlink()
    _, out, code = run(kit.copy)
    check(code is None and kit.differing() == ([], []) and kit.app_version(live) == "1.3.1"
          and not (kit.HERE / "pyc_check.pyc").exists() and "Reporneste" in out,
          "copy leaves the live files identical to files\\ (v1.3.1), Python compiled, says to restart", code or out)

    _, out, code = run(kit.rollback)
    now = tree_files(live)
    kept = {rel: data for rel, data in now.items() if not rel.startswith(("backups/", "data/backups/"))}
    check(code is None and kept == before and "v1.3.0 again" in out,
          "rollback puts v1.3.0 back byte for byte and removes the new file", (code, sorted(set(kept) ^ set(before))))


def log_line(text: str, minutes_ago: float) -> str:
    stamp = (datetime.now() - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
    return f"{stamp} INFO     {text}"


GOOD_LOG = [log_line("server.engine: Engine started with mt5 feed", 8),
            log_line("server.main: Connector online for user 1 (v1.3.0)", 7)]


def checked(name: str, pages: dict | None = None, server_log: list | None = None, extra=None):
    """check on a fake root after a copy: pages override the fake GETs (an Exception value is raised)."""
    live, dev = fake_live(name)
    for p in dev.rglob("*"):
        if p.is_file():
            write(live / p.relative_to(dev), p.read_text(encoding="utf-8"))
    index = (ROOT / "server" / "web" / "index.html").read_text(encoding="utf-8")
    served = {"/api/public/info": json.dumps({"version": "1.3.1"}), "/": index, **(pages or {})}

    def fake(path):
        v = served[path]
        if isinstance(v, Exception):
            raise v
        return v
    kit.fetch = fake
    write(live / "logs" / "server.log", "\n".join(GOOD_LOG if server_log is None else server_log) + "\n")
    if extra:
        extra(live, dev)
    res, out, code = run(kit.check, SINCE, live)
    return res, out, code


def failed_alone(res, out, want: str, label: str):
    fails = [ln for ln in out.splitlines() if ln.startswith("FAIL ")]
    check(res is False and len(fails) == 1 and want in fails[0] and kit.ROLLBACK in out and "v1.3.0 back" in out,
          label, fails or out[-300:])


def test_check():
    res, out, code = checked("good")
    passes = [ln for ln in out.splitlines() if ln.startswith("PASS ")]
    check(res is True and len(passes) == 6 and "ALL GOOD: v1.3.1 runs (6 checks passed)" in out
          and kit.ROLLBACK not in out, "a v1.3.1 that runs well: 6 items PASS, ALL GOOD, no rollback advice",
          code or out)
    res, out, _ = checked("old-server", {"/api/public/info": json.dumps({"version": "1.3.0"})})
    failed_alone(res, out, "the server runs v1.3.1 (/api/public/info: 1.3.0)", "the server still reports 1.3.0: FAIL")
    index = (ROOT / "server" / "web" / "index.html").read_text(encoding="utf-8")
    res, out, _ = checked("old-page", {"/": index.replace("?v=16", "?v=15")})
    failed_alone(res, out, "?v=16; also ?v=15", "a page asking for ?v=15 (cached or old index.html): FAIL")
    res, out, _ = checked("mixed-page", {"/": index.replace("/static/app.js?v=16", "/static/app.js?v=15", 1)})
    failed_alone(res, out, "also ?v=15", "a page asking for ?v=16 and ?v=15: FAIL")
    res, out, _ = checked("down", {"/api/public/info": ConnectionRefusedError("refused"),
                                   "/": ConnectionRefusedError("refused")})
    fails = [ln for ln in out.splitlines() if ln.startswith("FAIL ")]
    check(res is False and len(fails) == 2 and all("ConnectionRefusedError" in f for f in fails)
          and out.count("PASS ") == 4, "the server does not answer: both pages FAIL, the other items are still checked",
          fails)
    res, out, _ = checked("stale-file", extra=lambda live, dev: write(live / "server" / "web" / "app.js", "old app\n"))
    failed_alone(res, out, "the live files are this kit's (1 differ)", "a live file that is not the kit's: FAIL")
    res, out, _ = checked("no-conn", server_log=GOOD_LOG[:1])
    failed_alone(res, out, "connector 1.3.0 online", "no connector online since the restart: FAIL")
    res, out, _ = checked("old-conn", server_log=[GOOD_LOG[0], GOOD_LOG[1].replace("v1.3.0", "v1.2.0")])
    failed_alone(res, out, "connector 1.3.0 online", "only connector 1.2.0 came online: FAIL")
    res, out, _ = checked("conn-before", server_log=[GOOD_LOG[0], log_line(GOOD_LOG[1].split("INFO     ")[1], 30)])
    failed_alone(res, out, "connector 1.3.0 online", "a connector online before the restart does not count: FAIL")
    res, out, _ = checked("no-engine", server_log=GOOD_LOG[1:])
    failed_alone(res, out, "the engine started on the MT5 feed", "the engine not on the MT5 feed: FAIL")
    res, out, _ = checked("error", server_log=GOOD_LOG + [log_line("ERROR server.main: boom", 2),
                                                          "Traceback (most recent call last):", "ValueError: boom"])
    failed_alone(res, out, "no new errors in server.log, app.log, connector.log (1)", "a new error after the restart: FAIL")
    check("ValueError: boom" in out, "the error is shown with its last line", out[-400:])
    res, out, _ = checked("old-error", server_log=[log_line("ERROR server.main: before", 30)] + GOOD_LOG
                          + [log_line("WARNING x: BrokerError ERROR 10029", 1)])
    check(res is True, "an error from before the restart and a broker's refusal do not count", out[-400:])
    res, out, _ = checked("conn-error", extra=lambda live, dev: write(live / "connector" / "connector.log",
                                                                       log_line("ERROR connector: lost", 1) + "\n"))
    failed_alone(res, out, "no new errors", "an error in connector.log: FAIL")
    _, _, code = run(kit.check, "yesterday")
    check(code and "check wants the restart time in UTC" in code, "check wants the restart time in UTC", code)


def test_cli():
    _, _, code = run(kit.main, ["deploy_131.py"])
    check(code and all(w in code for w in ("prepare", "copy", "check", "rollback")),
          "with no command the help names prepare / copy / check / rollback")
    checked("cli-good")
    _, _, code = run(kit.main, ["deploy_131.py", "check", SINCE])
    check(code == "0", "check's exit code is 0 when every item passed", code)
    checked("cli-bad", {"/api/public/info": json.dumps({"version": "1.3.0"})})
    _, _, code = run(kit.main, ["deploy_131.py", "check", SINCE])
    check(code == "1", "check's exit code is 1 when an item failed", code)


def main():
    try:
        for t in (test_constants, test_prepare_copy_rollback, test_check, test_cli):
            print(f"\n== {t.__name__}")
            for k, v in REAL.items():
                setattr(kit, k, v)
            t()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    check(not NET, "no network was reached", NET)
    check(not list((ROOT / "updates").rglob("__pycache__")), "no __pycache__ in updates/")
    print(f"\n{'all checks passed' if not FAILED else f'{len(FAILED)} check(s) failed'}")
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
