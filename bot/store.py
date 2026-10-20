"""SQLite store shared by the live bot (writer) and the dashboard (reader)."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, symbol TEXT, side TEXT, lots REAL,
    entry_time TEXT, entry_price REAL, sl REAL, tp REAL,
    exit_time TEXT, exit_price REAL, pnl REAL, status TEXT, comment TEXT, dry_run INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS equity (time TEXT, balance REAL, equity REAL);
CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, level TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT, updated TEXT);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, path: Path = DB_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.executescript(SCHEMA)

    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # ── writes (live bot) ──────────────────────────────────────
    def event(self, level: str, message: str):
        with self._conn() as c:
            c.execute("INSERT INTO events(time, level, message) VALUES (?,?,?)", (now_iso(), level, message))
            c.execute("DELETE FROM events WHERE id <= (SELECT MAX(id) - 1000 FROM events)")

    def open_trade(self, **t):
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO trades(ticket, symbol, side, lots, entry_time, entry_price, sl, tp, status, comment, dry_run)"
                " VALUES (:ticket,:symbol,:side,:lots,:entry_time,:entry_price,:sl,:tp,'open',:comment,:dry_run)",
                {"comment": "", "dry_run": 0, **t},
            )

    def close_trade(self, ticket: int, exit_time: str, exit_price: float, pnl: float):
        with self._conn() as c:
            c.execute(
                "UPDATE trades SET exit_time=?, exit_price=?, pnl=?, status='closed' WHERE ticket=?",
                (exit_time, exit_price, pnl, ticket),
            )

    def add_equity(self, balance: float, equity: float):
        with self._conn() as c:
            c.execute("INSERT INTO equity VALUES (?,?,?)", (now_iso(), balance, equity))

    def set_state(self, key: str, value):
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO state VALUES (?,?,?)", (key, json.dumps(value, default=str), now_iso()))

    # ── reads (dashboard + bot) ────────────────────────────────
    def get_state(self, key: str, default=None):
        with self._conn() as c:
            row = c.execute("SELECT value, updated FROM state WHERE key=?", (key,)).fetchone()
        if not row:
            return default, None
        return json.loads(row["value"]), row["updated"]

    def trades(self, limit: int = 200) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM trades ORDER BY entry_time DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def open_trade_tickets(self) -> set[int]:
        with self._conn() as c:
            return {r[0] for r in c.execute("SELECT ticket FROM trades WHERE status='open'")}

    def equity(self, limit: int = 2000) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM (SELECT * FROM equity ORDER BY time DESC LIMIT ?) ORDER BY time", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def events(self, limit: int = 50) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
