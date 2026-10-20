"""Live connections: browser tabs (per user) and one trading connector per user."""
import asyncio
import logging
from collections import defaultdict

from fastapi import WebSocket

from .db import utcnow

log = logging.getLogger("hub")


class Hub:
    def __init__(self):
        self.browsers: dict[int, set[WebSocket]] = defaultdict(set)
        self.staff: set[int] = set()
        self.connectors: dict[int, WebSocket] = {}
        self.connector_state: dict[int, dict] = {}

    @staticmethod
    async def _send(ws: WebSocket, msg: dict) -> bool:
        try:
            await ws.send_json(msg)
            return True
        except Exception:
            return False

    # ── browsers ───────────────────────────────────────────────
    def add_browser(self, user_id: int, is_staff: bool, ws: WebSocket):
        self.browsers[user_id].add(ws)
        if is_staff:
            self.staff.add(user_id)

    def remove_browser(self, user_id: int, ws: WebSocket):
        self.browsers[user_id].discard(ws)
        if not self.browsers[user_id]:
            self.browsers.pop(user_id, None)
            self.staff.discard(user_id)

    async def to_user(self, user_id: int, msg: dict):
        for ws in list(self.browsers.get(user_id, ())):
            await self._send(ws, msg)

    async def to_staff(self, msg: dict):
        await asyncio.gather(*(self.to_user(uid, msg) for uid in list(self.staff)))

    async def to_browsers(self, msg: dict, user_ids: set[int] | None = None):
        targets = [ws for uid, socks in list(self.browsers.items())
                   if user_ids is None or uid in user_ids for ws in list(socks)]
        await asyncio.gather(*(self._send(ws, msg) for ws in targets))

    # ── connectors ─────────────────────────────────────────────
    async def set_connector(self, user_id: int, ws: WebSocket, hello: dict):
        old = self.connectors.get(user_id)
        if old is not None and old is not ws:
            await self._send(old, {"type": "replaced", "message": "Another connector logged in with your account."})
            try:
                await old.close()
            except Exception:
                pass
        self.connectors[user_id] = ws
        self.connector_state[user_id] = {
            "online": True, "since": utcnow().isoformat() + "Z", "version": hello.get("version", "?"),
            "account": hello.get("account") or {}, "positions": [], "updated": utcnow().isoformat() + "Z",
        }
        await self._push_state(user_id)

    async def update_connector(self, user_id: int, account: dict | None, positions: list | None):
        st = self.connector_state.setdefault(user_id, {"online": True})
        if account is not None:
            st["account"] = account
        if positions is not None:
            st["positions"] = positions
        st["updated"] = utcnow().isoformat() + "Z"
        await self._push_state(user_id)

    async def drop_connector(self, user_id: int, ws: WebSocket):
        if self.connectors.get(user_id) is ws:
            self.connectors.pop(user_id, None)
            st = self.connector_state.setdefault(user_id, {})
            st["online"] = False
            st["updated"] = utcnow().isoformat() + "Z"
            await self._push_state(user_id)

    async def _push_state(self, user_id: int):
        await self.to_user(user_id, {"type": "connector_state", "state": self.connector_state.get(user_id)})

    async def to_connector(self, user_id: int, msg: dict) -> bool:
        ws = self.connectors.get(user_id)
        return bool(ws) and await self._send(ws, msg)

    async def to_connectors(self, msg: dict, user_ids: set[int]):
        await asyncio.gather(*(self._send(ws, msg) for uid, ws in list(self.connectors.items()) if uid in user_ids))

    # ── access control ─────────────────────────────────────────
    def connected_user_ids(self) -> set[int]:
        return set(self.browsers) | set(self.connectors)

    async def kick(self, user_id: int, reason: str):
        """Close every socket of a user (ban, expired subscription, deleted account)."""
        for ws in list(self.browsers.get(user_id, ())):
            await self._send(ws, {"type": "kicked", "reason": reason})
            try:
                await ws.close()
            except Exception:
                pass
        self.browsers.pop(user_id, None)
        self.staff.discard(user_id)
        ws = self.connectors.pop(user_id, None)
        if ws:
            await self._send(ws, {"type": "kicked", "reason": reason})
            try:
                await ws.close()
            except Exception:
                pass
        if user_id in self.connector_state:
            self.connector_state[user_id]["online"] = False


hub = Hub()
