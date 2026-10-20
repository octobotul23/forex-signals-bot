"""Telegram notifications. Silently disabled if TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID are not set."""
import logging
import os
import time

import requests

log = logging.getLogger("notify")


class Notifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.token and self.chat_id)
        self._last_sent: dict[str, float] = {}

    def send(self, text: str, throttle_key: str | None = None, throttle_seconds: int = 600):
        """Send a message. With throttle_key, repeats of the same kind are muted for throttle_seconds."""
        if not self.enabled:
            return
        if throttle_key:
            last = self._last_sent.get(throttle_key, 0)
            if time.time() - last < throttle_seconds:
                return
            self._last_sent[throttle_key] = time.time()
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
        except requests.RequestException as e:
            log.warning("Telegram send failed: %s", type(e).__name__)
