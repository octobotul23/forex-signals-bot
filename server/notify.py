"""Per-user Telegram messages through one platform bot (TELEGRAM_BOT_TOKEN).

Each client opens a chat with the platform bot, gets their chat id (e.g. from @userinfobot) and saves it
in Settings.
"""
import logging

import requests

from bot.symbols import price_digits

from .settings import TELEGRAM_BOT_TOKEN

log = logging.getLogger("notify")


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN)


def send_telegram(chat_id: str, text: str) -> bool:
    if not (TELEGRAM_BOT_TOKEN and chat_id):
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", timeout=10, json={
            "chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
        return r.ok
    except requests.RequestException as e:
        log.warning("Telegram send failed: %s", type(e).__name__)
        return False


def signal_text(sig: dict, lang: str = "en") -> str:
    ro = lang == "ro"
    mode = {"trend": "Trend", "custom": "Custom" if not ro else "Reguli proprii", "news_ai": "AI News"}.get(sig["mode"], sig["mode"])
    side = sig["side"].upper() if not ro else ("CUMPĂRARE" if sig["side"] == "buy" else "VÂNZARE")
    d = price_digits(sig["symbol"])
    lines = [f"📈 <b>{side} {sig['symbol']}</b> · {mode}",
             f"{'Preț' if ro else 'Price'} {sig['price']:.{d}f} · SL {sig['sl']:.{d}f}" + (f" · TP {sig['tp']:.{d}f}" if sig.get("tp") else "")]
    if sig.get("confidence") is not None:
        lines.append(f"{'Încredere' if ro else 'Confidence'} {sig['confidence']}%")
    why = sig.get("rationale_ro") if ro and sig.get("rationale_ro") else sig.get("rationale")
    if why:
        lines.append(why[:500])
    return "\n".join(lines)
