"""Server configuration from environment variables (.env). Admin-editable runtime settings live in the DB."""
import os
import secrets
from pathlib import Path

from dotenv import load_dotenv

from bot.config import DATA_DIR, ROOT
from bot.rapid import DEFAULTS as RAPID_DEFAULTS, NEW_USER_MAX_HOLD as RAPID_NEW_USER_MAX_HOLD
from bot.scalp import DEFAULTS as SHORT_TERM_DEFAULTS, NEW_USER_MAX_HOLD

from .dashboard import DEFAULT as DASHBOARD_DEFAULT, MARKET_DEFAULT

load_dotenv(ROOT / ".env")


def _secret_key() -> str:
    key = _env("SECRET_KEY")
    if key:
        return key
    path = DATA_DIR / ".secret_key"          # dev convenience: generate once and keep
    path.parent.mkdir(parents=True, exist_ok=True)
    try:                                     # owner-only from the start (POSIX; Windows keeps the folder's ACL)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(secrets.token_urlsafe(48))
    return path.read_text(encoding="utf-8").strip()


def _env(name: str, default: str = "") -> str:
    """Env value, or `default` when unset OR blank (`.env.example` ships lines like `DATABASE_URL=`)."""
    return os.getenv(name, "").strip() or default


def _bool(name: str, default: bool) -> bool:
    return _env(name, str(default)).lower() in ("1", "true", "yes", "on")


DATABASE_URL = _env("DATABASE_URL", f"sqlite:///{(DATA_DIR / 'platform.db').as_posix()}")
SECRET_KEY = _secret_key()
COOKIE_SECURE = _bool("COOKIE_SECURE", False)          # set true behind HTTPS
SESSION_DAYS = int(_env("SESSION_DAYS", "7"))
PUBLIC_URL = _env("PUBLIC_URL", "http://127.0.0.1:8000")

PRICE_FEED = _env("PRICE_FEED", "demo")                  # demo | mt5 | twelvedata
TWELVEDATA_API_KEY = _env("TWELVEDATA_API_KEY")
DEMO_SECONDS_PER_BAR = float(_env("DEMO_SECONDS_PER_BAR", "0"))   # >0 speeds up the demo feed
MT5_SYMBOL_SUFFIX = _env("MT5_SYMBOL_SUFFIX")           # mt5 feed: broker names like EURUSD.m
# mt5 feed: hours the broker's server clock is ahead of UTC. Empty = learned from live ticks (recommended).
MT5_UTC_OFFSET_HOURS = float(_env("MT5_UTC_OFFSET_HOURS")) if _env("MT5_UTC_OFFSET_HOURS") else None
# mt5 feed: how that clock follows DST, for history: auto | nyclose | fixed | an IANA zone (see server/feeds/prices.py)
MT5_SERVER_TZ = _env("MT5_SERVER_TZ", "auto")

AI_MODEL = _env("AI_MODEL", "claude-opus-5-5")
AI_ENABLED = bool(os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN"))
TELEGRAM_BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN")

# Economic calendar (server/feeds/calendar.py): forexfactory (free weekly feed, no actual values) | investing
# (Investing.com through an Apify actor: needs APIFY_TOKEN, billed per row; ForexFactory stays the fallback)
CALENDAR_SOURCE = _env("CALENDAR_SOURCE", "forexfactory").lower()
APIFY_TOKEN = _env("APIFY_TOKEN")
INVESTING_ACTOR = _env("INVESTING_ACTOR", "kibaale~investing-economic-calendar-scraper")
INVESTING_COUNTRIES = [c.strip().upper() for c in _env("INVESTING_COUNTRIES", "US,EU,DE,GB,JP,CH,CA,AU,NZ").split(",")
                       if c.strip()]
INVESTING_FULL_HOURS = max(1.0, float(_env("INVESTING_FULL_HOURS", "12")))      # the next 7 days, High + Medium
INVESTING_TODAY_MINUTES = max(10.0, float(_env("INVESTING_TODAY_MINUTES", "60")))  # today's High releases

WEB_DIR = Path(__file__).resolve().parent / "web"
MAX_SYMBOLS = 20            # the admin's pair list holds at most this many (PUT /api/admin/engine, db.init_db)

# Defaults for the admin-editable engine settings (stored in DB on first start).
DEFAULT_APP_SETTINGS = {
    # GBPJPY and XAUUSD since v1.3.0 (a stored list gets them once: db.init_db, migr_130_symbols)
    "symbols": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "GBPJPY", "XAUUSD"],
    "timeframe": "H1",
    "modes": {
        # long-term: on its own timeframe when longer than the engine's (engine.mode_timeframe). Tested on 2023-2026
        # history, the EMA 20/50 crossover lost on H1 and H4 after costs and was slightly ahead on D1
        "trend": {"enabled": True, "strategy": "example_ema_cross", "timeframe": "D1",
                  "params": {"fast": 20, "slow": 50, "atr_period": 14, "sl_atr": 2.0, "tp_atr": 3.0}},
        "custom": {"enabled": False, "strategy": "my_strategy", "timeframe": "", "params": {}},
        "news_ai": {"enabled": True, "min_confidence": 70, "sl_atr": 1.5, "tp_atr": 2.5, "expire_hours": 24,
                    "min_interval_minutes": 30},
    },
    "event_guard_minutes": 30,
    "news_poll_minutes": 5,
    "rss_feeds": [
        "https://www.fxstreet.com/rss/news",
        "https://www.investing.com/rss/news_1.rss",
    ],
    "trial_days": 3,
    "signup_open": True,
    "coach_daily_limit": 30,
    "burst": {"enabled": True, "max_trades": 40},      # news burst: admin kill switch + cap on trades per burst
    # self-learning (docs/SELF_LEARNING.md): off | suggest | auto. Runs weekly (weekday 0 = Monday, 5 = Saturday,
    # markets closed) at hour_utc on history_bars bars; ai_lessons: Claude's post-mortems of losing AI signals
    "learning": {"mode": "suggest", "weekday": 5, "hour_utc": 6, "history_bars": 30000, "cost_pips": 1.0,
                 "ai_lessons": False},
}

DEFAULT_USER_SETTINGS = {
    "modes": {"trend": True, "custom": False, "news_ai": False},
    "execution": "notify",          # notify | confirm | auto
    "symbols": [],                  # empty = all symbols the engine trades
    "risk_per_trade_pct": 1.0,
    "max_open_trades": 3,
    "daily_loss_limit_pct": 3.0,
    "max_spread_pips": 3.0,
    "max_price_drift_pips": 5.0,    # skip execution if price moved this far from the signal price
    "follow_exits": True,
    "min_confidence": 70,           # for AI news signals
    "notify_telegram": True,
    "notify_browser": True,
    # news burst: straddle of stop orders placed by the connector just before high-impact releases
    # (docs/NEWS_BURST.md). risk_per_trade_pct above is the budget for the WHOLE group of tickets (sl_unit pips).
    "burst": {"enabled": False, "trades": 10, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0,
              "tp_start_pips": 4.0, "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90,
              "currencies": [],      # empty = all currencies
              "symbols": [],         # the pairs a release is traded on (burst.plan_for); empty = the currency's own
              # since v1.3.0, for users created from then on: the stop as money the whole run may lose (pct of
              # balance | usd; pips = sl_pips and risk_per_trade_pct, as before) with `lots` per order, hold until
              # profit from hold_seconds (X) up to max_hold_seconds (Y), the pre-news analysis on. A block stored by
              # v1.2.0 lacks these keys and keeps v1.2.0's behaviour (burst.legacy_burst)
              "sl_unit": "pct", "sl_money": 2.0, "lots": 0.10, "max_hold_seconds": 120, "analysis": True},
    # short-term strategy: fast trades run by the connector itself on live ticks (bot/scalp.py, docs/SHORT_TERM.md).
    # Long-term is the trend mode above. Its own risk range and daily limit; the settings above do not apply to it.
    # max_hold_seconds (Y, v1.3.0) for new users only: a stored block without it closes at hold_seconds, as before
    "short_term": {**SHORT_TERM_DEFAULTS, "max_hold_seconds": NEW_USER_MAX_HOLD},
    # Rapid (v1.3.0, bot/rapid.py): many stop-limits at once at a release (a Burst row of kind "rapid") or on a
    # short-term signal. Off; it excludes short_term.enabled (main.put_my_settings). A stored user without it gets this
    # block (db.User.merged_settings); a connector before v1.3.0 never sees it (main.for_connector)
    "rapid": {**RAPID_DEFAULTS, "triggers": dict(RAPID_DEFAULTS["triggers"]), "symbols": [],
              "max_hold_seconds": RAPID_NEW_USER_MAX_HOLD},
    "dashboard": DASHBOARD_DEFAULT,  # which numbers the dashboard shows (server/dashboard.py)
    "market": MARKET_DEFAULT,        # the market clock and its sessions card (v1.3.2, server/dashboard.py)
}
