"""Database models (SQLAlchemy 2). SQLite by default; set DATABASE_URL for PostgreSQL on the VPS."""
import json
import math
from datetime import datetime, timezone

from sqlalchemy import (Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine,
                        event, inspect, select, text)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.types import TypeDecorator

from bot.rapid import clamp_rapid
from bot.scalp import clamp_short

from .burst import legacy_burst
from .dashboard import DEFAULT as DASHBOARD_DEFAULT, clamp_dashboard, clamp_market
from .settings import DATABASE_URL, DEFAULT_APP_SETTINGS, DEFAULT_USER_SETTINGS, MAX_SYMBOLS


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)   # stored as naive UTC everywhere


class JSONText(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return None if value is None else json.dumps(value)

    def process_result_value(self, value, dialect):
        return None if value is None else json.loads(value)


class Base(DeclarativeBase):
    pass


ROLES = ("founder", "admin", "client")


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120), default="")
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(16), default="client")
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    ban_reason: Mapped[str] = mapped_column(String(500), default="")
    plan: Mapped[str] = mapped_column(String(40), default="trial")
    subscription_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    token_version: Mapped[int] = mapped_column(Integer, default=0)   # bump to log the user out everywhere
    lang: Mapped[str] = mapped_column(String(5), default="en")
    telegram_chat_id: Mapped[str] = mapped_column(String(64), default="")
    settings: Mapped[dict] = mapped_column(JSONText, default=lambda: dict(DEFAULT_USER_SETTINGS))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    @property
    def is_staff(self) -> bool:
        return self.role in ("founder", "admin")

    @property
    def subscription_active(self) -> bool:
        if self.is_banned:
            return False
        if self.is_staff:
            return True
        return bool(self.subscription_until and self.subscription_until > utcnow())

    def merged_settings(self) -> dict:
        s = {**DEFAULT_USER_SETTINGS, **(self.settings or {})}
        s["modes"] = {**DEFAULT_USER_SETTINGS["modes"], **(s.get("modes") or {})}
        # a key the stored block lacks is read as the version that saved it ran (a v1.2.0 block: the stop in pips, no
        # analysis, Y following X), never as a new user's default; nothing stored is rewritten
        s["burst"] = legacy_burst((self.settings or {}).get("burst"))
        st = (self.settings or {}).get("short_term")    # the stored block: none has no max_hold_seconds (Y follows X)
        s["short_term"] = clamp_short(None, st if isinstance(st, dict) else None)
        rp = (self.settings or {}).get("rapid")         # none stored (a v1.2.0 user) or gone bad: the defaults, off
        s["rapid"] = clamp_rapid(None, rp if isinstance(rp, dict) else DEFAULT_USER_SETTINGS["rapid"])
        # Rapid and short-term exclude each other (main.put_my_settings never stores both on): settings written some
        # other way with both on keep short-term, the older one
        if s["rapid"]["enabled"] and s["short_term"]["enabled"]:
            s["rapid"]["enabled"] = False
        try:
            s["dashboard"] = clamp_dashboard(s.get("dashboard"))
        except ValueError:
            s["dashboard"] = clamp_dashboard(DASHBOARD_DEFAULT)      # a fresh copy, never the shared default
        try:                                    # v1.3.2; none stored (an older user): the defaults, a fresh copy
            s["market"] = clamp_market(None, (self.settings or {}).get("market"))
        except ValueError:
            s["market"] = clamp_market(None)
        return s


class ConnectorToken(Base):
    __tablename__ = "connector_tokens"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    label: Mapped[str] = mapped_column(String(80), default="My PC")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)


class Signal(Base):
    __tablename__ = "signals"
    id: Mapped[int] = mapped_column(primary_key=True)
    mode: Mapped[str] = mapped_column(String(20), index=True)        # trend | custom | news_ai
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    side: Mapped[str] = mapped_column(String(4))
    timeframe: Mapped[str] = mapped_column(String(4), default="H1")
    price: Mapped[float] = mapped_column(Float)                       # reference price when issued
    sl: Mapped[float] = mapped_column(Float)
    tp: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rationale: Mapped[str] = mapped_column(Text, default="")
    rationale_ro: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(12), default="open", index=True)  # open|won|lost|closed|expired
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    track_from: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)   # first bar used for SL/TP tracking
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    result_pips: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_reason: Mapped[str] = mapped_column(String(40), default="")
    # provenance for self-learning: {feed, model, features, sl_scale, tp_scale}; None on signals from before it
    meta: Mapped[dict | None] = mapped_column(JSONText, nullable=True)

    def to_dict(self) -> dict:
        """What clients, connectors and browsers get. `meta` (feed, features, model) stays on the server."""
        return {k: (v.isoformat() + "Z" if isinstance(v, datetime) else v)
                for k, v in ((c.name, getattr(self, c.name)) for c in self.__table__.columns) if k != "meta"}


class SignalSkip(Base):
    """Why a user's connector did not trade a signal (reported as signal_skipped)."""
    __tablename__ = "signal_skips"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    signal_id: Mapped[int] = mapped_column(Integer, index=True)
    reason: Mapped[str] = mapped_column(String(300), default="")
    time: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class LearnRun(Base):
    """One self-learning run (docs/SELF_LEARNING.md). The active model is the row with active=True.
    decision: running | improved | no_change | rejected | error;  status of the proposal: proposed | applied |
    reverted | superseded | none"""
    __tablename__ = "learn_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    trigger: Mapped[str] = mapped_column(String(12), default="scheduled")     # scheduled | manual
    mode: Mapped[str] = mapped_column(String(20), default="trend")
    decision: Mapped[str] = mapped_column(String(12), default="running")
    status: Mapped[str] = mapped_column(String(12), default="none")
    active: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    model: Mapped[dict | None] = mapped_column(JSONText, nullable=True)       # the proposal
    report: Mapped[dict | None] = mapped_column(JSONText, nullable=True)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_reason: Mapped[str] = mapped_column(String(300), default="")

    def to_dict(self, full: bool = False) -> dict:
        d = {k: (v.isoformat() + "Z" if isinstance(v, datetime) else v)
             for k, v in ((c.name, getattr(self, c.name)) for c in self.__table__.columns)}
        if not full and d["report"]:
            d["report"] = {k: v for k, v in d["report"].items() if k in ("summary", "explain", "error", "gate")}
        return d


class UserTrade(Base):
    __tablename__ = "user_trades"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    signal_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    ticket: Mapped[int] = mapped_column(Integer, index=True)
    symbol: Mapped[str] = mapped_column(String(20))
    side: Mapped[str] = mapped_column(String(4))
    lots: Mapped[float] = mapped_column(Float)
    entry_price: Mapped[float] = mapped_column(Float)
    sl: Mapped[float | None] = mapped_column(Float, nullable=True)
    tp: Mapped[float | None] = mapped_column(Float, nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(10), default="open")
    demo: Mapped[bool] = mapped_column(Boolean, default=True)
    burst_id: Mapped[int | None] = mapped_column(Integer, nullable=True)   # added by init_db on older databases
    # long (a trend signal) | short (a scalp of the connector) | burst (news burst) | news (an AI news signal) |
    # custom (a custom-rules signal) | test (an admin's test signal) | rapid (a Rapid run: a news run of kind rapid or
    # a local signal run, v1.3.0); these three columns added by init_db too
    strategy: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    risk: Mapped[float | None] = mapped_column(Float, nullable=True)          # money it would lose at its stop
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)    # 0..100, the setup's strength

    def to_dict(self) -> dict:
        return {k: (v.isoformat() + "Z" if isinstance(v, datetime) else v)
                for k, v in ((c.name, getattr(self, c.name)) for c in self.__table__.columns)}


class Burst(Base):
    """One news burst of one user: a straddle the connector places before a release (docs/NEWS_BURST.md).
    planned -> scheduled -> armed -> triggered -> closed | expired | skipped | cancelled | error"""
    __tablename__ = "bursts"
    __table_args__ = (UniqueConstraint("user_id", "symbol", "event_time"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    # CalendarEvent.id; a test's is None, or the event whose forecast its pre-news look reads (POST /api/bursts/test)
    event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_title: Mapped[str] = mapped_column(String(200), default="")
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    symbol: Mapped[str] = mapped_column(String(20))                                # base name, no broker suffix
    event_time: Mapped[datetime] = mapped_column(DateTime)                         # the release moment
    test: Mapped[bool] = mapped_column(Boolean, default=False)
    params: Mapped[dict] = mapped_column(JSONText, default=dict)                   # snapshot sent to the connector
    status: Mapped[str] = mapped_column(String(16), default="planned", index=True)
    side: Mapped[str | None] = mapped_column(String(8), nullable=True)             # buy | sell | both
    orders: Mapped[int] = mapped_column(Integer, default=0)
    fills: Mapped[int] = mapped_column(Integer, default=0)
    pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    # burst (the classic straddle) | rapid; the release's pre-trade analysis (JSON) or None. Both added by init_db on
    # older databases: v1.2.0 never names them, its rows get 'burst'
    kind: Mapped[str] = mapped_column(String(8), default="burst", server_default="burst")
    analysis: Mapped[dict | None] = mapped_column(JSONText, nullable=True)

    def to_dict(self) -> dict:
        return {k: (v.isoformat() + "Z" if isinstance(v, datetime) else v)
                for k, v in ((c.name, getattr(self, c.name)) for c in self.__table__.columns)}


class CalendarEvent(Base):
    __tablename__ = "calendar_events"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    time: Mapped[datetime] = mapped_column(DateTime, index=True)
    currency: Mapped[str] = mapped_column(String(8), index=True)
    title: Mapped[str] = mapped_column(String(255))
    impact: Mapped[str] = mapped_column(String(10))                    # High | Medium | Low | Holiday
    forecast: Mapped[str] = mapped_column(String(40), default="")
    previous: Mapped[str] = mapped_column(String(40), default="")
    actual: Mapped[str] = mapped_column(String(40), default="")
    source: Mapped[str] = mapped_column(String(40), default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class NewsItem(Base):
    __tablename__ = "news_items"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(80))
    title: Mapped[str] = mapped_column(String(500))
    link: Mapped[str] = mapped_column(String(1000))
    summary: Mapped[str] = mapped_column(Text, default="")
    published_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    analyzed: Mapped[bool] = mapped_column(Boolean, default=False)
    ai: Mapped[dict | None] = mapped_column(JSONText, nullable=True)    # {currencies, sentiment, impact, note}


class AIAnalysis(Base):
    __tablename__ = "ai_analyses"
    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    kind: Mapped[str] = mapped_column(String(20), default="news")
    model: Mapped[str] = mapped_column(String(60), default="")
    result: Mapped[dict] = mapped_column(JSONText)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)


class CoachMessage(Base):
    __tablename__ = "coach_messages"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    role: Mapped[str] = mapped_column(String(10))          # user | assistant
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class AuditLog(Base):
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(primary_key=True)
    time: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    actor_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    actor_email: Mapped[str] = mapped_column(String(255), default="")
    action: Mapped[str] = mapped_column(String(60))
    target_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_email: Mapped[str] = mapped_column(String(255), default="")
    details: Mapped[str] = mapped_column(Text, default="")


class AppSetting(Base):
    __tablename__ = "app_settings"
    key: Mapped[str] = mapped_column(String(60), primary_key=True)
    value: Mapped[object] = mapped_column(JSONText)


class EngineState(Base):
    """What the signal engine keeps across restarts (get_state / set_state), e.g. mode_bars: the bar each mode on
    its own timeframe decided on last, {"trend|EURUSD|D1": "2026-07-14T21:00:00"}."""
    __tablename__ = "engine_state"
    key: Mapped[str] = mapped_column(String(60), primary_key=True)
    value: Mapped[object] = mapped_column(JSONText)


# ── engine / session ───────────────────────────────────────────
_connect_args = {"check_same_thread": False, "timeout": 15} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args, pool_pre_ping=True)

if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# a trade's strategy from its burst or its signal's mode, as main.py books new ones: a Rapid news run (Burst.kind
# "rapid") is 'rapid', any other burst row 'burst' (one not found too). Test signals (main.TEST_SIGNAL_BASE and up) are
# never stored in signals; a signal not found counts as long-term
TRADE_STRATEGY = ("CASE WHEN burst_id IS NOT NULL THEN COALESCE((SELECT CASE bursts.kind WHEN 'rapid' THEN 'rapid' "
                  "END FROM bursts WHERE bursts.id = user_trades.burst_id), 'burst') "
                  "WHEN signal_id >= 1000000000 THEN 'test' "
                  "ELSE COALESCE((SELECT CASE mode WHEN 'news_ai' THEN 'news' WHEN 'custom' THEN 'custom' END "
                  "FROM signals WHERE signals.id = user_trades.signal_id), 'long') END")
NEW_SYMBOLS_130 = ("GBPJPY", "XAUUSD")       # pairs v1.3.0 trades too: appended once to a stored list (init_db)


def init_db():
    from bot.config import DATA_DIR
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)
    # create_all never alters an existing table: add columns that came after the first release, each checked on its
    # own (SQLite commits every ALTER by itself: a start cut short between two leaves the rest to the next start)
    for table, col, ddl in (("user_trades", "burst_id", "INTEGER"), ("user_trades", "strategy", "VARCHAR(8)"),
                            ("user_trades", "risk", "FLOAT"), ("user_trades", "confidence", "FLOAT"),
                            ("signals", "meta", "TEXT"), ("bursts", "kind", "VARCHAR(8) NOT NULL DEFAULT 'burst'"),
                            ("bursts", "analysis", "TEXT")):
        if col not in {c["name"] for c in inspect(engine).get_columns(table)}:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}"))
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_trades_strategy ON user_trades (strategy)"))
        # rows booked before strategies existed (or by an older connector), and AI news, custom and test signal
        # trades an earlier version booked as long-term
        conn.execute(text(f"UPDATE user_trades SET strategy = {TRADE_STRATEGY} WHERE strategy IS NULL "
                          f"OR (strategy = 'long' AND {TRADE_STRATEGY} <> 'long')"))
    with engine.begin() as conn:     # a run cut short by a restart never finishes
        conn.execute(text("UPDATE learn_runs SET decision = 'error', finished_at = CURRENT_TIMESTAMP "
                          "WHERE decision = 'running'"))
    with Session(engine) as db:
        _migrate_130_symbols(db)


def _migrate_130_symbols(db: Session):
    """Once (flag migr_130_symbols in engine_state): the admin's stored pairs get the ones v1.3.0 added to the
    defaults, and a pair the admin removes afterwards stays removed. The flag records the pairs the upgrade brought
    in, those the list in use lacked, so a rollback removes exactly these from the stored list. Nothing stored: the
    defaults carry both now and v1.2.0's had neither, so both are recorded (for a list the admin saves later). A
    stored list never grows past MAX_SYMBOLS (the admin could not save it again): a pair with no room is left out."""
    if get_state(db, "migr_130_symbols") is not None:
        return
    row = db.get(AppSetting, "symbols")
    stored = row is not None and isinstance(row.value, list)
    added = [s for s in NEW_SYMBOLS_130 if not stored or s not in row.value]
    if stored:
        added = added[:max(0, MAX_SYMBOLS - len(row.value))]
    if stored and added:
        row.value = [*row.value, *added]
    set_state(db, "migr_130_symbols", {"added": added})     # one commit with the list


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_app_settings(db: Session) -> dict:
    rows = {r.key: r.value for r in db.scalars(select(AppSetting))}
    merged = {**DEFAULT_APP_SETTINGS, **rows}
    merged["modes"] = {k: {**DEFAULT_APP_SETTINGS["modes"].get(k, {}), **v}
                       for k, v in {**DEFAULT_APP_SETTINGS["modes"], **(rows.get("modes") or {})}.items()}
    merged["burst"] = {**DEFAULT_APP_SETTINGS["burst"], **(rows.get("burst") or {})}
    merged["learning"] = {**DEFAULT_APP_SETTINGS["learning"], **(rows.get("learning") or {})}
    return merged


def save_app_settings(db: Session, values: dict):
    for key, value in values.items():
        if key not in DEFAULT_APP_SETTINGS:
            continue
        row = db.get(AppSetting, key)
        if row:
            row.value = value
        else:
            db.add(AppSetting(key=key, value=value))
    db.commit()


def get_state(db: Session, key: str, default=None):
    row = db.get(EngineState, key)
    return row.value if row else default


def set_state(db: Session, key: str, value):
    row = db.get(EngineState, key)
    if row:
        row.value = value
    else:
        db.add(EngineState(key=key, value=value))
    db.commit()


def burst_trades_pnl(db: Session, user_id: int, bid: int, default: float | None = None) -> float | None:
    """A burst's result: the P&L of its closed trades (`default` while none is closed, or on a sum that is not a
    number)."""
    pnls = db.scalars(select(UserTrade.pnl).where(UserTrade.user_id == user_id, UserTrade.burst_id == bid,
                                                  UserTrade.status == "closed")).all()
    total = round(sum(p or 0.0 for p in pnls), 2) if pnls else None
    return total if total is not None and math.isfinite(total) else default


def audit(db: Session, actor: "User | None", action: str, target: "User | None" = None, details: str = ""):
    db.add(AuditLog(actor_id=actor.id if actor else None, actor_email=actor.email if actor else "system",
                    action=action, target_id=target.id if target else None,
                    target_email=target.email if target else "", details=details))
    db.commit()
