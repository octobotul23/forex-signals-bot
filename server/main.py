"""FastAPI application: REST API, live WebSockets for browsers and trading connectors, and the web app.

Run locally:   python -m uvicorn server.main:app --port 8000
On the VPS:    see deploy/DEPLOY.md (behind Caddy for HTTPS, one worker process)
"""
import asyncio
import io
import json
import logging
import math
import re
import secrets
import time
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from bot.config import DATA_DIR, LOGS_DIR, ROOT
from bot.indicators import atr
from bot.rapid import clamp_rapid
from bot.scalp import clamp_short
from bot.version import APP_VERSION

from . import ai, learning
from .auth import (COOKIE, active_user, can_manage, clear_session, client_ip, current_user, hash_password,
                   hash_token, issue_session, new_connector_token, rate_limit, staff_user, temp_password,
                   user_from_connector_token, user_from_token, validate_password, verify_password)
from .burst import (FINAL, RANK, RAPID, REPORTED, TIMED_OUT, USER_CANCEL, V130, candidates, clamp_burst,
                    needs_version, pairs_for, params_for, plan_payload, plan_runs, raise_max_hold, seconds_until,
                    version_ok)
from .burst import wants as wants_runs
from .dashboard import MODE_SCOPE, SCOPES, clamp_dashboard, clamp_market
from .db import (AIAnalysis, AuditLog, Burst, CalendarEvent, CoachMessage, ConnectorToken, LearnRun, NewsItem,
                 SessionLocal, Signal, SignalSkip, User, UserTrade, audit, burst_trades_pnl, get_app_settings, get_db,
                 init_db, save_app_settings, utcnow)
from .engine import engine, takes
from .feeds.prices import TF_SECONDS, pip_size
from .hub import hub
from .notify import send_telegram, telegram_enabled
from .settings import AI_ENABLED, AI_MODEL, DEFAULT_USER_SETTINGS, MAX_SYMBOLS, PRICE_FEED, PUBLIC_URL, WEB_DIR

LOGS_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, handlers=[logging.StreamHandler()])
# Attach the file handler even when the root logger was configured earlier (e.g. by `main.py serve`).
_root = logging.getLogger()
_server_log = str((LOGS_DIR / "server.log").resolve())
if not any(getattr(h, "baseFilename", None) == _server_log for h in _root.handlers):
    _fh = logging.FileHandler(_server_log, encoding="utf-8")
    _fh.setFormatter(logging.Formatter(_LOG_FORMAT))
    _root.addHandler(_fh)
    # `main.py serve` also wrote everything to bot.log (the v1 bot's log); keep the platform out of it.
    for _h in list(_root.handlers):
        if getattr(_h, "baseFilename", "").endswith("bot.log"):
            _root.removeHandler(_h)
            _h.close()
log = logging.getLogger("server")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
SETUP_FILE = DATA_DIR / "setup_token.txt"
VERSION = APP_VERSION       # the app's release (bot/version.py): GET /api/public/info, the web app's header
# the connector download (connector/ files go to the zip's top folder): every bot module connector.py imports
CONNECTOR_FILES = ["connector/connector.py", "connector/README.md", "connector/requirements.txt", "connector/run.bat",
                   "bot/__init__.py", "bot/hold.py", "bot/news_view.py", "bot/rapid.py", "bot/risk.py", "bot/scalp.py",
                   "bot/symbols.py", "bot/mt5_broker.py"]
ANALYSIS_MAX = 2000         # characters of JSON a burst's pre-news analysis may take (larger ones are not kept)
_setup_token: str | None = None


def _ensure_setup_token():
    """While no founder exists, a one-time setup token (printed in the log) is needed to create one."""
    global _setup_token
    with SessionLocal() as db:
        has_founder = db.scalar(select(User.id).where(User.role == "founder")) is not None
    if has_founder:
        _setup_token = None
        SETUP_FILE.unlink(missing_ok=True)
        return
    _setup_token = secrets.token_urlsafe(12)
    SETUP_FILE.write_text(_setup_token, encoding="utf-8")
    log.warning("No founder account yet. Open the web app and use this setup token: %s  (also saved in %s)",
                _setup_token, SETUP_FILE)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    _ensure_setup_token()
    await engine.start()
    yield
    await engine.stop()


app = FastAPI(title="FX Signals Platform", version=VERSION, lifespan=lifespan, docs_url=None, redoc_url=None)


@app.middleware("http")
async def security(request: Request, call_next):
    # CSRF guard: state-changing API calls must carry a custom header (cross-site forms cannot set one)
    if request.url.path.startswith("/api/") and request.method in ("POST", "PUT", "PATCH", "DELETE"):
        if request.headers.get("x-fx") != "1":
            return JSONResponse({"detail": "bad_request"}, status_code=400)
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    if request.url.path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")
    return response


# ── helpers ────────────────────────────────────────────────────
def iso(dt: datetime | None) -> str | None:
    return dt.isoformat() + "Z" if dt else None


def user_dict(u: User, full: bool = False) -> dict:
    d = {"id": u.id, "email": u.email, "name": u.name, "role": u.role, "lang": u.lang, "plan": u.plan,
         "subscription_until": iso(u.subscription_until), "subscription_active": u.subscription_active,
         "is_banned": u.is_banned, "telegram_chat_id": u.telegram_chat_id}
    if full:
        d.update(ban_reason=u.ban_reason, created_at=iso(u.created_at), last_login_at=iso(u.last_login_at),
                 connector=hub.connector_state.get(u.id, {}).get("online", False), online=u.id in hub.browsers)
    return d


def clamp(v, lo, hi, cast=float):
    try:
        v = cast(v)
        if not math.isfinite(v):                    # JSON allows NaN and Infinity; min(hi, nan) would save `hi`
            raise ValueError
    except (TypeError, ValueError, OverflowError):  # int(Infinity), or an int too big for a float
        raise HTTPException(400, "invalid_number")
    return max(lo, min(hi, v))


def wants(settings: dict, sig: dict) -> bool:
    return engine._wants(settings, sig)


def pairs_gone(st: dict, symbols: list[str]) -> bool:
    """The user chose short-term (or Rapid) pairs and the engine trades none of them any more (an admin removed
    them)."""
    return bool(st["symbols"]) and not any(x in symbols for x in st["symbols"])


def for_client(s: dict, symbols: list[str], web: bool = False) -> dict:
    """A user's merged settings as the connector (or, `web`, the web app) gets them: short-term pairs the engine no
    longer trades (an admin removed them) are left out. With none of the user's pairs left ("short_term_note":
    "pairs_gone"), the connector gets short-term off: no pair would mean every pair, which the user never picked. The
    web app gets the stored switch, so what it sends back is the user's (put_my_settings keeps the stored pairs unless
    it picked others: web_pairs_kept). The stored settings keep the pairs and the switch, so both come back if the
    admin adds a pair. Rapid's pairs (v1.3.0) the same way ("rapid_note": "pairs_gone")."""
    out = dict(s)
    for key, note in (("short_term", "short_term_note"), ("rapid", "rapid_note")):
        if key not in s:
            continue
        st = s[key]
        if pairs_gone(st, symbols):
            out[key], out[note] = {**st, "symbols": [], "enabled": st["enabled"] and web}, "pairs_gone"
        else:
            out[key] = {**st, "symbols": [x for x in st["symbols"] if x in symbols]}
    return out


def for_connector(s: dict, symbols: list[str], version) -> tuple[dict, list[str]]:
    """for_client's settings and the engine's pairs as a connector of `version` gets them: one before v1.3.0 without
    the pairs added in v1.3.0 (burst.pairs_for), in its short-term pairs too (its scalper would watch them; with none
    of the user's pairs left, short-term is off for it, as for_client does), and without the rapid block (Rapid needs
    v1.3.0; it excludes short-term, so such a connector sees short-term off while Rapid is on). The user's own signal
    pairs stay: such a connector only filters signals by them, and gets none on those pairs (engine.takes)."""
    syms = pairs_for(symbols, version)
    out = for_client(s, syms)
    if not version_ok(version, V130):
        out = {k: v for k, v in out.items() if k not in ("rapid", "rapid_note")}
    return out, syms


def web_pairs_kept(body: dict, st: dict, symbols: list[str]) -> bool:
    """Whether a short-term PUT's `symbols` leave the stored pairs as they are. The web app sends its whole short-term
    object, with `symbols_picked`: whether pairs were picked on that page. If not, its list is only what it was served
    (without the pairs the engine no longer trades, or [] with all of them gone; a page left open may even predate an
    admin's change), so the stored list, the removed pairs included, stays. [] while the pairs are gone always keeps
    them (a pair picked and unpicked again). A body without the flag (a page of an older version): kept when it is the
    list the web app is served now."""
    if "symbols" not in body:
        return True
    gone = pairs_gone(st, symbols)
    if body["symbols"] == [] and gone:
        return True
    if "symbols_picked" in body:
        return not body["symbols_picked"]
    return body["symbols"] == ([] if gone else [x for x in st["symbols"] if x in symbols])


# ── public / setup / auth ──────────────────────────────────────
class SetupIn(BaseModel):
    token: str
    email: str
    password: str
    name: str = ""


class RegisterIn(BaseModel):
    email: str
    password: str
    name: str = Field("", max_length=120)
    lang: str = "en"
    accept_terms: bool = False


class LoginIn(BaseModel):
    email: str
    password: str


@app.get("/api/public/info")
def public_info(db: Session = Depends(get_db)):
    s = get_app_settings(db)
    return {"version": VERSION, "setup_required": _setup_token is not None, "signup_open": bool(s["signup_open"]),
            "trial_days": int(s["trial_days"]), "ai_enabled": AI_ENABLED, "telegram_enabled": telegram_enabled(),
            "feed": PRICE_FEED}


@app.post("/api/setup")
def setup(body: SetupIn, request: Request, response: Response, db: Session = Depends(get_db)):
    rate_limit(f"setup:{client_ip(request)}", 10, 600)
    if _setup_token is None:
        raise HTTPException(409, "already_set_up")
    if not secrets.compare_digest(body.token.strip(), _setup_token):
        raise HTTPException(403, "bad_setup_token")
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "invalid_email")
    validate_password(body.password)
    user = db.scalar(select(User).where(User.email == email))
    if user:                                   # promote an existing account
        user.role, user.password_hash = "founder", hash_password(body.password)
        user.token_version += 1
    else:
        user = User(email=email, name=body.name.strip()[:120], password_hash=hash_password(body.password), role="founder",
                    plan="founder", settings=dict(DEFAULT_USER_SETTINGS))
        db.add(user)
    db.commit()
    audit(db, user, "founder_created", user)
    _ensure_setup_token()
    issue_session(response, user)
    return user_dict(user)


@app.post("/api/auth/register")
def register(body: RegisterIn, request: Request, response: Response, db: Session = Depends(get_db)):
    rate_limit(f"register:{client_ip(request)}", 5, 3600)
    s = get_app_settings(db)
    if not s["signup_open"]:
        raise HTTPException(403, "signup_closed")
    if not body.accept_terms:
        raise HTTPException(400, "terms_required")
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email) or len(email) > 255:
        raise HTTPException(400, "invalid_email")
    validate_password(body.password)
    if db.scalar(select(User.id).where(User.email == email)):
        raise HTTPException(409, "email_taken")
    days = int(s["trial_days"])
    user = User(email=email, name=body.name.strip(), password_hash=hash_password(body.password), role="client",
                lang="ro" if body.lang == "ro" else "en", plan="trial" if days else "none",
                subscription_until=utcnow() + timedelta(days=days) if days else None,
                settings=dict(DEFAULT_USER_SETTINGS), last_login_at=utcnow())
    db.add(user)
    db.commit()
    audit(db, user, "register", user, f"trial {days} days")
    issue_session(response, user)
    return user_dict(user)


_DUMMY_HASH = hash_password("timing-equaliser-not-a-password")


@app.post("/api/auth/login")
def login(body: LoginIn, request: Request, response: Response, db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    rate_limit(f"login-ip:{client_ip(request)}", 20, 600)
    rate_limit(f"login-email:{email}", 8, 600)
    user = db.scalar(select(User).where(User.email == email))
    ok = verify_password(user.password_hash if user else _DUMMY_HASH, body.password)
    if not user or not ok:
        raise HTTPException(401, "bad_credentials")
    if user.is_banned:
        raise HTTPException(403, {"code": "banned", "reason": user.ban_reason})
    user.last_login_at = utcnow()
    db.commit()
    issue_session(response, user)
    return user_dict(user)


@app.post("/api/auth/logout")
def logout(response: Response):
    clear_session(response)
    return {"ok": True}


@app.get("/api/auth/me")
def me(user: User = Depends(current_user)):
    return user_dict(user)


# ── own account ────────────────────────────────────────────────
class ProfileIn(BaseModel):
    name: str | None = Field(None, max_length=120)
    lang: str | None = None
    telegram_chat_id: str | None = Field(None, max_length=64)


class PasswordIn(BaseModel):
    current: str
    new: str


@app.get("/api/me/settings")
def get_my_settings(user: User = Depends(current_user), db: Session = Depends(get_db)):
    s = get_app_settings(db)
    return {"settings": for_client(user.merged_settings(), s["symbols"], web=True), "symbols": s["symbols"],
            "modes_enabled": {k: bool(v.get("enabled")) for k, v in s["modes"].items()},
            "burst_app": {"enabled": bool(s["burst"]["enabled"]), "max_trades": int(s["burst"]["max_trades"])}}


@app.put("/api/me/settings")
async def put_my_settings(body: dict, user: User = Depends(current_user), db: Session = Depends(get_db)):
    cur = user.merged_settings()
    app_s = get_app_settings(db)
    symbols = app_s["symbols"]
    if "modes" in body:
        cur["modes"] = {m: bool(body["modes"].get(m, cur["modes"].get(m))) for m in ("trend", "custom", "news_ai")}
    if "execution" in body:
        if body["execution"] not in ("notify", "confirm", "auto"):
            raise HTTPException(400, "invalid_execution")
        cur["execution"] = body["execution"]
    if "symbols" in body:
        cur["symbols"] = [s for s in body["symbols"] if s in symbols]
    limits = {"risk_per_trade_pct": (0.1, 5.0, float), "max_open_trades": (1, 20, int),
              "daily_loss_limit_pct": (0.5, 20.0, float), "max_spread_pips": (0.2, 20.0, float),
              "max_price_drift_pips": (0.5, 50.0, float), "min_confidence": (50, 100, int)}
    for key, (lo, hi, cast) in limits.items():
        if key in body:
            cur[key] = clamp(body[key], lo, hi, cast)
    for key in ("follow_exits", "notify_telegram", "notify_browser"):
        if key in body:
            cur[key] = bool(body[key])
    if "burst" in body:
        if not isinstance(body["burst"], dict):
            raise HTTPException(400, "invalid_burst")
        try:          # Y stays >= X (raised, never swapped); no Y the user did not have; a null Y removed (follows X)
            cur["burst"] = raise_max_hold({**cur["burst"], **clamp_burst(body["burst"], app_s["burst"]["max_trades"],
                                                                          cur["burst"])})
        except ValueError as e:
            raise HTTPException(400, str(e))
    if "short_term" in body:
        st_body = body["short_term"]
        if not isinstance(st_body, dict):
            raise HTTPException(400, "invalid_short_term")
        kept = web_pairs_kept(st_body, cur["short_term"], symbols)       # the web app's view of them (for_client)
        st_body = {k: v for k, v in st_body.items() if k != "symbols_picked" and not (k == "symbols" and kept)}
        try:                                     # pairs not sent: the stored ones are kept as they are (for_client)
            new = clamp_short(st_body, cur["short_term"], symbols if "symbols" in st_body else None)
        except ValueError as e:                  # its code: invalid_number, invalid_unit, invalid_enabled, ...
            raise HTTPException(400, str(e))
        except OverflowError:                    # an int too big for a float
            raise HTTPException(400, "invalid_number")
        if "symbols" in st_body and st_body["symbols"] and not new["symbols"]:
            raise HTTPException(400, "pairs_not_traded")   # none of the chosen pairs is traded: never "every pair"
        cur["short_term"] = new
    if "rapid" in body:                          # Rapid (v1.3.0): its pairs as short-term's
        rp_body = body["rapid"]
        if not isinstance(rp_body, dict):
            raise HTTPException(400, "invalid_rapid")
        kept = web_pairs_kept(rp_body, cur["rapid"], symbols)
        rp_body = {k: v for k, v in rp_body.items() if k != "symbols_picked" and not (k == "symbols" and kept)}
        try:
            new = clamp_rapid(rp_body, cur["rapid"], symbols if "symbols" in rp_body else None)
        except ValueError as e:                  # invalid_number, invalid_unit, invalid_symbols, invalid_triggers, ...
            raise HTTPException(400, str(e))
        except OverflowError:
            raise HTTPException(400, "invalid_number")
        if "symbols" in rp_body and rp_body["symbols"] and not new["symbols"]:
            raise HTTPException(400, "pairs_not_traded")
        cur["rapid"] = new
    # Rapid and short-term exclude each other: the one this body switches on turns the other off (both at once: 400)
    was = user.merged_settings()
    on = [k for k in ("short_term", "rapid") if cur[k]["enabled"] and not was[k]["enabled"]]
    if len(on) == 2:
        raise HTTPException(400, "rapid_excludes_short")
    if on:
        cur["rapid" if on[0] == "short_term" else "short_term"]["enabled"] = False
    if "dashboard" in body:
        try:
            cur["dashboard"] = clamp_dashboard(body["dashboard"])
        except ValueError as e:
            raise HTTPException(400, str(e))
    if "market" in body:                         # the market clock (v1.3.2): only the keys sent change
        try:
            cur["market"] = clamp_market(body["market"], cur["market"])
        except ValueError as e:
            raise HTTPException(400, str(e))
    user.settings = cur                          # reassign (JSON column is not mutation-tracked)
    db.commit()
    if set(body) - {"dashboard", "market"}:      # the dashboard's layout and the clock are nothing the connector needs
        sent, syms = for_connector(cur, symbols, _connector_info(user.id)["version"])
        await hub.to_connector(user.id, {"type": "settings", "settings": sent, "symbols": syms})
    return {"settings": for_client(cur, symbols, web=True)}


@app.put("/api/me/profile")
def put_profile(body: ProfileIn, user: User = Depends(current_user), db: Session = Depends(get_db)):
    if body.name is not None:
        user.name = body.name.strip()
    if body.lang in ("en", "ro"):
        user.lang = body.lang
    if body.telegram_chat_id is not None:
        tid = body.telegram_chat_id.strip()
        if tid and not re.fullmatch(r"-?\d{3,20}", tid):
            raise HTTPException(400, "invalid_telegram_id")
        user.telegram_chat_id = tid
    db.commit()
    return user_dict(user)


@app.post("/api/me/password")
def change_password(body: PasswordIn, response: Response, user: User = Depends(current_user),
                    db: Session = Depends(get_db)):
    rate_limit(f"pw:{user.id}", 10, 600)
    if not verify_password(user.password_hash, body.current):
        raise HTTPException(400, "bad_current_password")
    validate_password(body.new)
    user.password_hash = hash_password(body.new)
    user.token_version += 1                     # logs out every other device
    db.commit()
    issue_session(response, user)
    return {"ok": True}


@app.post("/api/me/telegram-test")
async def telegram_test(user: User = Depends(current_user)):
    if not telegram_enabled():
        raise HTTPException(409, "telegram_not_configured")
    if not user.telegram_chat_id:
        raise HTTPException(400, "no_telegram_id")
    text = "✅ Notificările funcționează." if user.lang == "ro" else "✅ Notifications are working."
    ok = await asyncio.to_thread(send_telegram, user.telegram_chat_id, text)
    if not ok:
        raise HTTPException(502, "telegram_failed")
    return {"ok": True}


# ── connector tokens & download ────────────────────────────────
class TokenIn(BaseModel):
    label: str = Field("My PC", max_length=80)


@app.get("/api/connector/tokens")
def list_tokens(user: User = Depends(current_user), db: Session = Depends(get_db)):
    rows = db.scalars(select(ConnectorToken).where(ConnectorToken.user_id == user.id, ConnectorToken.revoked.is_(False))
                      .order_by(ConnectorToken.created_at.desc()))
    return [{"id": r.id, "label": r.label, "created_at": iso(r.created_at), "last_used_at": iso(r.last_used_at)}
            for r in rows]


@app.post("/api/connector/tokens")
def create_token(body: TokenIn, user: User = Depends(active_user), db: Session = Depends(get_db)):
    count = db.scalar(select(func.count()).select_from(ConnectorToken)
                      .where(ConnectorToken.user_id == user.id, ConnectorToken.revoked.is_(False)))
    if count >= 5:
        raise HTTPException(400, "too_many_tokens")
    return {"token": new_connector_token(db, user, body.label.strip() or "My PC")}


@app.delete("/api/connector/tokens/{token_id}")
async def revoke_token(token_id: int, user: User = Depends(current_user), db: Session = Depends(get_db)):
    row = db.get(ConnectorToken, token_id)
    if not row or row.user_id != user.id:
        raise HTTPException(404, "not_found")
    row.revoked = True
    db.commit()
    if hub.connector_state.get(user.id, {}).get("token_id") == token_id:
        ws = hub.connectors.get(user.id)
        if ws:
            await hub.to_connector(user.id, {"type": "kicked", "reason": "token_revoked"})
            await ws.close()
    return {"ok": True}


@app.get("/api/connector/status")
def connector_status(user: User = Depends(current_user)):
    return hub.connector_state.get(user.id) or {"online": False}


@app.get("/api/connector/download")
def connector_download(user: User = Depends(active_user)):
    """Zip of the connector program (code only, no secrets) with this server's URL pre-filled."""
    missing = [f for f in CONNECTOR_FILES if not (ROOT / f).exists()]
    if missing:                      # a zip without them would only fail on the client's PC, at its first start
        log.error("Connector download: %s missing on the server", ", ".join(missing))
        raise HTTPException(500, "connector_incomplete")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in CONNECTOR_FILES:
            z.write(ROOT / f, f"fx-connector/{f.split('/', 1)[1] if f.startswith('connector/') else f}")
        z.writestr("fx-connector/connector.ini",
                   f"[connector]\nserver = {PUBLIC_URL}\n# paste the token from the web app (Connector page)\ntoken =\n"
                   f"# Safety: only demo accounts unless you set this to yes\nallow_real_account = no\n"
                   f"# if your broker names symbols like EURUSD.m, put the ending here (e.g. .m)\nsymbol_suffix =\n")
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip",
                             headers={"Content-Disposition": 'attachment; filename="fx-connector.zip"'})


# ── signals ────────────────────────────────────────────────────
@app.get("/api/signals")
def list_signals(status: str = "all", mode: str = "", symbol: str = "", limit: int = Query(100, le=500),
                 user: User = Depends(active_user), db: Session = Depends(get_db)):
    q = select(Signal).order_by(Signal.created_at.desc()).limit(limit)
    if status == "open":
        q = q.where(Signal.status == "open")
    elif status == "closed":
        q = q.where(Signal.status != "open")
    if mode:
        q = q.where(Signal.mode == mode)
    if symbol:
        q = q.where(Signal.symbol == symbol.upper())
    settings = user.merged_settings()
    rows = [s.to_dict() for s in db.scalars(q)]
    ids = [r["id"] for r in rows]
    # a skip is old news once the user's connector traded the signal (e.g. a manual Execute after an auto skip)
    traded = set(db.scalars(select(UserTrade.signal_id).where(UserTrade.user_id == user.id, UserTrade.signal_id.in_(ids))))
    skips = {sid: reason for sid, reason in db.execute(select(SignalSkip.signal_id, SignalSkip.reason).where(
        SignalSkip.user_id == user.id, SignalSkip.signal_id.in_(ids)).order_by(SignalSkip.id)).tuples()
        if sid not in traded}                                       # newest last: it wins
    return [{**r, "for_me": wants(settings, r), "skip_reason": skips.get(r["id"])} for r in rows]


@app.get("/api/signals/stats")
def signal_stats(days: int = Query(30, ge=1, le=365), user: User = Depends(active_user), db: Session = Depends(get_db)):
    since = utcnow() - timedelta(days=days)
    rows = db.scalars(select(Signal).where(Signal.created_at >= since)).all()
    out = {}
    for mode in ("trend", "custom", "news_ai"):
        sigs = [s for s in rows if s.mode == mode]
        closed = [s for s in sigs if s.status != "open" and s.result_pips is not None]
        wins = [s for s in closed if (s.result_pips or 0) > 0]
        out[mode] = {"total": len(sigs), "open": sum(s.status == "open" for s in sigs), "closed": len(closed),
                     "wins": len(wins), "win_rate": round(100 * len(wins) / len(closed), 1) if closed else None,
                     "pips": round(sum(s.result_pips or 0 for s in closed), 1)}
    return {"days": days, "modes": out}


@app.post("/api/signals/{signal_id}/execute")
async def execute_signal(signal_id: int, user: User = Depends(active_user), db: Session = Depends(get_db)):
    sig = db.get(Signal, signal_id)
    if not sig or sig.status != "open":
        raise HTTPException(409, "signal_not_open")
    if not hub.connectors.get(user.id):
        raise HTTPException(409, "connector_offline")
    if not takes(user.id, sig.symbol):           # a pair added in v1.3.0, on an older connector
        raise HTTPException(409, "connector_outdated")
    await hub.to_connector(user.id, {"type": "execute", "signal": sig.to_dict()})
    return {"ok": True}


# ── market data ────────────────────────────────────────────────
@app.get("/api/market/calendar")
def calendar(days_back: int = Query(1, le=7), days_ahead: int = Query(7, le=14), impact: str = "",
             user: User = Depends(active_user), db: Session = Depends(get_db)):
    now = utcnow()
    q = select(CalendarEvent).where(CalendarEvent.time.between(now - timedelta(days=days_back),
                                                               now + timedelta(days=days_ahead))).order_by(CalendarEvent.time)
    if impact:
        q = q.where(CalendarEvent.impact.in_(impact.split(",")))
    return [{"id": e.id, "time": iso(e.time), "currency": e.currency, "title": e.title, "impact": e.impact,
             "forecast": e.forecast, "previous": e.previous, "actual": e.actual, "source": e.source}
            for e in db.scalars(q)]


@app.get("/api/market/news")
def news(limit: int = Query(60, le=200), user: User = Depends(active_user), db: Session = Depends(get_db)):
    rows = db.scalars(select(NewsItem).order_by(NewsItem.published_at.desc()).limit(limit))
    return [{"id": n.id, "source": n.source, "title": n.title, "link": n.link, "summary": n.summary[:400],
             "published_at": iso(n.published_at), "ai": n.ai, "analyzed": n.analyzed} for n in rows]


@app.get("/api/market/analysis")
def analysis(user: User = Depends(active_user), db: Session = Depends(get_db)):
    row = db.scalar(select(AIAnalysis).where(AIAnalysis.kind == "news").order_by(AIAnalysis.created_at.desc()))
    if not row:
        return {"available": False, "ai_enabled": AI_ENABLED}
    return {"available": True, "ai_enabled": AI_ENABLED, "created_at": iso(row.created_at), "model": row.model,
            "summary_en": row.result.get("summary_en"), "summary_ro": row.result.get("summary_ro"),
            "biases": row.result.get("biases", [])}


@app.get("/api/market/prices")
def prices(user: User = Depends(active_user), db: Session = Depends(get_db)):
    out = []
    for sym in get_app_settings(db)["symbols"]:
        df = engine.candles.get(sym)
        if df is None or df.empty:
            out.append({"symbol": sym, "last": None})
            continue
        closes = df["close"].iloc[-48:].tolist()
        out.append({"symbol": sym, "last": closes[-1], "time": iso(df["time"].iat[-1].to_pydatetime()),
                    "change_pips": round((closes[-1] - closes[0]) / pip_size(sym), 1), "spark": closes})
    return out


# ── trades (reported by the user's connector) ──────────────────
@app.get("/api/trades")
def trades(limit: int = Query(200, le=1000), strategy: str = "", user: User = Depends(current_user),
           db: Session = Depends(get_db)):
    q = select(UserTrade).where(UserTrade.user_id == user.id).order_by(UserTrade.opened_at.desc()).limit(limit)
    if strategy in SCOPES and strategy != "all":
        q = q.where(UserTrade.strategy == strategy)
    rows = db.scalars(q).all()
    closed = [t for t in rows if t.status == "closed" and t.pnl is not None]
    wins = [t for t in closed if t.pnl > 0]
    return {"trades": [t.to_dict() for t in rows],
            "summary": {"closed": len(closed), "open": sum(t.status == "open" for t in rows),
                        "win_rate": round(100 * len(wins) / len(closed), 1) if closed else None,
                        "pnl": round(sum(t.pnl for t in closed), 2)}}


def _trade_numbers(closed: int, wins: int, pnl: float, gross_win: float, gross_loss: float) -> dict:
    return {"closed": closed, "wins": wins, "win_rate": round(100 * wins / closed, 1) if closed else None,
            "pnl": round(pnl, 2), "avg": round(pnl / closed, 2) if closed else None,
            "profit_factor": round(gross_win / -gross_loss, 2) if gross_loss < 0 else None}


def _account_demo(db: Session, user_id: int) -> bool:
    """Which account the user's numbers are about by default: the one their connector reports (online, or its last
    report since the server started), else the one of their newest trade, else demo."""
    demo = ((hub.connector_state.get(user_id) or {}).get("account") or {}).get("demo")
    if isinstance(demo, bool):
        return demo
    newest = db.scalar(select(UserTrade.demo).where(UserTrade.user_id == user_id)
                       .order_by(UserTrade.opened_at.desc(), UserTrade.id.desc()).limit(1))
    return newest is None or bool(newest)


@app.get("/api/trades/stats")
def trade_stats(days: str = Query("30", max_length=60), account: str = "", user: User = Depends(current_user),
                db: Session = Depends(get_db)):
    """The dashboard's numbers for the user's own closed trades on one kind of account (?account=demo|real, by
    default _account_demo; the reply says which): one block per period asked (?days=7,30), each split by
    strategy (dashboard.SCOPES; "all" counts test trades too), plus the trades open now."""
    if account not in ("", "demo", "real"):
        raise HTTPException(400, "invalid_account")
    try:
        periods = sorted({max(1, min(365, int(d))) for d in days.split(",") if d.strip()})
    except ValueError:
        raise HTTPException(400, "invalid_number")
    demo = account == "demo" if account else _account_demo(db, user.id)
    mine = (UserTrade.user_id == user.id, UserTrade.demo.is_(demo))   # demo and real money never add up
    now, out = utcnow(), {}
    win = case((UserTrade.pnl > 0, 1), else_=0)
    for d in periods[:12]:
        rows = db.execute(
            select(UserTrade.strategy, func.count(), func.sum(win), func.sum(UserTrade.pnl),
                   func.sum(case((UserTrade.pnl > 0, UserTrade.pnl), else_=0.0)),
                   func.sum(case((UserTrade.pnl < 0, UserTrade.pnl), else_=0.0)))
            .where(*mine, UserTrade.status == "closed", UserTrade.pnl.is_not(None),
                   UserTrade.closed_at >= now - timedelta(days=d))
            .group_by(UserTrade.strategy)).all()
        by = {s: [0, 0, 0.0, 0.0, 0.0] for s in SCOPES}
        for strat, n, w, pnl, gw, gl in rows:
            for key in {"all", strat} & by.keys():              # a test trade: in "all" only
                b = by[key]
                b[0] += n; b[1] += int(w or 0); b[2] += pnl or 0.0; b[3] += gw or 0.0; b[4] += gl or 0.0
        out[str(d)] = {k: _trade_numbers(*v) for k, v in by.items()}
    opened = dict.fromkeys(SCOPES, 0)
    for strat, n in db.execute(select(UserTrade.strategy, func.count()).where(
            *mine, UserTrade.status == "open").group_by(UserTrade.strategy)).all():
        for key in {"all", strat} & opened.keys():
            opened[key] += n
    return {"account": "demo" if demo else "real", "periods": out, "open": opened}


@app.post("/api/trades/{ticket}/close")
async def close_trade(ticket: int, user: User = Depends(active_user)):
    if not hub.connectors.get(user.id):
        raise HTTPException(409, "connector_offline")
    await hub.to_connector(user.id, {"type": "close", "ticket": ticket})
    return {"ok": True}


# ── auto-trading check: a test signal for the caller's own connector ──
class TestSignalIn(BaseModel):
    symbol: str | None = Field(None, max_length=20)
    side: str = Field("buy", pattern="^(buy|sell)$")


TEST_SIGNAL_BASE = 1_000_000_000          # test signal ids: above any real signal id, never stored as signals
TEST_SIGNAL_CONNECTOR = (1, 1, 2)         # first connector that caps test-signal risk and refuses them on real accounts
_test_signals: dict[int, dict[int, dict]] = {}   # user id -> {id: test signal} sent to their connector, not closed yet
_last_test_id = 0


def _new_test_id(db: Session) -> int:
    """Unique and increasing: about one id per second since mid-2025, so they fit the 32-bit Integer columns of
    PostgreSQL until 2061, and stay past the ones sent before a restart (a connector skips ids it has handled)."""
    global _last_test_id
    clock = TEST_SIGNAL_BASE + int(time.time()) - 1_750_000_000
    if not _last_test_id:                 # after a restart: past the ids connectors reported (a junk one: a day at most)
        _last_test_id = max(db.scalar(select(func.max(col)).where(col.between(TEST_SIGNAL_BASE, clock + 86400))) or 0
                            for col in (UserTrade.signal_id, SignalSkip.signal_id))
    _last_test_id = max(_last_test_id + 1, clock)
    return _last_test_id


def _check_test_connector(user_id: int, symbol: str | None = None):
    con = _connector_info(user_id)
    if not con["online"]:
        raise HTTPException(400, "connector_offline")
    if con["demo"] is not True:
        raise HTTPException(400, "demo_only")
    if not version_ok(con["version"], TEST_SIGNAL_CONNECTOR):    # older ones would trade it at full risk
        raise HTTPException(400, "connector_outdated_test")
    if symbol and not takes(user_id, symbol):                   # a pair added in v1.3.0, on an older connector
        raise HTTPException(400, "connector_outdated")


@app.post("/api/test-signal")
async def test_signal(body: TestSignalIn, user: User = Depends(active_user), db: Session = Depends(get_db)):
    """Send a signal to this user's connector only (demo accounts), the way real signals arrive: the connector
    trades it only if auto-trading would trade a real one. Close it with /api/test-signal/close."""
    s, app_s = user.merged_settings(), get_app_settings(db)
    _check_test_connector(user.id)
    if s.get("execution") != "auto":
        raise HTTPException(400, "execution_not_auto")
    modes = [m for m in ("trend", "custom", "news_ai") if (s.get("modes") or {}).get(m)]
    if not modes:
        raise HTTPException(400, "no_mode_enabled")
    allowed = [x for x in app_s["symbols"] if not s.get("symbols") or x in s["symbols"]]
    usable = [x for x in allowed if takes(user.id, x)]          # none of v1.3.0's pairs on an older connector
    symbol = (body.symbol or (usable[0] if usable else "")).strip().upper()
    if symbol not in allowed:
        raise HTTPException(400, "invalid_symbol")
    _check_test_connector(user.id, symbol)
    df = engine.candles.get(symbol)
    if df is None or len(df) < 20 or engine.feed is None:
        raise HTTPException(409, "no_price_yet")
    rate_limit(f"test-signal:{user.id}", 5, 300)
    price = await asyncio.to_thread(engine.feed.quote, symbol) or float(df["close"].iat[-1])
    # again, with no await until it is sent: another connector of this user may have logged in meanwhile
    _check_test_connector(user.id, symbol)
    a = float(atr(df, 14).iat[-1])
    sign = 1 if body.side == "buy" else -1
    now = utcnow()
    sig = {"id": _new_test_id(db), "mode": modes[0], "symbol": symbol,
           "side": body.side, "timeframe": app_s["timeframe"], "price": price, "sl": price - sign * 1.5 * a,
           "tp": price + sign * 2.5 * a, "confidence": 100, "status": "open", "test": True,
           "rationale": "Test signal: checks that auto-trading works (demo account).",
           "rationale_ro": "Semnal de test: verifică dacă tranzacționarea automată funcționează (cont demo).",
           "created_at": iso(now), "expires_at": iso(now + timedelta(minutes=10))}
    if not await hub.to_connector(user.id, {"type": "signal", "signal": sig}):
        raise HTTPException(409, "connector_offline")
    mine = _test_signals.setdefault(user.id, {})
    for sid in [k for k, v in mine.items() if _parse_time(v["expires_at"]) < now]:
        del mine[sid]                     # can no longer open a trade; one it opened is found from its trade
    mine[sig["id"]] = sig
    audit(db, user, "test_signal", None, f"#{sig['id']} {sig['side']} {symbol}")
    return {"signal": sig, "feed": engine.feed.name}


@app.post("/api/test-signal/close")
async def test_signal_close(user: User = Depends(active_user), db: Session = Depends(get_db)):
    """Close every test signal of this user the way the engine closes real ones (`signal_closed`, followed by the
    connector), also those only known from their trades (sent before a server restart)."""
    if not hub.connectors.get(user.id):
        raise HTTPException(409, "connector_offline")
    mine = _test_signals.setdefault(user.id, {})
    rows = db.execute(select(UserTrade.signal_id, UserTrade.ticket).where(
        UserTrade.user_id == user.id, UserTrade.status == "open", UserTrade.signal_id >= TEST_SIGNAL_BASE)).tuples().all()
    live = [(_int(p.get("signal_id")), _int(p.get("ticket")))           # the snapshot also has trades not reported
            for p in (hub.connector_state.get(user.id) or {}).get("positions") or [] if isinstance(p, dict)]
    live = [(sid, t) for sid, t in live if sid is not None and sid >= TEST_SIGNAL_BASE and t is not None]
    ids = sorted(set(mine) | {sid for sid, _ in rows + live})
    if not ids:
        raise HTTPException(404, "no_test_signal")
    now = iso(utcnow())
    for sid in ids:
        sig = {**mine.get(sid, {"id": sid, "test": True}), "status": "closed", "close_reason": "test finished",
               "closed_at": now}
        if not await hub.to_connector(user.id, {"type": "signal_closed", "signal": sig}):
            raise HTTPException(409, "connector_offline")           # the ones not sent stay for the next try
        mine.pop(sid, None)
    tickets = sorted({t for _, t in rows + live})
    if not user.merged_settings().get("follow_exits", True):      # the connector ignores signal_closed then
        for t in tickets:
            await hub.to_connector(user.id, {"type": "close", "ticket": t})
    return {"ok": True, "signal_id": ids[-1], "tickets": tickets}


# ── news burst (planned by the engine, run by the user's connector) ──
class BurstTestIn(BaseModel):
    symbol: str = Field(..., max_length=20)
    offset_pips: float | None = None
    event_id: str | None = Field(None, max_length=64)   # a calendar event whose forecast the pre-news look reads


def _connector_info(user_id: int) -> dict:
    st = hub.connector_state.get(user_id) or {}
    return {"online": user_id in hub.connectors, "demo": (st.get("account") or {}).get("demo"),
            "version": st.get("version")}


@app.get("/api/bursts")
def list_bursts(user: User = Depends(active_user), db: Session = Depends(get_db)):
    s, app_s, now = user.merged_settings(), get_app_settings(db), utcnow()
    rows = db.scalars(select(Burst).where(Burst.user_id == user.id)
                      .order_by(Burst.event_time.desc(), Burst.id.desc()).limit(50))
    events = db.scalars(select(CalendarEvent).where(CalendarEvent.impact == "High", CalendarEvent.time > now,
                                                    CalendarEvent.time <= now + timedelta(days=7))
                        .order_by(CalendarEvent.time))
    # a release whose burst already ended (cancelled, switched off and on again, skipped) is not traded again; the
    # runs as burst_tick plans them (plan_runs: Rapid's first, then the classic burst's on the pairs Rapid left). With
    # Rapid's news runs off the classic burst's are listed switched off too, as up to v1.2.0 (what it would trade).
    # In time order before the first ten are kept (the Dashboard's next release, the Market page's chips); the sort is
    # stable, so at one release Rapid's runs stay before the classic burst's
    done = set(db.execute(select(Burst.symbol, Burst.event_time, Burst.currency, Burst.kind).where(
        Burst.user_id == user.id, Burst.status.in_(FINAL), Burst.event_time > now)).tuples())
    classic = wants_runs(s, "burst") or not wants_runs(s, "rapid")
    runs = sorted(plan_runs(s, candidates(events, app_s), app_s["symbols"], done, classic), key=lambda r: r[0]["time"])
    upcoming = [{"event_id": c["event_id"], "time": iso(c["time"]), "currency": c["currency"], "title": c["title"],
                 "symbol": c["symbol"], "kind": kind} for c, kind in runs]
    return {"bursts": [b.to_dict() for b in rows], "upcoming": upcoming[:10],
            "app": {"enabled": bool(app_s["burst"]["enabled"]), "max_trades": int(app_s["burst"]["max_trades"])},
            "connector": _connector_info(user.id)}


@app.post("/api/bursts/test")
async def burst_test(body: BurstTestIn, user: User = Depends(active_user), db: Session = Depends(get_db)):
    """A burst 15 s from now on a demo account, to see the whole cycle without waiting for a release."""
    app_s, con, symbol = get_app_settings(db), _connector_info(user.id), body.symbol.strip().upper()
    if not app_s["burst"]["enabled"]:
        raise HTTPException(400, "burst_disabled")
    if not con["online"]:
        raise HTTPException(400, "connector_offline")
    if con["demo"] is not True:
        raise HTTPException(400, "demo_only")
    if not version_ok(con["version"]):
        raise HTTPException(400, "connector_outdated")
    if symbol not in app_s["symbols"]:
        raise HTTPException(400, "invalid_symbol")
    if body.event_id is not None and db.get(CalendarEvent, body.event_id) is None:
        raise HTTPException(400, "invalid_event")
    override = {"lead_seconds": 15}
    if body.offset_pips is not None:
        override["offset_pips"] = clamp(body.offset_pips, 1, 100)
    params = params_for(user.merged_settings(), app_s["burst"]["max_trades"], **override)
    if not version_ok(con["version"], needs_version(params, "burst", symbol)):    # what the run uses, on that pair
        raise HTTPException(400, "connector_outdated")
    if db.scalar(select(Burst.id).where(Burst.user_id == user.id, Burst.test.is_(True), Burst.status.not_in(FINAL))):
        raise HTTPException(400, "test_running")
    rate_limit(f"burst-test:{user.id}", 3, 300)
    row = Burst(user_id=user.id, event_id=body.event_id, event_title="Test burst", symbol=symbol,
                event_time=utcnow() + timedelta(seconds=15), test=True, params=params)
    db.add(row)
    db.commit()
    context = engine.burst_contexts([row], utcnow()).get(row.id)     # the event's forecast, if any; the AI's bias
    await hub.to_connector(user.id, {"type": "burst", "burst": plan_payload(row, utcnow(), context)})
    payload = row.to_dict()
    await hub.to_user(user.id, {"type": "burst", "burst": payload})
    return payload


@app.post("/api/bursts/{burst_id}/cancel")
async def burst_cancel(burst_id: int, user: User = Depends(current_user), db: Session = Depends(get_db)):
    row = db.get(Burst, burst_id)
    if not row or row.user_id != user.id:
        raise HTTPException(404, "not_found")
    if row.status in FINAL:
        raise HTTPException(400, "burst_finished")
    if row.status == "planned":
        row.status = "cancelled"
        await engine.send_burst_cancel(user.id, row.id)         # the plan may already be on its way
    elif not await engine.send_burst_cancel(user.id, row.id):
        raise HTTPException(409, "connector_offline")           # only the connector can remove its orders
    row.reason = USER_CANCEL                                    # burst_tick re-sends the cancel after a reconnect
    db.commit()
    payload = row.to_dict()
    await hub.to_user(user.id, {"type": "burst", "burst": payload})
    return payload


# ── AI coach ───────────────────────────────────────────────────
class CoachIn(BaseModel):
    text: str = Field(..., min_length=1, max_length=2000)
    signal_id: int | None = None


@app.get("/api/coach/history")
def coach_history(user: User = Depends(active_user), db: Session = Depends(get_db)):
    rows = db.scalars(select(CoachMessage).where(CoachMessage.user_id == user.id)
                      .order_by(CoachMessage.created_at.desc()).limit(60)).all()
    return {"ai_enabled": AI_ENABLED, "messages": [{"role": m.role, "content": m.content, "time": iso(m.created_at)}
                                                   for m in reversed(rows)]}


@app.delete("/api/coach/history")
def coach_clear(user: User = Depends(active_user), db: Session = Depends(get_db)):
    for m in db.scalars(select(CoachMessage).where(CoachMessage.user_id == user.id)):
        db.delete(m)
    db.commit()
    return {"ok": True}


@app.post("/api/coach/message")
async def coach_message(body: CoachIn, user: User = Depends(active_user), db: Session = Depends(get_db)):
    if not AI_ENABLED:
        raise HTTPException(409, "ai_not_configured")
    rate_limit(f"coach:{user.id}", 6, 60)
    limit = int(get_app_settings(db)["coach_daily_limit"])
    today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    used = db.scalar(select(func.count()).select_from(CoachMessage).where(
        CoachMessage.user_id == user.id, CoachMessage.role == "user", CoachMessage.created_at >= today))
    if not user.is_staff and used >= limit:
        raise HTTPException(429, "coach_daily_limit")
    history = [{"role": m.role, "content": m.content} for m in reversed(db.scalars(
        select(CoachMessage).where(CoachMessage.user_id == user.id).order_by(CoachMessage.created_at.desc()).limit(12)).all())]
    s = user.merged_settings()
    context = {"client_settings": {k: s[k] for k in ("modes", "execution", "risk_per_trade_pct", "max_open_trades",
                                                     "daily_loss_limit_pct")}}
    latest = db.scalar(select(AIAnalysis).order_by(AIAnalysis.created_at.desc()))
    if latest:
        context["market_summary"] = latest.result.get("summary_en")
    if body.signal_id:
        sig = db.get(Signal, body.signal_id)
        if sig:
            context["signal_in_question"] = sig.to_dict()
    context["recent_signals"] = [{k: v for k, v in s.to_dict().items() if k in ("id", "mode", "symbol", "side", "status",
                                                                                  "result_pips", "created_at")}
                                 for s in db.scalars(select(Signal).order_by(Signal.created_at.desc()).limit(8))]
    try:
        reply = await asyncio.to_thread(ai.coach_reply, user.lang, history, body.text, context)
    except ai.AIError as e:
        raise HTTPException(503, str(e))
    db.add(CoachMessage(user_id=user.id, role="user", content=body.text))
    db.add(CoachMessage(user_id=user.id, role="assistant", content=reply))
    db.commit()
    return {"reply": reply, "used_today": used + 1, "limit": None if user.is_staff else limit}


# ── admin ──────────────────────────────────────────────────────
class AdminCreateIn(BaseModel):
    email: str
    name: str = ""
    role: str = "client"
    days: int = Field(30, ge=0, le=3650)
    plan: str = "monthly"


class AdminPatchIn(BaseModel):
    ban: bool | None = None
    ban_reason: str | None = Field(None, max_length=500)
    extend_days: int | None = Field(None, ge=-3650, le=3650)
    subscription_until: str | None = None      # ISO date, or "" to cut immediately
    plan: str | None = Field(None, max_length=40)
    role: str | None = None
    name: str | None = Field(None, max_length=120)


def _target(db: Session, actor: User, user_id: int) -> User:
    target = db.get(User, user_id)
    if not target:
        raise HTTPException(404, "not_found")
    if not can_manage(actor, target):
        raise HTTPException(403, "not_allowed")
    return target


@app.get("/api/admin/overview")
def admin_overview(actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    users = db.scalars(select(User)).all()
    today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    tokens = db.execute(select(func.coalesce(func.sum(AIAnalysis.input_tokens), 0),
                               func.coalesce(func.sum(AIAnalysis.output_tokens), 0))
                        .where(AIAnalysis.created_at >= today)).one()
    return {"users": len(users), "clients": sum(u.role == "client" for u in users),
            "active_clients": sum(u.role == "client" and u.subscription_active for u in users),
            "banned": sum(u.is_banned for u in users), "online": len(hub.browsers), "connectors": len(hub.connectors),
            "signals_today": db.scalar(select(func.count()).select_from(Signal).where(Signal.created_at >= today)),
            "ai_runs_today": db.scalar(select(func.count()).select_from(AIAnalysis).where(AIAnalysis.created_at >= today)),
            "ai_tokens_today": {"input": tokens[0], "output": tokens[1]}, "engine": engine_status()}


@app.get("/api/admin/users")
def admin_users(q: str = "", user: User = Depends(staff_user), db: Session = Depends(get_db)):
    stmt = select(User).order_by(User.created_at.desc())
    if q:
        stmt = stmt.where(User.email.contains(q.lower()) | User.name.contains(q))
    return [user_dict(u, full=True) | {"manageable": can_manage(user, u)} for u in db.scalars(stmt)]


@app.post("/api/admin/users")
def admin_create_user(body: AdminCreateIn, actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "invalid_email")
    if body.role not in ("client", "admin"):
        raise HTTPException(400, "invalid_role")
    if body.role == "admin" and actor.role != "founder":
        raise HTTPException(403, "founder_only")
    if db.scalar(select(User.id).where(User.email == email)):
        raise HTTPException(409, "email_taken")
    pw = temp_password()
    u = User(email=email, name=body.name.strip(), password_hash=hash_password(pw), role=body.role, plan=body.plan,
             subscription_until=utcnow() + timedelta(days=body.days) if body.days else None,
             settings=dict(DEFAULT_USER_SETTINGS))
    db.add(u)
    db.commit()
    audit(db, actor, "user_create", u, f"role={body.role} days={body.days}")
    return {"user": user_dict(u, full=True), "temp_password": pw}


@app.patch("/api/admin/users/{user_id}")
async def admin_patch_user(user_id: int, body: AdminPatchIn, actor: User = Depends(staff_user),
                           db: Session = Depends(get_db)):
    t = _target(db, actor, user_id)
    changes, kick = [], None
    if body.name is not None:
        t.name = body.name.strip()
        changes.append("name")
    if body.ban is not None:
        if t.role == "founder":
            raise HTTPException(403, "founder_protected")
        if t.id == actor.id:
            raise HTTPException(400, "cannot_ban_self")
        t.is_banned = body.ban
        t.ban_reason = (body.ban_reason or "").strip() if body.ban else ""
        if body.ban:
            t.token_version += 1
            kick = "banned"
        changes.append(f"ban={body.ban}" + (f" ({t.ban_reason})" if body.ban and t.ban_reason else ""))
    if body.extend_days:
        base = max(t.subscription_until or utcnow(), utcnow())
        t.subscription_until = base + timedelta(days=body.extend_days)
        changes.append(f"extend {body.extend_days}d -> {t.subscription_until:%Y-%m-%d}")
    if body.subscription_until is not None:
        if body.subscription_until == "":
            t.subscription_until = utcnow()               # cut now
            changes.append("subscription cut")
        else:
            try:
                t.subscription_until = datetime.fromisoformat(body.subscription_until.replace("Z", ""))
            except ValueError:
                raise HTTPException(400, "invalid_date")
            changes.append(f"until {t.subscription_until:%Y-%m-%d %H:%M}")
    if body.plan is not None:
        t.plan = body.plan.strip() or t.plan
        changes.append(f"plan={t.plan}")
    if body.role is not None and body.role != t.role:
        if actor.role != "founder":
            raise HTTPException(403, "founder_only")
        if body.role not in ("client", "admin") or t.role == "founder":
            raise HTTPException(400, "invalid_role")
        t.role = body.role
        t.token_version += 1
        kick = kick or "role_changed"
        changes.append(f"role={body.role}")
    db.commit()
    if not t.subscription_active and t.role == "client":
        kick = kick or "subscription_inactive"
    if kick:
        await hub.kick(t.id, kick)
    if changes:
        audit(db, actor, "user_update", t, "; ".join(changes))
    return user_dict(t, full=True)


@app.post("/api/admin/users/{user_id}/reset-password")
async def admin_reset_password(user_id: int, actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    t = _target(db, actor, user_id)
    if t.role == "founder" and actor.id != t.id:
        raise HTTPException(403, "founder_protected")
    pw = temp_password()
    t.password_hash = hash_password(pw)
    t.token_version += 1
    db.commit()
    await hub.kick(t.id, "password_reset")
    audit(db, actor, "password_reset", t)
    return {"temp_password": pw}


@app.post("/api/admin/users/{user_id}/logout")
async def admin_force_logout(user_id: int, actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    t = _target(db, actor, user_id)
    t.token_version += 1
    db.commit()
    await hub.kick(t.id, "logged_out")
    audit(db, actor, "force_logout", t)
    return {"ok": True}


@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: int, request: Request, actor: User = Depends(staff_user),
                            db: Session = Depends(get_db)):
    t = _target(db, actor, user_id)
    if t.role == "founder" and actor.id != t.id:
        raise HTTPException(403, "founder_protected")      # only the founder can delete the founder account
    if t.id == actor.id and t.role != "founder":
        raise HTTPException(400, "cannot_delete_self")
    if t.role == "founder" and request.headers.get("x-confirm") != "DELETE-FOUNDER":
        raise HTTPException(400, "confirmation_required")
    email, was_founder = t.email, t.role == "founder"
    for row in db.scalars(select(ConnectorToken).where(ConnectorToken.user_id == t.id)):
        db.delete(row)
    for row in db.scalars(select(CoachMessage).where(CoachMessage.user_id == t.id)):
        db.delete(row)
    for row in db.scalars(select(UserTrade).where(UserTrade.user_id == t.id)):
        db.delete(row)
    for row in db.scalars(select(Burst).where(Burst.user_id == t.id)):
        db.delete(row)
    db.delete(t)
    db.commit()
    await hub.kick(user_id, "account_deleted")
    db.add(AuditLog(actor_id=actor.id if actor.id != user_id else None, actor_email=actor.email,
                    action="user_delete", target_id=user_id, target_email=email))
    db.commit()
    if was_founder:
        _ensure_setup_token()
    return {"ok": True}


def engine_status() -> dict:
    return {k: v for k, v in engine.status.items() if not k.startswith("_")} | {"model": AI_MODEL}


def available_strategies() -> list[str]:
    return sorted(p.stem for p in (ROOT / "strategies").glob("*.py") if p.stem not in ("__init__", "base"))


@app.get("/api/admin/engine")
def admin_engine(actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    return {"settings": get_app_settings(db), "status": engine_status(), "strategies": available_strategies(),
            "timeframes": list(TF_SECONDS), "timeframe_seconds": TF_SECONDS}


@app.put("/api/admin/engine")
async def admin_engine_put(body: dict, actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    cur = get_app_settings(db)
    new = {}
    if "symbols" in body:
        syms = [s.strip().upper() for s in body["symbols"] if re.fullmatch(r"[A-Za-z]{6}", s.strip())]
        if not syms or len(syms) > MAX_SYMBOLS:
            raise HTTPException(400, "invalid_symbols")
        new["symbols"] = list(dict.fromkeys(syms))
    if "timeframe" in body:
        if body["timeframe"] not in TF_SECONDS:
            raise HTTPException(400, "invalid_timeframe")
        new["timeframe"] = body["timeframe"]
    if "modes" in body:
        modes = cur["modes"]
        strategies = available_strategies()
        for name, m in body["modes"].items():
            if name not in modes:
                continue
            m2 = {**modes[name]}
            if "enabled" in m:
                m2["enabled"] = bool(m["enabled"])
            if name in ("trend", "custom"):
                if "timeframe" in m:         # "" = the engine's; a shorter one than the engine's is ignored
                    if m["timeframe"] not in ("", *TF_SECONDS):
                        raise HTTPException(400, "invalid_timeframe")
                    m2["timeframe"] = m["timeframe"]
                if "strategy" in m:
                    if m["strategy"] not in strategies:
                        raise HTTPException(400, "unknown_strategy")
                    m2["strategy"] = m["strategy"]
                if "params" in m:
                    if not isinstance(m["params"], dict) or not all(isinstance(v, (int, float, str, bool))
                                                                    for v in m["params"].values()):
                        raise HTTPException(400, "invalid_params")
                    if any(isinstance(v, float) and not math.isfinite(v) for v in m["params"].values()):
                        raise HTTPException(400, "invalid_number")
                    m2["params"] = m["params"]
            else:
                for key, (lo, hi, cast) in {"min_confidence": (50, 100, int), "sl_atr": (0.3, 10, float),
                                            "tp_atr": (0.3, 20, float), "expire_hours": (1, 72, int),
                                            "min_interval_minutes": (5, 1440, int)}.items():
                    if key in m:
                        m2[key] = clamp(m[key], lo, hi, cast)
            modes[name] = m2
        new["modes"] = modes
    for key, (lo, hi, cast) in {"event_guard_minutes": (0, 240, int), "news_poll_minutes": (1, 120, int),
                                "trial_days": (0, 60, int), "coach_daily_limit": (0, 1000, int)}.items():
        if key in body:
            new[key] = clamp(body[key], lo, hi, cast)
    if "signup_open" in body:
        new["signup_open"] = bool(body["signup_open"])
    if "rss_feeds" in body:
        feeds = [u.strip() for u in body["rss_feeds"] if re.fullmatch(r"https://\S+", u.strip())]
        new["rss_feeds"] = feeds[:10]
    if isinstance(body.get("burst"), dict):
        b = {**cur["burst"]}
        if "enabled" in body["burst"]:
            b["enabled"] = bool(body["burst"]["enabled"])
        if "max_trades" in body["burst"]:
            b["max_trades"] = clamp(body["burst"]["max_trades"], 1, 40, int)
        new["burst"] = b
    if isinstance(body.get("learning"), dict):
        lb, cur_l = body["learning"], {**cur["learning"]}
        if "mode" in lb:
            if lb["mode"] not in ("off", "suggest", "auto"):
                raise HTTPException(400, "invalid_learning_mode")
            cur_l["mode"] = lb["mode"]
        for key, (lo, hi, cast) in {"weekday": (0, 6, int), "hour_utc": (0, 23, int),
                                    "history_bars": (3000, 44000, int), "cost_pips": (0, 10, float)}.items():
            if key in lb:
                cur_l[key] = clamp(lb[key], lo, hi, cast)
        if "ai_lessons" in lb:
            cur_l["ai_lessons"] = bool(lb["ai_lessons"])
        new["learning"] = cur_l
    save_app_settings(db, new)
    audit(db, actor, "engine_settings", None, ", ".join(new))
    if new.get("symbols", cur["symbols"]) != cur["symbols"]:
        # the scalpers watch the engine's pairs, and each user's short-term and Rapid pairs stay within them
        msgs = {}
        for u in db.scalars(select(User).where(User.id.in_(list(hub.connectors)))):
            sent, syms = for_connector(u.merged_settings(), new["symbols"], _connector_info(u.id)["version"])
            msgs[u.id] = {"type": "settings", "settings": sent, "symbols": syms}
        await asyncio.gather(*(hub.to_connector(uid, m) for uid, m in msgs.items()))
    return {"settings": get_app_settings(db)}


@app.post("/api/admin/engine/run-ai")
async def admin_run_ai(actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    if not AI_ENABLED:
        raise HTTPException(409, "ai_not_configured")
    audit(db, actor, "ai_run_manual")
    return await engine.run_ai()


# ── self-learning (docs/SELF_LEARNING.md) ──────────────────────
_background: set[asyncio.Task] = set()


def _model_info(db: Session) -> dict | None:
    m = engine.model
    if not m:
        return None
    s = get_app_settings(db)
    on = {**s, "learning": {**s["learning"], "mode": "suggest"}}     # as if learning were on
    return {**learning.clean(m), "id": m["id"], "mode": m["mode"], "applied_at": iso(m.get("applied_at")),
            "applies": engine.model_for(m["mode"], s) is not None,
            "stale": engine.model_for(m["mode"], on) is None}       # learned for another strategy or timeframe


@app.get("/api/admin/learning")
def admin_learning(actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    runs = db.scalars(select(LearnRun).order_by(LearnRun.id.desc()).limit(30)).all()
    st = engine.learning
    return {"settings": get_app_settings(db)["learning"], "model": _model_info(db), "runs": [r.to_dict() for r in runs],
            "running": st["running"], "step": st["step"], "blocked": st["blocked"], "regret": st["regret"],
            "feed": engine.status["feed"],
            "limits": {"scales": [learning.SCALE_MIN, learning.SCALE_MAX], "auto_scales": list(learning.AUTO_SCALES),
                       "max_filters": learning.MAX_FILTERS, "max_blocked": learning.MAX_BLOCKED}}


@app.get("/api/admin/learning/runs/{run_id}")
def admin_learning_run(run_id: int, actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    row = db.get(LearnRun, run_id)
    if not row:
        raise HTTPException(404, "not_found")
    return row.to_dict(full=True)


@app.post("/api/admin/learning/run")
async def admin_learning_start(actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    if engine.learning["running"]:
        raise HTTPException(409, "already_running")
    if engine.feed is None:
        raise HTTPException(409, "no_price_feed")
    audit(db, actor, "learning_run")
    task = asyncio.create_task(engine.run_learning("manual"))      # a minute or two: the page polls for the result
    _background.add(task)
    task.add_done_callback(_background.discard)
    return {"started": True}


@app.post("/api/admin/learning/runs/{run_id}/apply")
async def admin_learning_apply(run_id: int, actor: User = Depends(staff_user)):
    if not await engine.activate(run_id, actor, "applied by an admin"):
        raise HTTPException(409, "not_applicable")
    return {"ok": True}


class RevertIn(BaseModel):
    to_base: bool = False


@app.post("/api/admin/learning/revert")
async def admin_learning_revert(body: RevertIn, actor: User = Depends(staff_user)):
    if not engine.model:
        raise HTTPException(409, "no_active_model")
    back = await engine.revert(actor, "reverted by an admin", to_base=body.to_base)
    return {"ok": True, "active": back}


@app.get("/api/admin/audit")
def admin_audit(limit: int = Query(200, le=1000), actor: User = Depends(staff_user), db: Session = Depends(get_db)):
    rows = db.scalars(select(AuditLog).order_by(AuditLog.time.desc()).limit(limit))
    return [{"time": iso(r.time), "actor": r.actor_email, "action": r.action, "target": r.target_email,
             "details": r.details} for r in rows]


# ── WebSockets ─────────────────────────────────────────────────
def _origin_ok(ws: WebSocket) -> bool:
    origin = ws.headers.get("origin")
    if not origin:
        return True                                  # non-browser clients
    host = ws.headers.get("host", "")
    return origin.split("://", 1)[-1] == host


@app.websocket("/ws")
async def ws_browser(ws: WebSocket):
    if not _origin_ok(ws):
        await ws.close(code=4403)
        return
    with SessionLocal() as db:
        user = user_from_token(db, ws.cookies.get(COOKIE))
    if not user or user.is_banned:
        await ws.close(code=4401)
        return
    if not user.subscription_active:
        await ws.close(code=4402)
        return
    await ws.accept()
    hub.add_browser(user.id, user.is_staff, ws)
    try:
        await ws.send_json({"type": "hello", "connector": hub.connector_state.get(user.id) or {"online": False},
                            "engine": {"feed": engine.status["feed"], "ai_enabled": AI_ENABLED}})
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        hub.remove_browser(user.id, ws)


@app.websocket("/ws/connector")
async def ws_connector(ws: WebSocket):
    await ws.accept()
    try:
        first = await asyncio.wait_for(ws.receive_json(), timeout=15)
    except Exception:
        await ws.close(code=4400)
        return
    token = str(first.get("token", ""))
    with SessionLocal() as db:
        rate_limit_ok = True
        try:
            rate_limit(f"connector:{ws.client.host if ws.client else '?'}", 30, 600)
        except HTTPException:
            rate_limit_ok = False
        user = user_from_connector_token(db, token) if rate_limit_ok and first.get("type") == "auth" else None
        token_row = db.scalar(select(ConnectorToken.id).where(ConnectorToken.token_hash == hash_token(token))) if user else None
    if not user:
        await ws.send_json({"type": "error", "reason": "invalid_token"})
        await ws.close(code=4401)
        return
    if user.is_banned or not user.subscription_active:
        await ws.send_json({"type": "error", "reason": "banned" if user.is_banned else "subscription_inactive"})
        await ws.close(code=4402)
        return
    await hub.set_connector(user.id, ws, first)
    hub.connector_state[user.id]["token_id"] = token_row
    version = hub.connector_state[user.id]["version"]
    with SessionLocal() as db:
        # a connector before v1.3.0 gets nothing on v1.3.0's pairs (engine.takes)
        open_sigs = [s.to_dict() for s in db.scalars(select(Signal).where(Signal.status == "open"))
                     if takes(user.id, s.symbol)]
        app_s, now = get_app_settings(db), utcnow()
        mine = db.scalars(select(Burst).where(Burst.user_id == user.id, Burst.status.not_in(FINAL))
                          .order_by(Burst.event_time)).all()
        on, me, live = bool(app_s["burst"]["enabled"]), db.get(User, user.id), engine.live_releases(
            db, {b.event_time for b in mine if not b.test})
        # a burst that was told to stop is not replayed: burst_tick sends its cancel again (see below). Nor one this
        # connector is too old for (a downgrade): burst_tick skips it while still planned
        replay = [b for b in mine if b.status in ("planned", "scheduled") and seconds_until(b, now) > 3
                  and not engine.burst_stop_reason(b, on, me, live)
                  and version_ok(version, needs_version(b.params, b.kind, b.symbol))]
    contexts = engine.burst_contexts(replay, now) if replay else {}
    plans = [plan_payload(b, now, contexts.get(b.id)) for b in replay]
    sent, syms = for_connector(user.merged_settings(), app_s["symbols"], version)
    await ws.send_json({"type": "welcome", "user": {"email": user.email, "name": user.name, "lang": user.lang},
                        "settings": sent, "symbols": syms,
                        "open_signals": open_sigs,
                        "bursts": plans, "server_time": iso(utcnow())})
    # a burst_cancel written into the old link (sleep, half-open socket) may never have arrived: send them again
    engine.burst_cancel_sent -= {b.id for b in mine}
    log.info("Connector online for user %s (v%s)", user.id, first.get("version"))
    await hub.to_user(user.id, {"type": "connector_log", "level": "info", "message": "Connector connected",
                                "time": iso(utcnow())})
    try:
        while True:
            msg = await ws.receive_json()
            kind = msg.get("type")
            if kind == "snapshot":
                await hub.update_connector(user.id, msg.get("account"), msg.get("positions"))
            elif kind == "trade_opened":
                await _record_trade_open(user.id, msg.get("trade") or {})
            elif kind == "trade_closed":
                await _record_trade_close(user.id, msg.get("trade") or {})
            elif kind == "burst_update":
                await _record_burst_update(user.id, msg)
            elif kind == "signal_skipped":
                await _record_skip(user.id, msg)
            elif kind == "log":
                await hub.to_user(user.id, {"type": "connector_log", "level": msg.get("level", "info"),
                                            "message": str(msg.get("message"))[:500], "time": iso(utcnow())})
            elif kind == "ping":
                await ws.send_json({"type": "pong"})
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        await hub.drop_connector(user.id, ws)
        log.info("Connector offline for user %s", user.id)


# Connector numbers: anything the database cannot store becomes None. JSON allows NaN, Infinity and huge ints,
# and a failing commit would end the connector's socket.
def _float(v) -> float | None:
    try:
        f = float(v) if v is not None else None
    except (TypeError, ValueError, OverflowError):
        return None
    return f if f is None or math.isfinite(f) else None


def _int(v) -> int | None:
    try:
        n = int(v) if v is not None else None
    except (TypeError, ValueError, OverflowError):
        return None
    return n if n is None or -2**63 <= n < 2**63 else None


def _parse_time(v) -> datetime:
    try:
        return datetime.fromisoformat(str(v).replace("Z", "")) if v else utcnow()
    except ValueError:
        return utcnow()


def _strategy(db: Session, t: dict, burst_id: int | None, signal_id: int | None) -> str:
    """A trade's strategy (dashboard.SCOPES, or test): a burst trade carries its burst (rapid for a Rapid news run,
    Burst.kind), a scalp or a Rapid signal run says so itself (connector ≥ 1.2.0, ≥ 1.3.0), a test signal's trade is
    test, a signal's trade follows the signal's mode (MODE_SCOPE). Anything else (a signal no longer stored) is long."""
    if burst_id:
        return RAPID if db.scalar(select(Burst.kind).where(Burst.id == burst_id)) == RAPID else "burst"
    if t.get("strategy") in ("short", RAPID):
        return t["strategy"]
    if signal_id is None:
        return "long"
    if signal_id >= TEST_SIGNAL_BASE:
        return "test"
    return MODE_SCOPE.get(db.scalar(select(Signal.mode).where(Signal.id == signal_id)), "long")


async def _record_trade_open(user_id: int, t: dict):
    ticket = _int(t.get("ticket"))
    if ticket is None:
        return
    with SessionLocal() as db:
        # look the burst and the signal up before adding the row: a query would autoflush the half-filled trade
        bid, sid = _int(t.get("burst_id")), _int(t.get("signal_id"))
        if bid and not db.scalar(select(Burst.id).where(Burst.id == bid, Burst.user_id == user_id)):
            bid = None
        strategy = _strategy(db, t, bid, sid)
        row = db.scalar(select(UserTrade).where(UserTrade.user_id == user_id, UserTrade.ticket == ticket))
        if not row:
            row = UserTrade(user_id=user_id, ticket=ticket)
            db.add(row)
        row.signal_id, row.burst_id, row.strategy = sid, bid, strategy
        row.risk, row.confidence = _float(t.get("risk")), _float(t.get("confidence"))
        row.symbol, row.side = str(t.get("symbol", ""))[:20], "buy" if t.get("side") == "buy" else "sell"
        row.lots, row.entry_price = _float(t.get("lots")) or 0.0, _float(t.get("entry_price")) or 0.0
        row.sl, row.tp = _float(t.get("sl")), _float(t.get("tp"))
        # a re-sent open (the connector's outbox sends the very same message again) never reopens a trade already
        # booked as closed; any other open on a known ticket is a new trade (paper mode numbers from 1001 each start).
        # Compared as stored (the database keeps no time zone)
        opened = _parse_time(t.get("opened_at"))
        if not (row.status == "closed" and row.opened_at == opened.replace(tzinfo=None)):
            row.status, row.closed_at, row.exit_price, row.pnl = "open", None, None, None
        row.opened_at, row.demo = opened, bool(t.get("demo", True))
        db.commit()
        payload = row.to_dict()
    await hub.to_user(user_id, {"type": "trade", "trade": payload})


async def _record_skip(user_id: int, m: dict):
    """Why the connector did not trade a signal: kept, so the Signals page can say it next to the signal."""
    sid, reason = _int(m.get("signal_id")), str(m.get("reason") or "")[:300]
    if sid is not None:
        with SessionLocal() as db:
            db.add(SignalSkip(user_id=user_id, signal_id=sid, reason=reason))
            db.commit()
    await hub.to_user(user_id, {"type": "signal_skip", "signal_id": sid, "reason": reason})
    await hub.to_user(user_id, {"type": "connector_log", "level": "info", "time": iso(utcnow()),
                                "message": f"Signal #{sid} skipped: {reason}"[:500]})


async def _record_trade_close(user_id: int, t: dict):
    ticket = _int(t.get("ticket"))
    if ticket is None:
        return
    with SessionLocal() as db:
        row = db.scalar(select(UserTrade).where(UserTrade.user_id == user_id, UserTrade.ticket == ticket))
        if not row:
            # a news-burst trade, a scalp or a Rapid run's trade whose trade_opened never came (its runner crashed
            # first, a restart): booked from what its close carries. The burst is looked up before the row is added
            # (a query would autoflush it)
            bid = _int(t.get("burst_id"))
            if bid and not db.scalar(select(Burst.id).where(Burst.id == bid, Burst.user_id == user_id)):
                bid = None
            if not bid and t.get("strategy") not in ("short", RAPID):
                return
            row = UserTrade(user_id=user_id, ticket=ticket, burst_id=bid, strategy=_strategy(db, t, bid, None),
                            risk=_float(t.get("risk")), confidence=_float(t.get("confidence")),
                            symbol=str(t.get("symbol", ""))[:20],
                            side="buy" if t.get("side") == "buy" else "sell", lots=_float(t.get("lots")) or 0.0,
                            entry_price=_float(t.get("entry_price")) or 0.0, opened_at=_parse_time(t.get("opened_at")),
                            demo=bool(t.get("demo", True)), status="closed")
            db.add(row)
        row.status, row.closed_at = "closed", _parse_time(t.get("closed_at"))
        row.exit_price, row.pnl = _float(t.get("exit_price")), _float(t.get("pnl"))
        burst = db.get(Burst, row.burst_id) if row.burst_id else None
        if burst and burst.status in FINAL:
            # closed after its burst ended (the connector's sweeper took it over): counted in the burst's result
            burst.pnl = burst_trades_pnl(db, user_id, burst.id)
        db.commit()
        payload = row.to_dict()
        late = burst.to_dict() if burst and burst.status in FINAL else None
    await hub.to_user(user_id, {"type": "trade", "trade": payload})
    if late:
        await hub.to_user(user_id, {"type": "burst", "burst": late})


def _analysis_ok(a) -> bool:
    """A run's pre-news analysis as the connector reported it can be kept: a JSON object (no NaN: the API's JSON
    could not send it back) of at most ANALYSIS_MAX characters."""
    if not isinstance(a, dict):
        return False
    try:
        return len(json.dumps(a, allow_nan=False)) <= ANALYSIS_MAX
    except (TypeError, ValueError, RecursionError):
        return False


async def _record_burst_update(user_id: int, m: dict):
    """A step reported by the connector's burst runner. Only the keys present (and not null) are applied.
    `analysis` (since v1.3.0): the look the run was placed on, kept once, even on a run the server already ended."""
    bid, status = _int(m.get("id")), m.get("status")
    if bid is None or (status is not None and status not in REPORTED):
        return
    with SessionLocal() as db:
        row = db.get(Burst, bid)
        if not row or row.user_id != user_id:            # only the run's own connector reports it
            return
        old = row.status
        kept = row.analysis is None and _analysis_ok(m.get("analysis"))
        if kept:
            row.analysis = m["analysis"]
        # the server's own timeout gives way to the runner's final report: it knows the side, fills and result
        late = old == "error" and row.reason == TIMED_OUT and status in FINAL
        if late:
            row.status, row.reason = status, None
        if old not in FINAL or late:
            if status and RANK[status] > RANK[old]:          # never step back (e.g. a late duplicate `scheduled`)
                row.status = status
            if m.get("side") in ("buy", "sell", "both"):
                row.side = m["side"]
            for key in ("orders", "fills"):
                if _int(m.get(key)) is not None:
                    setattr(row, key, max(0, _int(m[key])))
            pnl = _float(m.get("pnl"))
            if pnl is not None and math.isfinite(pnl):
                row.pnl = round(pnl, 2)
            if m.get("reason") is not None:
                row.reason = str(m["reason"])[:300]
            if row.status in FINAL:
                # the runner's own sum leaves out a trade its connector's sweeper reported while the burst still ran
                row.pnl = burst_trades_pnl(db, user_id, bid, row.pnl)
        db.commit()
        payload = row.to_dict()
    if old in FINAL and not late:
        # the server already gave up on this burst (cancelled, skipped, timed out): a runner that still
        # reports progress must stop and remove its orders
        if status in ("scheduled", "armed", "triggered"):
            await hub.to_connector(user_id, {"type": "burst_cancel", "id": bid})
        if kept:                 # a watch-only analysis can come after the run's end: the history still shows it
            await hub.to_user(user_id, {"type": "burst", "burst": payload})
        return
    if payload["status"] in FINAL:
        engine.burst_cancel_sent.discard(bid)
    await hub.to_user(user_id, {"type": "burst", "burst": payload})
    if payload["status"] != old and (payload["status"] in FINAL or payload["status"] in ("armed", "triggered")):
        await hub.to_user(user_id, {"type": "connector_log", "time": iso(utcnow()),
                                    "level": "warn" if payload["status"] in ("skipped", "error") else "info",
                                    "message": _burst_line(payload)})


def _burst_line(b: dict) -> str:
    name = "Rapid run" if b.get("kind") == "rapid" else "News burst"
    line = f"{name} #{b['id']} {b['symbol']} {b['status']}"
    if b["status"] == "armed":
        line += f": {b['orders']} orders placed"
    elif b["status"] == "triggered":
        line += f": {b['side']} side filled"
    elif b["status"] == "closed":
        line += f": {b['fills']} filled, P&L {b['pnl'] if b['pnl'] is not None else '?'}"
    return line + (f" ({b['reason']})" if b["reason"] else "")


# ── web app ────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"ok": True, "engine_started": engine.status["started"] is not None}


@app.get("/")
def index():
    return FileResponse(WEB_DIR / "index.html", headers={"Cache-Control": "no-cache"})


app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
