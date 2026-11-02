"""The signal engine: runs inside the server process as background tasks.

price loop    on each newly closed bar: track open signals (SL/TP/expiry), run the trend and custom
              strategies, publish new signals and exits
news loop     every few minutes: refresh the economic calendar and RSS headlines; when fresh headlines
              arrive, ask Claude for a structured bias per pair and publish confident ones as AI signals
housekeeping  every minute: disconnect users whose subscription ended or who were banned
news burst    every 2 s: plan straddles for clients' connectors before high-impact releases, deliver the
              plans and settle bursts the connector can no longer finish (docs/NEWS_BURST.md)
learning      every minute: the weekly self-learning run and the daily regret check, both in a child
              process (docs/SELF_LEARNING.md)

Same Strategy classes as the backtester, so a mode can be backtested before it is enabled.
"""
import asyncio
import json
import logging
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import and_, or_, select

from bot.indicators import atr
from strategies import PositionView, load_strategy

from . import ai, learning
from .burst import (BURST_SYMBOLS, FINAL, OUTDATED, RAPID, TIMED_OUT, USER_CANCEL, V130, burst_picks, candidates,
                    needs_version, new_pair, params_for, plan_payload, plan_runs, run_seconds, seconds_until, stands,
                    version_ok, wants)
from .db import (AIAnalysis, AuditLog, Burst, CalendarEvent, LearnRun, NewsItem, SessionLocal, Signal, User,
                 burst_trades_pnl, get_app_settings, get_state, save_app_settings, set_state, utcnow)
from .feeds.calendar import fetch_forexfactory_week, fetch_investing
from .feeds.news import fetch_rss
from .feeds.prices import TF_SECONDS, make_feed, pip_size, quiet_until
from .hub import hub
from .news_view import context_for
from .notify import send_telegram, signal_text, telegram_enabled
from .settings import (AI_ENABLED, APIFY_TOKEN, CALENDAR_SOURCE, DEMO_SECONDS_PER_BAR, INVESTING_ACTOR,
                       INVESTING_COUNTRIES, INVESTING_FULL_HOURS, INVESTING_TODAY_MINUTES, MT5_SERVER_TZ,
                       MT5_SYMBOL_SUFFIX, MT5_UTC_OFFSET_HOURS, PRICE_FEED, TWELVEDATA_API_KEY)

log = logging.getLogger("engine")
REGRET_EVERY = 24 * 3600        # seconds between regret checks of the active self-learning model
MIN_DWELL_DAYS = 28             # auto mode leaves a model in place at least this long
BLACKLIST_DAYS = 90             # a model reverted by the regret check is not proposed again for this long
LEARN_RETRY = 3600              # seconds before a weekly run that could not run or failed is tried again
LEARN_TRIES = 3                 # failed weekly runs stored per week; then it waits for the next scheduled time
INVESTING_STALE = 6 * 3600      # Investing.com failing and nothing from it for this long: ForexFactory takes over
INVESTING_RETRY = 900           # seconds before a failed Investing.com run is tried again
MODE_LABEL = {"trend": "Trend", "custom": "Custom rules", "news_ai": "AI news"}
# Signal.comment from a strategy -> (English, Romanian) explanation shown to clients.
REASONS = {
    "ema up": ("Fast EMA crossed above the slow EMA (uptrend starting)",
               "EMA rapidă a trecut peste EMA lentă (începe un trend ascendent)"),
    "ema down": ("Fast EMA crossed below the slow EMA (downtrend starting)",
                 "EMA rapidă a trecut sub EMA lentă (începe un trend descendent)"),
}


def takes(user_id: int, symbol: str) -> bool:
    """The user's connector can trade `symbol`: one before v1.3.0 gets nothing on a pair added in v1.3.0 (gold's pip
    was wrong there, nothing was scaled). Its exits (signal_closed) still go to every connector."""
    return not new_pair(symbol) or version_ok(hub.connector_state.get(user_id, {}).get("version"), V130)


def mode_timeframe(settings: dict, mode: str) -> str:
    """The bars a strategy mode decides on: its own timeframe when that is longer than the engine's (long-term
    trend on D1 while the engine reads H1), else the engine's."""
    tf, own = settings["timeframe"], settings["modes"].get(mode, {}).get("timeframe")
    return own if own in TF_SECONDS and tf in TF_SECONDS and TF_SECONDS[own] > TF_SECONDS[tf] else tf


class Engine:
    def __init__(self):
        self.feed = None
        self.tasks: list[asyncio.Task] = []
        self.candles: dict[str, pd.DataFrame] = {}
        self.last_bar: dict[str, pd.Timestamp] = {}
        self.mode_bar: dict[str, pd.Timestamp] = {}        # "mode|symbol|timeframe" -> newest bar decided on (saved)
        self.next_due: dict[str, float] = {}
        self.started_at = utcnow()
        self.strategies: dict[str, tuple[str, object]] = {}
        self.last_calendar_fetch = 0.0
        # Investing.com runs (refresh_calendar): when each kind is due next, last success, whether the last run failed
        self.cal = {"loaded": False, "warned": False, "due": {"week": 0.0, "actuals": 0.0, "today": 0.0},
                    "ok": 0.0, "failing": False}
        self.last_ai_ts = 0.0
        self.ai_lock = asyncio.Lock()
        self.burst_cancel_sent: set[int] = set()          # bursts told to stop: burst_cancel goes out only once
        self.failing: dict[str, str] = {}                  # symbol -> why its last price tick failed (warned once)
        self.timeframe: str | None = None
        self.model: dict | None = None                     # active self-learning model {"id", "mode", ...}
        # the first regret check runs an hour after start, then daily
        self.learning = {"running": False, "step": "", "last_regret_check": time.time() - REGRET_EVERY + 3600,
                         "blocked": [], "regret": None, "retry_at": 0.0}
        self._learn_pool: ProcessPoolExecutor | None = None
        self.status = {"feed": PRICE_FEED, "ai_enabled": AI_ENABLED, "started": None, "last_bar": {},
                       "last_news_run": None, "last_ai_run": None, "last_ai_error": None, "errors": []}

    # ── lifecycle ──────────────────────────────────────────────
    async def start(self):
        try:
            self.feed = await asyncio.to_thread(make_feed, PRICE_FEED, DEMO_SECONDS_PER_BAR, TWELVEDATA_API_KEY,
                                                MT5_SYMBOL_SUFFIX, MT5_UTC_OFFSET_HOURS, MT5_SERVER_TZ)
        except Exception as e:
            self._error(f"Price feed '{PRICE_FEED}' failed to start: {e}")
            return
        self.started_at = utcnow()
        self.status["started"] = self.started_at.isoformat() + "Z"
        self.load_mode_bars()
        self.tasks = [asyncio.create_task(self._loop(self.price_tick, 5), name="prices"),
                      asyncio.create_task(self._loop(self.news_tick, 60, first_delay=3), name="news"),
                      asyncio.create_task(self._loop(self.housekeeping, 60, first_delay=30), name="housekeeping"),
                      asyncio.create_task(self._loop(self.burst_tick, 2, first_delay=5), name="burst"),
                      asyncio.create_task(self._loop(self.learn_tick, 60, first_delay=20), name="learning")]
        self.load_model()
        log.info("Engine started with %s feed", self.feed.name)

    async def stop(self):
        for t in self.tasks:
            t.cancel()
        if self._learn_pool is not None:
            self._learn_pool.shutdown(wait=False, cancel_futures=True)

    async def _loop(self, fn, every: float, first_delay: float = 0):
        await asyncio.sleep(first_delay)
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("engine task failed")
                self._error(f"{fn.__name__}: {type(e).__name__}: {e}")
            await asyncio.sleep(every)

    def _error(self, msg: str, level: int = logging.ERROR):
        log.log(level, msg)
        self.status["errors"] = ([{"time": utcnow().isoformat() + "Z", "message": msg}] + self.status["errors"])[:20]

    # ── prices & strategies ────────────────────────────────────
    def _strategy(self, mode: str, cfg: dict):
        key = json.dumps([cfg.get("strategy"), cfg.get("params")], sort_keys=True)
        cached = self.strategies.get(mode)
        if not cached or cached[0] != key:
            cached = (key, load_strategy(cfg["strategy"], cfg.get("params") or None))
            self.strategies[mode] = cached
        return cached[1]

    def _bars_for(self, mode: str, cfg: dict) -> int:
        """Closed bars a strategy needs to decide on the newest one (never fewer than the features need)."""
        return max(learning.LIVE_BARS, int(self._strategy(mode, cfg).warmup_bars) + 50)

    def _live_bars(self, settings: dict) -> int:
        """Bars fetched per symbol on each price tick: enough for every enabled strategy on the engine's timeframe
        (one on its own timeframe reads its bars itself), within the feed's reach."""
        n = learning.LIVE_BARS
        for mode in ("trend", "custom"):
            cfg = settings["modes"].get(mode, {})
            if cfg.get("enabled") and mode_timeframe(settings, mode) == settings["timeframe"]:
                try:
                    n = max(n, self._bars_for(mode, cfg))
                except Exception:          # a strategy that does not load is reported by _run_strategies
                    pass
        return min(n, getattr(self.feed, "max_history", 44000))

    async def price_tick(self):
        with SessionLocal() as db:
            settings = get_app_settings(db)
        tf = settings["timeframe"]
        if tf != self.timeframe:          # bars of another timeframe: start over (no signals on the first look);
            if self.timeframe is not None:      # mode_bar stays: its keys carry the mode's own timeframe
                log.info("Timeframe changed %s -> %s", self.timeframe, tf)
            self.timeframe = tf
            self.last_bar.clear()
            self.next_due.clear()
            self.candles.clear()
        self.feed.timeframe = tf          # the demo feed's quote follows the engine's bars
        bar_secs = self.feed.bar_seconds(tf)
        bars = self._live_bars(settings)
        failed: dict[str, Exception] = {}                  # symbol -> why its bars failed to load on this tick
        for symbol in settings["symbols"]:
            if time.time() < self.next_due.get(symbol, 0):
                continue
            # a pair whose bars fail to load (GBPJPY before the terminal has them) never stops the rest; an error after
            # the fetch (outcomes, strategies) still ends the tick and is reported by _loop, as before
            try:
                df = await asyncio.to_thread(self.feed.candles, symbol, tf, bars)
            except Exception as e:
                failed[symbol] = e
                continue
            self.failing.pop(symbol, None)
            if df.empty:
                continue
            last = df["time"].iat[-1]
            prev = self.last_bar.get(symbol)
            if prev is not None and last == prev:
                # nothing new yet. `last` is the open time of the newest closed bar, so the next bar
                # closes at last + 2 bars; live feeds are not polled again before then (saves API credits)
                if self.feed.name == "demo":
                    self.next_due[symbol] = time.time() + 2
                else:
                    next_close = (last + pd.Timedelta(seconds=2 * bar_secs)).timestamp() + 3
                    self.next_due[symbol] = max(time.time() + 30, next_close)
                continue
            self.candles[symbol] = df
            self.last_bar[symbol] = last
            self.status["last_bar"][symbol] = str(last)
            # on the first look every bar counts: signals left open by a restart are checked over the bars
            # that closed while the server was down (each from its own track_from)
            new_bars = df[df["time"] > prev] if prev is not None else df
            await self._track_outcomes(symbol, new_bars, bar_secs)
            await self._run_strategies(symbol, df, settings, bar_secs, first=prev is None)
            await hub.to_browsers({"type": "price", "symbol": symbol, "time": str(last),
                                   "close": float(df["close"].iat[-1])})
        if failed:
            self._pairs_failed(settings["symbols"], failed)

    def _pairs_failed(self, symbols: list[str], failed: dict):
        """Pairs whose bars failed to load on this price tick (`failed`: symbol -> error). While another pair still
        loads, each is warned about once (again when its error changes), not every 5 s. When every pair failed, the
        feed is down: the first error is raised, for _loop to report at ERROR on every tick, as up to v1.2.0."""
        if set(symbols) <= failed.keys():
            raise next(iter(failed.values()))
        for symbol, e in failed.items():
            why = f"price tick {symbol}: {type(e).__name__}: {e}"
            if self.failing.get(symbol) != why:
                self._error(why, logging.WARNING)
            self.failing[symbol] = why

    @staticmethod
    def _guard_events(db, symbol: str, lo: datetime, hi: datetime) -> list[CalendarEvent]:
        """High-impact releases of either currency of `symbol` between lo and hi: what the event guard avoids."""
        return db.scalars(select(CalendarEvent).where(
            CalendarEvent.impact == "High",
            CalendarEvent.currency.in_([symbol[:3], symbol[3:6]]),
            CalendarEvent.time.between(lo, hi)).order_by(CalendarEvent.time)).all()

    def _event_guard(self, symbol: str, minutes: int) -> str | None:
        if not minutes:
            return None
        now, pad = utcnow(), timedelta(minutes=minutes)
        with SessionLocal() as db:
            ev = next(iter(self._guard_events(db, symbol, now - pad, now + pad)), None)
        return f"{ev.currency} {ev.title} at {ev.time:%H:%M} UTC" if ev else None

    def _guard_windows(self, symbols: list[str], minutes: int) -> dict[str, list[tuple]]:
        """The event guard for the replay: per symbol, the (start, end) windows around the stored releases in which
        the live engine opens nothing. History older than the stored calendar stays unguarded."""
        if not minutes:
            return {}
        pad = timedelta(minutes=minutes)
        with SessionLocal() as db:
            return {s: [(e.time - pad, e.time + pad) for e in self._guard_events(db, s, datetime.min, utcnow() + pad)]
                    for s in symbols}

    def load_mode_bars(self):
        """The bar each mode on its own timeframe decided on last, as earlier runs saved it (_mode_bars)."""
        with SessionLocal() as db:
            self.mode_bar = {k: pd.Timestamp(v) for k, v in (get_state(db, "mode_bars") or {}).items()}

    def _decided(self, key: str, bar: pd.Timestamp):
        """Remember `bar` as decided on, saved first: no run decides it again."""
        bars = {**self.mode_bar, key: bar}
        with SessionLocal() as db:
            set_state(db, "mode_bars", {k: v.isoformat() for k, v in bars.items()})
        self.mode_bar = bars

    async def _mode_bars(self, mode: str, symbol: str, tf: str, cfg: dict, engine_df: pd.DataFrame,
                         bar_secs: float) -> pd.DataFrame | None:
        """The bars of a mode on its own (longer) timeframe when one closed that no run has decided on yet, else None.
        Asked at each close of the engine's bars (`engine_df`). A D1 bar closes at the daily rollover, whose spreads
        are wide: its decision is due 3 h later (Friday's on Monday) and made at the first close from then on, a late
        one too (a restart, a PC that was off at broker 03:00). The bar is saved as decided before the strategy runs
        (engine_state mode_bars): a restart never decides it again, a crash meanwhile loses at most that bar. One that
        stayed the newest for more of the engine's closes than a bar of the mode lasts (a newer one should be there:
        stale prices), or was due before the engine's oldest bar, is saved as decided and skipped with a warning."""
        live = self.feed.name != "demo"            # synthetic prices have no rollover
        now = utcnow()
        if live and quiet_until(now):
            return None
        key, tf_secs = f"{mode}|{symbol}|{tf}", self.feed.bar_seconds(tf)
        prev = self.mode_bar.get(key)
        if live and prev is not None and now < prev + pd.Timedelta(seconds=2 * tf_secs):
            return None                            # no newer bar can have closed yet: not fetched (API credits)
        df = await asyncio.to_thread(self.feed.candles, symbol, tf,
                                     min(self._bars_for(mode, cfg), getattr(self.feed, "max_history", 44000)))
        if df.empty:
            return None
        last = df["time"].iat[-1]
        if prev is not None and last <= prev:
            return None
        close = (last + pd.Timedelta(seconds=tf_secs)).to_pydatetime()
        due = (quiet_until(close) if live else None) or close
        closes = engine_df["time"] + pd.Timedelta(seconds=bar_secs)       # the engine's chances to decide
        if closes.iat[-1] < due:
            return None                    # a look at an earlier close: not yet, asked again at the next one
        chances = [c for c in closes[closes >= due] if not (live and quiet_until(c.to_pydatetime()))]
        self._decided(key, last)
        if closes.iat[0] > due or len(chances) * bar_secs > tf_secs:
            log.warning("%s %s: the newest %s bar (%s, due %s) is not decided on: a newer one should be there "
                        "(stale prices?)", mode, symbol, tf, last, due)
            return None
        return df

    async def _run_strategies(self, symbol: str, df: pd.DataFrame, settings: dict, bar_secs: float,
                              first: bool = False):
        """Each enabled strategy mode decides on the newest closed bar: of `df` (the engine's timeframe) or of its own
        longer timeframe (_mode_bars). Entries and exits go out at the price now, the close of `df`'s newest bar.
        `first`: the first look after a start; an earlier run may have acted on that bar already.
        The mode's position is its open signal on the timeframe it decides on. One left open on another timeframe
        (from before an admin moved the mode) keeps its stop and target and is closed when the mode enters anew."""
        price = float(df["close"].iat[-1])
        track_from = (df["time"].iat[-1] + pd.Timedelta(seconds=bar_secs)).to_pydatetime()
        for mode in ("trend", "custom"):
            cfg = settings["modes"].get(mode, {})
            if not cfg.get("enabled"):
                continue
            tf = mode_timeframe(settings, mode)
            if tf == settings["timeframe"]:
                if first:          # never act on bars that closed before the engine started
                    continue
                bars, tf_secs = df, bar_secs
            else:
                try:
                    bars = await self._mode_bars(mode, symbol, tf, cfg, df, bar_secs)
                except Exception as e:     # a failing fetch: the next mode and symbol still run
                    self._error(f"{mode} {symbol} {tf} bars: {type(e).__name__}: {e}")
                    continue
                if bars is None:
                    continue
                tf_secs = self.feed.bar_seconds(tf)
            strat = self._strategy(mode, cfg)
            prepared = strat.prepare(bars.copy())
            i = len(prepared) - 1
            if i < strat.warmup_bars:
                continue
            close = float(prepared["close"].iat[i])
            with SessionLocal() as db:
                open_sig = db.scalar(select(Signal).where(Signal.mode == mode, Signal.symbol == symbol,
                                                          Signal.timeframe == tf, Signal.status == "open"))
            if open_sig:
                # the bars after the one it was decided on, as the replay counts them (older signals: from track_from)
                bar_time = (open_sig.meta or {}).get("bar_time")
                bars_held = int((prepared["time"] > pd.Timestamp(bar_time)).sum() if bar_time else
                                (prepared["time"] >= (open_sig.track_from or open_sig.created_at)).sum())
                view = PositionView(symbol, open_sig.side, open_sig.price, open_sig.sl, open_sig.tp,
                                    open_sig.created_at, bars_held)
                if not strat.exit_signal(prepared, i, view):
                    continue
                # closed by its exit rule, the bar may open the next trade (a crossover's exit is the opposite
                # crossover's entry), as the replay and the research trade it
                await self._close_signal(open_sig.id, price, "closed", "exit rule")
            sig = strat.entry_signal(prepared, i)
            if sig is None:
                continue
            guard = self._event_guard(symbol, int(settings.get("event_guard_minutes") or 0))
            if guard:
                log.info("%s %s %s skipped: high-impact event %s", mode, sig.side, symbol, guard)
                continue
            feats = learning.features(learning.add_context(prepared), i, sig.side, tf_secs)
            sl, tp = float(sig.sl), float(sig.tp) if sig.tp else None
            model = self.model_for(mode, settings)
            meta = {"feed": self.feed.name, "model": model["id"] if model else None, "features": feats,
                    "bar_time": pd.Timestamp(prepared["time"].iat[i]).isoformat()}
            if model:
                rule = learning.blocked_by(model, symbol, feats)
                if rule:
                    why = learning.filter_text(rule)
                    log.info("%s %s %s skipped by self-learning model #%s: %s", mode, sig.side, symbol, model["id"], why)
                    self.learning["blocked"] = ([{"time": utcnow().isoformat() + "Z", "symbol": symbol, "side": sig.side,
                                                  "model": model["id"], "rule": why,
                                                  "rule_ro": learning.filter_text(rule, "ro")}]
                                                + self.learning["blocked"])[:20]
                    continue
                sl, tp = learning.scaled_levels(model, sig.side, close, sl, tp)
                meta.update(sl_scale=model["sl_scale"], tp_scale=model["tp_scale"])
            # the levels keep their distance from the bar's close, around the price now (the same on the engine's
            # timeframe; hours later for a D1 bar)
            sl, tp = sl + price - close, (tp + price - close if tp is not None else None)
            pip = pip_size(symbol)
            sl_pips = abs(price - sl) / pip
            tp_pips = abs(tp - price) / pip if tp else None
            rr = f"1:{tp_pips / sl_pips:.1f}" if tp_pips and sl_pips else "—"
            why_en, why_ro = REASONS.get(sig.comment or "", (f"{strat.name}: {sig.comment or strat.name}",) * 2)
            rationale = (f"{why_en}. Stop-loss {sl_pips:.0f} pips"
                         + (f", take-profit {tp_pips:.0f} pips (risk:reward {rr})." if tp_pips else "."))
            rationale_ro = (f"{why_ro}. Stop-loss {sl_pips:.0f} pips"
                            + (f", take-profit {tp_pips:.0f} pips (risc:câștig {rr.replace('.', ',')})." if tp_pips else "."))
            if model and (model["sl_scale"], model["tp_scale"]) != (1.0, 1.0):
                rationale += f" Self-learning #{model['id']}: stop ×{model['sl_scale']:g}, target ×{model['tp_scale']:g}."
                rationale_ro += f" Auto-învățare #{model['id']}: stop ×{model['sl_scale']:g}, țintă ×{model['tp_scale']:g}."
            with SessionLocal() as db:     # left open on another timeframe: closed first (one position per symbol)
                left = db.scalars(select(Signal.id).where(Signal.mode == mode, Signal.symbol == symbol,
                                                          Signal.timeframe != tf, Signal.status == "open")).all()
            for sid in left:
                await self._close_signal(sid, price, "closed", "timeframe changed")
            await self._publish(Signal(mode=mode, symbol=symbol, side=sig.side, timeframe=tf, price=price, sl=sl, tp=tp,
                                       rationale=rationale, rationale_ro=rationale_ro, track_from=track_from,
                                       meta=meta))

    async def _track_outcomes(self, symbol: str, bars: pd.DataFrame, bar_secs: float):
        if bars.empty:
            return
        with SessionLocal() as db:
            open_sigs = db.scalars(select(Signal).where(Signal.symbol == symbol, Signal.status == "open")).all()
        for s in open_sigs:
            start = s.track_from or s.created_at
            for _, b in bars[bars["time"] >= pd.Timestamp(start)].iterrows():
                hit = learning.walk_bar(s.side, s.sl, s.tp, b["high"], b["low"], b["open"])   # the replay's rule too
                if hit:
                    await self._close_signal(s.id, hit[0], None, hit[1])
                    break
                bar_end = b["time"] + pd.Timedelta(seconds=bar_secs)
                if s.expires_at and bar_end >= pd.Timestamp(s.expires_at):
                    await self._close_signal(s.id, float(b["close"]), "expired", "expired")
                    break

    async def _close_signal(self, signal_id: int, price: float, status: str | None, reason: str):
        with SessionLocal() as db:
            s = db.get(Signal, signal_id)
            if not s or s.status != "open":
                return
            pips = (price - s.price) / pip_size(s.symbol) * (1 if s.side == "buy" else -1)
            s.status = status or ("won" if pips > 0 else "lost")
            s.close_price, s.closed_at, s.result_pips, s.close_reason = price, utcnow(), round(pips, 1), reason
            db.commit()
            payload = s.to_dict()
            users = self._eligible_users(db, s)
        msg = {"type": "signal_closed", "signal": payload}
        await hub.to_browsers(msg, {u.id for u in users})
        await hub.to_connectors(msg, {u.id for u in users})

    async def _publish(self, signal: Signal):
        with SessionLocal() as db:
            db.add(signal)
            db.commit()
            payload = signal.to_dict()
            users = self._eligible_users(db, signal)
            tg = [(u.telegram_chat_id, u.lang) for u in users
                  if u.telegram_chat_id and u.merged_settings().get("notify_telegram")
                  and self._wants(u.merged_settings(), payload)]
        log.info("Signal #%s %s %s %s @ %s", payload["id"], payload["mode"], payload["side"], payload["symbol"], payload["price"])
        ids = {u.id for u in users}
        await hub.to_browsers({"type": "signal", "signal": payload}, ids)
        await hub.to_connectors({"type": "signal", "signal": payload}, {i for i in ids if takes(i, payload["symbol"])})
        if tg and telegram_enabled():
            await asyncio.gather(*(asyncio.to_thread(send_telegram, chat, signal_text(payload, lang)) for chat, lang in tg))

    @staticmethod
    def _wants(settings: dict, sig: dict) -> bool:
        if not settings["modes"].get(sig["mode"]):
            return False
        if settings.get("symbols") and sig["symbol"] not in settings["symbols"]:
            return False
        if sig["mode"] == "news_ai" and (sig.get("confidence") or 0) < settings.get("min_confidence", 0):
            return False
        return True

    @staticmethod
    def _eligible_users(db, _signal) -> list[User]:
        """Users allowed to receive signals: staff, and clients with an active subscription."""
        return [u for u in db.scalars(select(User).where(User.is_banned.is_(False))) if u.subscription_active]

    # ── news + AI ──────────────────────────────────────────────
    async def news_tick(self):
        with SessionLocal() as db:
            settings = get_app_settings(db)
        last = self.status.get("_news_ts", 0)
        if time.time() - last < max(1, int(settings.get("news_poll_minutes", 5))) * 60 - 5:
            return
        self.status["_news_ts"] = time.time()
        self.status["last_news_run"] = utcnow().isoformat() + "Z"

        try:
            await self.refresh_calendar()
        except Exception as e:                      # never let the calendar stop the headlines and the AI
            self._error(f"calendar refresh failed: {e}")

        fresh = []
        for url in settings.get("rss_feeds", []):
            try:
                items = await asyncio.to_thread(fetch_rss, url)
            except Exception as e:
                self._error(f"RSS {url}: {type(e).__name__}")
                continue
            with SessionLocal() as db:
                for it in items:
                    if not db.get(NewsItem, it["id"]):
                        db.add(NewsItem(**it))
                        fresh.append(it)
                db.commit()
        if fresh:
            await hub.to_browsers({"type": "news_updated", "count": len(fresh)})

        mode_cfg = settings["modes"].get("news_ai", {})
        gap = max(5, int(mode_cfg.get("min_interval_minutes", 30))) * 60     # each run is a paid API call
        if AI_ENABLED and mode_cfg.get("enabled") and time.time() - self.last_ai_ts >= gap:
            with SessionLocal() as db:
                pending = db.scalars(select(NewsItem).where(NewsItem.analyzed.is_(False),
                                                            NewsItem.published_at > utcnow() - timedelta(hours=12))
                                     .order_by(NewsItem.published_at.desc()).limit(40)).all()
            if pending:
                await self.run_ai(settings)

    # ── economic calendar ──────────────────────────────────────
    async def refresh_calendar(self, now: float | None = None):
        """Keep the calendar table current; the news loop calls it, so at most every news_poll_minutes.
        forexfactory (default): the week, every 30 minutes.
        investing (CALENDAR_SOURCE=investing + APIFY_TOKEN): Apify bills every row, so one run per call at most, the
        first one due of: the next 7 days, High + Medium, every INVESTING_FULL_HOURS; today's High releases a few
        minutes after one printed and still shows no actual value; today's High releases every INVESTING_TODAY_MINUTES
        (the times bursts and the guard go by). While Investing.com fails and nothing came from it for INVESTING_STALE
        (or ever), the ForexFactory week is kept fresh instead: bursts and the guard never run without a calendar."""
        now = time.time() if now is None else now
        if CALENDAR_SOURCE == "investing":
            if not APIFY_TOKEN:
                if not self.cal["warned"]:
                    self.cal["warned"] = True
                    self._error("CALENDAR_SOURCE=investing needs APIFY_TOKEN in .env: "
                                "the calendar stays on ForexFactory")
            else:
                if not self.cal["loaded"]:
                    self._load_calendar_state(now)
                job = self._investing_job(now)
                if job:
                    await self._run_investing(now, *job)
                if not self.cal["failing"] or (self.cal["ok"] and now - self.cal["ok"] < INVESTING_STALE):
                    return
        if now - self.last_calendar_fetch > 1800:
            try:
                self.save_calendar(await asyncio.to_thread(fetch_forexfactory_week), replaces=("investing",))
                self.last_calendar_fetch = now
                self.status["calendar"] = {"source": "forexfactory", "updated": utcnow().isoformat() + "Z"}
                await hub.to_browsers({"type": "calendar_updated"})
            except Exception as e:
                self._error(f"calendar fetch failed: {e}")

    def _load_calendar_state(self, now: float):
        """After a restart: the last 7-day Investing.com run stands while it is recent and its rows are still there."""
        self.cal["loaded"] = True
        with SessionLocal() as db:
            at = get_state(db, "calendar_investing_week")
            kept = db.scalar(select(CalendarEvent.id).where(CalendarEvent.source == "investing",
                                                            CalendarEvent.time > utcnow()).limit(1))
        if isinstance(at, (int, float)) and kept and 0 <= now - at < INVESTING_FULL_HOURS * 3600:
            self.cal["due"]["week"] = at + INVESTING_FULL_HOURS * 3600
            self.cal["ok"] = at

    def _investing_job(self, now: float) -> tuple | None:
        """The Investing.com run due now: (kind, first UTC day, days, importance asked, impacts covered) or None."""
        due, t = self.cal["due"], utcnow()
        if now >= due["week"]:
            return "week", t.date(), 7, "high_medium", ("High", "Medium")
        if now >= due["actuals"]:
            with SessionLocal() as db:
                released = db.scalar(select(CalendarEvent).where(
                    CalendarEvent.source == "investing", CalendarEvent.impact == "High", CalendarEvent.actual == "",
                    or_(CalendarEvent.forecast != "", CalendarEvent.previous != ""),     # a figure is due, not a talk
                    CalendarEvent.time.between(t - timedelta(minutes=30), t - timedelta(minutes=2)))
                    .order_by(CalendarEvent.time).limit(1))
            if released:
                return "actuals", released.time.date(), 1, "high", ("High",)
        if now >= due["today"]:
            return "today", t.date(), 1, "high", ("High",)
        return None

    async def _run_investing(self, now: float, kind: str, start, days: int, importance: str, impacts: tuple):
        try:
            events = await asyncio.to_thread(fetch_investing, APIFY_TOKEN, INVESTING_COUNTRIES, start, days,
                                             importance, INVESTING_ACTOR)
            if kind == "week" and not events:
                raise RuntimeError("no releases at all for the next 7 days")
        except Exception as e:
            self.cal["due"][kind] = now + INVESTING_RETRY
            self.cal["failing"] = True
            self._error(f"calendar (Investing.com, {kind} run) failed: {e}")
            return
        lo = datetime.combine(start, datetime.min.time())
        self.save_calendar(events, window=(lo, lo + timedelta(days=days)), impacts=impacts, replaces=("forexfactory",))
        due = self.cal["due"]
        if kind == "week":
            due["week"] = now + INVESTING_FULL_HOURS * 3600
            with SessionLocal() as db:
                set_state(db, "calendar_investing_week", now)
        due["today"], due["actuals"] = now + INVESTING_TODAY_MINUTES * 60, now + 240
        self.cal.update(ok=now, failing=False)
        self.status["calendar"] = {"source": "investing", "updated": utcnow().isoformat() + "Z", "run": kind,
                                   "releases": len(events)}
        log.info("calendar: Investing.com %s run, %d releases", kind, len(events))
        await hub.to_browsers({"type": "calendar_updated"})

    @staticmethod
    def save_calendar(events: list[dict], window: tuple[datetime, datetime] | None = None,
                      impacts: tuple[str, ...] | None = None, replaces: tuple[str, ...] = ()):
        """Store a fetched calendar (rows of one source). A release still to come that the fetch covered but did not
        return is deleted, and a burst planned on it is then cancelled as 'event rescheduled' (ForexFactory ids include
        the release time, so a moved release comes back under a new id). Covered: rows of the same source inside
        `window` [start, end) (default: first to last fetched release, both included) and, when given, of one of
        `impacts` (a fetch of High releases only leaves the Medium ones alone). Upcoming rows of the `replaces` sources
        in that window go too: the source the engine switched from. Past rows stay (the guard's history), a released
        figure is never blanked by a later answer, and an empty fetch changes nothing."""
        if not events:
            return
        with SessionLocal() as db:
            for e in events:
                row = db.get(CalendarEvent, e["id"])
                if row:
                    for k in ("time", "impact", "forecast", "previous", "actual"):
                        if k != "actual" or e[k]:
                            setattr(row, k, e[k])
                    row.updated_at = utcnow()
                else:
                    db.add(CalendarEvent(**e))
            db.flush()
            if window:
                inside = (CalendarEvent.time >= window[0], CalendarEvent.time < window[1])
            else:
                inside = (CalendarEvent.time.between(min(e["time"] for e in events), max(e["time"] for e in events)),)
            same = CalendarEvent.source == events[0]["source"]
            if impacts:
                same = and_(same, CalendarEvent.impact.in_(impacts))
            covered = or_(same, CalendarEvent.source.in_(replaces)) if replaces else same
            gone = db.scalars(select(CalendarEvent).where(
                CalendarEvent.time > utcnow(), *inside, covered,
                CalendarEvent.id.not_in([e["id"] for e in events]))).all()
            for row in gone:
                db.delete(row)
            db.commit()

    def _price_summary(self, symbols: list[str]) -> dict:
        out = {}
        for s in symbols:
            df = self.candles.get(s)
            if df is None or len(df) < 30:
                continue
            pip = pip_size(s)
            a = atr(df, 14).iat[-1]
            back = df[df["time"] <= df["time"].iat[-1] - pd.Timedelta(hours=24)]
            ref = back["close"].iat[-1] if not back.empty else df["close"].iat[0]
            out[s] = {"last": float(df["close"].iat[-1]), "change_24h_pips": float((df["close"].iat[-1] - ref) / pip),
                      "atr_pips": float(a / pip), "atr": float(a)}
        return out

    async def run_ai(self, settings: dict | None = None) -> dict:
        """Analyse pending headlines with Claude and publish confident biases as signals."""
        if self.ai_lock.locked():
            return {"error": "An analysis is already running"}
        async with self.ai_lock:
            self.last_ai_ts = time.time()
            return await self._run_ai(settings)

    async def _run_ai(self, settings: dict | None) -> dict:
        with SessionLocal() as db:
            settings = settings or get_app_settings(db)
            now = utcnow()
            events = [dict(time=e.time, currency=e.currency, impact=e.impact, title=e.title, actual=e.actual,
                           forecast=e.forecast, previous=e.previous)
                      for e in db.scalars(select(CalendarEvent).where(
                          CalendarEvent.impact.in_(["High", "Medium"]),
                          CalendarEvent.time.between(now - timedelta(hours=24), now + timedelta(hours=24)))
                          .order_by(CalendarEvent.time))]
            pending = db.scalars(select(NewsItem).where(NewsItem.analyzed.is_(False),
                                                        NewsItem.published_at > now - timedelta(hours=12))
                                 .order_by(NewsItem.published_at.desc()).limit(40)).all()
            recent = db.scalars(select(NewsItem).where(NewsItem.analyzed.is_(True),
                                                       NewsItem.published_at > now - timedelta(hours=12))
                                .order_by(NewsItem.published_at.desc()).limit(30)).all()
            as_dict = lambda n: {"id": n.id, "source": n.source, "title": n.title, "summary": n.summary,
                                 "published_at": n.published_at}
            fresh_items, recent_items = [as_dict(n) for n in pending], [as_dict(n) for n in recent]
        symbols = settings["symbols"]
        prices = self._price_summary(symbols)
        self.status["last_ai_run"] = utcnow().isoformat() + "Z"
        try:
            # approved lessons belong to this mode: used whatever the trend fingerprint, never while learning is off
            lcfg = settings["learning"]
            lessons = (self.model or {}).get("lessons") if lcfg.get("ai_lessons") and lcfg.get("mode") != "off" else None
            result, usage = await asyncio.to_thread(ai.analyze_market, now, symbols, prices, events,
                                                    fresh_items, recent_items, lessons)
        except ai.AIError as e:
            self.status["last_ai_error"] = str(e)
            self._error(f"AI analysis: {e}")
            return {"error": str(e)}
        self.status["last_ai_error"] = None
        data = result.model_dump()
        tags = {t["id"]: t for t in data["headlines"]}
        with SessionLocal() as db:
            db.add(AIAnalysis(kind="news", model=usage["model"], result=data,
                              input_tokens=usage["input_tokens"], output_tokens=usage["output_tokens"]))
            for item in fresh_items:
                row = db.get(NewsItem, item["id"])
                row.analyzed = True
                tag = tags.get(item["id"][:12]) or tags.get(item["id"])
                if tag:
                    row.ai = {k: tag[k] for k in ("effects", "impact", "note")}
            db.commit()
        await hub.to_browsers({"type": "analysis", "summary_en": data["summary_en"], "summary_ro": data["summary_ro"]})

        cfg = settings["modes"]["news_ai"]
        floor = int(cfg.get("min_confidence", 70))
        for b in data["biases"]:
            sym = b["symbol"].upper().replace("/", "")
            if (b["direction"] == "neutral" or b["confidence"] < floor
                    or sym not in symbols or sym not in prices):
                continue
            with SessionLocal() as db:
                if db.scalar(select(Signal).where(Signal.mode == "news_ai", Signal.symbol == sym, Signal.status == "open")):
                    continue
            guard = self._event_guard(sym, int(settings.get("event_guard_minutes") or 0))
            if guard:
                log.info("AI %s %s skipped: high-impact event %s", b["direction"], sym, guard)
                continue
            p = prices[sym]
            # the live price when the feed has one: the last bar's close can be up to a bar old, and connectors
            # skip signals whose price is more than max_price_drift_pips away from theirs
            price, a = await asyncio.to_thread(self.feed.quote, sym) or p["last"], p["atr"]
            sign = 1 if b["direction"] == "buy" else -1
            hours = min(max(int(b["horizon_hours"] or 12), 1), int(cfg.get("expire_hours", 24)))
            bar_secs = self.feed.bar_seconds(settings["timeframe"])
            last_bar = self.last_bar.get(sym)
            await self._publish(Signal(
                mode="news_ai", symbol=sym, side=b["direction"], timeframe=settings["timeframe"], price=price,
                sl=price - sign * float(cfg.get("sl_atr", 1.5)) * a, tp=price + sign * float(cfg.get("tp_atr", 2.5)) * a,
                confidence=int(b["confidence"]), rationale=b["rationale_en"], rationale_ro=b["rationale_ro"],
                expires_at=utcnow() + timedelta(hours=hours),
                track_from=(last_bar + pd.Timedelta(seconds=bar_secs)).to_pydatetime() if last_bar is not None else utcnow(),
                meta={"feed": self.feed.name, "model": (self.model or {}).get("id") if lessons else None,
                      "lessons": len(lessons or [])}))
        return {"ok": True, "biases": len(data["biases"]), "headlines": len(fresh_items)}

    # ── self-learning (docs/SELF_LEARNING.md) ──────────────────
    def load_model(self):
        with SessionLocal() as db:
            row = db.scalar(select(LearnRun).where(LearnRun.active.is_(True)).order_by(LearnRun.id.desc()))
            self.model = ({**learning.clean(row.model), "id": row.id, "mode": row.mode,
                           "fingerprint": row.model.get("fingerprint"), "applied_at": row.applied_at}
                          if row and row.model else None)

    def model_for(self, mode: str, settings: dict) -> dict | None:
        """The active model when it was learned for this mode's current strategy, parameters and timeframe.
        None while learning is off: the strategy then runs exactly as written."""
        m, cfg = self.model, settings["modes"].get(mode, {})
        if not m or m["mode"] != mode or settings["learning"].get("mode") == "off":
            return None
        if m.get("fingerprint") not in learning.fingerprints(cfg.get("strategy"), cfg.get("params"),
                                                             mode_timeframe(settings, mode)):
            return None
        return m

    def _defers(self, settings: dict, mode: str) -> bool:
        """Whether the mode's decisions wait out the rollover's wide spreads (_mode_bars): what the replay models."""
        return self.feed.name != "demo" and mode_timeframe(settings, mode) != settings["timeframe"]

    async def _offload(self, fn, *args):
        """CPU-heavy learning work runs in a child process, so the event loop (bursts are timed to the second)
        never waits on it. FX_LEARN_IN_THREAD=1 (tests) keeps it in a thread."""
        if os.environ.get("FX_LEARN_IN_THREAD"):
            return await asyncio.to_thread(fn, *args)
        if self._learn_pool is None:
            self._learn_pool = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"),
                                                   initializer=learning.die_with_parent)
        try:
            return await asyncio.get_running_loop().run_in_executor(self._learn_pool, fn, *args)
        except BrokenProcessPool:
            self._learn_pool = None          # the child died (out of memory?): a new one next time
            raise

    async def _history(self, symbols: list[str], tf: str, bars: int) -> dict:
        """Each symbol's bars, those long enough to learn on. A symbol whose bars fail to load (GBPJPY before the
        terminal has its history) is left out with a warning; when every one fails, the feed is down: the first
        error is raised."""
        data, errors = {}, []
        for sym in symbols:
            self.learning["step"] = f"loading {sym}"
            try:
                df = await asyncio.to_thread(self.feed.candles, sym, tf, bars)
            except Exception as e:
                log.warning("Self-learning: %s left out, its bars failed to load: %s: %s", sym, type(e).__name__, e)
                errors.append(e)
                continue
            if len(df) >= learning.CTX_BARS + 50:
                data[sym] = df
        if errors and len(errors) == len(symbols):
            raise errors[0]
        return data

    @staticmethod
    def _blacklist(db) -> list[dict]:
        """Changes of models the regret check reverted in the last 90 days: not proposed again meanwhile."""
        out = []
        for row in db.scalars(select(LearnRun).where(LearnRun.status == "reverted",
                                                     LearnRun.end_reason.like("regret%"),
                                                     LearnRun.ended_at > utcnow() - timedelta(days=BLACKLIST_DAYS))):
            m = learning.clean(row.model)
            if (m["sl_scale"], m["tp_scale"]) != (1.0, 1.0):
                out.append({"kind": "scale", "sl_scale": round(m["sl_scale"], 2), "tp_scale": round(m["tp_scale"], 2)})
            out += [{"kind": "filter", "dim": x["dim"], "value": x["value"]} for x in m["filters"]]
        return out

    @staticmethod
    def _learn_due(cfg: dict, now: datetime) -> datetime:
        """The latest scheduled time (weekday, hour_utc) at or before now: the weekly run's week starts there."""
        due = (now - timedelta(days=(now.weekday() - int(cfg.get("weekday", 5))) % 7)).replace(
            hour=int(cfg.get("hour_utc", 6)), minute=0, second=0, microsecond=0)
        return due if due <= now else due - timedelta(days=7)

    @staticmethod
    def _ident(m: dict | None) -> tuple | None:
        """Which activation a model dict is: a reverted-to model comes back with a new applied_at."""
        return (m["id"], m["applied_at"]) if m else None

    async def learn_tick(self):
        with SessionLocal() as db:
            settings = get_app_settings(db)
            cfg = settings["learning"]
            tries = db.execute(select(LearnRun.decision, LearnRun.created_at).where(
                LearnRun.trigger == "scheduled", LearnRun.created_at >= self._learn_due(cfg, utcnow()))
                .order_by(LearnRun.id)).all()
        if cfg.get("mode", "off") == "off" or self.learning["running"]:
            return
        if self.model and time.time() - self.learning["last_regret_check"] >= REGRET_EVERY:
            self.learning["last_regret_check"] = time.time()
            await self.regret_check(settings)
        # one run per scheduled week. One that could not run (MT5 cannot date bars while the market is closed) or
        # failed is tried again hourly; a 'running' row here was cut short by a restart
        failed = [t for d, t in tries if d in ("error", "running")]
        if (len(failed) == len(tries) < LEARN_TRIES and time.time() >= self.learning["retry_at"]
                and (not failed or utcnow() - failed[-1] >= timedelta(seconds=LEARN_RETRY))):
            await self.run_learning("scheduled")

    async def run_learning(self, trigger: str) -> dict:
        """Replay, diagnose, search and gate (in a child process), then store the run. In auto mode a scheduled
        run whose proposal passed every check becomes the active model. Never more than one run at a time."""
        if self.learning["running"]:
            return {"error": "already_running"}
        if self.feed is None:
            return {"error": "no_price_feed"}
        self.learning.update(running=True, step="starting")
        run_id = None
        try:
            with SessionLocal() as db:
                settings = get_app_settings(db)
            cfg, mode_cfg, tf = settings["learning"], settings["modes"]["trend"], mode_timeframe(settings, "trend")
            cost = float(cfg.get("cost_pips", 1.0))
            bars = min(getattr(self.feed, "max_history", 44000),
                       max(3000, min(44000, int(cfg.get("history_bars", 30000)))))
            held = self.model                    # the active model as the run starts, whatever it was learned for
            if trigger == "scheduled":           # no row is stored until the history is in: without this a feed
                self.learning["retry_at"] = time.time() + LEARN_RETRY       # error would retry every minute
            data = await self._history(settings["symbols"], tf, bars)
            if trigger == "scheduled" and not any(len(df) >= learning.MIN_HISTORY for df in data.values()):
                log.warning("Weekly self-learning run: not enough price history yet, trying again in an hour")
                return {"error": "no_history"}
            with SessionLocal() as db:
                row = LearnRun(trigger=trigger, mode="trend")
                db.add(row)
                db.commit()
                run_id = row.id
                blacklist = self._blacklist(db)
            fp = learning.fingerprint(mode_cfg["strategy"], mode_cfg.get("params"), tf)
            fps = learning.fingerprints(mode_cfg["strategy"], mode_cfg.get("params"), tf)
            active = held if held and held.get("fingerprint") in fps else None
            guard = self._guard_windows(list(data), int(settings.get("event_guard_minutes") or 0))
            self.learning["step"] = "analysing the history"
            report, proposal = await self._offload(
                learning.learn_job, data, mode_cfg["strategy"], mode_cfg.get("params"), learning.clean(active),
                blacklist, cost, self.feed.bar_seconds(tf), guard, self._defers(settings, "trend"))
            self.learning["step"] = "AI signals"
            news = await self._learn_news(settings, held, cost)
            report["news"] = news["info"]
            proposal.update(lessons=news["lessons"], lesson_ids=news["lesson_ids"], fingerprint=fp,
                            strategy=mode_cfg["strategy"], timeframe=tf)
            report["blacklist"] = blacklist
            decision = report["gate"]["decision"]
            lessons_changed = news["lessons"] != learning.clean(held)["lessons"]
            status = "proposed" if decision == "improved" or lessons_changed else "none"
            with SessionLocal() as db:            # an admin may have changed the mode, applied or reverted meanwhile
                cfg = get_app_settings(db)["learning"]
            why_not = self._why_not_auto(trigger, cfg, decision, lessons_changed, proposal, active,
                                         self._ident(self.model) != self._ident(held))
            report["auto"] = {"applied": status == "proposed" and not why_not, "why_not": why_not}
            if self.feed.name == "demo":
                report["warning"] = "synthetic demo prices: a model learned from them is never applied automatically"
            with SessionLocal() as db:
                row = db.get(LearnRun, run_id)
                row.report, row.model, row.decision, row.status, row.finished_at = report, proposal, decision, status, utcnow()
                db.commit()
            log.info("Self-learning run #%s (%s): %s", run_id, trigger, decision)
            if report["auto"]["applied"]:
                await self.activate(run_id, None, "auto mode: passed every check")
            return {"id": run_id, "decision": decision, "status": status}
        except Exception as e:
            log.exception("self-learning run failed")
            self._error(f"self-learning: {type(e).__name__}: {e}")
            with SessionLocal() as db:
                row = db.get(LearnRun, run_id) if run_id is not None else None
                if row is None and trigger != "scheduled":    # a run started by hand that failed reading prices:
                    row = LearnRun(trigger=trigger, mode="trend")   # its admin sees why on the page
                    db.add(row)
                if row is not None:
                    row.decision, row.finished_at, row.report = "error", utcnow(), {"error": f"{type(e).__name__}: {e}"}
                    db.commit()
                    run_id = row.id
            return {"id": run_id, "error": str(e)}
        finally:
            self.learning.update(running=False, step="")

    def _why_not_auto(self, trigger, cfg, decision, lessons_changed, proposal, active, changed=False) -> str | None:
        """Why auto mode leaves this run for an admin (None: it applies it). `changed`: the active model is not
        the one the run started from (an admin's apply or revert wins)."""
        if cfg.get("mode") != "auto":
            return "suggest mode: an admin applies proposals"
        if trigger != "scheduled":
            return "runs started by hand are never applied automatically"
        if decision != "improved" and not lessons_changed:
            return "nothing to apply"
        if lessons_changed:
            return "new AI lessons need an admin's approval"
        if self.feed.name == "demo":
            return "learned from synthetic demo prices"
        if not learning.auto_ok(proposal):
            return f"stop/target scale outside {learning.AUTO_SCALES[0]}-{learning.AUTO_SCALES[1]}: needs an admin"
        if changed:
            return "the active model changed during the run"
        if active and active.get("applied_at") and utcnow() - active["applied_at"] < timedelta(days=MIN_DWELL_DAYS):
            return f"the active model has been in use less than {MIN_DWELL_DAYS} days"
        return None

    async def _learn_news(self, settings: dict, held: dict | None, cost: float) -> dict:
        """AI news signals: results by confidence band (a suggestion for the admin, never applied by itself) and,
        when enabled, lessons Claude draws from its own losing calls (applied only by an admin). `held`: the
        active model, whatever the trend strategy it was learned for: its approved lessons are carried forward."""
        base_min = int(settings["modes"]["news_ai"].get("min_confidence", 70))
        with SessionLocal() as db:
            rows = db.scalars(select(Signal).where(Signal.mode == "news_ai", Signal.status != "open",
                                                   Signal.created_at > utcnow() - timedelta(days=120))).all()
            live = [s.to_dict() for s in rows if (s.meta or {}).get("feed") not in (None, "demo")]
        closed = [(s["confidence"] or 0, r) for s in live
                  if (r := learning.signal_r(s, cost, pip_size(s["symbol"]))) is not None]
        prev = held or {}
        with SessionLocal() as db:                 # ids of the losses earlier lessons were drawn from
            row = db.get(LearnRun, prev["id"]) if prev.get("id") else None
            lesson_ids = list(((row.model or {}) if row else {}).get("lesson_ids") or [])
        out = {"lessons": list(prev.get("lessons") or []), "lesson_ids": lesson_ids,
               "info": learning.confidence_bands(closed, base_min)}
        if not settings["learning"].get("ai_lessons"):
            return out
        seen = set(lesson_ids)
        losses = [s for s in live if (s["result_pips"] or 0) < 0 and s["id"] not in seen][-10:]
        if len(losses) >= 3 and AI_ENABLED:
            try:
                new = await asyncio.to_thread(ai.post_mortem, losses)
                out["lessons"] = (new + out["lessons"])[:6]
                out["lesson_ids"] = sorted(seen | {s["id"] for s in losses})[-500:]
                out["info"]["new_lessons"] = new
            except ai.AIError as e:
                out["info"]["lessons_error"] = str(e)
        return out

    async def activate(self, run_id: int, actor: User | None, reason: str) -> bool:
        with SessionLocal() as db:
            row = db.get(LearnRun, run_id)
            if not row or row.model is None or row.status not in ("proposed", "superseded"):
                return False
            prev = db.scalar(select(LearnRun).where(LearnRun.active.is_(True), LearnRun.id != run_id))
            if prev:
                prev.active, prev.status, prev.ended_at, prev.end_reason = False, "superseded", utcnow(), f"replaced by #{run_id}"
            row.active, row.status, row.applied_at, row.ended_at, row.end_reason = True, "applied", utcnow(), None, ""
            row.model = {**row.model, "prev_id": prev.id if prev else None}     # a new dict: JSON columns need it
            db.add(AuditLog(actor_id=actor.id if actor else None, actor_email=actor.email if actor else "system",
                            action="learning_apply", details=f"#{run_id}: {reason}"[:500]))
            db.commit()
        self.load_model()
        log.info("Self-learning model #%s active (%s)", run_id, reason)
        return True

    async def revert(self, actor: User | None, reason: str, to_base: bool = False,
                     keep_lessons: bool = False) -> int | None:
        """Deactivate the active model and bring back the one it replaced (to_base: none, i.e. the plain
        strategy; keep_lessons: the plain strategy plus the model's approved AI lessons, as a new row).
        Returns the id now active."""
        with SessionLocal() as db:
            row = db.scalar(select(LearnRun).where(LearnRun.active.is_(True)))
            if not row:
                return None
            now, m = utcnow(), row.model or {}
            row.active, row.status, row.ended_at, row.end_reason = False, "reverted", now, reason[:300]
            prev = None if to_base else db.get(LearnRun, m.get("prev_id") or 0)
            back = None
            if prev and prev.status == "superseded" and prev.model is not None:
                # in use again from now: the dwell time and the regret check's weeks start over
                prev.active, prev.status, prev.applied_at, prev.ended_at, prev.end_reason = True, "applied", now, None, ""
                back = prev.id
            elif to_base and keep_lessons and m.get("lessons"):
                keep = LearnRun(trigger="regret", mode=row.mode, decision="no_change", status="applied", active=True,
                                applied_at=now, finished_at=now, report={},
                                model={"sl_scale": 1.0, "tp_scale": 1.0, "filters": [], "lessons": list(m["lessons"]),
                                       "lesson_ids": list(m.get("lesson_ids") or []), "prev_id": None,
                                       **{k: m.get(k) for k in ("fingerprint", "strategy", "timeframe")}})
                db.add(keep)
                db.flush()
                back = keep.id
            db.add(AuditLog(actor_id=actor.id if actor else None, actor_email=actor.email if actor else "system",
                            action="learning_revert",
                            details=f"#{row.id} -> {'#' + str(back) if back else 'no model'}: {reason}"[:500]))
            db.commit()
        self.load_model()
        log.warning("Self-learning model reverted (%s); active now: %s", reason, back or "none")
        return back

    async def regret_check(self, settings: dict):
        """Replay the active model and the plain strategy over the completed weeks since it was applied; when the
        model is clearly worse, go back to the plain strategy, switch learning to suggest mode and blacklist its
        changes. Its approved AI lessons stay: only the trend corrections were judged."""
        model = self.model
        if not model or not model.get("applied_at") or self.feed is None or self.feed.name == "demo":
            return
        if not self.model_for(model["mode"], settings):
            return                                # learned for another setup: not in use, nothing to judge
        mode_cfg, tf = settings["modes"][model["mode"]], mode_timeframe(settings, model["mode"])
        cost = float(settings["learning"].get("cost_pips", 1.0))
        bar_secs = self.feed.bar_seconds(tf)
        since = model["applied_at"]
        bars = min(getattr(self.feed, "max_history", 44000), int((utcnow() - since).total_seconds() / bar_secs)
                   + self._bars_for(model["mode"], mode_cfg) + 200)
        with SessionLocal() as db:           # closed ones only: the replay never reports a position still open
            live = [{"symbol": s.symbol, "side": s.side,
                     "bar_time": (s.meta or {}).get("bar_time")       # older signals: the bar before track_from
                     or (pd.Timestamp(s.track_from) - pd.Timedelta(seconds=bar_secs)).isoformat()}
                    for s in db.scalars(select(Signal).where(Signal.mode == model["mode"], Signal.created_at >= since,
                                                             Signal.status != "open"))
                    if (s.meta or {}).get("model") == model["id"] and s.track_from]
        data = await self._history(settings["symbols"], tf, bars)
        self.learning["step"] = ""
        guard = self._guard_windows(list(data), int(settings.get("event_guard_minutes") or 0))
        res = await self._offload(learning.regret_job, data, mode_cfg["strategy"], mode_cfg.get("params"),
                                  learning.clean(model), since, cost, bar_secs, live, guard,
                                  self._defers(settings, model["mode"]))
        if self._ident(self.model) != self._ident(model):
            log.info("Regret check of model #%s dropped: the active model changed meanwhile", model["id"])
            return
        self.learning["regret"] = {**res, "model": model["id"], "checked_at": utcnow().isoformat() + "Z"}
        fid = res.get("fidelity")
        if fid and fid["live"] >= 10 and fid["share"] < 0.8:
            self._error(f"self-learning: only {fid['matched']} of {fid['live']} live signals of model #{model['id']} "
                        f"match the replay; the learner's view of the strategy may be off")
        if res.get("revert"):        # still the judged model: nothing awaited since the check above
            await self.revert(None, f"regret check: {res['why']}", to_base=True, keep_lessons=True)
            with SessionLocal() as db:
                cfg = get_app_settings(db)["learning"]
                if cfg.get("mode") == "auto":
                    save_app_settings(db, {"learning": {**cfg, "mode": "suggest"}})

    # ── housekeeping ───────────────────────────────────────────
    async def housekeeping(self):
        ids = hub.connected_user_ids()
        if not ids:
            return
        with SessionLocal() as db:
            for u in db.scalars(select(User).where(User.id.in_(ids))):
                if u.is_banned:
                    await hub.kick(u.id, "banned")
                elif not u.subscription_active:
                    await hub.kick(u.id, "subscription_inactive")
            missing = ids - {u.id for u in db.scalars(select(User).where(User.id.in_(ids)))}
        for uid in missing:
            await hub.kick(uid, "account_deleted")

    # ── news burst ─────────────────────────────────────────────
    async def burst_tick(self):
        """Reconcile every unfinished burst, plan bursts for releases in the next 15 minutes, deliver plans.
        The connector runs the burst itself and reports each step back (burst_update)."""
        now = utcnow()
        changed, new, deliver, cancels = [], [], [], []
        with SessionLocal() as db:
            app = get_app_settings(db)
            on = bool(app["burst"].get("enabled"))
            unfinished = db.scalars(select(Burst).where(Burst.status.not_in(FINAL))).all()
            # one query for their users and one for the calendar, not one per burst every 2 s
            owners = {u.id: u for u in db.scalars(select(User).where(User.id.in_({b.user_id for b in unfinished})))}
            live = self.live_releases(db, {b.event_time for b in unfinished if not b.test})
            for b in unfinished:
                p, left = b.params or {}, seconds_until(b, now)
                why = self.burst_stop_reason(b, on, owners.get(b.user_id), live)
                if b.status == "planned":
                    if why or left <= 0:
                        b.status, b.reason = ("cancelled", why) if why else ("skipped", "connector was offline")
                        changed.append(b)
                        if why:          # the plan may already be on its way to the connector
                            cancels.append(b)
                elif -left > run_seconds(p, b.kind) + 300:          # its orders' life + the hold up to Y
                    b.status, b.reason = "error", TIMED_OUT
                    b.pnl = burst_trades_pnl(db, b.user_id, b.id, b.pnl)     # what its connector reported closed
                    changed.append(b)
                elif why and b.id not in self.burst_cancel_sent:
                    if b.reason != why:
                        b.reason = why
                        changed.append(b)
                    cancels.append(b)    # the connector holds the orders: it reports the final state

            online = set(hub.connectors)
            if on and online:
                events = db.scalars(select(CalendarEvent).where(
                    CalendarEvent.impact == "High", CalendarEvent.time > now,
                    CalendarEvent.time <= now + timedelta(minutes=15)).order_by(CalendarEvent.time)).all()
                cands = candidates(events, app)
                users = db.scalars(select(User).where(User.id.in_(online), User.is_banned.is_(False))).all() if cands else []
                have = {tuple(r) for r in db.execute(
                    select(Burst.user_id, Burst.symbol, Burst.event_time, Burst.currency, Burst.kind).where(
                        Burst.event_time.in_({c["time"] for c in cands})))} if users else set()
                for u in users:
                    s = u.merged_settings()
                    if not u.subscription_active or not (wants(s, "burst") or wants(s, RAPID)):
                        continue
                    version = hub.connector_state.get(u.id, {}).get("version")
                    mine = {(sym, t, cur, kind) for uid, sym, t, cur, kind in have if uid == u.id}
                    # Rapid's runs first, then one classic run per release on a pair Rapid left; never on `mine`
                    for c, kind in plan_runs(s, cands, app["symbols"], mine, wants(s, "burst")):
                        row = Burst(user_id=u.id, event_id=c["event_id"], event_title=c["title"][:200],
                                    currency=c["currency"], symbol=c["symbol"], event_time=c["time"], kind=kind,
                                    params=params_for(s, app["burst"]["max_trades"], kind))
                        # a connector too old for what the run uses (burst.needs_version) would run it some other way
                        if not version_ok(version, needs_version(row.params, row.kind, row.symbol)):
                            row.status, row.reason = "skipped", OUTDATED
                        db.add(row)
                        new.append(row)

            # resent every tick until the connector confirms with `scheduled` (it ignores duplicates)
            for b in db.scalars(select(Burst).where(Burst.status == "planned")).all():
                if b.user_id not in hub.connectors or seconds_until(b, now) <= 3:
                    continue
                if not version_ok(hub.connector_state.get(b.user_id, {}).get("version"),
                                  needs_version(b.params, b.kind, b.symbol)):    # e.g. connector downgraded since
                    b.status, b.reason = "skipped", OUTDATED
                    changed.append(b)
                    continue
                deliver.append(b)
            db.commit()
            pushes = [(b.user_id, b.to_dict()) for b in dict.fromkeys(changed + new)]
            self.burst_cancel_sent &= set(db.scalars(select(Burst.id).where(Burst.status.not_in(FINAL))))
        contexts = self.burst_contexts(deliver, now) if deliver else {}
        for b in cancels:
            await self.send_burst_cancel(b.user_id, b.id)
        for b in deliver:
            await hub.to_connector(b.user_id, {"type": "burst", "burst": plan_payload(b, utcnow(), contexts.get(b.id))})
        for uid, payload in pushes:
            await hub.to_user(uid, {"type": "burst", "burst": payload})

    @staticmethod
    def live_releases(db, times) -> dict:
        """{time: the currencies of the qualifying releases the calendar still holds then}. Which of them keep a
        burst is burst.stands(): a collapsed burst stands while any of its releases does; a moved one leaves its old
        time empty."""
        evs = db.scalars(select(CalendarEvent).where(CalendarEvent.impact == "High",
                                                     CalendarEvent.time.in_(times))).all() if times else []
        live: dict = {}
        for c in candidates(evs, {"symbols": BURST_SYMBOLS}):
            live.setdefault(c["time"], set()).update(c["currencies"])
        return live

    @staticmethod
    def burst_contexts(bursts, now: datetime) -> dict:
        """{burst id: the pre-news `context` its plan carries (server/news_view.py context_for)}, worked out once per
        (release time, pair): the High releases the calendar holds at that time that land on the pair (as
        burst.candidates collapses them; their forecast against previous) and the newest AI analysis. A test run reads
        the event it was started for, if any, else the AI part only. Never raises: a run whose context cannot be made
        goes without one (the connector then trades as before v1.3.0's analysis)."""
        out, cache = {}, {}
        try:
            with SessionLocal() as db:
                latest = db.scalars(select(AIAnalysis).where(AIAnalysis.kind == "news")
                                    .order_by(AIAnalysis.created_at.desc(), AIAnalysis.id.desc()).limit(1)).first()
                for b in bursts:
                    key = ("test", b.event_id, b.symbol) if b.test else (b.event_time, b.symbol)
                    try:
                        if key not in cache:
                            cache[key] = None                    # a failure is not tried again for its twins
                            if b.test:
                                evs = [e for e in (db.get(CalendarEvent, b.event_id) if b.event_id else None,) if e]
                            else:
                                rows = db.scalars(select(CalendarEvent).where(CalendarEvent.impact == "High",
                                                                              CalendarEvent.time == b.event_time)).all()
                                ids = {e[1] for c in candidates(rows, {"symbols": [b.symbol]}) for e in c["events"]}
                                evs = [e for e in rows if e.id in ids]
                            cache[key] = context_for(evs, b.symbol, latest, now)
                    except Exception:
                        log.exception("burst #%s: no pre-news context", b.id)
                    if cache.get(key) is not None:
                        out[b.id] = cache[key]
        except Exception:
            log.exception("burst plans: no pre-news context")
        return out

    @staticmethod
    def burst_stop_reason(b: Burst, on: bool, u: User | None, live: dict) -> str | None:
        """Why an unfinished burst must not go ahead, or None. `u` is its owner, `live` from live_releases(). The
        admin's switch stops both kinds; a Rapid news run (kind "rapid") follows the user's Rapid switches, and stands
        on a pair Rapid may take (rapid.symbols; none picked: any)."""
        if b.reason == USER_CANCEL:  # stays a stop reason until the connector confirms, even across a reconnect
            return USER_CANCEL
        if not on:
            return "news burst disabled by admin"
        if not u or u.is_banned or not u.subscription_active:
            return "account inactive"
        if b.test:               # an explicit one-off: runs even while the user's burst switch is off
            return None
        s = u.merged_settings()
        if b.kind == RAPID:
            if not wants(s, RAPID):
                return "Rapid news runs switched off"
            picks = s["rapid"]["symbols"] or BURST_SYMBOLS
        else:
            if not s["burst"].get("enabled"):
                return "news burst switched off"
            picks = burst_picks(s["burst"])
        if b.status in ("planned", "scheduled") and not stands(b.symbol, b.currency, live.get(b.event_time, ()),
                                                                picks):
            return "event rescheduled"
        return None

    async def send_burst_cancel(self, user_id: int, burst_id: int) -> bool:
        ok = await hub.to_connector(user_id, {"type": "burst_cancel", "id": burst_id})
        if ok:
            self.burst_cancel_sent.add(burst_id)
        return ok


engine = Engine()
