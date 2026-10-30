"""Price feeds for the signal engine. Every feed returns CLOSED bars only (the forming bar is dropped).

  demo        synthetic random-walk prices, for development and demos ONLY
  mt5         MetaTrader 5 terminal on the same (Windows) server
  twelvedata  REST API (works on a Linux VPS); the free tier is enough for a few H1 pairs
"""
import logging
import os
import threading
import time
import zlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

from bot.symbols import pip_size   # used here; engine, main and learning also import it from here

log = logging.getLogger("feed")
TF_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400}
BASE_PRICES = {"EURUSD": 1.085, "GBPUSD": 1.27, "USDJPY": 149.5, "AUDUSD": 0.66, "USDCAD": 1.37,
               "USDCHF": 0.88, "NZDUSD": 0.60, "EURJPY": 162.0, "GBPJPY": 190.0, "EURGBP": 0.855, "XAUUSD": 2650.0}
# "New York close" server clock (most MT5 FX brokers): New York time + 7h, so UTC+3 in the US summer, UTC+2 in winter
NY_CLOSE = (ZoneInfo("America/New_York"), 7)
# The daily rollover (17:00 New York, server 00:00: where D1 bars end) widens spreads for about this long: measured on
# the broker's ticks, 5-22 pips in its first hour and AUDUSD wide until about 03:00 server time
ROLLOVER_QUIET = timedelta(hours=3)


def quiet_until(t: datetime) -> datetime | None:
    """When the rollover's wide spreads end, if naive-UTC `t` falls in them (None otherwise)."""
    tz = NY_CLOSE[0]
    ny = t.replace(tzinfo=timezone.utc).astimezone(tz)
    roll = ny.replace(hour=17, minute=0, second=0, microsecond=0)
    if ny < roll:
        roll -= timedelta(days=1)          # wall-clock arithmetic: 17:00 New York the day before, in either season
    end = (roll + ROLLOVER_QUIET).astimezone(timezone.utc).replace(tzinfo=None)
    return end if t < end else None


def server_zone(name: str):
    """MT5_SERVER_TZ -> "auto", None (one offset for every bar) or (zone, hours the clock runs ahead of it)."""
    key = name.strip().lower() or "auto"
    if key in ("auto", "fixed", "nyclose"):
        return {"auto": "auto", "fixed": None, "nyclose": NY_CLOSE}[key]
    try:
        return ZoneInfo(name.strip()), 0
    except (KeyError, ValueError, OSError):         # OSError: a folder of the zone database ("Europe")
        raise RuntimeError(f"MT5_SERVER_TZ={name!r} is not a time zone: use auto, nyclose, fixed or an IANA name "
                           "such as Europe/Athens") from None


def zone_offset(zone, at: float) -> int:
    """Seconds a server clock on `zone` runs ahead of UTC at epoch `at`."""
    tz, ahead = zone
    return int(datetime.fromtimestamp(at, tz).utcoffset().total_seconds()) + ahead * 3600


def to_utc(t: pd.Series, zone) -> pd.Series:
    """Server clock stamps -> naive UTC, each with the offset its zone had then. A stamp in an hour a DST switch skips
    or repeats is read on the clock from before the switch (brokers move their clock at the weekend, FX is shut)."""
    tz, ahead = zone
    local = (t - pd.Timedelta(hours=ahead)).dt.tz_localize(tz, ambiguous=np.ones(len(t), dtype=bool),
                                                           nonexistent=pd.Timedelta(hours=1))
    return local.dt.tz_convert("UTC").dt.tz_localize(None)


class PriceFeed:
    name = "base"
    max_history = 44000       # most bars one candles() call returns (the engine clamps history requests to it)
    timeframe = "H1"          # the engine's bars, set at each price tick (the demo feed's quote follows them)

    def candles(self, symbol: str, timeframe: str, count: int) -> pd.DataFrame:
        raise NotImplementedError

    def bar_seconds(self, timeframe: str) -> float:
        return TF_SECONDS[timeframe]

    def quote(self, symbol: str) -> float | None:
        """Live bid, when the feed has one (signals issued between bars use it as their reference price)."""
        return None


class DemoFeed(PriceFeed):
    """Deterministic random walk. With seconds_per_bar > 0 time is compressed so signals appear quickly."""
    name = "demo"
    max_history = 44000

    def __init__(self, seconds_per_bar: float = 0):
        self.seconds_per_bar = seconds_per_bar
        self._cache: dict[tuple, pd.DataFrame] = {}
        self._lock = threading.Lock()

    def bar_seconds(self, timeframe: str) -> float:
        return self.seconds_per_bar or TF_SECONDS[timeframe]

    def candles(self, symbol, timeframe, count):
        with self._lock:
            count = min(count, self.max_history)
            secs = self.bar_seconds(timeframe)
            current = int(time.time() // secs)          # index of the bar now forming
            key = (symbol, timeframe)
            df = self._cache.get(key)
            if df is None:
                df = self._generate(symbol, current - max(count, 600), current, None)
            elif df["idx"].iat[-1] < current - 1:
                new = self._generate(symbol, int(df["idx"].iat[-1]) + 1, current, df["close"].iat[-1])
                df = pd.concat([df, new]).iloc[-max(2000, count, len(df)):].reset_index(drop=True)
            if len(df) < count:          # older bars: a walk moved to end where the cached one starts
                first = int(df["idx"].iat[0])
                old = self._generate(symbol, first - (count - len(df)), first, None)
                old[["open", "high", "low", "close"]] += df["open"].iat[0] - old["close"].iat[-1]
                df = pd.concat([old, df]).reset_index(drop=True)
            self._cache[key] = df
            out = df.iloc[-count:].copy()
            out["time"] = pd.to_datetime(out["idx"] * secs, unit="s")
            return out[["time", "open", "high", "low", "close"]].reset_index(drop=True)

    @staticmethod
    def _generate(symbol, start_idx, end_idx, last_close):
        n = end_idx - start_idx
        pip = pip_size(symbol)
        vol = 12 * pip
        idx = np.arange(start_idx, end_idx)
        rng = np.random.default_rng(zlib.crc32(f"{symbol}:{start_idx}".encode()))
        regime = np.array([np.random.default_rng(zlib.crc32(f"{symbol}:r{i // 150}".encode())).normal(0, vol * 0.12)
                           for i in idx])
        steps = rng.normal(0, vol, n) + regime
        p0 = last_close if last_close is not None else BASE_PRICES.get(symbol.upper(), 1.0)
        close = p0 + np.cumsum(steps)
        open_ = np.r_[p0, close[:-1]]
        high = np.maximum(open_, close) + np.abs(rng.normal(0, vol * 0.5, n))
        low = np.minimum(open_, close) - np.abs(rng.normal(0, vol * 0.5, n))
        return pd.DataFrame({"idx": idx, "open": open_, "high": high, "low": low, "close": close})

    def quote(self, symbol):
        """The synthetic price now: the latest close (demo prices move bar by bar) of the series the engine reads,
        its own timeframe's (each timeframe is its own walk: a mode's longer bars or the learning history read
        meanwhile do not move it)."""
        return float(self.candles(symbol, self.timeframe, 1)["close"].iat[-1])


class MT5Feed(PriceFeed):
    """MT5 stamps bars in the broker's server time (often UTC+2/+3, changing with DST); the engine works in
    UTC, so each bar is converted with the offset the server clock had when the bar was made.
    MT5_SERVER_TZ: auto (default) | nyclose | fixed | an IANA name such as Europe/Athens. auto takes New York
    close (New York + 7h, most FX brokers) when the live offset agrees with it and the history's week opens
    moved with US DST, else shifts every bar by the live offset (right for a clock on one offset all year; for
    other DST rules history from the other season may be an hour off); fixed always does the latter.
    The live offset is learned from a tick seen to arrive while we watch: such a tick is at most a few minutes
    old, so rounding to the half hour gives it exactly."""
    name = "mt5"
    max_history = 44000

    def __init__(self, suffix: str = "", utc_offset_hours: float | None = None, server_tz: str | None = None):
        from bot.mt5_broker import MT5Broker
        self.zone = server_zone(server_tz if server_tz is not None else os.getenv("MT5_SERVER_TZ", ""))
        self.broker = MT5Broker()
        self.broker.connect()
        self._lock = threading.Lock()     # the MT5 package is not thread-safe
        self.suffix = suffix
        self._fixed = utc_offset_hours is not None
        self.offset = int(utc_offset_hours * 3600) if self._fixed else None
        self._learned_at = 0.0            # when a live tick last gave self.offset
        self._seen: dict[str, tuple[int, float]] = {}   # broker symbol -> (last tick time_msc, when we saw it)
        self._waiting_logged = False
        self._logged = None               # the conversion last logged about, so each is logged once
        self._us_dst: bool | None = None  # did the history's week opens move with US DST (None: not known yet)
        self._us_dst_retry = 0.0          # after a failed history read: when to try again
        self._us_dst_skip: dict[str, float] = {}   # broker symbol -> until when: its history could not tell
        self._points: dict[str, float] = {}

    def _learn_offset(self, sym: str, wait: float):
        """Update self.offset from `sym`'s ticks; watch for up to `wait` seconds for a new tick."""
        deadline = time.time() + wait
        self.broker._select(sym)          # ticks are only available for symbols in Market Watch
        while True:
            tick, now = self.broker.tick(sym), time.time()
            prev = self._seen.get(sym)
            self._seen[sym] = (tick.time_msc, now)
            if prev and tick.time_msc != prev[0] and now - prev[1] < 600:
                # a tick arrived since we last looked (< 10 min ago), so it is fresh to within 10 min
                offset = round((tick.time_msc / 1000 - now) / 1800) * 1800
                if offset != self.offset:
                    log.info("MT5 server clock is UTC%+.1fh", offset / 3600)
                self.offset, self._learned_at = offset, tick.time_msc / 1000 - offset   # the tick's UTC time
                return
            if time.time() >= deadline:
                return
            time.sleep(0.25)

    def _moves_with_us_dst(self, sym: str) -> bool:
        """Whether the server clock's history moved with US DST, as a New York close clock does: there the week
        opens at server 00:00 in both US seasons (Sunday 17:00 New York), on a clock with one offset all year an
        hour apart. A live offset that matches New York close now cannot tell these apart.
        Until one symbol's history tells (a 24/7 symbol has no weekend gaps; a short history has one season), New
        York close is assumed: a symbol that could not tell is not read again for a day, others are read at once; a
        failed read is retried in 10 min."""
        now = time.time()
        if self._us_dst is None and now >= self._us_dst_retry and now >= self._us_dst_skip.get(sym, 0.0):
            try:
                t = self.broker.rates(sym, "H1", 7000)["time"]       # over a year: both seasons
            except Exception as e:
                self._us_dst_retry = now + 600
                log.warning("MT5 feed: could not read %s history to check the server clock (tried again in 10 min): "
                            "%s", sym, e)
                return True
            opens = t[t.diff() > pd.Timedelta(days=1)]               # the first bar after each weekend
            mins = ((opens.dt.hour * 60 + opens.dt.minute + 720) % 1440 - 720).to_numpy()  # from server midnight
            near = opens.dt.as_unit("s").astype("int64").to_numpy() - 3 * 3600         # UTC within an hour
            summer = np.array([zone_offset(NY_CLOSE, float(e)) == 3 * 3600 for e in near], dtype=bool)
            if summer.sum() >= 4 and (~summer).sum() >= 4:
                self._us_dst = bool(abs(np.median(mins[summer]) - np.median(mins[~summer])) < 30)
            else:
                self._us_dst_skip[sym] = now + 86400
        return self._us_dst is not False      # not known yet: both readings agree on the current season's bars

    def _scheme(self, sym: str):
        """The zone bars are converted with, or None: every bar is shifted by self.offset."""
        zone = self.zone
        if zone == "auto":
            # compared when the offset was learned, not now: over a DST weekend the old offset is still the latest
            ny = not self._fixed and self.offset == zone_offset(NY_CLOSE, self._learned_at)
            zone = NY_CLOSE if ny and self._moves_with_us_dst(sym) else None
            state = ("nyclose" if self._us_dst else "nyclose?") if zone else "one"
            if state != self._logged:
                if state == "nyclose":
                    log.info("MT5 server clock follows New York close (New York + 7h): history is converted with US "
                             "DST")
                elif state == "nyclose?":
                    log.info("MT5 server clock matches New York close (New York + 7h) now: history is converted with "
                             "US DST, assumed until its history can be checked")
                elif ny:
                    log.info("MT5 server clock UTC%+.1fh all year (its history did not move with US DST): history is "
                             "shifted by this one offset (set MT5_SERVER_TZ if that is wrong)", self.offset / 3600)
                else:
                    log.warning("MT5 server clock UTC%+.1fh %s: history is shifted by this one offset, so bars from "
                                "the other DST season may be an hour off (set MT5_SERVER_TZ)", self.offset / 3600,
                                "set by MT5_UTC_OFFSET_HOURS" if self._fixed else "is not New York close")
            self._logged = state
        elif zone and self._learned_at and self._logged != "mismatch" \
                and zone_offset(zone, self._learned_at) != self.offset:
            log.warning("MT5_SERVER_TZ gives UTC%+.1fh but the live server clock is UTC%+.1fh: check the setting",
                        zone_offset(zone, self._learned_at) / 3600, self.offset / 3600)
            self._logged = "mismatch"
        return zone

    def candles(self, symbol, timeframe, count):
        sym = symbol + self.suffix
        with self._lock:
            known = isinstance(self.zone, tuple)    # a configured zone converts bars without the live offset
            if not self._fixed:
                try:
                    self._learn_offset(sym, wait=0 if self.offset is not None or known else 3)
                except Exception:
                    if self.offset is None and not known:
                        raise
                    # this symbol's ticks failed, but the clock is known (from other symbols): its bars still convert;
                    # a symbol the broker does not offer at all fails on its own bars below
            if self.offset is None and not known:  # market closed or no ticks yet: bar times would be wrong, so wait
                if not self._waiting_logged:
                    log.info("MT5 feed: waiting for live ticks to learn the broker's time zone")
                    self._waiting_logged = True
                return pd.DataFrame(columns=["time", "open", "high", "low", "close"])
            zone, offset = self._scheme(sym), self.offset
            df = self.broker.rates(sym, timeframe, count + 1)
            if sym not in self._points:
                self._points[sym] = self.broker.symbol_spec(sym).point
        df["time"] = to_utc(df["time"], zone) if zone else df["time"] - pd.Timedelta(seconds=offset)
        df["spread_price"] = df["spread"] * self._points[sym]      # MT5 records each bar's spread in points
        return df.iloc[:-1][["time", "open", "high", "low", "close", "spread_price"]].reset_index(drop=True)

    def quote(self, symbol):
        with self._lock:
            try:
                bid = float(self.broker.tick(symbol + self.suffix).bid)
            except Exception:
                return None
        return bid if bid > 0 else None


class TwelveDataFeed(PriceFeed):
    name = "twelvedata"
    INTERVALS = {"M1": "1min", "M5": "5min", "M15": "15min", "M30": "30min", "H1": "1h", "H4": "4h", "D1": "1day"}
    PAGE = 5000                 # /time_series serves at most 5000 bars a request: longer histories are paged back
    max_history = 20000         # 5 requests, about 40 s a symbol on the free tier
    QUOTE_SECONDS = 15          # a quote is reused this long

    def __init__(self, api_key: str):
        if not api_key:
            raise RuntimeError("TWELVEDATA_API_KEY is not set")
        self.api_key = api_key
        self._lock = threading.Lock()
        self._last_call = 0.0
        self._quotes: dict[str, tuple[float, float]] = {}    # symbol -> (price, when)

    def _get(self, endpoint: str, **params) -> dict:
        with self._lock:                       # free tier: 8 requests/minute
            wait = 8 - (time.time() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()
            try:
                r = requests.get(f"https://api.twelvedata.com/{endpoint}", timeout=20,
                                 params={**params, "apikey": self.api_key})
            except OSError as e:           # requests puts the URL in its error text, the key included
                text = str(e).replace(self.api_key, "***")
                if text == str(e):
                    raise
                try:
                    clean = type(e)(text)
                except Exception:
                    clean = OSError(text)
                raise clean from None      # the original (and its chain) carries the key too
        return r.json()

    def candles(self, symbol, timeframe, count):
        count = min(count, self.max_history)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        pages, end, have, retried, why = [], None, 0, False, "no older bars"
        while have < count:
            params = {"symbol": f"{symbol[:3]}/{symbol[3:]}", "interval": self.INTERVALS[timeframe],
                      "outputsize": min(count - have + 1, self.PAGE), "timezone": "UTC"}
            if end is not None:
                params["end_date"] = end.strftime("%Y-%m-%d %H:%M:%S")
            try:
                data = self._get("time_series", **params)
            except (OSError, ValueError) as e:     # requests' errors are OSErrors; ValueError: a reply that is not JSON
                if not pages:
                    raise
                why = f"{type(e).__name__}: {e}"
                break                          # an older page failed: serve the newer ones already fetched
            if data.get("code") == 429 and not retried:    # this minute's credits are gone (key used elsewhere too?)
                retried = True
                time.sleep(60)
                continue
            if data.get("status") != "ok":
                if not pages:
                    raise RuntimeError(f"Twelve Data error for {symbol}: {data.get('message', data)}")
                why = data.get("message", data)
                break                          # older history ran out (or was refused): serve what we have
            values = data.get("values") or []
            df = pd.DataFrame(values, columns=["datetime", "open", "high", "low", "close"])
            df["time"] = pd.to_datetime(df["datetime"])
            if end is None:
                df = df[df["time"] + timedelta(seconds=TF_SECONDS[timeframe]) <= now]     # drop the bar still forming
            else:
                df = df[df["time"] < end]      # end_date may be inclusive
            if not df.empty:
                pages.append(df)
                have += len(df)
                end = df["time"].min()
            if df.empty or len(values) < params["outputsize"]:
                break                          # the oldest bars there are
        if pages and have < count:
            log.warning("Twelve Data: %d of %d %s %s bars (%s)", have, count, symbol, timeframe, why)
        if not pages:
            return pd.DataFrame(columns=["time", "open", "high", "low", "close"])
        df = pd.concat(pages).drop_duplicates("time").sort_values("time")
        for c in ("open", "high", "low", "close"):
            df[c] = df[c].astype(float)
        return df[["time", "open", "high", "low", "close"]].iloc[-count:].reset_index(drop=True)

    def quote(self, symbol):
        hit = self._quotes.get(symbol)
        if hit and time.time() - hit[1] < self.QUOTE_SECONDS:
            return hit[0]
        try:
            price = float(self._get("price", symbol=f"{symbol[:3]}/{symbol[3:]}")["price"])
        except Exception:
            return None
        if not price > 0:
            return None
        self._quotes[symbol] = (price, time.time())
        return price


def make_feed(kind: str, seconds_per_bar: float = 0, twelvedata_key: str = "", mt5_suffix: str = "",
              mt5_utc_offset_hours: float | None = None, mt5_server_tz: str | None = None) -> PriceFeed:
    if kind == "mt5":
        return MT5Feed(mt5_suffix, mt5_utc_offset_hours, mt5_server_tz)
    if kind == "twelvedata":
        return TwelveDataFeed(twelvedata_key)
    return DemoFeed(seconds_per_bar)
