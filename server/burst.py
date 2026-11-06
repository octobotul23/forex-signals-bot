"""News burst mode: pure helpers shared by the engine and the API (no FastAPI, no database session).

Before a high-impact release the client's connector places a straddle of stop orders on the pair the
release moves and exits the filled side in staggered steps. Full spec: docs/NEWS_BURST.md.
Since v1.3.0 a release may also start Rapid news runs (bot/rapid.py): Burst rows of kind "rapid", planned first
(plan_runs), on pairs the classic burst then leaves alone.
"""
import math
import re
from datetime import datetime

from bot.rapid import DEFAULTS as RAPID_DEFAULTS, RUN_KIND as RAPID, clamp_rapid
from bot.symbols import symbol_scale

from .settings import DEFAULT_USER_SETTINGS

# the pairs a currency's releases can be traded on, in order: the first is the currency's own pair (the only one up
# to v1.2.0, and still the one traded when the user picked no pairs)
BURST_PAIRS = {"USD": ["EURUSD", "XAUUSD", "GBPUSD", "USDJPY"], "EUR": ["EURUSD"], "GBP": ["GBPUSD", "GBPJPY"],
               "JPY": ["USDJPY", "GBPJPY"], "AUD": ["AUDUSD"], "NZD": ["NZDUSD"], "CAD": ["USDCAD"], "CHF": ["USDCHF"]}
BURST_SYMBOLS = list(dict.fromkeys(s for pairs in BURST_PAIRS.values() for s in pairs))    # every one, once
FINAL = {"closed", "expired", "skipped", "cancelled", "error"}
REPORTED = ("scheduled", "armed", "triggered", *sorted(FINAL))      # statuses a connector may report
RANK = {"planned": 0, "scheduled": 1, "armed": 2, "triggered": 3} | {s: 4 for s in FINAL}
MIN_CONNECTOR = (1, 1, 0)
V130 = (1, 3, 0)            # the connector that knows the v1.3.0 keys, Rapid runs and the new pairs (needs_version)
OUTDATED = "connector out of date: download the new connector"
USER_CANCEL = "cancelled by you"        # also the marker that keeps a user's cancel alive until the connector confirms
TIMED_OUT = "no final report from the connector"    # the server's own `error`: a final report that comes later wins
# per-user settings clamps (§1); trades is also capped by the admin's max_trades
LIMITS = {"trades": (1, 40, int), "lead_seconds": (10, 600, int), "offset_pips": (1.0, 100.0, float),
          "sl_pips": (2.0, 200.0, float), "tp_start_pips": (1.0, 500.0, float), "tp_step_pips": (0.0, 100.0, float),
          "hold_seconds": (5, 600, int), "expire_seconds": (10, 1800, int), "lots": (0.01, 100.0, float)}
# since v1.3.0 (connector/connector.py BURST_LIMITS and BURST_MONEY clamp them the same way): the stop in pips (sl_pips
# and the risk budget, as before) or as the money the whole run may lose; sl_money by its unit: (min, max, default),
# pips reads the pct range (it is not used there)
SL_UNITS = ("pips", "pct", "usd")
SL_MONEY = {"pct": (0.1, 10.0, 2.0), "usd": (1.0, 100_000.0, 100.0)}
MAX_HOLD = 1800             # max_hold_seconds (Y): hold_seconds (X) .. this; missing = X
# what a burst block saved by v1.2.0 (without these keys) ran with: legacy_burst fills them in, never the new defaults
LEGACY_BURST = {"sl_unit": "pips", "analysis": False}
# a Rapid news run's params (rapid_params): these keys of the user's rapid block, then max_hold_seconds and
# max_spread_pips
RAPID_PARAMS = ("orders", "sl_unit", "risk_unit", "risk_min", "risk_max", "sl_pips", "lots", "tp_min_usd", "tp_max_usd",
                "limit_pips", "offset_pips", "expire_seconds", "hold_seconds", "analysis")
# talks, minutes, auctions... have no single release moment for a straddle to catch
_NO_SINGLE_RELEASE = re.compile(r"speak|speech|press conference|conference|testif|hearing|minutes|meeting|summit"
                                r"|election|budget|holiday|auction|parliament", re.I)


def event_ok(title: str | None) -> bool:
    return not _NO_SINGLE_RELEASE.search(title or "")


def _field(e, key):
    return e.get(key) if isinstance(e, dict) else getattr(e, key)


def _collapse(evs: list[tuple]) -> dict:
    """The fields of the releases (currency, event id, title) that land on one pair in one minute: the first one's
    id and currency, every title once, every currency once."""
    title = evs[0][2]
    for _, _, t in evs[1:]:
        if t not in title.split(" + "):
            title = f"{title} + {t}"
    return {"event_id": evs[0][1], "currency": evs[0][0], "title": title,
            "currencies": list(dict.fromkeys(cur for cur, _, _ in evs))}


def candidates(events, app_settings: dict) -> list[dict]:
    """Calendar events (rows or dicts) a burst can trade: one per (pair, release time), for every pair of the
    currency's list in the app symbols. Events that land on the same pair in the same minute collapse into one;
    `currencies` lists every currency behind it, `ranks` the pair's place in each one's list and `events` the
    releases themselves (currency, id, title), for plan_for."""
    symbols = app_settings.get("symbols") or []
    found: dict[tuple, list[tuple]] = {}
    for e in sorted(events, key=lambda e: _field(e, "time")):
        cur, title = (_field(e, "currency") or "").upper(), _field(e, "title") or ""
        if _field(e, "impact") != "High" or not event_ok(title):
            continue
        for sym in BURST_PAIRS.get(cur, ()):
            if sym in symbols:
                found.setdefault((sym, _field(e, "time")), []).append((cur, _field(e, "id"), title))
    return [{"time": t, "symbol": sym, **_collapse(evs), "events": evs,
             "ranks": {cur: BURST_PAIRS[cur].index(sym) for cur, _, _ in evs}} for (sym, t), evs in found.items()]


def pick_pairs(cands: list[dict], picks: list[str], taken=(), n: int = 1) -> list[dict]:
    """The pairs one run takes at a release. `cands`: that release's pairs the user may trade, in the currency's
    list order, each with its `rank` there. The first n the user picked that no other run took (`taken`); no picks:
    the currency's own pair (rank 0), as up to v1.2.0, and when a run of another kind took it (Rapid wins on its
    pairs) the next ones of the list that none took: the classic burst covers the pairs Rapid left. No own pair
    among `cands` (the user does not trade it): none, as up to v1.2.0."""
    free = [c for c in cands if c["symbol"] not in taken]
    if picks:
        return [c for c in free if c["symbol"] in picks][:n]
    if any(c["rank"] == 0 and c["symbol"] in taken for c in cands):
        return free[:n]
    return [c for c in free if c["rank"] == 0][:n]


def burst_picks(burst: dict) -> list[str]:
    """The pairs the user picked for the burst (the stored `burst` block), clamped; empty = each currency's own."""
    try:
        return clamp_burst({"symbols": burst.get("symbols") or []}, 1)["symbols"]
    except ValueError:
        return []                                        # a corrupt stored value: the currencies' own pairs


def stands(symbol: str, currency: str, live, picks) -> bool:
    """A planned burst on `symbol` still has a release to catch: one of the currencies released at its time (`live`,
    from Engine.live_releases) lists the pair and has it as its own pair (the only rule up to v1.2.0), is the burst's
    own currency, or had it picked by the user. A release of another currency that merely lists the pair (USD and
    GBPUSD) does not keep it."""
    return any(symbol in BURST_PAIRS.get(cur, ())
               and (symbol == BURST_PAIRS[cur][0] or cur == currency or symbol in picks) for cur in live)


def plan_for(user_settings: dict, cands: list[dict], app_symbols: list[str], have=frozenset(), taken=()) -> list[dict]:
    """The candidates the user's classic burst trades: at each release (one currency's events in one minute) the
    pair pick_pairs gives. None for a release that already has a burst of this user (`have`: (symbol, time,
    currency), final ones too; (symbol, time) where the currency is not known): one planned for that currency,
    whatever its pair (the picks may have changed since), or one on the release's own pair, on one the user picked or
    on the one it falls back to (its own pair `taken`, no picks). One run per release, and a cancel stands. Each comes
    with the releases that chose its pair only (their first event, their titles)."""
    b = user_settings.get("burst") or {}
    picks = burst_picks(b)
    wanted = b.get("currencies") or []
    pairs = {(h[0], h[1]) for h in have}
    # the releases the bursts were planned for; a run of another kind holds its pair (`taken`), not the release
    runs = {(h[2], h[1]) for h in have if len(h) > 2 and h[0] not in taken}
    releases: dict[tuple, list[dict]] = {}
    for c in cands:
        if allowed_for(user_settings, c, app_symbols):
            for cur, rank in c["ranks"].items():
                releases.setdefault((cur, c["time"]), []).append({**c, "rank": rank})
    picked, run = {}, set()
    for (cur, t), group in releases.items():
        pool = pick_pairs(sorted(group, key=lambda c: c["rank"]), picks, taken, len(group))
        # no picks: the release's own pair or the one it falls back to, not every pair of the list Rapid left
        own = {c["symbol"] for c in (pool if picks else pool[:1])} | ({BURST_PAIRS[cur][0]} - set(taken))
        if not pool or (cur, t) in runs or any((s, t) in pairs for s in own):
            continue
        # a release the user does not trade still names the burst of a pair it would have chosen (as in v1.2.0)
        picked[(cur, t)] = pool[0]["symbol"]
        if not wanted or cur in wanted:
            run.add((pool[0]["symbol"], t))
    out = []
    for c in cands:
        if (c["symbol"], c["time"]) in run:
            curs = {cur for (cur, t), s in picked.items() if (s, t) == (c["symbol"], c["time"])}
            out.append({**c, **_collapse([e for e in c["events"] if e[0] in curs])})
    return out


def allowed_for(user_settings: dict, cand: dict, app_symbols: list[str]) -> bool:
    """The user's own pair picks and burst currencies allow this candidate (empty lists allow all)."""
    if cand["symbol"] not in app_symbols:
        return False
    if user_settings.get("symbols") and cand["symbol"] not in user_settings["symbols"]:
        return False
    wanted = (user_settings.get("burst") or {}).get("currencies") or []
    return not wanted or any(c in wanted for c in cand.get("currencies") or [cand.get("currency")])


def wants(user_settings: dict, kind: str | None = "burst") -> bool:
    """The user's settings ask for news runs of `kind`: "burst" = the classic burst's switch, as up to v1.2.0; "rapid"
    = Rapid on with its news trigger (rapid.triggers.news; its signal runs are the connector's own). No other kind."""
    if (kind or "burst") == "burst":
        return bool((user_settings.get("burst") or {}).get("enabled"))
    r = user_settings.get("rapid") if isinstance(user_settings.get("rapid"), dict) else {}
    trig = r.get("triggers") if isinstance(r.get("triggers"), dict) else {}
    return kind == RAPID and r.get("enabled") is True and trig.get("news") is True


def rapid_allowed(user_settings: dict, cand: dict, app_symbols: list[str], picks) -> bool:
    """Rapid may trade this candidate's pair: the engine and the user trade it (allowed_for's pair checks; the classic
    burst's currencies are its own) and Rapid's pairs allow it (`picks`, rapid.symbols; empty = any)."""
    if cand["symbol"] not in app_symbols:
        return False
    if user_settings.get("symbols") and cand["symbol"] not in user_settings["symbols"]:
        return False
    return not picks or cand["symbol"] in picks


def rapid_plan(user_settings: dict, cands: list[dict], app_symbols: list[str], have=frozenset()) -> list[dict]:
    """The candidates the user's Rapid news runs trade (none unless wants(…, "rapid")): at each release (one currency's
    events in one minute) its first rapid.max_runs pairs, in the currency's list order (BURST_PAIRS), that
    rapid_allowed lets through and no run of another kind holds. `have`: (symbol, time, currency, kind) of the user's
    runs at those times, final ones too. None for a release that already has a Rapid run (one planned for its
    currency, or one on a pair it would take): it is planned once, and a cancel stands. A pair two releases take is one
    run (the unique user, symbol, event_time), with the releases that chose it (their first event, their titles)."""
    if not wants(user_settings, RAPID):
        return []
    r = clamp_rapid(None, user_settings["rapid"])        # the stored block clamped again; one gone bad: its default
    held = {(h[0], h[1]) for h in have if len(h) > 3 and h[3] != RAPID}
    mine = {(h[0], h[1]) for h in have if len(h) > 3 and h[3] == RAPID}
    runs = {(h[2], h[1]) for h in have if len(h) > 3 and h[3] == RAPID}
    releases: dict[tuple, list[dict]] = {}
    for c in cands:
        if rapid_allowed(user_settings, c, app_symbols, r["symbols"]):
            for cur, rank in c["ranks"].items():
                releases.setdefault((cur, c["time"]), []).append({**c, "rank": rank})
    picked: dict[tuple, set] = {}
    for (cur, t), group in releases.items():
        pool = [c["symbol"] for c in sorted(group, key=lambda c: c["rank"]) if (c["symbol"], t) not in held]
        pool = pool[:r["max_runs"]]
        if (cur, t) in runs or any((s, t) in mine for s in pool):
            continue
        for s in pool:
            picked.setdefault((s, t), set()).add(cur)
    return [{**c, **_collapse([e for e in c["events"] if e[0] in picked[(c["symbol"], c["time"])]])}
            for c in cands if (c["symbol"], c["time"]) in picked]


def plan_runs(user_settings: dict, cands: list[dict], app_symbols: list[str], have=frozenset(),
              classic: bool = True) -> list[tuple[dict, str]]:
    """The news runs to plan for the user, as (candidate, kind): Rapid's first (rapid_plan), then with `classic` the
    classic burst's (plan_for: one run per release) on the pairs no run of another kind holds at that release time:
    never two runs on one pair. Rapid on a release's own pair moves a classic burst without picks to the next pair of
    the list Rapid left (pick_pairs). `have`: (symbol, time, currency, kind) of the user's runs at those times, final
    ones too. With no run of another kind at those times (Rapid off), the classic burst's are plan_for's as before."""
    rapid = rapid_plan(user_settings, cands, app_symbols, have)
    out = [(c, RAPID) for c in rapid]
    if not classic:
        return out
    held = {(h[0], h[1]) for h in have if len(h) > 3 and h[3] != "burst"} | {(c["symbol"], c["time"]) for c in rapid}
    mine = {tuple(h[:3]) for h in have}
    if not held:
        burst = plan_for(user_settings, cands, app_symbols, mine)
    else:      # plan_for's `taken` is pairs, not times: one call per release time (cands come grouped by time)
        burst = [b for t in dict.fromkeys(c["time"] for c in cands)
                 for b in plan_for(user_settings, [c for c in cands if c["time"] == t], app_symbols, mine,
                                   {s for s, ht in held if ht == t})]
    return out + [(c, "burst") for c in burst]


def _number(v, cast):
    try:
        v = cast(v)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("invalid_number")
    if v != v:                                           # NaN
        raise ValueError("invalid_number")
    return v


def clamp_burst(body: dict, max_trades: int, cur: dict | None = None) -> dict:
    """The burst settings found in `body`, clamped to their ranges; unknown keys are dropped.
    `cur`: the settings `body` changes (a user's merged block), for what a key depends on: sl_money's range is its
    unit's (body's, else cur's; pct when neither has one), a unit changed without an sl_money resets it to the new
    unit's default (2 % must not become 2 $; cur without a unit is pips), and max_hold_seconds (Y) is raised to
    hold_seconds (X, body's, else cur's), never swapped; a Y sent as None (null: the user's way back to "Y follows X",
    as v1.2.0 held a trade) comes back as None, for the caller to remove the key (raise_max_hold drops it). Only keys
    in `body` come back, plus that reset sl_money. Raises ValueError("invalid_number" | "invalid_currencies" |
    "invalid_symbols" | "invalid_unit" | "invalid_analysis") for values that cannot be used."""
    out = {}
    for key, (lo, hi, cast) in LIMITS.items():
        if key not in body:
            continue
        if key == "trades":
            hi = max(1, min(hi, int(max_trades)))
        out[key] = max(lo, min(hi, _number(body[key], cast)))
    unit = (cur or {}).get("sl_unit", "pips")
    if "sl_unit" in body:
        if body["sl_unit"] not in SL_UNITS:
            raise ValueError("invalid_unit")
        if cur is not None and body["sl_unit"] != unit and "sl_money" not in body:
            out["sl_money"] = SL_MONEY.get(body["sl_unit"], SL_MONEY["pct"])[2]
        out["sl_unit"] = unit = body["sl_unit"]
    if "sl_money" in body:
        lo, hi, _ = SL_MONEY.get(unit, SL_MONEY["pct"])
        out["sl_money"] = max(lo, min(hi, _number(body["sl_money"], float)))
    if "max_hold_seconds" in body and body["max_hold_seconds"] is None:
        out["max_hold_seconds"] = None                  # removed: it follows X again
    elif "max_hold_seconds" in body:
        y = _number(body["max_hold_seconds"], int)
        x = out.get("hold_seconds")
        if x is None:                                    # body did not change X: cur's, when it can be read
            try:
                x = max(LIMITS["hold_seconds"][0], min(LIMITS["hold_seconds"][1], int((cur or {})["hold_seconds"])))
            except (KeyError, TypeError, ValueError, OverflowError):
                x = LIMITS["hold_seconds"][0]
        out["max_hold_seconds"] = max(x, min(MAX_HOLD, y))
    if "analysis" in body:
        if not isinstance(body["analysis"], bool):     # "false" must never switch it on
            raise ValueError("invalid_analysis")
        out["analysis"] = body["analysis"]
    if "enabled" in body:
        out["enabled"] = bool(body["enabled"])
    if "currencies" in body:
        if not isinstance(body["currencies"], list):
            raise ValueError("invalid_currencies")
        out["currencies"] = [c for c in dict.fromkeys(str(c).upper() for c in body["currencies"]) if c in BURST_PAIRS]
    if "symbols" in body:            # the pairs the user's burst trades at a release (plan_for); empty = each own pair
        if not isinstance(body["symbols"], list):
            raise ValueError("invalid_symbols")
        out["symbols"] = [s for s in dict.fromkeys(str(s).upper() for s in body["symbols"]) if s in BURST_SYMBOLS]
    return out


def legacy_burst(block) -> dict:
    """A stored burst block over the defaults, the keys it lacks filled as the version that saved it ran: a block
    saved by v1.2.0 has no v1.3.0 keys and gets its stop in pips and no analysis (LEGACY_BURST), lots and sl_money
    from the defaults (sl_money: its unit's), and no max_hold_seconds (it follows hold_seconds, X). Never the new
    users' defaults for those: no stored setting is rewritten, the gap is read the old way (db.User.merged_settings,
    params_for)."""
    b = block if isinstance(block, dict) else {}
    out = {**DEFAULT_USER_SETTINGS["burst"], **LEGACY_BURST, **b}
    if "max_hold_seconds" not in b:
        out.pop("max_hold_seconds", None)
    if "sl_money" not in b:
        out["sl_money"] = SL_MONEY.get(out["sl_unit"], SL_MONEY["pct"])[2]
    return out


def params_for(user_settings: dict, max_trades: int, kind: str | None = "burst", **override) -> dict:
    """Snapshot sent to the connector: the user's burst settings clamped again (the admin cap may have dropped
    since they were saved) + the risk budget for the whole group + the spread limit. A key the stored block lacks
    is read as legacy_burst reads it; a corrupt stored value gives the v1.2.0 defaults rather than no burst.
    max_hold_seconds is always there (Y, at least hold_seconds; a missing one is X). A Rapid news run's (`kind`
    "rapid") are rapid_params'."""
    if kind == RAPID:
        return rapid_params(user_settings, max_trades)
    try:
        p = clamp_burst(legacy_burst(user_settings.get("burst")), max_trades)
    except ValueError:
        p = clamp_burst(legacy_burst({}), max_trades)    # a corrupt stored value: defaults rather than no burst
    p.update(clamp_burst(override, max_trades, p))
    if p.get("max_hold_seconds") is None:               # none (or null): Y follows X
        p["max_hold_seconds"] = p["hold_seconds"]
    p["max_hold_seconds"] = max(p["hold_seconds"], p["max_hold_seconds"])
    return {**{k: p[k] for k in LIMITS}, "risk_pct": float(user_settings.get("risk_per_trade_pct", 1.0)),
            "max_spread_pips": float(user_settings.get("max_spread_pips", 3.0)), "sl_unit": p["sl_unit"],
            "sl_money": p["sl_money"], "analysis": p["analysis"], "max_hold_seconds": p["max_hold_seconds"]}


def rapid_params(user_settings: dict, max_trades: int) -> dict:
    """A Rapid news run's snapshot (Burst.params of a row of kind "rapid"): the RAPID_PARAMS of the user's rapid block
    clamped again (bot/rapid.py clamp_rapid; a block missing or gone bad reads as a new user's), its orders capped by
    the admin's max_trades as the burst's trades are, max_hold_seconds always there (Y; a missing one is X), and the
    user's max_spread_pips: the classic burst's spread cap, since a news run is placed into the same pre-release
    spread (the rapid block has none; short_term's is the Scalper's, for its signals)."""
    rp = user_settings.get("rapid")
    r = clamp_rapid(None, rp if isinstance(rp, dict) else DEFAULT_USER_SETTINGS["rapid"])
    p = {k: r[k] for k in RAPID_PARAMS}
    p["orders"] = max(1, min(p["orders"], int(max_trades)))
    p["max_hold_seconds"] = r.get("max_hold_seconds", r["hold_seconds"])
    p["max_spread_pips"] = float(user_settings.get("max_spread_pips", 3.0))
    return p


def needs_version(params, kind: str | None = "burst", symbol: str | None = None) -> tuple:
    """The oldest connector that runs this plan as planned: V130 for anything v1.2.0's does not know (a stop in money,
    a hold past X, the pre-news analysis, a Rapid run, a pair added in v1.3.0), else MIN_CONNECTOR. A value it cannot
    read counts as new: an older connector would run it some other way. A plan stored by v1.2.0 needs MIN_CONNECTOR."""
    p = params if isinstance(params, dict) else {}
    if (kind or "burst") != "burst" or (symbol and new_pair(symbol)):
        return V130
    if p.get("sl_unit", "pips") != "pips" or p.get("analysis", False) is not False:
        return V130
    if "max_hold_seconds" in p:
        try:
            y, x = float(p["max_hold_seconds"]), float(p.get("hold_seconds", LIMITS["hold_seconds"][0]))
        except (TypeError, ValueError, OverflowError):
            return V130
        if not y <= x:                                   # NaN too
            return V130
    return MIN_CONNECTOR


def new_pair(symbol) -> bool:
    """A pair a connector before v1.3.0 cannot trade right: gold (its pip was 0.0001) and the pairs whose pip
    settings are scaled (bot/symbols.py SYMBOL_SCALE: GBPJPY, XAUUSD)."""
    s = str(symbol or "").upper()
    return "XAU" in s or symbol_scale(s) != 1.0


def pairs_for(symbols, version) -> list:
    """The pairs of a list sent to a connector of `version`: before v1.3.0 without the new_pair ones (its scalper
    would watch them, its pip for gold is wrong)."""
    symbols = list(symbols) if isinstance(symbols, (list, tuple)) else []
    return symbols if version_ok(version, V130) else [s for s in symbols if not new_pair(s)]


def raise_max_hold(block: dict) -> dict:
    """`block` with its max_hold_seconds (Y) raised to hold_seconds (X) when X went above it (never swapped); a None
    one (clamp_burst's null: the user's "Y follows X") or one that cannot be read is dropped (it follows X again). A
    block without Y keeps none."""
    out = dict(block)
    if "max_hold_seconds" not in out:
        return out
    try:
        y = int(out["max_hold_seconds"])                # None too: TypeError
    except (TypeError, ValueError, OverflowError):
        out.pop("max_hold_seconds")
        return out
    try:
        out["max_hold_seconds"] = max(int(out["hold_seconds"]), min(MAX_HOLD, y))
    except (KeyError, TypeError, ValueError, OverflowError):
        out["max_hold_seconds"] = min(MAX_HOLD, y)       # X unreadable: Y capped, not raised
    return out


def run_seconds(params, kind: str | None = "burst") -> int:
    """How long a run lasts after its release at most: expire_seconds for its orders to fill, then the hold up to
    max_hold_seconds (Y; missing = hold_seconds, X, as up to v1.2.0). A value that cannot be read is its default: the
    burst's, or Rapid's for a run of `kind` "rapid" (its params carry Rapid's keys: rapid_params)."""
    p = params if isinstance(params, dict) else {}
    d = RAPID_DEFAULTS if kind == RAPID else {"expire_seconds": 90, "hold_seconds": 30}

    def num(key, default):
        try:
            v = float(p.get(key, default))
        except (TypeError, ValueError, OverflowError):
            return default
        return v if math.isfinite(v) else default

    x = num("hold_seconds", d["hold_seconds"])
    return int(num("expire_seconds", d["expire_seconds"])) + int(max(x, num("max_hold_seconds", x)))


def version_ok(version, minimum: tuple = MIN_CONNECTOR) -> bool:
    """Connector version (from its hello) is new enough to run bursts (or `minimum`)."""
    nums = [int(n) for n in re.findall(r"\d+", str(version or ""))[:3]]
    return bool(nums) and tuple(nums + [0] * (3 - len(nums))) >= minimum


def seconds_until(burst, now: datetime) -> float:
    return (burst.event_time - now).total_seconds()


def plan_payload(burst, now: datetime, context: dict | None = None) -> dict:
    """The `burst` message for the connector; it counts down from `seconds_until` on its own clock. `kind` (since
    v1.3.0; a connector before it ignores the keys it does not know) and the release's pre-news `context`
    (server/news_view.py context_for) when there is one."""
    out = {"id": burst.id, "symbol": burst.symbol, "event_title": burst.event_title, "currency": burst.currency,
           "event_time": burst.event_time.isoformat() + "Z", "seconds_until": round(seconds_until(burst, now), 1),
           "test": bool(burst.test), "params": dict(burst.params or {}),
           "kind": getattr(burst, "kind", None) or "burst"}
    if context is not None:
        out["context"] = context
    return out
