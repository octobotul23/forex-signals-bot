"""Economic calendar sources. Each fetch_* returns the same dicts: id, time (naive UTC), currency, title, impact
(High | Medium | Low | Holiday), forecast, previous, actual (display strings, "" when unknown) and source.

forexfactory  default. The public ForexFactory weekly JSON feed: free, the whole week, every impact level, but no
              'actual' values (the released figure never shows).
investing     CALENDAR_SOURCE=investing + APIFY_TOKEN (server/settings.py). The Investing.com calendar through the
              Apify actor kibaale/investing-economic-calendar-scraper: the released figure as soon as it prints,
              numbers with their unit, ids that survive a moved release. Apify bills every row returned, so the engine
              asks for High/Medium releases only and only when a run is due (Engine.refresh_calendar).

investing.com has no public API and its terms prohibit using or redistributing its data without written
permission; the actor is a third-party scraper the site has not licensed. That source stays off unless the operator
switches it on and takes that on. A licensed calendar API plugs in the same way: another fetch_* returning these
dicts, called from Engine.refresh_calendar.
"""
import hashlib
from datetime import date, datetime, timezone

import requests

FF_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (FX platform calendar)"}


def fetch_forexfactory_week() -> list[dict]:
    r = requests.get(FF_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    out = []
    for e in r.json():
        t = datetime.fromisoformat(e["date"]).astimezone(timezone.utc).replace(tzinfo=None)
        key = f"{e.get('title')}|{e.get('country')}|{e.get('date')}"
        out.append({
            "id": hashlib.sha1(key.encode()).hexdigest(),
            "time": t,
            "currency": (e.get("country") or "").upper(),
            "title": e.get("title") or "",
            "impact": e.get("impact") or "Low",
            "forecast": e.get("forecast") or "",
            "previous": e.get("previous") or "",
            "actual": e.get("actual") or "",
            "source": "forexfactory",
        })
    return out


# ── Investing.com through Apify ─────────────────────────────────
INVESTING_ACTOR = "kibaale~investing-economic-calendar-scraper"
APIFY_RUN = "https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
IMPACTS = {"high": "High", "medium": "Medium", "low": "Low"}


def _utc(value) -> datetime | None:
    """ISO time ('2026-09-16T12:30:00Z') -> naive UTC. None for a bare date (all day, tentative) or anything else:
    the guard and bursts need the minute."""
    if not isinstance(value, str) or "T" not in value:
        return None
    try:
        t = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return t.astimezone(timezone.utc).replace(tzinfo=None) if t.tzinfo else t


def _figure(value, unit, precision) -> str:
    """A released / forecast / previous figure as the calendar shows it: 0.5 with unit '%' and precision 1 -> '0.5%'."""
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        try:
            text = f"{value:.{int(precision)}f}" if precision not in (None, "") else f"{value:g}"
        except (TypeError, ValueError):
            text = f"{value:g}"
    else:
        text = str(value).strip()
    unit = str(unit or "").strip()
    if text and unit and not text.endswith(unit):
        text += unit
    return text[:40]


def parse_investing(rows) -> list[dict]:
    """Dataset rows of the actor -> calendar dicts. Holidays, rows without an exact time or a currency and repeats of
    one release are left out. The id comes from Investing.com's occurrence id, so a moved release keeps its row."""
    out, seen = [], set()
    for e in rows if isinstance(rows, list) else []:
        if not isinstance(e, dict) or (e.get("type") or "event") != "event":
            continue
        t, cur = _utc(e.get("time")), str(e.get("currency") or "").strip().upper()
        title = str(e.get("event") or e.get("shortName") or "").strip()
        if t is None or not cur or len(cur) > 8 or not title:
            continue
        occ = e.get("occurrenceId")
        key = f"investing|{occ}" if occ not in (None, "") else f"investing|{e.get('eventId')}|{cur}|{title}|{t.isoformat()}"
        eid = hashlib.sha1(key.encode()).hexdigest()
        if eid in seen:
            continue
        seen.add(eid)
        unit, prec = e.get("unit"), e.get("precision")
        out.append({
            "id": eid,
            "time": t,
            "currency": cur,
            "title": title[:255],
            "impact": IMPACTS.get(str(e.get("importance") or "").strip().lower(), "Low"),
            "forecast": _figure(e.get("forecast"), unit, prec),
            "previous": _figure(e.get("previous"), unit, prec),
            "actual": _figure(e.get("actual"), unit, prec),
            "source": "investing",
        })
    return out


def fetch_investing(token: str, countries: list[str], start: date, days: int, importance: str = "high_medium",
                    actor: str = INVESTING_ACTOR, timeout: float = 120) -> list[dict]:
    """One synchronous actor run: `days` UTC days from `start` for these ISO country codes (EU = euro area);
    importance high | high_medium | all. Raises when Apify refuses, fails or answers something unreadable, so the
    caller keeps what it has instead of wiping the calendar; an empty list is a valid answer (a quiet day)."""
    if not token:
        raise RuntimeError("APIFY_TOKEN is not set")
    body = {"startDate": start.isoformat(), "days": max(1, min(90, int(days))), "countries": list(countries),
            "importance": importance, "includeExchangeHolidays": False, "includeDescriptions": False}
    r = requests.post(APIFY_RUN.format(actor=actor), params={"format": "json", "clean": "1"}, json=body,
                      headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Apify HTTP {r.status_code}: {r.text[:200]}")
    rows = r.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected Apify answer: {str(rows)[:200]}")
    if rows and not any(isinstance(e, dict) and "time" in e and ("event" in e or "shortName" in e) for e in rows):
        raise RuntimeError(f"none of the {len(rows)} rows Apify returned looks like a calendar event")
    return parse_investing(rows)
