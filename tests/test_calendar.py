"""Economic calendar (server/feeds/calendar.py, Engine.refresh_calendar / save_calendar): the Investing.com rows the
Apify actor returns, the call made to Apify, which run the engine makes and when, the ForexFactory fallback and what
each fetch replaces in the table.

    .venv\\Scripts\\python tests\\test_calendar.py

Runs on a throw-away SQLite file with a fixed clock; requests.post and both fetchers are fakes, so no network and no
Apify credit is used; never writes to data/ or logs/. Exit code 0 = every check passed.
"""
import asyncio
import hashlib
import logging
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = Path(tempfile.mkdtemp(prefix="fx-calendar-"))
# settings are read when server modules are imported, so configure first (.env never overrides these)
os.environ.update(DATABASE_URL=f"sqlite:///{(TMP / 'platform.db').as_posix()}", PRICE_FEED="demo",
                  COOKIE_SECURE="false", SECRET_KEY="calendar-test-" + "k" * 40, ANTHROPIC_API_KEY="",
                  ANTHROPIC_AUTH_TOKEN="", TELEGRAM_BOT_TOKEN="", CALENDAR_SOURCE="", APIFY_TOKEN="",
                  INVESTING_COUNTRIES="", INVESTING_FULL_HOURS="", INVESTING_TODAY_MINUTES="")

import server.engine as E                                       # noqa: E402
from server import settings as S                                # noqa: E402
from server.db import CalendarEvent, EngineState, SessionLocal, get_state, init_db  # noqa: E402
from server.feeds import calendar as C                          # noqa: E402

init_db()
FAILED: list[str] = []
PASSED: list[str] = []


def check(ok, label: str, detail=""):
    got = f"\n       got: {detail!r}"[:700] if not ok and detail != "" else ""
    print(f"{'ok  ' if ok else 'FAIL'} {label}{got}")
    (PASSED if ok else FAILED).append(label)


class Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(record.getMessage())


LOG = Records()
logging.getLogger("engine").addHandler(LOG)
logging.getLogger("engine").setLevel(logging.INFO)
logging.getLogger("engine").propagate = False

# a fixed clock for the engine: today is 2026-10-30, it is 12:00 UTC
NOW = datetime(2026, 10, 30, 12, 0)
E.utcnow = lambda: NOW
DAY = datetime(2026, 10, 30)


# ── the actor's rows ───────────────────────────────────────────
SAMPLE = {"type": "event", "eventId": 63, "occurrenceId": 556265, "date": "2026-09-16", "time": "2026-09-16T12:30:00Z",
          "country": "United States", "countryCode": "US", "countryId": 5, "currency": "USD",
          "event": "U.S. Core Retail Sales MoM", "shortName": "Core Retail Sales", "importance": "high",
          "category": "consumer", "actual": 0.5, "forecast": 0.4, "previous": -0.3, "revisedFrom": None, "unit": "%",
          "precision": 1, "referencePeriod": "Aug", "actualVsForecast": "bullish", "revisedVsPrevious": "neutral",
          "preliminary": False, "url": "https://www.investing.com/economic-calendar/core-retail-sales-63"}


def raw(occ, t: datetime, cur="USD", title=None, importance="high", actual=None, forecast=None, previous=None,
        unit="%", precision=1, **extra):
    return {"type": "event", "eventId": occ, "occurrenceId": occ, "time": t.isoformat() + "Z", "currency": cur,
            "event": title or f"event {occ}", "importance": importance, "actual": actual, "forecast": forecast,
            "previous": previous, "unit": unit, "precision": precision, **extra}


def iid(occ) -> str:
    return hashlib.sha1(f"investing|{occ}".encode()).hexdigest()


def test_parse():
    rows = C.parse_investing([SAMPLE])
    r = rows[0] if rows else {}
    check(len(rows) == 1 and r["time"] == datetime(2026, 9, 16, 12, 30) and r["currency"] == "USD"
          and r["title"] == "U.S. Core Retail Sales MoM" and r["impact"] == "High" and r["source"] == "investing",
          "the README row: UTC time, currency, title, High", r)
    check((r.get("actual"), r.get("forecast"), r.get("previous")) == ("0.5%", "0.4%", "-0.3%"),
          "figures carry their unit and precision", r)
    check(r.get("id") == iid(556265), "the id comes from the occurrence id (a moved release keeps its row)", r)

    t = datetime(2026, 10, 30, 12, 30)
    rows = C.parse_investing([
        raw(1, t, actual=215, unit="K", precision=0),
        raw(2, t, actual=1.482, unit="M", precision=3, importance="medium"),
        raw(3, t, actual="3.2%", forecast="3.1", unit="%", importance="low"),
        raw(4, t, actual=0.3, precision=None, importance="??"),
        {**raw(5, t), "time": "2026-10-30T14:30:00+02:00", "event": None, "shortName": "Short name"},
        {**raw(6, t), "type": "holiday"},
        {**raw(7, t), "time": "2026-10-30"},                     # all day / tentative: no minute
        {**raw(8, t), "currency": None},
        raw(1, t),                                               # the same occurrence again
        "not a row", None,
    ])
    got = {r["id"]: r for r in rows}
    check([r["id"] for r in rows] == [iid(n) for n in (1, 2, 3, 4, 5)], "holidays, rows without a minute or a "
          "currency, repeats and junk are left out", [r["title"] for r in rows])
    check(got[iid(1)]["actual"] == "215K" and got[iid(2)]["actual"] == "1.482M" and got[iid(2)]["impact"] == "Medium",
          "K / M units, Medium", (got[iid(1)], got[iid(2)]))
    check(got[iid(3)]["actual"] == "3.2%" and got[iid(3)]["forecast"] == "3.1%" and got[iid(3)]["impact"] == "Low",
          "a figure that already carries its unit is not given it twice", got[iid(3)])
    check(got[iid(4)]["actual"] == "0.3%" and got[iid(4)]["impact"] == "Low" and got[iid(4)]["previous"] == "",
          "no precision, unknown importance -> Low, no value -> ''", got[iid(4)])
    check(got[iid(5)]["time"] == datetime(2026, 10, 30, 12, 30) and got[iid(5)]["title"] == "Short name",
          "a time with an offset becomes UTC; the short name stands in for a missing title", got[iid(5)])
    check(C.parse_investing({"error": "x"}) == [] and C.parse_investing(None) == [], "a non-list answer -> no rows")


def test_fetch():
    calls, answers = [], []

    class Resp:
        def __init__(self, status, data, text=""):
            self.status_code, self.data, self.text = status, data, text

        def json(self):
            return self.data

    def post(url, params=None, json=None, headers=None, timeout=None):
        calls.append(SimpleNamespace(url=url, params=params, json=json, headers=headers, timeout=timeout))
        return answers.pop(0)

    real = C.requests
    C.requests = SimpleNamespace(post=post)
    try:
        answers.append(Resp(200, [SAMPLE]))
        rows = C.fetch_investing("tok-123", ["US", "EU"], date(2026, 10, 30), 7, "high_medium")
        c = calls[-1]
        check(c.url == "https://api.apify.com/v2/acts/kibaale~investing-economic-calendar-scraper/"
                       "run-sync-get-dataset-items", "the kibaale actor, run synchronously", c.url)
        check(c.headers == {"Authorization": "Bearer tok-123"} and "tok-123" not in c.url + repr(c.params),
              "the token travels in the Authorization header only", (c.headers, c.params))
        check(c.json == {"startDate": "2026-10-30", "days": 7, "countries": ["US", "EU"], "importance": "high_medium",
                         "includeExchangeHolidays": False, "includeDescriptions": False}, "the actor input", c.json)
        check(len(rows) == 1 and rows[0]["source"] == "investing", "rows come back parsed", rows)

        answers.extend([Resp(200, []), Resp(200, [])])
        C.fetch_investing("t", ["US"], date(2026, 10, 30), 0, "high")
        C.fetch_investing("t", ["US"], date(2026, 10, 30), 500, "high")
        check([c.json["days"] for c in calls[-2:]] == [1, 90], "days are kept to the actor's 1..90",
              [c.json["days"] for c in calls[-2:]])

        for status, data, want in ((402, None, "Apify HTTP 402"), (200, {"error": "x"}, "unexpected Apify answer"),
                                   (200, [{"foo": 1}], "looks like a calendar event")):
            answers.append(Resp(status, data, text="Payment required" if status == 402 else ""))
            try:
                C.fetch_investing("t", ["US"], date(2026, 10, 30), 1)
                err = ""
            except RuntimeError as e:
                err = str(e)
            check(want in err, f"raises on: {want}", err)
        n = len(calls)
        try:
            C.fetch_investing("", ["US"], date(2026, 10, 30), 1)
            err = ""
        except RuntimeError as e:
            err = str(e)
        check("APIFY_TOKEN" in err and len(calls) == n, "no token: no call at all", err)
        answers.append(Resp(200, []))
        check(C.fetch_investing("t", ["US"], date(2026, 10, 30), 1, "high") == [], "an empty answer is a quiet day")
    finally:
        C.requests = real


# ── the engine ─────────────────────────────────────────────────
INV, FF, ANSWERS = [], [], []


def fake_investing(token, countries, start, days, importance, actor):
    INV.append(SimpleNamespace(token=token, countries=countries, start=start, days=days, importance=importance,
                               actor=actor))
    r = ANSWERS.pop(0)
    if isinstance(r, Exception):
        raise r
    return r


def ff(i, t, impact="High"):
    return {"id": f"ff-{i}", "time": t, "currency": "USD", "title": f"ff {i}", "impact": impact, "forecast": "",
            "previous": "", "actual": "", "source": "forexfactory"}


FF_WEEK = [ff("past", NOW - timedelta(days=1)), ff("today", NOW + timedelta(hours=1)),
           ff("tomorrow", NOW + timedelta(days=1)), ff("later", NOW + timedelta(days=3), "Low")]


def fake_ff():
    FF.append(1)
    return [dict(e) for e in FF_WEEK]


E.fetch_investing, E.fetch_forexfactory_week = fake_investing, fake_ff


def run(eng, now):
    asyncio.run(eng.refresh_calendar(now=now))


def rows() -> dict[str, CalendarEvent]:
    with SessionLocal() as db:
        return {r.id: r for r in db.query(CalendarEvent).all()}


def reset():
    with SessionLocal() as db:
        db.query(CalendarEvent).delete()
        db.query(EngineState).delete()
        db.commit()
    INV.clear(), FF.clear(), ANSWERS.clear()


def use(source: str, token: str = "tok"):
    E.CALENDAR_SOURCE, E.APIFY_TOKEN = source, token


# the Investing.com week: 1 released at 11:55 without its figure yet, 2 Medium, 3 High later today, 4 a talk that
# never has a figure, 5 tomorrow, 6 in two days, 7 High later today
WEEK = [raw(1, NOW - timedelta(minutes=5), "USD", "U.S. CPI MoM", forecast=0.3, previous=0.2),
        raw(2, NOW + timedelta(hours=2), "EUR", "German Ifo", "medium", forecast=88.0, previous=87.7),
        raw(3, NOW + timedelta(hours=3), "GBP", "BoE Rate", forecast=4.0, previous=4.25),
        raw(4, NOW - timedelta(minutes=6), "USD", "Fed Chair Powell Speaks", unit=None),
        raw(5, NOW + timedelta(hours=13), "JPY", "BoJ Rate", forecast=0.5, previous=0.5),
        raw(6, NOW + timedelta(days=2), "USD", "Retail Sales", "medium", forecast=0.2, previous=0.1),
        raw(7, NOW + timedelta(hours=4), "CAD", "Employment Change", forecast=20.0, previous=15.0, unit="K")]


def test_defaults():
    check(S.CALENDAR_SOURCE == "forexfactory" and S.APIFY_TOKEN == "", "default source: ForexFactory, no token")
    check(S.INVESTING_COUNTRIES == ["US", "EU", "DE", "GB", "JP", "CH", "CA", "AU", "NZ"]
          and S.INVESTING_FULL_HOURS == 12 and S.INVESTING_TODAY_MINUTES == 60, "Investing.com defaults",
          (S.INVESTING_COUNTRIES, S.INVESTING_FULL_HOURS, S.INVESTING_TODAY_MINUTES))


def test_forexfactory_default():
    reset()
    use("forexfactory")
    eng, t0 = E.Engine(), 1_000_000.0
    run(eng, t0)
    check(len(FF) == 1 and not INV and set(rows()) == {e["id"] for e in FF_WEEK}, "default: the ForexFactory week")
    check(eng.status.get("calendar", {}).get("source") == "forexfactory", "status names the source", eng.status)
    run(eng, t0 + 600)
    run(eng, t0 + 1801)
    check(len(FF) == 2 and not INV, "ForexFactory every 30 minutes, Investing.com never asked", (len(FF), len(INV)))


def test_investing_runs():
    # the ForexFactory rows of the previous test are still there: the first Investing.com week replaces them
    use("investing")
    eng, t0, ff_before = E.Engine(), 2_000_000.0, len(FF)
    ANSWERS.append(C.parse_investing(WEEK))
    run(eng, t0)
    c = INV[-1] if INV else None
    check(c and (c.start, c.days, c.importance) == (DAY.date(), 7, "high_medium") and c.token == "tok"
          and c.countries == S.INVESTING_COUNTRIES and c.actor == "kibaale~investing-economic-calendar-scraper",
          "first run: the next 7 days, High + Medium", c)
    have = rows()
    check({iid(n) for n in range(1, 8)} <= set(have) and "ff-past" in have
          and not {"ff-today", "ff-tomorrow", "ff-later"} & set(have),
          "upcoming ForexFactory rows are replaced, past ones stay (the guard's history)", sorted(have))
    check(len(FF) == ff_before, "ForexFactory is not asked while Investing.com answers", len(FF) - ff_before)
    with SessionLocal() as db:
        check(get_state(db, "calendar_investing_week") == t0, "the week run is remembered across restarts")
    check(eng.status["calendar"]["source"] == "investing" and eng.status["calendar"]["releases"] == 7,
          "status: Investing.com, 7 releases", eng.status["calendar"])
    check(any("Investing.com week run, 7 releases" in m for m in LOG.lines), "the run is logged", LOG.lines[-3:])

    n = len(INV)
    run(eng, t0 + 60)
    check(len(INV) == n, "nothing due a minute later: no paid run", len(INV))

    # 4 minutes on: #1 (High, a figure due, released 5 min ago) still has no actual -> today's High releases again
    ANSWERS.append(C.parse_investing([{**WEEK[0], "actual": 0.4}, WEEK[2], WEEK[3]]))
    run(eng, t0 + 300)
    c = INV[-1]
    have = rows()
    check((c.start, c.days, c.importance) == (DAY.date(), 1, "high"), "a release without its figure: today's High "
          "releases are asked again", c)
    check(have[iid(1)].actual == "0.4%", "the figure is filled in", have[iid(1)].actual)
    check(iid(7) not in have, "a High release of today the answer no longer has is deleted (rescheduled)", sorted(have))
    check(iid(2) in have and iid(5) in have and iid(6) in have,
          "Medium releases and other days are not touched by a High-only run of today")

    n = len(INV)
    run(eng, t0 + 400)
    check(len(INV) == n, "no new run while nothing printed and the hourly check is not due", len(INV))
    ANSWERS.append(C.parse_investing([{**WEEK[0], "actual": None}, WEEK[2]]))
    run(eng, t0 + 300 + 3600)
    check(len(INV) == n + 1 and (INV[-1].days, INV[-1].importance) == (1, "high"), "today's High releases every hour",
          INV[-1])
    check(rows()[iid(1)].actual == "0.4%", "a figure once printed is never blanked by a later answer",
          rows()[iid(1)].actual)
    check(iid(4) in rows() and rows()[iid(4)].actual == "", "a talk (no figure due) never triggers a run")

    # a restart: the week stays (12 h), only today's High releases are asked
    eng2 = E.Engine()
    ANSWERS.append([])
    run(eng2, t0 + 7200)
    check((INV[-1].days, INV[-1].importance) == (1, "high") and iid(6) in rows(),
          "after a restart the recent week stands: only today's run, an empty answer changes nothing", INV[-1])
    eng3 = E.Engine()
    ANSWERS.append(C.parse_investing(WEEK))
    run(eng3, t0 + 12 * 3600 + 1)
    check((INV[-1].days, INV[-1].importance) == (7, "high_medium"), "after 12 h a restart asks for the week again",
          INV[-1])


def test_fallback():
    reset()
    use("investing")
    eng, t1 = E.Engine(), 3_000_000.0
    ANSWERS.append(RuntimeError("Apify HTTP 403: blocked"))
    run(eng, t1)
    check(len(INV) == 1 and len(FF) == 1 and "ff-today" in rows(), "Investing.com fails at the start: the "
          "ForexFactory week right away", (len(INV), len(FF)))
    check(any("Investing.com, week run) failed: Apify HTTP 403" in e["message"] for e in eng.status["errors"]),
          "the failure is reported", eng.status["errors"])
    ANSWERS.append(RuntimeError("still blocked"))
    run(eng, t1 + 300)
    check(len(INV) == 2 and len(FF) == 1, "the next run fails too: ForexFactory keeps its 30 minutes",
          (len(INV), len(FF)))
    ANSWERS.append(RuntimeError("still blocked"))
    run(eng, t1 + 1801)
    check(len(INV) == 3 and INV[-1].days == 7 and len(FF) == 2, "the week is retried after 15 min, ForexFactory "
          "refreshed", (len(INV), len(FF)))
    ANSWERS.append(C.parse_investing(WEEK))
    run(eng, t1 + 2800)
    have = rows()
    check(iid(3) in have and not {"ff-today", "ff-tomorrow", "ff-later"} & set(have) and "ff-past" in have,
          "Investing.com back: its week replaces the upcoming ForexFactory rows", sorted(have))
    ANSWERS.append([])
    run(eng, t1 + 2800 + 1900)
    check(len(FF) == 2, "and ForexFactory is no longer asked", len(FF))

    ok = eng.cal["ok"]
    ANSWERS.extend([RuntimeError("down")] * 3)
    run(eng, ok + 2 * 3600)
    check(len(FF) == 2 and iid(3) in rows(), "a failure 2 h after the last answer: the Investing.com rows stay",
          (len(FF), sorted(rows())))
    run(eng, ok + E.INVESTING_STALE + 60)
    have = rows()
    check(len(FF) == 3 and "ff-today" in have and iid(3) not in have, "nothing from Investing.com for 6 h: "
          "ForexFactory takes the upcoming rows over", (len(FF), sorted(have)))

    reset()
    eng = E.Engine()
    ANSWERS.append([])
    run(eng, 4_000_000.0)
    check(len(FF) == 1 and any("no releases at all for the next 7 days" in e["message"] for e in eng.status["errors"]),
          "an empty week is a failure (blocked actor), not a quiet week", eng.status["errors"])


def test_no_token():
    reset()
    use("investing", token="")
    eng = E.Engine()
    run(eng, 5_000_000.0)
    run(eng, 5_000_000.0 + 1801)
    msgs = [e["message"] for e in eng.status["errors"] if "needs APIFY_TOKEN" in e["message"]]
    check(len(FF) == 2 and not INV and len(msgs) == 1, "investing without a token: ForexFactory, said once",
          (len(FF), len(INV), msgs))


def test_save_calendar():
    reset()
    other = {"id": "manual", "time": NOW + timedelta(hours=5), "currency": "USD", "title": "manual", "impact": "High",
             "forecast": "", "previous": "", "actual": "", "source": "test"}
    rows_in = C.parse_investing([raw(10, NOW + timedelta(hours=1)), raw(11, NOW + timedelta(hours=6)),
                                 raw(12, datetime(2026, 10, 31))])            # 12: the next day's 00:00
    E.Engine.save_calendar(rows_in)
    E.Engine.save_calendar([other])
    E.Engine.save_calendar(C.parse_investing([raw(10, NOW + timedelta(hours=1))]),
                           window=(DAY, DAY + timedelta(days=1)), impacts=("High",), replaces=("forexfactory",))
    have = rows()
    check(iid(10) in have and iid(11) not in have, "inside the window: a missing release of the source goes")
    check(iid(12) in have, "the window's end is excluded: the next day's 00:00 release stays")
    check("manual" in have, "rows of other sources stay unless the fetch replaces their source")
    E.Engine.save_calendar([])
    check(set(rows()) == set(have), "an empty fetch changes nothing")


if __name__ == "__main__":
    t0 = time.time()
    for fn in (test_parse, test_fetch, test_defaults, test_forexfactory_default, test_investing_runs, test_fallback,
               test_no_token, test_save_calendar):
        print(f"── {fn.__name__}")
        try:
            fn()
        except Exception as exc:                                # a crash is a failure, the next test still runs
            import traceback
            traceback.print_exc()
            check(False, f"{fn.__name__} crashed", exc)
    print(f"\n{len(PASSED)} passed / {len(FAILED)} failed  ({time.time() - t0:.1f} s)")
    sys.exit(1 if FAILED else 0)
