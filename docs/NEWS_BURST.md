# News burst mode — implementation spec

A one-button mode for all clients. Before each high-impact economic release the client's connector places
a **straddle** on the pair that the release moves: N buy-stop orders above the price and N sell-stop orders
below it, all with the same stop-loss and **staggered take-profits**. The spike triggers one side; the
other side is cancelled at once (OCO). Each filled position is closed after a short **hold time**
(5 s – 10 min, usually under a minute) unless its TP or SL hits first; since v1.3.0 the hold is two times, X and Y
(§6 Hold X / Y): from X on a position in profit is closed, at Y any position. Unfilled orders are cancelled a
short time after the release.

User decisions (fixed, do not change):
- both directions (straddle), same pair, staggered exits;
- the client's `risk_per_trade_pct` is the budget for the **whole group**, split over the tickets;
- available to all clients (own switch in Settings + a big ON/OFF button on the dashboard);
- up to 40 trades per burst.

Hard rules that stay in force: demo-only unless the client set `allow_real_account = yes`; a **test**
burst is demo-only, always; broker credentials never reach the server; investing.com is never scraped
(events come from the ForexFactory feed already in `CalendarEvent`); every untrusted string in the web UI
goes through `esc()`.

**v1.3.0** adds, each needing connector 1.3.0 (§3 Version gating):
- the stop in pips, % of balance or $ (`sl_unit`, §6 Stop in money);
- hold X / Y (`max_hold_seconds`, §6 Hold X / Y);
- the pre-news analysis, or only its shadow (`analysis`, §6 Pre-news analysis);
- several pairs per currency, GBPJPY and XAUUSD among them, with the pip settings scaled per symbol (§1);
- Rapid news runs (`kind` "rapid", [RAPID.md](RAPID.md)), planned first on their pairs;
- stop-limit orders and new broker reads (§6 Broker additions).

A burst block stored before v1.3.0 has none of the new keys and is read as v1.2.0 ran it (`burst.legacy_burst`: the
stop in pips, no analysis, Y = X); no stored setting is rewritten. Only a user created from v1.3.0 on starts with the
new defaults (stop in % of balance, Y = 120 s, analysis on), and the burst itself is still off for them.

---

## 1. Settings

### Per user — `DEFAULT_USER_SETTINGS["burst"]` (server/settings.py)
| key | default | clamp (server PUT) | meaning |
|---|---|---|---|
| enabled | false | bool | the one button |
| trades | 10 | 1 – app `burst.max_trades` (≤ 40), int | positions per burst (orders placed = 2 × trades) |
| lead_seconds | 60 | 10 – 600, int | pending orders are placed this long before the release |
| offset_pips | 7.0 | 1 – 100, float | distance of the stop orders from the current bid/ask |
| sl_pips | 10.0 | 2 – 200, float | stop-loss distance from each order price (same for all) |
| tp_start_pips | 4.0 | 1 – 500, float | TP of ticket 0 |
| tp_step_pips | 1.5 | 0 – 100, float | ticket i has TP = tp_start + i × tp_step |
| hold_seconds | 30 | 5 – 600, int | X: from this long after its fill a position in profit is closed (Y = X: every position closes at X, as up to v1.2.0) |
| expire_seconds | 90 | 10 – 1800, int | cancel unfilled orders this long after the release |
| currencies | [] | list ⊆ keys of `BURST_PAIRS` | empty = all currencies |
| symbols | [] | list ⊆ `BURST_SYMBOLS` (not a list: 400 `invalid_symbols`) | the pairs a release is traded on (below); empty = each currency's own pair |
| sl_unit | "pct" \* | pips \| pct \| usd (else 400 `invalid_unit`) | pips = `sl_pips` with `risk_per_trade_pct` as the budget, as up to v1.2.0; pct / usd = the stop set in money (§6) |
| sl_money | 2.0 \* | pct 0.1 – 10, usd 1 – 100 000, float | the most the whole run may lose at its stops (pct = % of balance); not used in pips |
| lots | 0.10 \* | 0.01 – 100, float | the lot of every order when the stop is set in money |
| max_hold_seconds | 120 \* | X – 1800, int; null removes it | Y: a position still open then is closed at market; missing = X |
| analysis | true \* | bool (else 400 `invalid_analysis`) | the pre-news analysis picks the sides; false = it only watches (shadow, §6) |

\* the default of a user created from v1.3.0 on. A block stored before v1.3.0 lacks these keys: `legacy_burst` fills
them in as v1.2.0 ran (`LEGACY_BURST = {"sl_unit": "pips", "analysis": False}`, `lots` 0.10, `sl_money` its unit's
default, no Y), never with the new users' defaults. A unit changed without an `sl_money` resets it to the new unit's
default (`SL_MONEY = {"pct": (0.1, 10.0, 2.0), "usd": (1.0, 100_000.0, 100.0)}`: 2 % must not become 2 $). Y under X
is raised to X, never swapped, also when a later PUT raises X (`raise_max_hold`); a Y sent as null removes it, so it
follows X again. A switch sent as a string ("false") is refused, never read as on.

`User.merged_settings()` must deep-merge `burst` (like `modes`) so new keys get defaults (through `legacy_burst`).

### App-wide — `DEFAULT_APP_SETTINGS["burst"] = {"enabled": True, "max_trades": 40}`
Admin kill switch + cap. `get_app_settings` must deep-merge `burst`. Admin `PUT /api/admin/engine`
accepts `{"burst": {"enabled": bool, "max_trades": 1..40}}`.

### Currency → pairs (server/burst.py)
```python
BURST_PAIRS = {"USD": ["EURUSD", "XAUUSD", "GBPUSD", "USDJPY"], "EUR": ["EURUSD"], "GBP": ["GBPUSD", "GBPJPY"],
               "JPY": ["USDJPY", "GBPJPY"], "AUD": ["AUDUSD"], "NZD": ["NZDUSD"], "CAD": ["USDCAD"], "CHF": ["USDCHF"]}
```
The first pair of each list is the currency's **own pair**, the single pair it had up to v1.2.0. `candidates()` gives
one candidate per (pair, release time) for every pair of the list in the app's `symbols`. The pair must also be in the
user's `symbols` picks when those are non-empty, and the currency in the user's `burst.currencies` when that is
non-empty.

Which pair a release's classic burst trades (`plan_for` → `pick_pairs`; one run per release = one currency's events
in one minute):
- `burst.symbols` picked: the first picked pair of that currency's list that no other run took;
- no picks: the currency's own pair, as up to v1.2.0. When a Rapid run took it, the next pair of the list that no
  run took and the user trades: the **classic fallback pair** (USD with Rapid on EURUSD: XAUUSD, or GBPUSD when
  Rapid took XAUUSD too; GBP with Rapid on GBPUSD, JPY with Rapid on USDJPY: GBPJPY). With picks, or with Rapid's
  news runs off, nothing changes: a fallback happens only to a burst without picks whose own pair Rapid holds. A
  connector older than 1.3.0 skips a fallback on XAUUSD or GBPJPY as outdated (§3 Version gating), as it skips
  Rapid's runs;
- a user who does not trade the own pair (and picked none) gets no burst at that release, as up to v1.2.0.

**Precedence with Rapid** (`plan_runs`): at each release Rapid's news runs are planned first (`rapid_plan`: the first
`rapid.max_runs` pairs of the list that Rapid may trade, [RAPID.md](RAPID.md)), then the classic burst on the pairs
left. Never two runs on one pair: the unique (user, symbol, event_time) holds for both kinds. With Rapid's news
trigger off the classic burst's runs are exactly `plan_for`'s, as before.

**Symbol scale** (`bot/symbols.py`). Every pip setting (offset, stop, TP start and step, the spread cap; Rapid's too)
is written for EURUSD-like volatility. On the pairs of `SYMBOL_SCALE = {"XAUUSD": 5.0, "GBPJPY": 2.0}` (matched on the
name without the broker's suffix) the connector multiplies it by the factor (`SymbolSpec.scale`, 1 on every other
pair), so 7 pips of offset are 35 on gold and 14 on GBPJPY. `pip_size()` is the one rule for server and connector:
0.1 for gold (0.0001 before v1.3.0, wrong for gold), 0.01 for JPY pairs, else 0.0001; a symbol's point is
`10 ** -digits` (2 digits for gold, 3 for JPY pairs, else 5). A skip for spread quotes the scaled cap with 2 decimals
on a scaled pair and the user's own number elsewhere, as v1.2.0 did. GBPJPY and XAUUSD are in the default app
`symbols`; an admin's stored list gets them once (`db.init_db`, engine_state flag `migr_130_symbols`, which records
the pairs it added so a rollback removes exactly those), and a pair the admin removes afterwards stays removed.

### Which events
`impact == "High"` and the title does NOT match (case-insensitive)
`speak|speech|press conference|conference|testif|hearing|minutes|meeting|summit|election|budget|holiday|auction|parliament`
(those have no single release moment). One burst per (user, symbol, event time): two qualifying events
that map to the same pair at the same minute collapse into one (keep the first title, join others with " + "
if convenient, not required).

---

## 2. Data model (server/db.py)

New table (created by `create_all`):
```python
class Burst(Base):
    __tablename__ = "bursts"
    id: int PK
    user_id: FK users.id ondelete CASCADE, index
    event_id: str(64) | None          # CalendarEvent.id; a test's: None, or the event its analysis reads (§5)
    event_title: str(200)
    currency: str(8) | None
    symbol: str(20)                   # base name, no broker suffix
    event_time: DateTime              # naive UTC = the release moment
    test: bool = False
    params: JSONText                  # snapshot sent to the connector (see §3)
    status: str(16) = "planned", index
    side: str(8) | None               # "buy" | "sell" | "both"
    orders: int = 0                   # pending orders placed
    fills: int = 0                    # positions opened
    pnl: float | None
    reason: str(300) | None
    created_at, updated_at: DateTime
    kind: str(8) = "burst"            # v1.3.0: burst (the classic straddle) | rapid (a Rapid news run)
    analysis: JSONText | None         # v1.3.0: the pre-news analysis the connector reported (§3), kept once
    UniqueConstraint(user_id, symbol, event_time)
    def to_dict(self)  # same style as UserTrade.to_dict (ISO + "Z")
```
Statuses: `planned` (row exists, connector has not confirmed) → `scheduled` (connector confirmed) →
`armed` (orders placed) → `triggered` (first fill) → final: `closed` (had fills) | `expired` (no fill) |
`skipped` (a pre-check failed; `reason` says which) | `cancelled` | `error`.
Final statuses never change again.

`UserTrade` gets `burst_id: int | None`. `init_db()` must add the column to existing databases:
after `create_all`, inspect `user_trades`; if `burst_id` is missing run
`ALTER TABLE user_trades ADD COLUMN burst_id INTEGER` (works on SQLite and PostgreSQL).
Deleting a user must also delete their bursts (follow how `UserTrade` rows are handled).
v1.3.0 adds `bursts.kind` (`VARCHAR(8) NOT NULL DEFAULT 'burst'`: v1.2.0 never names it, and its rows read as
classic bursts) and `bursts.analysis` (`TEXT`) the same way, each column checked on its own. A trade of a Rapid news
run is booked with strategy `rapid`, any other burst trade `burst` (`db.TRADE_STRATEGY`).

---

## 3. Server ⇄ connector messages (`/ws/connector`)

### server → connector
```jsonc
{"type": "burst", "burst": {
  "id": 12, "symbol": "EURUSD", "event_title": "Federal Funds Rate", "currency": "USD",
  "event_time": "2026-10-28T18:00:00Z",
  "seconds_until": 734.2,          // release − now at send time; the connector uses its own monotonic clock from here
  "test": false,
  "kind": "burst",                 // v1.3.0: "burst" | "rapid" (a Rapid news run, params as in RAPID.md)
  "context": {"expect": -0.76, "ai": null, "title": "Federal Funds Rate"},  // v1.3.0, only when there is one
  "params": {"trades": 10, "lead_seconds": 60, "offset_pips": 7.0, "sl_pips": 10.0, "tp_start_pips": 4.0,
             "tp_step_pips": 1.5, "hold_seconds": 30, "expire_seconds": 90, "lots": 0.1,
             "risk_pct": 1.0,          // user's risk_per_trade_pct when planned: the WHOLE group's budget (pips)
             "max_spread_pips": 3.0,   // user's max_spread_pips
             "sl_unit": "pips", "sl_money": 2.0, "analysis": false,    // v1.3.0
             "max_hold_seconds": 30}}} // v1.3.0: Y, always sent (X when the user has none)
{"type": "burst_cancel", "id": 12}
```
`welcome` gains `"bursts": [plan, ...]` = this user's bursts with status `planned`/`scheduled` and
`seconds_until > 3` (each plan built at send time, with its `context`; since v1.3.0 only those the connector's version
runs as planned, `needs_version`). `context` (`server/news_view.py` `context_for`, worked out once per release time
and pair by `Engine.burst_contexts`): `expect` = the release's forecast against its previous value turned into the
pair's direction (+ = the pair should rise; `EXPECT_PP` 0.25 pp, `EXPECT_POINTS` 2.0, `EXPECT_GAIN` 3.0 set how big a
change reads; a higher unemployment rate or jobless count is bad for its currency), `ai` = the newest AI bias for the
pair when under `AI_MAX_AGE_H` = 2 h old, `title` = the events' titles. A run whose context cannot be made is sent
without one.

### connector → server
```jsonc
{"type": "burst_update", "id": 12, "status": "scheduled|armed|triggered|closed|expired|skipped|cancelled|error",
 "side": "buy|sell|both|null", "orders": 20, "fills": 10, "pnl": -12.4, "reason": "text or null"}
{"type": "burst_update", "id": 12, "analysis": {"action": "both", "score": 0.21, "reason": "weak",
 "parts": {"trend": 0.4, "momentum": 0.2, "expect": null, "ai": null}, "shadow": false}}   // v1.3.0, no status
```
`analysis` (v1.3.0, once per run, never for a local Rapid run): what the pre-news analysis answered (§6), `shadow`
true when its switch was off. The server keeps it once (`row.analysis` still None) when it is a JSON object of at most
`ANALYSIS_MAX` = 2000 characters (no NaN), also on a run already final: a watch-only analysis may arrive after the
run ended. It is returned in `to_dict()` and shown in the burst history.
Only keys present are applied. Server: `id` must belong to this user; status must be one of the list;
ignore updates to a burst already in a final status; `reason` ≤ 300 chars; numbers via `_int`/`_float`.
After applying: push `{"type": "burst", "burst": row.to_dict()}` to the user's browsers and a
`connector_log` line for armed/triggered/final states.

Burst fills are reported with the existing trade messages, plus `burst_id`:
`trade_opened.trade = {ticket, burst_id, symbol, side, lots, entry_price, sl, tp, demo, opened_at}`
(`signal_id` absent/None). `_record_trade_open` stores `burst_id`. A burst trade's `trade_closed` also carries
`{burst_id, symbol, side, lots, entry_price, demo, opened_at}` as last reported: `_record_trade_close` books a
closed trade from it when it never saw the ticket opened (a runner that crashed first, a restart), provided the
burst is this user's; without a `burst_id` (or with another user's) an unknown ticket is ignored as before.

**Version gating (v1.3.0).** `burst.needs_version(params, kind, symbol)` is the oldest connector that runs a plan as
planned: `V130` = (1, 3, 0) for a `kind` other than "burst", a pair added in v1.3.0 (`new_pair`: XAU, or a pair of
`SYMBOL_SCALE`), an `sl_unit` other than pips, an `analysis` other than false, or Y above X (a value it cannot read
counts as new: an older connector would run it some other way); else `MIN_CONNECTOR` = (1, 1, 0), so a plan stored by
v1.2.0 still needs only 1.1.0. A connector older than that is never sent the plan: the burst is `skipped` with
`OUTDATED` (`connector out of date: download the new connector`) when it is planned, and again when it is delivered (a
connector downgraded since), and `POST /api/bursts/test` answers 400 `connector_outdated`. A connector before 1.3.0
also gets the settings without the new pairs (`pairs_for`, in its short-term pairs too) and without the `rapid` block
(`main.for_connector`). A 1.3.0 connector skips a plan whose `kind` it does not know ("unknown run kind") or whose
`sl_unit` / `analysis` it cannot read ("invalid plan (invalid_unit)", a switch sent as "false" never counts as on); a
key a plan lacks (one from a server before v1.3.0) gives the run v1.2.0 made.

Old connectors (version < 1.1.0, from the `auth` hello) cannot run bursts: the server marks such bursts
`skipped` with reason `connector out of date: download the new connector`. Connector `VERSION = "1.3.0"` (1.3.0: the
version gating above, Rapid ([RAPID.md](RAPID.md)), the stop in money, hold X / Y, the pre-news analysis, the scaled
pairs and stop-limit orders; 1.2.0: the short-term strategy ([SHORT_TERM.md](SHORT_TERM.md)); 1.1.5: `--stop-on-stdin-eof`, how the Forex Bot app stops it: once its stdin is closed it stops as the first Ctrl+C does (the third stops at once) — new signals are skipped (`the connector is stopping`) and new plans go unanswered, its running bursts end first (orders cancelled, positions closed, each close on its own, reported), what they left is swept every 0.5 s until nothing is (a result being reported when the stop comes, or whose read fails, is handed to the sweeper, never lost; with no burst ended and nothing waiting, one sweep is all and one that cannot run is only logged), a close that kept failing is tried once more, a trade being placed is let finish, and whatever is still left after 40 s is reported as an error; a burst that was only `scheduled` gets no final status, so the server sends it again to the next start; 1.1.4: tickets.json is written whole or not at all; 1.1.1: broker clock proven, see below; 1.1.2: test signals capped at 0.1 % risk and demo only, netting-account skip when another position is open on the symbol, close retried 3×; 1.1.3: a failed close is retried for up to 10 min, also while the server is unreachable, large closes are split at the broker's maximum volume, burst trades whose close could not be checked are reported by the sweeper, test signals skip the price-drift check, a burst exit that went through only in part is finished by the next deal (never sent in full again), Close clicked again on a close being retried is answered again, close warnings wait for the server while it is unreachable, a netting burst counts what entered its position from the position's own deal history, never from the order's volume or a read of the position, and works out its slices only on a poll where that history adds up to the position as read and already shows what left it — any reduction an earlier read showed (a read that shows less is behind, however long it lasts), and what the connector closed itself, counted by volume without the fills of the burst's own orders (an opposite stop never stands in for a close still missing, nor does a close by hand or by another EA: a deal without this bot's magic number is never counted as ours; for 2 s after a close its own deals must also be among the history's, by their orders); of a close that raised, what went out before the error counts as ours at once (MT5Broker reports it; answers 10008, 10011, 10012 and 10028 count like no answer: they may have gone through), and a deal sent with no answer is waited for in history for up to 2 s before the slice is sent again (one of the two may be behind, or both behind our own deal; a read right after our own deal is not trusted for 0.5 s from when the deal returned): a split order executed late is reported, and its share of the slices already hit leaves at once, also after the last target; a stop on the other side that reverses the position — MT5 keeps its identifier — or a position reopened after going flat is reported as a new entry whose slices start again from 1; a whipsaw between two reads reports only what entered; a fill whose position is not listed yet, or a followed position missing from a few reads, is closed only once its history adds up to nothing (or after 30 polls), never taken as come and gone; one its history still holds after that, or does not show at all, is left to the sweeper, which closes it and reports it once — only when its history shows the close (or after 10 sweeps), so an earlier slice's exit is never taken as its result; a position the connector closes itself (the sweeper, the clean-up after a crash) is reported likewise only once its history adds up to nothing; a position that grew or reversed and closed between two polls is reported with what entered it last, before it is left to the sweeper and again by the sweeper with what its history shows by then; a fill that came and went between two polls is reported with what entered first at its average price, with the stops of an order on that side, then with its last entry if it reversed (even back to the same side); a fill whose deals reach history late is asked for for 30 polls too; the clean-up after a crash first reports a position its runner never took up; the sweeper reports a position whose close raised once its history shows it closed; an order placed with no answer (the reply lost) is followed by its comment `fxb:<id>` as the book shows it, after a failed placement and on every poll, and a position is the burst's by that comment too; the clean-up after a crash looks up in history each order that left the book unseen (filled, or its cancel found it gone) and reports a fill that came and went — its SL/TP hit while the terminal did not answer — opened and closed in the burst's result; while the terminal answers nothing at all those orders are left to the sweeper, which looks them up for up to 10 sweeps once the clean-up is over; with none placed a burst gives up only once a look at the book shows none of its orders, and the clean-up looks for them too; an orphan's `trade_closed` carries its last entry as its history shows once flat, not what an earlier sweep saw; a sweep that removed nothing sends no "Cleaned up" message (the failures are in the log); a burst trade's `trade_closed` carries its burst). Server: a burst's P&L is that of its closed trades — at its final report, when a trade closes after the burst ended (by the sweeper; pushed to the browsers again), and when the burst is marked `error` for want of a final report, which a final report that comes later replaces; a burst trade the server never saw opened is booked from its `trade_closed`.

---

## 4. Server engine — `burst_tick` (every 2 s, new task in `Engine.start`, first_delay 5)
1. **Reconcile** every non-final burst:
   - app `burst.enabled` false, or the user disabled `burst.enabled`, or the user is banned / inactive →
     `planned` → `cancelled` directly; `scheduled|armed|triggered` → send `burst_cancel` once (remember ids
     in memory) and let the connector report the final state.
   - non-test `planned|scheduled` whose `CalendarEvent` is gone or now has a different time → same cancel,
     reason `event rescheduled`.
   - `planned` whose release has passed → `skipped`, reason `connector was offline`.
   - `scheduled|armed|triggered` with now > event_time + `run_seconds(params, kind)` + 300 s → `error`,
     reason `no final report from the connector` (`burst.TIMED_OUT`); `run_seconds` = `expire_seconds` +
     max(X, Y) (Y missing = X, so v1.2.0's expire + hold; a value it cannot read is the burst's, or Rapid's, default); its P&L is that of its trades the
     connector reported closed. This `error` is the server's own guess: a final report that comes later
     replaces it (status, side, fills, P&L; reason cleared), once. A later report that is not final changes
     nothing and is answered with `burst_cancel`, as for any burst already final.
2. **Plan** (only when app burst enabled): qualifying events with now < time ≤ now + 15 min; for every
   eligible user (subscription active, not banned, `burst.enabled` or Rapid's news runs on, connector online, pair
   allowed for them) create a `planned` row if none exists for (user, symbol, event_time). The runs are
   `plan_runs`' (§1: Rapid's first, `kind` "rapid", then the classic burst's on the pairs left). `params` = user's
   burst settings (trades clamped to app max) + `risk_pct` + `max_spread_pips` (`params_for`; a Rapid run's are
   `rapid_params`). Connector version under `needs_version` (under 1.1.0 for a v1.2.0 plan) → `skipped`.
3. **Deliver**: every `planned` row whose user's connector is online and `seconds_until > 3` → send the
   plan with its `context` (resent each tick until the connector answers `scheduled`; the connector ignores
   duplicates); one whose connector is now too old for it is `skipped` instead.
Every status change goes to the user's browsers as `{"type": "burst", ...}`.

Shared pure helpers live in **server/burst.py** (no FastAPI imports): `BURST_PAIRS`, `event_ok(title)`,
`candidates(events, app_settings)` → list of `{event_id, time, currency, title, symbol}` deduped by
(symbol, time), `allowed_for(user_settings, cand, app_symbols)`, `clamp_burst(body, max_trades)`,
`plan_payload(burst, now)`, `FINAL = {...}`. Since v1.3.0 also `BURST_SYMBOLS`, `pick_pairs`, `plan_for`,
`rapid_plan`, `plan_runs`, `wants(user_settings, kind)`, `legacy_burst`, `params_for(…, kind)`, `rapid_params`,
`needs_version`, `new_pair`, `pairs_for`, `raise_max_hold`, `run_seconds`, and `plan_payload(burst, now, context)`.

---

## 5. REST API (server/main.py)
- `GET /api/me/settings` → also `"burst_app": {"enabled": bool, "max_trades": int}`.
- `PUT /api/me/settings` → accepts `"burst": {partial}`; clamps per §1; unknown keys ignored.
- `GET /api/bursts` (active user) → `{"bursts": [last 50 of this user, newest first],
  "upcoming": [next ≤ 10 qualifying events within 7 days that this user's settings allow:
  {event_id, time, currency, title, symbol, kind}], "app": {"enabled", "max_trades"},
  "connector": {"online": bool, "demo": bool|null, "version": str|null}}`. `upcoming` is `plan_runs`' answer
  (v1.3.0: `kind` "burst" | "rapid"); with Rapid's news runs off the classic burst's are listed even while it is
  switched off, as up to v1.2.0 (what it would trade).
- `POST /api/bursts/test` body `{"symbol": "EURUSD", "offset_pips": 2, "event_id": "…"}` (offset optional, clamp
  1–100; `event_id` optional, ≤ 64 chars, v1.3.0: a calendar event whose forecast the pre-news analysis reads):
  requires app burst enabled, connector online, connector account `demo == true`, connector ≥ 1.1.0 and ≥ what the
  test's params use on that symbol (`needs_version`), symbol in app symbols, `event_id` a known event, no other
  non-final test burst of this user; rate limit 3 per 5 min per user.
  Creates `Burst(test=True, event_id, event_title="Test burst", event_time=now+15 s, params = user's burst params with
  lead_seconds = 15 and the offset override)`, sends the plan at once (with the event's `context`, if any; the AI part
  only without an event), returns `to_dict()`.
  Errors: 400 `burst_disabled` / `connector_offline` / `demo_only` / `connector_outdated` /
  `invalid_symbol` / `invalid_event` / `test_running`.
- `POST /api/bursts/{id}/cancel` (owner only) → `planned` → `cancelled` now; `scheduled|armed|triggered` →
  send `burst_cancel`; final → 400 `burst_finished`.
- `PUT /api/admin/engine` accepts `burst` (see §1); the audit log records it like other engine changes.

---

## 6. Connector (connector/connector.py) — the burst runner

### Account modes: hedging AND netting (both must work)
`account_info().margin_mode`: 2 = retail hedging, 0 = retail netting, 1 = exchange (→ `skipped`,
"exchange accounts are not supported"). **The owner's own demo account is NETTING** (MEXAtlantic-Demo,
EURUSD: market execution, filling_mode 1 = FOK only, stops_level 5 points, freeze_level 10 points,
volume 0.01 / 0.01 / 30), so netting is the case that gets used first.

- **Hedging**: N separate positions per side; the staggered TPs sit on the orders, so the broker exits them.
- **Netting**: MT5 keeps ONE position per symbol (fills add to it; an opposite fill reduces/reverses it), and
  a filled order's SL/TP overwrite the position's. So:
  - place ONE buy-stop and ONE sell-stop, each with volume `n × lots` (split into several orders of
    ≤ `volume_max` only when needed), SL on the order, **no TP** (tp = 0). Comments `fxb:<id>:b` / `fxb:<id>:s`
    (`b1`, `b2`… for splits). 2 orders instead of 2N: faster to place and the OCO is a single cancel.
  - the position is `n` slices of `lots`. The connector does the staggered exits itself: on each poll, for
    every slice i not yet closed, when the bid (long) / ask (short) reaches entry ± (tp_start + i·tp_step)
    — entry = the position's actual `price_open` after the fill — close `lots` of the position (partial
    close: opposite DEAL with `position` = ticket, volume = lots). Several slices may close in one poll.
  - OCO is critical here (an opposite fill would shrink or flip the position): cancel the other side the
    moment a fill is seen.
  - hold time: close whatever is left of the position.
  - pre-check: a non-burst position already open on that symbol → `skipped` ("netting account: the open
    position on SYMBOL would merge with the burst").
  - reporting: one `trade_opened` for the position (total lots) and one `trade_closed` when it is gone
    (`closed_result` already sums every deal of the position). `fills` = slices closed + still open count
    is not needed — report `fills = n` once filled, and `orders` = orders placed.
- In both modes, while a burst on a symbol is armed or triggered, the normal `execute()` skips signals on
  that symbol ("news burst running on SYMBOL").
- Cancels can fail with `TRADE_RETCODE_FROZEN` (10029) when the price is inside the freeze level of the
  order: a failed cancel is retried on the next poll, never fatal. Same for a failed partial/hold close.

### Broker additions (bot/mt5_broker.py, wrapped 1:1 by `MT5Adapter`)
- `account()` adds `"margin_mode": "hedging" | "netting" | "exchange"` (from `margin_mode` 2 / 0 / 1).
- `close_position(pos, comment, volume=None, orders=None)` — `volume` closes only part of the position (netting
  slices), in deals of at most `volume_max`; `orders` gets the ticket of each deal's order (never 0);
  `MT5Adapter.close_position(pos, comment="", price=None, volume=None)` passes it through and returns them. A
  deal that fails raises `BrokerError` with `retcode`, `done` (what the deals before it closed) and `unsure`
  (what it sent when there was no answer, or an answer that leaves the outcome open: `UNSURE_RETCODES` 10008
  placed, 10011 error, 10012 timeout, 10028 locked; else 0).
- `positions()` adds `"identifier": p.identifier`.
- `place_pending(symbol, side, lots, price, sl, tp, comment, expire_in=None, kind="stop", limit_price=None) -> int`
  — `TRADE_ACTION_PENDING`, `ORDER_TYPE_BUY_STOP`/`SELL_STOP`, magic, prices rounded to `digits`. v1.3.0: `kind`
  "stop" (the default: the request v1.2.0 sent, unchanged) or "stop_limit" (`ORDER_TYPE_BUY_STOP_LIMIT` /
  `SELL_STOP_LIMIT`, types 6 / 7; Rapid's): `price` is the trigger and `limit_price` (`stoplimit` in the request) the
  limit the order turns into once triggered, on the pullback side. A stop-limit without `limit_price`, or with a buy's
  limit not below its trigger (a sell's not above), raises `ValueError`; a symbol that takes no stop-limits raises
  `BrokerError("stop-limit orders are not allowed on SYMBOL")` before anything is sent. There is no plain-stop
  fallback. The working filling mode is remembered per (symbol, kind). With `expire_in` (the runner passes
  release + expire_seconds + 120 s from now) and a symbol whose `expiration_mode & 4`: `ORDER_TIME_SPECIFIED`
  with `expiration` on the broker's clock (`_server_now`, see "Timestamps are UTC" below), so the broker drops
  the straddle even if the PC dies; retcode 10022 → GTC (logged), remembered per symbol. Filling: try
  `_filling(symbol)`; on retcode `TRADE_RETCODE_INVALID_FILL` (10030) retry once with
  `ORDER_FILLING_RETURN` and remember the working mode per symbol. Returns `res.order`.
- `pending_orders(symbol=None) -> list[dict]` (magic only): ticket, symbol, side, price, sl, tp, lots, comment;
  v1.3.0 adds `type` (MT5's own), `kind` ("stop" | "stop_limit" | "limit" | "market") and `price_stoplimit`. A
  waiting stop-limit has `price` = its trigger and `price_stoplimit` = its limit; once triggered the **same ticket**
  is a limit (type 6 → 2, 7 → 3, `kind` "limit"): `price` = the limit, `price_stoplimit` 0.0 (M0 demo result,
  [RAPID.md](RAPID.md)).
- v1.3.0: `order_state(ticket) -> dict | None` — where an order stands, from the book (`orders_get`) or else its
  history (`history_orders_get`): `{"state": "started" | "placed" | "partial" | "filled" | "canceled" | "expired" |
  "rejected", "type", "kind", "volume_initial", "volume_current", "price_open", "price_stoplimit"}`. None when it is
  in neither (history can lag behind the book) or a read failed: never proof that the order is gone.
- v1.3.0: `pending_room() -> int | None` — how many more pending orders the account may hold: `account_info()
  .limit_orders` minus `orders_total()` (this bot's orders and any other's); None when the broker sets no cap. A
  failed read raises.
- v1.3.0: `stop_limit_allowed(symbol) -> bool` — the symbol's `order_mode` has flag 8 (`SYMBOL_ORDER_STOP_LIMIT`).
- `margin_for(symbol, side, lots) -> float | None` (`order_calc_margin` at the ask / bid, already there for the
  short-term strategy) and `rates(symbol, timeframe, count)` (wrapped by `MT5Adapter` since v1.3.0) serve the v1.3.0
  runner: the margin cap of a money stop and of Rapid, the analysis' H1 / M1 bars.
- `cancel_order(ticket)` — `TRADE_ACTION_REMOVE`; an order that no longer exists is not an error.
- `deal_entry(ticket) -> dict | None` — from `history_deals_get(position=ticket)` the `DEAL_ENTRY_IN`
  deal: `{"price", "time", "lots", "side"}`; None when the order never filled.
- `position_deals(position) -> list[dict]` — the buys and sells booked on the position, oldest first:
  `{"side", "lots", "price", "order", "mine"}`; `mine` = the deal carries this bot's magic (a close by hand has
  0, another EA its own), so a netting burst never counts someone else's close as one of its slices.
- **Timestamps are UTC.** MT5 stamps positions and deals on the broker's server clock (MEXAtlantic: UTC+3,
  UTC+2 from 1 Nov). `tick()` learns the offset (`time_msc` − local UTC, rounded to 30 min, accepted only
  within −12 h…+14 h) from **proof only**: the same symbol's stamp moved forward on two looks in a row, each
  under 10 min after the one before (one move can be a reconnect delivering a tick from before gold's or an
  index's own break), and FX is open. The same stamp seen again within 2 s is the same look (`positions()` then
  `close_position()` in one poll), not a stall; v1 `live.poll_seconds` is capped at 300 s so its looks can prove. `_fx_closed()` follows New York: Friday 17:00 – Sunday 17:00 (21:00 or
  22:00 UTC with US DST), plus 25 Dec, 1 Jan and their eves from 21:00 UTC. Each new offset is appended to a
  history `[(broker time from, offset)]`; when the old offset was last proven before the latest weekend close
  (`_fx_last_close()`, Friday 17:00 New York) the change is that weekend's DST switch and the new entry starts
  at the close (on the old clock), so the first deals after the reopen, made before the proof, convert right.
  `_utc()` looks at a tick again when the proof is over 5 min old and returns **None** until an offset is
  proven, and also for a stamp made after the latest weekend close while the offset has not been proven since
  that close. It converts `positions()["open_time"]` (the raw stamp stays in `broker_time`), `closed_result()`
  exit time and `deal_entry()["time"]`. Callers wait instead of guessing: v1 `bot/live.py` adopts/closes trades
  on a later tick, `report_closed` retries while the exit time is None (after its retries it falls back to
  "now"). v1 strategies get `PositionView.open_time` on the broker clock, like `df["time"]` and the
  backtester, and count bars against `broker_time`; exits never wait for the offset. The expiry clock
  `_server_now` is `max(tick stamp, local + offset)` while the offset was proven in the last 5 min;
  otherwise local + offset only if it is 0–15 min ahead of the stamp, else the stamp. So neither a slow PC
  clock nor an offset an hour out of date (DST) moves the broker-side expiry. Known limit: positions opened
  before a DST weekend and still open when the connector is restarted after it convert with the new offset
  (display only: the connector reports fills with its own UTC clock).
`PaperBroker` does not get these: paper mode answers every burst with `skipped` ("paper mode: bursts need
MetaTrader 5").

### Lot sizing — `bot/risk.py`
```python
def burst_lots(balance, risk_pct, sl_pips, trades, spec) -> tuple[int, float]:
    """One risk budget for the whole group: if every ticket of ONE side hits its stop the loss is
    ≈ risk_pct of balance. Returns (n, lots_per_ticket). Lots round DOWN to volume_step. If that is under
    volume_min, n shrinks so each ticket can use volume_min; (0, 0.0) if even one minimum lot is too much."""
```
This is the `sl_unit` "pips" sizing (v1.2.0's): the stop is `sl_pips` × scale, the lots come from `risk_pct`. With the
analysis' "both" (reduced risk) it runs on `risk_pct / 2`.

### Stop in money — `sl_unit` pct | usd (v1.3.0, `Connector._burst_money`)
The user sets the lot (`lots`, every order) and the money the whole run may lose; the stop moves, not the lot:
- **Money per event**: `money_for(sl_unit, sl_money, balance)` = `sl_money` % of the balance (pct) or `sl_money` in the
  account currency (usd), capped at `MAX_RISK_PCT` = 10 % of the balance either way. A balance not above 0 → skipped
  "no money to risk: the balance is not above 0".
- **The stop**: `money_stop(money, lots, trades, spec, floor, cap)` puts it where `trades` orders of `lots` lose the
  money together (`money / (trades × lots × pip value)`), rounded **down** to the point.
- **Floor**: `stop_floor_pips(spec, spread, max_spread_pips × scale)` = max(stops level, freeze level, 3 × the larger
  of the spread now and its cap): a stop inside the spread's noise is taken out by the release's widening alone. A
  stop under the floor is the floor (rounded up to the point) and the **lots are cut** so the money still holds
  (down to the volume step); under `volume_min` → skipped "M at risk is too little for N × min lots with the closest
  stop allowed on SYM (F pips)", and a `lots` under the minimum → "L lots is under the minimum lot size of SYM (min)".
- **Cap**: `sl_pips`' top, 200 pips × scale; a stop past it is the cap and the run risks less than the money.
- **Reduced risk** (the analysis' "both") = **half the lots**, the stop kept (`_halved`: half the lots rounded down;
  under the minimum lot, fewer orders of the minimum lot; none fits → skipped "half the risk is under the minimum lot
  size"); the money is halved with them.
- Hedging: one ticket per order, so the lots are at most `volume_max` (netting splits the side instead).
- **Margin**: one side may take at most `Scalper.MARGIN_USE` = 0.5 of the free margin (`margin_for` of trades × lots);
  more is cut to the lots that fit, with a warning "News burst #id: the free margin allows X lots per order on SYM, not
  Y: smaller orders" (it then risks less); not even the minimum lot → skipped "not enough free margin for N × min lots
  of SYM". A margin MT5 cannot tell is no cap: the broker refuses an order it cannot cover anyway.
- **Slippage close**: a fill whose distance to its stop risks over `Scalper.RISK_SLACK` = 1.25 × its share of the money
  is closed at once (the stop is never moved), with a warning "#pid filled at X, too far from its stop (R at risk, S
  allowed): closed at once". Pips stops are left as v1.2.0 left them.

### Pre-news analysis (v1.3.0, bot/news_view.py; `analysis` true)
At the check (`lead_seconds` before the release), after the spread check, the runner reads the symbol's H1 bars
(`H1_BARS` = 120; asked for with the plan when it arrives at least `BARS_EARLY` = 5 s before the check) and M1 bars
(`M1_BARS` = 140), each waited for at most `ANALYSIS_WAIT` = 2 s (missing bars count as missing), and `analyse()`
weighs four parts, each in [−1, 1] (+ = up) or missing (counts 0): `trend` (H1 EMA 20 vs 50 in H1 ATRs, ≥ 60 closed
bars), `momentum` (the last 15 and 30 minutes on M1 in M1 ATRs, ≥ 45 rows), and the plan's `context` `expect` and `ai`.
Score s = 0.35 trend + 0.35 momentum + 0.15 expect + 0.15 ai (`WEIGHTS`). Answer (`decide`):
- **one side** ("buy" / "sell", full risk) when |s| ≥ `CLEAR` = 0.5 and at least `AGREE` = 2 parts point the way of s
  (|part| ≥ `QUIET` = 0.05); reason `clear`;
- **both** at reduced risk (half, above) when |s| is under 0.5 (`weak`) or too few parts agree (`alone`);
- **skip** when the last 10 minutes' range is over 3 × the median of the 12 before (`blow_out`): the run is skipped
  with that line as its reason. (`spread` is a skip too, but the runner checks the spread itself first.)
The line it logs and the server shows (`describe`): "Analysis: BUY clear (0.62): H1 trend +, momentum +", in Romanian
"Analiză: …" in the web app. After the bars the quote and the spread are read again, and the straddle is placed from
the price then. The answer goes to the server once (§3 `analysis`).

**Shadow mode** (`analysis` false on a v1.3.0 plan): once the orders are in (`armed`), the same analysis is worked out
beside the run, logged as "Analysis (watch only), would have: …" and sent with `shadow: true`; it never changes the
run, and a failure in it is a debug line only. The user can watch it before turning it on.

### Hold X / Y (v1.3.0, bot/hold.py `hold_action`)
X = `hold_seconds`, Y = `max_hold_seconds` (never under X; missing = X). Each filled position, counted from its fill
(on a v1.3.0 plan a netting position that reversed counts from the reversal), is closed:
1. from X on, as soon as it is in profit (P/L ≥ `min_profit`: one point on its lots) — "in profit after Xs";
2. at any age, when the release turned against it (`news_against`: the mid now back through the mid quoted just
   before the release, `pre_mid`; mid against mid, so the post-release widening alone never counts) — "turned against
   the release"; only while Y is above X;
3. at Y, whatever it shows — "max hold Ys".
Y = X (a v1.2.0 block, or Y never set above X) is v1.2.0's close at exactly X, in profit or not ("hold time"), with no
look at the release. The first answer is latched and that close retried until done. Its stop-loss and take-profit
stay at the broker all along. A close refused as frozen (10029, the position's SL/TP within the freeze level: the
broker's own is about to fire) is retried quietly and warned about only after `BURST_FROZEN` = 5 tries; a v1.2.0
plan warns at once, as before.

### Runner behaviour (one asyncio task per burst; all MT5 calls through `self.call`)
State: `self.bursts: dict[id, state]`, `self.burst_owned: set[ticket]` (orders/positions owned by a runner).
1. On `burst` message: duplicate id → re-send `scheduled` if still waiting, else ignore. New → clamp params
   to §1 ranges (defence in depth), compute `release = loop.time() + seconds_until`, store, reply
   `scheduled`, start the task. On `welcome`: schedule each plan in `bursts` the same way.
2. Sleep until `release − lead_seconds` (immediately if already past).
3. Pre-checks, each failure → `skipped` with a clear reason + `report(..., "warn")`:
   paper; dry-run (report what it would place, then skipped "dry-run"); account not demo and
   `allow_real` false; `test` and account not demo; exchange margin mode; netting with an open non-burst
   position on the symbol; Algo Trading off; daily loss limit hit (`RiskManager`); spread > max_spread_pips;
   `offset` or `sl` distance under `spec.min_stop_distance`; `burst_lots` gives n = 0. v1.3.0: every pip
   distance × the symbol's scale first; with the stop in money `_burst_money`'s skips instead of `burst_lots`' (§6
   Stop in money); with the analysis on its `skip` (§6 Pre-news analysis), and the spread checked again after it. A
   Rapid plan (`kind` "rapid") takes the same account checks, then its own sizing and daily limit
   ([RAPID.md](RAPID.md)) instead of the long-term daily loss limit and `burst_lots`.
4. Netting: place the two orders as described above, then `armed`. Hedging: place orders, **interleaved** b0, s0, b1, s1 …: buy-stop at `ask + offset`, sell-stop at
   `bid − offset` (prices taken once, before the loop), SL = order price ∓ sl_pips, TP_i = order price ±
   (tp_start + i·tp_step). Comment `fxb:<burst_id>:<i>` (≤ 31 chars). Add each ticket to `burst_owned`.
   Stop placing after 3 consecutive rejections. Zero placed → `error`. Then `armed` (orders = count).
   A placement that raised may still have reached the book (its reply lost): after any failure, and on every
   poll, an order in the book with this burst's comment and no ticket of ours is followed like a placed one.
   With none placed, the burst gives up only once a look at the book shows none of its orders (it retries a
   failed look for ≈ 5 s); an order found then is followed, or cancelled on a `burst_cancel`.
5. Monitor every 0.1 s until done:
   - An owned pending ticket that disappeared and that we did not cancel → it filled (or the broker removed
     it). Find the position by `identifier == ticket` (fallback `ticket`); if not open any more use
     `deal_entry(ticket)`; None → just drop it. A fill → `trade_opened` (with `burst_id`), remember
     `filled_at = loop.time()`.
   - First fill on a side → cancel every pending order of the **other** side immediately, then report
     `triggered` with that side. A fill on the other side anyway (whipsaw) → side `both`.
   - Open burst positions whose hold is over → close them (oldest first): at `loop.time() − filled_at ≥
     hold_seconds` on a plan with Y = X (v1.2.0's), else as `hold_action` answers (§6 Hold X / Y). A fill that
     slipped too far from a money stop is closed at once (§6 Stop in money).
   - v1.3.0, the analysis' one side: only that side's orders were placed, so there is no other side to cancel.
   - A burst position that is gone (TP/SL/our close) → `closed_result` (retry ≤ 3 × 0.3 s) →
     `trade_closed`, add to the burst's P&L.
   - `loop.time() ≥ release + expire_seconds` → cancel all remaining pending orders.
   - `burst_cancel` received → cancel all pending orders now; positions already open still run to their
     hold time; final status `cancelled`.
   - Done when nothing is pending and nothing is open after a fill, or after expiry with no fills.
6. Final `burst_update`: `closed` (fills, pnl) | `expired` (no fill) | `cancelled` | `error` (exception text).
   A crash inside the task must still cancel this burst's pending orders and close its positions
   (try/finally) and report `error`. A position of the burst its runner never took up (it crashed before a
   poll saw the fill) is reported first (`trade_opened`, as its deals show it when they add up to it), so its
   close is reported too; each close is reported once its history adds up to nothing. The clean-up first
   looks at the book for orders sent with no answer, so they are cancelled too. Orders that left the
   book unseen are looked up in history (`deal_entry`): a fill that came and went (its SL/TP hit meanwhile) is
   reported opened and closed; what the terminal does not answer here goes to the sweeper (`unchecked`), which
   looks at them only once this clean-up is over.

### Orphan sweeper
At start-up (after `broker.connect`, before connecting to the server) and every 30 s: cancel magic pending
orders whose comment starts `fxb:` and whose burst id is not in `self.bursts`; close magic positions with
an `fxb:` comment whose burst id is not in `self.bursts`. Report what it did. A position it closed (or one a
runner handed over, `unreported`) is reported closed only once its deal history adds up to nothing — up to
10 sweeps for one already gone — after bringing its `trade_opened` up to date with what that history shows.
A close that raises may have gone through all the same: the position is then reported like one already gone
(a close really refused leaves it listed, and the next sweep retries). The `trade_closed` of a position no
runner's trade stands for (a restart, a crash before its runner took it up) carries its burst as its deals show
it (`burst_id`, side, lots, entry price), so the server books it even without its `trade_opened`; those are
read from its history once flat (its last entry), not from what an earlier sweep saw. The sweeper also looks
up the orders a crashed burst's clean-up could not check (`unchecked`, up to 10 sweeps): a fill that came and
went is taken up for its burst and reported once its history shows it closed.

v1.3.0: a run's comments are read by one `run_tag(comment)` (`fx([brs]):<id>`): `fxb:` news runs (classic burst and
Rapid news runs), `fxr:` a Rapid run started on a short-term signal, `fxs:` a scalp (not the sweeper's). The sweeper
takes `fxb:` and `fxr:` orders and positions whose run is not in `self.bursts`. Its message names whose they were, by
the strategy of what it removed (`_swept_runs`): only classic-burst leftovers keep v1.2.0's text, "Cleaned up after
an earlier news burst: removed N pending order(s), closed M position(s)"; Rapid's alone give "Cleaned up after an
earlier Rapid run: …", both together "Cleaned up after an earlier news burst and Rapid run: …" (the dry-run line
"would remove … left by …" the same way). A Rapid news run's leftovers count as Rapid's across a restart too
(`rapid_news` in rapid_open.json).

### Existing paths must ignore burst positions
`execute()` counts only non-burst positions for `max_open_trades` and for "already have a position on
symbol" (burst = ticket in `burst_owned` or comment starts `fxb:`). `push_snapshot()` does not send
`trade_closed` for tickets in `burst_owned` (the runner reports those). Snapshot positions get
`burst_id` parsed from the comment. The regex `fx:(\d+)` must not match `fxb:` comments (it does not).

---

## 7. Web app (server/web)
- **Dashboard**: a "News burst" card near the top. One large ON/OFF button bound to `burst.enabled`
  (saves via `saveSettings({burst: {enabled}})`). Shows: state (ON/OFF, "disabled by admin" when
  `burst_app.enabled` is false, warning when the connector is offline or not ≥ 1.1.0), the next upcoming
  event it will trade (title · currency · pair · countdown), the live/most recent burst (status chip, side,
  orders, fills, P&L), and a **Test now (demo)** button → modal with pair select + offset pips
  (default 2) → `POST /api/bursts/test`; a Cancel button on a non-final burst.
- **Settings**: a "News burst" section with the switch, numeric fields for §1 params (with the clamps as
  min/max), currency chips, and a short plain-language explanation: the risk % in Settings is the budget
  for the whole group; ~0.1–0.3 s per order, so 40 trades need roughly 10–25 s to place (keep
  `lead_seconds` ≥ 30 for big bursts); spreads widen and stop orders can slip at news; on a netting
  account MT5 keeps one position per pair, so the bot opens one position split into N slices and closes
  the slices one by one at the staggered targets (on hedging accounts: N separate trades); demo first.
- **Market** (calendar): events in `upcoming` get a "⚡ burst · PAIR" chip.
- **Trades**: burst trades show a "⚡ #id" chip; a "Bursts" card lists recent bursts (time, event, pair,
  status, side, orders/fills, P&L).
- **v1.3.0 in Settings → News burst**: "Stop-loss in" pips | % of balance | $ (`sl_unit`) with the amount
  (`sl_money`) and "Lot per order" (`lots`), and a line with what the whole burst loses at its stops (≈ the money,
  capped at 10 % of the balance; with the connector offline in % only); the two hold times "Close if in profit after
  (s)" (X) and "Hold at most (s)" (Y; a Y the user clears is saved as null, so it follows X again, and the field
  shows X; one left alone is sent back as served, the short-term and Rapid cards alike); "Pairs at a release"
  (`symbols`: only the pairs the user trades are offered, each with the currencies whose releases trade it; none =
  each currency's own pair, or its fallback pair while Rapid holds that one); "Pre-news analysis picks the
  side" (`analysis`). A connector online and older than 1.3.0 while any of these is set (or Rapid is on) gets a
  banner: it skips those runs. The Test now modal has an optional "Event" (an upcoming release, `event_id`), and the
  bursts table a "Kind" column (Classic | Rapid) and the run's analysis line ("Analysis: …", "Analysis (watch only),
  would have …").
- **Admin → Engine**: a "News burst" card with the kill switch and max trades, saved with the rest.
- WS: `case "burst"` updates the dashboard card / refreshes Trades & Dashboard softly; toast on
  `triggered` and final states.
- All new strings in EN and RO (i18n.js). All server strings through `esc()`.

## 8. Tests (no pytest in the venv: plain scripts, `python tests/<file>.py`, exit code ≠ 0 on failure)
- `tests/test_burst_connector.py` — a `SimBroker` with the adapter API (scripted price path, hedging
  account, instant stop triggering at the broker, TP/SL hits, history) drives the real runner with a fake
  `send`: hedging — interleaved placement of 2N orders with the right prices/SL/staggered TPs, lot split,
  OCO cancel on first fill, hold-time close; netting (the SimBroker must merge fills into one position per
  symbol and apply partial closes) — two orders of n × lots without TP, slice-by-slice partial closes at
  the staggered levels from the real entry, OCO, remainder closed at hold time, skip when a non-burst
  position is open; both — `trade_opened`/`trade_closed` with `burst_id`, `burst_update` sequence, expiry
  with no fills, cancel, frozen-cancel retry, exchange/real/paper/dry-run skips, orphan sweeper,
  `execute()` skipping a symbol with a running burst, `burst_lots` edge cases; lost placement replies, closes by
  hand (magic 0), stale reads, and fills that came and went while the terminal did not answer (clean-up and
  sweeper); a late no-answer close that must not stand in for a later one; looks at the book that fail after
  placements with no answer; a sweep during a crashed burst's clean-up.
- `tests/test_burst_server.py` — `server/burst.py` helpers + an API/WS flow with FastAPI `TestClient`
  on a temporary SQLite database and `PRICE_FEED=demo`: settings clamps, `GET /api/bursts`, test-burst
  checks (offline, not demo, outdated), a fake connector over `/ws/connector` receiving the plan and sending
  `burst_update`/`trade_opened` with `burst_id`, reconcile/cancel, admin kill switch, `init_db` adding
  `burst_id` to an old `user_trades` table.
- v1.3.0: `tests/test_hold_money.py` (`hold_action`, `min_profit`, `news_against`, `money_for`, `money_stop`,
  `stop_floor_pips`), `tests/test_news_view.py` (both halves of the analysis), `tests/test_mt5_broker.py` (the plain
  stop sent as v1.2.0 sent it, stop-limits, `pending_orders`' new keys, `order_state`, `pending_room`, on a fake
  MetaTrader5 that answers as the M0 demo did), `tests/test_rapid.py` (Rapid, [RAPID.md](RAPID.md), the sweeper's
  wording too); the burst tests above gained the v1.3.0 keys (money stop, hold X / Y, analysis and shadow, frozen
  closes), a v1.2.0 block and plan run and reported word for word as before, the pairs per currency, Rapid's
  precedence and the version gating.
