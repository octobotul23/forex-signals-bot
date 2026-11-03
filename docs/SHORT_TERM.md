# Short-term and long-term strategies

Two strategies, each with its own settings:

- **Long-term** is the engine's `trend` mode: the server decides on daily (D1) bars and sends signals; the client's
  connector executes them with the client's usual settings (risk per trade, max open trades, daily loss limit).
- **Short-term** (scalping) runs inside the client's connector on live ticks: many quick trades, each closed after
  `hold_seconds` at the latest (since v1.3.0: in profit from `hold_seconds`, X, on, and at `max_hold_seconds`, Y, at
  the latest: §3 Hold X / Y), sized from a risk **range** by how strong the setup is, with its own daily loss limit.
  Since v1.3.0 its **Variant** is Classic (this one) or Rapid (many stop-limit orders at once,
  [RAPID.md](RAPID.md)).

User decisions (fixed, do not change):
- short-term hold 30 s by default, adjustable 10 – 300 s (X); since v1.3.0 a second hold Y, X – 600 s;
- short-term risk is a range in % of balance or in account currency; the setup's strength picks the point in it;
- short-term has a separate daily loss limit: when hit, only short-term stops until the next day;
- long-term: several variants were tested and the one chosen on the data is the default.

Hard rules that stay in force: demo-only unless the client set `allow_real_account = yes`; broker credentials never
reach the server; every untrusted string in the web UI goes through `esc()`.

---

## 1. What the research found (be honest with users about it)

**Short-term.** Every rule tested on one-second bars of the six pairs and gold (Sep 2025 – Oct 2026; chosen on the data
before June 2026, checked on June – October) lost money after the spread. The rule shipped (`bot/scalp.py` `RULE`, "fade
a stretch from the 10-minute average") lost the least while trading often: out of sample about **−0.05 R** a trade held
120 s and **−0.12 R** held 30 s; the same trades with a random side lost about as much, and the strength did not pick
out the better trades. It ships because it was asked for, **off by default**, with a warning in Settings: keep it on a
demo account; longer holds lose less.

**Long-term.** The EMA 20/50 crossover (stop 2 × ATR, target 3 × ATR, exit and reverse on the opposite cross), filled
realistically (first H1 open past the rollover, costs included), on 2023 – 2026 history:

| timeframe | mean R per trade (test) |
|---|---|
| H1 | −0.055 |
| H4 | −0.091 |
| **D1** | **+0.19** (n = 133, PF 1.41, t 1.26); realistic expectation +0.05 – +0.10 |

About 31 trades a year over 7 symbols, held about 22 days. Not proven (t < 2): roughly break-even to slightly positive.

## 2. Long-term on its own timeframe

`DEFAULT_APP_SETTINGS["modes"]["trend"]["timeframe"] = "D1"` while the engine reads H1 (`settings["timeframe"]`).
`engine.mode_timeframe(settings, mode)` returns the mode's own timeframe only when it is a known one **longer** than the
engine's; `""` (or a shorter one) means the engine's. Admins set it per mode in Admin → Engine
(`PUT /api/admin/engine {"modes": {"trend": {"timeframe": "D1"}}}`, 400 `invalid_timeframe` otherwise).

When it decides (`Engine._mode_bars`, asked at each H1 close):
- A D1 bar of a New-York-close broker closes at the daily rollover (17:00 New York), when spreads are widest. The
  decision waits out `ROLLOVER_QUIET` = 3 h (`feeds/prices.quiet_until`): it is made at the first H1 close outside
  17:00 – 20:00 New York (broker 03:00). Friday's bar is decided on Monday at that hour.
- A look at an H1 close before that time leaves the bar undecided (asked again at the next close).
- The bar decided on is **saved before the strategy runs** (`engine_state` key `mode_bars`, `mode|symbol|timeframe` ->
  bar open time; `Engine.load_mode_bars` at start, `Engine._decided`): a restart never decides it again, a crash in
  between loses at most that bar.
- A **late** decision is made too (a restart, a PC that was off at broker 03:00): at the first H1 close from the due
  time on, as long as the bar has been the newest for no more H1 closes than a D1 bar lasts (rollover closes are no
  chances, so a restart over the weekend still decides Friday's bar on Monday).
- A bar that stayed the newest longer than that (a newer one should be there: **stale prices**), or that was due before
  the oldest H1 bar the engine holds, is saved as decided and **skipped** with a warning.
- Live feeds do not even fetch the D1 bars until 2 D1 bars after the last one decided (no newer bar can have closed:
  saves API credits). A failing fetch is logged and skips only that mode and symbol for this tick.
- The demo feed has no rollover: its D1 bars are decided on at their close.

What it publishes (`Engine._run_strategies`):
- `price` = the newest H1 close (the price now); stop and target keep their distance from the D1 bar's close.
- `timeframe` = `"D1"`; `track_from` = the next H1 bar (SL/TP are tracked on H1 bars from then on).
- `meta.bar_time` = the open time of the D1 bar decided on (the regret check matches live signals to the replay by it).
- When an open signal's exit rule fires, it is closed (`closed`, "exit rule") and the same bar may open the next trade:
  the opposite cross is both the exit and the new entry. The replay (`learning.replay`) does the same. The exit rule
  counts `bars_held` as the D1 bars **after** `meta.bar_time`, as the replay does (older signals: from `track_from`).
- The mode's position is its open signal **on the timeframe it decides on**. A signal left open on another timeframe
  (the H1 trend signals from before the move to D1) keeps its stop and target, and is closed ("timeframe changed") when
  the mode enters anew on that symbol: one position per symbol.
- The event guard is checked at the decision time; the replay checks it there too (`History(defer=True)`, live feeds
  only: `Engine._defers`).

## 3. Short-term

### Per user — `DEFAULT_USER_SETTINGS["short_term"]` = `bot/scalp.py` `DEFAULTS`
Clamped by `scalp.clamp_short` on the server (Settings PUT) and again in the connector (so both agree).

| key | default | range | meaning |
|---|---|---|---|
| enabled | false | bool | the switch |
| risk_unit | pct | pct \| usd | unit of risk_min / risk_max |
| risk_min | 0.5 | 0.1 – 10 % or 0.1 – 100 000 | risk of the weakest setup taken (strength 0) |
| risk_max | 2.0 | same | risk of the strongest (strength 1); swapped with risk_min if smaller |
| hold_seconds | 30 | 10 – 300 | X: from this on a scalp in profit is closed (Y = X: every scalp still open then is closed at market, as up to v1.2.0) |
| max_hold_seconds | 120 \* | X – 600 (`MAX_HOLD_TOP`); null removes it | Y: a scalp still open then is closed at market; missing = X |
| loss_unit | pct | pct \| usd | unit of daily_loss |
| daily_loss | 5.0 | 0.5 – 50 % or 1 – 1 000 000 | the day's short-term loss that stops short-term until the next day (UTC) |
| max_open | 2 | 1 – 10 | scalps open at once (one per symbol) |
| max_spread_pips | 1.5 | 0.1 – 10 | no scalp opens on a wider spread |
| symbols | [] | pairs the engine trades | empty = all of them |

\* v1.3.0, not in `scalp.DEFAULTS`: a user created from v1.3.0 on starts with `NEW_USER_MAX_HOLD` = 120
(`DEFAULT_USER_SETTINGS["short_term"]`); a block stored under v1.2.0 has none and closes at X, as before. Y is
clamped once X is: under X it is raised to X, never swapped; a stored Y gone bad is left out (it follows X again), and
so is one sent as null (the user's way back to "Y follows X": the web app sends null when the "Hold at most (s)"
field is cleared, and shows X there).

Changing a unit without a new value resets that key to the unit's default (`UNIT_DEFAULTS`: 50 $ never becomes 50 %).
A $ risk is capped at `MAX_RISK_PCT` = 10 % of balance as well. A bad value is refused with 400 `invalid_enabled`,
`invalid_number`, `invalid_unit`, `invalid_symbols` (not a list) or `pairs_not_traded` (shown translated in the web
app, `e_<code>`).

**Pairs.** A PUT without `symbols` keeps the stored list. A list is filtered to the pairs the engine trades; a non-empty
list of which none is traded is refused (`pairs_not_traded`): it must never be stored as `[]`, which means every pair.
When the admin later removes every pair a user chose (`main.pairs_gone`), `main.for_client` adds
`"short_term_note": "pairs_gone"` and serves `symbols: []`: the connector gets short-term **off**, the web app the
stored switch (its banner says short-term does not trade until other pairs are chosen). The stored switch and pairs are
kept. The web app saves the whole short-term object with `symbols_picked`: true only when pairs were picked on that
page (that save, one still to go out or one on its way). Unpicked, its list is only what it was served (some pairs
removed, `[]` with all gone, or a page left open from before the admin's change), so `main.web_pairs_kept` keeps the
stored list, the removed pairs included, and editing any other field changes nothing else; `[]` while the pairs are
gone is kept even when picked (a pair picked and unpicked again: the web app does not mark it picked either, unless
that pick is already on its way: the server has it by then, and the empty list means every pair, as after its
answer). A body
without the flag (an older page) keeps the stored list when its list is the one served now. The flag itself is never
stored. Picking traded pairs stores them and, with the switch on, starts short-term again; the note goes and the
banner with it. After each save the pair buttons show the list the server kept (a page left open shows the newer
list), and an edit still to go out takes that list unless it picks pairs itself. A save that fails with no refusal (no
answer, a server error) goes out again with the next edit, which wins where both change the same field. A settings
load answered after a save was answered is older than that save: the page keeps the save's answer. A logout or login
on the tab starts a new session for the settings (`saveEpoch` in `app.js`): edits not sent yet are dropped, and an
answer (or a browser-notification prompt) from the session before is never applied to, or saved into, the next account.

### Sizing
`risk_money = risk_min + (risk_max − risk_min) × strength` (of balance for pct), capped by what is still free of the
daily limit after what the open scalps risk at their stops; `lots_for_money` rounds **down** to the volume step and
skips the trade (0 lots) when even the minimum lot would risk more. A scalp may use at most half the free margin
(`MARGIN_USE`): more lots than that allows are cut to what fits (it then risks less; the web app's connector log says
so), and none opens when even the minimum lot does not fit.

The risk booked is computed from the **fill**, not the quote: a slip moves the entry, never the stop. A scalp filled so
far from its stop that it risks over `RISK_SLACK` = 1.25 × its money is closed at once (reported as a warning).

### Hold X / Y and "against" (v1.3.0)
X = `hold_seconds`, Y = `max_hold_seconds` (`Scalper.holds()`; Y = X when none is set). Every step each open scalp is
checked by `bot/hold.py` `hold_action`, the rule the news burst and Rapid close by too:
1. from X on, it is closed as soon as it is in profit (P/L ≥ one point on its lots) — "short-term: in profit after Xs";
2. at any age, when the rule has turned **against** it — "short-term: turned against" — only while Y is above X;
3. at Y, whatever it shows — "short-term: max hold Ys".

"Against" (`scalp.scalp_against`, worked out each second only for the scalps whose Y is above X): the rule now opens a
trade the other way (`decide()`'s side this second), or the price has stretched `AGAINST_WIDEN` = 1.0 volatility
unit further from its 10-minute average than when the scalp opened (`decide()`'s `stretch` at the entry, kept with the
scalp, against `scalp.stretch()` now): a buy opened at −2.4 is against from −3.4 down, a sell opened at +2.4 from
+3.4 up. Either stretch unknown (the rule's filters would not look now: no tick, the rollover hours, a gap, the
spread, too quiet): not against, nothing is decided on that.

With Y = X (a block saved under v1.2.0) a scalp is closed at exactly X as "short-term hold time", in profit or not, and
the rule is not looked at: v1.2.0's close. The first answer is latched and that close retried until done; the
stop-loss stays at the broker all along. The stop is sized to Y (`decide(…, hold)`: the longest the trade may be
held), and `closes_soon` counts Y as the hold.

### Variant: Classic | Rapid (v1.3.0)
The short-term card has a **Variant** selector, "Classic | Rapid" ("Clasic | Rapid" in Romanian). Classic is the
Scalper of this section. Rapid places many stop-limit orders at once, on a short-term signal (demo accounts only) or
at a high-impact release, with its own settings in the card below the selector ([RAPID.md](RAPID.md)). Only one of
the two runs: they are stored as `short_term.enabled` and `rapid.enabled`, and the PUT that switches one on turns the
other off (both switched on in one PUT: 400 `rapid_excludes_short`; a stored pair with both on reads Rapid off,
`db.User.merged_settings`). With Rapid on and its signal trigger on, the rule's signals start Rapid runs
(`Connector.start_local`) instead of scalps. A connector before 1.3.0 never gets the `rapid` block
(`main.for_connector`): it sees short-term off and ignores Rapid. The selector's hint says Rapid needs connector 1.3.0.

### When no scalp opens
- Switched off, execution not on auto, paper mode, the connector stopping, or the server refused the session for an
  inactive subscription (`paused` until the next welcome; open scalps still close at their time).
- `scalp.closes_soon`: from 17:00 New York minus the hold (Y since v1.3.0: the longest a scalp may stay open; X with
  no Y set) minus 5 min until 18:05 every day (the rollover's wide
  spreads), from Friday's until Sunday 18:05, all of 25 December and 1 January, and on 24 / 31 December from 21:00
  UTC minus the hold minus 5 min (the hours `bot/mt5_broker.py` `_fx_closed` takes FX as shut).
- A spread over `max_spread_pips`, a tick older than 60 s, another position on the symbol, a news burst within 15 s,
  `max_open` reached, or today's short-term limit used up.

### Daily limits
Each strategy has its own. When short-term hits its limit, only short-term stops until the next UTC day ("short-term
trading paused until tomorrow"); long-term goes on. Today's short-term result (`Scalper.pnl_today`: booked plus what
the open scalps show) is taken out of the equity the long-term daily loss guard compares, so scalps never use up the
long-term limit (nor the reverse).

### Across restarts and lost links (all in the connector's folder, written atomically; none in paper mode)
- `scalp_open.json`: the open scalps and those closed but not booked. At the next start (`Scalper.first_look`, at most
  10 s) those still open are closed at once and those closed meanwhile are reported with their real result.
- `scalp_day.json`: the day's short-term result and its starting balance (the daily limit survives a restart).
- `outbox.json`: results queued while the server link was down (`trade_opened`, `trade_closed`, `burst_update`; at most
  500, oldest dropped). Sent in order once the next session is welcomed, even after a restart. Re-sending is harmless:
  the server books trades by ticket.
- A scalp that closed while the connector was away gets its close time from MT5's history. Until MT5's clock offset is
  proven (`MT5Broker._utc`), that time is unknown: `Scalper._clock_known` waits up to 120 s (re-reading the history
  every 5 s proves it while the market is open) so the result is booked to its real UTC day; after that, now. Only
  such a scalp waits: one the run closes itself (the first look's leftovers, the hold time, a close asked for from the
  web app, its retries by `close_loop`) or sees close while it runs closed just now. While a result waits to be
  booked, its risk still counts against the day's limit (`Scalper._free`), so no scalp opens on room the loss is about
  to take.
- A closed scalp is reported once its result is in MT5's history (read up to `REPORT_TRIES` = 10 times, 3 s apart; the
  last try reports it without one). A result never read is booked as a loss of all the scalp risked at its stop, so
  the daily limit never misses it, and the web app's connector log asks the user to check it in MT5's history. For a
  scalp it was not following (left by the last run, an open cut short: risk 0), that is measured from its stop-loss and
  lots, else from the loss it last showed (`Scalper._risked`). That loss is kept for the day by ticket
  (`scalp_day.json` `guessed`, across a restart too): if MT5 had only left the scalp out of its list for longer than
  the report waited, it is closed once listed again (as a scalp not followed) and its real result replaces that loss
  instead of adding to it (`Scalper._book`); the connector log says so, and the limit's warning can come again.
- MT5 may leave an open position out of one read of the positions: the scalp is taken for closed and its report waits
  for the result. Listed again, it is followed again as it was, at once (`Scalper.adopt`; positions are read at every
  step while a result waits); its report stands down, since each report owns its own booking entry, and the real
  close is reported and booked once.
- `outbox.json` is sent oldest first; each message leaves the list once sent (by identity, so a result queued meanwhile
  past the 500 cap never costs one still unsent). A re-sent `trade_opened` (the very same message, its `opened_at`
  too) never reopens a trade already booked closed; an open on a closed trade's ticket opened at another time is a new
  trade (paper mode numbers its tickets from 1001 again at each start).

### Netting with the other strategies
One position per symbol (netting accounts): long-term signals and news bursts go first. No scalp opens next to another
position or within `BURST_GAP` = 15 s of a news burst checking its symbol (counted from Y: the scalp would still be
open); a long-term signal closes the scalp in its way. Rapid's signal runs follow the same order (a long-term signal
ends them, [RAPID.md](RAPID.md)).

### Reports
The connector reports each scalp like any trade with `strategy = "short"`, `risk` (money at the stop) and `confidence`
(the strength); `user_trades` gained `strategy`, `risk` and `confidence`. Strategies: `long` (trend signals), `short`,
`burst`, `news` (AI news signals), `custom` (custom rules), `test` (the "Test now" demo trade), and since v1.3.0
`rapid` (a Rapid run's trades: a news run's by its Burst row of kind "rapid", a signal run's as the connector reports
them). Each missing column and
the `strategy` index are added on their own in `db.init_db`; old rows are tagged from what they carry
(`db.TRADE_STRATEGY`: a burst, a test signal id, the signal's mode, else `long`), and earlier wrong `long` tags are
corrected. `GET /api/trades?strategy=` filters (the Trades page shows at most the newest 200 rows);
`GET /api/trades/stats?days=7,30&account=demo|real` gives the dashboard's numbers per period and strategy, one kind of
account at a time (demo and real money never add up; by default the account the connector reports).

## 4. Dashboard numbers
Each user chooses which numbers the dashboard shows, in what order, and the period of each
(`DEFAULT_USER_SETTINGS["dashboard"]` = `{"tiles": [{"k": kind, "days": 1-365, "scope": scope}, ...]}`, scopes
`all | long | short | burst | news | custom | rapid` (`dashboard.SCOPES`, `rapid` since v1.3.0; `all` counts test
trades too), at most 12 tiles,
validated by `server/dashboard.clamp_dashboard`, saved server-side). Kinds: equity, open signals, open trades, P/L,
win rate, trades, profit factor, average trade (the user's own trades, per strategy scope) and the engine's signal win
rate and pips. Each tile says what it counts: strategy, period and Demo / Real. The default shows today's four
numbers plus the account's 30-day P/L.

## 5. Tests
`tests/test_short_term.py` (rule parity, sizing, the New York and holiday gate, connector scalper: fills, margin,
restarts, the outbox kept across a restart, a close booked once MT5's clock is known, the daily limit while it waits,
closes of its own not waiting: the first look, a close asked for and retried, a stop hit while it runs; a result late
or never in the history, a scalp left out of one read booked once, one left out past its report's wait booked once,
a stray never read booking its stop; v1.3.0: Y's clamp, X alone as in v1.2.0, held past X unless turned against,
`scalp_against`),
`tests/test_burst_connector.py` (`outbox_cap_during_a_flush`),
`tests/test_short_server.py` (settings, pairs removed by the admin and the web app's saves meanwhile, a page left open
from before, `symbols_picked`, reports, re-sent opens and reused tickets, stats per account, migration),
`tests/test_long_term.py` (quiet
window, decision timing, saved and late decisions, stale and skipped fetches, restarts, weekends, re-anchored levels,
exit and reverse, leftover signals, deferred guard, admin setting, regret bar times),
`tests/test_rapid.py` (the Rapid variant, its signal runs on the Scalper's rule) and `tests/test_burst_server.py` /
`tests/test_web.py` (the Variant's exclusion, `rapid_excludes_short`).
