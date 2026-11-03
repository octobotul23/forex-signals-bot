# Rapid — implementation spec (v1.3.0)

Rapid is the short-term strategy's second **Variant** (Settings → Short-term: "Classic | Rapid", see
[SHORT_TERM.md](SHORT_TERM.md) §3 Variant). The classic Scalper opens one trade at a time. Rapid instead places
**many stop-limit orders at once** next to the price. Two things start a run:

- a **news run**, at a high-impact release. The server plans it like a news burst: a `bursts` row of `kind` "rapid"
  ([NEWS_BURST.md](NEWS_BURST.md)).
- a **signal run** (local), when the Scalper's rule gives a short-term signal. The connector starts it on its own,
  on **demo accounts only**.

Every fill closes on the first of three things: hold X / Y (`bot/hold.py`, the same rule the news burst and the
short-term trades close by), its stop-loss, or its take-profit. The stop-loss and take-profit sit at the broker.
Rapid has its own risk range, its own take-profit range and its own daily loss limit.

| part | where |
|---|---|
| settings, confidence, sizing, how many orders fit, prices, comments (pure: no broker, no clock, no files) | `bot/rapid.py` |
| news runs: who gets one, on which pair, with which params | `server/burst.py` `wants` / `rapid_allowed` / `rapid_plan` / `plan_runs` / `rapid_params` / `needs_version`, `server/engine.py` `burst_tick` |
| settings API and the exclusion with short-term | `server/main.py` `put_my_settings`, `server/db.py` `User.merged_settings` |
| the runs | `connector/connector.py`: the news runner `run_burst` (one task per run, for both kinds) with `_rapid_plan` / `_rapid_fit`, `start_local`, `rapid_room`, `rapid_margin`, `_rapid_adopt` |
| the card | `server/web/app.js`, `server/web/i18n.js` (`rp_*`) |

Hard rules:
- **Off by default** (`enabled` false), for every new user too.
- **Signal runs are demo only.** They trade on the Scalper's rule, which lost money after the spread in every test
  (SHORT_TERM.md §1), and many orders multiply that loss.
- **News runs** trade a real account only with `allow_real_account = yes`, like every news run.
- **Needs connector 1.3.0.**
- **Never together with the classic short-term:** only one of the two runs.

---

## 1. Settings

### Per user — `DEFAULT_USER_SETTINGS["rapid"]` = `bot/rapid.py` `DEFAULTS` (+ `max_hold_seconds`)

| key | default | range (`LIMITS`) | meaning |
|---|---|---|---|
| enabled | false | bool | Rapid on. Turning it on turns `short_term.enabled` off (§1 Exclusion). |
| triggers | `{"signals": true, "news": true}` | two bools | What starts a run: a short-term signal (demo only), or a high-impact release. |
| orders | 10 | 1 – 40, int | Orders per side of a run. Netting places one order per side that carries them all (§4). |
| sl_unit | "pips" | pips \| money | **pips**: the stop is `sl_pips` and the lots come from the risk. **money**: the lot is `lots` scaled by the confidence, and the stop goes as far as the risk money allows. |
| risk_unit | "pct" | pct \| usd | % of balance, or money in the account currency. |
| risk_min | 0.2 | pct 0.05 – 5, usd 1 – 10 000 | What EACH order may lose at its stop, at confidence 0. |
| risk_max | 1.0 | pct 0.05 – 5, usd 1 – 10 000 | The same, at confidence 1. Under risk_min it is raised to it. |
| sl_pips | 8.0 | 1 – 200 | sl_unit pips: each order's stop (× the symbol's scale). |
| lots | 0.10 | 0.01 – 100 | sl_unit money: each order's lot at confidence 1. |
| tp_min_usd | 10.0 | 1 – 10 000 | Each order's take-profit in money, at confidence 0. |
| tp_max_usd | 15.0 | 1 – 10 000 | The same, at confidence 1. Under tp_min_usd it is raised to it. |
| offset_pips | 2.0 | 0.5 – 50 | How far the trigger sits from the price (× scale). |
| limit_pips | 0.0 | 0 – 5 | How far the entry sits behind the trigger, on the pullback side (× scale). 0 = the broker's stops level. |
| expire_seconds | 30 | 5 – 600, int | An order still unfilled after this is cancelled. |
| hold_seconds | 30 | 5 – 600, int | X: from this point on, a fill in profit is closed. |
| max_hold_seconds | 120 \* | X – 1800 (`MAX_HOLD_TOP`); null removes it | Y: a fill still open at this point is closed, whatever it shows. Missing = X. |
| analysis | true | bool | News runs: the pre-news analysis picks the sides. Off, it is only worked out and logged (shadow). |
| loss_unit | "pct" | pct \| usd | |
| daily_loss | 5.0 | pct 0.5 – 50, usd 1 – 1 000 000 | Rapid's own daily loss limit (§7). |
| max_runs | 2 | 1 – 5, int | News runs per release ("Pairs per release"). |
| cooldown_seconds | 120 | 0 – 3600, int | How long a symbol rests after a signal run on it ends. |
| symbols | [] | the engine's pairs | Empty = at news, any pair of the release; on signals, the short-term pairs (or the engine's when those are empty too). |

\* `NEW_USER_MAX_HOLD`. It is set only in the server's `DEFAULT_USER_SETTINGS`, not in `DEFAULTS`, as for
short-term. A block without it follows X wherever the block is used.

The other constants in `bot/rapid.py`:

| constant | value | meaning |
|---|---|---|
| `SL_CAP_PIPS` | 200 | The farthest a money stop may go (× scale). |
| `RAPID_MARGIN_USE` | 0.5 | One side of a run may use at most this share of the free margin. |
| `TP_MAX_SL` | 3.0 | The take-profit is at most 3 stops away. |
| `COMMENT_MAX` | 20 | Longest comment on a local run's order. |
| `RUN_KIND` | "rapid" | A news run's `Burst.kind`, a local run's `st["kind"]`, and its trades' strategy. |

**`clamp_rapid(body, cur, symbols)`** mirrors `bot/scalp.py` `clamp_short`. The server runs it on every PUT and
again whenever it reads a stored block (`merged_settings`, `rapid_plan`, `rapid_params`). The connector runs it on
every settings message (`apply_rapid`) and on every plan (`_rapid_params`). The rules:

- **Merging.** Unknown keys are dropped. Missing keys keep the value in `cur` (or their default). Every number is
  clamped to its range, and the `INTS` keys are cast to int (orders, expire_seconds, hold_seconds, max_runs,
  cooldown_seconds).
- **Bad values in `body`** raise `ValueError`, which becomes a 400 with the same code: `invalid_number` (a bool,
  NaN, text, an int key sent as a decimal), `invalid_unit`, `invalid_symbols`, `invalid_triggers`,
  `invalid_enabled`, `invalid_analysis`. The text "false" never switches anything on. A stored value that went bad
  gets its default instead; for a money key, that is its unit's default.
- **Unit changes.** Changing a unit without sending a new value resets that unit's keys to `UNIT_DEFAULTS`: pct =
  risk 0.2 / 1.0 and daily 5.0; usd = risk 10 / 50 and daily 100. So 50 $ never becomes 50 %.
- **Minimums and maximums.** If risk_max is under risk_min, it is raised to risk_min. tp_max_usd is treated the same
  way against tp_min_usd. The two are never swapped.
- **Y.** It is in the result only when `body` or `cur` has it. It is clamped to X .. 1800 after X is clamped. Null
  removes it (Y follows X again), and a stored Y that went bad is dropped.
- **triggers.** The keys sent change. The ones not sent keep their stored value.
- **symbols.** Only pairs the engine trades are kept.

### Exclusion with short-term (the Variant selector)
- `put_my_settings`:
  - Switching one of the two on turns the other off.
  - Switching both on in one PUT is refused: 400 `rapid_excludes_short`.
  - A `rapid` body that is not an object is refused: 400 `invalid_rapid`.
  - Pairs none of which are traded are refused: 400 `pairs_not_traded`. Rapid's pairs are kept as short-term's
    are (`web_pairs_kept`).
- `db.User.merged_settings`:
  - A block that is missing (a user from v1.2.0) or gone bad reads as the defaults, which means off.
  - A stored pair with both switches on reads Rapid off.
- `main.for_client` adds `"rapid_note": "pairs_gone"` when none of Rapid's pairs is traded any more. Rapid then
  trades nothing until the user picks other pairs ("The pairs you chose for Rapid are no longer traded...").
- `main.for_connector` leaves out `rapid` and `rapid_note` for a connector before 1.3.0 (§10).

## 2. Confidence, risk and take-profit
Each run has one confidence c (0 .. 1). Every order of the run gets the same risk and take-profit from it:
- `risk_at(c)` = risk_min + c × (risk_max − risk_min), in `risk_unit`, per order.
- `tp_usd_at(c)` = tp_min_usd + c × (tp_max_usd − tp_min_usd), in money, per order.

Where c comes from:
- **News run:** `confidence_from_score(score)` = |score| of the pre-news analysis (`bot/news_view.py` analyse,
  −1 .. 1), held to 0 .. 1. It is 0 when there is no number.
- **Signal run:** `confidence_from_stretch(stretch, z)` = (|stretch| − z) / z, held to 0 .. 1, where z is the
  Scalper's threshold (`RULE["z"]`). So c is 0 at the threshold and 1 at twice it. It is 0 when either value is
  unknown.

## 3. Sizing one order — `size_order(cfg, c, balance, spec, spread_pips)`
It returns {lots, sl_pips, tp_pips, tp_usd, money, tp_clamped}, or {skip: reason}.

**The money at risk.** `money` = `money_for(risk_unit, risk_at(c), balance)` (`bot/risk.py`): what one order may
lose at its stop. It is capped at `MAX_RISK_PCT` (10 %) of the balance.

**The floor.** Stops and take-profits are whole points, measured from the order's **limit** price. None is ever
closer than the floor: max(`stop_floor_pips`, freeze level + spread). `stop_floor_pips` covers the broker's stops and
freeze levels and three spreads. Freeze level + spread is the line inside which a stop could be neither placed nor
changed.

**The stop and the lots.**
- **sl_unit pips:**
  - The stop is sl_pips × scale, rounded UP to the point; below the floor, the floor.
  - The lots are what loses `money` at that stop (`lots_for_money`): rounded DOWN to the volume step, at most
    volume_max.
- **sl_unit money:**
  - The wanted lot is `lots` × risk_at(c) / risk_max, rounded DOWN to the step. So the lot is full at confidence 1
    and smaller when the run is less sure.
  - `bot/risk.py` `money_stop` then puts the stop where that lot loses `money`. The stop is at least the floor; if
    it would sit inside the floor, the lots are cut to fit there instead. It goes at most `SL_CAP_PIPS` × scale
    away; the order then risks less.
  - The lots are then capped at volume_max (one order is one ticket).

**The take-profit.** `tp_usd_at(c)` is turned into pips on those lots: tp_usd / (lots × pip value). It is rounded
to the point and held to [floor, `TP_MAX_SL` × the stop]. When it was moved, `tp_clamped` is set and the connector
logs "take-profit of X held to Y pips on L lots".

**Skips** (the run is skipped with this text):
- "no contract data for SYM (pip, point, pip value or lot step)"
- "no money to risk: the balance is not above 0"
- "{want} lots per order is under the minimum lot size of SYM (min)"
- "{money} at risk is too little for {min} lots with the closest stop allowed on SYM (… pips)" (money mode), or
  "… with a N pip stop on SYM" (pips mode)

## 4. Stop-limit orders and the broker

### Prices — `stop_limit_prices(side, bid, ask, spec, offset_pips, limit_pips)`

| side | trigger | limit |
|---|---|---|
| buy | ask + offset | trigger − gap |
| sell | bid − offset | trigger + gap |

- Both distances are × scale. gap = max(limit_pips × scale, the broker's stops level, 1 point).
- The trigger is rounded away from the market, and the limit away from the trigger, so neither distance shrinks.
- A fill is therefore never worse than the limit: no slippage. When the price runs on without a pullback, the order
  expires unfilled.
- The SL and TP of each order are measured from its limit.
- The run is skipped when the trigger would sit within max(freeze level + spread, stops level, 1 point) of the
  price: "the trigger X pips from the price is within the freeze level and spread of SYM (N pips): an order that
  close could not be cancelled". It is also skipped with "no price for SYM", or for an unknown side.

### What the broker does (demo spike M0, 30 Oct, XAUUSD 0.01 lot)
- **Placing.**
  - The broker refuses a limit equal to the trigger, and a limit closer than its stops level (5 points there).
  - It accepts a limit on the pullback side, at least the stops level away: for a buy below the trigger, for a sell
    above it.
  - Pending orders placed with FOK were accepted (10009). Both GTC and a set expiry are accepted.
- **When it triggers.** The **same ticket** changes from BUY_STOP_LIMIT (6) to BUY_LIMIT (2). Its `price_open`
  becomes the limit and its `price_stoplimit` becomes 0. The comment, SL, TP and expiry are kept, and the stored
  filling is RETURN.
- **The fill** came at exactly the limit, 3 s after the trigger. On netting, the position ticket = the order ticket.
- **Closing.** A close is refused with 10029 when the position's SL or TP is within the freeze level. The latched
  close retries it, because the broker's own SL or TP is about to fire anyway.

### Broker calls (`bot/mt5_broker.py`, wrapped 1:1 by `MT5Adapter`; NEWS_BURST.md §6 Broker additions)
- **`stop_limit_allowed(symbol)`**: whether the symbol's `order_mode` allows stop-limits. If not, the run is skipped
  with "stop-limit orders are not allowed on SYM". There is **no plain-stop fallback**.
- **`place_pending(…, kind="stop_limit", limit_price=…)`**: `price` is the trigger. The broker drops the order 2 min
  after our own expiry, even if this PC is gone by then.
- **`pending_orders`** gives `type` (6 / 7 waiting, 2 / 3 triggered), `kind` (stop_limit | limit | stop) and
  `price_stoplimit`. A triggered order is still pending: only a fill counts as one for the OCO and for the end of
  the run. The log says "order #T triggered, a buy limit at P now".
- **`order_state(ticket)`**: an order that left the book with no deal, whose history shows it canceled / expired /
  rejected with nothing filled, is dropped at once ("order #T expired without a fill").
- **`pending_room()`**: the account's `limit_orders` minus the orders open now. None = not known.
- **`margin_for(symbol, side, lots)`**.

### Netting and hedging (`_rapid_plan`)
- **Netting** (one position per symbol): one order per side carrying all n × lots. `split_volume` splits it only
  above volume_max, into as few even chunks of whole steps as it takes. Comments: `fxb:<id>:b` / `fxb:<id>:s`, with
  a part number after the side when the order is split (`:b1`, `:b2`). Its take-profit sits at the broker, so the
  runner does not close staggered slices itself as the classic burst does.
- **Hedging**: one ticket per order, the two sides interleaved, so a late spike still finds both sides. Comments:
  `fxb:<id>:<i>`.
- **A local run's** orders carry `local_comment(id, i)` = `fxr:<id>:<i>` instead. The id is the run's start in ms
  (13 digits), and the comment is at most 20 characters; a comment too long is never cut (the run is skipped
  instead).
- `connector.run_tag(comment)` reads either back as (kind, id): `fxb:` → "burst" (every news run's, Rapid's too),
  `fxr:` → "rapid".

## 5. How many orders fit — `fit_orders(n, money, free_room, pending_room, margin_per_order, free_margin)`
A run's `orders` per side are cut to the tightest of three limits:

| limit | how many fit | when not known |
|---|---|---|
| daily limit | `rapid_room` / money: what is left of Rapid's daily loss limit (§7) | no cap |
| pending-order limit | `pending_room`, as orders per side: hedging places one ticket per order and side, netting one per side and volume_max chunk | no cap; the broker refuses what is over it |
| free margin | `RAPID_MARGIN_USE` × (free margin − `rapid_margin`) / the margin one order needs | no cap; the broker refuses an order it cannot cover |

`rapid_margin` is the margin the other Rapid runs reserved, counted only while none of their orders has filled.
Once an order fills, its margin is in the account's own figures.

On a tie, the first limit in the table wins. If not one order fits, the run is skipped:
- "Rapid's daily loss limit leaves no room for an order risking X"
- "the account's pending-order limit leaves no room for an order"
- "not enough free margin for one order of L lots on SYM"

A cut is reported as a warning, "Rapid run #id: n of N orders per side (daily limit)", and the run goes on with n.

What the run then holds is reserved, from the room check up to the reservation, with nothing awaited in between.
So two runs starting together never take the same room. The reservation is:
- `st["reserved"]` = n × money, against the daily limit;
- `st["margin"]` = n × the margin of one order, for one side.

Both are released when the run ends.

## 6. The two kinds of run

### News runs (the server plans them, the connector runs them)
- **`wants(s, "rapid")`**: Rapid is on and `triggers.news` is on. The admin's burst switch (`app.burst.enabled`)
  stops both kinds.
- **`rapid_plan`**, every 2 s in `burst_tick`:
  - A release is one currency's events within one minute (`candidates`, as for the burst).
  - For each release it takes the first `max_runs` pairs of the currency's `BURST_PAIRS` list that
    `rapid_allowed` lets through. Those are pairs the engine and the user trade and that `rapid.symbols` allows
    (empty = any), and that no run of another kind holds.
  - A release that already has a Rapid run gets no second one. A cancel stands.
- **`plan_runs`** plans Rapid's runs first, then the classic burst's on the pairs Rapid left. So there are never two
  runs on one pair (NEWS_BURST.md §1 Precedence with Rapid). A classic burst without pair picks whose own pair Rapid
  took moves to the next pair of the list Rapid left (USD: XAUUSD when Rapid has EURUSD; GBP or JPY: GBPJPY); with
  picks, or with Rapid's news runs off, the classic burst plans as before.
- **`rapid_params`** is what the row stores:
  - the `RAPID_PARAMS` keys of the user's block, clamped again;
  - `orders` capped by the admin's `max_trades`;
  - `max_hold_seconds` always present (Y, or X when missing);
  - the user's `max_spread_pips` (3.0 by default): the news run is placed into the same pre-release spread as the
    classic burst.
  - It carries no `lead_seconds`; the connector uses the burst's default of 60 s.
- **`run_seconds(params, "rapid")`** = expire_seconds + Y; a value it cannot read is Rapid's default. A run with no
  final report `run_seconds` + 300 s after its release is marked `error` ("no final report from the connector").
- `burst_stop_reason` cancels a planned or running Rapid run when the user switches Rapid's news runs off ("Rapid
  news runs switched off"), or when the release moves off a pair Rapid may take ("event rescheduled").
- **`needs_version`**: any run of kind "rapid" needs connector 1.3.0 (§10).

On the connector (`on_burst`, then `run_burst` → `_burst_plan` → `_rapid_plan`), at `release − lead_seconds`:
1. The common checks (NEWS_BURST.md §6):
   - the real-account lock;
   - exchange accounts are not supported;
   - another run on the symbol;
   - on netting, a position on the symbol;
   - Algo Trading on.
   Then Rapid's own daily limit applies instead of the long-term one.
2. `stop_limit_allowed`, else the run is skipped.
3. **The analysis always runs**, because its confidence c sets the risk and the take-profit.
   - With `analysis` on, it also picks the sides: one when it is clear, both (at c) when it is not, and none on a
     skip, which skips the run with the analysis' own text.
   - With `analysis` off, it is only reported (shadow) and both sides are placed.
   - Either way the answer goes to the server once (`burst_update` with `analysis`), as for the burst.
4. The quote is read again, because the bars took a moment. The spread is checked against the cap when the plan
   sets one.
5. `size_order`, `stop_limit_prices` for each side, then `_rapid_fit`.
6. Its id goes into `rapid_open.json` (`rapid_news`) **before** the first order is sent. That way whatever the run
   leaves behind under its `fxb:` comments is still Rapid's after a restart (§8).
7. The orders are placed. Then come the monitor and the OCO: the first fill cancels the other side at once. At
   release + expire_seconds, every order still pending (triggered or not) is cancelled.

A dry run reports what it would do without placing anything: "[dry-run] Rapid run #id would place N stop-limit
orders on SYM: buy-stop-limit T limit L / sell-stop-limit …, n × lots lots per side, SL s pips, TP t pips (m per
order), confidence c".

### Signal runs (local, in the connector) — `Connector.start_local`
**Gate.** `Scalper.rapid_on()` must hold. It needs all of:
- Rapid on;
- `triggers.signals` on;
- short-term off;
- execution on auto;
- MT5, not paper mode;
- the connector not stopping and the session not paused.

The rule's signals then start runs instead of scalps. It watches `rapid.symbols`, else short-term's pairs, else
the engine's.

**One run per signal**, the surest first. No run starts in any of these cases:
- **The symbol is resting:** within `cooldown_seconds` after a run there ended (`rapid_book`).
- **The symbol is held:** `symbol_owner` sees a running run there.
- **The account is not demo.** The connector says "Rapid: signal runs need a demo account: none starts on this one"
  (once a day).
- **A news run is close:** one will check the symbol before this run could be over. The run's life is
  expire_seconds + Y, and `_burst_soon` adds `BURST_GAP` = 15 s to it.
- **The close is near:** `closes_soon(life)`, around the daily and weekly close. The connector says "Rapid: no new
  run around the daily and weekly close (17:00 New York)".
- **Something is already there:** any position on the symbol (all magics), or a run's order still pending there.
- **No room is left** of Rapid's daily limit.

**The run itself:**
- Its id is the start time in ms, and its comments are `fxr:`. It has no Burst row and sends no `burst_update`.
- It trades the signal's side only, at confidence `confidence_from_stretch`. There is no analysis (the side and
  confidence are the signal's), and `release` = now, so its orders expire `expire_seconds` after the signal.
- The log says "Rapid run #id: a buy signal on SYM (stretch s, confidence c)".
- A run skipped on every signal is reported in the web app at most once per `RAPID_SKIP_EVERY` = 900 s per symbol;
  the other times it only goes to the log.
- **A long-term signal on the symbol goes first** (`rapid_make_way`). Its runs there are ended: their orders are
  removed and their positions closed at once ("a long-term signal on the same symbol"). The signal waits at most
  `RAPID_WAY_WAIT` = 10 s for that.
- The long-term position count does not count `fxr:` positions.

## 7. Hold X / Y, the daily limit

### Hold X / Y (`bot/hold.py` `hold_action`, as NEWS_BURST.md §6 Hold X / Y)
Every fill is checked in this order:

| check | when | closed as |
|---|---|---|
| profit | from X on, P/L ≥ one point on its lots | "in profit after Xs" |
| against | at any age, only while Y > X | "turned against the release" (news run) or "turned against the signal" (signal run) |
| max | at Y, whatever it shows | "max hold Ys" |

- **What "against" means.**
  - For a news run (`news_against`): the mid is back through the mid quoted just before the release.
  - For a signal run: `Scalper.step` keeps `st["against"]` up to date with `scalp.scalp_against`, from the stretch
    at the signal. It is against when the rule now opens the other way, or the price has stretched
    `AGAINST_WIDEN` = 1.0 further.
- **With Y = X**, the fill is closed at exactly X as "hold time", in profit or not.
- The first answer is latched, and that close is retried until done; a 10029 is a retry. The stop-loss and
  take-profit stay at the broker all along.

### Rapid's own daily loss limit — `rapid_day.json` (`DayBook`, beside `scalp_day.json`)
- **The limit** is `daily_loss`, in `loss_unit` (pct = % of the day's starting balance), per UTC day. When it is
  reached, only Rapid stops until the next UTC day. The warning comes once a day: "Rapid daily loss limit reached
  (X lost today): no new Rapid run until tomorrow (UTC). The other strategies go on."
- **`rapid_room`** is what each new run may still use. It is the limit, less:
  - today's booked Rapid result;
  - for each other live Rapid run, the larger of its reservation (`reserved`) and what its positions lose now;
  - what Rapid positions of no live run lose now (left to the sweeper);
  - for a trade that has closed but is not booked yet, what it showed last (`rapid_booking`).
- **Booking.** Each Rapid close is booked once per ticket (`_rapid_closed`), on the day it closed. A trade booked
  before a crash is not booked again: `rapid_day.json` keeps the day's tickets.
- **A result that cannot be read** counts the loss it showed last, with the warning "Rapid #t: its result could not
  be read, so today's Rapid loss counts X (what it showed last). Check it in MetaTrader 5's history." A result read
  later replaces that guess.
- **The other daily limits.** The long-term daily loss limit leaves Rapid out (`rapid_pnl_today`), as it leaves
  short-term out: the equity it checks is `equity − scalper.pnl_today() − rapid_pnl_today()`. The classic burst's
  check does the same.

## 8. Across restarts and the sweeper
`rapid_open.json` (beside `scalp_open.json`, written atomically and only when it changes; never in paper mode)
holds two kinds of entry:
- **Each local run** that is open, or that was handed to the sweeper and is not all reported yet. It keeps the
  symbol, side, demo flag, the symbol's digits and pip, the orders that may still fill, and the trades not yet
  reported closed.
- **`{"news": true}`** for each Rapid news run that sent orders (`rapid_news`), until nothing of it is left. Its
  `fxb:` leftovers are then still counted as Rapid's (strategy, daily limit) after a restart.

**At start** (`_rapid_adopt`, before the first sweep), the connector logs "Rapid: N run(s) left by the last run:
their orders are removed and their positions closed now". The local runs the file holds are handed to the sweeper:
- their orders are removed and their positions closed;
- each trade is reported and booked to Rapid's day once its history shows it closed, including one that closed at
  its SL or TP while the connector was away;
- an order whose history shows it left unfilled is dropped;
- a trade already booked by its result is not reported again.

**`strategy_of(position)`** is "rapid" for an `fxr:` comment, or for a news run's position whose run is (or was) a
Rapid one: in `rapid_news`, or of kind rapid. A Rapid orphan's report carries `strategy: "rapid"`, so the server
books it as Rapid's even without a Burst row.

**The sweeper's message** names whose leftovers it cleaned up:
- "Cleaned up after an earlier Rapid run: removed … pending order(s), closed … position(s)";
- "… after an earlier news burst and Rapid run" when there were both;
- a classic-only sweep keeps the v1.2.0 text, "Cleaned up after an earlier news burst".

## 9. Reports and the dashboard
- **Strategy "rapid".** Trades are booked with strategy `rapid`:
  - a news run's trades through its Burst row of kind "rapid": `main._strategy`, and the `db.TRADE_STRATEGY` CASE,
    which joins `bursts.kind`;
  - a signal run's trades because the connector says so (`strategy: "rapid"` in its snapshot and closes, from
    connector 1.3.0 on).
- **Server lines.** News runs show up in the bursts list and the log as "Rapid run #id SYM status" (`_burst_line`).
- **The dashboard** has the scope `rapid`: `dashboard.SCOPES` = `all | long | short | burst | news | custom | rapid`,
  the web's `TILE_SCOPES` and the Trades filter, and i18n `scope_rapid` "Rapid".

## 10. Old connectors and the rollback
- **A connector before 1.3.0:**
  - It never gets the `rapid` block (`for_connector`). Since Rapid excludes short-term, it sees short-term off.
  - Rapid news runs need 1.3.0 (`needs_version` → `V130`). They are skipped with `OUTDATED` ("connector out of date:
    download the new connector") when planned, and again at delivery (a connector downgraded meanwhile).
  - `welcome` replays only the plans its version runs.
- **A 1.3.0 connector given a run it cannot read:**
  - a kind it does not know → "unknown run kind";
  - a unit or switch it cannot read → "invalid plan (invalid_unit)".
  It never guesses either.
- **After `deploy_130.py rollback`:**
  - Rapid is switched off in every stored user.
  - Unfinished Rapid runs are cancelled ("cancelled: update rolled back"). The rollback is refused while any burst
    or burst / Rapid trade is open.
  - v1.2.0 then ignores the `rapid` block and its files (`rapid_day.json`, `rapid_open.json`).

## 11. Web app (Settings → Short-term, card "Rapid")
- **The Variant selector** "Classic | Rapid" ("Clasic | Rapid") sits in the short-term card. Its hint says
  "… Rapid needs connector 1.3.0." Picking Rapid turns the classic short-term off, and the other way round.
- **The Rapid card's fields:**
  - "Starts on": "Short-term signals (demo only)" / "High-impact releases";
  - "Stop-loss by": pips / risk money;
  - "Risk per order in", with the least sure / most sure risk;
  - "Lot per order (most sure)";
  - the two take-profits in money;
  - "Orders per side";
  - "Trigger distance from the price (pips)";
  - "Entry pullback behind the trigger (pips, 0 = broker minimum)";
  - "Cancel unfilled orders after (s)";
  - the hold X / Y ("Close if in profit after (s)", "Hold at most (s)"; Y cleared = saved as null, it follows X);
  - "Pairs per release";
  - "Pause on a pair after a signal run (s)";
  - "Pre-news analysis picks the side (news runs)";
  - "Daily loss limit · Rapid only";
  - "Pairs for Rapid".
- **"How Rapid works"** (`rp_n1` – `rp_n6`) covers: demo-only signal runs; the stop-limit entry with no slippage;
  Rapid taking a release's pairs first; confidence picking risk and take-profit; its own daily limit; and the need
  for connector 1.3.0.
- **The web header** shows the app's version, "V1.3.0" (`/api/public/info`, `bot/version.py` `APP_VERSION`).

## 12. Tests (plain scripts, `python tests/<file>.py`, exit code ≠ 0 on failure)
- **`tests/test_rapid.py`**:
  - the pure part: `clamp_rapid` (defaults, ranges, units, min / max kept in order, Y, switches, triggers,
    symbols), the confidence, `size_order` (pips, money, the take-profit clamp, gold's scale), `fit_orders`,
    `stop_limit_prices`, `split_volume`, `local_comment`;
  - then the local runs, on the real Connector and Scalper with test_short_term's simulated broker:
    - starting: demo only, one run per symbol then the cooldown, no run before a news run or where a trade is, none
      around the New York close, none unless execution is auto, none while paused or stopping;
    - while running: make-way for a long-term signal, the reservation cutting the count, the daily limit stopping
      Rapid only, held past X until it turns against;
    - afterwards: closes booked once, the sweeper's wording, restart adoption, a trade booked before a crash not
      booked again, a news run's leftovers still Rapid's after a restart.
- **`tests/test_burst_connector.py`**: Rapid news runs in the runner:
  - triggered, filled and the other side cancelled; triggered and never filled = no fill;
  - one side or none by the analysis; skipped without stop-limits or inside the freeze level;
  - netting with one order per side; cut to the pending-order limit and the margin;
  - take-profit and risk by confidence; hold until profit or Y; gold scaled;
  - left out of the long-term daily limit.
- **`tests/test_burst_server.py`**: the server's Rapid planning and params, the version gate, and `deploy_130.py`
  `rollback_db` (Rapid off, its runs cancelled).
- **`tests/test_web.py`**: the card's fields are ones `clamp_rapid` keeps, the client's copies of `DEFAULTS` /
  `UNIT_DEFAULTS` / `LIMITS`, the Variant's texts, the `rapid` strategy filter, the `rapid_excludes_short` message.
