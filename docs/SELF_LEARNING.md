# Self-learning (auto-învățare) — v2

The engine replays its own strategy on real price history, finds where and why it lost, tries a small set of
corrections and keeps one only when it also wins on prices that were never used to choose it. It never invents
a strategy and never touches risk %, lot size, symbols, the event guard or the news-burst mode: it only
rescales the stop / target and skips signals in conditions that clearly lose.

Honest expectation: no correction can create an edge the strategy does not have. On real MT5 history
(6 majors, H1, 2022–2026) the EMA-cross strategy makes about −0.02R per trade after costs, and the learner
correctly answers "no change" — the useful output then is the diagnosis and the edge alert, not a tweak.

## 1. Experience: a replay of real history

Live history is tiny, so `learning.replay()` trades the history from the price feed (`history_bars`, default
30 000 H1 bars ≈ 5 years per symbol) the way the live engine would:

* entry at the close of the signal bar; from the next bar `walk_bar()` checks the stop-loss before the
  take-profit (bid bars), and a bar that **opens beyond** a level (a gap) fills at its open;
* the strategy's exit rule closes at the bar's close; one position per symbol;
* no entry whose bar closes inside an event-guard window (a High-impact release of either currency
  ± `event_guard_minutes`; a window ending at the close does not count, as the live engine decides seconds
  after it), taken from the stored economic calendar with the live engine's rule. The calendar
  is stored only since the server started saving it, so older history stays **unguarded** (there the replay
  trades through releases the live engine would skip);
* cost per trade = max(`cost_pips`, the broker's recorded spread on that bar), so illiquid hours cost more;
* every result is in **R** (result ÷ risk distance, after cost) — what a client sizing by risk % feels.

`walk_bar()` is also what `engine._track_outcomes` uses, so live outcomes and the replay share one rule.
The engine fetches max(600, the strategy's warmup + 50) bars live (clamped to the feed's `max_history`), so
live features see the same context as the replay's. Live signals carry `meta` (feed, model id, features, applied scales); the synthetic demo feed never counts.

## 2. Features at entry

| feature | values | how |
|---|---|---|
| session | asia 00–07, london 07–12, overlap 12–16, newyork 16–21, late 21–24 (UTC) | hour of the bar's close |
| vol | low / mid / high | ATR(14) percentile in the last 250 bars |
| trend | with / against | signal side vs. close relative to EMA(200) |

## 3. What went wrong (labels on losing trades)

| label | rule |
|---|---|
| `gave_back` | was ≥ +1R in profit, ended at a loss |
| `stop_too_tight` | stopped out, then price reached the target within 48 bars |
| `exit_rule` | closed by the strategy's exit rule at a loss |
| `wrong_way` | never got +0.3R in profit |
| `chop` | anything else (went both ways, then hit the stop) |

The report shows the shares, per-symbol counts and the 12 newest losing trades of the test period.

## 4. Honest validation

History is split by time: **train** (oldest 60 %) · **validation** (next 20 %) · **test** (newest 20 %).
A trade still open at a split, or entered within **48 bars** after one (embargo), belongs to no period.

All comparisons are **paired by week**: the candidate and the active model saw the same market each week, so
the market's own swings cancel out of the difference (R per week, t = mean ÷ standard error over weeks).
Standard errors of single-model results are **clustered by week** (trades of one week are not independent).

1. **Stop / target scales** — the 3×3 neighbourhood (±0.25) of the active scales, range 0.75–1.5, one pair
   for all symbols. Best weekly gain on train with t ≥ 1.0, kept only if it also gains on validation (t ≥ 1.0).
2. **Filters** ("don't trade when session = asia", pooled over all symbols, at most 2, blocking ≤ 25 % of
   signals) — a candidate needs on train n ≥ 40, week-clustered t ≤ −2.58 and a mean shrunk toward the
   overall mean (as if by 50 average trades) ≤ −0.10R; on validation n ≥ 100 and mean < 0. Each is added
   only if re-replaying with it gains on validation (t ≥ 1.0). A filter already in use stays on weaker
   evidence (t ≤ −1.645, shrunk ≤ −0.05).
3. **Validation as a whole** — the candidate must gain ≥ 0.02R per trade (expressed per week) with t ≥ 1.5.
4. **Gate on test** (never used for choosing): ≥ 30 test trades; ≥ 50 trades that differ from the active
   model; weekly gain > 0 with t ≥ 1.28; still positive with **double cost**; test drawdown ≤ 110 % of the
   active model's + 1R.

Decision: `improved` (passed everything), `no_change` (nothing beat the active model, or too little on
validation) or `rejected` (failed the gate). **Edge alert**: the plain strategy loses on the test period with
week-clustered t ≤ −2 — no correction will fix that; the strategy itself needs attention.

## 5. Modes and safety

* `off` — nothing runs and nothing is applied: no scales, no filters, no AI lessons (the stored model is
  kept and used again when learning is switched back on). `suggest` (**default**) — analyses weekly
  (`weekday`, `hour_utc`, default Saturday 06:00 UTC); an admin applies a proposal. `auto` — a scheduled
  run's proposal becomes active by itself only if it passed every test, its scales stay within 0.75–1.25, it
  adds no AI lessons, the feed is real, the active model has been in use ≥ 28 days, learning is still on auto
  when the run ends and the active model did not change while it ran. Runs started by hand are never applied
  automatically.
* A weekly run that was missed (server down) or failed is retried **hourly** until the next scheduled time,
  at most 3 failed runs a week. A scheduled run that finds no symbol with enough history stores no run.
* **Regret check**, daily while a model is active: the model and the plain strategy are replayed over the
  **completed** weeks since activation (the week under way is left out); after ≥ 8 weeks with a weekly loss
  at t ≤ −2.58 the engine reverts to the plain strategy, switches auto to suggest and blacklists those
  changes for 90 days. The verdict is re-tested every week, so the threshold is stricter than a one-off
  test: a model with no real effect is reverted by mistake in about 6 % of cases within a year. Approved AI
  lessons survive the revert (kept as a new run with trigger `regret` and no corrections). A verdict is
  dropped when the active model changed during the check. It also checks that the replay reproduces ≥ 80 %
  of the model's closed live signals of bars opened after activation (otherwise an engine error is raised).
* A model's corrections are tied to the strategy, its parameters (numbers compared by value, so `2` and
  `2.0` match) and the timeframe (`fingerprint`); after a change they are ignored until a new analysis. AI
  lessons do not depend on the strategy and stay in use.
* Every apply / revert is in the audit log; an admin can revert to the previous model or to none. A model
  restored by a revert counts as applied again now (28-day dwell and regret window restart).
* Learning runs in a **child process** (`spawn`), so the event loop — bursts are timed to the second — never
  waits on it. `FX_LEARN_IN_THREAD=1` (tests) keeps it in a thread.

## 6. AI news signals

Report only: results of closed live AI signals by confidence band and the minimum confidence the data would
support (never raised automatically). With `ai_lessons` on, Claude writes short lessons from its own losing
calls; they reach the AI prompt only after an admin applies them.

## 7. Where it lives

* `server/learning.py` — pure functions: replay, features, labels, statistics, search, gate, regret.
* `server/engine.py` — `learn_tick` / `run_learning` / `regret_check` / `activate` / `revert`, applies the
  active model when producing signals (`model_for`, `blocked_by`, `scaled_levels`).
* DB: `learn_runs` (one row per run: report, proposal, decision, status), `signals.meta`.
* API (staff): `GET /api/admin/learning`, `GET /api/admin/learning/runs/{id}`, `POST /api/admin/learning/run`,
  `POST /api/admin/learning/runs/{id}/apply`, `POST /api/admin/learning/revert {to_base}`; settings via
  `PUT /api/admin/engine {"learning": {...}}`.
* SPA: Admin → Self-learning / Auto-învățare. Tests: `tests/test_learning.py`.

## 8. Checking auto-trading (test signal)

Connector page → **Test auto-trading**: `POST /api/test-signal` sends a signal flagged `test` to the caller's
own connector only. Requirements: demo account, connector ≥ 1.1.2, execution "auto", a bot mode on, an allowed
symbol. The connector trades it like a real signal but with at most **0.1 % risk** and refuses it on a real
account; **Close test trade** sends the close the engine would send. Skips are stored (`signal_skips`) and
shown next to the signal on the Signals page.
