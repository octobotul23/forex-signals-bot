# FX Signals: forex bot + subscription platform

Two things live in this folder:

1. **Personal bot (v1)**: backtest and run a strategy on your own MetaTrader 5 account (`main.py fetch | backtest | live | dashboard`).
2. **Subscription platform**: a web app you host on a VPS. Clients log in, get live signals from three bot modes,
   learn in the Academy, chat with an AI coach, and (optionally) let a small **connector** trade for them on their own MT5.

> Trading forex with leverage carries a high risk of losing money. Everything ships in safe mode:
> demo accounts only, paper/dry-run options, and real accounts refused unless explicitly allowed.

## Forex Bot app (Windows, one window)

Double-click **Forex Bot** (Desktop / Start menu / this folder; or `Forex Bot.bat`, which sets everything up the first
time). `launcher/forexbot.py` runs the platform server and then the MT5 connector (`connector/connector.ini`) as
background processes, restarts either if it stops (3 s … 5 min pauses) and the server if it stops answering (~2 min,
or 5 min after a start), shows the server, connector and MetaTrader 5 state (Algo Trading, balance, open positions,
latest trades and bursts) and opens the web app in its own Edge window. Closing the window asks first; "Stop all"
stops trading but keeps the window (both ask first while a news burst runs; a double-click is one click, and no
button takes a second command before the first is carried out; closing during a restart asks too). The window's
title shows the release that runs, **Forex Bot V1.3.0** (`launcher/forexbot.py` `window_title`, from `bot/version.py`
`APP_VERSION`, the same "V1.3.0" the web app's header shows; read when the window opens, so after an update it changes
once the app is closed and opened again: Restart starts the server and the connector anew, not the window); the
connector it runs is 1.3.0. The
connector is stopped cleanly (`--stop-on-stdin-eof`, since connector 1.2.0; Ctrl+C in its console does the same, a third
Ctrl+C stops at once): new signals and bursts are refused, a running burst cancels its orders and closes its
positions (each on its own: one refused close does not keep the others open), what is left is swept until nothing
is, a result being reported when the stop comes is handed to that sweep (never lost), a burst still only scheduled
is left to the next start, and what could not be removed within 40 s is reported (the app waits 50 s before it ends
the connector; with no burst to end and nothing left to report, one sweep is all); the server is stopped outright. A
double-click on a shortcut also brings the packages up to date when `requirements-lock.txt` changed (it runs
`install.ps1`; one that fails keeps its console open a minute and shows in the window's last problem, and the bot
starts with the packages it has: nothing waits for a click). The processes sit in a Windows job object, so they end with the app even if it crashes
(MT5 stays open; the connector cleans up a burst left behind at its next start). While the bot runs the PC is kept awake
(checkbox). Logs: `logs/app.log`. First-time setup on a new PC: `launcher/install.ps1` (venv from
`requirements-lock.txt` + shortcuts). Romanian quick guide: `CITESTE-MA.txt`. Tests: `tests/test_launcher.py`.

## Quick start (local)

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
copy .env.example .env
.venv\Scripts\python main.py serve --demo-speed 4
```

Open http://127.0.0.1:8000. The first visit asks for the **setup token**, printed in the console
and saved to `data/setup_token.txt`. Use it to create the **founder** account.
`--demo-speed 4` makes the synthetic demo feed produce a new bar every 4 seconds, so you see signals quickly.

## Platform overview

| Area | What it does |
|---|---|
| **Bot modes** | **Trend** (EMA cross with ATR stop/target, backtested), **Custom rules** (`strategies/my_strategy.py`, waiting for your rules), **AI news** (economic calendar + headlines analysed by Claude, trades only on strong evidence) |
| **Settings (per client)** | Execution: *notifications only*, *confirm each trade* or *automatic*; modes on/off; symbols; risk % per trade; max open trades; daily loss limit; max spread; min AI confidence; Telegram alerts. (Skipping signals around high-impact news is set for everyone in Admin → Engine) |
| **Market & news** | Economic calendar (ForexFactory weekly feed, or Investing.com with the released values: `CALENDAR_SOURCE`, see Configuration) and RSS headlines, refreshed automatically. The AI tags each headline and gives a bias per pair |
| **Live updates** | WebSocket: new signals, results, prices, connector status and trades appear without reloading |
| **Academy + AI coach** | Bilingual lessons (EN/RO) and a coach that knows the client's settings and the current market |
| **Roles** | **Client**; **Admin** (ban/unban, cut or extend subscriptions, reset passwords, engine settings); **Founder** (everything, manages admins). The founder can be deleted only by the founder |
| **Connector** | Windows app next to the client's MT5. Gets signals over WSS, checks risk, places orders. Broker logins stay on the client's PC |
| **News burst** | One ON/OFF button: before each high-impact release the connector straddles the pair with stop orders and exits in steps (see below) |
| **Short-term** | Settings → Short-term, off by default, its own risk range and daily loss limit. **Variant** Classic: the connector scalps on live ticks, one trade per pair at a time ([docs/SHORT_TERM.md](docs/SHORT_TERM.md)). **Variant** Rapid (v1.3.0): many stop-limit orders at once next to the price, at a high-impact release or on a short-term signal (demo accounts only), each order with its own stop-loss and take-profit at the broker ([docs/RAPID.md](docs/RAPID.md)). Only one of the two runs; Rapid needs connector 1.3.0 |
| **Version** | The web app's header shows the release, **V1.3.0** (`bot/version.py` `APP_VERSION`, `GET /api/public/info`), as does the Forex Bot window's title; the Connector page shows the client's connector version (`v1.3.0`) |
| **Self-learning** | Admin → Self-learning: replays the trend strategy on real history, labels why trades lost, tests stop/target and "don't trade when…" corrections on data not used to choose them; *suggest* (default) or *automatic* mode, daily regret check with auto-revert ([docs/SELF_LEARNING.md](docs/SELF_LEARNING.md)) |
| **Auto-trading check** | Connector → *Test auto-trading*: sends a test signal to your own connector (demo only, ≤ 0.1 % risk) and shows whether it opened, then closes it |

### How a trade flows

```
engine (VPS) ── new signal ──> client's browser (alert)  +  Telegram (optional)
                         └───> client's connector ── checks: execution mode, symbols, price drift,
                                                     spread, daily loss, max open, lot size
                                                  └─> MT5 order (SL/TP re-anchored to broker price)
                                                  └─> reports open/close + P&L back to the platform
```

## News burst mode

A one-button mode for every client: the big **ON/OFF** button on the Dashboard (same switch as **Settings → News burst**).
Before each high-impact release in the economic calendar, the client's connector places a **straddle** on the pair
the release moves: N buy-stop orders above the price and N sell-stop orders below it, all with the same stop-loss and
**staggered take-profits**. The spike fills one side and the other side is cancelled at once. Each filled trade is closed
after the hold time unless its TP or SL hits first; unfilled orders are cancelled shortly after the release.

| Setting | Default | Meaning |
|---|---|---|
| Trades per burst | 10 (1–40, capped by admin) | positions per burst; orders placed = 2 × trades |
| Place orders before release | 60 s | keep ≥ 30 s for big bursts: each order takes ~0.1–0.3 s, so 40 trades need 10–25 s |
| Order distance / Stop-loss | 7 / 10 pips | distance of the stop orders from bid/ask; the same SL for every order |
| Stop-loss in (v1.3.0) | % of balance (new users; pips for a burst saved before v1.3.0) | pips: the SL above, sized from *Risk per trade*; % or money: the most the whole burst may lose at its stops (2 %), with a fixed lot per order (0.10) |
| First TP / TP step | 4 / 1.5 pips | trade i takes profit at first TP + i × step |
| Close if in profit after (X) / Hold at most (Y) / Cancel unfilled after | 30 s / 120 s / 90 s | from X on a filled trade in profit is closed, at Y any (Y = X: closed at X, as up to v1.2.0; a burst saved before v1.3.0 has no Y); expiry of unfilled orders after the release |
| Pre-news analysis (v1.3.0) | on (new users; off for a burst saved before v1.3.0) | reads the bars, the event's forecast and the AI's bias before the release and may place one side only or skip it; off, it only logs what it would have done |
| Currencies | all | USD and EUR trade EURUSD; GBP, JPY, AUD, NZD, CAD, CHF trade their USD pair. Since v1.3.0 each currency has a list of pairs (USD: EURUSD, XAUUSD, GBPUSD, USDJPY; GBP: GBPUSD, GBPJPY; JPY: USDJPY, GBPJPY): **Pairs at a release** picks among them, none picked = the currency's own pair as before |

- **Risk:** the client's *Risk per trade* % is the budget for the **whole group**, split over its trades.
- **Which events:** high impact only. Speeches, minutes, press conferences, hearings, auctions and similar are skipped (no single release moment).
- **Netting accounts** (one position per pair in MT5): one position split into N slices, closed slice by slice at the staggered targets.
  **Hedging accounts:** N separate trades. Exchange accounts are not supported.
- **Safety:** demo only unless `allow_real_account = yes` in `connector.ini`; **Test now (demo)** on the Dashboard always runs on demo.
  Spreads widen and stop orders can slip at news time.
- The live and past bursts show on the Dashboard and in **My trades → Bursts**; burst trades carry a ⚡ #id tag, and the
  calendar marks the events a client's burst will trade.
- **Admin → Engine → News burst**: kill switch for everyone and the max trades per burst.
- **Gold and GBPJPY** (v1.3.0): every pip setting is written for EURUSD-sized moves and scaled on these pairs (× 5 on
  XAUUSD, × 2 on GBPJPY; `bot/symbols.py` `SYMBOL_SCALE`). Both are in the default traded pairs; an admin's saved list
  gets them once at the first v1.3.0 start.
- **Rapid** (Settings → Short-term → Variant: Rapid) takes a release's pairs first; the classic burst then trades
  another pair of that release, never the same one ([docs/RAPID.md](docs/RAPID.md)).
- Needs **connector 1.1.0** or newer; the v1.3.0 options (stop in % or money, Y, the analysis), GBPJPY / XAUUSD and
  Rapid's runs need **connector 1.3.0**. Older connectors get those bursts skipped with a "download the new connector"
  note.

## Commands

| Command | Purpose |
|---|---|
| `python main.py serve [--host 127.0.0.1] [--port 8000] [--demo-speed N]` | Run the platform |
| `python manage.py create-founder --email you@example.com` | Create the founder from the console |
| `python manage.py reset-password --email …` | New password for a user |
| `python manage.py set-role --email … --role admin\|client` | Change a role |
| `python manage.py extend --email … --days 30` | Extend a subscription |
| `python manage.py unban --email …` / `list-users` | Unban / list accounts |
| `python main.py backtest [--strategy example_ema_cross]` | v1: backtest a strategy (report in `reports/`) |
| `python main.py live [--dry-run]` | v1: trading loop on your own MT5 (`config.yaml`) |
| `python main.py dashboard` | v1: local dashboard for the personal bot |

## Connector (clients)

In the web app: **Connector → Create token → Download**. The zip contains `connector.ini` already filled in with
the server address (paste the token after `token =`), `connector.py`, `run.bat` and the `bot/` modules it imports
(`hold.py`, `news_view.py`, `rapid.py` new in 1.3.0, beside `risk.py`, `scalp.py`, `symbols.py`, `mt5_broker.py`).
On Windows with MT5 open: run `run.bat`. Options: `--paper` (simulated account, no MT5 needed), `--dry-run` (logs
what it would do). See `connector/README.md`.
Current version **1.3.0** (1.3.0 or newer is needed for Rapid, the v1.3.0 news-burst options and GBPJPY / XAUUSD,
1.2.0 for the short-term strategy, 1.1.2 for the auto-trading check); after an update, copy the new files over the old
folder keeping `connector.ini`.

## Configuration

All server settings are in `.env` (see `.env.example`). The important ones:
`SECRET_KEY`, `COOKIE_SECURE=true` + `PUBLIC_URL=https://…` in production, `PRICE_FEED` (`demo`, `mt5` or `twelvedata`),
`ANTHROPIC_API_KEY` (enables AI news + coach) and `TELEGRAM_BOT_TOKEN`.
`CALENDAR_SOURCE=investing` + `APIFY_TOKEN` switch the economic calendar from the ForexFactory weekly feed (no
released values) to Investing.com through the Apify actor `kibaale/investing-economic-calendar-scraper`. Apify bills
every row, so the engine asks for High/Medium releases only (the next 7 days every `INVESTING_FULL_HOURS`, today's
High releases every `INVESTING_TODAY_MINUTES` and again just after one prints) and falls back to ForexFactory while
Investing.com does not answer. Investing.com's terms forbid using its data without its written permission.
Tests: `tests/test_calendar.py`.
Engine settings (symbols, timeframe, AI interval, trial days, sign-up open/closed) are edited live in **Admin → Engine**.

## Deploying

See [deploy/DEPLOY.md](deploy/DEPLOY.md): Linux VPS with Caddy + systemd (or Docker), or a Windows VPS with MT5.

## Folder map

```
server/        platform: FastAPI app (main.py), engine.py, ai.py, auth, db, feeds/, web/ (single-page app)
connector/     client-side trader (connector.py, run.bat)
launcher/      Forex Bot app: forexbot.py (window + process supervisor), install.ps1, icon
bot/           shared trading code: risk manager, MT5 broker, backtester, v1 live loop; scalp.py (short-term),
               rapid.py (Rapid), hold.py (hold X / Y), news_view.py (pre-news analysis), symbols.py, version.py
docs/          NEWS_BURST.md, SHORT_TERM.md, RAPID.md, SELF_LEARNING.md (specs)
strategies/    example_ema_cross.py (Trend mode), my_strategy.py (your rules)
deploy/        Caddyfile, systemd unit, Dockerfile, docker-compose, DEPLOY.md
data/          databases, setup token, secret key (never commit)
logs/          app.log (Forex Bot app), server.log (platform), bot.log (v1 bot)
```
