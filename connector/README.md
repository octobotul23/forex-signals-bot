# FX Signals connector

Runs on **your** Windows PC (or Windows VPS) next to MetaTrader 5 and links your MT5 account to the
platform. Your broker login never leaves this computer: the platform only sends signals, and the
connector reports back your balance and trades so you can follow them in the web app.

**Version 1.3.0.** The web app's **Connector** page shows the version that is online (`v1.3.0 · updated …`).
Rapid and the news-burst options added in v1.3.0 need this version. An older connector keeps working as before:
- it sees the short-term strategy as off while Rapid is on;
- it is not sent the runs it does not know (the web app shows them skipped as "connector out of date: download the
  new connector").

**What the download holds.** `connector.py`, `run.bat`, `requirements.txt`, this README, a `connector.ini` with the
server filled in, and the `bot/` modules the connector uses:
- `risk.py`, `scalp.py`, `symbols.py`, `mt5_broker.py`;
- new in 1.3.0: `hold.py` (hold X / Y), `news_view.py` (the pre-news analysis) and `rapid.py` (Rapid).

To update, unzip the new download over the old folder, but keep your own `connector.ini`: the one in the zip has no
token.

## Setup (5 minutes)

1. Install **MetaTrader 5** from your broker and log in to a **demo account** first.
2. In MT5, turn on **Algo Trading** (toolbar button, must be green).
3. Install **Python 3.11+** from python.org (tick "Add python.exe to PATH").
4. In the web app, open **Connector**, click **Create token** and copy it.
5. Open `connector.ini` in this folder and paste the token after `token =`.
6. Double-click **run.bat**. Keep the window open while you want the bot to trade.

What happens to each signal depends on **Settings → Execution** in the web app:

| Execution | What the connector does |
|---|---|
| Notifications only | Nothing. You get the alert and decide yourself. |
| Confirm each trade | Opens the trade only when you press **Execute** on the signal in the web app. |
| Automatic | Opens trades for the modes and pairs you enabled, sized by your risk settings. |

## Safety

- Real-money accounts are **refused** unless you set `allow_real_account = yes` in `connector.ini`.
  Run at least a few weeks on demo first.
- Every trade has a stop-loss. Lot size is calculated so that hitting it loses at most your
  "risk per trade" percentage of balance; if even the broker's minimum lot would risk more, the trade is skipped.
- Daily loss limit, max open trades, max spread and max price drift are checked before every order.
- `run.bat --dry-run` logs what it would do without placing orders.
- `run.bat --paper` runs without MetaTrader at all (simulated account) to try the platform.

## News burst

With **News burst** switched on in the web app (dashboard button or Settings), the connector trades the
spike of high-impact economic releases. About a minute before the release (the server sends the plan; your
PC keeps the time from there) it places a **straddle** on the pair the release moves: buy-stop orders above
the price and sell-stop orders below it, all with the same stop-loss. The first fill cancels the other side
at once; each filled trade is closed after the hold time unless its target or stop hits first; orders that
did not fill are cancelled shortly after the release. Your "risk per trade" is the budget for the **whole
group**, split over the trades.

- **Hedging accounts** (best fit): N separate trades per side with staggered take-profits that the broker
  executes.
- **Netting accounts** (MT5 keeps one position per pair): one order per side; the connector closes the
  position in N slices at the staggered targets itself. A burst is skipped if you already hold a position
  on that pair, because it would merge with it.
- Needs MetaTrader 5 with **Algo Trading** on (paper mode skips bursts). Try it on **demo** first: the
  **Test now (demo)** button runs one on a demo account only. Spreads widen and stop orders can slip at news.
- While a burst runs on a pair, normal signals on that pair are skipped.
- The stop orders also carry an **expiry time at the broker** (a couple of minutes after the release), so
  they disappear even if your PC or the connector stops in the meantime. Brokers that refuse expiry times
  get plain orders that the connector cancels itself.
- **Clean-up**: at start-up and every 30 seconds the connector removes pending orders and closes positions
  left by a burst it no longer runs (for example after a crash or restart). They are recognisable by an
  order comment starting with `fxb:`. Rapid's signal runs use `fxr:`, and are cleaned up the same way. Don't
  place manual orders with such comments.
- **Pairs.** A release trades its currency's own pair, as before. Since 1.3.0, a pair list per currency adds two
  pairs: USD releases may also trade **XAUUSD** (gold), and GBP and JPY releases **GBPJPY**. Every pip setting is
  written for EURUSD-sized moves, so on these two pairs the connector scales it: × 5 on gold, × 2 on GBPJPY.
- **New in 1.3.0** (Settings → News burst):
  - **Stop-loss in pips, % or money:** in money, your risk is the money each event may lose. The lot is per order.
  - **Hold X / Y:** a filled trade is closed from X seconds on as soon as it is in profit, and at Y seconds at the
    latest. If the move turns against it while Y is above X, it is closed before that.
  - **Pre-news analysis:** it may pick one side, or skip the release. When switched off, it is only logged.

## Rapid (short-term → Variant: Rapid)

Rapid places **many stop-limit orders at once** next to the price. Two things start a run:
- a high-impact release ("news runs");
- a short-term signal ("signal runs", **demo accounts only**).

How it trades:
- Each order's entry is a limit a small pullback behind its trigger, so a fill is never worse than that price. When
  the price runs on without pulling back, the order expires unfilled.
- Each order carries its own stop-loss and take-profit at the broker. Fills close by the hold times X / Y.
- Netting accounts get one order per side carrying all the volume; hedging accounts get one ticket per order.
- Rapid has its own risk range, take-profit range and **daily loss limit**. When the limit is hit, only Rapid stops
  for the day.
- If your broker does not allow stop-limit orders on a pair, Rapid skips that pair. It never falls back to plain
  stop orders.

Files it keeps next to `connector.ini`:
- `rapid_day.json`: today's Rapid result;
- `rapid_open.json`: its runs that are still open, so a restart cleans up after them.

Rapid and the classic short-term never run together: switching one on turns the other off.

## Options in connector.ini

```ini
[connector]
server = https://your-platform.example.com
token = fxc_...
allow_real_account = no
# if your broker names symbols like EURUSD.m or EURUSDpro, put the ending here
symbol_suffix =
```

Trading foreign exchange on margin carries a high level of risk and may not be suitable for everyone.
Past signal performance does not guarantee future results.
