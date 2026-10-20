"""Forex bot command line.

  python main.py fetch       download history from MT5 into data/*.csv
  python main.py backtest    test the strategy on history, save a report for the dashboard
  python main.py live        run the trading loop on MT5 (demo by default)
  python main.py dashboard   open the web dashboard at http://127.0.0.1:8050
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta

from bot.config import LOGS_DIR, load_config


def setup_logging():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    LOGS_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOGS_DIR / "bot.log", encoding="utf-8")],
    )


def _symbols(cfg, args):
    return args.symbols.split(",") if args.symbols else cfg["symbols"]


def cmd_fetch(cfg, args):
    from bot.data import save_csv
    from bot.mt5_broker import MT5Broker

    bt = cfg["backtest"]
    start = datetime.fromisoformat(str(args.start or bt.get("start", "2023-01-01")))
    end = datetime.fromisoformat(str(args.end or bt.get("end"))) + timedelta(days=1) if (args.end or bt.get("end")) \
        else datetime.now()
    broker = MT5Broker()
    broker.connect()
    try:
        for sym in _symbols(cfg, args):
            df = broker.rates_range(sym, cfg["timeframe"], start, end)
            path = save_csv(df, sym, cfg["timeframe"])
            print(f"{sym}: {len(df)} bars {df['time'].iat[0]} -> {df['time'].iat[-1]}  saved to {path}")
            if df["time"].iat[0] > start + timedelta(days=7):
                print(f"  note: MT5 returned less history than requested. Raise Tools > Options > Charts > "
                      f"'Max bars in chart' in the terminal, or scroll the {sym} chart back to load more.")
    finally:
        broker.shutdown()


def cmd_backtest(cfg, args):
    from bot.backtest import Backtester, save_report
    from bot.data import load_csv, slice_dates
    from bot.symbols import spec_from_config
    from strategies import load_strategy

    bt = cfg["backtest"]
    symbols = _symbols(cfg, args)
    start, end = args.start or bt.get("start"), args.end or bt.get("end")
    source = args.source or bt.get("source", "csv")
    data, specs = {}, {}

    if source == "mt5":
        from bot.mt5_broker import MT5Broker
        broker = MT5Broker()
        broker.connect()
        try:
            for s in symbols:
                end_dt = datetime.fromisoformat(str(end)) + timedelta(days=1) if end else datetime.now()
                data[s] = broker.rates_range(s, cfg["timeframe"], datetime.fromisoformat(str(start)), end_dt)
                specs[s] = broker.symbol_spec(s)
        finally:
            broker.shutdown()
    else:
        from pathlib import Path
        data_dir = Path(args.data_dir) if args.data_dir else None
        for s in symbols:
            df = load_csv(s, cfg["timeframe"], data_dir) if data_dir else load_csv(s, cfg["timeframe"])
            data[s] = slice_dates(df, start, end)
            specs[s] = spec_from_config(s, cfg)

    strategy = load_strategy(args.strategy or cfg["strategy"], cfg["strategy_params"] if not args.strategy else None)
    print(f"Backtesting {strategy.name} on {', '.join(symbols)} {cfg['timeframe']} "
          f"({sum(len(d) for d in data.values())} bars)...")
    report = Backtester(cfg, strategy, data, specs).run()
    path, csv = save_report(report)
    print_summary(report)
    print(f"\nReport: {path}\nTrades: {csv}\nView it:  python main.py dashboard")


def print_summary(r):
    m = r["metrics"]
    pf = m["profit_factor"] if m["profit_factor"] is not None else "n/a"
    rows = [
        ("Period", f"{r['period'][0]}  ->  {r['period'][1]}"),
        ("Trades", f"{m['trades']}  (won {m['wins']}, lost {m['losses']})"),
        ("Win rate", f"{m['win_rate']}%"),
        ("Net profit", f"{m['net_profit']:,.2f}  ({m['return_pct']:+.2f}%)"),
        ("Final balance", f"{m['final_balance']:,.2f}"),
        ("Profit factor", pf),
        ("Max drawdown", f"{m['max_drawdown']:,.2f}  ({m['max_drawdown_pct']}%)"),
        ("Avg win / loss", f"{m['avg_win']:,.2f} / {m['avg_loss']:,.2f}"),
        ("Expectancy/trade", f"{m['expectancy']:,.2f}"),
        ("Worst losing streak", m["max_losing_streak"]),
    ]
    print("\n" + "-" * 56)
    for k, v in rows:
        print(f"  {k:<20} {v}")
    if m["by_symbol"]:
        print("  " + "-" * 52)
        for s, x in m["by_symbol"].items():
            print(f"  {s:<10} {x['trades']:>4} trades  win {x['win_rate']:>5}%  net {x['net_profit']:>12,.2f}")
    if r["skipped_signals"]:
        print("  Skipped signals: " + ", ".join(f"{k}: {v}" for k, v in r["skipped_signals"].items()))
    print("-" * 56)


def cmd_live(cfg, args):
    from bot.live import LiveTrader
    from strategies import load_strategy

    if args.dry_run:
        cfg["live"]["dry_run"] = True
    if args.symbols:
        cfg["symbols"] = args.symbols.split(",")
    LiveTrader(cfg, load_strategy(cfg["strategy"], cfg["strategy_params"])).run()


def cmd_dashboard(cfg, args):
    from dashboard.app import create_app
    d = cfg["dashboard"]
    host, port = d.get("host", "127.0.0.1"), int(args.port or d.get("port", 8050))
    print(f"Dashboard: http://{host}:{port}")
    create_app(cfg).run(host=host, port=port, debug=False)


def cmd_serve(cfg, args):
    """The subscription platform (web app + API + signal engine). One process: the engine runs inside it."""
    import os
    import uvicorn
    if args.demo_speed:
        os.environ["DEMO_SECONDS_PER_BAR"] = str(args.demo_speed)
    print(f"Platform: http://{args.host}:{args.port}")
    uvicorn.run("server.main:app", host=args.host, port=args.port, workers=1, proxy_headers=True,
                forwarded_allow_ips="127.0.0.1", log_level="info")


def main():
    setup_logging()
    p = argparse.ArgumentParser(description="Forex trading bot (MetaTrader 5)")
    p.add_argument("--config", help="path to config file (default config.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="download price history from MT5 to data/")
    b = sub.add_parser("backtest", help="test the strategy on historical data")
    for sp in (f, b):
        sp.add_argument("--symbols", help="comma-separated, e.g. EURUSD,GBPUSD")
        sp.add_argument("--start", help="YYYY-MM-DD")
        sp.add_argument("--end", help="YYYY-MM-DD")
    b.add_argument("--source", choices=["csv", "mt5"])
    b.add_argument("--strategy", help="strategy file name in strategies/ (uses its default params)")
    b.add_argument("--data-dir", help="folder with SYMBOL_TIMEFRAME.csv files (default data/)")

    lv = sub.add_parser("live", help="run the trading loop on MT5")
    lv.add_argument("--dry-run", action="store_true", help="force dry run (signals only)")
    lv.add_argument("--symbols")

    d = sub.add_parser("dashboard", help="web dashboard")
    d.add_argument("--port", type=int)

    sv = sub.add_parser("serve", help="run the subscription platform server (web app, API, signal engine)")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8000)
    sv.add_argument("--demo-speed", type=float, help="demo feed only: seconds per bar (e.g. 5) to see signals quickly")

    args = p.parse_args()
    cfg = load_config(args.config)
    {"fetch": cmd_fetch, "backtest": cmd_backtest, "live": cmd_live, "dashboard": cmd_dashboard,
     "serve": cmd_serve}[args.cmd](cfg, args)


if __name__ == "__main__":
    main()
