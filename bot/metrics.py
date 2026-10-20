"""Performance statistics for a list of closed trades and an equity curve."""
from collections import defaultdict


def _stats(pnls: list[float]) -> dict:
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_win, gross_loss = sum(wins), -sum(losses)
    n = len(pnls)
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(100 * len(wins) / n, 2) if n else 0.0,
        "net_profit": round(sum(pnls), 2),
        "gross_profit": round(gross_win, 2),
        "gross_loss": round(gross_loss, 2),
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss else None,  # None = no losses
        "avg_win": round(gross_win / len(wins), 2) if wins else 0.0,
        "avg_loss": round(-gross_loss / len(losses), 2) if losses else 0.0,
        "expectancy": round(sum(pnls) / n, 2) if n else 0.0,
    }


def compute_metrics(trades: list[dict], equity_curve: list[tuple], initial: float) -> dict:
    pnls = [t["pnl"] for t in trades]
    m = _stats(pnls)

    peak, max_dd, max_dd_pct = initial, 0.0, 0.0
    for _, eq in equity_curve:
        peak = max(peak, eq)
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
        if peak > 0:
            max_dd_pct = max(max_dd_pct, 100 * dd / peak)

    streak = worst = 0
    for p in pnls:
        streak = streak + 1 if p <= 0 else 0
        worst = max(worst, streak)

    final = initial + sum(pnls)
    m.update({
        "initial_balance": round(initial, 2),
        "final_balance": round(final, 2),
        "return_pct": round(100 * (final - initial) / initial, 2) if initial else 0.0,
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "max_losing_streak": worst,
        "avg_pips": round(sum(t["pips"] for t in trades) / len(trades), 1) if trades else 0.0,
    })

    by_sym = defaultdict(list)
    for t in trades:
        by_sym[t["symbol"]].append(t["pnl"])
    m["by_symbol"] = {s: _stats(p) for s, p in sorted(by_sym.items())}

    by_reason = defaultdict(int)
    for t in trades:
        by_reason[t["reason"]] += 1
    m["exit_reasons"] = dict(by_reason)
    return m
