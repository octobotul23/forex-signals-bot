"""Per-symbol contract details needed to turn price moves into money."""
from dataclasses import dataclass

# Pip-denominated settings (offsets, stops, spread caps) are written for EURUSD-like volatility; on these symbols
# they are multiplied by the factor, matched on the name without the broker's suffix (XAUUSD.m is XAUUSD).
SYMBOL_SCALE = {"XAUUSD": 5.0, "GBPJPY": 2.0}


def pip_size(symbol: str) -> float:
    """One pip in price units: 0.1 for gold, 0.01 for JPY pairs, else 0.0001. The single rule for server and connector."""
    s = symbol.upper()
    if "XAU" in s:
        return 0.1
    return 0.01 if "JPY" in s else 0.0001


def price_digits(symbol: str) -> int:
    """Decimals a price of this symbol is shown with: 2 for gold, 3 for JPY pairs, else 5."""
    s = symbol.upper()
    if "XAU" in s:
        return 2
    return 3 if "JPY" in s else 5


def symbol_scale(symbol: str) -> float:
    s = symbol.upper()
    return next((f for name, f in SYMBOL_SCALE.items() if s.startswith(name)), 1.0)


@dataclass
class SymbolSpec:
    symbol: str
    pip: float                  # price size of one pip (pip_size: 0.0001, 0.01 for JPY pairs, 0.1 for gold)
    pip_value_per_lot: float    # account-currency value of one pip on 1.0 lot
    digits: int = 5
    point: float = 0.00001
    volume_min: float = 0.01
    volume_step: float = 0.01
    volume_max: float = 100.0
    min_stop_distance: float = 0.0   # broker "stops level" in price units
    freeze_distance: float = 0.0     # broker "freeze level" in price units: orders this close cannot be changed
    scale: float = 1.0               # symbol_scale: multiplier for pip-denominated settings


def spec_from_config(symbol: str, cfg: dict) -> SymbolSpec:
    """Fallback spec for backtests without a broker connection."""
    table = cfg.get("symbol_specs", {})
    merged = {**table.get("default", {}), **table.get(symbol, {})}
    pip = float(table.get(symbol, {}).get("pip", pip_size(symbol)))   # the default row's pip is an FX pip
    digits = price_digits(symbol)
    return SymbolSpec(
        symbol=symbol,
        pip=pip,
        pip_value_per_lot=float(merged.get("pip_value_per_lot", 10.0)),
        digits=digits,
        point=10.0 ** -digits,
        scale=symbol_scale(symbol),
    )
