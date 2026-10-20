"""Historical price data: CSV files in data/ (from `main.py fetch` or MT5's 'Export bars')."""
from pathlib import Path

import pandas as pd

from .config import DATA_DIR


def csv_path(symbol: str, timeframe: str, data_dir: Path = DATA_DIR) -> Path:
    return Path(data_dir) / f"{symbol}_{timeframe}.csv"


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Accepts our own CSVs and MetaTrader 'Export bars' files (<DATE> <TIME> <OPEN> ...)."""
    df = df.rename(columns=lambda c: str(c).strip().strip("<>").lower())
    if "time" in df.columns and "date" in df.columns:
        df["time"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    elif "date" in df.columns:
        df["time"] = pd.to_datetime(df["date"])
    elif "time" in df.columns:
        if pd.api.types.is_numeric_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"], unit="s")
        else:
            df["time"] = pd.to_datetime(df["time"])
    else:
        raise ValueError("CSV needs a 'time' (or DATE + TIME) column")
    if "tickvol" in df.columns and "tick_volume" not in df.columns:
        df["tick_volume"] = df["tickvol"]
    missing = {"open", "high", "low", "close"} - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {sorted(missing)}")
    keep = [c for c in ("time", "open", "high", "low", "close", "tick_volume", "spread") if c in df.columns]
    return df[keep].sort_values("time").drop_duplicates("time").reset_index(drop=True)


def load_csv(symbol: str, timeframe: str, data_dir: Path = DATA_DIR) -> pd.DataFrame:
    path = csv_path(symbol, timeframe, data_dir)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run `python main.py fetch` (needs MT5) or put an exported CSV there."
        )
    sep = "\t" if path.read_text(encoding="utf-8", errors="ignore")[:200].count("\t") > 2 else ","
    return normalize(pd.read_csv(path, sep=sep))


def save_csv(df: pd.DataFrame, symbol: str, timeframe: str, data_dir: Path = DATA_DIR) -> Path:
    path = csv_path(symbol, timeframe, data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def slice_dates(df: pd.DataFrame, start=None, end=None) -> pd.DataFrame:
    if start:
        df = df[df["time"] >= pd.Timestamp(start)]
    if end:
        df = df[df["time"] < pd.Timestamp(end) + pd.Timedelta(days=1)]
    return df.reset_index(drop=True)
