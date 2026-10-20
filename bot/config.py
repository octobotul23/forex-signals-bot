"""Loads config.yaml and .env, and defines the project folders."""
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
LOGS_DIR = ROOT / "logs"
DB_PATH = DATA_DIR / "bot.db"


def load_config(path: str | None = None) -> dict:
    load_dotenv(ROOT / ".env")
    cfg_path = Path(path) if path else ROOT / "config.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    for key in ("risk", "backtest", "live", "dashboard", "symbol_specs", "strategy_params"):
        cfg.setdefault(key, {})
    return cfg
