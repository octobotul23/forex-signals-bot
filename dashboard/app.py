"""Local web dashboard: reads the bot's SQLite store and the latest backtest report."""
import json
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

from bot.config import REPORTS_DIR
from bot.store import Store


def create_app(cfg: dict) -> Flask:
    app = Flask(__name__)
    store = Store()

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/live")
    def live():
        status, updated = store.get_state("status", {})
        control, _ = store.get_state("control", {"paused": False})
        age = None
        if updated:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(updated)).total_seconds()
        poll = (status or {}).get("poll_seconds", 10)
        online = bool(status and status.get("running") and age is not None and age < poll * 3 + 30)
        return jsonify({
            "online": online, "updated": updated, "status": status or {}, "control": control or {"paused": False},
            "trades": store.trades(100), "equity": store.equity(2000), "events": store.events(40),
            "config": {"symbols": cfg.get("symbols"), "timeframe": cfg.get("timeframe"),
                       "strategy": cfg.get("strategy"), "risk": cfg.get("risk"),
                       "dry_run": cfg["live"].get("dry_run", True)},
        })

    @app.post("/api/control")
    def control():
        paused = bool((request.get_json(silent=True) or {}).get("paused"))
        store.set_state("control", {"paused": paused})
        store.event("warning" if paused else "info", "Paused from dashboard" if paused else "Resumed from dashboard")
        return jsonify({"paused": paused})

    @app.get("/api/backtest")
    def backtest():
        path = REPORTS_DIR / "latest_backtest.json"
        if not path.exists():
            return jsonify(None)
        report = json.loads(path.read_text(encoding="utf-8"))
        report["trades"] = report["trades"][-300:]
        return jsonify(report)

    return app
