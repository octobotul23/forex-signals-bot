# Deploying the platform on a VPS

The server is one Python process: web app + API + live WebSockets + the signal engine.
Clients connect from their browsers, and their **connector** (on their own Windows PC next to MT5)
connects over a secure WebSocket. Broker logins never reach the server.

```
 clients' browsers ──HTTPS/WSS──┐
                                ├──> Caddy (443, auto HTTPS) ──> platform :8000 (127.0.0.1)
 clients' connectors ──WSS──────┘                                   │
   (Windows PC + MT5)                                               ├─ price feed (Twelve Data / MT5)
                                                                    ├─ ForexFactory calendar + RSS
                                                                    └─ Claude API (AI analysis, coach)
```

## Option A: Linux VPS (recommended, cheapest)

Any Ubuntu 24.04 VPS with 1 vCPU / 1–2 GB RAM is enough to start. Prices come from **Twelve Data**
(`PRICE_FEED=twelvedata`); the free plan covers a few pairs on H1.

```bash
# as root
adduser --system --group --home /opt/forex-bot fx
apt update && apt install -y python3-venv caddy git
# copy the forex-bot folder to /opt/forex-bot (git clone, scp or rsync), then:
cd /opt/forex-bot
python3 -m venv .venv && .venv/bin/pip install -r requirements-server.txt
cp .env.example .env && nano .env          # SECRET_KEY, COOKIE_SECURE=true, PUBLIC_URL=https://your-domain,
                                           # PRICE_FEED=twelvedata, TWELVEDATA_API_KEY, ANTHROPIC_API_KEY, ...
mkdir -p data logs && chown -R fx:fx /opt/forex-bot
cp deploy/fx-platform.service /etc/systemd/system/ && systemctl enable --now fx-platform
cp deploy/Caddyfile /etc/caddy/Caddyfile && nano /etc/caddy/Caddyfile   # your domain
systemctl reload caddy
journalctl -u fx-platform -n 30            # shows the one-time SETUP TOKEN
```

Point your domain's DNS **A record** to the VPS IP, open `https://your-domain`, paste the setup token and
create the **founder** account. (Alternative: `sudo -u fx .venv/bin/python manage.py create-founder --email you@…`.)

Firewall: allow only 22, 80 and 443 (`ufw allow OpenSSH; ufw allow 80; ufw allow 443; ufw enable`).

**Docker instead:** edit `deploy/Caddyfile.docker` (domain) and `.env`, then
`docker compose -f deploy/docker-compose.yml up -d --build` and `docker compose -f deploy/docker-compose.yml logs app` for the token.

## Option B: Windows VPS with MetaTrader 5 as the price feed

Use this if you want the engine to read prices from the same broker feed your clients trade on.

1. Install MT5 and log in (a demo account is fine for prices). Keep it running.
2. Install Python 3.12, copy the folder, `python -m venv .venv`, `.venv\Scripts\pip install -r requirements.txt`.
3. `.env`: `PRICE_FEED=mt5`, plus the same settings as above.
4. Run as a service with NSSM (`nssm install fx-platform C:\fx\forex-bot\.venv\Scripts\python.exe main.py serve`).
5. Install Caddy for Windows with the same Caddyfile for HTTPS.

## Backups

Everything lives in `data/platform.db` (SQLite). Copy it nightly, e.g. cron:
`sqlite3 /opt/forex-bot/data/platform.db ".backup '/opt/backups/platform-$(date +\%F).db'"`.
For hundreds of clients, switch to PostgreSQL with `DATABASE_URL` (uncomment psycopg in requirements-server.txt).

## Run exactly one process

The engine runs inside the web process, so start the server with **one worker** (the systemd unit
and Docker image already do). Several workers would generate duplicate signals.

## Updating

```bash
cd /opt/forex-bot && git pull && .venv/bin/pip install -r requirements-server.txt && systemctl restart fx-platform
```

Clients' browsers reconnect automatically within seconds; connectors reconnect with back-off.
