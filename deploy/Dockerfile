# Optional container build for a Linux VPS (PRICE_FEED=twelvedata). Build from the forex-bot folder:
#   docker compose -f deploy/docker-compose.yml up -d --build
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements-server.txt .
RUN pip install --no-cache-dir -r requirements-server.txt
COPY bot ./bot
COPY strategies ./strategies
COPY server ./server
COPY connector ./connector
COPY main.py manage.py config.yaml ./
RUN useradd -r -u 1000 fx && mkdir -p data logs && chown -R fx /app/data /app/logs
USER fx
EXPOSE 8000
CMD ["python", "main.py", "serve", "--host", "0.0.0.0", "--port", "8000"]
