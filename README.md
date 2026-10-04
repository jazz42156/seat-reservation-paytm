# Seat Reservation

Minimal service skeleton. The process exposes a liveness check and does not reserve seats yet.

## Run locally

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 8000
```

On macOS or Linux, use `.venv/bin/pip` and `.venv/bin/uvicorn` instead.

Liveness: [http://127.0.0.1:8000/health/live](http://127.0.0.1:8000/health/live)

```json
{"status": "alive"}
```

## Run with Docker

```bash
docker compose up --build
```

The API listens on port 8000. Postgres 16 is started for later steps. This step does not connect to it.

Copy `.env.example` to `.env` when you want local environment variables. The liveness check does not require them.
