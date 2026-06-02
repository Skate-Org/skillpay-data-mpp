FROM python:3.12-slim
WORKDIR /app

# deps first for layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY databento_sidecar.py ./

# NDJSON TCP feed port (override with SIDECAR_PORT)
EXPOSE 5051

# Bind 0.0.0.0 so the stream container can reach it over the compose network.
# DATABENTO_API_KEY must be provided via env (see .env.example) — the script
# aborts without it. --strikes-window controls the ATM strike depth per side.
CMD ["sh", "-c", "python databento_sidecar.py --host 0.0.0.0 --port ${SIDECAR_PORT:-5051} --strikes-window ${STRIKES_WINDOW:-40}"]
