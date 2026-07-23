#!/usr/bin/env bash
# Run the end-to-end replay test against a fresh database and server.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DB_NAME="kitaru_e2e"
PORT="${KITARU_E2E_PORT:-8300}"
API_URL="http://127.0.0.1:${PORT}"
SERVER_LOG="$(mktemp -t kitaru-e2e-server)"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "[run_e2e] Starting database container"
docker compose up -d db
for _ in $(seq 1 60); do
    if docker compose exec -T db pg_isready -U postgres >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
docker compose exec -T db pg_isready -U postgres >/dev/null

echo "[run_e2e] Recreating database ${DB_NAME}"
docker compose exec -T db psql -U postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE);" \
    -c "CREATE DATABASE ${DB_NAME};" >/dev/null

echo "[run_e2e] Starting server on ${API_URL} (log: ${SERVER_LOG})"
KITARU_SERVER_DB_HOST=localhost \
KITARU_SERVER_DB_PORT=5433 \
KITARU_SERVER_DB_USER=postgres \
KITARU_SERVER_DB_PWD=password \
KITARU_SERVER_DB_NAME="$DB_NAME" \
KITARU_SERVER_HOST=127.0.0.1 \
KITARU_SERVER_PORT="$PORT" \
KITARU_SERVER_AUTH_SCHEME=local \
KITARU_SERVER_JWT_SIGNING_KEY=e2e-signing-key \
KITARU_SERVER_SECRET_ENCRYPTION_KEY=e2e-encryption-key \
KITARU_SERVER_DEFAULT_ACCOUNT_NAME=default \
KITARU_SERVER_DEFAULT_ACCOUNT_PASSWORD=password \
KITARU_SERVER_LOG_LEVEL=INFO \
    uv run python -m kitaru.server.api.main >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo "[run_e2e] Waiting for ${API_URL}/health"
HEALTHY=0
for _ in $(seq 1 60); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break
    fi
    if curl -fsS "${API_URL}/health" >/dev/null 2>&1; then
        HEALTHY=1
        break
    fi
    sleep 1
done
if [[ "$HEALTHY" -ne 1 ]]; then
    echo "[run_e2e] Server did not become healthy, log tail:"
    tail -50 "$SERVER_LOG"
    echo "[run_e2e] FAIL"
    exit 1
fi

echo "[run_e2e] Running the e2e driver"
EXIT_CODE=0
KITARU_E2E_API_URL="$API_URL" uv run python scripts/e2e_test.py || EXIT_CODE=$?

if [[ "$EXIT_CODE" -eq 0 ]]; then
    echo "[run_e2e] PASS"
else
    echo "[run_e2e] Server log tail:"
    tail -50 "$SERVER_LOG"
    echo "[run_e2e] FAIL"
fi
exit "$EXIT_CODE"
