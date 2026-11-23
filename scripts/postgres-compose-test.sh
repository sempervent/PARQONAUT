#!/usr/bin/env bash
# Automated PostgreSQL Docker Compose integration test for Tasks 3 & 4:
# - Special-character passwords (@ : / # ? %) via compose.postgres.yaml
# - No weak default passwords & no fallback to SQLite on Postgres failure
# - Zero secret leakage in container logs or HTTP error bodies
# - Liveness (200) vs Readiness (503 service_unavailable) during DB outage and automatic recovery (200)
# - pg_dump backup and psql restore with special-character credentials
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROJECT="parqonaut-pg-test-$$"
PORT="${PARQONAUT_TEST_HTTP_PORT:-18082}"
SPECIAL_PW="${PARQONAUT_TEST_PG_PASSWORD:-p@ss:w/o#rd?100%rc1}"
ADMIN_TOK="tok_pg_compose_test_$(date +%s)"
PG_USER="${PARQONAUT_TEST_PG_USER:-parqonaut}"
PG_DB="${PARQONAUT_TEST_PG_DB:-parqonaut}"
TMP_DIR="$(mktemp -d)"

cleanup() {
    PARQONAUT_PORT="$PORT" \
    PARQONAUT_PG_USER="$PG_USER" \
    PARQONAUT_PG_DB="$PG_DB" \
    PARQONAUT_PG_PASSWORD="$SPECIAL_PW" \
    PRQNT_BOOTSTRAP_ADMIN_TOKEN="$ADMIN_TOK" \
    docker compose -p "$PROJECT" -f compose.yaml -f compose.postgres.yaml --profile postgres down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

compose() {
    PARQONAUT_PORT="$PORT" \
    PARQONAUT_PG_USER="$PG_USER" \
    PARQONAUT_PG_DB="$PG_DB" \
    PARQONAUT_PG_PASSWORD="$SPECIAL_PW" \
    PRQNT_BOOTSTRAP_ADMIN_TOKEN="$ADMIN_TOK" \
    docker compose -p "$PROJECT" -f compose.yaml -f compose.postgres.yaml --profile postgres "$@"
}

echo "=== 1. Verify compose.postgres.yaml rejects missing PARQONAUT_PG_PASSWORD ==="
if env -u PARQONAUT_PG_PASSWORD PARQONAUT_PORT="$PORT" docker compose -p "$PROJECT" -f compose.yaml -f compose.postgres.yaml --profile postgres config >/dev/null 2>&1; then
    echo "FAIL: compose.postgres.yaml allowed missing PARQONAUT_PG_PASSWORD" >&2
    exit 1
fi
echo "PASS: missing PARQONAUT_PG_PASSWORD rejected"

echo "=== 2. Start PostgreSQL + PARQONAUT with special-char password ==="
if ! docker image inspect parqonaut:1.0.0-rc.1 >/dev/null 2>&1 || [[ "${PARQONAUT_COMPOSE_BUILD:-0}" == "1" ]]; then
    compose up -d --build
else
    compose up -d
fi

BASE_URL="http://127.0.0.1:${PORT}"
for _ in $(seq 1 60); do
    if curl -fsS "${BASE_URL}/api/v1/health/ready" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

READY_CODE=$(curl -s -o "$TMP_DIR/ready.json" -w "%{http_code}" "${BASE_URL}/api/v1/health/ready")
if [[ "$READY_CODE" != "200" ]]; then
    echo "FAIL: expected 200 from /api/v1/health/ready, got $READY_CODE" >&2
    compose logs
    exit 1
fi
echo "PASS: /api/v1/health/ready returned 200 with special-char PG password"

echo "=== 3. Verify authenticated API write/read on PostgreSQL ==="
curl -fsS -H "Authorization: Bearer ${ADMIN_TOK}" "${BASE_URL}/api/v1/whoami" | grep -q '"role":"admin"'
TOKEN_JSON=$(curl -fsS -X POST -H "Authorization: Bearer ${ADMIN_TOK}" -H "Content-Type: application/json" \
    -d '{"label":"compose-e2e-op","role":"operator"}' "${BASE_URL}/api/v1/admin/tokens")
OP_TOK=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["token_secret"])' <<<"$TOKEN_JSON")
curl -fsS -H "Authorization: Bearer ${OP_TOK}" "${BASE_URL}/api/v1/whoami" | grep -q '"label":"compose-e2e-op"'
echo "PASS: operator token persisted in PostgreSQL"

echo "=== 4. Verify no SQLite fallback when PostgreSQL credentials fail ==="
if PARQONAUT_PORT="$PORT" \
   PARQONAUT_PG_USER="$PG_USER" \
   PARQONAUT_PG_DB="$PG_DB" \
   PARQONAUT_PG_PASSWORD="wrong_password_check" \
   PRQNT_BOOTSTRAP_ADMIN_TOKEN="$ADMIN_TOK" \
   docker compose -p "$PROJECT" -f compose.yaml -f compose.postgres.yaml run --rm --no-deps -T parqonaut \
   serve --listen 127.0.0.1:18999 --state-dir /var/lib/parqonaut/state >"$TMP_DIR/fail_boot.log" 2>&1; then
    echo "FAIL: server unexpectedly started despite wrong PostgreSQL password (possible SQLite fallback!)" >&2
    exit 1
fi
if grep -q "sqlite" "$TMP_DIR/fail_boot.log" && grep -q "starting PARQONAUT server" "$TMP_DIR/fail_boot.log" && ! grep -q "database_backend=postgres" "$TMP_DIR/fail_boot.log"; then
    echo "FAIL: server selected sqlite instead of postgres" >&2
    cat "$TMP_DIR/fail_boot.log" >&2
    exit 1
fi
echo "PASS: PostgreSQL failure does not fall back to SQLite"

echo "=== 5. Verify DB outage -> /live=200, /ready=503 (no secrets), auto-recovery -> 200 ==="
compose stop postgres >/dev/null
LIVE_CODE=$(curl -s -o "$TMP_DIR/live_outage.json" -w "%{http_code}" "${BASE_URL}/api/v1/health/live")
READY_OUTAGE_CODE=$(curl -s -o "$TMP_DIR/ready_outage.json" -w "%{http_code}" "${BASE_URL}/api/v1/health/ready")
if [[ "$LIVE_CODE" != "200" ]]; then
    echo "FAIL: expected /api/v1/health/live 200 during postgres outage, got $LIVE_CODE" >&2
    exit 1
fi
if [[ "$READY_OUTAGE_CODE" != "503" ]]; then
    echo "FAIL: expected /api/v1/health/ready 503 during postgres outage, got $READY_OUTAGE_CODE" >&2
    cat "$TMP_DIR/ready_outage.json" >&2
    exit 1
fi
grep -q '"code":"service_unavailable"' "$TMP_DIR/ready_outage.json"
if grep -Fq "$SPECIAL_PW" "$TMP_DIR/ready_outage.json"; then
    echo "FAIL: password leaked in 503 readiness response" >&2
    exit 1
fi

compose start postgres >/dev/null
RECOVERED=0
for _ in $(seq 1 60); do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/api/v1/health/ready" || true)
    if [[ "$CODE" == "200" ]]; then
        RECOVERED=1
        break
    fi
    sleep 1
done
if [[ "$RECOVERED" != "1" ]]; then
    echo "FAIL: /api/v1/health/ready did not auto-recover to 200 after postgres restart" >&2
    exit 1
fi
echo "PASS: outage /live=200, /ready=503, auto-recovery to 200 without restarting parqonaut"

echo "=== 6. Verify pg_dump backup and psql restore with special-char password ==="
compose exec -T -e PGPASSWORD="$SPECIAL_PW" postgres pg_dump -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" --clean --if-exists > "$TMP_DIR/backup.sql"
test -s "$TMP_DIR/backup.sql"
compose exec -T -e PGPASSWORD="$SPECIAL_PW" postgres psql -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 < "$TMP_DIR/backup.sql" >/dev/null
curl -fsS -H "Authorization: Bearer ${OP_TOK}" "${BASE_URL}/api/v1/whoami" | grep -q '"label":"compose-e2e-op"'
echo "PASS: pg_dump backup & psql restore verified"

echo "=== 7. Verify zero credentials in container logs ==="
compose logs > "$TMP_DIR/compose.log" 2>&1
if grep -Fq "$SPECIAL_PW" "$TMP_DIR/compose.log" || grep -Fq "$ADMIN_TOK" "$TMP_DIR/compose.log"; then
    echo "FAIL: credential leaked into container logs!" >&2
    exit 1
fi
echo "PASS: zero credentials in docker compose logs"
