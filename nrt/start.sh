#!/usr/bin/env bash
# =============================================================================
# start.sh — Start / stop the local NRT MDM container stack
#
# Usage:
#   bash nrt/start.sh              # build + start, wait until healthy (= up)
#   bash nrt/start.sh up [--no-build]
#   bash nrt/start.sh down         # stop containers (keep Postgres data)
#   bash nrt/start.sh reset        # stop + delete Postgres volume, then start fresh
#   bash nrt/start.sh status       # container state + endpoint check
#   bash nrt/start.sh logs [svc]   # follow logs (all or one service)
# =============================================================================
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$PROJECT_DIR/docker-compose.yml")
WAIT_TIMEOUT="${WAIT_TIMEOUT:-240}"

ENDPOINTS=(
  "MDM REST API|http://localhost:8000/api/v1/health"
  "API docs|http://localhost:8000/docs"
  "Golden Record Viewer|http://localhost:8501"
  "Audit Log Viewer|http://localhost:8502"
  "Kafka UI|http://localhost:8080"
)

log() { echo "[nrt] $*"; }
die() { echo "[nrt] ERROR: $*" >&2; exit 1; }

preflight() {
  command -v docker >/dev/null || die "docker not found"
  docker info >/dev/null 2>&1 || die "Docker daemon not running"
  docker compose version >/dev/null 2>&1 || die "docker compose plugin missing"
  # host ports published by the stack; only checked when the stack is not running yet
  [[ -n "$("${COMPOSE[@]}" ps -q 2>/dev/null)" ]] && return
  local port
  for port in 5432 19092 8000 8080 8501 8502; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      log "WARN: port $port is already in use by another process"
    fi
  done
}

wait_http() {
  local url="$1" deadline=$((SECONDS + WAIT_TIMEOUT))
  until curl -sf -o /dev/null "$url"; do
    (( SECONDS < deadline )) || return 1
    sleep 2
  done
}

print_endpoints() {
  local entry name url state
  echo
  for entry in "${ENDPOINTS[@]}"; do
    name="${entry%%|*}"; url="${entry#*|}"
    if curl -sf -o /dev/null --max-time 3 "$url"; then state="up"; else state="down"; fi
    printf "  %-22s %-40s %s\n" "$name" "$url" "$state"
  done
  printf "  %-22s %s\n" "Postgres" "postgresql://mdm:mdm@localhost:5432/mdm"
  printf "  %-22s %s\n" "Kafka (host)" "localhost:19092"
  echo
}

cmd_up() {
  preflight
  local build=(--build)
  [[ "${1:-}" == "--no-build" ]] && build=()
  log "Starting stack (waiting up to ${WAIT_TIMEOUT}s for healthchecks)..."
  # ${a[@]+...}: empty-array safe under `set -u` on macOS bash 3.2
  "${COMPOSE[@]}" up -d ${build[@]+"${build[@]}"} --wait --wait-timeout "$WAIT_TIMEOUT" \
    || { "${COMPOSE[@]}" ps; die "stack did not become healthy -- see: bash nrt/start.sh logs"; }
  log "Creating source topics (idempotent)..."
  local topic
  for topic in topic.crm.a topic.crm.b topic.crm.c; do
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$topic" >/dev/null
  done
  log "Waiting for MDM API..."
  wait_http "http://localhost:8000/api/v1/health" || die "MDM API not responding -- see: bash nrt/start.sh logs mdm-api"
  log "Stack is up:"
  print_endpoints
}

cmd_down() {
  log "Stopping stack (Postgres data is kept)..."
  "${COMPOSE[@]}" down
}

cmd_reset() {
  log "Stopping stack and deleting the Postgres volume (schema re-initialised on start)..."
  "${COMPOSE[@]}" down -v
  cmd_up "$@"
}

cmd_status() {
  "${COMPOSE[@]}" ps
  print_endpoints
}

case "${1:-up}" in
  up)     shift || true; cmd_up "$@" ;;
  down)   cmd_down ;;
  reset)  shift; cmd_reset "$@" ;;
  status) cmd_status ;;
  logs)   shift; "${COMPOSE[@]}" logs -f --tail 100 "$@" ;;
  -h|--help|help) sed -n '2,13p' "$0" ;;
  *) die "unknown command '$1' (try: up, down, reset, status, logs)" ;;
esac
