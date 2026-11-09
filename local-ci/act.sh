#!/bin/bash
# -----------------------------------------------------------------------------
# Run .github/workflows/ci.yml locally with nektos/act -- same jobs as GitHub:
# lint, sonar, image (amd64 + arm64), deploy. publish (GHCR) and image pushes
# are skipped. Uses the working copy, so uncommitted changes are tested.
#
#   deploy runs the DataOps pipeline from the local DataOpsBackbone checkout with
#   PAT auth (no OIDC locally): DRY_RUN by default (prepare + scan, no Snowflake
#   changes); --full deploys to a per-build clone schema, never to MDM_DEV itself.
#
# Usage: bash local-ci/act.sh [--job=<id>] [--full] [-- <extra act args>]
#   --job=<id>   run a single job (lint | sonar | image | deploy), e.g. --job=sonar
#   --full       deploy to a clone schema instead of a dry run
# Env:   BACKBONE (default ../DataOpsBackbone), SNOW_CONFIG_B64 (else built from
#        the DataOpsBackbone .env via local-ci/snow-config.sh), GITHUB_TOKEN
#        (else from `gh auth token`)
# Requires: act, docker, the running DataOpsBackbone stack (SonarQube, sonarnet)
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKBONE="$(cd "${BACKBONE:-$REPO/../DataOpsBackbone}" && pwd)"
JOB=""
FULL=false
ACT_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --job=*) JOB="${1#*=}" ;;
    --full) FULL=true ;;
    --) shift; ACT_ARGS=("$@"); break ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
  shift
done

for cmd in act docker; do
  command -v "$cmd" >/dev/null || { echo "ERROR: $cmd not found"; exit 1; }
done
NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 'sonarnet$' || true)
VOLUME=$(docker volume ls --format '{{.Name}}' | grep -m1 'sonar_env$' || true)
[[ -n "$NETWORK" && -n "$VOLUME" ]] || { echo "ERROR: DataOpsBackbone stack not running -- run its ./start.sh first"; exit 1; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
chmod 700 "$TMP"

# github.event.act marks local runs in ci.yml; full=true switches deploy to a clone schema
cat > "$TMP/event.json" <<JSON
{"act": true, "full": $FULL,
 "repository": {"name": "MasterDataManagement", "full_name": "zbrainiac-labs/MasterDataManagement",
                "owner": {"login": "zbrainiac-labs"}}}
JSON

# --- secrets (never echoed; files are removed on exit) -----------------------
SNOW_CONFIG_B64="${SNOW_CONFIG_B64:-}"
if [[ -z "$SNOW_CONFIG_B64" && ( -z "$JOB" || "$JOB" == deploy ) ]]; then
  SNOW_CONFIG_B64=$(cd "$BACKBONE" && bash local-ci/snow-config.sh) \
    || { echo "ERROR: no Snowflake credentials (SNOW_CONFIG_B64 or SNOW_* in $BACKBONE/.env)"; exit 1; }
fi
{
  echo "SNOW_CONFIG_B64=$SNOW_CONFIG_B64"
  echo "GITHUB_TOKEN=${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
} > "$TMP/secrets"

# the DataOps pipeline's scan job reads SONAR_TOKEN from the env (as run.sh does)
SONAR_TOKEN=$(docker run --rm -v "$VOLUME:/sonar:ro" --entrypoint sh brainiac/local-github-runner \
  -c 'grep -m1 "^SONAR_TOKEN=" /sonar/.sonar_env | cut -d= -f2-' 2>/dev/null || true)
RUN_SEQ=$(date +%s)   # act always reports run 1; keep DCM aliases / clone schemas unique
{
  echo "SONAR_TOKEN=$SONAR_TOKEN"
  echo "GITHUB_RUN_NUMBER=$RUN_SEQ"
  echo "GITHUB_RUN_ID=$RUN_SEQ"
  echo "GITHUB_OWNER=zbrainiac-labs"   # sql-validate.sh names the UnitTH package owner_repo
  echo "GITHUB_REPO=MasterDataManagement"
} > "$TMP/env"

# spans are only exported with an OTLP endpoint (same as DataOpsBackbone local-ci/run.sh)
if [[ -n "$(docker ps -q --filter label=com.docker.compose.service=otel-collector)" ]]; then
  echo "OTEL_EXPORTER_OTLP_ENDPOINT=host.docker.internal:4317" >> "$TMP/env"
  echo "Tracing via otel-collector: service MasterDataManagement in Grafana http://localhost:3000"
fi

# same report mounts as the compose runner, so results show up on http://localhost:8080
mkdir -p "$BACKBONE/unitth-store" "$BACKBONE/sql-unit-reports"
REPORT_MOUNTS="-v sql_unit_report:/home/docker/sql-report-vol -v $BACKBONE/unitth-store:/home/docker/unitth-store -v $BACKBONE/sql-unit-reports:/home/docker/sql-unit-reports"

# --- run ----------------------------------------------------------------------
LOG_DIR="$REPO/local-ci/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/$(date +%Y%m%dT%H%M%S)-ci.log"
echo "act: ci.yml ${JOB:+job=$JOB }(deploy: $([[ $FULL == true ]] && echo 'clone schema' || echo 'dry run')) -- log: $LOG"

# self-hosted = the DataOpsBackbone runner image; ubuntu-* = act image with buildx
# (arm64 runs natively on Apple Silicon, the amd64 matrix leg builds via emulation)
set +e
act workflow_dispatch \
  -C "$REPO" \
  -W "$REPO/.github/workflows/ci.yml" \
  -e "$TMP/event.json" \
  ${JOB:+-j "$JOB"} \
  -P self-hosted=brainiac/local-github-runner \
  -P ubuntu-latest=catthehacker/ubuntu:act-24.04 \
  -P ubuntu-24.04-arm=catthehacker/ubuntu:act-24.04 \
  --pull=false \
  --local-repository "zbrainiac-labs/DataOpsBackbone@main=$BACKBONE" \
  --network "$NETWORK" \
  --container-options "-v $VOLUME:/sonar:ro $REPORT_MOUNTS" \
  --secret-file "$TMP/secrets" \
  --env-file "$TMP/env" \
  ${ACT_ARGS[@]+"${ACT_ARGS[@]}"} 2>&1 | tee "$LOG"
RC=${PIPESTATUS[0]}
set -e

if grep -qE 'squ_[A-Za-z0-9]{20,}|gh[ops]_[A-Za-z0-9]{20,}' "$LOG"; then
  echo "ERROR: secret-looking value found in log $LOG"
  RC=1
fi
echo "act exit code: $RC"
exit "$RC"
