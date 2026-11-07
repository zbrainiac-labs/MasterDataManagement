#!/bin/bash
# -----------------------------------------------------------------------------
# Scan this repo with the DataOpsBackbone SonarQube and fail on test failures,
# scanner errors, a failed quality gate, or any HIGH/BLOCKER/CRITICAL issue
# (impact severity or legacy severity). Used locally and in CI.
# Uses the shared token from the sonar-init volume; config: sonar-project.properties
#
# Local: runs inside a throwaway runner container on the stack's network.
# CI:    on a DataOpsBackbone self-hosted runner (/sonar mounted, on sonarnet)
#        it runs inline -- no docker-in-docker bind mounts.
#
# Usage: bash local-ci/sonar-self-scan.sh
# Env:   SONAR_HOST (default http://sonarqube:9000), SONAR_PROJECT_KEY
#        (default MasterDataManagement-local), RUNNER_IMAGE (default brainiac/local-github-runner)
#        TRIVY_VERSION (default 0.76.2), SKIP_TRIVY=1 to skip the Trivy step
# Out:   .sonar-issues.tsv (severity, rule, file, line, message) in the repo root
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck disable=SC2016  # expanded by the inner shell
SCAN='
  set -Eeuo pipefail
  source /sonar/.sonar_env
  git config --global --add safe.directory "$SRC"
  WORK=$(mktemp -d)
  trap "rm -rf $WORK" EXIT
  python3 -m venv "$WORK/venv"
  # faker is pinned: the generator snapshot test hashes seeded Faker output;
  # fastapi + httpx are only needed for the REST API tests (TestClient)
  "$WORK/venv/bin/pip" install -q --disable-pip-version-check \
    pytest pytest-cov jellyfish faker==40.44.0 fastapi httpx >/dev/null
  # offline unit tests only (regression/e2e need Kafka, Postgres or Snowflake)
  PYTHONPATH="$SRC/nrt/src" PYTHONDONTWRITEBYTECODE=1 COVERAGE_FILE="$WORK/.coverage" \
    "$WORK/venv/bin/python" -m pytest -q -p no:cacheprovider \
      --cov=nrt_mdm --cov=shared/scripts --cov-report=xml:"$WORK/coverage.xml" \
      --ignore=nrt/tests/regression nrt/tests/test_*.py shared/tests | tail -n 3
  RC=0
  sonar-scanner -Dsonar.host.url="$H" -Dsonar.token="$SONAR_TOKEN" -Dsonar.projectKey="$KEY" \
    -Dsonar.python.coverage.reportPaths="$WORK/coverage.xml" \
    -Dsonar.working.directory="$WORK/.scannerwork" -Dsonar.qualitygate.wait=true \
    >"$WORK/scanner.log" 2>&1 || RC=$?
  grep -E "ERROR|ANALYSIS SUCCESSFUL|QUALITY GATE|EXECUTION" "$WORK/scanner.log" || true
  q() { curl -sf -u "$SONAR_TOKEN:" "$H/api/issues/search?componentKeys=$KEY&issueStatuses=OPEN,CONFIRMED&ps=500&$1"; }
  ISSUES=$(jq -s "[.[].issues[]] | unique_by(.key)" \
    <(q "impactSeverities=BLOCKER,HIGH") <(q "severities=BLOCKER,CRITICAL"))
  jq -r ".[] | [(.impacts|map(.severity)|join(\"/\")) + \"|\" + .severity, .rule,
    (.component|sub(\"^[^:]*:\";\"\")), (.line//0|tostring), .message] | @tsv" <<<"$ISSUES" \
    | sort -k3,3 -k4,4n | tee "$SRC/.sonar-issues.tsv"
  COUNT=$(jq length <<<"$ISSUES")
  echo "HIGH/BLOCKER/CRITICAL issues: $COUNT"
  [[ "$RC" -eq 0 ]] || { echo "sonar-scanner or quality gate failed (exit $RC)"; tail -n 20 "$WORK/scanner.log"; exit "$RC"; }
  [[ "$COUNT" -eq 0 ]]
'

export H="${SONAR_HOST:-http://sonarqube:9000}"
export KEY="${SONAR_PROJECT_KEY:-MasterDataManagement-local}"

# Trivy (Aqua Security): dependency CVEs, Dockerfile/compose misconfig, secrets.
# Fails on HIGH/CRITICAL, like the Sonar gate. SKIP_TRIVY=1 to skip.
TRIVY_VERSION="${TRIVY_VERSION:-0.76.2}"
TRIVY_ARGS=(fs --scanners "vuln,misconfig,secret" --severity "HIGH,CRITICAL" --exit-code 1
  --skip-dirs .venv --skip-dirs nrt/.venv --skip-dirs shared/output --no-progress .)

trivy_scan() {
  [[ "${SKIP_TRIVY:-0}" == 1 ]] && { echo "Trivy skipped (SKIP_TRIVY=1)"; return; }
  echo "=== Trivy $TRIVY_VERSION"
  if [[ "$1" == inline ]]; then
    # self-hosted runner: no bind mounts possible, use the pinned release binary
    local bin
    bin=$(mktemp -d)
    curl -sfL "https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz" \
      | tar -xz -C "$bin" trivy
    (cd "$REPO" && "$bin/trivy" "${TRIVY_ARGS[@]}") || { rm -rf "$bin"; return 1; }
    rm -rf "$bin"
  else
    docker run --rm -v "$REPO:/src" -w /src -v trivy-cache:/root/.cache/trivy \
      "aquasec/trivy:$TRIVY_VERSION" "${TRIVY_ARGS[@]}"
  fi
}

if [[ -r /sonar/.sonar_env ]] && command -v sonar-scanner >/dev/null; then
  trivy_scan inline
  cd "$REPO"
  SRC="$REPO" bash -c "$SCAN"
  exit
fi

NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 'sonarnet$' || true)
VOLUME=$(docker volume ls --format '{{.Name}}' | grep -m1 'sonar_env$' || true)
[[ -n "$NETWORK" && -n "$VOLUME" ]] || { echo "ERROR: DataOpsBackbone stack not running -- run its ./start.sh first"; exit 1; }

trivy_scan docker

docker run --rm --network "$NETWORK" \
  -v "$VOLUME:/sonar:ro" -v "$REPO:/src" -w /src \
  -e H -e KEY -e SRC=/src \
  --user root --entrypoint bash "${RUNNER_IMAGE:-brainiac/local-github-runner}" -c "$SCAN"
