#!/usr/bin/env bash
# =============================================================================
# run_e2e.sh — Run the NRT MDM E2E test with proper Python environment
# Ensures Docker containers are running and test data exists before executing.
#
# Usage:
#   ./run_e2e.sh                                # quick functional test (small)
#   ./run_e2e.sh --mode continuous --rate 5     # latency monitoring
#   ./run_e2e.sh --scale medium --duration 60   # 100K load + 1 min steady-state
#   ./run_e2e.sh --scale large --duration 300   # 1M load + 5 min steady-state
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
SHARED_DIR="$REPO_DIR/shared"

# --- Ensure Docker stack is running ---
echo "[docker] Checking containers..."
if docker compose -f "$PROJECT_DIR/docker-compose.yml" ps --format '{{.Service}}' 2>/dev/null | grep -q "postgres"; then
    echo "[docker] OK"
else
    echo "[docker] Starting stack..."
    docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d
    echo "[docker] Waiting for services to be healthy..."
    sleep 10
    echo "[docker] OK"
fi

# --- Python venv ---
if [ ! -d "$VENV_DIR" ]; then
    echo "[venv] Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

if ! python -c "import nrt_mdm" 2>/dev/null; then
    echo "[venv] Installing nrt-mdm package..."
    pip install -q -e "$PROJECT_DIR[dev]"
fi

# --- Generate bulk test data if needed (small scale only) ---
if [[ "${*:-}" != *"--scale medium"* ]] && [[ "${*:-}" != *"--scale large"* ]]; then
    if [ ! -d "$SHARED_DIR/output/initial" ]; then
        echo "[data] Generating bulk test data (1,500 records)..."
        python3 "$SHARED_DIR/scripts/generate_test_data.py"
        echo "[data] OK"
    fi
fi

# --- Run E2E test ---
echo ""
echo "TIP: To watch CDC events live in another terminal:"
echo "  docker exec mdm-nrt-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic.mdm.golden"
echo ""
python "$SCRIPT_DIR/e2e_test.py" "$@"
