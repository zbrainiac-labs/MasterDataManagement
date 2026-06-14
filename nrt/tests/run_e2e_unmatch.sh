#!/usr/bin/env bash
# Run e2e_unmatch.py with proper venv and environment.
# Usage: ./run_e2e_unmatch.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/.venv"

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

# --- Run test ---
python "$SCRIPT_DIR/e2e_unmatch.py" "$@"
