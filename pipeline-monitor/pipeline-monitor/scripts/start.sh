#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
#  DataPulse — AI Pipeline Monitor
#  Start Script
# ─────────────────────────────────────────────────────────────
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
ENV_FILE="$SCRIPT_DIR/.env"

echo ""
echo "  ⚡ DataPulse — AI Pipeline Monitor"
echo "  ────────────────────────────────────"

# ── Check Python ──────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "  ✗ Python 3 not found. Please install Python 3.9+"
  exit 1
fi

PYTHON=$(command -v python3)
echo "  ✓ Python: $($PYTHON --version)"

# ── Check/create .env ─────────────────────────────────────────
if [ ! -f "$ENV_FILE" ]; then
  cp "$SCRIPT_DIR/.env.example" "$ENV_FILE"
  echo ""
  echo "  ⚠  .env file created from .env.example"
  echo "     Please edit .env and add your ANTHROPIC_API_KEY"
  echo ""
  read -p "  Press Enter to continue (or Ctrl+C to exit and set key first)..."
fi

# ── Load .env ─────────────────────────────────────────────────
set -a
source "$ENV_FILE"
set +a

if [ -z "$ANTHROPIC_API_KEY" ] || [ "$ANTHROPIC_API_KEY" = "sk-ant-your-key-here" ]; then
  echo ""
  echo "  ⚠  WARNING: ANTHROPIC_API_KEY is not set in .env"
  echo "     The dashboard will work but the AI Agent chat will fail."
  echo "     Get a key at: https://console.anthropic.com"
  echo ""
fi

# ── Create virtualenv if needed ───────────────────────────────
VENV_DIR="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "  → Creating virtual environment..."
  $PYTHON -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
echo "  ✓ Virtual environment activated"

# ── Install dependencies ──────────────────────────────────────
echo "  → Installing backend dependencies..."
pip install -q -r "$BACKEND_DIR/requirements.txt"
echo "  ✓ Dependencies installed"

# ── Start server ──────────────────────────────────────────────
PORT=${PORT:-8000}
echo ""
echo "  🚀 Starting DataPulse on http://localhost:$PORT"
echo ""
echo "  Open your browser at: http://localhost:$PORT"
echo "  Press Ctrl+C to stop"
echo ""

cd "$BACKEND_DIR"
uvicorn main:app --host 0.0.0.0 --port "$PORT" --reload
