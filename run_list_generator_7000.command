#!/bin/zsh
set -e

SCRIPT_DIR="${0:A:h}"
cd "$SCRIPT_DIR"

if [[ ! -f ".env" ]]; then
    cp ".env.example" ".env"
    echo "Created $SCRIPT_DIR/.env"
    echo "Add your CLIENT_ID, CLIENT_SECRET, and FLASK_SECRET_KEY, then run this command again."
    exit 1
fi

if [[ ! -x ".venv/bin/python" ]]; then
    echo "Creating the List Generator Python environment..."
    python3 -m venv .venv
fi

if ! .venv/bin/python -c "import dash, dash_auth_external, pandas, requests, dotenv" 2>/dev/null; then
    echo "Installing required packages..."
    .venv/bin/python -m pip install -r requirements.txt
fi

echo "Starting List Generator 7000 at the APP_URL configured in .env"
exec .venv/bin/python list_generator_7000.py
