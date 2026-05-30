#!/usr/bin/env bash
# install.sh — Install the BR CNPJ → POI pipeline on Linux.
#
# Creates a virtual environment, installs dependencies, creates the
# required directory structure, and copies .env.example if no .env exists.
#
# Usage:
#   bash install.sh                          # install in current directory
#   bash install.sh --dir /opt/cnpj-poi      # install to a custom directory

set -euo pipefail

# ── Defaults ───────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${INSTALL_DIR:-$SCRIPT_DIR}"

# ── Parse args ─────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir) INSTALL_DIR="$2"; shift 2 ;;
        *)     echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "============================================================"
echo " BR CNPJ → POI Pipeline — Installation"
echo " Install directory: $INSTALL_DIR"
echo "============================================================"

# ── Create directory structure ─────────────────────────────────────────────
mkdir -p \
    "$INSTALL_DIR/data/input" \
    "$INSTALL_DIR/data/intermediate" \
    "$INSTALL_DIR/data/cache" \
    "$INSTALL_DIR/data/output" \
    "$INSTALL_DIR/logs"

echo "[1/4] Directory structure created."

# ── Copy project files (if installing to a different directory) ────────────
if [[ "$INSTALL_DIR" != "$SCRIPT_DIR" ]]; then
    echo "[2/4] Copying project files to $INSTALL_DIR…"
    rsync -av --exclude='.git' --exclude='venv' --exclude='data' --exclude='logs' \
        "$SCRIPT_DIR/" "$INSTALL_DIR/"
else
    echo "[2/4] Installing in-place — no copy needed."
fi

# ── Virtual environment ────────────────────────────────────────────────────
VENV="$INSTALL_DIR/venv"
if [[ ! -d "$VENV" ]]; then
    echo "[3/4] Creating virtual environment at $VENV…"
    python3 -m venv "$VENV"
else
    echo "[3/4] Virtual environment already exists — skipping."
fi

# geopandas (used in Step 4 geocoding) requires GDAL system libraries
if command -v apt-get &>/dev/null; then
    echo "      Installing GDAL system libraries (required by geopandas)…"
    sudo apt-get install -y -q libgdal-dev python3-gdal gdal-bin 2>/dev/null || \
        echo "      WARNING: Could not install GDAL — geopandas may fail. Install manually."
fi

"$VENV/bin/pip" install --upgrade pip --quiet
"$VENV/bin/pip" install -r "$INSTALL_DIR/requirements.txt" --quiet
echo "      Dependencies installed."

# ── Environment file ───────────────────────────────────────────────────────
if [[ ! -f "$INSTALL_DIR/.env" ]]; then
    cp "$INSTALL_DIR/.env.example" "$INSTALL_DIR/.env"
    echo "[4/4] Created .env from .env.example — edit it if needed."
else
    echo "[4/4] .env already exists — not overwritten."
fi

echo ""
echo "============================================================"
echo " Installation complete."
echo ""
echo " Activate the virtual environment:"
echo "   source $VENV/bin/activate"
echo ""
echo " Run the pipeline:"
echo "   python -m app.main --dump-date 2026-05"
echo "   python -m app.main --help"
echo "============================================================"
