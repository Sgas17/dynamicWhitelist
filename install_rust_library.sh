#!/bin/bash
# Install scrape_rethdb_data Rust library into dynamicWhitelist venv

set -e  # Exit on error

echo "Installing scrape_rethdb_data Rust library..."
echo "=============================================="
echo ""

# Ensure we're in the dynamicWhitelist directory
cd ~/dynamicWhitelist

# Check if maturin is installed
if ! uv run python -c "import maturin" 2>/dev/null; then
    echo "Installing maturin..."
    uv add --dev maturin
fi

# Build and install the Rust library
echo "Building and installing Rust library..."
uv run maturin develop --release \
    --manifest-path ~/scrape_rethdb_data/Cargo.toml \
    --features=python

echo ""
echo "✅ Installation complete!"
echo ""
echo "Testing import..."
uv run python -c "import scrape_rethdb_data; print('✓ Successfully imported scrape_rethdb_data')"

echo ""
echo "To use in your code:"
echo "  from scrape_rethdb_data import collect_pools"
echo ""
