#!/bin/bash
set -e

echo "🚀 Synchronizing dependencies..."

# 1. Sync lockfile
echo "📦 Updating uv.lock..."
uv lock

# 2. Sync requirements.txt (without header for consistency)
echo "📄 Updating requirements.txt..."
uv pip compile pyproject.toml --no-header -o requirements.txt

echo "✅ Dependencies synchronized successfully!"
