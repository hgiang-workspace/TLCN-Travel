#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SENTIMENT_DIR="$REPO_ROOT/ingestion/sentiment"

# Build zip từ package ingestion/sentiment (không tests, không __pycache__)
zip -r dist/ingestion_sentiment.zip "$SENTIMENT_DIR" \
  -x "$SENTIMENT_DIR/tests/*" "**/__pycache__/*"

# Kiểm tra __init__.py có trong zip không
if [ "$(unzip -l dist/ingestion_sentiment.zip | grep '__init__' | wc -l)" -eq 0 ]; then
  echo "ERROR: __init__.py không tìm thấy trong zip!"
  exit 1
fi

echo "✓ Package built: dist/ingestion_sentiment.zip"