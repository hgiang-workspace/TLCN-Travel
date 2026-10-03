#!/usr/bin/env bash
# Package sentiment pipeline for deployment
# Usage: ./scripts/package_sentiment.sh [output_dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SENTIMENT_DIR="$PROJECT_ROOT/ingestion/sentiment"

OUTPUT_DIR="${1:-$PROJECT_ROOT/dist/sentiment_pipeline}"

echo "=== Packaging Sentiment Pipeline ==="
echo "Project root: $PROJECT_ROOT"
echo "Output dir: $OUTPUT_DIR"

# Clean output
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Copy source code
echo "Copying source code..."
cp -r "$SENTIMENT_DIR" "$OUTPUT_DIR/"

# Copy configs
echo "Copying configs..."
mkdir -p "$OUTPUT_DIR/configs/ingestion"
cp "$PROJECT_ROOT/configs/ingestion/sentiment.yaml" "$OUTPUT_DIR/configs/ingestion/"
cp "$PROJECT_ROOT/configs/ingestion/aspect_keywords.yaml" "$OUTPUT_DIR/configs/ingestion/"

# Copy requirements
echo "Copying requirements..."
cp "$SENTIMENT_DIR/requirements.txt" "$OUTPUT_DIR/"

# Create deployment README
cat > "$OUTPUT_DIR/README.md" << 'EOF'
# Sentiment Pipeline Package (v1.1.0)

## Cấu trúc
```
sentiment_pipeline/
├── ingestion/sentiment/     # Source code
├── configs/ingestion/       # Config files
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Cài đặt
```bash
pip install -r requirements.txt
# Hoặc nếu cần NLP dependencies:
pip install transformers torch underthesea sentencepiece
```

## Chạy pipeline
```bash
# Chạy toàn bộ pipeline S1->S7
python -m ingestion.sentiment.run_pipeline \
    --source tripadvisor \
    --run-id run_20240101 \
    --allow-stub

# Chạy chỉ S1-S5 (sentiment cơ bản)
python -m ingestion.sentiment.run_pipeline \
    --source tripadvisor \
    --run-id run_20240101 \
    --to S5 \
    --allow-stub

# Chạy chỉ ABSA (S6-S7) - cần đã chạy S1-S5 trước
python -m ingestion.sentiment.run_pipeline \
    --source tripadvisor \
    --run-id run_20240101 \
    --from S6 \
    --allow-stub
```

## Output
- `data/sentiment/<source>/sentiment.jsonl` - Sentiment cơ bản (S4)
- `data/sentiment/<source>/absa.jsonl` - ABSA output (S7)
- `data/sentiment/_reports/<run_id>_quality.json` - Quality report (S5)
- `data/sentiment/_reports/<run_id>_absa_quality.json` - ABSA quality report (S7)

## Config
Chỉnh sửa `configs/ingestion/sentiment.yaml`:
- `aspects.enabled`: true/false để bật/tắt ABSA
- `aspects.granularity`: "sentence" hoặc "clause"
- `model.provider`: "transformers" hoặc "stub" (cho test)

## Test
```bash
python -m pytest ingestion/sentiment/tests -q
```
EOF

echo "=== Package created at $OUTPUT_DIR ==="
echo "Contents:"
find "$OUTPUT_DIR" -type f | head -30
EOF