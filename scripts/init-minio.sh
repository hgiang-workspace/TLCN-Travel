#!/bin/bash
# Initialize MinIO bucket and setup

MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}
MINIO_BUCKET=${MINIO_BUCKET:-tourism}

# Create bucket
mc alias set myminio "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb myminio/"$MINIO_BUCKET" || true

# Create bucket logical structure
mcla set myminio/"$MINIO_BUCKET" private || true

echo "MinIO initialized with bucket: $MINIO_BUCKET"
echo "Bucket structure: bronze/silver/gold/ml/models"