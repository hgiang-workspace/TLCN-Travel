#!/bin/bash
# Healthcheck script for the tourism-data-platform

set -e

echo "Running healthchecks..."

# Check MinIO
MINIO_STATUS=$(curl -s -f http://localhost:9000/minio/health/live && echo "healthy" || echo "unhealthy")
echo "MinIO: $MINIO_STATUS"

# Check PostgreSQL
PG_STATUS=$(docker exec postgres pg_isready -U tourism -d tourism 2>&1 || echo "unhealthy")
echo "PostgreSQL: $PG_STATUS"

# Check Spark Master
SPARK_STATUS=$(curl -s -f http://localhost:8080 || echo "unhealthy")
echo "Spark Master: $SPARK_STATUS"

# Check Trino
TRINO_STATUS=$(curl -s -f http://localhost:8081/health && echo "healthy" || echo "unhealthy")
echo "Trino: $TRINO_STATUS"

# Check Airflow
AIRFLOW_STATUS=$(curl -s -f http://localhost:8082/health && echo "healthy" || echo "unhealthy")
echo "Airflow: $AIRFLOW_STATUS"

echo "Healthcheck complete."