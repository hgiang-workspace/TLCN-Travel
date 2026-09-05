#!/bin/bash
# Initialize PostgreSQL database and schemas

POSTGRES_USER=${POSTGRES_USER:-tourism}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-tourism}
POSTGRES_DB=${POSTGRES_DB:-tourism}

# Wait for PostgreSQL to be ready
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

# Create database if not exists
psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE $POSTGRES_DB;" 2>/dev/null || true

# Create schemas
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    CREATE SCHEMA IF NOT EXISTS metadata;
    CREATE SCHEMA IF NOT EXISTS serving;
"

echo "PostgreSQL initialized with database: $POSTGRES_DB"
echo "Schemas created: metadata, serving"