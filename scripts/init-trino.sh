#!/bin/bash
# Initialize Trino Iceberg catalog

TRINO_HOST=${TRINO_HOST:-trino}
ICEBERG_CATALOG=${ICEBERG_CATALOG:-iceberg}
ICEBERG_URI=${ICEBERG_URI:-s3a://tourism}

# Create iceberg catalog properties
cat > /etc/trino/catalog/iceberg.properties <<EOF
connector.name=iceberg
iceberg.catalog=hive
iceberg.schema-autorename.enabled=true
iceberg.tmp-dir=$ICEBERG_URI/ml/tmp
EOF

echo "Trino Iceberg catalog initialized at $ICEBERG_URI"