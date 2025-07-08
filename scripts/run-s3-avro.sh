#!/bin/bash

# PostgreSQL CDC to S3 with Avro Format - Production Script
# This script runs the S3ProductionPostgresCdcJob with Avro format output

set -e

echo "🚀 Starting PostgreSQL CDC to S3 Pipeline with Avro Format"
echo "==========================================================="

# Load LocalStack environment if it exists
if [ -f ".env.localstack" ]; then
    echo "📂 Loading LocalStack environment..."
    source .env.localstack
fi

# Configuration
FLINK_HOME="./flink-1.18.0"
JAR_FILE="target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar"
MAIN_CLASS="com.example.cdc.S3ProductionPostgresCdcJob"

# Database Configuration
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-cdc_source}"
POSTGRES_USER="${POSTGRES_USER:-cdc_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cdc_password}"
POSTGRES_TABLES="${POSTGRES_TABLES:-public.users,public.orders}"
POSTGRES_SLOT="${POSTGRES_SLOT:-flink_cdc_slot_avro}"

# S3 Configuration - REQUIRED
S3_BUCKET="${S3_BUCKET:-my-cdc-bucket}"
S3_BASE_PATH="${S3_BASE_PATH:-cdc-avro-data}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-}"

# Avro-specific Configuration
S3_FILE_FORMAT="avro"
S3_COMPRESSION="${S3_COMPRESSION:-gzip}"
S3_MAX_FILE_SIZE="${S3_MAX_FILE_SIZE:-256MB}"
S3_ROLLOVER_INTERVAL="${S3_ROLLOVER_INTERVAL:-10min}"

# Validate required parameters
if [ -z "$S3_BUCKET" ] || [ "$S3_BUCKET" = "my-cdc-bucket" ]; then
    echo "❌ Error: S3_BUCKET environment variable must be set to a real S3 bucket"
    echo "Example: export S3_BUCKET=my-production-cdc-bucket"
    echo "Or for LocalStack: source .env.localstack"
    exit 1
fi

echo "📝 Configuration:"
echo "  Database: $POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
echo "  Tables: $POSTGRES_TABLES"
echo "  S3 Bucket: s3://$S3_BUCKET/$S3_BASE_PATH"
echo "  S3 Endpoint: ${S3_ENDPOINT:-AWS S3}"
echo "  S3 Region: $S3_REGION"
echo "  File Format: $S3_FILE_FORMAT"
echo "  Compression: $S3_COMPRESSION"
echo "  Max File Size: $S3_MAX_FILE_SIZE"
echo "  Rollover Interval: $S3_ROLLOVER_INTERVAL"
echo ""

# Check if Flink is running
if ! curl -s http://localhost:8081/config > /dev/null; then
    echo "❌ Flink cluster is not running on localhost:8081"
    echo "Please start Flink cluster first: ./flink-1.18.0/bin/start-cluster.sh"
    exit 1
fi

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "❌ JAR file not found: $JAR_FILE"
    echo "Please build the project first: sbt clean assembly"
    exit 1
fi

echo "🔧 Submitting Flink job..."

# Submit the job
$FLINK_HOME/bin/flink run \
    -c $MAIN_CLASS \
    $JAR_FILE \
    --host $POSTGRES_HOST \
    --port $POSTGRES_PORT \
    --database $POSTGRES_DB \
    --username $POSTGRES_USER \
    --password $POSTGRES_PASSWORD \
    --tables "$POSTGRES_TABLES" \
    --slot-name $POSTGRES_SLOT \
    --s3-bucket $S3_BUCKET \
    --s3-base-path $S3_BASE_PATH \
    --s3-region $S3_REGION \
    ${S3_ENDPOINT:+--s3-endpoint "$S3_ENDPOINT"} \
    ${S3_ENDPOINT:+--s3-path-style-access true} \
    --s3-file-format $S3_FILE_FORMAT \
    --s3-compression $S3_COMPRESSION \
    --s3-max-file-size $S3_MAX_FILE_SIZE \
    --s3-rollover-interval $S3_ROLLOVER_INTERVAL

echo ""
echo "✅ Job submitted successfully!"
echo "🔍 Monitor job status: http://localhost:8081"
echo "📁 S3 output location: s3://$S3_BUCKET/$S3_BASE_PATH/"
echo ""
echo "📄 Avro files will be created with structure:"
echo "   s3://$S3_BUCKET/$S3_BASE_PATH/users/2025/07/08/14/users-*.avro"
echo "   s3://$S3_BUCKET/$S3_BASE_PATH/orders/2025/07/08/14/orders-*.avro"
echo ""
echo "🔧 To read Avro files, you can use:"
echo "   - Apache Avro tools"
echo "   - Apache Spark"
echo "   - Confluent Schema Registry"
echo "   - Any Avro-compatible tool" 