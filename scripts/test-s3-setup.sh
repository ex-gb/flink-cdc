#!/bin/bash

# test-s3-setup.sh
# Test script to verify S3 setup and trigger CDC events

set -e

echo "🧪 Testing S3 CDC Pipeline..."

# Load LocalStack environment if it exists
if [ -f ".env.localstack" ]; then
    echo "📂 Loading LocalStack environment..."
    source .env.localstack
fi

# Check required environment variables
if [ -z "$S3_BUCKET" ]; then
    echo "❌ S3_BUCKET environment variable not set"
    echo "Please run: source .env.localstack"
    exit 1
fi

# Determine S3 endpoint
S3_ENDPOINT_URL=""
if [ ! -z "$S3_ENDPOINT" ]; then
    S3_ENDPOINT_URL="--endpoint-url=$S3_ENDPOINT"
fi

echo "📋 Configuration:"
echo "   Bucket: $S3_BUCKET"
echo "   Endpoint: ${S3_ENDPOINT:-AWS S3}"
echo "   Region: ${S3_REGION:-us-east-1}"

# Test S3 connectivity
echo "🔗 Testing S3 connectivity..."
if aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ >/dev/null 2>&1; then
    echo "✅ S3 bucket accessible"
else
    echo "❌ Cannot access S3 bucket: $S3_BUCKET"
    exit 1
fi

# Check if PostgreSQL is running
echo "🐘 Checking PostgreSQL connectivity..."
if ! pg_isready -h localhost -p 5432 -U cdc_user >/dev/null 2>&1; then
    echo "❌ PostgreSQL is not running or not accessible"
    echo "Please start PostgreSQL with: docker-compose up -d postgres-cdc"
    exit 1
fi

echo "✅ PostgreSQL is running"

# Check if Flink is running
echo "🌊 Checking Flink cluster..."
if ! curl -s http://localhost:8081/jobs >/dev/null 2>&1; then
    echo "❌ Flink cluster is not running"
    echo "Please start Flink with: flink-1.18.0/bin/start-cluster.sh"
    exit 1
fi

echo "✅ Flink cluster is running"

# Create test data function
create_test_data() {
    echo "📊 Creating test data..."
    
    psql -h localhost -p 5432 -U cdc_user -d cdc_source << EOF
-- Insert test users
INSERT INTO users (name, email) VALUES 
    ('Alice Test', 'alice@test.com'),
    ('Bob Test', 'bob@test.com'),
    ('Charlie Test', 'charlie@test.com');

-- Insert test orders
INSERT INTO orders (user_id, product_name, quantity, price) VALUES 
    (1, 'Test Product A', 2, 29.99),
    (2, 'Test Product B', 1, 49.99),
    (3, 'Test Product C', 3, 19.99);

-- Update some records
UPDATE users SET email = 'alice.updated@test.com' WHERE name = 'Alice Test';
UPDATE orders SET quantity = 5 WHERE product_name = 'Test Product A';

-- Delete a record
DELETE FROM orders WHERE product_name = 'Test Product C';
EOF

    echo "✅ Test data created"
}

# Monitor S3 files function
monitor_s3_files() {
    echo "👀 Monitoring S3 files for 30 seconds..."
    
    for i in {1..6}; do
        echo "   Check $i/6..."
        aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ --recursive | grep -E "\.(avro|json)$" || echo "   No files yet..."
        sleep 5
    done
}

# Build the project if JAR doesn't exist
if [ ! -f "target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar" ]; then
    echo "🔨 Building project..."
    sbt assembly
fi

# Ask user what to do
echo ""
echo "🎯 What would you like to do?"
echo "1) Create test data only"
echo "2) Start CDC job and create test data"
echo "3) Monitor S3 files only"
echo "4) Run full test (start job + create data + monitor)"
echo ""
read -p "Enter choice (1-4): " choice

case $choice in
    1)
        create_test_data
        ;;
    2)
        echo "🚀 Starting CDC job..."
        # Start CDC job in background
        nohup flink-1.18.0/bin/flink run \
            -c com.example.cdc.S3ProductionPostgresCdcJob \
            target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar \
            --host localhost \
            --port 5432 \
            --database cdc_source \
            --username cdc_user \
            --password cdc_password \
            --tables "public.users,public.orders" \
            --slot-name flink_cdc_slot_test \
            --s3-bucket $S3_BUCKET \
            --s3-base-path cdc-avro-test \
            --s3-region ${S3_REGION:-us-east-1} \
            ${S3_ENDPOINT:+--s3-endpoint $S3_ENDPOINT} \
            ${S3_ENDPOINT:+--s3-path-style-access true} \
            --s3-file-format avro > cdc-job.log 2>&1 &
        
        echo "✅ CDC job started (check cdc-job.log for output)"
        sleep 10
        create_test_data
        ;;
    3)
        monitor_s3_files
        ;;
    4)
        echo "🚀 Starting CDC job..."
        nohup flink-1.18.0/bin/flink run \
            -c com.example.cdc.S3ProductionPostgresCdcJob \
            target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar \
            --host localhost \
            --port 5432 \
            --database cdc_source \
            --username cdc_user \
            --password cdc_password \
            --tables "public.users,public.orders" \
            --slot-name flink_cdc_slot_test \
            --s3-bucket $S3_BUCKET \
            --s3-base-path cdc-avro-test \
            --s3-region ${S3_REGION:-us-east-1} \
            ${S3_ENDPOINT:+--s3-endpoint $S3_ENDPOINT} \
            ${S3_ENDPOINT:+--s3-path-style-access true} \
            --s3-file-format avro > cdc-job.log 2>&1 &
        
        echo "✅ CDC job started"
        sleep 10
        create_test_data
        sleep 5
        monitor_s3_files
        ;;
    *)
        echo "❌ Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "🎉 Test completed!"
echo ""
echo "🔗 Useful commands:"
echo "   # Check Flink jobs:"
echo "   curl http://localhost:8081/jobs"
echo ""
echo "   # List S3 files:"
echo "   aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ --recursive"
echo ""
echo "   # Download and inspect Avro file:"
echo "   aws $S3_ENDPOINT_URL s3 cp s3://$S3_BUCKET/cdc-avro-test/users/ ./ --recursive"
echo "   # (then use avro-tools to convert to JSON)"
echo ""
echo "   # View job logs:"
echo "   cat cdc-job.log"
echo "   tail -f flink-1.18.0/log/flink-*-taskexecutor-*.log" 