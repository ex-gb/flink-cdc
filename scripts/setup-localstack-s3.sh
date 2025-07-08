#!/bin/bash

# setup-localstack-s3.sh
# Quick setup script for LocalStack S3 testing

set -e

echo "🚀 Setting up LocalStack S3 for CDC testing..."

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "⚠️  AWS CLI not found. Installing..."
    if command -v pip3 &> /dev/null; then
        pip3 install awscli
    elif command -v brew &> /dev/null; then
        brew install awscli
    else
        echo "❌ Please install AWS CLI manually: pip install awscli"
        exit 1
    fi
fi

# Stop and remove existing LocalStack container
echo "🧹 Cleaning up existing LocalStack container..."
docker stop localstack 2>/dev/null || true
docker rm localstack 2>/dev/null || true

# Start LocalStack
echo "🐳 Starting LocalStack container..."
docker run -d \
  --name localstack \
  -p 4566:4566 \
  -e SERVICES=s3 \
  -e DEBUG=1 \
  -e DATA_DIR=/tmp/localstack/data \
  localstack/localstack:latest

# Wait for LocalStack to be ready
echo "⏳ Waiting for LocalStack to start..."
sleep 10

# Check if LocalStack is healthy
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:4566/health | grep -q "\"s3\": \"available\""; then
        echo "✅ LocalStack is ready!"
        break
    fi
    echo "Waiting for LocalStack... (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ LocalStack failed to start properly"
    docker logs localstack
    exit 1
fi

# Configure AWS CLI for LocalStack
echo "🔧 Configuring AWS CLI for LocalStack..."
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set region us-east-1
aws configure set output json

# Create S3 bucket
echo "🪣 Creating S3 bucket..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://cdc-test-bucket

# Verify bucket creation
echo "✅ Verifying bucket creation..."
aws --endpoint-url=http://localhost:4566 s3 ls

# Test upload
echo "📁 Testing file upload..."
echo "LocalStack S3 test file - $(date)" > test-localstack.txt
aws --endpoint-url=http://localhost:4566 s3 cp test-localstack.txt s3://cdc-test-bucket/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cdc-test-bucket/
rm test-localstack.txt

# Set environment variables
echo "🌍 Setting environment variables..."
export S3_BUCKET=cdc-test-bucket
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Create environment file for later use
cat > .env.localstack << EOF
# LocalStack S3 Configuration
export S3_BUCKET=cdc-test-bucket
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export S3_ENDPOINT=http://localhost:4566
export S3_REGION=us-east-1
EOF

echo ""
echo "🎉 LocalStack S3 setup completed successfully!"
echo ""
echo "📋 Configuration:"
echo "   S3 Endpoint: http://localhost:4566"
echo "   Bucket Name: cdc-test-bucket"
echo "   Access Key:  test"
echo "   Secret Key:  test"
echo "   Region:      us-east-1"
echo ""
echo "🔗 Useful commands:"
echo "   # List buckets:"
echo "   aws --endpoint-url=http://localhost:4566 s3 ls"
echo ""
echo "   # List files in bucket:"
echo "   aws --endpoint-url=http://localhost:4566 s3 ls s3://cdc-test-bucket/ --recursive"
echo ""
echo "   # Check LocalStack status:"
echo "   curl http://localhost:4566/health"
echo ""
echo "   # View LocalStack logs:"
echo "   docker logs localstack"
echo ""
echo "   # Load environment variables:"
echo "   source .env.localstack"
echo ""
echo "🚀 Ready to run CDC job! Use the following command:"
echo "   source .env.localstack"
echo "   ./scripts/run-s3-avro.sh"
echo ""
echo "   Or manually:"
echo "   flink-1.18.0/bin/flink run \\"
echo "     -c com.example.cdc.S3ProductionPostgresCdcJob \\"
echo "     target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar \\"
echo "     --host localhost \\"
echo "     --port 5432 \\"
echo "     --database cdc_source \\"
echo "     --username cdc_user \\"
echo "     --password cdc_password \\"
echo "     --tables \"public.users,public.orders\" \\"
echo "     --slot-name flink_cdc_slot_localstack \\"
echo "     --s3-bucket cdc-test-bucket \\"
echo "     --s3-base-path cdc-avro-test \\"
echo "     --s3-region us-east-1 \\"
echo "     --s3-endpoint http://localhost:4566 \\"
echo "     --s3-path-style-access true \\"
echo "     --s3-file-format avro" 