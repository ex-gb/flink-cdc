#!/bin/bash

# run-local-to-aws-s3.sh
# Run Flink CDC locally with Real AWS S3 output (Avro format)

set -e

echo "🌐 Running Flink Local → Real AWS S3 Pipeline"
echo "=============================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Clear any LocalStack environment
unset S3_ENDPOINT
unset AWS_SESSION_TOKEN

echo -e "${BLUE}🔧 AWS S3 Configuration Setup${NC}"
echo ""

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}❌ AWS CLI not configured or credentials invalid${NC}"
    echo ""
    echo "Please configure AWS CLI first:"
    echo "  aws configure"
    echo ""
    echo "Or set environment variables:"
    echo "  export AWS_ACCESS_KEY_ID=your-access-key"
    echo "  export AWS_SECRET_ACCESS_KEY=your-secret-key"
    echo "  export AWS_DEFAULT_REGION=us-east-1"
    exit 1
fi

# Get AWS account info
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "Unknown")
AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
AWS_USER=$(aws sts get-caller-identity --query Arn --output text 2>/dev/null | cut -d'/' -f2 || echo "Unknown")

echo -e "${GREEN}✅ AWS Credentials Valid${NC}"
echo "   Account: $AWS_ACCOUNT"
echo "   Region: $AWS_REGION"
echo "   User: $AWS_USER"
echo ""

# Prompt for S3 bucket
if [ -z "$S3_BUCKET" ]; then
    echo -e "${YELLOW}📝 S3 Bucket Configuration${NC}"
    echo ""
    read -p "Enter your AWS S3 bucket name: " S3_BUCKET
    
    if [ -z "$S3_BUCKET" ]; then
        echo -e "${RED}❌ S3 bucket name is required${NC}"
        exit 1
    fi
fi

# Validate S3 bucket access
echo -e "${BLUE}🔍 Validating S3 bucket access...${NC}"
if ! aws s3 ls "s3://$S3_BUCKET/" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Bucket '$S3_BUCKET' not accessible. Attempting to create...${NC}"
    
    if aws s3 mb "s3://$S3_BUCKET" --region "$AWS_REGION" 2>/dev/null; then
        echo -e "${GREEN}✅ Created bucket: $S3_BUCKET${NC}"
    else
        echo -e "${RED}❌ Cannot access or create bucket: $S3_BUCKET${NC}"
        echo ""
        echo "Please ensure:"
        echo "1. Bucket name is globally unique"
        echo "2. You have S3 permissions"
        echo "3. Bucket exists or you can create it"
        exit 1
    fi
else
    echo -e "${GREEN}✅ Bucket accessible: $S3_BUCKET${NC}"
fi

# Test write permissions
echo -e "${BLUE}🔐 Testing S3 write permissions...${NC}"
TEST_FILE="cdc-test-$(date +%s).txt"
echo "Test file from Flink CDC - $(date)" | aws s3 cp - "s3://$S3_BUCKET/$TEST_FILE"
aws s3 rm "s3://$S3_BUCKET/$TEST_FILE" > /dev/null 2>&1
echo -e "${GREEN}✅ S3 write permissions confirmed${NC}"

echo ""

# Configuration summary
echo -e "${BLUE}📋 Final Configuration:${NC}"
echo "   Local Flink: localhost:8081"
echo "   PostgreSQL: localhost:5432/cdc_source"
echo "   AWS S3 Bucket: s3://$S3_BUCKET"
echo "   AWS Region: $AWS_REGION"
echo "   Output Format: Avro"
echo "   Output Path: s3://$S3_BUCKET/cdc-avro-data/"
echo ""

# Check prerequisites
echo -e "${BLUE}🔍 Checking prerequisites...${NC}"

# Check Flink
if ! curl -s http://localhost:8081/config > /dev/null 2>&1; then
    echo -e "${RED}❌ Flink cluster not running${NC}"
    echo "Start Flink: ./flink-1.18.0/bin/start-cluster.sh"
    exit 1
fi
echo -e "${GREEN}✅ Flink cluster running${NC}"

# Check PostgreSQL
if ! pg_isready -h localhost -p 5432 -U cdc_user > /dev/null 2>&1; then
    echo -e "${RED}❌ PostgreSQL not accessible${NC}"
    echo "Start PostgreSQL: docker-compose up -d postgres-cdc"
    exit 1
fi
echo -e "${GREEN}✅ PostgreSQL accessible${NC}"

# Check JAR file
JAR_FILE="target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${RED}❌ JAR file not found: $JAR_FILE${NC}"
    echo "Build project: sbt assembly"
    exit 1
fi
echo -e "${GREEN}✅ JAR file ready${NC}"

echo ""

# Set environment for AWS S3 (no LocalStack)
export S3_BUCKET="$S3_BUCKET"
export S3_REGION="$AWS_REGION"
unset S3_ENDPOINT  # Ensure no LocalStack endpoint

# Create environment file for reuse
cat > .env.aws-s3 << EOF
# AWS S3 Configuration for Local Flink
export S3_BUCKET=$S3_BUCKET
export S3_REGION=$AWS_REGION
# Note: AWS credentials should be configured via 'aws configure' or environment variables
# Do NOT put credentials in this file for security reasons
EOF

echo -e "${GREEN}💾 Environment saved to .env.aws-s3${NC}"
echo ""

# Run the job
echo -e "${GREEN}🚀 Launching Flink CDC Job (Local → AWS S3)...${NC}"
echo "   Monitor: http://localhost:8081"
echo "   S3 Output: https://s3.console.aws.amazon.com/s3/buckets/$S3_BUCKET"
echo ""

# Use the existing run-s3-avro.sh script which already supports AWS S3
if [ -f "scripts/run-s3-avro.sh" ]; then
    exec ./scripts/run-s3-avro.sh
else
    # Fallback: run directly
    ./flink-1.18.0/bin/flink run \
        -c com.example.cdc.S3ProductionPostgresCdcJob \
        "$JAR_FILE" \
        --host localhost \
        --port 5432 \
        --database cdc_source \
        --username cdc_user \
        --password cdc_password \
        --tables "public.users,public.orders" \
        --slot-name flink_cdc_slot_aws_s3 \
        --s3-bucket "$S3_BUCKET" \
        --s3-base-path cdc-avro-data \
        --s3-region "$AWS_REGION" \
        --s3-file-format avro
fi 