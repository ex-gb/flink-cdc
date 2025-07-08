# 📦 Complete S3 Configuration Guide for PostgreSQL CDC

**One comprehensive guide for all S3 scenarios with your Flink CDC pipeline**

## 🎯 Choose Your S3 Setup

| Scenario | Use Case | Cost | Setup Time |
|----------|----------|------|------------|
| **[LocalStack](#-localstack-local-s3)** | Development, Testing | FREE | 2 minutes |
| **[Local Flink → AWS S3](#-local-flink--real-aws-s3)** | Production Testing | ~$1-10/month | 5 minutes |
| **[Full AWS Production](#-full-aws-s3-production)** | Production Deployment | Variable | 10 minutes |

---

## 🏠 LocalStack (Local S3)

**Perfect for:** Development, testing, learning - **Zero AWS costs**

### Quick Setup
```bash
# One-command setup
./scripts/setup-localstack-s3.sh

# Interactive testing
./scripts/test-s3-setup.sh
```

### Manual Setup
```bash
# 1. Start LocalStack
docker run -d \
  --name localstack \
  -p 4566:4566 \
  -e SERVICES=s3 \
  -e DEBUG=1 \
  localstack/localstack:latest

# 2. Configure AWS CLI for LocalStack
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set region us-east-1

# 3. Create bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://cdc-test-bucket

# 4. Set environment
export S3_BUCKET=cdc-test-bucket
export S3_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# 5. Run CDC job
./scripts/run-s3-avro.sh
```

### Verify LocalStack
```bash
# Check LocalStack health
curl http://localhost:4566/health

# List files
aws --endpoint-url=http://localhost:4566 s3 ls s3://cdc-test-bucket/ --recursive

# Monitor in real-time
./scripts/monitor-s3-pipeline.sh
```

---

## 🌐 Local Flink → Real AWS S3

**Perfect for:** Production testing with real storage - **Best of both worlds**

### Quick Setup
```bash
# 1. Configure AWS credentials
aws configure

# 2. One-command setup
./scripts/run-local-to-aws-s3.sh
```

### Manual Setup
```bash
# 1. Configure AWS CLI
aws configure
# Enter: Access Key, Secret Key, Region, Output format

# 2. Clear LocalStack settings
unset S3_ENDPOINT

# 3. Set AWS S3 configuration
export S3_BUCKET=your-aws-bucket-name
export S3_REGION=us-east-1

# 4. Create bucket (if needed)
aws s3 mb s3://your-aws-bucket-name --region us-east-1

# 5. Run CDC job
./scripts/run-s3-avro.sh
```

### Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   Flink Local    │    │    AWS S3       │
│   (Local)       │───▶│   (localhost)    │───▶│  (Production)   │
│                 │    │                  │    │                 │
│ localhost:5432  │    │ localhost:8081   │    │ Real S3 Bucket  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Cost Estimates
- **Light Testing (1GB)**: ~$0.10/month
- **Heavy Testing (10GB)**: ~$1.00/month
- **Development (100GB)**: ~$10.00/month

### Verify AWS S3
```bash
# Check AWS credentials
aws sts get-caller-identity

# List S3 files
aws s3 ls s3://your-bucket/cdc-avro-data/ --recursive

# Download and inspect Avro
aws s3 sync s3://your-bucket/cdc-avro-data/users/ ./downloaded-avro/ --exclude "*" --include "*.avro"
java -jar avro-tools-1.11.3.jar tojsonpretty downloaded-avro/users-*.avro
```

---

## ☁️ Full AWS S3 Production

**Perfect for:** Production deployment with scalable infrastructure

### Setup AWS Infrastructure
```bash
# 1. Configure AWS CLI
aws configure

# 2. Create production bucket
aws s3 mb s3://your-production-cdc-bucket --region us-east-1

# 3. Set up IAM permissions
aws iam create-policy --policy-name FlinkCDCS3Policy --policy-document '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-production-cdc-bucket",
        "arn:aws:s3:::your-production-cdc-bucket/*"
      ]
    }
  ]
}'

# 4. Set production environment
export S3_BUCKET=your-production-cdc-bucket
export S3_REGION=us-east-1
```

### Production Deployment Options

#### Option A: AWS EMR (Managed Flink)
```bash
aws emr create-cluster \
  --name "CDC-Production-Pipeline" \
  --release-label emr-6.10.0 \
  --applications Name=Flink \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole
```

#### Option B: AWS EKS (Kubernetes)
```yaml
# flink-cdc-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-cdc-pipeline
spec:
  replicas: 3
  selector:
    matchLabels:
      app: flink-cdc
  template:
    metadata:
      labels:
        app: flink-cdc
    spec:
      containers:
      - name: flink-cdc
        image: your-ecr-repo/flink-cdc:latest
        env:
        - name: S3_BUCKET
          value: your-production-cdc-bucket
```

#### Option C: AWS Kinesis Analytics
```bash
# Create Kinesis Analytics application
aws kinesisanalyticsv2 create-application \
  --application-name flink-cdc-pipeline \
  --runtime-environment FLINK-1_18 \
  --service-execution-role arn:aws:iam::account:role/KinesisAnalyticsRole
```

---

## 📊 Monitoring & Validation

### Real-Time Monitoring
```bash
# Pipeline health dashboard
./scripts/monitor-s3-pipeline.sh

# Data quality validation
./scripts/validate-s3-data.sh
```

### AWS CloudWatch Integration
The pipeline automatically sends metrics to CloudWatch:
- S3 PUT operations count
- Error rates and types
- File sizes and processing latency
- CDC lag measurements

### Set Up CloudWatch Alarms
```bash
# High error rate alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "CDC-High-Error-Rate" \
  --alarm-description "CDC pipeline error rate > 5%" \
  --metric-name ErrorRate \
  --namespace FlinkCDC \
  --statistic Average \
  --period 300 \
  --threshold 5.0 \
  --comparison-operator GreaterThanThreshold

# Low throughput alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "CDC-Low-Throughput" \
  --alarm-description "CDC throughput < 100 records/min" \
  --metric-name RecordsPerMinute \
  --namespace FlinkCDC \
  --statistic Average \
  --period 300 \
  --threshold 100 \
  --comparison-operator LessThanThreshold
```

---

## 🗂️ S3 Output Structure

All configurations produce the same organized output structure:

```
s3://your-bucket/cdc-avro-data/
├── users/
│   └── 2025/07/08/14/
│       ├── users-20250708-140001.avro
│       ├── users-20250708-140002.avro
│       └── users-20250708-140003.avro
├── orders/
│   └── 2025/07/08/14/
│       ├── orders-20250708-140001.avro
│       └── orders-20250708-140002.avro
├── errors/
│   └── 2025/07/08/14/
│       └── errors-20250708-140001.avro
└── schema-changes/
    └── 2025/07/08/14/
        └── schema-changes-20250708-140001.avro
```

### Avro Record Schema
```json
{
  "table_name": "users",
  "operation": "INSERT",
  "timestamp": 1704723601000,
  "before": null,
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2025-07-08T14:00:01.000Z"
  },
  "source": {
    "version": "3.4.0",
    "connector": "postgresql",
    "name": "postgres-cdc",
    "ts_ms": 1704723601000,
    "snapshot": false,
    "db": "cdc_source",
    "schema": "public",
    "table": "users"
  },
  "transaction": null,
  "processing_time": 1704723601123,
  "pipeline_version": "1.0.0",
  "processing_latency_ms": 45
}
```

---

## 🔄 Easy Environment Switching

### Environment Files
The scripts create environment files for easy switching:

**`.env.localstack`** (Local S3)
```bash
export S3_BUCKET=cdc-test-bucket
export S3_ENDPOINT=http://localhost:4566
export S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

**`.env.aws-s3`** (Real AWS S3)
```bash
export S3_BUCKET=your-production-bucket
export S3_REGION=us-east-1
# AWS credentials via 'aws configure'
```

### Quick Switching Commands
```bash
# Switch to LocalStack
source .env.localstack && ./scripts/run-s3-avro.sh

# Switch to AWS S3
source .env.aws-s3 && ./scripts/run-s3-avro.sh

# Use dedicated AWS script
./scripts/run-local-to-aws-s3.sh
```

---

## 🛠️ Script Reference

| Script | Purpose | Environment |
|--------|---------|-------------|
| `setup-localstack-s3.sh` | Auto-setup LocalStack | Local |
| `run-local-to-aws-s3.sh` | Local Flink → AWS S3 | Hybrid |
| `run-s3-avro.sh` | Universal S3 runner | Any |
| `test-s3-setup.sh` | Interactive testing | Any |
| `monitor-s3-pipeline.sh` | Real-time monitoring | Any |
| `validate-s3-data.sh` | Data quality checks | Any |

---

## 🚨 Troubleshooting

### Common Issues

#### **LocalStack Not Starting**
```bash
# Check Docker
docker info

# Check LocalStack logs
docker logs localstack

# Restart LocalStack
docker restart localstack
```

#### **AWS Access Denied**
```bash
# Verify credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://your-bucket/

# Check IAM permissions
aws iam get-user
```

#### **No Files in S3**
```bash
# Check Flink job status
curl http://localhost:8081/jobs

# Check job logs
tail -f flink-1.18.0/log/flink-*-taskexecutor-*.log

# Verify CDC events
psql -h localhost -p 5432 -U cdc_user -d cdc_source -c "SELECT * FROM users LIMIT 5;"
```

#### **High Latency**
```bash
# Test S3 connectivity
aws s3 cp /dev/null s3://your-bucket/test --region your-region
aws s3 rm s3://your-bucket/test

# Check network
ping s3.amazonaws.com

# Monitor metrics
./scripts/monitor-s3-pipeline.sh
```

### Performance Optimization

#### **Increase Parallelism**
```bash
# Edit S3ProductionPostgresCdcJob.scala
env.setParallelism(4)  // Increase from 2 to 4
```

#### **Optimize S3 Settings**
```bash
# Larger files, less frequent uploads
export S3_MAX_FILE_SIZE=512MB
export S3_ROLLOVER_INTERVAL=15min
```

#### **Enable S3 Transfer Acceleration**
```bash
aws s3api put-bucket-accelerate-configuration \
  --bucket your-bucket \
  --accelerate-configuration Status=Enabled
```

---

## 📈 Performance Benchmarks

### Expected Performance
- **Processing Latency**: 200-500ms average
- **Throughput**: 1,000-10,000 records/second (depending on parallelism)
- **File Size**: 64MB-512MB per Avro file
- **Rollover Frequency**: 5-15 minutes

### Scaling Guidelines
- **Light Load** (< 1K records/sec): 1-2 parallelism
- **Medium Load** (1K-10K records/sec): 2-4 parallelism  
- **Heavy Load** (> 10K records/sec): 4-8 parallelism + EMR cluster

---

## 🎯 Quick Reference Commands

### Development Workflow
```bash
# 1. Start services
./flink-1.18.0/bin/start-cluster.sh
docker-compose up -d postgres-cdc

# 2. Choose environment
./scripts/setup-localstack-s3.sh          # Local development
./scripts/run-local-to-aws-s3.sh           # Production testing
source .env.aws-s3 && ./scripts/run-s3-avro.sh  # Production

# 3. Monitor and validate
./scripts/monitor-s3-pipeline.sh
./scripts/validate-s3-data.sh

# 4. Create test data
psql -h localhost -p 5432 -U cdc_user -d cdc_source
INSERT INTO users (name, email) VALUES ('Test', 'test@example.com');
```

### Data Analysis
```bash
# Download Avro tools
wget https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar

# Convert Avro to JSON
java -jar avro-tools-1.11.3.jar tojsonpretty file.avro

# Query with AWS Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM cdc_users WHERE operation='INSERT'" \
  --result-configuration OutputLocation=s3://your-results-bucket/
```

---

🎉 **Complete S3 Setup Guide** - Choose your scenario and follow the appropriate section. All paths lead to the same high-quality Avro CDC data in S3! 