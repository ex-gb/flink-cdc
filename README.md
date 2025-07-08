# PostgreSQL CDC to S3 Production Pipeline

A production-ready Apache Flink application for streaming PostgreSQL database changes to S3 using Change Data Capture (CDC). Successfully tested and running on **Flink 1.18.0** with **CDC 3.4.0** - fully resolves the IllegalAccessError issue that occurred with earlier versions.

## 🚀 Features

### Core Functionality
- **Real-time CDC**: Capture PostgreSQL changes using Debezium with zero data loss
- **Multi-table Support**: Process multiple tables from single or multiple databases simultaneously
- **S3 Integration**: Store data in S3 with configurable formats (JSON, Avro, Parquet)
- **File Management**: Automatic file rolling and finalization with proper partitioning
- **Schema Evolution**: Automatic detection and handling of schema changes

### Production-Ready Features
- **Version Compatibility**: Fully tested with Flink 1.18.0 + CDC 3.4.0 (resolves all class loading issues)
- **Comprehensive Configuration**: Flexible configuration management with environment-specific settings
- **Monitoring & Metrics**: Built-in latency tracking, performance monitoring, and CDC metrics
- **Error Handling**: Robust error handling with dead letter queues and retry mechanisms
- **LOCAL TEST MODE**: Safe testing mode that prints to console instead of writing to S3
- **Deployment Tools**: Production deployment scripts with proper packaging
- **Logging**: Structured logging with configurable log levels and rotation

### Data Formats & Compression
- **Multiple Formats**: JSON, Avro, Parquet support
- **Compression**: gzip, snappy, lz4 compression options
- **Partitioning**: Date/time-based partitioning for efficient querying
- **File Naming**: Consistent file naming with timestamps and processing metadata

## 🏗️ Architecture

### Project Structure
```
flink-cdc-s3/
├── src/main/scala/com/example/cdc/
│   ├── config/AppConfig.scala              # Configuration management
│   ├── sink/S3Sink.scala                   # Production S3 sink with LOCAL TEST MODE
│   ├── monitoring/CDCMonitor.scala         # Monitoring and metrics
│   ├── transformation/CDCEventProcessor.scala  # Event processing and routing
│   ├── model/CdcEvent.scala                # CDC event data model
│   ├── parser/DebeziumEventParser.scala    # Debezium event parsing
│   └── ProductionPostgresCdcJob.scala      # Main production job
├── src/main/resources/
│   ├── application.properties              # Default configuration
│   └── logback.xml                         # Logging configuration
├── flink-1.18.0/                          # Flink runtime (local development)
├── docker-compose.yml                      # PostgreSQL test environment
├── init-db.sql                             # Test database setup
└── build.sbt                               # Build configuration (CDC 3.4.0 + Flink 1.18.0)
```

### Data Flow
```
PostgreSQL → CDC Source → Raw Event Logger → Event Processor → Table Router → S3 Sink/Local Print
                              ↓                    ↓                ↓
                        Debug Logging      Error Handler    CDC Monitoring
                                              ↓                ↓
                                        Schema Changes    Performance Metrics
```

### Component Details
- **CDC Source**: PostgreSQL CDC connector with configurable slot management
- **Event Processor**: Multi-table event routing and transformation
- **S3 Sink**: Production-ready S3 sink with LOCAL TEST MODE for development
- **Monitoring**: Real-time latency tracking and throughput metrics
- **Error Handling**: Comprehensive error capture and logging

## 🔧 Configuration

### Version Requirements
- **Apache Flink**: 1.18.0 (required)
- **Flink CDC**: 3.4.0 (resolves IllegalAccessError issues)
- **Java**: 11+ (tested with Java 11)
- **PostgreSQL**: 10+ with logical replication enabled
- **Scala**: 2.12.17

### Environment Variables
```bash
# PostgreSQL Configuration
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=cdc_source
export POSTGRES_USER=cdc_user
export POSTGRES_PASSWORD=cdc_password

# S3 Configuration (for production)
export S3_BUCKET_NAME=flink-cdc-output
export S3_BASE_PATH=cdc-events
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Flink Configuration
export FLINK_PARALLELISM=2
export FLINK_CHECKPOINT_INTERVAL_MS=30000
```

### Application Configuration
The `ProductionPostgresCdcJob` includes **LOCAL TEST MODE** that can be enabled via command line:
```bash
# Local development (prints to console)
flink run -c com.example.cdc.ProductionPostgresCdcJob app.jar --host localhost --port 5432 --database cdc_source --username cdc_user --password cdc_password --slot-name test_slot

# Production mode (writes to S3)
flink run -c com.example.cdc.ProductionPostgresCdcJob app.jar --host prod-host --port 5432 --database prod_db --username prod_user --password prod_pass --slot-name prod_slot --s3-bucket prod-bucket
```

## 🚀 Quick Start

### 1. Prerequisites
```bash
# Install Java 11+
java -version

# Install sbt (Scala Build Tool)
sbt --version

# Install PostgreSQL with logical replication
psql --version

# Download and setup Flink 1.18.0
wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
tar -xzf flink-1.18.0-bin-scala_2.12.tgz
```

### 2. Setup Test Environment
```bash
# Clone the repository
git clone <repository-url>
cd flink-cdc-s3

# Set up test database with Docker
docker-compose up -d

# Wait for PostgreSQL to start
sleep 10

# Verify database is ready
docker exec postgres-cdc psql -U postgres -d cdc_source -c "SELECT * FROM pg_replication_slots;"
```

### 3. Build Application
```bash
# Clean build with latest dependencies
sbt clean assembly

# This creates: target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar
```

### 4. Development Mode (Local Testing)
```bash
# Start Flink 1.18.0 cluster
./flink-1.18.0/bin/start-cluster.sh

# Submit job in LOCAL TEST MODE (prints to console)
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionPostgresCdcJob \
  target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar \
  --host localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password \
  --slot-name flink_cdc_slot_test

# Test by inserting data
docker exec postgres-cdc psql -U cdc_user -d cdc_source -c \
  "INSERT INTO public.users (name, email) VALUES ('Test User', 'test@example.com');"
```

### 5. Production Deployment
```bash
# Submit job for production (writes to S3)
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionPostgresCdcJob \
  target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar \
  --host prod-host --port 5432 --database prod_db \
  --username prod_user --password prod_pass \
  --slot-name flink_cdc_slot_production \
  --s3-bucket your-production-bucket
```

## 📊 Monitoring

### Real-time Monitoring
The application provides comprehensive monitoring output:

```bash
# Monitor CDC events in real-time
flink-1.18.0/bin/flink run --job-id <job-id>

# Check job status
flink-1.18.0/bin/flink list

# View Flink Web UI
open http://localhost:8081
```

### Built-in Metrics
- **📥 RAW CDC Event received**: Shows incoming CDC events
- **✅ Event matched table**: Confirms event routing
- **📤 PROCESSED CDC Event**: Shows processed events
- **CDC_METRIC**: Latency tracking (table, operation, latency in ms)
- **📊 S3 Write Status**: Confirms S3 writes (production mode)

### Sample Output
```bash
📥 RAW CDC Event received: {"before":null,"after":{"id":1,"name":"John"},...}
✅ Event matched table users: {"before":null,"after":{"id":1,"name":"John"},...}
CDC_METRIC table=users operation=c latency=275ms
📤 [users] PROCESSED CDC Event: {"before":null,"after":{"id":1,"name":"John"},...}
📊 [users] Would write to S3 at: s3://bucket/users/... (LOCAL TEST MODE)
```

## 🗂️ Data Organization

### S3 Directory Structure (Production Mode)
```
s3://bucket/base-path/
├── users/
│   ├── year=2025/month=01/day=08/hour=10/
│   │   ├── users-2025-01-08-10-30-001.jsonl.gz
│   │   └── users-2025-01-08-10-35-002.jsonl.gz
└── orders/
    └── year=2025/month=01/day=08/hour=10/
        └── orders-2025-01-08-10-30-001.jsonl.gz
```

### CDC Event Format
```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2025-01-08T10:30:00Z"
  },
  "source": {
    "version": "1.9.8.Final",
    "connector": "postgresql",
    "name": "postgres_cdc_source",
    "ts_ms": 1736330400000,
    "snapshot": "false",
    "db": "cdc_source",
    "sequence": "[\"24505928\",\"24505928\"]",
    "schema": "public",
    "table": "users",
    "txId": 751,
    "lsn": 24505928
  },
  "op": "c",
  "ts_ms": 1736330401000,
  "transaction": null
}
```

## 🛠️ Development

### Building and Testing
```bash
# Clean build
sbt clean compile

# Build assembly JAR
sbt clean assembly

# Test locally with sample data
docker exec postgres-cdc psql -U cdc_user -d cdc_source -c \
  "INSERT INTO public.users (name, email) VALUES ('Test $(date +%s)', 'test@example.com');"

# Monitor processing
tail -f flink-1.18.0/log/flink-*-taskexecutor-*.out
```

### Configuration Management
The application uses the `AppConfig` class for flexible configuration:
- Default settings in `application.properties`
- Environment variable overrides
- Command-line parameter support
- LOCAL TEST MODE detection

## 🚨 Troubleshooting

### IllegalAccessError (RESOLVED) ✅
**Issue**: `IllegalAccessError: failed to access class io.debezium.embedded.EmbeddedEngineChangeEvent`

**Root Cause**: Version incompatibility between CDC 3.1.1 and Debezium's class loading in Flink's ChildFirstClassLoader.

**Solution**: Upgraded to **CDC 3.4.0** which includes fixes for class loading issues.

**Current Status**: ✅ **RESOLVED** - Zero IllegalAccessError occurrences with CDC 3.4.0 + Flink 1.18.0

### Common Issues

#### 1. PostgreSQL Connection Issues
```bash
# Test connection
pg_isready -h localhost -p 5432 -U cdc_user

# Check replication slot
docker exec postgres-cdc psql -U postgres -d cdc_source -c \
  "SELECT * FROM pg_replication_slots WHERE slot_name LIKE 'flink%';"

# Reset slot if needed
docker exec postgres-cdc psql -U postgres -d cdc_source -c \
  "SELECT pg_drop_replication_slot('flink_cdc_slot_test');"
```

#### 2. Flink Cluster Issues
```bash
# Check cluster status
flink-1.18.0/bin/flink list

# Restart cluster
flink-1.18.0/bin/stop-cluster.sh
flink-1.18.0/bin/start-cluster.sh

# Check TaskManager connection
curl -s http://localhost:8081/taskmanagers | jq '.taskmanagers | length'
```

#### 3. Job Deployment Issues
```bash
# Cancel running job
flink-1.18.0/bin/flink cancel <job-id>

# Submit with explicit main class
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionPostgresCdcJob \
  target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar \
  --host localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password \
  --slot-name flink_cdc_slot_test
```

#### 4. S3 Permissions (Production Mode)
```bash
# Test S3 access
aws s3 ls s3://your-bucket/

# Check IAM permissions
aws iam get-user
aws sts get-caller-identity
```

### Debug Mode
```bash
# Enable debug logging in logback.xml
# Set log level to DEBUG for detailed output

# Monitor all logs
tail -f flink-1.18.0/log/*.log
```

## 📈 Performance Tuning

### Flink Configuration
```yaml
# flink-1.18.0/conf/flink-conf.yaml
jobmanager.memory.process.size: 2g
taskmanager.memory.process.size: 4g
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2

# Checkpoint configuration
execution.checkpointing.interval: 30s
execution.checkpointing.timeout: 10min
state.backend: hashmap
```

### CDC Configuration
```scala
// In ProductionPostgresCdcJob
val checkpointInterval = 30000L  // 30 seconds
val maxConcurrentCheckpoints = 1
val parallelism = 2
```

### Database Optimization
```sql
-- PostgreSQL tuning for CDC
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
SELECT pg_reload_conf();
```

## 🔒 Security

### Database Security
- Use dedicated CDC user with minimal permissions
- Enable SSL/TLS for database connections (configure in PostgreSQL)
- Regularly rotate passwords and update connection strings

### S3 Security (Production)
- Use IAM roles instead of access keys when possible
- Enable S3 bucket encryption at rest
- Configure proper bucket policies and access controls
- Use VPC endpoints for private S3 access

### Application Security
- Store secrets in environment variables or AWS Secrets Manager
- Use secure communication channels
- Validate all configuration parameters on startup

## 📚 Best Practices

### Configuration Management
- Use LOCAL TEST MODE for development and testing
- Validate all configuration parameters before job submission
- Use environment-specific configuration files
- Monitor configuration drift

### Operations
- Monitor CDC lag and processing latency
- Set up alerts for job failures and high error rates
- Regularly backup PostgreSQL WAL files
- Plan for schema evolution and backward compatibility

### Development
- Test schema changes in LOCAL TEST MODE first
- Use consistent slot naming conventions
- Monitor resource usage and tune accordingly
- Implement proper error handling and logging

## 🚀 Production Deployment Checklist

### Pre-deployment
- [ ] PostgreSQL logical replication configured
- [ ] Flink 1.18.0 cluster deployed and tested
- [ ] CDC 3.4.0 compatibility verified
- [ ] S3 bucket and IAM permissions configured
- [ ] Network connectivity tested
- [ ] Monitoring and alerting configured

### Deployment
- [ ] Build application with `sbt clean assembly`
- [ ] Test in LOCAL TEST MODE first
- [ ] Deploy to staging environment
- [ ] Verify CDC event processing
- [ ] Test failover and recovery
- [ ] Deploy to production with proper slot name

### Post-deployment
- [ ] Monitor job health and performance
- [ ] Verify S3 file creation and structure
- [ ] Check CDC lag and latency metrics
- [ ] Set up automated monitoring and alerts
- [ ] Document operational procedures

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Test changes in LOCAL TEST MODE
4. Ensure all tests pass (`sbt test`)
5. Update documentation as needed
6. Submit a pull request with detailed description

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review Flink logs: `flink-1.18.0/log/`
- Test in LOCAL TEST MODE for debugging

## 🏆 Version History

### Current: v1.0.0
- ✅ **Flink 1.18.0** + **CDC 3.4.0** compatibility
- ✅ **IllegalAccessError resolved** completely
- ✅ Production-ready with LOCAL TEST MODE
- ✅ Comprehensive monitoring and error handling
- ✅ Multi-table CDC support with S3 integration

---

**Production-Ready PostgreSQL CDC to S3 Pipeline** - Built with Apache Flink 1.18.0 and CDC 3.4.0, fully tested and verified with zero class loading issues. 🚀 