# PostgreSQL CDC to S3 Production Pipeline

A production-ready Apache Flink application for streaming PostgreSQL database changes to S3 using Change Data Capture (CDC). **Currently focused on PostgreSQL** with **architecture prepared for future MySQL/Oracle expansion**. Successfully tested and running on **Flink 1.18.0** with **CDC 3.4.0** - fully resolves the IllegalAccessError issue that occurred with earlier versions.

## 🚀 Features

### Core Functionality
- **PostgreSQL CDC**: Capture PostgreSQL changes using Debezium with zero data loss (architecture ready for MySQL/Oracle)
- **Multi-table Support**: Process multiple tables from single or multiple PostgreSQL databases simultaneously
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
- **Default Format**: **Avro** (with automatic schema evolution support)
- **Multiple Formats**: JSON, Avro, Parquet support (configurable)
- **Default Compression**: **Snappy** (optimal for Avro format)
- **Compression Options**: gzip, snappy, lz4 compression options
- **Partitioning**: Date/time-based partitioning for efficient querying
- **File Naming**: Consistent file naming with timestamps and processing metadata

## 🏗️ Architecture

### Project Structure
```
flink-cdc-s3/
├── src/main/scala/com/example/cdc/
│   ├── config/AppConfig.scala              # Configuration management (Avro defaults)
│   ├── sink/S3Sink.scala                   # Production S3 sink with Avro support
│   ├── monitoring/CDCMonitor.scala         # Monitoring and metrics
│   ├── transformation/CDCEventProcessor.scala  # Event processing and routing
│   ├── model/CdcEvent.scala                # CDC event data model
│   ├── parser/DebeziumEventParser.scala    # Debezium event parsing
│   └── ProductionCdcJob.scala              # Main production job (writes Avro to S3)
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
- **S3 Plugin**: Flink S3 filesystem plugin (`flink-s3-fs-hadoop-1.18.0.jar`) for S3 connectivity
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

# S3 Configuration (for ProductionCdcJob)
export S3_BUCKET_NAME=flink-cdc-output
export S3_BASE_PATH=cdc-events
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# S3 Format Configuration (optional - defaults to Avro)
export S3_FILE_FORMAT=avro              # avro (default), json, parquet
export S3_COMPRESSION_TYPE=snappy       # snappy (default), gzip, lz4
export S3_MAX_FILE_SIZE=128MB           # Maximum file size before rollover
export S3_ROLLOVER_INTERVAL=5min        # Time-based rollover interval

# Flink Configuration
export FLINK_PARALLELISM=2
export FLINK_CHECKPOINT_INTERVAL_MS=30000
```

### Application Configuration
The **ProductionCdcJob** is the main production job for PostgreSQL CDC that writes to S3 with **Avro format by default**:

```bash
# Production deployment with Avro format (default)
flink run -c com.example.cdc.ProductionCdcJob app.jar \
  --hostname prod-host --port 5432 --database prod_db \
  --username prod_user --password prod_pass --slot-name prod_slot \
  --s3-bucket prod-bucket --s3-region us-east-1 \
  --s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY

# Override format to JSON if needed
flink run -c com.example.cdc.ProductionCdcJob app.jar \
  --hostname prod-host --port 5432 --database prod_db \
  --username prod_user --password prod_pass --slot-name prod_slot \
  --s3-bucket prod-bucket --s3-file-format json --s3-compression-type gzip

# Local development (without S3 parameters, for testing)
flink run -c com.example.cdc.ProductionCdcJob app.jar \
  --hostname localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password --slot-name test_slot
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

### 1.1. Enable S3 Filesystem Plugin (Required for S3 Integration)
```bash
# Navigate to Flink directory
cd flink-1.18.0

# Enable the S3 filesystem plugin by moving it to the plugins directory
# This enables Flink to write directly to S3 buckets
mkdir -p plugins/flink-s3-fs-hadoop
cp opt/flink-s3-fs-hadoop-1.18.0.jar plugins/flink-s3-fs-hadoop/

# Verify the plugin is enabled
ls -la plugins/flink-s3-fs-hadoop/
```

**Why is this needed?**
- The `flink-s3-fs-hadoop-1.18.0.jar` provides S3 filesystem connectivity for Flink
- Without this plugin, Flink cannot write to S3 buckets (you'll get filesystem errors)
- The plugin handles S3 authentication, multipart uploads, and AWS S3 API interactions
- It's required for both production S3 writing and S3-based checkpointing

**Note**: The plugin is included in the Flink distribution but needs to be manually enabled by moving it to the plugins directory.

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

# This creates: target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar
```

### 4. Local Development and Testing
```bash
# Start Flink 1.18.0 cluster
./flink-1.18.0/bin/start-cluster.sh

# Submit job for local testing (without S3 parameters)
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionCdcJob \
  target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --hostname localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password \
  --slot-name flink_cdc_slot_test

# Test by inserting data
docker exec postgres-cdc psql -U cdc_user -d cdc_source -c \
  "INSERT INTO public.users (name, email) VALUES ('Test User', 'test@example.com');"
```

### 5. Production Deployment (S3 with Avro Format)
```bash
# Submit production job (writes Avro files to S3 by default)
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionCdcJob \
  target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --hostname prod-host --port 5432 --database prod_db \
  --username prod_user --password prod_pass \
  --slot-name flink_cdc_slot_production \
  --s3-bucket your-production-bucket \
  --s3-region us-east-1 \
  --s3-access-key $AWS_ACCESS_KEY_ID \
  --s3-secret-key $AWS_SECRET_ACCESS_KEY

# Alternative: Use environment variables for credentials
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionCdcJob \
  target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --hostname prod-host --port 5432 --database prod_db \
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

### Sample Output (ProductionCdcJob)
```bash
📥 RAW CDC Event received: {"before":null,"after":{"id":1,"name":"John"},...}
✅ Event matched table users: {"before":null,"after":{"id":1,"name":"John"},...}
CDC_METRIC table=users operation=c latency=275ms
📤 [users] WRITING to S3: s3://flink-cdc-output/cdc-events/users/
📊 [users] Event size: 1225 bytes
📄 File format: AVRO with snappy compression
[users] S3_WRITTEN: {"before":null,"after":{"id":1,"name":"John"},...}
```

### Sample Output (ProductionCdcJob - Local Test Mode)
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
│   ├── year=2025/month=07/day=09/hour=10/minute=15/
│   │   ├── users-2025-07-09-10-15-01-...-0.avro
│   │   └── users-2025-07-09-10-15-01-...-1.avro
│   └── year=2025/month=07/day=09/hour=10/minute=16/
│       ├── users-2025-07-09-10-15-01-...-0.avro
│       └── users-2025-07-09-10-15-01-...-1.avro
├── orders/
│   └── year=2025/month=07/day=09/hour=10/minute=15/
│       └── orders-2025-07-09-10-15-01-...-0.avro
├── errors/
│   └── year=2025/month=07/day=09/hour=10/minute=15/
│       └── errors-2025-07-09-10-15-01-...-0.avro
└── schema-changes/
    └── year=2025/month=07/day=09/hour=10/minute=15/
        └── schema-changes-2025-07-09-10-15-01-...-0.avro
```

**File Format Details:**
- **Extension**: `.avro` (Apache Avro format)
- **Compression**: Snappy compression (default)
- **Schema**: Comprehensive CDC schema with before/after/source metadata
- **Partitioning**: Time-based partitioning down to minute level
- **Separate Streams**: Users, orders, errors, and schema changes in separate directories

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
- **Default settings**: Avro format with Snappy compression
- **Environment variable overrides**: Support for all configuration parameters
- **Command-line parameter support**: Override any configuration via CLI
- **Format selection**: `--s3-file-format avro|json|parquet`
- **Compression options**: `--s3-compression-type snappy|gzip|lz4`
- **Single production-ready job**: ProductionCdcJob for PostgreSQL with S3 output (Avro format by default)

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

# Submit with explicit main class (Local Test Mode)
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionCdcJob \
  target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --hostname localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password \
  --slot-name flink_cdc_slot_test

# Submit S3 production job (writes Avro to S3)
flink-1.18.0/bin/flink run -c com.example.cdc.ProductionCdcJob \
  target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --hostname localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password \
  --slot-name flink_cdc_slot_test \
  --s3-bucket flink-cdc-output
```

#### 4. S3 Plugin Issues
```bash
# Check if S3 plugin is enabled
ls -la flink-1.18.0/plugins/flink-s3-fs-hadoop/

# If plugin is missing, enable it
cd flink-1.18.0
mkdir -p plugins/flink-s3-fs-hadoop
cp opt/flink-s3-fs-hadoop-1.18.0.jar plugins/flink-s3-fs-hadoop/

# Restart Flink cluster after enabling plugin
./bin/stop-cluster.sh
./bin/start-cluster.sh
```

**Common S3 Plugin Errors:**
- `UnknownStoreException: NoSuchBucket` - Create the S3 bucket first
- `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` - S3 plugin not enabled
- `IllegalArgumentException: AWS credentials not found` - Set AWS credentials

#### 5. S3 Permissions (Production Mode)
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
// In ProductionCdcJob
val checkpointInterval = 30000L  // 30 seconds (production: 60000L)
val maxConcurrentCheckpoints = 1
val parallelism = 2  // production mode uses 2 for better throughput

// S3-specific configuration
val fileFormat = "avro"  // default format
val compressionType = "snappy"  // default compression
val maxFileSize = "128MB"
val rolloverInterval = "5min"
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
- [ ] **S3 filesystem plugin enabled** (`flink-s3-fs-hadoop-1.18.0.jar` in plugins directory)
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

### Current: v1.2.0
- ✅ **Future-ready architecture** - Renamed to `ProductionCdcJob` for extensibility
- ✅ **Generic CDC framework** - Removed PostgreSQL-specific naming constraints
- ✅ **Generic project naming** - Updated from `postgres-cdc-s3` to `flink-cdc-s3`
- ✅ **Extensible design** - Prepared for future MySQL/Oracle expansion
- ✅ **Enhanced banner and messaging** - Reflects architectural readiness

### v1.1.0
- ✅ **Avro as default format** for S3 output with Snappy compression
- ✅ **ProductionPostgresCdcJob** unified job for S3 writing (Avro default)
- ✅ **Enhanced S3 integration** with proper error handling and monitoring
- ✅ **Comprehensive file format support** (Avro, JSON, Parquet)
- ✅ **Production-ready checkpointing** with S3 state backend support

### v1.0.0
- ✅ **Flink 1.18.0** + **CDC 3.4.0** compatibility
- ✅ **IllegalAccessError resolved** completely
- ✅ Production-ready with LOCAL TEST MODE
- ✅ Comprehensive monitoring and error handling
- ✅ Multi-table CDC support with S3 integration

---

**Production-Ready PostgreSQL CDC to S3 Pipeline** - Built with Apache Flink 1.18.0 and CDC 3.4.0, featuring **Avro as default format** with Snappy compression. **Currently focused on PostgreSQL** with extensible architecture for future MySQL/Oracle support. Fully tested and verified with zero class loading issues. 🚀 