# GCP Flink CDC Administration Guide
## Complete Guide for Migrating and Managing Flink CDC Pipeline on Google Cloud Platform

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Migration Overview](#migration-overview)
3. [GCP Infrastructure Setup](#gcp-infrastructure-setup)
4. [Flink Cluster Deployment](#flink-cluster-deployment)
5. [Code Migration & Build](#code-migration--build)
6. [Job Management](#job-management)
7. [Monitoring & Operations](#monitoring--operations)
8. [Security & Authentication](#security--authentication)
9. [Troubleshooting](#troubleshooting)
10. [Production Checklist](#production-checklist)

---

## 📋 Executive Summary

This guide provides everything needed to migrate and operate a **Multi-Database CDC Pipeline** from AWS to Google Cloud Platform (GCP). The migration transforms your current AWS S3-based architecture to a GCP-native solution using Google Cloud Storage (GCS) and managed Flink services.

### Migration Benefits
- **Managed Flink service** via Google Dataproc
- **Enhanced monitoring** with GCP-native tools
- **Improved scalability** with auto-scaling capabilities
- **Reduced operational overhead** through managed services

### Architecture Transformation

```
CURRENT (AWS):
PostgreSQL/MySQL → Self-managed Flink → AWS S3
                ↓
            AWS IAM/Profiles

TARGET (GCP):
PostgreSQL/MySQL → Google Dataproc Flink → Google Cloud Storage
                ↓
            Service Accounts/ADC
```

---

## 🔄 Migration Overview

### Component Mapping

| Current (AWS) | Target (GCP) | Migration Effort |
|---------------|--------------|------------------|
| AWS S3 (`s3a://`) | Google Cloud Storage (`gs://`) | Low |
| AWS IAM/Profiles | Service Accounts/ADC | Medium |
| Self-managed Flink | Google Dataproc | Medium |
| AWS CLI | Google Cloud CLI (`gcloud`) | Low |

### Migration Timeline
- **Week 1-2**: Development environment setup and code changes
- **Week 3**: Staging environment validation
- **Week 4-5**: Production migration with parallel validation

---

## 🏗️ GCP Infrastructure Setup

### Prerequisites
1. **Google Cloud CLI**: Install and configure `gcloud` with authentication
2. **Project Setup**: Create GCP project and set default region (recommended: `us-central1`)
3. **APIs**: Enable Dataproc, Storage, Compute, Monitoring, and Logging APIs

### Service Account Configuration
Create dedicated service account with minimal required permissions:
- **Storage Admin**: For GCS bucket access
- **Dataproc Worker**: For job submission and management
- **Monitoring Metric Writer**: For custom metrics

### GCS Bucket Structure
Create three main buckets with proper directory structure:

**Data Bucket**: `gs://flink-cdc-data-{project}`
- CDC output storage organized by table and date partitions
- Lifecycle policies for cost optimization

**JAR Bucket**: `gs://flink-cdc-jars-{project}`  
- Application JAR files and dependencies
- Version-controlled artifact storage

**Config Bucket**: `gs://flink-cdc-config-{project}`
- `checkpoints/` - Automatic Flink checkpoints (7-day retention)
- `savepoints/` - Manual savepoints for job recovery
- `ha/` - High availability metadata (optional)

```bash
# Create bucket structure
PROJECT_ID=$(gcloud config get-value project)

# Create main buckets
gsutil mb -p $PROJECT_ID -c STANDARD -l us-central1 gs://flink-cdc-data-$PROJECT_ID
gsutil mb -p $PROJECT_ID -c STANDARD -l us-central1 gs://flink-cdc-jars-$PROJECT_ID  
gsutil mb -p $PROJECT_ID -c STANDARD -l us-central1 gs://flink-cdc-config-$PROJECT_ID

# Create directory structure in config bucket
gsutil -m cp /dev/null gs://flink-cdc-config-$PROJECT_ID/checkpoints/.gitkeep
gsutil -m cp /dev/null gs://flink-cdc-config-$PROJECT_ID/savepoints/.gitkeep
gsutil -m cp /dev/null gs://flink-cdc-config-$PROJECT_ID/ha/.gitkeep
```

### Network Security
- Configure firewall rules for internal Flink communication
- Restrict external access to admin IPs only
- Enable private Google Access for enhanced security

---

## 🚀 Flink Cluster Deployment

### Option 1: Google Dataproc (Recommended)

**Production Cluster Configuration:**
- **Master**: 1x n1-standard-4 (100GB disk)
- **Workers**: 3x n1-standard-4 (100GB disk)
- **Auto-scaling**: 2-10 workers with preemptible instances
- **Components**: Flink enabled with optimized properties

**Development Cluster Configuration:**
- **Master**: 1x n1-standard-2 (50GB disk)
- **Workers**: 2x n1-standard-2 (50GB disk)
- **Preemptible workers**: Cost-optimized for development

### Option 2: Google Kubernetes Engine (Advanced)

For organizations requiring fine-grained control:
- **Workload Identity**: Secure authentication without service account keys
- **Auto-scaling**: Horizontal Pod Autoscaler for dynamic scaling
- **Resource Management**: Custom resource limits and requests

### Cluster Management Operations

```bash
# Essential cluster operations
gcloud dataproc clusters create/update/stop/start/delete
gcloud dataproc clusters list --region=us-central1
gcloud dataproc clusters describe CLUSTER_NAME --region=us-central1
```

---

## 🔧 Code Migration & Build

### Step 1: Update Dependencies (build.sbt)

```scala
// Replace AWS dependencies with GCP equivalents
libraryDependencies ++= Seq(
  // Remove AWS S3 dependencies
  // "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  
  // Add GCS dependencies
  "org.apache.flink" % "flink-gs-fs-hadoop" % flinkVersion,
  "com.google.cloud" % "google-cloud-storage" % "2.29.1",
  "com.google.auth" % "google-auth-library-oauth2-http" % "1.20.0",
  
  // Existing dependencies remain the same
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "com.ververica" % "flink-connector-postgres-cdc" % postgresCdcVersion,
  "com.ververica" % "flink-connector-mysql-cdc" % mysqlCdcVersion,
  // ... other existing dependencies
)
```

### Step 2: Create GCP Configuration

**File: `src/main/scala/com/example/cdc/config/AppConfig.scala`** (Update existing)

```scala
trait AppConfig {
  lazy val params: ParameterTool = ParameterTool.fromSystemProperties()
  
  // Add GCP configuration alongside existing S3 config
  object GcpConfig {
    val projectId: String = params.get("gcp.project-id", "")
    val bucketName: String = params.get("gcs.bucket-name", "flink-cdc-output")
    val basePath: String = params.get("gcs.base-path", "cdc-events")
    val region: String = params.get("gcs.region", "us-central1")
    val serviceAccountKeyPath: String = params.get("gcp.service-account-key", "")
    
    // File format and compression
    val fileFormat: String = params.get("gcs.file-format", "avro")
    val compressionType: String = params.get("gcs.compression-type", "snappy")
    val maxFileSize: String = params.get("gcs.max-file-size", "128MB")
    val rolloverInterval: String = params.get("gcs.rollover-interval", "5min")
    
    // Partitioning pattern
    val partitionFormat: String = params.get("gcs.partition-format", 
      "'year='yyyy'/month='MM'/day='dd'/hour='HH'/minute='mm")
    val timezone: String = params.get("gcs.timezone", "UTC")
    
    // GCS path construction
    def getFullPath: String = s"gs://$bucketName/$basePath"
    
    // Validation
    def validate(): Unit = {
      require(projectId.nonEmpty, "GCP project ID is required")
      require(bucketName.nonEmpty, "GCS bucket name is required")
    }
  }
  
  // Keep existing S3Config for backward compatibility
  // ... existing S3Config code
}
```

### Step 3: Implement GCS Sink

**File: `src/main/scala/com/example/cdc/sink/GCSSink.scala`** (New)

```scala
package com.example.cdc.sink

import com.example.cdc.config.AppConfig
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, RollingPolicy}
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneId
import java.util.concurrent.TimeUnit

object GCSSink {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Creates enhanced CDC sink for GCS with same API as S3Sink
   */
  def createEnhancedCDCSink(dataStream: DataStream[String], tableName: String, config: AppConfig): Unit = {
    
    // GCS path construction
    val tableSpecificPath = s"${config.GcpConfig.getFullPath}/$tableName"
    
    // Create bucket assigner for date/time partitioning
    val bucketAssigner = new DateTimeBucketAssigner[String](
      config.GcpConfig.partitionFormat,
      ZoneId.of(config.GcpConfig.timezone)
    )
    
    // Create rolling policy
    val rollingPolicy = DefaultRollingPolicy.builder()
      .withRolloverInterval(parseTimeToMillis(config.GcpConfig.rolloverInterval))
      .withInactivityInterval(parseTimeToMillis(config.GcpConfig.rolloverInterval))
      .withMaxPartSize(parseMemorySize(config.GcpConfig.maxFileSize))
      .build()
    
    // Create output file configuration
    val outputFileConfig = OutputFileConfig.builder()
      .withPartPrefix(generateFilePrefix(tableName, config))
      .withPartSuffix(getFileSuffix(config))
      .build()
    
    // Create GCS FileSink (same API as S3, different protocol)
    val fileSink = FileSink
      .forRowFormat(new Path(tableSpecificPath), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(bucketAssigner)
      .withRollingPolicy(rollingPolicy.asInstanceOf[RollingPolicy[String, String]])
      .withOutputFileConfig(outputFileConfig)
      .build()
    
    dataStream
      .sinkTo(fileSink)
      .name(s"$tableName-gcs-sink")
      .uid(s"$tableName-gcs-sink")
    
    logger.info(s"✅ Created GCS sink for table: $tableName -> $tableSpecificPath")
  }
  
  // Helper methods
  private def generateFilePrefix(tableName: String, config: AppConfig): String = {
    val timestamp = System.currentTimeMillis()
    val hostName = java.net.InetAddress.getLocalHost.getHostName.replace('.', '_')
    s"$tableName-$timestamp-$hostName"
  }
  
  private def getFileSuffix(config: AppConfig): String = {
    val formatSuffix = config.GcpConfig.fileFormat match {
      case "json" => ".jsonl"
      case "avro" => ".avro"
      case "parquet" => ".parquet"
      case _ => ".jsonl"
    }
    
    val compressionSuffix = config.GcpConfig.compressionType match {
      case "gzip" => ".gz"
      case "snappy" => ".snappy"
      case "lz4" => ".lz4"
      case _ => ""
    }
    
    formatSuffix + compressionSuffix
  }
  
  private def parseTimeToMillis(timeStr: String): Long = {
    val pattern = """(\d+)(min|sec|ms)""".r
    timeStr.toLowerCase match {
      case pattern(value, unit) =>
        val numValue = value.toLong
        unit match {
          case "min" => TimeUnit.MINUTES.toMillis(numValue)
          case "sec" => TimeUnit.SECONDS.toMillis(numValue)
          case "ms" => numValue
          case _ => TimeUnit.MINUTES.toMillis(5)
        }
      case _ => TimeUnit.MINUTES.toMillis(5)
    }
  }
  
  private def parseMemorySize(sizeStr: String): MemorySize = {
    val pattern = """(\d+)(MB|GB|KB)""".r
    sizeStr.toUpperCase match {
      case pattern(value, unit) =>
        val numValue = value.toInt
        unit match {
          case "MB" => MemorySize.ofMebiBytes(numValue)
          case "GB" => MemorySize.ofMebiBytes(numValue * 1024)
          case "KB" => MemorySize.parse(s"${numValue}kb")
          case _ => MemorySize.ofMebiBytes(128)
        }
      case _ => MemorySize.ofMebiBytes(128)
    }
  }
}
```

### Step 4: Update Main Job Class

**File: `src/main/scala/com/example/cdc/ProductionCdcJob.scala`** (Update existing)

```scala
// In the processMultiTableCDC method, update the sink selection:

envMode match {
  case "local" =>
    // LOCAL MODE: Just print, no cloud storage writing
    monitoredStream
      .map(new LocalSimulationMapper(tableName))
      .name(s"$tableName-local-simulation")
      .print(s"LOCAL-$tableName")
      
  case "dev" | "stg" | "prod" =>
    // Determine cloud provider based on configuration
    val cloudProvider = params.get("cloud.provider", "gcp") // Default to GCP
    
    cloudProvider.toLowerCase match {
      case "gcp" =>
        // GCS-ENABLED MODES: Write to GCS
        monitoredStream
          .map(new GcsLoggingMapper(tableName, config.GcpConfig.bucketName, config.GcpConfig.basePath, envMode))
          .name(s"$tableName-gcs-logger")
        
        // Use GCSSink
        GCSSink.createEnhancedCDCSink(monitoredStream, tableName, config)
        
      case "aws" =>
        // Legacy S3-ENABLED MODES: Write to S3 (for backward compatibility)
        S3Sink.createEnhancedCDCSink(monitoredStream, tableName, config)
    }
    
    // Monitor output
    monitoredStream
      .map(new CloudMonitoringMapper(tableName, envMode, cloudProvider))
      .name(s"$tableName-monitor-print")
      .print(s"${envMode.toUpperCase}-$tableName")
}
```

### Step 5: Build and Deploy

```bash
# Clean and build
sbt clean assembly

# Verify JAR was created
ls -la target/scala-2.12/

# Upload to GCS
PROJECT_ID=$(gcloud config get-value project)
gsutil cp target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-production-assembly-1.2.0.jar

# Verify upload
gsutil ls gs://flink-cdc-jars-$PROJECT_ID/
```

---

## 🎯 Job Management

### Environment-Specific Configuration Files

The application now supports environment-specific configuration files that eliminate the need for long CLI commands. Configuration files are automatically loaded based on the `--env` parameter:

- **`dev.properties`**: Development settings (JSON format, gzip compression, single parallelism)
- **`staging.properties`**: Staging settings (Avro format, snappy compression, moderate parallelism) 
- **`production.properties`**: Production settings (Avro format, snappy compression, high parallelism, HA enabled)

**Configuration Loading Priority:**
1. Command line arguments (highest priority)
2. Environment-specific config file (e.g., `dev.properties`)
3. Default `application.properties`
4. Environment variables (${VAR_NAME} syntax supported)
5. System properties (lowest priority)

### Required Environment Variables

Set these environment variables before job submission:

```bash
# Required for all environments
export PROJECT_ID=$(gcloud config get-value project)

# Database credentials (environment-specific)
export POSTGRES_HOST="your-postgres-host"
export POSTGRES_PASSWORD="your-postgres-password"
export MYSQL_HOST="your-mysql-host" 
export MYSQL_PASSWORD="your-mysql-password"

# Production only
export ZOOKEEPER_QUORUM="zk1:2181,zk2:2181,zk3:2181"
```

### Simplified Job Submission Commands

#### Development Environment - PostgreSQL

```bash
export PROJECT_ID=$(gcloud config get-value project)
export POSTGRES_HOST="localhost"
export POSTGRES_PASSWORD="cdc_password"

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-dev \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=dev,database=postgres" \
  -- \
  --env dev \
  --database.type postgres
```

#### Development Environment - MySQL

```bash
export PROJECT_ID=$(gcloud config get-value project)
export MYSQL_HOST="localhost"
export MYSQL_PASSWORD="cdc_password"

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-dev \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=dev,database=mysql" \
  -- \
  --env dev \
  --database.type mysql
```

#### Staging Environment - PostgreSQL

```bash
export PROJECT_ID=$(gcloud config get-value project)
export POSTGRES_HOST="staging-postgres.example.com"
export POSTGRES_PASSWORD="your-staging-password"

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=staging,database=postgres" \
  -- \
  --env staging \
  --database.type postgres
```

#### Production Environment - PostgreSQL

```bash
export PROJECT_ID=$(gcloud config get-value project)
export POSTGRES_HOST="production-postgres.example.com"
export POSTGRES_PASSWORD="your-secure-production-password"
export ZOOKEEPER_QUORUM="zk1:2181,zk2:2181,zk3:2181"

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=production,database=postgres,critical=true" \
  --max-restarts-per-hour=3 \
  -- \
  --env production \
  --database.type postgres
```

#### Production Environment - MySQL

```bash
export PROJECT_ID=$(gcloud config get-value project)
export MYSQL_HOST="production-mysql.example.com"
export MYSQL_PASSWORD="your-secure-production-password"
export ZOOKEEPER_QUORUM="zk1:2181,zk2:2181,zk3:2181"

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=production,database=mysql,critical=true" \
  --max-restarts-per-hour=3 \
  -- \
  --env production \
  --database.type mysql
```

#### Override Configuration Values

You can override any configuration value using command line arguments:

```bash
# Override specific database tables
gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=production,database=postgres" \
  -- \
  --env production \
  --database.type postgres \
  --postgres.table-list "public.users,public.critical_orders"

# Override file format for testing
gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-dev \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --labels="env=dev,database=postgres" \
  -- \
  --env dev \
  --database.type postgres \
  --gcs.file-format json \
  --gcs.compression-type none
```

### Configuration File Management

#### Viewing Current Configuration

The application automatically loads and validates configuration on startup. To verify your configuration:

```bash
# Check configuration loading in job logs
gcloud dataproc jobs describe JOB_ID --region=us-central1

# View configuration summary in the application logs
grep "Configuration Summary" /tmp/flink-jobmanager-*.log
```

#### Configuration File Validation

Environment-specific configuration files are validated automatically:

1. **Required Fields**: Database connection, GCP project, storage buckets
2. **Environment Variables**: `${VAR_NAME}` syntax must resolve to actual values
3. **Cloud Provider**: Validates GCP vs AWS compatibility 
4. **Resource Settings**: Parallelism, memory, and checkpoint settings

#### Common Configuration Issues

| Issue | Solution |
|-------|----------|
| `GCP project ID is required` | Set `PROJECT_ID` environment variable |
| `Environment config file not found` | Ensure `dev.properties`, `staging.properties`, or `production.properties` exists in classpath |
| `Database hostname is required` | Set `POSTGRES_HOST` or `MYSQL_HOST` environment variable |
| `Environment variable not resolved` | Verify `${VAR_NAME}` variables are exported in shell |

#### Configuration Override Examples

```bash
# Test with different table subset
--postgres.table-list "public.users"

# Use different file format temporarily
--gcs.file-format json --gcs.compression-type gzip

# Override checkpoint interval for testing
--flink.checkpoint-interval-ms 10000

# Change parallelism for specific job
--flink.parallelism 2
```

### Parameter Reference

| Parameter Category | Parameter | Description | Example |
|-------------------|-----------|-------------|---------|
| **Environment** | `--env` | Deployment environment (loads config file) | `dev`, `staging`, `production` |
| **Database** | `--database.type` | Database type override | `postgres`, `mysql` |
| **Override** | `--postgres.table-list` | Override PostgreSQL tables | `"public.users,public.orders"` |
| **Override** | `--mysql.table-list` | Override MySQL tables | `"db.users,db.orders"` |
| **Override** | `--gcs.file-format` | Override file format | `json`, `avro`, `parquet` |
| **Override** | `--gcs.compression-type` | Override compression | `gzip`, `snappy`, `lz4`, `none` |
| **Override** | `--flink.parallelism` | Override job parallelism | `1`, `2`, `4` |
| **Cloud Provider** | `--cloud.provider` | Cloud platform | `gcp`, `aws` |
| **Database Type** | `--database.type` | Database type | `postgres`, `mysql` |
| **Database Connection** | `--hostname` | Database host | `10.1.2.3`, `localhost` |
| | `--port` | Database port | `5432`, `3306` |
| | `--database` | Database name | `cdc_source` |
| | `--username` | Database username | `cdc_user` |
| | `--password` | Database password | `your_password` |
| **PostgreSQL Specific** | `--postgres.schema-list` | Schemas to capture | `public` |
| | `--postgres.table-list` | Tables to capture | `public.users,public.orders` |
| | `--postgres.slot-name` | Replication slot name | `flink_cdc_slot_prod` |
| | `--postgres.plugin-name` | Logical decoding plugin | `pgoutput` |
| **MySQL Specific** | `--mysql.table-list` | Tables to capture | `db.users,db.orders` |
| | `--mysql.server-id` | Server ID range | `5400-5410` |
| | `--mysql.server-time-zone` | Server timezone | `UTC` |
| | `--mysql.incremental-snapshot-enabled` | Enable incremental snapshots | `true`, `false` |
| **GCP Configuration** | `--gcp-project` | GCP project ID | `my-project-123` |
| | `--gcs-bucket` | GCS bucket name | `flink-cdc-data-prod` |
| | `--gcs-region` | GCS region | `us-central1` |
| | `--gcs-file-format` | Output file format | `avro`, `json`, `parquet` |
| | `--gcs-compression-type` | Compression type | `snappy`, `gzip`, `lz4` |
| | `--gcs-max-file-size` | Maximum file size | `128MB`, `256MB` |
| | `--gcs-rollover-interval` | File rollover interval | `5min`, `10min` |

### Checkpoint & Savepoint Configuration

#### GCS State Backend Configuration

For production deployments, configure Flink to store checkpoints and savepoints in GCS:

**Flink Configuration (flink-conf.yaml):**
```yaml
# State Backend Configuration
state.backend: filesystem
state.checkpoints.dir: gs://flink-cdc-config-{project}/checkpoints
state.savepoints.dir: gs://flink-cdc-config-{project}/savepoints

# Checkpoint Configuration
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 300000
execution.checkpointing.min-pause: 10000
execution.checkpointing.max-concurrent-checkpoints: 1

# GCS Authentication
fs.gs.project.id: {project-id}
fs.gs.auth.service.account.enable: true
fs.gs.auth.type: APPLICATION_DEFAULT

# High Availability (Optional)
high-availability: zookeeper
high-availability.storageDir: gs://flink-cdc-config-{project}/ha
high-availability.cluster-id: flink-cdc-cluster
```

#### Production Job Submission with GCS Checkpoints

```bash
PROJECT_ID=$(gcloud config get-value project)

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --properties="flink.parallelism.default=4,execution.checkpointing.interval=60000,state.backend=filesystem,state.checkpoints.dir=gs://flink-cdc-config-$PROJECT_ID/checkpoints,state.savepoints.dir=gs://flink-cdc-config-$PROJECT_ID/savepoints" \
  --labels="env=production,database=postgres,critical=true" \
  --max-restarts-per-hour=3 \
  -- \
  --env prod \
  --cloud.provider gcp \
  --database.type postgres \
  --hostname YOUR_PRODUCTION_POSTGRES_HOST \
  --port 5432 \
  --database cdc_source \
  --username cdc_user \
  --password YOUR_SECURE_DB_PASSWORD \
  --postgres.table-list "public.users,public.orders,public.transactions" \
  --postgres.slot-name flink_cdc_slot_production \
  --gcp-project $PROJECT_ID \
  --gcs-bucket flink-cdc-data-$PROJECT_ID \
  --gcs-region us-central1 \
  --gcs-file-format avro \
  --gcs-compression-type snappy
```

#### Checkpoint Management Commands

**Monitor Checkpoints:**
```bash
PROJECT_ID=$(gcloud config get-value project)

# List checkpoint directories
gsutil ls gs://flink-cdc-config-$PROJECT_ID/checkpoints/

# Check latest checkpoints for a specific job
gsutil ls -l gs://flink-cdc-config-$PROJECT_ID/checkpoints/JOB_ID/ | tail -10

# Monitor checkpoint sizes
gsutil du -sh gs://flink-cdc-config-$PROJECT_ID/checkpoints/*/
```

**Create Manual Savepoint:**
```bash
# Get running job ID
JOB_ID=$(gcloud dataproc jobs list --region=us-central1 --filter="status.state=RUNNING" --format="value(reference.jobId)" | head -1)

# Create savepoint
gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=org.apache.flink.client.cli.CliFrontend \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  -- \
  savepoint $JOB_ID gs://flink-cdc-config-$PROJECT_ID/savepoints/manual-$(date +%Y%m%d_%H%M%S)

# List savepoints
gsutil ls gs://flink-cdc-config-$PROJECT_ID/savepoints/
```

**Restore from Savepoint:**
```bash
# Restore job from specific savepoint
SAVEPOINT_PATH="gs://flink-cdc-config-$PROJECT_ID/savepoints/savepoint-123456-abcdef"

gcloud dataproc jobs submit flink \
  --cluster=flink-cdc-cluster \
  --region=us-central1 \
  --class=com.example.cdc.ProductionCdcJob \
  --jars=gs://flink-cdc-jars-$PROJECT_ID/flink-cdc-gcs-assembly.jar \
  --properties="execution.savepoint.path=$SAVEPOINT_PATH,flink.parallelism.default=4" \
  -- \
  [your job arguments]
```

#### GCS Bucket Lifecycle for State Management

```bash
# Create lifecycle policy for checkpoint cleanup
cat > checkpoint-lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 7,
          "matchesPrefix": ["checkpoints/"]
        }
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {
          "age": 30,
          "matchesPrefix": ["savepoints/"]
        }
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
        "condition": {
          "age": 90,
          "matchesPrefix": ["savepoints/"]
        }
      }
    ]
  }
}
EOF

# Apply lifecycle policy
gsutil lifecycle set checkpoint-lifecycle.json gs://flink-cdc-config-$PROJECT_ID
```

### Job Control Operations
- **Monitor**: `gcloud dataproc jobs list/describe/wait`
- **Control**: `gcloud dataproc jobs cancel/kill`
- **Recovery**: Savepoint creation and restoration from GCS
- **State Management**: Checkpoint monitoring and cleanup

---

## 📊 Monitoring & Operations

### GCS Output Monitoring
```bash
# Monitor data output
gsutil ls -r gs://flink-cdc-data-{project}/cdc-events/
gsutil ls -l gs://flink-cdc-data-{project}/cdc-events/users/ | tail -10
```

### Cloud Monitoring Setup
- **Custom Dashboards**: CDC lag metrics, cluster health, GCS operations
- **Alerting Policies**: Job failures, high latency, resource usage
- **Log Aggregation**: Structured logging with severity filtering

### Automated Health Checks
Create monitoring script to check:
- Cluster status and running jobs
- Recent data output in GCS
- Database connectivity
- Alert integration for failures

---

## 🔒 Security & Authentication

### Production Authentication
- **Service Accounts**: Use Application Default Credentials in production
- **Workload Identity**: For GKE deployments (no service account keys)
- **Key Rotation**: Quarterly rotation of service account keys

### Network Security
- **Firewall Rules**: Internal Flink communication ports
- **Private Access**: Use private IPs for database connections
- **SSL/TLS**: Enable encryption for database connections

### Database Security
- **Private IP**: Use Cloud SQL private connections
- **SSL Certificates**: Client-side certificates for enhanced security
- **IAM Authentication**: Database IAM users where supported

---

## 🔧 Troubleshooting

### Common Issues

**Authentication Errors:**
- Verify credentials: `gcloud auth list`
- Test GCS access: `gsutil ls gs://bucket`
- Check service account permissions

**Job Submission Failures:**
- Verify cluster status and JAR existence
- Check Dataproc logs for startup errors
- Test with simple Flink job first

**Database Connectivity:**
- Test from Dataproc cluster using SSH
- Verify firewall rules and IP allowlists
- Check database connection parameters

**Performance Issues:**
- Scale cluster workers and increase parallelism
- Monitor Flink Web UI for backpressure
- Check database query performance

**Checkpoint/Savepoint Issues:**
- Verify GCS bucket permissions and accessibility
- Check Flink state backend configuration
- Monitor checkpoint sizes and completion times
- Validate savepoint compatibility across job restarts

### Emergency Recovery
Automated recovery script for:
1. Creating emergency savepoints
2. Cancelling failed jobs
3. Restarting cluster if needed
4. Manual intervention points

---

## 📋 Production Checklist

### Pre-Deployment
- [ ] GCP project and APIs configured
- [ ] Service accounts and permissions setup
- [ ] GCS buckets created with lifecycle policies
- [ ] Dataproc cluster provisioned and tested
- [ ] Network security configured
- [ ] Database connectivity verified

### Code Migration
- [ ] Dependencies updated to GCP
- [ ] GCP configuration classes implemented
- [ ] GCSSink replacing S3Sink
- [ ] Environment validation updated
- [ ] JAR built and uploaded to GCS

### Deployment
- [ ] Environment-specific configurations ready
- [ ] Job submission tested in development
- [ ] Monitoring and alerting configured
- [ ] Performance baseline established
- [ ] Documentation updated

### Operations
**Daily:** 
- Cluster health and job status verification
- Data output and checkpoint success monitoring
- GCS bucket storage utilization check

**Weekly:** 
- Performance metrics and CDC lag analysis
- Checkpoint/savepoint storage review and cleanup
- Cost analysis and optimization opportunities
- Log review and error pattern analysis

**Monthly:** 
- Security updates and service account key rotation
- Disaster recovery testing with savepoint restoration
- Checkpoint retention policy evaluation
- Documentation and runbook updates

### State Management Checklist
- [ ] GCS checkpoint directories created and accessible
- [ ] Flink state backend properly configured for GCS
- [ ] Checkpoint retention policies implemented
- [ ] Savepoint creation and restoration procedures tested
- [ ] High availability configuration validated (if enabled)
- [ ] State backend monitoring and alerting configured

---

This streamlined guide provides comprehensive coverage for migrating and operating Flink CDC jobs on GCP while maintaining concise, actionable content for administrators. 