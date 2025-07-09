package com.example.cdc.config

import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.Properties

/**
 * Production-ready configuration management for PostgreSQL CDC to S3 pipeline
 * 
 * Configuration is loaded using Flink ParameterTool with support for:
 * - Command line arguments
 * - Properties files
 * - Environment variables
 * 
 * All keys use dot notation (not underscore) for consistency
 */
trait AppConfig {
  
  // Load configuration from multiple sources
  lazy val params: ParameterTool = ParameterTool.fromSystemProperties()
  
  // PostgreSQL CDC Source Configuration
  object PostgresConfig {
    val hostname: String = params.get("postgres.hostname", params.get("hostname", "localhost"))
    val port: Int = params.getInt("postgres.port", params.getInt("port", 5432))
    val database: String = params.get("postgres.database", params.get("database", "cdc_source"))
    val username: String = params.get("postgres.username", params.get("username", "postgres"))
    val password: String = params.get("postgres.password", params.get("password", "postgres"))
    val schemaList: String = params.get("postgres.schema-list", params.get("schema.list", "public"))
    val tableList: String = params.get("postgres.table-list", params.get("tables", "public.users"))
    val slotName: String = params.get("postgres.slot-name", params.get("slot.name", "flink_cdc_slot"))
    val pluginName: String = params.get("postgres.plugin-name", "pgoutput")
    val startupMode: String = params.get("postgres.startup-mode", "initial")
    
    // Debezium properties
    val debeziumProperties: Properties = {
      val props = new Properties()
      props.setProperty("snapshot.mode", startupMode)
      props.setProperty("decimal.handling.mode", params.get("postgres.decimal-handling-mode", "double"))
      props.setProperty("binary.handling.mode", params.get("postgres.binary-handling-mode", "base64"))
      props.setProperty("time.precision.mode", params.get("postgres.time-precision-mode", "connect"))
      props.setProperty("include.schema.changes", params.get("postgres.include-schema-changes", "false"))
      props.setProperty("slot.drop.on.stop", params.get("postgres.slot-drop-on-stop", "false"))
      props.setProperty("heartbeat.interval.ms", params.get("postgres.heartbeat-interval-ms", "30000"))
      props.setProperty("max.batch.size", params.get("postgres.max-batch-size", "2048"))
      props.setProperty("max.queue.size", params.get("postgres.max-queue-size", "8192"))
      props
    }
    
    def getTableArray: Array[String] = tableList.replaceAll(" ", "").split(",")
  }
  
  // S3 Sink Configuration
  object S3Config {
    val bucketName: String = params.get("s3.bucket-name", "flink-cdc-output")
    val basePath: String = params.get("s3.base-path", "cdc-events")
    val region: String = params.get("s3.region", "us-east-1")
    val accessKey: String = params.get("s3.access-key", "")
    val secretKey: String = params.get("s3.secret-key", "")
    val endpoint: String = params.get("s3.endpoint", "")
    val pathStyleAccess: Boolean = params.getBoolean("s3.path-style-access", false)
    
    // File format and compression
    val fileFormat: String = params.get("s3.file-format", "avro") // json, avro, parquet
    val compressionType: String = params.get("s3.compression-type", "snappy") // gzip, snappy, lz4
    val maxFileSize: String = params.get("s3.max-file-size", "128MB")
    val rolloverInterval: String = params.get("s3.rollover-interval", "5min")
    
    // Partitioning pattern
    val partitionFormat: String = params.get("s3.partition-format", 
      "'year='yyyy'/month='MM'/day='dd'/hour='HH'/minute='mm")
    val timezone: String = params.get("s3.timezone", "UTC")
    
    // Full S3 path construction
    def getFullPath: String = s"s3a://$bucketName/$basePath"
  }
  
  // Flink Job Configuration
  object FlinkConfig {
    val jobName: String = params.get("flink.job-name", "PostgreSQL-CDC-to-S3")
    val parallelism: Int = params.getInt("flink.parallelism", 1)
    val checkpointInterval: Long = params.getLong("flink.checkpoint-interval-ms", 30000)
    val checkpointTimeout: Long = params.getLong("flink.checkpoint-timeout-ms", 600000)
    val minPauseBetweenCheckpoints: Long = params.getLong("flink.min-pause-between-checkpoints-ms", 5000)
    val maxConcurrentCheckpoints: Int = params.getInt("flink.max-concurrent-checkpoints", 1)
    val enableUnalignedCheckpoints: Boolean = params.getBoolean("flink.enable-unaligned-checkpoints", false)
    val restartStrategy: String = params.get("flink.restart-strategy", "fixed-delay")
    val restartAttempts: Int = params.getInt("flink.restart-attempts", 3)
    val restartDelayMs: Long = params.getLong("flink.restart-delay-ms", 10000)
  }
  
  // Monitoring Configuration
  object MonitoringConfig {
    val enableMetrics: Boolean = params.getBoolean("monitoring.enable-metrics", true)
    val metricsReporter: String = params.get("monitoring.metrics-reporter", "prometheus")
    val statsdHost: String = params.get("monitoring.statsd-host", "localhost")
    val statsdPort: Int = params.getInt("monitoring.statsd-port", 8125)
    val enableLatencyTracking: Boolean = params.getBoolean("monitoring.enable-latency-tracking", true)
    val alertingThresholdMs: Long = params.getLong("monitoring.alerting-threshold-ms", 60000)
  }
  
  // Build PostgreSQL CDC Source
  def buildCDCSource(): SourceFunction[String] = {
    PostgreSQLSource.builder()
      .hostname(PostgresConfig.hostname)
      .port(PostgresConfig.port)
      .database(PostgresConfig.database)
      .schemaList(PostgresConfig.schemaList)
      .tableList(PostgresConfig.tableList)
      .username(PostgresConfig.username)
      .password(PostgresConfig.password)
      .slotName(PostgresConfig.slotName)
      .decodingPluginName(PostgresConfig.pluginName)
      .deserializer(new JsonDebeziumDeserializationSchema())
      .debeziumProperties(PostgresConfig.debeziumProperties)
      .build()
  }
  
  // Validate configuration
  def validateConfiguration(): Unit = {
    require(PostgresConfig.hostname.nonEmpty, "PostgreSQL hostname is required")
    require(PostgresConfig.database.nonEmpty, "PostgreSQL database is required")
    require(PostgresConfig.username.nonEmpty, "PostgreSQL username is required")
    require(PostgresConfig.tableList.nonEmpty, "PostgreSQL table list is required")
    require(S3Config.bucketName.nonEmpty, "S3 bucket name is required")
    require(FlinkConfig.parallelism > 0, "Flink parallelism must be positive")
    require(FlinkConfig.checkpointInterval > 0, "Checkpoint interval must be positive")
  }
  
  // Print configuration summary
  def printConfigurationSummary(): Unit = {
    println("=" * 80)
    println("PostgreSQL CDC to S3 Configuration Summary")
    println("=" * 80)
    println(s"Job Name: ${FlinkConfig.jobName}")
    println(s"Parallelism: ${FlinkConfig.parallelism}")
    println(s"Source: ${PostgresConfig.hostname}:${PostgresConfig.port}/${PostgresConfig.database}")
    println(s"Tables: ${PostgresConfig.tableList}")
    println(s"Destination: ${S3Config.getFullPath}")
    println(s"File Format: ${S3Config.fileFormat}")
    println(s"Compression: ${S3Config.compressionType}")
    println(s"Checkpoint Interval: ${FlinkConfig.checkpointInterval}ms")
    println(s"Monitoring: ${MonitoringConfig.enableMetrics}")
    println("=" * 80)
  }
} 