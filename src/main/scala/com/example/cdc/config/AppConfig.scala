package com.example.cdc.config

import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}

/**
 * Production-ready configuration management for Multi-Database CDC to S3 pipeline
 * 
 * Now supports both PostgreSQL and MySQL databases with automatic detection
 * and backward compatibility for existing PostgreSQL configurations.
 * 
 * Configuration is loaded using Flink ParameterTool with support for:
 * - Command line arguments
 * - Properties files
 * - Environment variables
 * 
 * All keys use dot notation (not underscore) for consistency
 */
trait AppConfig {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  // Load configuration from multiple sources
  lazy val params: ParameterTool = ParameterTool.fromSystemProperties()
  
  // Database configuration - abstracted for multi-database support
  lazy val databaseConfig: DatabaseConfig = DatabaseConfigFactory.createDatabaseConfig(params)
  
  // Legacy PostgreSQL access for backward compatibility
  object PostgresConfig {
    val hostname: String = databaseConfig match {
      case config: PostgresConfig => config.hostname
      case _ => params.get("postgres.hostname", params.get("hostname", "localhost"))
    }
    val port: Int = databaseConfig match {
      case config: PostgresConfig => config.port
      case _ => params.getInt("postgres.port", params.getInt("port", 5432))
    }
    val database: String = databaseConfig.database
    val username: String = databaseConfig.username
    val password: String = databaseConfig.password
    val schemaList: String = databaseConfig.schemaList
    val tableList: String = databaseConfig.tableList
    val slotName: String = databaseConfig match {
      case config: PostgresConfig => config.slotName
      case _ => params.get("postgres.slot-name", params.get("slot.name", "flink_cdc_slot"))
    }
    val pluginName: String = databaseConfig match {
      case config: PostgresConfig => config.pluginName
      case _ => params.get("postgres.plugin-name", "pgoutput")
    }
    val startupMode: String = databaseConfig.startupMode
    val debeziumProperties = databaseConfig.debeziumProperties
    def getTableArray: Array[String] = databaseConfig.getTableArray
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
    val jobName: String = {
      val dbType = databaseConfig.databaseType.name.toUpperCase
      params.get("flink.job-name", s"$dbType-CDC-to-S3")
    }
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
  
  // Build CDC Source using the unified database abstraction
  def buildCDCSource(): CDCSource = {
    DatabaseSourceFactory.createSource(databaseConfig)
  }
  
  // Validate configuration
  def validateConfiguration(): Unit = {
    require(databaseConfig.hostname.nonEmpty, s"${databaseConfig.databaseType.name} hostname is required")
    require(databaseConfig.database.nonEmpty, s"${databaseConfig.databaseType.name} database is required")
    require(databaseConfig.username.nonEmpty, s"${databaseConfig.databaseType.name} username is required")
    require(databaseConfig.tableList.nonEmpty, s"${databaseConfig.databaseType.name} table list is required")
    require(S3Config.bucketName.nonEmpty, "S3 bucket name is required")
    require(FlinkConfig.parallelism > 0, "Flink parallelism must be positive")
    require(FlinkConfig.checkpointInterval > 0, "Checkpoint interval must be positive")
  }
  
  // Print configuration summary
  def printConfigurationSummary(): Unit = {
    val dbType = databaseConfig.databaseType.name.toUpperCase
    logger.info("=" * 80)
    logger.info(s"$dbType CDC to S3 Configuration Summary")
    logger.info("=" * 80)
    logger.info(s"Database Type: $dbType")
    logger.info(s"Job Name: ${FlinkConfig.jobName}")
    logger.info(s"Parallelism: ${FlinkConfig.parallelism}")
    logger.info(s"Source: ${databaseConfig.hostname}:${databaseConfig.port}/${databaseConfig.database}")
    logger.info(s"Tables: ${databaseConfig.tableList}")
    logger.info(s"Destination: ${S3Config.getFullPath}")
    logger.info(s"File Format: ${S3Config.fileFormat}")
    logger.info(s"Compression: ${S3Config.compressionType}")
    logger.info(s"Checkpoint Interval: ${FlinkConfig.checkpointInterval}ms")
    logger.info(s"Monitoring: ${MonitoringConfig.enableMetrics}")
    logger.info("=" * 80)
  }
} 