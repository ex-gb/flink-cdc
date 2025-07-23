package com.example.cdc.config

import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties
import java.io.{FileInputStream, InputStream}
import scala.util.{Try, Success, Failure}

/**
 * Production-ready configuration management for Multi-Database CDC to GCS pipeline
 * 
 * Now supports both PostgreSQL and MySQL databases with automatic detection,
 * GCP/GCS integration, environment-specific configuration files, and
 * backward compatibility for existing PostgreSQL configurations.
 * 
 * Configuration loading priority:
 * 1. Command line arguments (highest priority)
 * 2. Environment-specific config file (dev.properties, staging.properties, etc.)
 * 3. Default application.properties
 * 4. Environment variables
 * 5. System properties (lowest priority)
 * 
 * Usage: --env dev --config-file dev.properties
 * Environment variables with ${VAR_NAME} syntax are automatically substituted
 */
trait AppConfig {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  // Load configuration from multiple sources with priority order
  lazy val params: ParameterTool = loadConfiguration()
  
  // Database configuration - abstracted for multi-database support
  lazy val databaseConfig: DatabaseConfig = DatabaseConfigFactory.createDatabaseConfig(params)
  
  private def loadConfiguration(): ParameterTool = {
    logger.info("Loading configuration from multiple sources...")
    
    // Start with system properties as base
    var parameterTool = ParameterTool.fromSystemProperties()
    
    // Load default application.properties
    Try {
      val defaultProps = new Properties()
      val defaultStream: InputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
      if (defaultStream != null) {
        defaultProps.load(defaultStream)
        defaultStream.close()
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(defaultProps.asInstanceOf[java.util.Map[String, String]]))
        logger.info("✅ Loaded default application.properties")
      }
    }.recover {
      case ex => logger.warn(s"Could not load default application.properties: ${ex.getMessage}")
    }
    
    // Get environment from command line args first (for config file selection)
    val envFromArgs = sys.props.get("env").orElse(sys.env.get("ENV")).getOrElse("dev")
    
    // Load environment-specific configuration file
    val configFileName = s"${envFromArgs}.properties"
    Try {
      val envProps = new Properties()
      val envStream: InputStream = getClass.getClassLoader.getResourceAsStream(configFileName)
      if (envStream != null) {
        envProps.load(envStream)
        envStream.close()
        
        // Substitute environment variables
        val substitutedProps = substituteEnvironmentVariables(envProps)
        parameterTool = parameterTool.mergeWith(ParameterTool.fromMap(substitutedProps))
        logger.info(s"✅ Loaded environment-specific config: $configFileName")
      } else {
        logger.warn(s"Environment config file not found: $configFileName, using defaults")
      }
    }.recover {
      case ex => logger.warn(s"Could not load environment config file $configFileName: ${ex.getMessage}")
    }
    
    // Finally, merge with command line arguments (highest priority)
    parameterTool = parameterTool.mergeWith(ParameterTool.fromArgs(Array.empty))
    
    logger.info("Configuration loading completed")
    parameterTool
  }
  
  /**
   * Substitute environment variables in properties values
   * Supports ${VAR_NAME} syntax
   */
  private def substituteEnvironmentVariables(props: Properties): java.util.Map[String, String] = {
    import scala.collection.JavaConverters._
    
    val result = new java.util.HashMap[String, String]()
    
    props.asScala.foreach { case (key, value) =>
      val substitutedValue = substituteEnvVars(value.toString)
      result.put(key.toString, substitutedValue)
    }
    
    result
  }
  
  private def substituteEnvVars(value: String): String = {
    val pattern = """\$\{([^}]+)\}""".r
    pattern.replaceAllIn(value, m => {
      val envVar = m.group(1)
      sys.env.getOrElse(envVar, m.matched) // Keep original if env var not found
    })
  }
  
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
  
  // GCP Configuration for Google Cloud Storage
  object GcpConfig {
    val projectId: String = params.get("gcp.project-id", params.get("gcp-project", ""))
    val region: String = params.get("gcp.region", "us-central1")
    
    // GCS Configuration
    val bucketName: String = params.get("gcs.bucket-name", params.get("gcs-bucket", "flink-cdc-output"))
    val basePath: String = params.get("gcs.base-path", "cdc-events")
    val gcsRegion: String = params.get("gcs.region", region)
    
    // File format and compression
    val fileFormat: String = params.get("gcs.file-format", params.get("gcs-file-format", "avro")) // json, avro, parquet
    val compressionType: String = params.get("gcs.compression-type", params.get("gcs-compression-type", "snappy")) // gzip, snappy, lz4
    val maxFileSize: String = params.get("gcs.max-file-size", params.get("gcs-max-file-size", "128MB"))
    val rolloverInterval: String = params.get("gcs.rollover-interval", params.get("gcs-rollover-interval", "5min"))
    
    // Partitioning pattern
    val partitionFormat: String = params.get("gcs.partition-format", 
      "'year='yyyy'/month='MM'/day='dd'/hour='HH'/minute='mm")
    val timezone: String = params.get("gcs.timezone", "UTC")
    
    // Full GCS path construction
    def getFullPath: String = s"gs://$bucketName/$basePath"
    
    // Validation
    def validate(): Unit = {
      require(projectId.nonEmpty, "GCP project ID is required")
      require(bucketName.nonEmpty, "GCS bucket name is required")
    }
  }
  
  // S3 Sink Configuration (for backward compatibility)
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
  
  // Environment and Cloud Provider Configuration
  object EnvironmentConfig {
    val environment: String = params.get("env", "dev")
    val cloudProvider: String = params.get("cloud.provider", "aws") // aws or gcp
    
    def isGcp: Boolean = cloudProvider.toLowerCase == "gcp"
    def isAws: Boolean = cloudProvider.toLowerCase == "aws"
    def isDevelopment: Boolean = environment.toLowerCase == "dev"
    def isStaging: Boolean = environment.toLowerCase == "staging" || environment.toLowerCase == "stg"
    def isProduction: Boolean = environment.toLowerCase == "production" || environment.toLowerCase == "prod"
  }
  
  // Flink Job Configuration
  object FlinkConfig {
    val jobName: String = {
      val dbType = databaseConfig.databaseType.name.toUpperCase
      val cloudSuffix = if (EnvironmentConfig.isGcp) "GCS" else "S3"
      val envPrefix = EnvironmentConfig.environment.capitalize
      params.get("flink.job-name", s"$envPrefix-$dbType-CDC-to-$cloudSuffix")
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
    
    // State Backend Configuration
    val stateBackend: String = params.get("flink.state-backend", "filesystem")
    val checkpointsDirectory: String = {
      if (EnvironmentConfig.isGcp) {
        params.get("flink.checkpoints-directory", s"gs://flink-cdc-config-${GcpConfig.projectId}/checkpoints/${EnvironmentConfig.environment}")
      } else {
        params.get("flink.checkpoints-directory", s"s3a://flink-cdc-checkpoints/checkpoints/${EnvironmentConfig.environment}")
      }
    }
    val savepointsDirectory: String = {
      if (EnvironmentConfig.isGcp) {
        params.get("flink.savepoints-directory", s"gs://flink-cdc-config-${GcpConfig.projectId}/savepoints/${EnvironmentConfig.environment}")
      } else {
        params.get("flink.savepoints-directory", s"s3a://flink-cdc-checkpoints/savepoints/${EnvironmentConfig.environment}")
      }
    }
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
    
    if (EnvironmentConfig.isGcp) {
      GcpConfig.validate()
    } else {
      require(S3Config.bucketName.nonEmpty, "S3 bucket name is required")
    }
    
    require(FlinkConfig.parallelism > 0, "Flink parallelism must be positive")
    require(FlinkConfig.checkpointInterval > 0, "Checkpoint interval must be positive")
  }
  
  // Print configuration summary
  def printConfigurationSummary(): Unit = {
    val dbType = databaseConfig.databaseType.name.toUpperCase
    val cloudProvider = EnvironmentConfig.cloudProvider.toUpperCase
    val storageTarget = if (EnvironmentConfig.isGcp) GcpConfig.getFullPath else S3Config.getFullPath
    
    logger.info("=" * 80)
    logger.info(s"$dbType CDC to $cloudProvider Configuration Summary")
    logger.info("=" * 80)
    logger.info(s"Environment: ${EnvironmentConfig.environment}")
    logger.info(s"Cloud Provider: $cloudProvider")
    logger.info(s"Database Type: $dbType")
    logger.info(s"Job Name: ${FlinkConfig.jobName}")
    logger.info(s"Parallelism: ${FlinkConfig.parallelism}")
    logger.info(s"Source: ${databaseConfig.hostname}:${databaseConfig.port}/${databaseConfig.database}")
    logger.info(s"Tables: ${databaseConfig.tableList}")
    logger.info(s"Destination: $storageTarget")
    if (EnvironmentConfig.isGcp) {
      logger.info(s"GCP Project: ${GcpConfig.projectId}")
      logger.info(s"File Format: ${GcpConfig.fileFormat}")
      logger.info(s"Compression: ${GcpConfig.compressionType}")
    } else {
      logger.info(s"File Format: ${S3Config.fileFormat}")
      logger.info(s"Compression: ${S3Config.compressionType}")
    }
    logger.info(s"Checkpoint Interval: ${FlinkConfig.checkpointInterval}ms")
    logger.info(s"State Backend: ${FlinkConfig.stateBackend}")
    logger.info(s"Monitoring: ${MonitoringConfig.enableMetrics}")
    logger.info("=" * 80)
  }
} 