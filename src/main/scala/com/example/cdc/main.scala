package com.example.cdc

import com.example.cdc.config.AppConfig
import com.example.cdc.sink.{S3Sink, GCSSink}
import com.example.cdc.monitoring.CDCMonitor
import com.example.cdc.transformation.CDCEventProcessor
import com.example.cdc.handlers.{ErrorHandler, SchemaChangeHandler}
import com.example.cdc.mappers.{S3LoggingMapper, S3MonitoringMapper, GCSLoggingMapper, GCSMonitoringMapper, LocalSimulationMapper, AllEventsMonitor}
import com.example.cdc.filters.TableFilter
import com.example.cdc.validation.{EnvironmentValidator, ConfigurationException, EnvironmentValidationException, S3ValidationException}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.util.OutputTag
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit

/**
 * CDC-specific exception for processing errors
 */
case class CDCProcessingException(message: String, cause: Throwable = null) extends Exception(message, cause)

/**
 * Production CDC to Multi-Cloud Storage Job - Supports AWS S3 and Google Cloud Storage
 * 
 * Features:
 * - PostgreSQL and MySQL CDC processing
 * - Multi-table CDC processing
 * - Multi-cloud storage support (AWS S3 and Google Cloud Storage)
 * - Comprehensive error handling
 * - Monitoring and metrics
 * - Environment-aware deployment:
 *   * LOCAL MODE (--env local): Same processing pipeline, no cloud storage writing
 *   * DEV MODE (--env dev): Cloud storage writing to dev environment
 *   * STAGING MODE (--env staging): Cloud storage writing to staging environment
 *   * PRODUCTION MODE (--env production): Cloud storage writing to production environment
 * - Cloud provider selection:
 *   * AWS (--cloud.provider aws): Uses S3Sink for AWS S3 storage
 *   * GCP (--cloud.provider gcp): Uses GCSSink for Google Cloud Storage
 */
object ProductionCdcJob extends AppConfig {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val objectMapper = new ObjectMapper()
  
  def main(args: Array[String]): Unit = {
    try {
      // Parse command line arguments and merge with system properties
      val argsParams = ParameterTool.fromArgs(args)
      val mergedParams = ParameterTool.fromSystemProperties().mergeWith(argsParams)
      
      // Extract environment mode
      val envMode = mergedParams.get("env", "prod").toLowerCase
      
      // Create a new AppConfig instance with the merged parameters
      val config = new AppConfig {
        override lazy val params: ParameterTool = mergedParams
      }
      
      // Print banner with environment mode
      printBanner(envMode)
      
      // Log parsed command line arguments for debugging
      logger.info("Command line arguments received:")
      args.zipWithIndex.foreach { case (arg, i) => logger.info(s"  [$i]: $arg") }
      
      logger.info(s"Configuration loaded (Environment: ${envMode.toUpperCase}):")
      logger.info(s"  Cloud Provider: ${config.EnvironmentConfig.cloudProvider.toUpperCase}")
      logger.info(s"  Database Type: ${config.databaseConfig.databaseType}")
      logger.info(s"  Hostname: ${config.databaseConfig.hostname}")
      logger.info(s"  Database: ${config.databaseConfig.database}")
      logger.info(s"  Username: ${config.databaseConfig.username}")
      logger.info(s"  Tables: ${config.databaseConfig.tableList}")
      if (config.EnvironmentConfig.isGcp) {
        logger.info(s"  GCP Project: ${config.GcpConfig.projectId}")
        logger.info(s"  GCS Bucket: ${config.GcpConfig.bucketName}")
      } else {
        logger.info(s"  S3 Bucket: ${config.S3Config.bucketName}")
      }
      
      // Environment-specific configuration validation
      envMode match {
        case "local" =>
          logger.info("LOCAL MODE: Cloud storage operations will be simulated (print only)")
          logger.info("  ✅ No cloud credentials required")
          logger.info("  ✅ No actual cloud storage writes will occur")
          EnvironmentValidator.validateLocalModeConfig(config)
          
        case "dev" | "development" =>
          if (config.EnvironmentConfig.isGcp) {
            logger.info("DEVELOPMENT MODE: Real GCS writing to dev environment")
            logger.info(s"  🌥️ Writing to GCS bucket: ${config.GcpConfig.bucketName}")
          } else {
            logger.info("DEVELOPMENT MODE: Real S3 writing to dev environment")
            logger.info(s"  ☁️ Writing to S3 bucket: ${config.S3Config.bucketName}")
          }
          EnvironmentValidator.validateS3ModeConfig(config, "DEV")
          
        case "stg" | "staging" =>
          if (config.EnvironmentConfig.isGcp) {
            logger.info("STAGING MODE: Real GCS writing to staging environment")
            logger.info(s"  🌥️ Writing to GCS bucket: ${config.GcpConfig.bucketName}")
          } else {
            logger.info("STAGING MODE: Real S3 writing to staging environment")
            logger.info(s"  ☁️ Writing to S3 bucket: ${config.S3Config.bucketName}")
          }
          EnvironmentValidator.validateS3ModeConfig(config, "STAGING")
          
        case "prod" | "production" =>
          if (config.EnvironmentConfig.isGcp) {
            logger.info("PRODUCTION MODE: Real GCS writing to production environment")
            logger.info(s"  🌥️ Writing to GCS bucket: ${config.GcpConfig.bucketName}")
          } else {
            logger.info("PRODUCTION MODE: Real S3 writing to production environment")
            logger.info(s"  ☁️ Writing to S3 bucket: ${config.S3Config.bucketName}")
          }
          EnvironmentValidator.validateS3ModeConfig(config, "PRODUCTION")
          
        case _ =>
          logger.error(s"Invalid environment mode: $envMode")
          logger.error("   Valid modes: 'local', 'dev', 'staging', or 'production'")
          printUsage()
          System.exit(1)
      }
      
      config.validateConfiguration()
      config.printConfigurationSummary()
      
      // Set up Flink execution environment
      val env = setupFlinkEnvironment(envMode)
      
      // Create CDC source using the config with merged parameters
      logger.info("Creating database CDC source...")
      val cdcSource = config.buildCDCSource()
      val cdcStream = cdcSource.addToEnvironment(env, "database-cdc-source", "database-cdc-source")
      
      // Add raw CDC stream logging
      cdcStream
        .map(event => {
          logger.debug(s"RAW CDC Event received: ${event.take(200)}...")
          event
        })
        .name("raw-cdc-logger")
      
      // Process multi-table CDC stream (same pipeline for both modes)
      processMultiTableCDC(env, cdcStream, config, envMode)
      
      // Start the job with environment-specific messaging
      val modeDescription = envMode match {
        case "local" => "LOCAL TESTING MODE - No cloud storage operations"
        case "dev" | "development" => 
          if (config.EnvironmentConfig.isGcp) 
            s"DEVELOPMENT MODE - Writing to GCS bucket: ${config.GcpConfig.bucketName}"
          else 
            s"DEVELOPMENT MODE - Writing to S3 bucket: ${config.S3Config.bucketName}"
        case "stg" | "staging" => 
          if (config.EnvironmentConfig.isGcp) 
            s"STAGING MODE - Writing to GCS bucket: ${config.GcpConfig.bucketName}"
          else 
            s"STAGING MODE - Writing to S3 bucket: ${config.S3Config.bucketName}"
        case "prod" | "production" => 
          if (config.EnvironmentConfig.isGcp) 
            s"PRODUCTION MODE - Writing to GCS bucket: ${config.GcpConfig.bucketName}"
          else 
            s"PRODUCTION MODE - Writing to S3 bucket: ${config.S3Config.bucketName}"
      }
      
      logger.info(s"Starting ${config.databaseConfig.databaseType.name.toUpperCase} CDC pipeline ($modeDescription)...")
      if (envMode != "local") {
        if (config.EnvironmentConfig.isGcp) {
          logger.info(s"☁️ Writing to GCS bucket: ${config.GcpConfig.bucketName}")
          logger.info(s"📄 File format: ${config.GcpConfig.fileFormat.toUpperCase} with ${config.GcpConfig.compressionType} compression")
        } else {
          logger.info(s"☁️ Writing to S3 bucket: ${config.S3Config.bucketName}")
          logger.info(s"📄 File format: ${config.S3Config.fileFormat.toUpperCase} with ${config.S3Config.compressionType} compression")
        }
      }
      env.execute(config.FlinkConfig.jobName)
      
    } catch {
      case ex: ConfigurationException =>
        logger.error(s"Configuration error: ${ex.getMessage}")
        System.exit(1)
      case ex: EnvironmentValidationException =>
        logger.error(s"Environment validation error: ${ex.getMessage}")
        System.exit(1)
      case ex: S3ValidationException =>
        logger.error(s"S3 configuration error: ${ex.getMessage}")
        System.exit(1)
      case ex: CDCProcessingException =>
        logger.error(s"CDC processing error: ${ex.getMessage}", ex)
        System.exit(1)
      case ex: Exception =>
        logger.error(s"Unexpected error starting CDC job: ${ex.getMessage}", ex)
        System.exit(1)
    }
  }
  

  
  /**
   * Print usage instructions
   */
  private def printUsage(): Unit = {
    logger.info("""
      |Usage:
      |  --env local      # Local testing mode (no cloud storage operations)
      |  --env dev        # Development mode (writes to dev cloud storage)
      |  --env staging    # Staging mode (writes to staging cloud storage)
      |  --env production # Production mode (writes to production cloud storage)
      |
      |  --cloud.provider aws  # Use AWS S3 (default)
      |  --cloud.provider gcp  # Use Google Cloud Storage
      |
      |  --database.type postgres  # PostgreSQL CDC (default)
      |  --database.type mysql     # MySQL CDC
      |
      |Examples:
      |  # Local testing (no cloud storage needed):
      |  flink run ... --env local --database.type postgres
      |
      |  # AWS S3 development deployment:
      |  flink run ... --env dev --cloud.provider aws --database.type postgres
      |
      |  # GCS development deployment:
      |  flink run ... --env dev --cloud.provider gcp --database.type postgres
      |
      |  # Production GCS with MySQL:
      |  flink run ... --env production --cloud.provider gcp --database.type mysql
      |""".stripMargin)
  }
  
  /**
   * Set up Flink execution environment with environment-specific settings
   */
  private def setupFlinkEnvironment(envMode: String): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    envMode match {
      case "local" =>
        // Local mode - lightweight configuration for testing
        env.setParallelism(1) // Single parallelism for local testing
        logger.info(s"🔧 Setting parallelism to: 1 (local mode)")
        
        // Minimal checkpointing for local testing
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE) // 30 seconds for local
        val checkpointConfig = env.getCheckpointConfig
        checkpointConfig.setCheckpointTimeout(60000) // 1 minute timeout
        checkpointConfig.setMinPauseBetweenCheckpoints(5000) // 5 seconds
        checkpointConfig.setMaxConcurrentCheckpoints(1)
        
        // Simple state backend for local testing
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints-local"))
        logger.info("🗄️ State backend: file:///tmp/flink-checkpoints-local")
        
        // More lenient restart strategy for local testing
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10000, TimeUnit.MILLISECONDS)))
        logger.info(" Restart strategy: 3 attempts with 10s delay (local mode)")
        
      case "dev" =>
        // Development mode - moderate configuration for dev testing
        env.setParallelism(1) // Single parallelism for dev testing
        logger.info(s"🔧 Setting parallelism to: 1 (development mode)")
        
        // Moderate checkpointing for development
        env.enableCheckpointing(45000, CheckpointingMode.EXACTLY_ONCE) // 45 seconds for dev
        val checkpointConfig = env.getCheckpointConfig
        checkpointConfig.setCheckpointTimeout(120000) // 2 minutes timeout
        checkpointConfig.setMinPauseBetweenCheckpoints(10000) // 10 seconds
        checkpointConfig.setMaxConcurrentCheckpoints(1)
        
        // Development state backend
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints-dev"))
        logger.info("🗄️ State backend: file:///tmp/flink-checkpoints-dev")
        
        // Moderate restart strategy for development
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(15000, TimeUnit.MILLISECONDS)))
        logger.info("🔄 Restart strategy: 3 attempts with 15s delay (development mode)")
        
      case "stg" =>
        // Staging mode - production-like configuration for staging testing
        env.setParallelism(2) // Use 2 for staging throughput testing
        logger.info(s"🔧 Setting parallelism to: 2 (staging mode)")
        
        // Production-like checkpointing for staging
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE) // 1 minute for staging
        val checkpointConfig = env.getCheckpointConfig
        checkpointConfig.setCheckpointTimeout(240000) // 4 minutes timeout
        checkpointConfig.setMinPauseBetweenCheckpoints(10000) // 10 seconds
        checkpointConfig.setMaxConcurrentCheckpoints(1)
        checkpointConfig.enableUnalignedCheckpoints(false)
        checkpointConfig.enableExternalizedCheckpoints(
          CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        )
        
        // Staging state backend
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints-staging"))
        logger.info("🗄️ State backend: file:///tmp/flink-checkpoints-staging")
        
        // Production-like restart strategy for staging
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.of(20000, TimeUnit.MILLISECONDS)))
        logger.info("🔄 Restart strategy: 4 attempts with 20s delay (staging mode)")
        
      case "prod" =>
        // Production mode - robust configuration for S3 writing
        env.setParallelism(2) // Use 2 for better throughput
        logger.info(s"🔧 Setting parallelism to: 2 (production mode)")
        
        // Configure checkpointing for production
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE) // 1 minute for production
        val checkpointConfig = env.getCheckpointConfig
        checkpointConfig.setCheckpointTimeout(300000) // 5 minutes timeout
        checkpointConfig.setMinPauseBetweenCheckpoints(10000) // 10 seconds
        checkpointConfig.setMaxConcurrentCheckpoints(1)
        checkpointConfig.enableUnalignedCheckpoints(false)
        checkpointConfig.enableExternalizedCheckpoints(
          CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        )
        
        // Set state backend for production
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints-production"))
        logger.info("🗄️ State backend: file:///tmp/flink-checkpoints-production")
        
        // Configure restart strategy for production
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(30000, TimeUnit.MILLISECONDS)))
        logger.info("🔄 Restart strategy: 5 attempts with 30s delay (production mode)")
    }
    
    // Set time characteristic (common for all modes)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    
    env
  }
  
  /**
   * Process multi-table CDC stream - UNIFIED PIPELINE for LOCAL and CLOUD modes
   * Intelligently routes to S3 or GCS based on cloud provider configuration
   */
  private def processMultiTableCDC(env: StreamExecutionEnvironment, cdcStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig, envMode: String): Unit = {
    
    // Extract serializable configuration values based on cloud provider
    val (bucketName, basePath) = if (config.EnvironmentConfig.isGcp) {
      (config.GcpConfig.bucketName, config.GcpConfig.basePath)
    } else {
      (config.S3Config.bucketName, config.S3Config.basePath)
    }
    val tableArray = config.databaseConfig.getTableArray
    val cloudProvider = config.EnvironmentConfig.cloudProvider
    
    // Define side output tags for different tables and errors
    val errorTag = new OutputTag[String]("error-records") {}
    val schemaChangeTag = new OutputTag[String]("schema-changes") {}
    
    logger.info("🏗️ Setting up CDC event processor...")
    
    // Process and route CDC events (same for both modes)
    val processedStream = cdcStream
      .process(new CDCEventProcessor(errorTag, schemaChangeTag))
      .name("cdc-event-processor")
      .uid("cdc-event-processor")
    
    // Handle error records (same processing, different output)
    val errorStream = processedStream.getSideOutput(errorTag)
    handleErrorRecords(errorStream, config, envMode)
    
    // Handle schema changes (same processing, different output)
    val schemaChangeStream = processedStream.getSideOutput(schemaChangeTag)
    handleSchemaChanges(schemaChangeStream, config, envMode)
    
    // Process each table individually (same pipeline, different final sink)
    logger.info(s"📋 Processing tables: ${tableArray.mkString(", ")}")
    
    tableArray.foreach { tableWithSchema =>
      val tableName = extractTableName(tableWithSchema)
      logger.info(s"🔧 Setting up pipeline for table: $tableName")
      
      // Filter events for this specific table (same for both modes)
      val tableStream = processedStream
        .filter(new TableFilter(tableWithSchema, tableName))
        .name(s"$tableName-filter")
        .uid(s"$tableName-filter")
      
      // Add monitoring for this table (same for both modes)
      val monitoredStream = tableStream
        .map(new CDCMonitor(tableName))
        .name(s"$tableName-monitor")
        .uid(s"$tableName-monitor")
      
      // Environment-specific final sink with cloud provider intelligence
      envMode match {
        case "local" =>
          // LOCAL MODE: Just print, no cloud storage writing
          monitoredStream
            .map(new LocalSimulationMapper(tableName))
            .name(s"$tableName-local-simulation")
            .print(s"LOCAL-$tableName")
            
        case "dev" | "development" | "stg" | "staging" | "prod" | "production" =>
          // CLOUD-ENABLED MODES: Write to S3 or GCS based on cloud provider
          if (config.EnvironmentConfig.isGcp) {
            // GCP MODE: Write to Google Cloud Storage
            monitoredStream
              .map(new GCSLoggingMapper(tableName, bucketName, basePath, envMode))
              .name(s"$tableName-gcs-logger")
            
            // Use GCSSink's enhanced CDC sink method
            GCSSink.createEnhancedCDCSink(monitoredStream, tableName, config)
            
            // Also print for monitoring
            monitoredStream
              .map(new GCSMonitoringMapper(tableName, envMode))
              .name(s"$tableName-gcs-monitor-print")
              .print(s"GCS-${envMode.toUpperCase}-$tableName")
              
          } else {
            // AWS MODE: Write to S3 (default/backward compatibility)
            monitoredStream
              .map(new S3LoggingMapper(tableName, bucketName, basePath, envMode))
              .name(s"$tableName-s3-logger")
            
            // Use S3Sink's enhanced CDC sink method
            S3Sink.createEnhancedCDCSink(monitoredStream, tableName, config)
            
            // Also print for monitoring
            monitoredStream
              .map(new S3MonitoringMapper(tableName, envMode))
              .name(s"$tableName-s3-monitor-print")
              .print(s"S3-${envMode.toUpperCase}-$tableName")
          }
      }
      
      logger.info(s"✅ Configured ${envMode.toUpperCase} pipeline for table: $tableName")
    }
    
    // Monitor all processed events (same for both modes)
    processedStream
      .map(new AllEventsMonitor(envMode))
      .name("debug-all-events")
      .print(s"ALL-${envMode.toUpperCase}-EVENTS")
  }
  
  /**
   * Handle error records - UNIFIED for all modes with cloud provider intelligence
   */
  private def handleErrorRecords(errorStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig, envMode: String): Unit = {
    val errorProcessedStream = errorStream
      .map(new ErrorHandler(envMode))
      .name("error-processor")
    
    envMode match {
      case "local" =>
        // LOCAL MODE: Just print errors
        errorProcessedStream.print("LOCAL-ERROR")
        
      case "dev" | "development" | "stg" | "staging" | "prod" | "production" =>
        // CLOUD-ENABLED MODES: Write errors to cloud storage
        if (config.EnvironmentConfig.isGcp) {
          GCSSink.createEnhancedCDCSink(errorProcessedStream, "errors", config)
          errorProcessedStream.print(s"GCS-${envMode.toUpperCase}-ERROR")
        } else {
          S3Sink.createEnhancedCDCSink(errorProcessedStream, "errors", config)
          errorProcessedStream.print(s"S3-${envMode.toUpperCase}-ERROR")
        }
    }
  }
  
  /**
   * Handle schema change events - UNIFIED for all modes with cloud provider intelligence
   */
  private def handleSchemaChanges(schemaChangeStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig, envMode: String): Unit = {
    val schemaProcessedStream = schemaChangeStream
      .map(new SchemaChangeHandler(envMode))
      .name("schema-change-processor")
    
    envMode match {
      case "local" =>
        // LOCAL MODE: Just print schema changes
        schemaProcessedStream.print("LOCAL-SCHEMA_CHANGE")
        
      case "dev" | "development" | "stg" | "staging" | "prod" | "production" =>
        // CLOUD-ENABLED MODES: Write schema changes to cloud storage
        if (config.EnvironmentConfig.isGcp) {
          GCSSink.createEnhancedCDCSink(schemaProcessedStream, "schema-changes", config)
          schemaProcessedStream.print(s"GCS-${envMode.toUpperCase}-SCHEMA_CHANGE")
        } else {
          S3Sink.createEnhancedCDCSink(schemaProcessedStream, "schema-changes", config)
          schemaProcessedStream.print(s"S3-${envMode.toUpperCase}-SCHEMA_CHANGE")
        }
    }
  }
  
  /**
   * Extract table name from schema.table format
   */
  private def extractTableName(tableWithSchema: String): String = {
    if (tableWithSchema.contains(".")) {
      tableWithSchema.split("\\.").last
    } else {
      tableWithSchema
    }
  }
  
  /**
   * Print application banner
   */
  private def printBanner(envMode: String): Unit = {
    val modeDescription = envMode match {
      case "local" => "LOCAL TESTING MODE - No cloud storage operations"
      case "dev" | "development" => "DEVELOPMENT MODE - Real cloud storage writing to dev environment"
      case "stg" | "staging" => "STAGING MODE - Real cloud storage writing to staging environment"  
      case "prod" | "production" => "PRODUCTION MODE - Real cloud storage writing to production environment"
      case _ => "UNKNOWN MODE"
    }
    
    val modeIcon = envMode match {
      case "local" => "🧪"
      case "dev" | "development" => "🔧"
      case "stg" | "staging" => "🎭"
      case "prod" | "production" => "🚀"
      case _ => "❓"
    }
    
    val warningMessage = envMode match {
      case "local" => "✅ Safe Mode: No files will be created in cloud storage!"
      case "dev" | "development" => "⚠️  Development: Files will be created in DEV cloud storage!"
      case "stg" | "staging" => "⚠️  Staging: Files will be created in STAGING cloud storage!"
      case "prod" | "production" => "⚠️  Production: Files will be created in PRODUCTION cloud storage!"
      case _ => "⚠️  Unknown mode!"
    }
    
    logger.info(
      s"""
        |  ____        _        _                     ____ ____   ____   _____ _____ 
        | |  _ \\  __ _| |_ __ _| |__   __ _ ___  ___  / ___|  _ \\ / ___| |___ /|___ / 
        | | | | |/ _` | __/ _` | '_ \\ / _` / __|/ _ \\ \\___ \\| |_) | |       |_ \\  |_ \\ 
        | | |_| | (_| | || (_| | |_) | (_| \\__ \\  __/  ___) |  __/| |___   ___) |___) |
        | |____/ \\__,_|\\__\\__,_|_.__/ \\__,_|___/\\___| |____/|_|    \\____| |____/|____/ 
        |                                                                              
        |
        | Database CDC to Multi-Cloud Pipeline - $modeDescription
        | Version: 1.2.0
        | $modeIcon Environment: ${envMode.toUpperCase} MODE
        | 🎯 Current Database: PostgreSQL & MySQL Support
        | ☁️ Cloud Provider: AWS S3 & Google Cloud Storage
        | 📄 Default Format: Avro with comprehensive CDC schema & Snappy compression
        | $warningMessage
        |""".stripMargin)
  }
} 