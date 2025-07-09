package com.example.cdc

import com.example.cdc.config.AppConfig
import com.example.cdc.sink.S3Sink
import com.example.cdc.monitoring.CDCMonitor
import com.example.cdc.transformation.CDCEventProcessor
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.util.OutputTag
import org.apache.flink.api.java.utils.ParameterTool

import java.util.concurrent.TimeUnit

/**
 * Production CDC to S3 Job - ACTUAL S3 WRITING
 * 
 * Features:
 * - PostgreSQL CDC processing (architecture ready for MySQL/Oracle)
 * - Multi-table CDC processing
 * - Comprehensive error handling
 * - Monitoring and metrics
 * - PRODUCTION S3: Actually writes to S3 buckets with Avro format by default
 */
object ProductionCdcJob extends AppConfig {
  
  private val objectMapper = new ObjectMapper()
  
  def main(args: Array[String]): Unit = {
    try {
      // Parse command line arguments and merge with system properties
      val argsParams = ParameterTool.fromArgs(args)
      val mergedParams = ParameterTool.fromSystemProperties().mergeWith(argsParams)
      
      // Create a new AppConfig instance with the merged parameters
      val config = new AppConfig {
        override lazy val params: ParameterTool = mergedParams
      }
      
      // Print banner
      printBanner()
      
      // Print parsed command line arguments for debugging
      println("📝 Command line arguments received:")
      args.zipWithIndex.foreach { case (arg, i) => println(s"  [$i]: $arg") }
      
      println("\n🔧 Configuration loaded:")
      println(s"  Hostname: ${config.PostgresConfig.hostname}")
      println(s"  Database: ${config.PostgresConfig.database}")
      println(s"  Username: ${config.PostgresConfig.username}")
      println(s"  Tables: ${config.PostgresConfig.tableList}")
      println(s"  Slot: ${config.PostgresConfig.slotName}")
      
      // S3 Configuration
      if (config.S3Config.bucketName.nonEmpty) {
        println(s"  S3 Bucket: ${config.S3Config.bucketName}")
        println(s"  S3 Base Path: ${config.S3Config.basePath}")
        println(s"  AWS Region: ${config.S3Config.region}")
        println(s"  File Format: ${config.S3Config.fileFormat}")
        println(s"  Compression: ${config.S3Config.compressionType}")
        println(s"  Max File Size: ${config.S3Config.maxFileSize}")
        println(s"  Rollover Interval: ${config.S3Config.rolloverInterval}")
      } else {
        println("❌ S3 bucket not configured! Please provide --s3-bucket parameter")
        System.exit(1)
      }
      
      config.validateConfiguration()
      config.printConfigurationSummary()
      
      // Set up Flink execution environment
      val env = setupFlinkEnvironment()
      
      // Create CDC source using the config with merged parameters
      println("🔧 Creating database CDC source...")
      val cdcSource = config.buildCDCSource()
      val cdcStream = env.addSource(cdcSource)
        .name("database-cdc-source")
        .uid("database-cdc-source")
      
      // Add raw CDC stream logging
      cdcStream
        .map(event => {
          println(s"📥 RAW CDC Event received: ${event.take(200)}...")
          event
        })
        .name("raw-cdc-logger")
      
      // Process multi-table CDC stream with S3 writing
      processMultiTableCDCWithS3(env, cdcStream, config)
      
      // Start the job
      println("🚀 Starting database CDC to S3 pipeline (PRODUCTION MODE)...")
      println(s"💾 Writing to S3 bucket: ${config.S3Config.bucketName}")
      println(s"📄 File format: ${config.S3Config.fileFormat.toUpperCase} with ${config.S3Config.compressionType} compression")
      env.execute(config.FlinkConfig.jobName)
      
    } catch {
      case ex: Exception =>
        System.err.println(s"❌ Failed to start CDC job: ${ex.getMessage}")
        ex.printStackTrace()
        System.exit(1)
    }
  }
  
  /**
   * Set up Flink execution environment with production settings
   */
  private def setupFlinkEnvironment(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    // Set parallelism (can be higher for production)
    env.setParallelism(2) // Use 2 for better throughput
    println(s"🔧 Setting parallelism to: 2 (production mode)")
    
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
    println("🗄️ State backend: file:///tmp/flink-checkpoints-production")
    
    // Configure restart strategy for production
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(30000, TimeUnit.MILLISECONDS)))
    println("🔄 Restart strategy: 5 attempts with 30s delay")
    
    // Set time characteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    
    env
  }
  
  /**
   * Process multi-table CDC stream with S3 writing - PRODUCTION MODE
   */
  private def processMultiTableCDCWithS3(env: StreamExecutionEnvironment, cdcStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig): Unit = {
    
    // Extract serializable configuration values
    val bucketName = config.S3Config.bucketName
    val basePath = config.S3Config.basePath
    val tableArray = config.PostgresConfig.getTableArray
    
    // Define side output tags for different tables and errors
    val errorTag = new OutputTag[String]("error-records") {}
    val schemaChangeTag = new OutputTag[String]("schema-changes") {}
    
    println("🏗️ Setting up CDC event processor...")
    
    // Process and route CDC events
    val processedStream = cdcStream
      .process(new CDCEventProcessor(errorTag, schemaChangeTag))
      .name("cdc-event-processor")
      .uid("cdc-event-processor")
    
    // Handle error records
    val errorStream = processedStream.getSideOutput(errorTag)
    handleErrorRecordsWithS3(errorStream, config)
    
    // Handle schema changes
    val schemaChangeStream = processedStream.getSideOutput(schemaChangeTag)
    handleSchemaChangesWithS3(schemaChangeStream, config)
    
    // Process each table individually with S3 sink
    println(s"📋 Processing tables: ${tableArray.mkString(", ")}")
    
    tableArray.foreach { tableWithSchema =>
      val tableName = extractTableName(tableWithSchema)
      println(s"🔧 Setting up S3 pipeline for table: $tableName")
      
      // Filter events for this specific table
      val tableStream = processedStream
        .filter(new TableFilter(tableWithSchema, tableName))
        .name(s"$tableName-filter")
        .uid(s"$tableName-filter")
      
      // Add monitoring for this table
      val monitoredStream = tableStream
        .map(new CDCMonitor(tableName))
        .name(s"$tableName-monitor")
        .uid(s"$tableName-monitor")
      
      // Write to S3 using the enhanced CDC sink
      monitoredStream
        .map(new S3LoggingMapper(tableName, bucketName, basePath))
        .name(s"$tableName-s3-logger")
      
      // Use S3Sink's enhanced CDC sink method
      S3Sink.createEnhancedCDCSink(monitoredStream, tableName, config)
      
      // Also print for monitoring (can be removed in pure production)
      monitoredStream
        .map(new S3MonitoringMapper(tableName))
        .name(s"$tableName-monitor-print")
        .print(s"S3-$tableName")
      
      println(s"✅ Configured S3 writing pipeline for table: $tableName")
    }
    
    // Monitor all processed events
    processedStream
      .map(new AllEventsMonitor())
      .name("debug-all-events")
      .print("ALL-S3-EVENTS")
  }
  
  /**
   * Handle error records by writing to S3 error bucket
   */
  private def handleErrorRecordsWithS3(errorStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig): Unit = {
    val errorProcessedStream = errorStream
      .map(new S3ErrorHandler())
      .name("error-processor")
    
    // Use S3Sink's CDC sink method for errors
    S3Sink.createEnhancedCDCSink(errorProcessedStream, "errors", config)
    
    // Also print errors for monitoring
    errorProcessedStream.print("S3-ERROR")
  }
  
  /**
   * Handle schema change events by writing to S3
   */
  private def handleSchemaChangesWithS3(schemaChangeStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig): Unit = {
    val schemaProcessedStream = schemaChangeStream
      .map(new S3SchemaChangeHandler())
      .name("schema-change-processor")
    
    // Use S3Sink's CDC sink method for schema changes
    S3Sink.createEnhancedCDCSink(schemaProcessedStream, "schema-changes", config)
    
    // Also print schema changes for monitoring
    schemaProcessedStream.print("S3-SCHEMA_CHANGE")
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
  private def printBanner(): Unit = {
    println(
      """
        |  ____        _        _                     ____ ____   ____   _____ _____ 
        | |  _ \  __ _| |_ __ _| |__   __ _ ___  ___  / ___|  _ \ / ___| |___ /|___ / 
        | | | | |/ _` | __/ _` | '_ \ / _` / __|/ _ \ \___ \| |_) | |       |_ \  |_ \ 
        | | |_| | (_| | || (_| | |_) | (_| \__ \  __/  ___) |  __/| |___   ___) |___) |
        | |____/ \__,_|\__\__,_|_.__/ \__,_|___/\___| |____/|_|    \____| |____/|____/ 
        |                                                                              
        |
        | Database CDC to S3 Production Pipeline - PRODUCTION MODE
        | Version: 1.2.0
        | 🎯 Current Database: PostgreSQL (MySQL/Oracle architecture ready)
        | 💾 Production Mode: Writing to S3 buckets in Avro format (default)
        | 📄 Default Format: Avro with comprehensive CDC schema & Snappy compression
        | ⚠️  Warning: This will create files in S3!
        |""".stripMargin)
  }
}

/**
 * Error handler for CDC events - Enhanced for S3 writing
 */
class S3ErrorHandler extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(value: String): String = {
    val timestamp = System.currentTimeMillis()
    val errorRecord = s"""{"timestamp": $timestamp, "error_type": "processing_error", "original_record": ${if (value.length > 1000) s""""${value.take(1000)}..."""" else s""""$value""""}, "processing_host": "${java.net.InetAddress.getLocalHost.getHostName}"}"""
    
    // Log the error
    println(s"❌ CDC Processing Error at $timestamp: ${value.take(200)}...")
    println(s"💾 Writing error to S3...")
    
    errorRecord
  }
}

/**
 * Schema change handler - Enhanced for S3 writing
 */
class S3SchemaChangeHandler extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(value: String): String = {
    val timestamp = System.currentTimeMillis()
    val enhancedRecord = s"""{"timestamp": $timestamp, "change_type": "schema_change", "details": $value, "processing_host": "${java.net.InetAddress.getLocalHost.getHostName}"}"""
    
    // Log schema change
    println(s"🔄 Schema Change Detected at $timestamp: ${value.take(200)}...")
    println(s"💾 Writing schema change to S3...")
    
    enhancedRecord
  }
}

/**
 * Serializable table filter for CDC events
 */
class TableFilter(tableWithSchema: String, tableName: String) extends org.apache.flink.api.common.functions.FilterFunction[String] {
  @transient private lazy val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
  
  override def filter(event: String): Boolean = {
    try {
      val jsonNode = objectMapper.readTree(event)
      val source = jsonNode.get("source")
      if (source != null && source.has("table")) {
        val eventTable = source.get("table").asText()
        val eventSchema = source.get("schema").asText()
        val matches = s"$eventSchema.$eventTable" == tableWithSchema
        if (matches) {
          println(s"✅ Event matched table $tableName: ${event.take(100)}...")
        }
        matches
      } else {
        false
      }
    } catch {
      case _: Exception => false
    }
  }
}

/**
 * Serializable S3 logging mapper
 */
class S3LoggingMapper(tableName: String, bucketName: String, basePath: String) extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(event: String): String = {
    println(s"📤 [$tableName] WRITING to S3: s3://$bucketName/$basePath/$tableName/")
    println(s"📊 [$tableName] Event size: ${event.length} bytes")
    event
  }
}

/**
 * Serializable S3 monitoring mapper
 */
class S3MonitoringMapper(tableName: String) extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(event: String): String = {
    val shortEvent = if (event.length > 200) event.take(200) + "..." else event
    s"[$tableName] S3_WRITTEN: $shortEvent"
  }
}

/**
 * Serializable all events monitor
 */
class AllEventsMonitor extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(event: String): String = {
    val shortEvent = if (event.length > 100) event.take(100) + "..." else event
    println(s"🔍 ALL PROCESSED: $shortEvent")
    s"PROCESSED: $shortEvent"
  }
} 