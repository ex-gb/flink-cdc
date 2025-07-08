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
 * Production-ready PostgreSQL CDC to S3 Job - LOCAL TEST MODE
 * 
 * Features:
 * - Multi-table CDC processing
 * - Comprehensive error handling
 * - Monitoring and metrics
 * - LOCAL TESTING: Print messages instead of writing to S3
 */
object ProductionPostgresCdcJob extends AppConfig {
  
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
      
      config.validateConfiguration()
      config.printConfigurationSummary()
      
      // Set up Flink execution environment
      val env = setupFlinkEnvironment()
      
      // Create CDC source using the config with merged parameters
      println("🔧 Creating PostgreSQL CDC source...")
      val cdcSource = config.buildCDCSource()
      val cdcStream = env.addSource(cdcSource)
        .name("postgresql-cdc-source")
        .uid("postgresql-cdc-source")
      
      // Add raw CDC stream logging
      cdcStream
        .map(event => {
          println(s"📥 RAW CDC Event received: ${event.take(200)}...")
          event
        })
        .name("raw-cdc-logger")
      
      // Process multi-table CDC stream
      processMultiTableCDC(env, cdcStream, config)
      
      // Start the job
      println("🚀 Starting PostgreSQL CDC to S3 pipeline (LOCAL TEST MODE)...")
      println("💡 Note: This will print messages instead of writing to S3")
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
    
    // Set parallelism
    env.setParallelism(1) // Use 1 for local testing
    println(s"🔧 Setting parallelism to: 1 (local test mode)")
    
    // Configure checkpointing
    env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE) // 30 seconds for testing
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointTimeout(60000) // 1 minute timeout for testing
    checkpointConfig.setMinPauseBetweenCheckpoints(5000) // 5 seconds
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    checkpointConfig.enableUnalignedCheckpoints(false)
    checkpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    
    // Set state backend
    env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints-test"))
    println("🗄️ State backend: file:///tmp/flink-checkpoints-test")
    
    // Configure restart strategy
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5000, TimeUnit.MILLISECONDS)))
    println("🔄 Restart strategy: 3 attempts with 5s delay")
    
    // Set time characteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    
    env
  }
  
  /**
   * Process multi-table CDC stream with error handling and monitoring - LOCAL TEST MODE
   */
  private def processMultiTableCDC(env: StreamExecutionEnvironment, cdcStream: org.apache.flink.streaming.api.datastream.DataStream[String], config: AppConfig): Unit = {
    
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
    handleErrorRecords(errorStream)
    
    // Handle schema changes
    val schemaChangeStream = processedStream.getSideOutput(schemaChangeTag)
    handleSchemaChanges(schemaChangeStream)
    
    // Process each table individually
    val tableArray = config.PostgresConfig.getTableArray
    println(s"📋 Processing tables: ${tableArray.mkString(", ")}")
    
    tableArray.foreach { tableWithSchema =>
      val tableName = extractTableName(tableWithSchema)
      println(s"🔧 Setting up pipeline for table: $tableName")
      
      // Filter events for this specific table
      val tableStream = processedStream
        .filter(event => {
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
        })
        .name(s"$tableName-filter")
        .uid(s"$tableName-filter")
      
      // Add monitoring for this table
      val monitoredStream = tableStream
        .map(new CDCMonitor(tableName))
        .name(s"$tableName-monitor")
        .uid(s"$tableName-monitor")
      
      // LOCAL TEST: Print instead of writing to S3
      monitoredStream
        .map(event => {
          println(s"📤 [$tableName] PROCESSED CDC Event: $event")
          println(s"📊 [$tableName] Would write to S3 at: s3://bucket/$tableName/...")
          event
        })
        .name(s"$tableName-local-print")
        .uid(s"$tableName-local-print")
        .print(s"CDC-$tableName")
      
      println(s"✅ Configured processing pipeline for table: $tableName")
    }
    
    // Also print all processed events for debugging
    processedStream
      .map(event => {
        println(s"🔍 ALL PROCESSED: $event")
        event
      })
      .name("debug-all-events")
      .print("ALL-EVENTS")
  }
  
  /**
   * Handle error records by logging and optionally sending to dead letter queue
   */
  private def handleErrorRecords(errorStream: org.apache.flink.streaming.api.datastream.DataStream[String]): Unit = {
    errorStream
      .map(new ErrorHandler())
      .name("error-handler")
      .uid("error-handler")
      .print("ERROR")
  }
  
  /**
   * Handle schema change events
   */
  private def handleSchemaChanges(schemaChangeStream: org.apache.flink.streaming.api.datastream.DataStream[String]): Unit = {
    schemaChangeStream
      .map(new SchemaChangeHandler())
      .name("schema-change-handler")
      .uid("schema-change-handler")
      .print("SCHEMA_CHANGE")
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
   * Parse command line arguments
   */
  private def parseArguments(args: Array[String]): Map[String, String] = {
    val parsed = args.grouped(2).collect {
      case Array(key, value) if key.startsWith("--") => 
        key.substring(2).replace("-", ".") -> value
    }.toMap
    
    println(s"📝 Parsed arguments: $parsed")
    parsed
  }
  
  /**
   * Print application banner
   */
  private def printBanner(): Unit = {
    println(
      """
        |  ____           _                    ____  _____ _          ____ ____   ____ 
        | |  _ \ ___  ___| |_ __ _ _ __ ___   / ___||  ___| |        / ___|  _ \ / ___|
        | | |_) / _ \/ __| __/ _` | '__/ _ \  \___ \| |_  | |  _____ \___ \| |_) | |    
        | |  __/ (_) \__ \ || (_| | | |  __/   ___) |  _| | |_|_____|___) |  __/| |___ 
        | |_|   \___/|___/\__\__, |_|  \___|  |____/|_|   |_(_)     |____/|_|    \____|
        |                    |___/                                                      
        |
        | PostgreSQL CDC to S3 Production Pipeline - LOCAL TEST MODE
        | Version: 1.0.0
        | 🧪 Testing Mode: Will print messages instead of writing to S3
        |""".stripMargin)
  }
}

/**
 * Error handler for CDC events
 */
class ErrorHandler extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(value: String): String = {
    val timestamp = System.currentTimeMillis()
    val errorRecord = s"""{"timestamp": $timestamp, "error_type": "processing_error", "original_record": "$value"}"""
    
    // Log the error
    println(s"❌ CDC Processing Error at $timestamp: $value")
    
    errorRecord
  }
}

/**
 * Schema change handler
 */
class SchemaChangeHandler extends org.apache.flink.api.common.functions.MapFunction[String, String] {
  override def map(value: String): String = {
    val timestamp = System.currentTimeMillis()
    
    // Log schema change
    println(s"🔄 Schema Change Detected at $timestamp: $value")
    
    value
  }
} 