package com.example.cdc.mappers

import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.{Logger, LoggerFactory}

/**
 * Serializable S3 logging mapper (S3-enabled modes)
 */
class S3LoggingMapper(tableName: String, bucketName: String, basePath: String, envMode: String) extends MapFunction[String, String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  
  override def map(event: String): String = {
    logger.info(s"📤 [$tableName] ${envMode.toUpperCase} - WRITING to S3: s3://$bucketName/$basePath/$tableName/")
    logger.info(s"📊 [$tableName] Event size: ${event.length} bytes")
    event
  }
}

/**
 * Serializable S3 monitoring mapper (S3-enabled modes)
 */
class S3MonitoringMapper(tableName: String, envMode: String) extends MapFunction[String, String] {
  override def map(event: String): String = {
    val shortEvent = if (event.length > 200) event.take(200) + "..." else event
    s"[$tableName] ${envMode.toUpperCase}_S3_WRITTEN: $shortEvent"
  }
}

/**
 * Serializable LOCAL simulation mapper (LOCAL mode only)
 */
class LocalSimulationMapper(tableName: String) extends MapFunction[String, String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  
  override def map(event: String): String = {
    logger.info(s"🧪 [$tableName] LOCAL MODE: Would write to S3 (simulated)")
    logger.info(s"🎯 [$tableName] Target: s3://local-simulation/cdc-events/$tableName/")
    val shortEvent = if (event.length > 200) event.take(200) + "..." else event
    s"[$tableName] LOCAL_SIMULATED: $shortEvent"
  }
}

/**
 * Serializable GCS logging mapper (GCS-enabled modes)
 */
class GCSLoggingMapper(tableName: String, bucketName: String, basePath: String, envMode: String) extends MapFunction[String, String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  
  override def map(event: String): String = {
    logger.info(s"🌥️ [$tableName] ${envMode.toUpperCase} - WRITING to GCS: gs://$bucketName/$basePath/$tableName/")
    logger.info(s"📊 [$tableName] Event size: ${event.length} bytes")
    event
  }
}

/**
 * Serializable GCS monitoring mapper (GCS-enabled modes)
 */
class GCSMonitoringMapper(tableName: String, envMode: String) extends MapFunction[String, String] {
  override def map(event: String): String = {
    val shortEvent = if (event.length > 200) event.take(200) + "..." else event
    s"[$tableName] ${envMode.toUpperCase}_GCS_WRITTEN: $shortEvent"
  }
}

/**
 * Serializable all events monitor - UNIFIED for all modes
 */
class AllEventsMonitor(envMode: String) extends MapFunction[String, String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  
  override def map(event: String): String = {
    val shortEvent = if (event.length > 100) event.take(100) + "..." else event
    val modePrefix = if (envMode == "local") "🧪 ALL LOCAL PROCESSED:" else "🔍 ALL PROCESSED:"
    logger.info(s"$modePrefix $shortEvent")
    s"PROCESSED: $shortEvent"
  }
} 