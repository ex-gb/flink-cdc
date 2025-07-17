package com.example.cdc.handlers

import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.{Logger, LoggerFactory}

/**
 * UNIFIED Error handler for CDC events 
 */
class ErrorHandler(envMode: String) extends MapFunction[String, String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  
  override def map(value: String): String = {
    val timestamp = System.currentTimeMillis()
    val errorRecord = s"""{"timestamp": $timestamp, "error_type": "processing_error", "original_record": ${if (value.length > 1000) s""""${value.take(1000)}..."""" else s""""$value""""}, "processing_host": "${java.net.InetAddress.getLocalHost.getHostName}", "mode": "$envMode"}"""
    
    // Log the error
    val modePrefix = if (envMode == "local") "🧪 LOCAL MODE -" else "💾"
    logger.error(s"❌ $modePrefix CDC Processing Error at $timestamp: ${value.take(200)}...")
    if (envMode == "local") {
      logger.info(s"📋 LOCAL MODE - Would write error to S3 (simulated)")
    } else {
      logger.info(s"💾 Writing error to S3...")
    }
    
    errorRecord
  }
} 