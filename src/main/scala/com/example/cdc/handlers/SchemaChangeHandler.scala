package com.example.cdc.handlers

import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.{Logger, LoggerFactory}

/**
 * UNIFIED Schema change handler
 */
class SchemaChangeHandler(envMode: String) extends MapFunction[String, String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  
  override def map(value: String): String = {
    val timestamp = System.currentTimeMillis()
    val enhancedRecord = s"""{"timestamp": $timestamp, "change_type": "schema_change", "details": $value, "processing_host": "${java.net.InetAddress.getLocalHost.getHostName}", "mode": "$envMode"}"""
    
    // Log schema change
    val modePrefix = if (envMode == "local") "🧪 LOCAL MODE -" else "🔄"
    logger.info(s"$modePrefix Schema Change Detected at $timestamp: ${value.take(200)}...")
    if (envMode == "local") {
      logger.info(s"📋 LOCAL MODE - Would write schema change to S3 (simulated)")
    } else {
      logger.info(s"💾 Writing schema change to S3...")
    }
    
    enhancedRecord
  }
} 