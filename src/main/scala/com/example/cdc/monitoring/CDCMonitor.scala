package com.example.cdc.monitoring

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction

/**
 * CDC Monitor for tracking metrics and latency
 */
class CDCMonitor(tableName: String) extends MapFunction[String, String] {
  
  private val objectMapper = new ObjectMapper()
  
  override def map(value: String): String = {
    try {
      val jsonNode: JsonNode = objectMapper.readTree(value)
      
      // Extract metrics
      val currentTime = System.currentTimeMillis()
      
      // Calculate latency if source timestamp is available
      if (jsonNode.has("source") && jsonNode.get("source").has("ts_ms")) {
        val sourceTimestamp = jsonNode.get("source").get("ts_ms").asLong()
        val latencyMs = currentTime - sourceTimestamp
        
        // Log metrics (in production, send to monitoring system)
        if (latencyMs > 10000) { // Alert if latency > 10 seconds
          println(s"HIGH LATENCY ALERT: Table $tableName, Latency: ${latencyMs}ms")
        }
        
        // Track operation type
        val operation = jsonNode.get("op").asText()
        trackOperationMetrics(operation, latencyMs)
      }
      
      value
      
    } catch {
      case ex: Exception =>
        System.err.println(s"Error in CDC monitor for table $tableName: ${ex.getMessage}")
        value
    }
  }
  
  private def trackOperationMetrics(operation: String, latencyMs: Long): Unit = {
    // TODO: Implement proper metrics collection
    // For now, just log
    println(s"CDC_METRIC table=$tableName operation=$operation latency=${latencyMs}ms")
  }
}

/**
 * Metrics collector for aggregating CDC statistics
 */
object MetricsCollector {
  
  private var recordCount = 0L
  private var totalLatency = 0L
  
  def recordEvent(latencyMs: Long): Unit = {
    recordCount += 1
    totalLatency += latencyMs
  }
  
  def getAverageLatency: Double = {
    if (recordCount > 0) totalLatency.toDouble / recordCount else 0.0
  }
  
  def getRecordCount: Long = recordCount
  
  def reset(): Unit = {
    recordCount = 0
    totalLatency = 0
  }
} 