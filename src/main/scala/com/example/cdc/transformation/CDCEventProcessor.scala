package com.example.cdc.transformation

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

/**
 * CDC Event Processor handles:
 * - Event validation and parsing
 * - Schema change detection
 * - Error handling and routing
 * - Data transformation and enrichment
 */
class CDCEventProcessor(errorTag: OutputTag[String], schemaChangeTag: OutputTag[String]) 
  extends ProcessFunction[String, String] {
  
  private val objectMapper = new ObjectMapper()
  
  override def processElement(
    value: String,
    ctx: ProcessFunction[String, String]#Context,
    out: Collector[String]): Unit = {
    
    try {
      // Parse JSON
      val jsonNode: JsonNode = objectMapper.readTree(value)
      
      // Validate CDC event structure
      if (!isValidCDCEvent(jsonNode)) {
        ctx.output(errorTag, createErrorRecord(value, "Invalid CDC event structure"))
        return
      }
      
      // Check for schema change events
      if (isSchemaChangeEvent(jsonNode)) {
        ctx.output(schemaChangeTag, value)
        return
      }
      
      // Process regular CDC event
      val processedEvent = processRegularCDCEvent(value)
      out.collect(processedEvent)
      
    } catch {
      case ex: Exception =>
        ctx.output(errorTag, createErrorRecord(value, s"Processing error: ${ex.getMessage}"))
    }
  }
  
  /**
   * Validate CDC event structure
   */
  private def isValidCDCEvent(jsonNode: JsonNode): Boolean = {
    jsonNode.has("source") && 
    jsonNode.has("op") && 
    jsonNode.get("source").has("table") && 
    jsonNode.get("source").has("schema")
  }
  
  /**
   * Check if event is a schema change
   */
  private def isSchemaChangeEvent(jsonNode: JsonNode): Boolean = {
    jsonNode.has("historyRecord") || 
    (jsonNode.has("op") && jsonNode.get("op").asText() == "r" && 
     jsonNode.has("source") && jsonNode.get("source").has("snapshot") && 
     jsonNode.get("source").get("snapshot").asText() == "false")
  }
  
  /**
   * Process regular CDC events with enhanced validation and transformation
   */
  private def processRegularCDCEvent(originalValue: String): String = {
    try {
      val jsonNode = objectMapper.readTree(originalValue)
      
      // Add processing timestamp
      val enrichedNode = jsonNode.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      enrichedNode.put("processing_timestamp", System.currentTimeMillis())
      enrichedNode.put("pipeline_version", "1.2.0")
      
      // Add data quality scores and validation
      if (jsonNode.has("after") && !jsonNode.get("after").isNull) {
        val dataQualityScore = calculateDataQualityScore(jsonNode.get("after"))
        enrichedNode.put("data_quality_score", dataQualityScore)
      }
      
      // Return enriched event
      objectMapper.writeValueAsString(enrichedNode)
      
    } catch {
      case _: Exception =>
        // Return original value if enrichment fails
        originalValue
    }
  }
  
  /**
   * Calculate data quality score based on field completeness
   */
  private def calculateDataQualityScore(dataNode: com.fasterxml.jackson.databind.JsonNode): Double = {
    if (dataNode == null || dataNode.isNull) return 0.0
    
    val fields = dataNode.fields()
    var totalFields = 0
    var nonNullFields = 0
    
    while (fields.hasNext) {
      val field = fields.next()
      totalFields += 1
      if (!field.getValue.isNull && field.getValue.asText().nonEmpty) {
        nonNullFields += 1
      }
    }
    
    if (totalFields == 0) 0.0 else nonNullFields.toDouble / totalFields
  }
  
  /**
   * Create error record
   */
  private def createErrorRecord(originalValue: String, errorMessage: String): String = {
    val timestamp = System.currentTimeMillis()
    s"""{"timestamp": $timestamp, "error": "$errorMessage", "original_record": "${originalValue.replace("\"", "\\\"")}"}"""
  }
} 