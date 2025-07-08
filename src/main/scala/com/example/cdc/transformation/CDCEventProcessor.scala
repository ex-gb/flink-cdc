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
   * Process regular CDC events
   */
  private def processRegularCDCEvent(originalValue: String): String = {
    // For now, return original value with minimal processing
    // TODO: Add data transformation, validation, and enrichment
    originalValue
  }
  
  /**
   * Create error record
   */
  private def createErrorRecord(originalValue: String, errorMessage: String): String = {
    val timestamp = System.currentTimeMillis()
    s"""{"timestamp": $timestamp, "error": "$errorMessage", "original_record": "${originalValue.replace("\"", "\\\"")}"}"""
  }
} 