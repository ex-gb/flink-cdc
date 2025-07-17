package com.example.cdc.filters

import org.apache.flink.api.common.functions.FilterFunction
import org.slf4j.{Logger, LoggerFactory}

/**
 * Serializable table filter for CDC events
 */
class TableFilter(tableWithSchema: String, tableName: String) extends FilterFunction[String] {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
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
          logger.info(s"✅ Event matched table $tableName: ${event.take(100)}...")
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