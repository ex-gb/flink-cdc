package com.example.cdc.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
case class SourceInfo(
  @JsonProperty("version") version: String,
  @JsonProperty("connector") connector: String,
  @JsonProperty("name") name: String,
  @JsonProperty("ts_ms") tsMs: Long,
  @JsonProperty("snapshot") snapshot: Option[String],
  @JsonProperty("db") db: String,
  @JsonProperty("schema") schema: String,
  @JsonProperty("table") table: String,
  @JsonProperty("txId") txId: Option[Long],
  @JsonProperty("lsn") lsn: Option[Long],
  @JsonProperty("xmin") xmin: Option[Long]
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class DebeziumEvent(
  @JsonProperty("before") before: Option[Map[String, Any]],
  @JsonProperty("after") after: Option[Map[String, Any]],
  @JsonProperty("source") source: SourceInfo,
  @JsonProperty("op") op: String,
  @JsonProperty("ts_ms") tsMs: Long,
  @JsonProperty("transaction") transaction: Option[Map[String, Any]]
)

case class CdcEvent(
  database: String,
  schema: String,
  table: String,
  operation: String,
  eventTimestamp: Long,
  before: Option[Map[String, Any]],
  after: Option[Map[String, Any]],
  sourceInfo: SourceInfo,
  processedAt: Long = System.currentTimeMillis()
) {
  def toJsonString: String = {
    CdcEvent.objectMapper.writeValueAsString(this)
  }
}

object CdcEvent {
  @transient private lazy val objectMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }
  
  def fromDebeziumEvent(debeziumEvent: DebeziumEvent): CdcEvent = {
    val source = debeziumEvent.source
    
    CdcEvent(
      database = source.db,
      schema = source.schema,
      table = source.table,
      operation = debeziumEvent.op,
      eventTimestamp = debeziumEvent.tsMs,
      before = debeziumEvent.before,
      after = debeziumEvent.after,
      sourceInfo = source
    )
  }
} 