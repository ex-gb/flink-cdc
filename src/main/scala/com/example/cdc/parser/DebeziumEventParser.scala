package com.example.cdc.parser

import com.example.cdc.model.{CdcEvent, DebeziumEvent}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class DebeziumEventParser extends FlatMapFunction[String, CdcEvent] {
  
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(classOf[DebeziumEventParser])
  
  @transient private lazy val objectMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }
  
  override def flatMap(jsonString: String, collector: Collector[CdcEvent]): Unit = {
    Try {
      logger.debug(s"Parsing JSON: $jsonString")
      
      // Parse the JSON string into a DebeziumEvent
      val debeziumEvent = objectMapper.readValue(jsonString, classOf[DebeziumEvent])
      
      // Convert to our internal CdcEvent model
      val cdcEvent = CdcEvent.fromDebeziumEvent(debeziumEvent)
      
      logger.info(s"Parsed CDC event: ${cdcEvent.database}.${cdcEvent.schema}.${cdcEvent.table} - ${cdcEvent.operation}")
      
      collector.collect(cdcEvent)
      
    } match {
      case Success(_) => // Event processed successfully
      case Failure(exception) =>
        logger.error(s"Failed to parse JSON: $jsonString", exception)
        // You might want to send this to a dead letter queue or error topic
    }
  }
}

object DebeziumEventParser {
  def parseDebeziumEvent(jsonString: String): Option[CdcEvent] = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    Try {
      val debeziumEvent = objectMapper.readValue(jsonString, classOf[DebeziumEvent])
      CdcEvent.fromDebeziumEvent(debeziumEvent)
    } match {
      case Success(event) => Some(event)
      case Failure(exception) =>
        LoggerFactory.getLogger(this.getClass).error(s"Failed to parse JSON: $jsonString", exception)
        None
    }
  }
} 