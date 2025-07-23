package com.example.cdc.config

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.connector.source.Source
import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.{Logger, LoggerFactory}

/**
 * Unified CDC source wrapper that handles both old SourceFunction and new Source API
 */
sealed trait CDCSource {
  def addToEnvironment(env: StreamExecutionEnvironment, sourceName: String, uid: String): DataStream[String]
}

case class NewAPISource(source: Source[String, _, _]) extends CDCSource {
  override def addToEnvironment(env: StreamExecutionEnvironment, sourceName: String, uid: String): DataStream[String] = {
    env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName).uid(uid)
  }
}

case class LegacyAPISource(sourceFunction: SourceFunction[String]) extends CDCSource {
  override def addToEnvironment(env: StreamExecutionEnvironment, sourceName: String, uid: String): DataStream[String] = {
    env.addSource(sourceFunction).name(sourceName).uid(uid)
  }
}

/**
 * Factory for creating database CDC sources with unified interface
 */
object DatabaseSourceFactory {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Creates appropriate CDC source based on database configuration
   */
  def createSource(databaseConfig: DatabaseConfig): CDCSource = {
    logger.info(s"Creating CDC source for ${databaseConfig.databaseType.name} database")
    
    databaseConfig match {
      case config: PostgresConfig => createPostgreSQLSource(config)
      case config: MySqlConfig => createMySqlSource(config)
      case _ => throw new IllegalArgumentException(s"Unsupported database type: ${databaseConfig.databaseType}")
    }
  }
  
  /**
   * Creates PostgreSQL CDC source using Ververica CDC
   */
  private def createPostgreSQLSource(config: PostgresConfig): CDCSource = {
    logger.info(s"Building PostgreSQL CDC source for ${config.hostname}:${config.port}/${config.database} (Ververica CDC)")
    
    val sourceFunction = PostgreSQLSource.builder()
      .hostname(config.hostname)
      .port(config.port)
      .database(config.database)
      .schemaList(config.schemaList)
      .tableList(config.tableList)
      .username(config.username)
      .password(config.password)
      .slotName(config.slotName)
      .decodingPluginName(config.pluginName)
      .deserializer(new JsonDebeziumDeserializationSchema())
      .debeziumProperties(config.debeziumProperties)
      .build()
    
    LegacyAPISource(sourceFunction)
  }
  
  /**
   * Creates MySQL CDC source using Ververica CDC
   */
  private def createMySqlSource(config: MySqlConfig): CDCSource = {
    logger.info(s"Building MySQL CDC source for ${config.hostname}:${config.port}/${config.database} (Ververica CDC)")
    logger.info(s"MySQL server timezone: ${config.serverTimeZone}")
    
    val sourceFunction = MySqlSource.builder()
      .hostname(config.hostname)
      .port(config.port)
      .databaseList(config.database)
      .tableList(config.tableList)
      .username(config.username)
      .password(config.password)
      .serverId(config.serverId.split("-").head.toInt)
      .serverTimeZone(config.serverTimeZone)
      .deserializer(new JsonDebeziumDeserializationSchema())
      .debeziumProperties(config.debeziumProperties)
      .build()
    
    LegacyAPISource(sourceFunction)
  }
} 