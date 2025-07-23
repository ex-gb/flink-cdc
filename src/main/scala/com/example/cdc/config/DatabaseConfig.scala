package com.example.cdc.config

import org.apache.flink.api.java.utils.ParameterTool
import java.util.Properties

/**
 * Database types supported by the CDC pipeline
 */
sealed trait DatabaseType {
  def name: String
}

object DatabaseType {
  case object PostgreSQL extends DatabaseType { val name = "postgres" }
  case object MySQL extends DatabaseType { val name = "mysql" }
  
  def fromString(value: String): DatabaseType = value.toLowerCase match {
    case "postgres" | "postgresql" => PostgreSQL
    case "mysql" => MySQL
    case _ => throw new IllegalArgumentException(s"Unsupported database type: $value")
  }
}

/**
 * Abstract database configuration with common properties
 */
sealed trait DatabaseConfig {
  def databaseType: DatabaseType
  def hostname: String
  def port: Int
  def database: String
  def username: String
  def password: String
  def schemaList: String
  def tableList: String
  def startupMode: String
  def debeziumProperties: Properties
  def getTableArray: Array[String] = tableList.replaceAll(" ", "").split(",")
}

/**
 * PostgreSQL-specific configuration
 */
case class PostgresConfig(
  hostname: String,
  port: Int,
  database: String,
  username: String,
  password: String,
  schemaList: String,
  tableList: String,
  slotName: String,
  pluginName: String,
  startupMode: String,
  debeziumProperties: Properties
) extends DatabaseConfig {
  override def databaseType: DatabaseType = DatabaseType.PostgreSQL
}

/**
 * MySQL-specific configuration
 */
case class MySqlConfig(
  hostname: String,
  port: Int,
  database: String,
  username: String,
  password: String,
  schemaList: String,
  tableList: String,
  serverId: String,
  serverTimeZone: String,
  startupMode: String,
  debeziumProperties: Properties
) extends DatabaseConfig {
  override def databaseType: DatabaseType = DatabaseType.MySQL
}

/**
 * Factory for creating database configurations from parameters
 */
object DatabaseConfigFactory {
  
  def createDatabaseConfig(params: ParameterTool): DatabaseConfig = {
    // Determine database type
    val databaseType = determineDatabaseType(params)
    
    databaseType match {
      case DatabaseType.PostgreSQL => createPostgresConfig(params)
      case DatabaseType.MySQL => createMySqlConfig(params)
    }
  }
  
  private def determineDatabaseType(params: ParameterTool): DatabaseType = {
    // Check explicit database type parameter
    val explicitType = params.get("database.type")
    if (explicitType != null) {
      return DatabaseType.fromString(explicitType)
    }
    
    // Check for database-specific parameters
    val hasPostgresParams = params.get("postgres.hostname") != null || params.get("postgres.port") != null
    val hasMysqlParams = params.get("mysql.hostname") != null || params.get("mysql.port") != null
    
    if (hasMysqlParams && !hasPostgresParams) {
      DatabaseType.MySQL
    } else if (hasPostgresParams && !hasMysqlParams) {
      DatabaseType.PostgreSQL
    } else {
      // Default to PostgreSQL for backward compatibility
      DatabaseType.PostgreSQL
    }
  }
  
  private def createPostgresConfig(params: ParameterTool): PostgresConfig = {
    val hostname = params.get("postgres.hostname", params.get("hostname", "localhost"))
    val port = params.getInt("postgres.port", params.getInt("port", 5432))
    val database = params.get("postgres.database", params.get("database", "cdc_source"))
    val username = params.get("postgres.username", params.get("username", "postgres"))
    val password = params.get("postgres.password", params.get("password", "postgres"))
    val schemaList = params.get("postgres.schema-list", params.get("schema.list", "public"))
    val tableList = params.get("postgres.table-list", params.get("tables", "public.users"))
    val slotName = params.get("postgres.slot-name", params.get("slot.name", "flink_cdc_slot"))
    val pluginName = params.get("postgres.plugin-name", "pgoutput")
    val startupMode = params.get("postgres.startup-mode", "initial")
    
    // Debezium properties for PostgreSQL
    val debeziumProperties = new Properties()
    debeziumProperties.setProperty("snapshot.mode", startupMode)
    debeziumProperties.setProperty("decimal.handling.mode", params.get("postgres.decimal-handling-mode", "double"))
    debeziumProperties.setProperty("binary.handling.mode", params.get("postgres.binary-handling-mode", "base64"))
    debeziumProperties.setProperty("time.precision.mode", params.get("postgres.time-precision-mode", "connect"))
    debeziumProperties.setProperty("include.schema.changes", params.get("postgres.include-schema-changes", "false"))
    debeziumProperties.setProperty("slot.drop.on.stop", params.get("postgres.slot-drop-on-stop", "false"))
    debeziumProperties.setProperty("heartbeat.interval.ms", params.get("postgres.heartbeat-interval-ms", "30000"))
    debeziumProperties.setProperty("max.batch.size", params.get("postgres.max-batch-size", "2048"))
    debeziumProperties.setProperty("max.queue.size", params.get("postgres.max-queue-size", "8192"))
    
    PostgresConfig(
      hostname, port, database, username, password, schemaList, tableList,
      slotName, pluginName, startupMode, debeziumProperties
    )
  }
  
  private def createMySqlConfig(params: ParameterTool): MySqlConfig = {
    val hostname = params.get("mysql.hostname", params.get("hostname", "localhost"))
    val port = params.getInt("mysql.port", params.getInt("port", 3306))
    val database = params.get("mysql.database", params.get("database", "cdc_source"))
    val username = params.get("mysql.username", params.get("username", "cdc_user"))
    val password = params.get("mysql.password", params.get("password", "cdc_password"))
    val schemaList = params.get("mysql.schema-list", params.get("schema.list", database))
    val tableList = params.get("mysql.table-list", params.get("tables", s"$database.users"))
    val serverId = params.get("mysql.server-id", "5400-5404")
    val serverTimeZone = params.get("mysql.server-time-zone", params.get("server-time-zone", "UTC"))
    val startupMode = params.get("mysql.startup-mode", "initial")
    
    // Debezium properties for MySQL
    val debeziumProperties = new Properties()
    debeziumProperties.setProperty("snapshot.mode", startupMode)
    debeziumProperties.setProperty("decimal.handling.mode", params.get("mysql.decimal-handling-mode", "double"))
    debeziumProperties.setProperty("binary.handling.mode", params.get("mysql.binary-handling-mode", "base64"))
    debeziumProperties.setProperty("time.precision.mode", params.get("mysql.time-precision-mode", "connect"))
    debeziumProperties.setProperty("include.schema.changes", params.get("mysql.include-schema-changes", "false"))
    debeziumProperties.setProperty("heartbeat.interval.ms", params.get("mysql.heartbeat-interval-ms", "30000"))
    debeziumProperties.setProperty("max.batch.size", params.get("mysql.max-batch-size", "2048"))
    debeziumProperties.setProperty("max.queue.size", params.get("mysql.max-queue-size", "8192"))
    debeziumProperties.setProperty("scan.incremental.snapshot.enabled", params.get("mysql.incremental-snapshot-enabled", "true"))
    
    MySqlConfig(
      hostname, port, database, username, password, schemaList, tableList,
      serverId, serverTimeZone, startupMode, debeziumProperties
    )
  }
} 