package com.example.cdc.validation

import com.example.cdc.config.AppConfig
import org.slf4j.{Logger, LoggerFactory}

/**
 * Custom exceptions for better error handling
 */
case class ConfigurationException(message: String, cause: Throwable = null) extends Exception(message, cause)
case class EnvironmentValidationException(message: String, cause: Throwable = null) extends Exception(message, cause)
case class S3ValidationException(message: String, cause: Throwable = null) extends Exception(message, cause)

/**
 * Environment-specific validation for CDC pipeline configurations
 * 
 * Handles validation for all supported environments:
 * - LOCAL: Database-only validation
 * - DEV/STG/PROD: Full database + S3 + AWS credentials validation
 */
object EnvironmentValidator {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Validate local mode configuration with enhanced error handling
   */
  def validateLocalModeConfig(config: AppConfig): Unit = {
    try {
      // Only validate database connection for local mode
      if (config.PostgresConfig.hostname.isEmpty) {
        throw ConfigurationException("PostgreSQL hostname is required for local mode")
      }
      if (config.PostgresConfig.database.isEmpty) {
        throw ConfigurationException("PostgreSQL database name is required for local mode")
      }
      if (config.PostgresConfig.username.isEmpty) {
        throw ConfigurationException("PostgreSQL username is required for local mode")
      }
      if (config.PostgresConfig.tableList.isEmpty) {
        throw ConfigurationException("PostgreSQL table list is required for local mode (format: schema.table1,schema.table2)")
      }
      
      logger.info("✅ Local mode validation passed - Database configuration OK")
      
    } catch {
      case ex: ConfigurationException =>
        logger.error(s"Local mode configuration validation failed: ${ex.getMessage}")
        throw ex
      case ex: Exception =>
        logger.error(s"Unexpected error during local mode validation: ${ex.getMessage}", ex)
        throw ConfigurationException("Local mode validation failed due to unexpected error", ex)
    }
  }
  
  /**
   * Validate S3-enabled mode configuration (dev/stg/prod) with enhanced error handling
   */
  def validateS3ModeConfig(config: AppConfig, envName: String): Unit = {
    try {
      // Validate database configuration with specific error messages
      validateDatabaseConfiguration(config, envName)
      
      // Validate S3 configuration with specific error messages
      validateS3Configuration(config, envName)
      
      // Validate AWS credentials
      validateAwsCredentials(config, envName)
      
      logger.info(s"✅ $envName mode validation passed - Database and S3 configuration OK")
      
    } catch {
      case ex @ (_: ConfigurationException | _: S3ValidationException | _: EnvironmentValidationException) =>
        logger.error(s"$envName mode configuration validation failed: ${ex.getMessage}")
        throw ex
      case ex: Exception =>
        logger.error(s"Unexpected error during $envName mode validation: ${ex.getMessage}", ex)
        throw EnvironmentValidationException(s"$envName mode validation failed due to unexpected error", ex)
    }
  }
  
  /**
   * Validate database configuration with specific error messages
   */
  private def validateDatabaseConfiguration(config: AppConfig, envName: String): Unit = {
    if (config.PostgresConfig.hostname.isEmpty) {
      throw ConfigurationException(s"PostgreSQL hostname is required for $envName mode. Use --hostname parameter.")
    }
    if (config.PostgresConfig.database.isEmpty) {
      throw ConfigurationException(s"PostgreSQL database name is required for $envName mode. Use --database parameter.")
    }
    if (config.PostgresConfig.username.isEmpty) {
      throw ConfigurationException(s"PostgreSQL username is required for $envName mode. Use --username parameter.")
    }
    if (config.PostgresConfig.tableList.isEmpty) {
      throw ConfigurationException(s"PostgreSQL table list is required for $envName mode. Use --tables parameter (format: schema.table1,schema.table2).")
    }
  }
  
  /**
   * Validate S3 configuration with specific error messages
   */
  private def validateS3Configuration(config: AppConfig, envName: String): Unit = {
    if (config.S3Config.bucketName.isEmpty) {
      val errorMsg = s"S3 bucket not configured for $envName mode! Please provide --s3-bucket parameter."
      logger.error(errorMsg)
      printS3ConfigurationHelp()
      throw S3ValidationException(errorMsg)
    }
    
    // Validate S3 configuration values
    val validFileFormats = Set("json", "avro", "parquet")
    if (!validFileFormats.contains(config.S3Config.fileFormat.toLowerCase)) {
      throw S3ValidationException(s"Invalid file format '${config.S3Config.fileFormat}'. Valid formats: ${validFileFormats.mkString(", ")}")
    }
    
    val validCompressionTypes = Set("gzip", "snappy", "lz4", "none")
    if (!validCompressionTypes.contains(config.S3Config.compressionType.toLowerCase)) {
      throw S3ValidationException(s"Invalid compression type '${config.S3Config.compressionType}'. Valid types: ${validCompressionTypes.mkString(", ")}")
    }
    
    logger.info(s"  S3 Bucket: ${config.S3Config.bucketName}")
    logger.info(s"  S3 Base Path: ${config.S3Config.basePath}")
    logger.info(s"  AWS Region: ${config.S3Config.region}")
    logger.info(s"  File Format: ${config.S3Config.fileFormat}")
    logger.info(s"  Compression: ${config.S3Config.compressionType}")
    logger.info(s"  Max File Size: ${config.S3Config.maxFileSize}")
    logger.info(s"  Rollover Interval: ${config.S3Config.rolloverInterval}")
  }
  
  /**
   * Validate AWS credentials for S3 access with enhanced error handling
   */
  private def validateAwsCredentials(config: AppConfig, envName: String): Unit = {
    try {
      val hasCommandLineCredentials = config.S3Config.accessKey.nonEmpty && config.S3Config.secretKey.nonEmpty
      val hasEnvironmentCredentials = sys.env.contains("AWS_ACCESS_KEY_ID") && sys.env.contains("AWS_SECRET_ACCESS_KEY")
      val hasAwsProfile = sys.env.contains("AWS_PROFILE") || new java.io.File(System.getProperty("user.home") + "/.aws/credentials").exists()
      
      if (hasCommandLineCredentials) {
        logger.info(s"✅ AWS Credentials ($envName): Command line parameters detected")
      } else if (hasEnvironmentCredentials) {
        logger.info(s"✅ AWS Credentials ($envName): Environment variables detected")
      } else if (hasAwsProfile) {
        logger.info(s"✅ AWS Credentials ($envName): AWS profile/credentials file detected")
        if (sys.env.contains("AWS_PROFILE")) {
          logger.info(s"   📋 Using AWS Profile: ${sys.env("AWS_PROFILE")}")
        }
      } else {
        val errorMsg = s"AWS Credentials for $envName mode: No valid credentials found! Please configure AWS credentials using one of the supported methods."
        logger.error(s"❌ $errorMsg")
        printS3ConfigurationHelp()
        throw S3ValidationException(errorMsg)
      }
      
    } catch {
      case ex: S3ValidationException =>
        throw ex
      case ex: Exception =>
        logger.error(s"Error validating AWS credentials for $envName mode: ${ex.getMessage}", ex)
        throw S3ValidationException(s"AWS credentials validation failed for $envName mode", ex)
    }
  }
  
  /**
   * Print S3 configuration help
   */
  private def printS3ConfigurationHelp(): Unit = {
    logger.info("""
      |💡 S3 Configuration Help:
      |
      |For PRODUCTION mode, you need to configure AWS credentials using ONE of these methods:
      |
      |1. Command Line Parameters:
      |   --s3-access-key YOUR_ACCESS_KEY --s3-secret-key YOUR_SECRET_KEY
      |
      |2. Environment Variables:
      |   export AWS_ACCESS_KEY_ID=your-access-key
      |   export AWS_SECRET_ACCESS_KEY=your-secret-key
      |
      |3. AWS Profile:
      |   aws configure --profile default
      |
      |4. IAM Role (when running on AWS EC2/ECS/EKS)
      |
      |Required S3 parameters for production mode:
      |  --s3-bucket BUCKET_NAME    (required)
      |  --s3-region REGION         (optional, default: us-east-1)
      |""".stripMargin)
  }
} 