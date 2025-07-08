package com.example.cdc

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object TestPostgresConnection {
  
  def main(args: Array[String]): Unit = {
    println("🔍 Testing PostgreSQL CDC Setup")
    
    // Database configuration
    val url = "jdbc:postgresql://localhost:5432/cdc_source"
    val username = "cdc_user"
    val password = "cdc_password"
    
    var connection: Connection = null
    var statement: Statement = null
    var resultSet: ResultSet = null
    
    try {
      // Load PostgreSQL driver
      Class.forName("org.postgresql.Driver")
      
      // Create connection
      connection = DriverManager.getConnection(url, username, password)
      println("✅ Connected to PostgreSQL successfully!")
      
      // Create statement
      statement = connection.createStatement()
      
      // Test 1: Check if tables exist
      println("\n📊 Testing table existence...")
      resultSet = statement.executeQuery("SELECT COUNT(*) FROM users")
      if (resultSet.next()) {
        val userCount = resultSet.getInt(1)
        println(s"✅ Users table has $userCount records")
      }
      
      resultSet.close()
      resultSet = statement.executeQuery("SELECT COUNT(*) FROM orders")
      if (resultSet.next()) {
        val orderCount = resultSet.getInt(1)
        println(s"✅ Orders table has $orderCount records")
      }
      
      // Test 2: Check replication configuration
      println("\n🔧 Testing replication configuration...")
      resultSet.close()
      resultSet = statement.executeQuery("SHOW wal_level")
      if (resultSet.next()) {
        val walLevel = resultSet.getString(1)
        println(s"✅ WAL level: $walLevel")
      }
      
      resultSet.close()
      resultSet = statement.executeQuery("SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots")
      println("\n🎯 Replication slots:")
      while (resultSet.next()) {
        val slotName = resultSet.getString("slot_name")
        val plugin = resultSet.getString("plugin")
        val slotType = resultSet.getString("slot_type")
        val active = resultSet.getBoolean("active")
        println(s"  - $slotName ($plugin, $slotType, active: $active)")
      }
      
      // Test 3: Insert test data
      println("\n✏️  Inserting test data...")
      val insertSql = "INSERT INTO users (name, email, age) VALUES ('Test User', 'test@example.com', 25)"
      val rowsAffected = statement.executeUpdate(insertSql)
      println(s"✅ Inserted $rowsAffected row(s)")
      
      // Test 4: Read the data back
      println("\n📖 Reading current data...")
      resultSet.close()
      resultSet = statement.executeQuery("SELECT id, name, email, age FROM users ORDER BY id DESC LIMIT 5")
      println("Recent users:")
      while (resultSet.next()) {
        val id = resultSet.getInt("id")
        val name = resultSet.getString("name")
        val email = resultSet.getString("email")
        val age = resultSet.getInt("age")
        println(s"  $id: $name ($email, age: $age)")
      }
      
      println("\n🎉 All tests passed! PostgreSQL CDC setup is working correctly.")
      
    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Close resources
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
} 