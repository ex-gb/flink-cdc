ThisBuild / version := "1.2.0"
ThisBuild / scalaVersion := "2.12.17"

// Version definitions - Using stable Flink CDC versions
lazy val flinkVersion = "1.18.0"
lazy val postgresCdcVersion = "2.4.2"
lazy val mysqlCdcVersion = "2.4.2"

// Project configuration
lazy val root = (project in file("."))
  .settings(
    name := "flink-cdc-multicloud",
    organization := "com.example",
    
    libraryDependencies ++= Seq(
      // Flink Core (provided - these are already in Flink runtime)
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
      "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
      "org.apache.flink" % "flink-runtime" % flinkVersion % "provided",
      
      // Ververica CDC Dependencies - More stable and compatible
      "com.ververica" % "flink-connector-postgres-cdc" % postgresCdcVersion,
      "com.ververica" % "flink-connector-mysql-cdc" % mysqlCdcVersion,
      "org.apache.flink" % "flink-avro" % flinkVersion % "provided",
      "org.apache.flink" % "flink-connector-files" % flinkVersion % "provided",
      
      // GCP GCS Dependencies - Google Cloud Storage support
      "org.apache.flink" % "flink-gs-fs-hadoop" % flinkVersion,
      "com.google.cloud" % "google-cloud-storage" % "2.29.1",
      "com.google.auth" % "google-auth-library-oauth2-http" % "1.19.0",
      "com.google.auth" % "google-auth-library-credentials" % "1.19.0",
      
      // AWS S3 Dependencies (for backward compatibility and migration support)
      "org.apache.flink" % "flink-s3-fs-hadoop" % flinkVersion,
      "org.apache.hadoop" % "hadoop-aws" % "3.3.6",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.565",
      
      // Avro for serialization
      "org.apache.avro" % "avro" % "1.11.3",
      
      // JSON Processing (for our custom parsing code) - Updated for Flink 1.18.0 compatibility
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      
      // Database connectors
      "org.postgresql" % "postgresql" % "42.7.1",
      "mysql" % "mysql-connector-java" % "8.0.33",
      
      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.14"
    ),
    
    // Assembly configuration
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => xs.map(_.toLowerCase) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
        case ("license" :: _) | ("license.txt" :: _) | ("notice" :: _) | ("notice.txt" :: _) => MergeStrategy.discard
        case ("services" :: _) => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
      case "application.conf" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case "log4j.properties" => MergeStrategy.discard
      case "log4j2.xml" => MergeStrategy.discard
      case "logback.xml" => MergeStrategy.first
      case x if x.contains("FlinkUserCodeClassLoaders") => MergeStrategy.first
      case x if x.contains("org.apache.flink.table.planner") => MergeStrategy.first
      case x if x.contains("codegen") => MergeStrategy.first
      case x if x.contains("janino") => MergeStrategy.first
      case x if x.endsWith(".class") => MergeStrategy.first
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".dtd") => MergeStrategy.first
      case x if x.endsWith(".xsd") => MergeStrategy.first
      case x if x.contains("mozilla") => MergeStrategy.first
      case x if x.contains("google") => MergeStrategy.first
      case x if x.contains("jersey") => MergeStrategy.first
      case x if x.contains("jaxb") => MergeStrategy.first
      case x if x.contains("javax.ws.rs") => MergeStrategy.first
      case x if x.contains("aopalliance") => MergeStrategy.first
      case x if x.contains("commons-") => MergeStrategy.first
      case x if x.contains("kryo") => MergeStrategy.first
      case x if x.contains("akka") => MergeStrategy.first
      case x if x.contains("scala") => MergeStrategy.first
      case x if x.contains("jackson") => MergeStrategy.first
      case x if x.contains("flink") => MergeStrategy.first
      case x if x.contains("hadoop") => MergeStrategy.first
      case x if x.contains("parquet") => MergeStrategy.first
      case x if x.contains("avro") => MergeStrategy.first
      case x if x.contains("aws") => MergeStrategy.first
      case x if x.contains("google") => MergeStrategy.first
      case x if x.contains("gcp") => MergeStrategy.first
      case x if x.contains("auth") => MergeStrategy.first
      case x if x.contains("debezium") => MergeStrategy.first
      case x if x.contains("kafka") => MergeStrategy.first
      case x if x.contains("connect") => MergeStrategy.first
      case x if x.contains("module-info") => MergeStrategy.discard
      case x if x.contains("MANIFEST") => MergeStrategy.discard
      case x if x.contains("LICENSE") => MergeStrategy.discard
      case x if x.contains("NOTICE") => MergeStrategy.discard
      case x if x.contains("DEPENDENCIES") => MergeStrategy.discard
      case x if x.contains("THIRD-PARTY") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    
    // Assembly settings
    assembly / assemblyJarName := "flink-cdc-gcs-assembly.jar",
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { f =>
        val name = f.data.getName.toLowerCase
        name.contains("flink-dist") || 
        name.contains("flink-table") ||
        name.contains("hadoop-mapreduce-client-core") ||
        name.contains("hadoop-mapreduce-client-common") ||
        name.contains("hadoop-yarn-")
      }
    },
    
    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused:imports",
      "-Ywarn-unused:locals",
      "-Ywarn-unused:params",
      "-Ywarn-unused:patvars",
      "-Ywarn-unused:privates"
    ),
    
    // Java options
    javacOptions ++= Seq(
      "-source", "8",
      "-target", "8",
      "-encoding", "UTF-8"
    ),
    
    // Test options
    Test / parallelExecution := false,
    Test / fork := true,
    Test / testOptions += Tests.Argument("-oDF"),
    
    // Runtime options
    run / javaOptions ++= Seq(
      "-Xmx2g",
      "-Xms1g",
      "-XX:+UseG1GC",
      "-XX:+PrintGCDetails",
      "-XX:+PrintGCTimeStamps",
      "-Dlogback.configurationFile=src/main/resources/logback.xml"
    ),
    
    // Resolvers
    resolvers ++= Seq(
      "Apache Repository" at "https://repo.maven.apache.org/maven2/",
      "Confluent Repository" at "https://packages.confluent.io/maven/",
      "Hortonworks Repository" at "https://repo.hortonworks.com/content/repositories/releases/",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    )
  )

 