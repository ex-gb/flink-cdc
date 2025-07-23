# Multi-Database CDC to S3 Pipeline

Real-time streaming of PostgreSQL and MySQL database changes to AWS S3 using Apache Flink and Change Data Capture (CDC).

## ✨ What This Does

- **Captures database changes** in real-time from PostgreSQL and MySQL
- **Streams data to S3** in Avro format (with JSON/Parquet options)
- **Four deployment modes**: Safe local testing + Dev/Staging/Production S3 writing
- **Environment-aware**: Easy switching between local/dev/staging/prod environments
- **Multi-database support**: PostgreSQL and MySQL with unified Ververica CDC 2.4.2
- **Production-ready**: Built with Flink 1.18.0 + organized configuration structure
- **Comprehensive monitoring**: Built-in metrics, error handling, and schema change detection

## 🏗️ Architecture & New Features (v1.4.0)

### Enhanced Architecture with Ververica CDC
```
PostgreSQL/MySQL → Unified CDC Source → Event Processor → Environment Router
                ↓              ↓                    ↓
           Raw Logging → Error Handler → Local Simulation / S3 Sink
                ↓              ↓                    ↓
           Table Filter → Schema Change → Monitoring & Metrics
```

### 📁 Organized Project Structure (NEW in v1.4.0)
```
flink-cdc-s3/
├── build.sbt                     # sbt build configuration with Ververica CDC 2.4.2
├── docker-compose.yml            # Database containers setup
├── database/                     # 📁 Organized database configuration
│   ├── init/                     # Database initialization scripts
│   │   ├── init-db.sql           # PostgreSQL: tables, data, CDC setup
│   │   └── init-mysql.sql        # MySQL: tables, data, permissions
│   └── config/                   # Database configuration files
│       └── mysql-cdc.cnf         # MySQL: CDC optimization & performance
├── src/main/scala/               # Scala source code
│   └── com/example/cdc/
│       ├── ProductionCdcJob.scala # Main application with unified CDC
│       ├── config/               # Database & CDC configuration
│       │   ├── DatabaseConfig.scala        # Database abstraction
│       │   └── DatabaseSourceFactory.scala # Unified CDC source factory
│       ├── filters/              # Event filtering logic
│       ├── handlers/             # Error & schema change handling
│       ├── mappers/              # Event transformation
│       ├── model/                # Data models
│       ├── monitoring/           # Performance monitoring
│       ├── parser/               # Event parsing
│       ├── sink/                 # S3 output handling
│       ├── transformation/       # Event processing
│       └── validation/           # Environment validation
├── flink-1.18.0/                 # Flink installation
└── target/                       # Build artifacts
```

### Key Improvements in v1.4.0
- **🎯 Ververica CDC 2.4.2**: Unified, stable CDC connectors for both databases
- **📂 Organized Structure**: Database files properly organized in `database/` folder
- **🔧 Better Configuration**: Separated initialization and configuration files
- **⚡ Performance Optimized**: MySQL CDC configuration tuned for production
- **🧪 I,D,U Testing**: Verified Insert, Delete, Update operations for both databases
- **🤝 Consistent API**: Both MySQL and PostgreSQL use Legacy API for uniformity

## 📋 Database Configuration Structure

The project now features an organized database configuration system for better maintainability:

### File Organization
```
database/
├── init/                         # Database initialization scripts
│   ├── init-db.sql              # PostgreSQL setup
│   │   ├── Tables: users, orders
│   │   ├── Sample data insertion
│   │   ├── CDC permissions (GRANT SELECT)
│   │   └── Logical replication setup (CREATE PUBLICATION)
│   └── init-mysql.sql           # MySQL setup  
│       ├── Tables: users, orders, products
│       ├── Sample data insertion
│       ├── CDC permissions (REPLICATION SLAVE/CLIENT)
│       └── Binary log diagnostics (SHOW MASTER STATUS)
└── config/                      # Database configuration files
    └── mysql-cdc.cnf           # MySQL CDC optimization
        ├── Binary logging (log-bin, binlog-format=ROW)
        ├── GTID configuration (gtid-mode=ON)
        ├── Performance tuning (buffers, timeouts)
        └── Character set (utf8mb4)
```

### Why This Organization?

#### Benefits
- **🎯 Logical Grouping**: All database files in one place
- **🔍 Easy Navigation**: Clear separation of initialization vs configuration
- **📊 Professional Structure**: Industry-standard project layout
- **🚀 Scalability**: Easy to add more database types or configuration files

#### File Purposes
| File | Purpose | When Used |
|------|---------|-----------|
| `database/init/init-db.sql` | PostgreSQL tables & data | Container startup |
| `database/init/init-mysql.sql` | MySQL tables & data | Container startup |
| `database/config/mysql-cdc.cnf` | MySQL CDC optimization | MySQL server startup |

#### Volume Mappings in docker-compose.yml
```yaml
postgres:
  volumes:
    - ./database/init/init-db.sql:/docker-entrypoint-initdb.d/init-db.sql

mysql:
  volumes:
    - ./database/init/init-mysql.sql:/docker-entrypoint-initdb.d/init-mysql.sql
    - ./database/config/mysql-cdc.cnf:/etc/mysql/conf.d/mysql-cdc.cnf
```

#### PostgreSQL vs MySQL Configuration Approach

**PostgreSQL**: Simple command-line configuration
- No config file needed - PostgreSQL CDC is simpler by design
- 4 command-line flags sufficient: `wal_level=logical`, `max_wal_senders=10`, etc.
- Runtime SQL setup via `CREATE PUBLICATION`

**MySQL**: File-based configuration required
- Complex CDC setup requires extensive server configuration
- 32 configuration parameters in `mysql-cdc.cnf`
- Binary logging, GTID, performance tuning all needed

### Testing the Organized Structure

After reorganization, verify everything works:

```bash
# 1. Test database startup with new structure
docker-compose up -d
docker ps  # Should show both databases healthy

# 2. Verify initialization files were loaded
docker exec postgres-cdc psql -U cdc_user -d cdc_source -c "SELECT COUNT(*) FROM users;"
docker exec mysql-cdc mysql -u cdc_user -pcdc_password cdc_source -e "SELECT COUNT(*) FROM users;"

# 3. Test CDC with organized configuration
cd flink-1.18.0
./bin/flink run -c com.example.cdc.ProductionCdcJob \
  ../target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --env local --database.type mysql

# 4. Add test data to verify CDC capture
docker exec mysql-cdc mysql -u cdc_user -pcdc_password cdc_source -e "
  INSERT INTO users (name, email, age) VALUES ('Structure Test', 'structure@test.com', 25);
  UPDATE users SET age = 26 WHERE email = 'structure@test.com';
  DELETE FROM users WHERE email = 'structure@test.com';
"
```

Expected results:
- ✅ Both databases start successfully
- ✅ Tables are created and populated 
- ✅ CDC jobs process I,D,U operations
- ✅ Configuration files are properly loaded

### New Components Added
- **Multi-Database Support**: PostgreSQL and MySQL with automatic type detection
- **Database Abstraction**: Clean configuration layer supporting both database types
- **Environment Validation**: `EnvironmentValidator.scala` - Validates configurations per environment
- **Table Filtering**: `TableFilter.scala` - Serializable multi-table CDC event filtering
- **Error Handling**: `ErrorHandler.scala` - Unified error processing for all environments
- **CDC Mappers**: `CDCMappers.scala` - Environment-specific event transformation
- **Enhanced Monitoring**: Environment-aware metrics and logging

### Environment-Specific Configurations
| Mode | Icon | Description | S3 Operations | Parallelism | Checkpointing | Use Case |
|------|------|-------------|---------------|-------------|---------------|----------|
| `--env local` | 🧪 | Safe testing | **Simulated only** | 1 | 30s (lenient) | Local development |
| `--env dev` | 🔧 | Development | Dev S3 writes | 1 | 45s (moderate) | Development testing |
| `--env stg` | 🎭 | Staging | Staging S3 writes | 2 | 60s (production-like) | Pre-production validation |
| `--env prod` | 🚀 | Production | Production S3 writes | 2 | 60s (robust) | Live production |

## 🚀 Quick Start (5 Minutes)

### Step 1: Install Prerequisites
```bash
# Java 11+, sbt, Docker
java -version && sbt --version && docker --version

# Download Flink 1.18.0
wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
tar -xzf flink-1.18.0-bin-scala_2.12.tgz
```

### Step 2: Setup & Build
```bash
# Clone and build
git clone <repository-url> && cd flink-cdc-s3
sbt clean assembly

# Start test databases with organized configuration
docker-compose up -d

# Enable S3 plugin
cd flink-1.18.0
mkdir -p plugins/flink-s3-fs-hadoop
cp opt/flink-s3-fs-hadoop-1.18.0.jar plugins/flink-s3-fs-hadoop/

# Start Flink
./bin/start-cluster.sh
```

### Step 3: Test with PostgreSQL (Default) - ✅ Verified Working
```bash
# Deploy in LOCAL mode with PostgreSQL (no S3 operations)
./bin/flink run -c com.example.cdc.ProductionCdcJob \
  ../target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --env local \
  --database.type postgres \
  --hostname localhost --port 5432 --database cdc_source \
  --username cdc_user --password cdc_password

# Test with data
docker exec postgres-cdc psql -U cdc_user -d cdc_source -c \
  "INSERT INTO public.users (name, email) VALUES ('Test User', 'test@example.com');"

# Watch logs for: 🧪 [users] LOCAL MODE: Would write to S3 (simulated)
tail -f log/flink-*-taskexecutor-*.out
```

### Step 4: Test with MySQL - 🆕 New Feature!
```bash
# Deploy in LOCAL mode with MySQL (no S3 operations)
./bin/flink run -c com.example.cdc.ProductionCdcJob \
  ../target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --env local \
  --database.type mysql \
  --hostname localhost --port 3306 --database cdc_source \
  --username cdc_user --password cdc_password

# Test with data
docker exec mysql-cdc mysql -u cdc_user -pcdc_password cdc_source -e \
  "INSERT INTO users (name, email) VALUES ('MySQL Test User', 'mysql@example.com');"

# Watch logs for: 🧪 [users] LOCAL MODE: Would write to S3 (simulated)
tail -f log/flink-*-taskexecutor-*.out
```

**✅ Success!** You should see CDC events being processed for both databases:
```
LOCAL-users> [users] LOCAL_SIMULATED: {"before":null,"after":{"id":1,"name":"Test User"...
MYSQL-users> [users] LOCAL_SIMULATED: {"before":null,"after":{"id":1,"name":"MySQL Test User"...
```

---

## 🔧 Production Setup

### Configure AWS Credentials (One-Time)

**Option 1: AWS Profiles (Recommended)**
```bash
# Setup environment-specific profiles
aws configure --profile dev
aws configure --profile staging  
aws configure --profile prod

# Configure Flink to use profiles
nano conf/flink-conf.yaml
```

Add to `flink-conf.yaml`:
```yaml
# Use dev profile (change to 'staging' or 'prod' for other environments)
env.java.opts.taskmanager: -DAWS_PROFILE=dev
env.java.opts.jobmanager: -DAWS_PROFILE=dev
```

### Deploy to Environments

**PostgreSQL Development Environment**
```bash
# Switch to dev profile in flink-conf.yaml
# Restart: ./bin/stop-cluster.sh && ./bin/start-cluster.sh

./bin/flink run -c com.example.cdc.ProductionCdcJob \
  ../target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --env dev \
  --database.type postgres \
  --hostname your-postgres-host --port 5432 --database your_db \
  --username your_user --password your_password \
  --s3-bucket your-dev-bucket \
  --s3-region us-east-1
```

**MySQL Production Environment** 🆕
```bash
# Switch to prod profile in flink-conf.yaml
# Restart: ./bin/stop-cluster.sh && ./bin/start-cluster.sh

./bin/flink run -c com.example.cdc.ProductionCdcJob \
  ../target/scala-2.12/flink-cdc-s3-production-assembly-1.2.0.jar \
  --env prod \
  --database.type mysql \
  --hostname your-mysql-host --port 3306 --database your_db \
  --username your_user --password your_password \
  --s3-bucket your-production-bucket \
  --s3-region us-east-1
```

---

## 📊 Database Configuration Guide

### PostgreSQL Configuration
- **Port**: 5432 (default)
- **Schema Support**: Full schema.table notation
- **CDC Method**: Logical replication with replication slots
- **Permissions Required**: REPLICATION, SELECT on tables

```bash
# Example PostgreSQL configuration
--database.type postgres
--postgres.hostname localhost
--postgres.port 5432
--postgres.database cdc_source
--postgres.username cdc_user
--postgres.password cdc_password
--postgres.schema-list public
--postgres.table-list public.users,public.orders
--postgres.slot-name flink_cdc_slot
```

### MySQL Configuration 🆕
- **Port**: 3306 (default)
- **Schema Support**: Database.table notation (schema = database)
- **CDC Method**: Binlog streaming with GTID support
- **Permissions Required**: REPLICATION SLAVE, REPLICATION CLIENT, SELECT

```bash
# Example MySQL configuration
--database.type mysql
--mysql.hostname localhost
--mysql.port 3306
--mysql.database cdc_source
--mysql.username cdc_user
--mysql.password cdc_password
--mysql.table-list cdc_source.users,cdc_source.orders
--mysql.server-id 5400-5404
```

### Automatic Database Detection

The system can automatically detect database type:
1. **Explicit**: `--database.type postgres|mysql`
2. **Parameter-based**: Presence of `postgres.*` or `mysql.*` parameters
3. **Default**: PostgreSQL (for backward compatibility)

---

## ⚙️ Configuration Reference

### Command Line Parameters
```bash
# Required
--env [local|dev|stg|prod]      # Deployment environment
--database.type [postgres|mysql] # Database type (optional - auto-detected)
--hostname <host>               # Database host
--database <db>                 # Database name
--username <user>               # Database user
--password <pass>               # Database password

# Required for S3-enabled modes (dev/stg/prod)
--s3-bucket <bucket>            # S3 bucket name
--s3-region <region>            # AWS region

# Database-Specific Optional Parameters
# PostgreSQL
--postgres.slot-name <slot>     # Replication slot name
--postgres.schema-list <schemas> # Schema list
--postgres.plugin-name <plugin> # Decoding plugin

# MySQL
--mysql.server-id <id-range>    # Server ID range
--mysql.table-list <tables>     # Full table names with database prefix

# General Optional
--s3-file-format [avro|json|parquet]
--s3-compression-type [snappy|gzip|lz4]
```

### Configuration File Examples

**PostgreSQL Configuration** (`application.properties`)
```properties
database.type=postgres
postgres.hostname=localhost
postgres.port=5432
postgres.database=cdc_source
postgres.username=cdc_user
postgres.password=cdc_password
postgres.schema-list=public
postgres.table-list=public.users,public.orders,public.products
postgres.slot-name=flink_cdc_slot_production
```

**MySQL Configuration** (`application.properties`)
```properties
database.type=mysql
mysql.hostname=localhost
mysql.port=3306
mysql.database=cdc_source
mysql.username=cdc_user
mysql.password=cdc_password
mysql.table-list=cdc_source.users,cdc_source.orders,cdc_source.products
mysql.server-id=5400-5404
mysql.incremental-snapshot-enabled=true
```

---

## 🔧 Technical Details

### Version Requirements
- **Apache Flink**: 1.18.0
- **Flink CDC**: 3.4.0
- **Java**: 11+
- **PostgreSQL**: 10+ with logical replication
- **MySQL**: 5.7+, 8.0+ with binlog enabled
- **Scala**: 2.12.17

### Database-Specific Requirements

**PostgreSQL Setup**
```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- Create CDC user
CREATE USER cdc_user WITH REPLICATION PASSWORD 'cdc_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

**MySQL Setup** 🆕
```sql
-- Enable binlog and GTID
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL gtid_mode = ON;
SET GLOBAL enforce_gtid_consistency = ON;

-- Create CDC user
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

### Project Structure (Enhanced v1.3.0)
```
src/main/scala/com/example/cdc/
├── ProductionCdcJob.scala           # Main application with 4-env + multi-DB support
├── config/
│   ├── AppConfig.scala             # Enhanced configuration with DB abstraction
│   ├── DatabaseConfig.scala        # 🆕 Database type abstraction
│   └── DatabaseSourceFactory.scala # 🆕 Multi-database source factory
├── sink/S3Sink.scala               # S3 integration  
├── monitoring/CDCMonitor.scala     # Environment-aware metrics
├── transformation/CDCEventProcessor.scala # Event processing
├── validation/EnvironmentValidator.scala  # Config validation
├── filters/TableFilter.scala             # Multi-table filtering
├── handlers/
│   ├── ErrorHandler.scala                # Unified error handling
│   └── SchemaChangeHandler.scala         # Schema change processing
└── mappers/CDCMappers.scala              # Environment-specific mappers
```

---

## 🚨 Troubleshooting

### Common Issues

**1. Database Connection Issues**
```bash
# PostgreSQL
pg_isready -h localhost -p 5432 -U cdc_user

# MySQL
mysqladmin ping -h localhost -P 3306 -u cdc_user -pcdc_password

# Check replication setup
# PostgreSQL
docker exec postgres-cdc psql -U postgres -d cdc_source -c \
  "SELECT * FROM pg_replication_slots;"

# MySQL
docker exec mysql-cdc mysql -u root -proot_password -e "SHOW MASTER STATUS;"
```

**2. Database Type Detection**
```bash
# Explicit database type (recommended)
--database.type mysql

# Check auto-detection in logs
grep "Database Type:" log/flink-*-jobmanager-*.log
```

**3. S3 Plugin Missing**
```bash
# Check plugin
ls -la flink-1.18.0/plugins/flink-s3-fs-hadoop/
# If missing: cp opt/flink-s3-fs-hadoop-1.18.0.jar plugins/flink-s3-fs-hadoop/
```

**4. MySQL-Specific Issues**
- ✅ Check binlog format: `SHOW VARIABLES LIKE 'binlog_format';` should be `ROW`
- ✅ Check GTID mode: `SHOW VARIABLES LIKE 'gtid_mode';` should be `ON`
- ✅ Verify server-id range covers parallelism: `--mysql.server-id 5400-5404` for parallelism ≤ 5

**5. PostgreSQL-Specific Issues**
- ✅ Check replication slots: `SELECT * FROM pg_replication_slots;`
- ✅ WAL level: `SHOW wal_level;` should be `logical`
- ✅ Plugin availability: `SELECT * FROM pg_available_extensions WHERE name = 'pgoutput';`

### Error Messages & Solutions
| Error | Database | Solution |
|-------|----------|----------|
| `ClassNotFoundException: S3AFileSystem` | Both | Enable S3 plugin |
| `Unable to load AWS credentials` | Both | Configure AWS profile |
| `Invalid database type: xyz` | Both | Use: postgres or mysql |
| `No replication slot` | PostgreSQL | Create replication slot |
| `Binlog position not found` | MySQL | Check binlog retention |
| `Server ID conflict` | MySQL | Use unique server-id range |

---

## 🚀 Production Checklist

### Pre-deployment (PostgreSQL)
- [ ] PostgreSQL logical replication enabled
- [ ] CDC user with proper permissions
- [ ] Replication slot created
- [ ] WAL level set to logical

### Pre-deployment (MySQL) 🆕
- [ ] MySQL binlog enabled (ROW format)
- [ ] GTID mode enabled
- [ ] CDC user with replication permissions
- [ ] Binlog retention configured

### Deployment (Both Databases)
- [ ] AWS credentials configured
- [ ] S3 bucket accessible
- [ ] Flink cluster running
- [ ] Monitoring enabled
- [ ] Database type explicitly specified

---

## 🔍 Migration Guide

### From PostgreSQL-only to Multi-Database

**Option 1: Zero Changes (Recommended)**
Your existing PostgreSQL deployments continue working without any changes.

**Option 2: Explicit Configuration**
```bash
# Add explicit database type for clarity
--database.type postgres

# Use database-specific parameters
--postgres.hostname instead of --hostname
--postgres.port instead of --port
```

**Option 3: Add MySQL Support**
```bash
# Deploy alongside PostgreSQL or replace entirely
--database.type mysql
--mysql.hostname your-mysql-host
--mysql.port 3306
--mysql.database your_db
```

---

## 📈 Performance Comparison

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| Snapshot Reading | ✅ Parallel | ✅ Parallel |
| Incremental CDC | ✅ Replication Slots | ✅ Binlog Streaming |
| Schema Evolution | ✅ Supported | ✅ Supported |
| Exactly-Once | ✅ Guaranteed | ✅ Guaranteed |
| High Availability | ✅ Standby Support | ✅ GTID Support |
| Table Filtering | ✅ Schema.table | ✅ Database.table |

Both databases provide excellent CDC performance with sub-second latency for real-time scenarios.

---

## 🎯 What's New in v1.4.0

- **🔧 Ververica CDC 2.4.2**: Upgraded to stable, unified CDC connectors
- **📁 Organized Structure**: Database configuration files moved to `database/` folder
- **⚡ Performance Optimized**: MySQL CDC configuration tuned for production use
- **🤝 Unified API**: Both MySQL and PostgreSQL use consistent Legacy API approach  
- **🧪 I,D,U Verified**: Comprehensive Insert, Delete, Update operation testing
- **📋 Better Organization**: Clean separation of init scripts and configuration files

## 🎯 Previous Updates (v1.3.0)

- **🆕 MySQL Support**: Full MySQL CDC integration alongside PostgreSQL
- **🎛️ Database Abstraction**: Clean, extensible architecture for future databases
- **🔄 Auto-Detection**: Intelligent database type detection
- **📊 Enhanced Config**: Comprehensive configuration management
- **🐳 Docker Support**: Both PostgreSQL and MySQL development environments
- **📚 Documentation**: Complete multi-database usage guide
- **🔧 Backward Compatible**: Existing PostgreSQL deployments unchanged

---

**Ready to stream your database changes to S3?** 
🚀 Start with local testing, then deploy to production with confidence! 