#!/bin/bash

# PostgreSQL CDC to S3 Production Deployment Script
# This script handles building, testing, and deploying the production CDC pipeline

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${PROJECT_DIR}/target"
DEPLOY_DIR="${PROJECT_DIR}/deploy"
CONFIG_DIR="${PROJECT_DIR}/config"
LOG_DIR="${PROJECT_DIR}/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
ENVIRONMENT="production"
FLINK_VERSION="1.17.0"
PARALLELISM=2
CHECKPOINT_INTERVAL=30000
S3_BUCKET="flink-cdc-output"
S3_BASE_PATH="cdc-events"

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "================================================================="
    echo "PostgreSQL CDC to S3 Production Deployment"
    echo "================================================================="
    echo -e "${NC}"
}

# Print usage information
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --env ENV                Set environment (default: production)"
    echo "  -p, --parallelism NUM        Set Flink parallelism (default: 2)"
    echo "  -c, --checkpoint-interval MS Set checkpoint interval (default: 30000)"
    echo "  -b, --s3-bucket NAME         Set S3 bucket name (default: flink-cdc-output)"
    echo "  -s, --s3-path PATH           Set S3 base path (default: cdc-events)"
    echo "  --skip-build                 Skip building JAR file"
    echo "  --skip-tests                 Skip running tests"
    echo "  --config-file FILE           Use custom configuration file"
    echo "  --dry-run                    Show what would be done without executing"
    echo "  -h, --help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --env staging --parallelism 1"
    echo "  $0 --s3-bucket my-bucket --s3-path my-path"
    echo "  $0 --config-file /path/to/config.properties"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -p|--parallelism)
                PARALLELISM="$2"
                shift 2
                ;;
            -c|--checkpoint-interval)
                CHECKPOINT_INTERVAL="$2"
                shift 2
                ;;
            -b|--s3-bucket)
                S3_BUCKET="$2"
                shift 2
                ;;
            -s|--s3-path)
                S3_BASE_PATH="$2"
                shift 2
                ;;
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            --config-file)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                echo -e "${RED}Error: Unknown option $1${NC}"
                print_usage
                exit 1
                ;;
        esac
    done
}

# Create necessary directories
create_directories() {
    echo -e "${BLUE}Creating directories...${NC}"
    
    mkdir -p "$BUILD_DIR"
    mkdir -p "$DEPLOY_DIR"
    mkdir -p "$CONFIG_DIR"
    mkdir -p "$LOG_DIR"
    
    echo -e "${GREEN}✓ Directories created${NC}"
}

# Validate environment and dependencies
validate_environment() {
    echo -e "${BLUE}Validating environment...${NC}"
    
    # Check Java version
    if ! java -version 2>&1 | grep -q "1.8\|11\|17"; then
        echo -e "${RED}Error: Java 8, 11, or 17 is required${NC}"
        exit 1
    fi
    
    # Check Scala version
    if ! scala -version 2>&1 | grep -q "2.12"; then
        echo -e "${YELLOW}Warning: Scala 2.12 is recommended${NC}"
    fi
    
    # Check sbt
    if ! command -v sbt &> /dev/null; then
        echo -e "${RED}Error: sbt is required${NC}"
        exit 1
    fi
    
    # Check PostgreSQL connectivity (optional)
    if [[ -n "$POSTGRES_HOST" ]]; then
        if ! pg_isready -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-postgres}" &> /dev/null; then
            echo -e "${YELLOW}Warning: Cannot connect to PostgreSQL${NC}"
        fi
    fi
    
    echo -e "${GREEN}✓ Environment validated${NC}"
}

# Build the application
build_application() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        echo -e "${YELLOW}Skipping build...${NC}"
        return
    fi
    
    echo -e "${BLUE}Building application...${NC}"
    
    cd "$PROJECT_DIR"
    
    # Clean previous builds
    echo "Cleaning previous builds..."
    sbt clean
    
    # Compile and run tests
    if [[ "$SKIP_TESTS" != "true" ]]; then
        echo "Running tests..."
        sbt test
    fi
    
    # Build assembly JAR
    echo "Building assembly JAR..."
    sbt assembly
    
    # Verify JAR was created
    JAR_FILE="$BUILD_DIR/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar"
    if [[ ! -f "$JAR_FILE" ]]; then
        echo -e "${RED}Error: JAR file not found: $JAR_FILE${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Application built successfully${NC}"
    echo -e "${GREEN}  JAR file: $JAR_FILE${NC}"
    echo -e "${GREEN}  Size: $(du -h "$JAR_FILE" | cut -f1)${NC}"
}

# Generate configuration file
generate_config() {
    echo -e "${BLUE}Generating configuration...${NC}"
    
    CONFIG_FILE="${CONFIG_DIR}/${ENVIRONMENT}.properties"
    
    cat > "$CONFIG_FILE" << EOF
# PostgreSQL CDC to S3 Configuration - Environment: ${ENVIRONMENT}
# Generated on: $(date)

# PostgreSQL Database Configuration
postgres.hostname=${POSTGRES_HOST:-localhost}
postgres.port=${POSTGRES_PORT:-5432}
postgres.database=${POSTGRES_DATABASE:-cdc_source}
postgres.username=${POSTGRES_USER:-postgres}
postgres.password=${POSTGRES_PASSWORD:-postgres}
postgres.schema-list=${POSTGRES_SCHEMA_LIST:-public}
postgres.table-list=${POSTGRES_TABLE_LIST:-public.users}
postgres.slot-name=${POSTGRES_SLOT_NAME:-flink_cdc_slot_production}

# S3 Configuration
s3.bucket-name=${S3_BUCKET}
s3.base-path=${S3_BASE_PATH}
s3.region=${AWS_REGION:-us-east-1}
s3.access-key=${AWS_ACCESS_KEY_ID:-}
s3.secret-key=${AWS_SECRET_ACCESS_KEY:-}

# Flink Configuration
flink.job-name=PostgreSQL-CDC-to-S3-${ENVIRONMENT}
flink.parallelism=${PARALLELISM}
flink.checkpoint-interval-ms=${CHECKPOINT_INTERVAL}

# File Format
s3.file-format=${FILE_FORMAT:-json}
s3.compression-type=${COMPRESSION_TYPE:-gzip}
s3.max-file-size=${MAX_FILE_SIZE:-128MB}
s3.rollover-interval=${ROLLOVER_INTERVAL:-5min}

# Monitoring
monitoring.enable-metrics=${ENABLE_METRICS:-true}
monitoring.metrics-reporter=${METRICS_REPORTER:-prometheus}
EOF
    
    echo -e "${GREEN}✓ Configuration generated: $CONFIG_FILE${NC}"
}

# Create deployment package
create_deployment_package() {
    echo -e "${BLUE}Creating deployment package...${NC}"
    
    PACKAGE_NAME="postgres-cdc-s3-${ENVIRONMENT}-$(date +%Y%m%d-%H%M%S)"
    PACKAGE_DIR="${DEPLOY_DIR}/${PACKAGE_NAME}"
    
    mkdir -p "$PACKAGE_DIR"
    
    # Copy JAR file
    cp "$BUILD_DIR/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar" "$PACKAGE_DIR/"
    
    # Copy configuration
    cp "$CONFIG_FILE" "$PACKAGE_DIR/application.properties"
    
    # Copy scripts
    cp -r "$SCRIPT_DIR" "$PACKAGE_DIR/scripts"
    
    # Create deployment info
    cat > "$PACKAGE_DIR/deployment-info.txt" << EOF
Deployment Package: ${PACKAGE_NAME}
Environment: ${ENVIRONMENT}
Build Date: $(date)
Git Commit: $(git rev-parse HEAD 2>/dev/null || echo "unknown")
JAR File: postgres-cdc-s3-production-assembly-1.0.0.jar
Configuration: application.properties
EOF
    
    # Create archive
    cd "$DEPLOY_DIR"
    tar -czf "${PACKAGE_NAME}.tar.gz" "$PACKAGE_NAME"
    
    echo -e "${GREEN}✓ Deployment package created: ${DEPLOY_DIR}/${PACKAGE_NAME}.tar.gz${NC}"
}

# Generate run script
generate_run_script() {
    echo -e "${BLUE}Generating run script...${NC}"
    
    RUN_SCRIPT="${DEPLOY_DIR}/run-${ENVIRONMENT}.sh"
    
    cat > "$RUN_SCRIPT" << 'EOF'
#!/bin/bash

# Production Run Script
# This script runs the PostgreSQL CDC to S3 pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/postgres-cdc-s3-production-assembly-1.0.0.jar"
CONFIG_FILE="$SCRIPT_DIR/application.properties"

# Check if JAR file exists
if [[ ! -f "$JAR_FILE" ]]; then
    echo "Error: JAR file not found: $JAR_FILE"
    exit 1
fi

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Set JVM options
JVM_OPTS="-Xmx2g -Xms1g -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

# Run the application
echo "Starting PostgreSQL CDC to S3 pipeline..."
echo "JAR: $JAR_FILE"
echo "Config: $CONFIG_FILE"
echo "JVM Options: $JVM_OPTS"

exec java $JVM_OPTS -jar "$JAR_FILE" \
  --config "$CONFIG_FILE" \
  "$@"
EOF
    
    chmod +x "$RUN_SCRIPT"
    
    echo -e "${GREEN}✓ Run script created: $RUN_SCRIPT${NC}"
}

# Main deployment function
main() {
    print_banner
    parse_args "$@"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}DRY RUN MODE - No actual deployment will be performed${NC}"
        echo "Environment: $ENVIRONMENT"
        echo "Parallelism: $PARALLELISM"
        echo "Checkpoint Interval: $CHECKPOINT_INTERVAL"
        echo "S3 Bucket: $S3_BUCKET"
        echo "S3 Base Path: $S3_BASE_PATH"
        exit 0
    fi
    
    create_directories
    validate_environment
    build_application
    generate_config
    create_deployment_package
    generate_run_script
    
    echo -e "${GREEN}"
    echo "================================================================="
    echo "Deployment completed successfully!"
    echo "================================================================="
    echo "Environment: $ENVIRONMENT"
    echo "Package: ${DEPLOY_DIR}/postgres-cdc-s3-${ENVIRONMENT}-*.tar.gz"
    echo "Run Script: ${DEPLOY_DIR}/run-${ENVIRONMENT}.sh"
    echo "Configuration: ${CONFIG_DIR}/${ENVIRONMENT}.properties"
    echo -e "${NC}"
    
    echo -e "${BLUE}Next steps:${NC}"
    echo "1. Review the configuration file"
    echo "2. Set up PostgreSQL replication slot"
    echo "3. Configure S3 permissions"
    echo "4. Deploy to target environment"
    echo "5. Start the pipeline with the run script"
}

# Run main function
main "$@" 