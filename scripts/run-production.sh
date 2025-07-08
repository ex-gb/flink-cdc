#!/bin/bash

# PostgreSQL CDC to S3 Production Run Script
# This script starts the production CDC pipeline with proper monitoring and configuration

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAR_FILE="$PROJECT_DIR/target/scala-2.12/postgres-cdc-s3-production-assembly-1.0.0.jar"
CONFIG_FILE="$PROJECT_DIR/config/production.properties"
LOG_DIR="$PROJECT_DIR/logs"
PID_FILE="$LOG_DIR/cdc-pipeline.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default JVM options
JVM_OPTS="-Xmx4g -Xms2g -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_DIR"
JVM_OPTS="$JVM_OPTS -Dlogback.configurationFile=$PROJECT_DIR/src/main/resources/logback.xml"

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "================================================================="
    echo "PostgreSQL CDC to S3 Production Pipeline"
    echo "================================================================="
    echo -e "${NC}"
}

# Print usage information
print_usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  start           Start the CDC pipeline"
    echo "  stop            Stop the CDC pipeline"
    echo "  restart         Restart the CDC pipeline"
    echo "  status          Show pipeline status"
    echo "  logs            Show recent logs"
    echo "  monitor         Monitor pipeline in real-time"
    echo ""
    echo "Options:"
    echo "  --config FILE   Use custom configuration file"
    echo "  --jar FILE      Use custom JAR file"
    echo "  --memory SIZE   Set JVM memory (e.g., 4g)"
    echo "  --parallelism N Set Flink parallelism"
    echo "  --checkpoint-interval MS Set checkpoint interval"
    echo "  --dry-run       Show what would be done without executing"
    echo "  -h, --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 start --config /path/to/config.properties"
    echo "  $0 start --memory 8g --parallelism 4"
    echo "  $0 stop"
    echo "  $0 status"
    echo "  $0 logs"
}

# Parse command line arguments
parse_args() {
    COMMAND="$1"
    shift
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --jar)
                JAR_FILE="$2"
                shift 2
                ;;
            --memory)
                MEMORY="$2"
                JVM_OPTS=$(echo "$JVM_OPTS" | sed -e "s/-Xmx[0-9]*[gm]/-Xmx$MEMORY/g" -e "s/-Xms[0-9]*[gm]/-Xms$MEMORY/g")
                shift 2
                ;;
            --parallelism)
                PARALLELISM="$2"
                shift 2
                ;;
            --checkpoint-interval)
                CHECKPOINT_INTERVAL="$2"
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

# Check if pipeline is running
is_running() {
    if [[ -f "$PID_FILE" ]]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            return 0
        else
            rm -f "$PID_FILE"
            return 1
        fi
    fi
    return 1
}

# Validate environment
validate_environment() {
    echo -e "${BLUE}Validating environment...${NC}"
    
    # Check Java version
    if ! java -version 2>&1 | grep -q "1.8\|11\|17"; then
        echo -e "${RED}Error: Java 8, 11, or 17 is required${NC}"
        exit 1
    fi
    
    # Check JAR file
    if [[ ! -f "$JAR_FILE" ]]; then
        echo -e "${RED}Error: JAR file not found: $JAR_FILE${NC}"
        echo "Please run 'sbt assembly' first or specify --jar option"
        exit 1
    fi
    
    # Check configuration file
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo -e "${YELLOW}Warning: Configuration file not found: $CONFIG_FILE${NC}"
        echo "Using default configuration"
    fi
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    echo -e "${GREEN}✓ Environment validated${NC}"
}

# Start the pipeline
start_pipeline() {
    if is_running; then
        echo -e "${YELLOW}Pipeline is already running (PID: $(cat "$PID_FILE"))${NC}"
        return 0
    fi
    
    echo -e "${BLUE}Starting PostgreSQL CDC to S3 pipeline...${NC}"
    
    validate_environment
    
    # Build command
    CMD="java $JVM_OPTS -jar \"$JAR_FILE\""
    
    # Add configuration file if it exists
    if [[ -f "$CONFIG_FILE" ]]; then
        CMD="$CMD --config \"$CONFIG_FILE\""
    fi
    
    # Add runtime parameters
    if [[ -n "$PARALLELISM" ]]; then
        CMD="$CMD --flink.parallelism $PARALLELISM"
    fi
    
    if [[ -n "$CHECKPOINT_INTERVAL" ]]; then
        CMD="$CMD --flink.checkpoint-interval-ms $CHECKPOINT_INTERVAL"
    fi
    
    # Show command in dry run mode
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}DRY RUN MODE - Command that would be executed:${NC}"
        echo "$CMD"
        return 0
    fi
    
    # Start the application
    echo -e "${GREEN}Starting pipeline...${NC}"
    echo "JAR: $JAR_FILE"
    echo "Config: $CONFIG_FILE"
    echo "Logs: $LOG_DIR"
    echo "JVM Options: $JVM_OPTS"
    
    # Start in background and save PID
    nohup bash -c "$CMD" > "$LOG_DIR/cdc-pipeline.out" 2>&1 &
    echo $! > "$PID_FILE"
    
    # Wait a moment and check if it's still running
    sleep 2
    if is_running; then
        echo -e "${GREEN}✓ Pipeline started successfully (PID: $(cat "$PID_FILE"))${NC}"
        echo "Use '$0 status' to check pipeline status"
        echo "Use '$0 logs' to view logs"
    else
        echo -e "${RED}✗ Pipeline failed to start${NC}"
        echo "Check logs for details: $LOG_DIR/cdc-pipeline.out"
        exit 1
    fi
}

# Stop the pipeline
stop_pipeline() {
    if ! is_running; then
        echo -e "${YELLOW}Pipeline is not running${NC}"
        return 0
    fi
    
    echo -e "${BLUE}Stopping PostgreSQL CDC to S3 pipeline...${NC}"
    
    PID=$(cat "$PID_FILE")
    echo "Stopping process $PID..."
    
    # Try graceful shutdown first
    kill "$PID" 2>/dev/null || true
    
    # Wait for graceful shutdown
    for i in {1..30}; do
        if ! ps -p "$PID" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Pipeline stopped gracefully${NC}"
            rm -f "$PID_FILE"
            return 0
        fi
        sleep 1
    done
    
    # Force kill if still running
    echo "Force killing process $PID..."
    kill -9 "$PID" 2>/dev/null || true
    rm -f "$PID_FILE"
    
    echo -e "${GREEN}✓ Pipeline stopped${NC}"
}

# Restart the pipeline
restart_pipeline() {
    echo -e "${BLUE}Restarting PostgreSQL CDC to S3 pipeline...${NC}"
    stop_pipeline
    sleep 2
    start_pipeline
}

# Show pipeline status
show_status() {
    echo -e "${BLUE}Pipeline Status:${NC}"
    
    if is_running; then
        PID=$(cat "$PID_FILE")
        echo -e "${GREEN}✓ Running (PID: $PID)${NC}"
        
        # Show process information
        echo ""
        echo "Process Information:"
        ps -p "$PID" -o pid,ppid,cmd,etime,pcpu,pmem 2>/dev/null || echo "Process details not available"
        
        # Show recent log entries
        echo ""
        echo "Recent Log Entries:"
        if [[ -f "$LOG_DIR/cdc-pipeline.out" ]]; then
            tail -5 "$LOG_DIR/cdc-pipeline.out"
        else
            echo "No logs available"
        fi
        
    else
        echo -e "${RED}✗ Not running${NC}"
    fi
    
    # Show system resources
    echo ""
    echo "System Resources:"
    echo "Memory: $(free -h | grep '^Mem:' | awk '{print $3 "/" $2}')"
    echo "CPU: $(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1"%"}')"
    echo "Disk: $(df -h / | tail -1 | awk '{print $3 "/" $2 " (" $5 ")"}')"
}

# Show logs
show_logs() {
    echo -e "${BLUE}Recent Logs:${NC}"
    
    if [[ -f "$LOG_DIR/cdc-pipeline.out" ]]; then
        tail -50 "$LOG_DIR/cdc-pipeline.out"
    else
        echo "No logs available"
    fi
}

# Monitor pipeline
monitor_pipeline() {
    echo -e "${BLUE}Monitoring PostgreSQL CDC to S3 pipeline...${NC}"
    echo "Press Ctrl+C to stop monitoring"
    
    if [[ -f "$LOG_DIR/cdc-pipeline.out" ]]; then
        tail -f "$LOG_DIR/cdc-pipeline.out"
    else
        echo "No logs available"
    fi
}

# Main function
main() {
    print_banner
    
    if [[ $# -eq 0 ]]; then
        print_usage
        exit 1
    fi
    
    parse_args "$@"
    
    case "$COMMAND" in
        start)
            start_pipeline
            ;;
        stop)
            stop_pipeline
            ;;
        restart)
            restart_pipeline
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs
            ;;
        monitor)
            monitor_pipeline
            ;;
        *)
            echo -e "${RED}Error: Unknown command '$COMMAND'${NC}"
            print_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@" 