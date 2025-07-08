#!/bin/bash

# Local Run Script for PostgreSQL CDC to File Pipeline
# Updated to include multiple job options and file finalization testing

set -e

echo "🚀 PostgreSQL CDC to File Pipeline - Local Execution"
echo "=================================================="

# Configuration
JAR_FILE="target/scala-2.12/postgres-cdc-s3-local-assembly-1.0.0.jar"
FLINK_DIR="./flink-1.17.0"
OUTPUT_DIR="$FLINK_DIR/output/cdc-events"

# Function to show usage
show_usage() {
    echo "Usage: $0 [JOB_TYPE] [OPTIONS]"
    echo ""
    echo "Job Types:"
    echo "  main       Run main CDC job with checkpointing (recommended)"
    echo "  alt        Run alternative CDC job without checkpointing"
    echo "  simple     Run simple CDC job (console output only)"
    echo ""
    echo "Options:"
    echo "  --build    Build the project before running"
    echo "  --clean    Clean output directory before running"
    echo "  --status   Show file status after running"
    echo "  --help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 main --build               # Build and run main job"
    echo "  $0 alt --clean                # Clean and run alternative job"
    echo "  $0 simple                     # Run simple job"
    echo "  ./scripts/finalize-files.sh   # Manually finalize files"
}

# Function to check prerequisites
check_prerequisites() {
    echo "🔍 Checking prerequisites..."
    
    # Check if PostgreSQL is running
    if ! docker ps | grep -q postgres-cdc; then
        echo "❌ PostgreSQL CDC container is not running!"
        echo "   Please run: ./scripts/setup.sh"
        exit 1
    fi
    
    # Check if JAR exists
    if [ ! -f "$JAR_FILE" ]; then
        echo "❌ JAR file not found: $JAR_FILE"
        echo "   Building project..."
        sbt assembly
    fi
    
    # Check Flink directory
    if [ ! -d "$FLINK_DIR" ]; then
        echo "❌ Flink directory not found: $FLINK_DIR"
        echo "   Please ensure Flink is properly installed"
        exit 1
    fi
    
    echo "✅ Prerequisites check completed"
}

# Function to build project
build_project() {
    echo "🔨 Building project..."
    sbt clean assembly
    echo "✅ Build completed"
}

# Function to clean output directory
clean_output() {
    echo "🧹 Cleaning output directory..."
    if [ -d "$OUTPUT_DIR" ]; then
        rm -rf "$OUTPUT_DIR"
        echo "✅ Output directory cleaned"
    else
        echo "ℹ️  Output directory doesn't exist"
    fi
}

# Function to create checkpoint directory
setup_checkpoints() {
    echo "📁 Setting up checkpoint directory..."
    mkdir -p /tmp/flink-checkpoints
    echo "✅ Checkpoint directory ready"
}

# Function to run main CDC job (with checkpointing)
run_main_job() {
    echo "🎯 Running Main CDC Job (with checkpointing)..."
    echo "This version enables checkpointing for automatic file finalization"
    echo ""
    
    setup_checkpoints
    
    cd "$FLINK_DIR"
    
    # Start Flink cluster if not running
    if ! ./bin/flink list 2>/dev/null | grep -q "Running Jobs"; then
        echo "Starting Flink cluster..."
        ./bin/start-cluster.sh
        sleep 3
    fi
    
    # Submit job
    ./bin/flink run \
        -c com.example.cdc.PostgresCdcToFileJob \
        "../$JAR_FILE"
    
    cd ..
}

# Function to run alternative CDC job (without checkpointing)
run_alt_job() {
    echo "🔄 Running Alternative CDC Job (no checkpointing)..."
    echo "This version uses time-based file rolling without checkpoints"
    echo ""
    
    cd "$FLINK_DIR"
    
    # Start Flink cluster if not running
    if ! ./bin/flink list 2>/dev/null | grep -q "Running Jobs"; then
        echo "Starting Flink cluster..."
        ./bin/start-cluster.sh
        sleep 3
    fi
    
    # Submit job
    ./bin/flink run \
        -c com.example.cdc.PostgresCdcToFileJobAlternative \
        "../$JAR_FILE"
    
    cd ..
}

# Function to run simple CDC job
run_simple_job() {
    echo "📺 Running Simple CDC Job (console output only)..."
    echo "This version outputs to console for testing"
    echo ""
    
    cd "$FLINK_DIR"
    
    # Start Flink cluster if not running
    if ! ./bin/flink list 2>/dev/null | grep -q "Running Jobs"; then
        echo "Starting Flink cluster..."
        ./bin/start-cluster.sh
        sleep 3
    fi
    
    # Submit job
    ./bin/flink run \
        -c com.example.cdc.SimplePostgresCdcJob \
        "../$JAR_FILE"
    
    cd ..
}

# Function to show file status
show_file_status() {
    echo ""
    echo "📊 File Status:"
    ./scripts/finalize-files.sh --status
}

# Function to show post-run information
show_post_run_info() {
    echo ""
    echo "📋 Post-Run Information:"
    echo "======================="
    echo ""
    echo "🔧 File Management Commands:"
    echo "  ./scripts/finalize-files.sh --status        # Check file status"
    echo "  ./scripts/finalize-files.sh --finalize      # Manually finalize files"
    echo "  ./scripts/finalize-files.sh --backup        # Create backup"
    echo ""
    echo "🎛️  Flink Management Commands:"
    echo "  cd $FLINK_DIR && ./bin/flink list           # List running jobs"
    echo "  cd $FLINK_DIR && ./bin/flink cancel <job-id># Cancel job"
    echo "  cd $FLINK_DIR && ./bin/stop-cluster.sh      # Stop Flink cluster"
    echo ""
    echo "📁 Output Directory: $OUTPUT_DIR"
    echo ""
    echo "💡 Troubleshooting:"
    echo "  - If files stay .inprogress: Use finalize-files.sh --finalize"
    echo "  - If checkpointing fails: Try the alternative job (alt)"
    echo "  - Check logs: $FLINK_DIR/log/"
}

# Parse arguments
JOB_TYPE=""
BUILD=false
CLEAN=false
STATUS=false

if [[ $# -eq 0 ]]; then
    show_usage
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        main|alt|simple)
            JOB_TYPE=$1
            shift
            ;;
        --build)
            BUILD=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --status)
            STATUS=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate job type
if [ -z "$JOB_TYPE" ]; then
    echo "❌ Please specify a job type (main, alt, or simple)"
    show_usage
    exit 1
fi

# Execute actions
check_prerequisites

if [ "$BUILD" = true ]; then
    build_project
fi

if [ "$CLEAN" = true ]; then
    clean_output
fi

# Run the specified job
case $JOB_TYPE in
    main)
        run_main_job
        ;;
    alt)
        run_alt_job
        ;;
    simple)
        run_simple_job
        ;;
esac

if [ "$STATUS" = true ]; then
    show_file_status
fi

show_post_run_info

echo ""
echo "✨ Job execution completed!" 