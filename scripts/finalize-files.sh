#!/bin/bash

# File Finalization Utility for Flink CDC Pipeline
# This script helps finalize .inprogress files when checkpointing issues occur

set -e

echo "🔧 Flink CDC File Finalization Utility"
echo "======================================"

# Configuration
OUTPUT_DIR="./flink-1.17.0/output/cdc-events"
BACKUP_DIR="./flink-1.17.0/output/cdc-events-backup-$(date +%Y%m%d_%H%M%S)"

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --finalize    Finalize all .inprogress files"
    echo "  --status      Show status of files"
    echo "  --backup      Create backup before finalizing"
    echo "  --clean       Remove empty .inprogress files"
    echo "  --help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --status                    # Show current file status"
    echo "  $0 --finalize                  # Finalize all .inprogress files"
    echo "  $0 --backup --finalize         # Backup then finalize"
    echo "  $0 --clean                     # Remove empty .inprogress files"
}

# Function to show file status
show_status() {
    echo "📊 File Status Report"
    echo "===================="
    
    if [ ! -d "$OUTPUT_DIR" ]; then
        echo "❌ Output directory not found: $OUTPUT_DIR"
        return 1
    fi
    
    echo "📂 Output directory: $OUTPUT_DIR"
    echo ""
    
    # Count different file types
    local inprogress_count=$(find "$OUTPUT_DIR" -name "*.inprogress" 2>/dev/null | wc -l)
    local finalized_count=$(find "$OUTPUT_DIR" -name "*.jsonl" ! -name "*.inprogress" 2>/dev/null | wc -l)
    local total_size=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)
    
    echo "📈 Summary:"
    echo "  - In-progress files: $inprogress_count"
    echo "  - Finalized files: $finalized_count"
    echo "  - Total size: $total_size"
    echo ""
    
    if [ $inprogress_count -gt 0 ]; then
        echo "📋 In-progress files:"
        find "$OUTPUT_DIR" -name "*.inprogress" | while read -r file; do
            local size=$(du -h "$file" 2>/dev/null | cut -f1)
            local lines=$(wc -l < "$file" 2>/dev/null || echo "0")
            echo "  - $(basename "$file") (${size}, ${lines} lines)"
        done
        echo ""
    fi
    
    if [ $finalized_count -gt 0 ]; then
        echo "✅ Recent finalized files:"
        find "$OUTPUT_DIR" -name "*.jsonl" ! -name "*.inprogress" | head -5 | while read -r file; do
            local size=$(du -h "$file" 2>/dev/null | cut -f1)
            local lines=$(wc -l < "$file" 2>/dev/null || echo "0")
            echo "  - $(basename "$file") (${size}, ${lines} lines)"
        done
        echo ""
    fi
}

# Function to create backup
create_backup() {
    echo "💾 Creating backup..."
    
    if [ ! -d "$OUTPUT_DIR" ]; then
        echo "❌ Output directory not found: $OUTPUT_DIR"
        return 1
    fi
    
    mkdir -p "$BACKUP_DIR"
    cp -r "$OUTPUT_DIR"/* "$BACKUP_DIR"/ 2>/dev/null || true
    
    echo "✅ Backup created: $BACKUP_DIR"
}

# Function to finalize .inprogress files
finalize_files() {
    echo "🔄 Finalizing .inprogress files..."
    
    if [ ! -d "$OUTPUT_DIR" ]; then
        echo "❌ Output directory not found: $OUTPUT_DIR"
        return 1
    fi
    
    local count=0
    find "$OUTPUT_DIR" -name "*.inprogress" | while read -r file; do
        # Remove .inprogress extension
        local new_file="${file%.inprogress}"
        
        # Check if file has content
        if [ -s "$file" ]; then
            mv "$file" "$new_file"
            echo "  ✅ Finalized: $(basename "$new_file")"
            count=$((count + 1))
        else
            echo "  ⚠️  Skipped empty file: $(basename "$file")"
        fi
    done
    
    echo "🎉 Finalization complete!"
}

# Function to clean empty .inprogress files
clean_empty_files() {
    echo "🧹 Cleaning empty .inprogress files..."
    
    if [ ! -d "$OUTPUT_DIR" ]; then
        echo "❌ Output directory not found: $OUTPUT_DIR"
        return 1
    fi
    
    local count=0
    find "$OUTPUT_DIR" -name "*.inprogress" -empty | while read -r file; do
        rm "$file"
        echo "  🗑️  Removed empty file: $(basename "$file")"
        count=$((count + 1))
    done
    
    echo "✅ Cleanup complete!"
}

# Main script logic
BACKUP=false
FINALIZE=false
STATUS=false
CLEAN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --backup)
            BACKUP=true
            shift
            ;;
        --finalize)
            FINALIZE=true
            shift
            ;;
        --status)
            STATUS=true
            shift
            ;;
        --clean)
            CLEAN=true
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

# Show status if no other action specified
if [ "$BACKUP" = false ] && [ "$FINALIZE" = false ] && [ "$CLEAN" = false ]; then
    STATUS=true
fi

# Execute actions
if [ "$STATUS" = true ]; then
    show_status
fi

if [ "$BACKUP" = true ]; then
    create_backup
fi

if [ "$CLEAN" = true ]; then
    clean_empty_files
fi

if [ "$FINALIZE" = true ]; then
    finalize_files
fi

echo ""
echo "✨ File finalization utility completed!" 